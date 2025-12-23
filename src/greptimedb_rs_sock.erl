%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(greptimedb_rs_sock).

-include("greptimedb_rs.hrl").

-behavior(gen_server).
-behavior(ecpool_worker).

-record(state, {
    client :: client_ref() | undefined,
    opts :: opts() | undefined,
    writers = #{} :: map()
}).

-define(client_ref(Ref), #state{client = Ref}).

-define(is_ok, {ok, _}).
-define(is_err, {error, _}).

-export([
    connect/1,
    sync_command/3
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_info/2,
    handle_cast/2,
    terminate/2
]).

-type args() :: term().
-type conn() :: pid().
-type opts() :: greptimedb_rs:opts().
-type client_ref() :: reference().

%% ================================================================================
%% API

-spec sync_command(conn(), command(), args()) -> any().
sync_command(Conn, Command, Args) ->
    gen_server:call(Conn, ?REQ(Command, Args), infinity).

%% ================================================================================
%% ecpool callbacks

connect(Args) when is_list(Args) ->
    case proplists:get_value(conn_opts, Args) of
        undefined -> {ok, #state{client = undefined, opts = undefined, writers = #{}}};
        Opts -> connect(Opts)
    end;
connect(Opts) when is_map(Opts) ->
    %% Start the connection gen_server process
    gen_server:start_link(?MODULE, [Opts], []).

%% ================================================================================

init([Opts0]) ->
    Opts = unwrap_password(Opts0),
    case apply_nif(?cmd_connect, [Opts]) of
        {ok, ClientRef} ->
            {ok, #state{client = ClientRef, opts = Opts0, writers = #{}}};
        {error, Reason} ->
            {stop, Reason}
    end.

%% ================================================================================
handle_call(
    ?REQ(?cmd_connect, _Args = [Opts0]),
    _From,
    State = #state{client = undefined}
) ->
    Opts = unwrap_password(Opts0),
    case apply_nif(?cmd_connect, [Opts]) of
        {ok, ClientRef} = Ok when is_reference(ClientRef) ->
            {reply, Ok, State#state{client = ClientRef, opts = Opts0}};
        ?is_err = Err ->
            {reply, Err, State}
    end;
handle_call(?REQ(?cmd_connect, _), _From, State) ->
    {reply, {ok, State#state.client}, State};
handle_call(?REQ(?cmd_execute, [Sql]), _From, State = ?client_ref(ClientRef)) ->
    Result = apply_nif(?cmd_execute, [ClientRef, Sql]),
    {reply, Result, State};
handle_call(?REQ(?cmd_insert, [Table, Rows]), _From, State = ?client_ref(ClientRef)) ->
    Result = apply_nif(?cmd_insert, [ClientRef, Table, Rows]),
    {reply, Result, State};
handle_call(
    ?REQ(?cmd_stream_start, [Table, FirstRow]),
    _From,
    State = #state{client = ClientRef, writers = Writers}
) ->
    case apply_nif(?cmd_stream_start, [ClientRef, Table, FirstRow]) of
        {ok, Ref} ->
            {reply, {ok, Ref}, State#state{writers = Writers#{Table => Ref}}};
        Error ->
            {reply, Error, State}
    end;
handle_call(?REQ(?cmd_stream_write, [Table, Rows]), _From, State = #state{}) ->
    Res = write_with_stream(Table, Rows, State),
    {reply, Res, State};
handle_call(?REQ(?cmd_stream_close, [Table]), _From, State = #state{writers = Writers}) ->
    NewWriters =
        case maps:take(Table, Writers) of
            {WriterRef, Remaining} ->
                apply_nif(?cmd_stream_close, [WriterRef]),
                Remaining;
            error ->
                Writers
        end,
    {reply, ok, State#state{writers = NewWriters}};
handle_call(?REQ(Func, Args), _From, State = ?client_ref(ClientRef)) ->
    case apply_nif(Func, [ClientRef | Args]) of
        ?is_ok = Ok -> {reply, Ok, State};
        ?is_err = Err -> {reply, Err, State}
    end;
handle_call(_, _From, State = #state{client = undefined}) ->
    {reply, {error, not_connected}, State};
handle_call(Req, _From, State) ->
    logger:error("handle_call mismatch. State: ~p. Req: ~p~n", [State, Req]),
    {reply, {error, unexpected_call}, State}.

%% ===================================================================
handle_cast(stop, State = ?client_ref(undefined)) ->
    {stop, normal, State};
handle_cast(stop, State = ?client_ref(_ClientRef)) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

%% ===================================================================
handle_info(
    ?ASYNC_REQ(?cmd_insert, [Table, Rows], {CallbackFun, CallBackArgs}),
    State = ?client_ref(ClientRef)
) ->
    Res = apply_nif(?cmd_insert, [ClientRef, Table, Rows]),
    _ = erlang:apply(CallbackFun, CallBackArgs ++ [Res]),
    {noreply, State};
handle_info(
    ?ASYNC_REQ(?cmd_stream_write, [Table, Rows], {CallbackFun, CallBackArgs}),
    State = #state{}
) ->
    Res = write_with_stream(Table, Rows, State),
    _ = erlang:apply(CallbackFun, CallBackArgs ++ [Res]),
    {noreply, State};
handle_info(?ASYNC_REQ(Func, Args, {CallbackFun, CallBackArgs}), State = ?client_ref(ClientRef)) ->
    Res = apply_nif(Func, [ClientRef | Args]),
    _ = erlang:apply(CallbackFun, CallBackArgs ++ [Res]),
    {noreply, State};
handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, #state{writers = Writers}) ->
    maps:fold(
        fun(_Table, WriterRef, _) ->
            apply_nif(?cmd_stream_close, [WriterRef])
        end,
        ok,
        Writers
    ),
    ok;
terminate(_Reason, _Pid) ->
    ok.

%% ================================================================================
%% Helpers

apply_nif(Func, Args) ->
    try
        erlang:apply(?NIF_MODULE, Func, Args)
    catch
        error:Reason -> {error, {nif_error, Reason}};
        exit:Reason -> {error, {nif_exit, Reason}}
    end.

write_with_stream(Table, Rows, _State = #state{writers = Writers}) ->
    case Writers of
        #{Table := WriterRef} ->
            apply_nif(?cmd_stream_write, [WriterRef, Rows]);
        _ ->
            {error, no_writer}
    end.

unwrap_password(#{password := Password} = Opts) ->
    Opts#{password := do_unwrap_password(Password)};
unwrap_password(Opts) ->
    Opts.

do_unwrap_password(Password) when is_function(Password, 0) ->
    do_unwrap_password(Password());
do_unwrap_password(Password) ->
    Password.
