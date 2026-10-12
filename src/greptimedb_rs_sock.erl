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
    writers = #{} :: map(),
    generation,
    runtime_monitor,
    waiting = {[], []},
    pending = #{},
    current = undefined
}).

-define(client_ref(Ref), #state{client = Ref}).

-define(is_ok, {ok, _}).
-define(is_err, {error, _}).

-export([
    connect/1,
    sync_command/3,
    sync_command/4
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
    Timeout =
        case Command of
            ?cmd_stream_start -> 30000;
            ?cmd_stream_write -> 30000;
            ?cmd_stream_close -> 30000;
            _ -> 10000
        end,
    sync_command(Conn, Command, Args, erlang:monotonic_time(microsecond) + Timeout * 1000).

sync_command(Conn, Command, Args, Deadline) ->
    gen_server:call(Conn, {sync, Command, Args, Deadline}, infinity).

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
    process_flag(trap_exit, true),
    RuntimeMonitor = erlang:monitor(process, greptimedb_rs_runtime),
    Opts = unwrap_password(Opts0),
    case apply_nif(?cmd_connect, [Opts]) of
        {ok, ClientRef} ->
            log_connect_info(Opts0),
            {ok, #state{
                client = ClientRef,
                opts = Opts0,
                writers = #{},
                generation = make_ref(),
                runtime_monitor = RuntimeMonitor
            }};
        {error, Reason} ->
            {stop, Reason}
    end.

%% ================================================================================
handle_call({sync, Cmd, Args, Deadline}, From, State) ->
    Monitor = erlang:monitor(process, element(1, From)),
    {noreply, enqueue(Cmd, Args, Deadline, {sync, From}, Monitor, State)};
handle_call(?REQ(Cmd, Args), From, State) ->
    handle_call({sync, Cmd, Args, local_deadline(Cmd, State)}, From, State).

handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({async, Cmd, Args, Callback, Deadline}, State) ->
    {noreply, enqueue(Cmd, Args, Deadline, {async, Callback}, undefined, State)};
handle_info(?ASYNC_REQ(Cmd, Args, Callback), State) ->
    handle_info({async, Cmd, Args, Callback, local_deadline(Cmd, State)}, State);
handle_info({greptimedb_nif, {Gen, Ref}, Result}, State = #state{generation = Gen, current = Ref}) ->
    Req = maps:get(Ref, State#state.pending),
    Expired =
        maps:get(replied, Req) orelse erlang:monotonic_time(microsecond) >= maps:get(deadline, Req),
    Next = finish_command(Req, Result, Expired, State),
    case maps:get(replied, Req) of
        true ->
            ok;
        false ->
            deliver(
                Req,
                case Expired of
                    true -> {error, timeout};
                    false -> public_result(Req, Result)
                end
            )
    end,
    {noreply, release(Ref, Next)};
handle_info({greptimedb_nif, _, {ok, Writer}}, State) when is_reference(Writer) ->
    _ = apply_nif(discard_writer, [Writer]),
    {noreply, State};
handle_info({deadline, Ref}, State) ->
    {noreply, expire(Ref, {error, timeout}, State)};
handle_info({'DOWN', Monitor, process, _Pid, _Reason}, State = #state{runtime_monitor = Monitor}) ->
    {stop, normal, State};
handle_info({'DOWN', Monitor, process, _Pid, _Reason}, State) ->
    Refs = [
        Ref
     || {Ref, Req} <- maps:to_list(State#state.pending), maps:get(monitor, Req) =:= Monitor
    ],
    {noreply, lists:foldl(fun(Ref, S) -> abandon(Ref, S) end, State, Refs)};
handle_info({reclaim, Ref}, State = #state{current = Ref}) ->
    Req = maps:get(Ref, State#state.pending),
    case ?NIF_MODULE:operation_active(maps:get(operation, Req)) of
        false ->
            Next = finish_command(Req, {error, cancelled}, true, State),
            {noreply, release(Ref, Next)};
        true ->
            erlang:send_after(10, self(), {reclaim, Ref}),
            {noreply, State}
    end;
handle_info(continue, State) ->
    {noreply, dispatch(State, 100)};
handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, State = #state{client = ClientRef, pending = Pending}) ->
    case ClientRef of
        undefined -> ok;
        _ -> apply_nif(disconnect, [ClientRef])
    end,
    maps:foreach(
        fun(_, Req) ->
            clear_request(Req),
            case maps:get(replied, Req) of
                false -> catch deliver(Req, {error, closed});
                true -> ok
            end
        end,
        Pending
    ),
    maps:foreach(fun(_, Writer) -> apply_nif(discard_writer, [Writer]) end, State#state.writers),
    ok.

enqueue(Cmd, Args, Deadline, Recipient, Monitor, State) ->
    Ref = make_ref(),
    Remaining = max(0, Deadline - erlang:monotonic_time(microsecond)),
    Timer = erlang:send_after((Remaining + 999) div 1000, self(), {deadline, Ref}),
    Req = #{
        ref => Ref,
        cmd => Cmd,
        args => Args,
        deadline => Deadline,
        recipient => Recipient,
        monitor => Monitor,
        timer => Timer,
        replied => false
    },
    Pending = (State#state.pending)#{Ref => Req},
    dispatch(State#state{pending = Pending, waiting = queue:in(Ref, State#state.waiting)}, 100).

dispatch(State = #state{current = Ref}, _) when Ref =/= undefined -> State;
dispatch(State, 0) ->
    self() ! continue,
    State;
dispatch(State, Budget) ->
    case queue:out(State#state.waiting) of
        {empty, _} ->
            State;
        {{value, Ref}, Waiting} ->
            Next = State#state{waiting = Waiting},
            case maps:find(Ref, State#state.pending) of
                error ->
                    dispatch(Next, Budget - 1);
                {ok, Req} ->
                    Remaining = maps:get(deadline, Req) - erlang:monotonic_time(microsecond),
                    case Remaining > 0 of
                        false -> dispatch(expire(Ref, {error, timeout}, Next), Budget - 1);
                        true -> start_request(Ref, Req, Next, Budget)
                    end
            end
    end.

start_request(Ref, Req, State, Budget) ->
    Cmd = maps:get(cmd, Req),
    Result =
        case nif_args(Cmd, maps:get(args, Req), State) of
            {ok, Args} ->
                apply_nif(submit, [
                    State#state.client,
                    Cmd,
                    Args,
                    {State#state.generation, Ref},
                    maps:get(deadline, Req)
                ]);
            Error ->
                Error
        end,
    case Result of
        {ok, Operation} ->
            Pending = (State#state.pending)#{Ref := Req#{operation => Operation}},
            State#state{current = Ref, pending = Pending};
        ErrorResult ->
            deliver(Req, public_result(Req, ErrorResult)),
            clear_request(Req),
            dispatch(State#state{pending = maps:remove(Ref, State#state.pending)}, Budget - 1)
    end.

nif_args(?cmd_stream_write, [Table, Rows], #state{writers = Writers}) ->
    case maps:find(Table, Writers) of
        {ok, Writer} -> {ok, [Writer, Rows]};
        error -> {error, no_writer}
    end;
nif_args(?cmd_stream_close, [Table], #state{writers = Writers}) ->
    case maps:find(Table, Writers) of
        {ok, Writer} -> {ok, [Writer]};
        error -> {error, no_writer}
    end;
nif_args(_, _, #state{client = undefined}) ->
    {error, not_connected};
nif_args(_, Args, _) ->
    {ok, Args}.

expire(Ref, Result, State) ->
    case maps:find(Ref, State#state.pending) of
        error ->
            State;
        {ok, Req} ->
            case maps:get(replied, Req) of
                false -> deliver(Req, Result);
                true -> ok
            end,
            cancel_request(Ref, Req, State)
    end.

abandon(Ref, State) ->
    Req = maps:get(Ref, State#state.pending),
    cancel_request(Ref, Req, State).

cancel_request(Ref, Req, State = #state{current = Ref}) ->
    clear_request(Req),
    _ = apply_nif(cancel, [maps:get(operation, Req)]),
    erlang:send_after(10, self(), {reclaim, Ref}),
    State#state{pending = (State#state.pending)#{Ref := Req#{replied := true}}};
cancel_request(Ref, Req, State) ->
    clear_request(Req),
    State#state{pending = maps:remove(Ref, State#state.pending)}.

release(Ref, State) ->
    Req = maps:get(Ref, State#state.pending),
    clear_request(Req),
    dispatch(
        State#state{current = undefined, pending = maps:remove(Ref, State#state.pending)}, 100
    ).

finish_command(#{cmd := ?cmd_stream_start, args := [Table, _]}, {ok, Writer}, false, State) ->
    State#state{writers = (State#state.writers)#{Table => Writer}};
finish_command(#{cmd := ?cmd_stream_start}, {ok, Writer}, true, State) ->
    apply_nif(discard_writer, [Writer]),
    State;
finish_command(#{cmd := ?cmd_stream_close, args := [Table]}, _, _, State) ->
    discard(Table, State);
finish_command(#{cmd := ?cmd_stream_write, args := [Table, _]}, Result, Expired, State) ->
    case Result =/= ok orelse Expired of
        true -> discard(Table, State);
        false -> State
    end;
finish_command(_, _, _, State) ->
    State.

discard(Table, State) ->
    case maps:take(Table, State#state.writers) of
        {Writer, Writers} ->
            apply_nif(discard_writer, [Writer]),
            State#state{writers = Writers};
        error ->
            State
    end.

public_result(#{cmd := ?cmd_stream_close}, _) -> ok;
public_result(_, Result) -> Result.

clear_request(Req) ->
    erlang:cancel_timer(maps:get(timer, Req)),
    case maps:get(monitor, Req) of
        undefined -> ok;
        Monitor -> erlang:demonitor(Monitor, [flush])
    end.

deliver(#{recipient := {sync, From}}, Result) -> gen_server:reply(From, Result);
deliver(#{recipient := {async, {Fun, Args}}}, Result) -> erlang:apply(Fun, Args ++ [Result]).

local_deadline(Cmd, #state{opts = Opts}) ->
    Key =
        case Cmd of
            ?cmd_stream_start -> stream_timeout;
            ?cmd_stream_write -> stream_timeout;
            ?cmd_stream_close -> stream_timeout;
            _ -> request_timeout
        end,
    Default =
        case Key of
            stream_timeout -> 30000;
            _ -> 10000
        end,
    erlang:monotonic_time(microsecond) + maps:get(Key, Opts, Default) * 1000.

apply_nif(Func, Args) ->
    try
        erlang:apply(?NIF_MODULE, Func, Args)
    catch
        error:Reason -> {error, {nif_error, Reason}};
        exit:Reason -> {error, {nif_exit, Reason}}
    end.

unwrap_password(#{password := Password} = Opts) ->
    Opts#{password := do_unwrap_password(Password)};
unwrap_password(Opts) ->
    Opts.

do_unwrap_password(Password) when is_function(Password, 0) ->
    do_unwrap_password(Password());
do_unwrap_password(Password) ->
    Password.

log_connect_info(#{endpoints := Endpoints} = Opts) ->
    TlsEnabled = maps:get(tls, Opts, false),
    CipherInfo =
        case maps:find(cipher_suites, Opts) of
            {ok, Ciphers} when Ciphers =/= [] ->
                [" ciphers=", string:join([binary_to_list(C) || C <- Ciphers], ":")];
            _ ->
                ""
        end,
    FipsInfo =
        case TlsEnabled of
            true ->
                case greptimedb_rs_nif:cached_fips_status() of
                    true -> " fips=enabled";
                    false -> " fips=disabled"
                end;
            _ ->
                ""
        end,
    EndpointStr = string:join([binary_to_list(E) || E <- Endpoints], ","),
    logger:notice(
        "greptimedb_rs_sock connecting to [~s] tls=~s~s~s",
        [EndpointStr, atom_to_list(TlsEnabled), CipherInfo, FipsInfo]
    );
log_connect_info(_Opts) ->
    ok.
