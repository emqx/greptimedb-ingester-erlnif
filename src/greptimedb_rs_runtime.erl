-module(greptimedb_rs_runtime).
-behaviour(gen_server).
-export([start_link/0]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    process_flag(trap_exit, true),
    Workers = application:get_env(greptimedb_rs, runtime_workers, 2),
    CodecWorkers = application:get_env(greptimedb_rs, codec_workers, 2),
    case greptimedb_rs_nif:runtime_start(Workers, CodecWorkers) of
        Runtime when is_reference(Runtime) -> {ok, Runtime};
        {error, Reason} -> {stop, Reason}
    end.

handle_call(_Request, _From, State) ->
    {reply, {error, unsupported}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, Runtime) ->
    greptimedb_rs_nif:runtime_stop(Runtime).
