-module(greptimedb_rs_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).

start_link() -> supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Child = #{
        id => greptimedb_rs_runtime,
        start => {greptimedb_rs_runtime, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => worker
    },
    {ok, {#{strategy => one_for_one, intensity => 3, period => 10}, [Child]}}.
