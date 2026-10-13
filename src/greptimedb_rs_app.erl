-module(greptimedb_rs_app).
-behaviour(application).
-export([
    start/2,
    stop/1
]).

start(_Type, _Args) ->
    greptimedb_rs_sup:start_link().

stop(_State) ->
    ok.
