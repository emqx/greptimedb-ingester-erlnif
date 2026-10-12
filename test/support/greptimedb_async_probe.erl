-module(greptimedb_async_probe).
-export([main/0]).

main() ->
    logger:set_primary_config(level, warning),
    {ok, _} = application:ensure_all_started(greptimedb_rs),
    {ok, Control} = gen_tcp:connect(
        "127.0.0.1",
        list_to_integer(os:getenv("PROBE_CONTROL_PORT")),
        [binary, {packet, line}, {active, false}],
        5000
    ),
    Scenario = list_to_existing_atom(os:getenv("PROBE_SCENARIO")),
    Timeout =
        case Scenario of
            queued -> 1000;
            _ -> 10000
        end,
    Opts = #{
        endpoints => [list_to_binary(os:getenv("PROBE_ENDPOINT"))],
        dbname => <<"public">>,
        pool_name => async_probe,
        pool_size => 1,
        auto_reconnect => 1,
        request_timeout => Timeout
    },
    {ok, Client} = greptimedb_rs:start_client(Opts),
    {ok, Healthy} = greptimedb_rs:start_client(Opts#{
        pool_name := async_probe_healthy,
        endpoints := [list_to_binary(os:getenv("PROBE_BACKEND"))]
    }),
    {ok, [[1]]} = greptimedb_rs:query(Client, <<"SELECT 1">>),
    {ok, [[1]]} = greptimedb_rs:query(Healthy, <<"SELECT 1">>),
    {Runtimes, _} = greptimedb_rs_nif:resource_counts(),
    event(Control, #{event => ready, runtimes => Runtimes}),
    {ok, <<"go\n">>} = gen_tcp:recv(Control, 0, 10000),
    Parent = self(),
    Caller =
        case Scenario of
            caller_cancel ->
                spawn(fun() -> greptimedb_rs:query(Client, <<"SELECT 1">>) end);
            _ ->
                submit(Client, first, Parent),
                undefined
        end,
    loop(Control, Scenario, Client, Healthy, Caller, []).

submit(Client, Id, Parent) ->
    Started = erlang:monotonic_time(microsecond),
    {ok, _} = greptimedb_rs:query_async(
        Client,
        <<"SELECT 1">>,
        {
            fun(Result) ->
                Parent ! {result, Id, Result, erlang:monotonic_time(microsecond) - Started}
            end,
            []
        }
    ).

loop(Control, Scenario, Client, Healthy, Caller, Results) ->
    Next = drain(Results),
    case gen_tcp:recv(Control, 0, 30) of
        {error, timeout} ->
            loop(Control, Scenario, Client, Healthy, Caller, Next);
        {ok, <<"action\n">>} ->
            case Scenario of
                queued ->
                    Opts = maps:get(conn_opts, Client),
                    submit(Client#{conn_opts := Opts#{request_timeout := 200}}, queued, self());
                caller_cancel ->
                    exit(Caller, kill);
                pool_stop ->
                    ok = greptimedb_rs:stop_client(Client);
                runtime_owner_exit ->
                    exit(whereis(greptimedb_rs_runtime), kill);
                library_stop ->
                    ok = application:stop(greptimedb_rs);
                socket_exit ->
                    [{_, Worker}] = ecpool:workers(async_probe),
                    {ok, Conn} = ecpool_worker:client(Worker),
                    exit(Conn, kill)
            end,
            event(Control, #{event => action_done}),
            loop(Control, Scenario, Client, Healthy, Caller, Next);
        {ok, <<"snapshot\n">>} ->
            {Runtimes, Operations} = greptimedb_rs_nif:resource_counts(),
            Current = drain(Next),
            event(Control, #{
                event => snapshot,
                results => Current,
                runtimes => Runtimes,
                operations => Operations
            }),
            loop(Control, Scenario, Client, Healthy, Caller, Current);
        {ok, <<"recover\n">>} when Scenario =:= library_stop ->
            ok = greptimedb_rs:stop_client(Client),
            ok = greptimedb_rs:stop_client(Healthy),
            {ok, _} = application:ensure_all_started(greptimedb_rs),
            {ok, NewClient} = greptimedb_rs:start_client(maps:get(conn_opts, Client)),
            {ok, NewHealthy} = greptimedb_rs:start_client(maps:get(conn_opts, Healthy)),
            {ok, [[1]]} = greptimedb_rs:query(NewClient, <<"SELECT 1">>),
            {ok, [[1]]} = greptimedb_rs:query(NewHealthy, <<"SELECT 1">>),
            event(Control, #{event => recovered}),
            loop(Control, Scenario, NewClient, NewHealthy, Caller, Next);
        {ok, <<"recover\n">>} ->
            {ok, [[1]]} = greptimedb_rs:query(Healthy, <<"SELECT 1">>),
            Recovered =
                case Scenario of
                    pool_stop ->
                        {ok, New} = greptimedb_rs:start_client(maps:get(conn_opts, Client)),
                        New;
                    _ ->
                        Client
                end,
            {ok, 1} = greptimedb_rs:insert(
                Recovered,
                <<"async_probe_recovered">>,
                [#{timestamp => 1700000000000, fields => #{<<"reading">> => 7}}]
            ),
            {ok, [[7]]} = greptimedb_rs:query(
                Recovered, <<"SELECT reading FROM async_probe_recovered">>
            ),
            event(Control, #{event => recovered}),
            loop(Control, Scenario, Recovered, Healthy, Caller, Next);
        {ok, <<"cycles\n">>} ->
            ok = greptimedb_rs:stop_client(Client),
            ok = greptimedb_rs:stop_client(Healthy),
            await_counts({1, 0}, erlang:monotonic_time(millisecond) + 5000),
            lists:foreach(
                fun(I) ->
                    Opts = maps:get(conn_opts, Healthy),
                    {ok, C} = greptimedb_rs:start_client(Opts#{pool_name := {cycle, I}}),
                    {ok, [[1]]} = greptimedb_rs:query(C, <<"SELECT 1">>),
                    ok = greptimedb_rs:stop_client(C),
                    await_counts({1, 0}, erlang:monotonic_time(millisecond) + 5000)
                end,
                lists:seq(1, 20)
            ),
            ok = application:stop(greptimedb_rs),
            await_counts({0, 0}, erlang:monotonic_time(millisecond) + 5000),
            {ok, _} = application:ensure_all_started(greptimedb_rs),
            {ok, Restarted} = greptimedb_rs:start_client(maps:get(conn_opts, Healthy)),
            {ok, [[1]]} = greptimedb_rs:query(Restarted, <<"SELECT 1">>),
            ok = greptimedb_rs:stop_client(Restarted),
            await_counts({1, 0}, erlang:monotonic_time(millisecond) + 5000),
            event(Control, #{event => cycles_passed, runtime_restarted => true}),
            loop(Control, Scenario, Client, Healthy, Caller, Next);
        {ok, <<"stop\n">>} ->
            event(Control, #{event => stopped}),
            halt(0)
    end.

await_counts(Expected, Deadline) ->
    case greptimedb_rs_nif:resource_counts() of
        Expected ->
            ok;
        Counts ->
            case erlang:monotonic_time(millisecond) < Deadline of
                true ->
                    timer:sleep(10),
                    await_counts(Expected, Deadline);
                false ->
                    error({resources_not_reclaimed, Counts})
            end
    end.

drain(Results) ->
    receive
        {result, Id, Result, Elapsed} ->
            drain([#{id => Id, result => Result, elapsed_us => Elapsed} | Results])
    after 0 -> Results
    end.

event(Control, Value) -> gen_tcp:send(Control, [json:encode(convert(Value)), "\n"]).
convert(M) when is_map(M) -> maps:map(fun(_, V) -> convert(V) end, M);
convert(L) when is_list(L) -> [convert(V) || V <- L];
convert(T) when is_tuple(T) -> convert(tuple_to_list(T));
convert(A) when is_atom(A) -> atom_to_binary(A);
convert(V) -> V.
