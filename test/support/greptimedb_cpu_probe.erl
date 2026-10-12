-module(greptimedb_cpu_probe).
-export([main/0]).

main() ->
    logger:set_primary_config(level, warning),
    application:load(greptimedb_rs),
    application:set_env(greptimedb_rs, cpu_workers, 1),
    {ok, _} = application:ensure_all_started(greptimedb_rs),
    Mode = list_to_existing_atom(os:getenv("CPU_SCENARIO")),
    Timeout =
        case Mode of
            timeout -> 150;
            _ -> 10000
        end,
    {ok, Client} = greptimedb_rs:start_client(#{
        endpoints => [list_to_binary(os:getenv("CPU_ENDPOINT"))],
        dbname => <<"public">>,
        pool_name => cpu_probe,
        pool_size => 4,
        pool_type => round_robin,
        request_timeout => Timeout
    }),
    {ok, _} = greptimedb_rs:query(
        Client, <<"CREATE TABLE cpu_probe (ts TIMESTAMP TIME INDEX, reading INT64)">>
    ),
    {1, 2, 0, 0, 0} = greptimedb_rs_nif:cpu_counts(),
    {ok, 0} = greptimedb_rs:insert(Client, <<"cpu_probe">>, []),
    Row = #{timestamp => 1700000000000, fields => #{<<"reading">> => 1}},
    Rows = lists:duplicate(600000, Row),
    Parent = self(),
    Request =
        case Mode of
            query_cancel ->
                WarmRows = [Row#{timestamp := 1700000000000 + I} || I <- lists:seq(1, 60000)],
                {ok, 60000} = greptimedb_rs:insert(Client, <<"cpu_probe">>, WarmRows),
                fun() ->
                    greptimedb_rs:query(
                        Client,
                        <<"SELECT a.reading FROM cpu_probe a CROSS JOIN cpu_probe b LIMIT 600000">>
                    )
                end;
            _ ->
                fun() -> greptimedb_rs:insert(Client, <<"cpu_probe">>, Rows) end
        end,
    Callers = [spawn(fun() -> Parent ! {result, Request()} end) || _ <- lists:seq(1, 4)],
    Busy = await_busy(Mode, erlang:monotonic_time(millisecond) + 5000),
    case Mode of
        query_cancel ->
            lists:foreach(fun(Pid) -> exit(Pid, kill) end, Callers);
        cancel ->
            lists:foreach(fun(Pid) -> exit(Pid, kill) end, Callers);
        timeout ->
            lists:foreach(
                fun(_) ->
                    receive
                        {result, {error, timeout}} -> ok
                    after 5000 -> error(missing_timeout)
                    end
                end,
                Callers
            );
        library_stop ->
            ok = application:stop(greptimedb_rs)
    end,
    Reclaimed = await_idle(erlang:monotonic_time(millisecond) + 5000, []),
    case Mode of
        library_stop ->
            {0, 0} = greptimedb_rs_nif:resource_counts(),
            {0, 0, 0, 0, 0} = greptimedb_rs_nif:cpu_counts(),
            ok = greptimedb_rs:stop_client(Client),
            {ok, _} = application:ensure_all_started(greptimedb_rs),
            {ok, New} = greptimedb_rs:start_client(maps:get(conn_opts, Client)),
            recover(New, Row);
        _ ->
            recover(Client, Row)
    end,
    ok = application:stop(greptimedb_rs),
    {0, 0} = greptimedb_rs_nif:resource_counts(),
    {0, 0, 0, 0, 0} = greptimedb_rs_nif:cpu_counts(),
    Report = #{passed => true, mode => Mode, busy => Busy, reclamation => Reclaimed},
    ok = file:write_file(os:getenv("CPU_RESULT"), json:encode(convert(Report))),
    halt(0).

await_busy(Mode, Deadline) ->
    Counts = {Workers, Capacity, Active, Queued, Waiting} = greptimedb_rs_nif:cpu_counts(),
    true = Active =< Workers,
    true = Queued =< Capacity,
    case
        Active > 0 andalso
            (Mode =:= timeout orelse Mode =:= query_cancel orelse (Queued > 0 andalso Waiting > 0))
    of
        true ->
            Counts;
        false ->
            true = erlang:monotonic_time(millisecond) < Deadline,
            timer:sleep(1),
            await_busy(Mode, Deadline)
    end.

await_idle(Deadline, Samples) ->
    {_, Operations} = greptimedb_rs_nif:resource_counts(),
    Counts = {Workers, Capacity, Active, Queued, Waiting} = greptimedb_rs_nif:cpu_counts(),
    true = Active =< Workers,
    true = Queued =< Capacity,
    true = Operations >= Active,
    Next = [#{operations => Operations, cpu => Counts} | Samples],
    case {Operations, Active, Queued, Waiting} of
        {0, 0, 0, 0} ->
            lists:reverse(Next);
        _ ->
            true = erlang:monotonic_time(millisecond) < Deadline,
            timer:sleep(1),
            await_idle(Deadline, Next)
    end.

recover(Client, Row) ->
    {ok, 1} = greptimedb_rs:insert(Client, <<"cpu_probe_recovered">>, [Row]),
    {ok, [[1]]} = greptimedb_rs:query(Client, <<"SELECT reading FROM cpu_probe_recovered">>),
    ok = greptimedb_rs:stop_client(Client).

convert(M) when is_map(M) -> maps:map(fun(_, V) -> convert(V) end, M);
convert(L) when is_list(L) -> [convert(V) || V <- L];
convert(T) when is_tuple(T) -> convert(tuple_to_list(T));
convert(true) -> true;
convert(false) -> false;
convert(A) when is_atom(A) -> atom_to_binary(A);
convert(V) -> V.
