-module(greptimedb_benchmark).
-export([run/0, ports/0]).

-elvis([
    {elvis_style, no_debug_call, #{
        ignore => [
            {greptimedb_benchmark, run, 0},
            {greptimedb_benchmark, scenario, 2},
            {greptimedb_benchmark, ports, 0}
        ]
    }}
]).
run() ->
    logger:set_primary_config(level, critical),
    try
        put(initial_threads, element(2, memory())),
        put(clock_ticks, integer_env("BENCH_CLOCK_TICKS", 100)),
        4 = erlang:system_info(schedulers_online),
        2 = erlang:system_info(dirty_cpu_schedulers_online),
        4 = erlang:system_info(dirty_io_schedulers),
        ok = application:load(greptimedb_rs),
        ok = application:set_env(
            greptimedb_rs, runtime_workers, integer_env("BENCH_RUNTIME_WORKERS", 2)
        ),
        ok = application:set_env(
            greptimedb_rs, codec_workers, integer_env("BENCH_CODEC_WORKERS", 2)
        ),
        {ok, _} = application:ensure_all_started(greptimedb_rs),
        {ok, Client} = greptimedb_rs:start_client(#{
            endpoints => [list_to_binary(os:getenv("BENCH_ENDPOINT"))],
            dbname => <<"public">>,
            pool_name => bench,
            pool_type => round_robin,
            pool_size => integer_env("BENCH_POOL_SIZE", 4),
            request_timeout => 60000,
            stream_timeout => 60000
        }),
        {ok, [[1]]} = greptimedb_rs:query(Client, <<"SELECT 1">>),
        timer:sleep(50),
        erlang:garbage_collect(),
        {PoolRss, PoolThreads, PoolNative} = snapshot(),
        put(pool_idle, {PoolRss, PoolThreads, PoolNative}),
        lists:foreach(fun(S) -> scenario(Client, S) end, cases()),
        ok = greptimedb_rs:stop_client(Client),
        ok = application:stop(greptimedb_rs),
        halt(0)
    catch
        Class:Reason:Stack ->
            io:format("BENCH_FAILED ~p:~p ~p~n", [Class, Reason, Stack]),
            halt(1)
    end.
cases() ->
    All = [
        {query_small_serial, query, 1, 1, 2000},
        {query_small_concurrent, query, 1, 16, 8000},
        {insert_single, insert, 1, 16, 4800},
        {insert100_serial, insert, 100, 1, 1200},
        {insert100_concurrent, insert, 100, 16, 4000},
        {insert5000, insert, 5000, 4, 480},
        {insert_async100, insert_async, 100, 16, 4000},
        {stream100, stream, 100, 16, 12000},
        {stream5000, stream, 5000, 4, 480},
        {query20000, query, 20000, 4, 240}
    ],
    case os:getenv("BENCH_CASE") of
        false -> All;
        Selected -> [C || C = {Name, _, _, _, _} <- All, atom_to_list(Name) =:= Selected]
    end.
scenario(Client, {Name, Mode, Batch, Concurrency, Ops}) ->
    Table = atom_to_binary(Name),
    {ok, _} = greptimedb_rs:query(
        Client,
        <<"CREATE TABLE ", Table/binary,
            " (ts TIMESTAMP TIME INDEX,sensor_id STRING,temperature DOUBLE,"
            "sensor_status INT64,PRIMARY KEY(sensor_id))">>
    ),
    QueryRows =
        case Mode of
            query -> Batch;
            _ -> 100
        end,
    Seed = rows(1700000000000, QueryRows),
    {ok, QueryRows} = greptimedb_rs:insert(Client, Table, Seed),
    SQL =
        case Batch of
            1 -> <<"SELECT 1">>;
            _ -> <<"SELECT temperature,sensor_status,sensor_id,ts FROM ", Table/binary>>
        end,
    Target =
        case Mode of
            stream ->
                {ok, St} = greptimedb_rs:stream_start(Client, Table, hd(Seed)),
                St;
            _ ->
                Client
        end,
    lists:foreach(fun(_) -> call(Mode, Target, Table, Seed, SQL, QueryRows) end, lists:seq(1, 8)),
    %% Keep input construction outside the timed interval for both versions.
    Payload = [
        case Mode of
            query -> [];
            _ -> rows(1800000000000 + I * Batch, Batch)
        end
     || I <- lists:seq(1, Ops)
    ],
    Parent = self(),
    Key = make_ref(),
    Assignments = assign(Payload, Concurrency),
    Workers = [
        spawn_monitor(fun() ->
            erlang:garbage_collect(),
            Parent ! {Key, ready, self()},
            receive
                {Key, go} -> ok
            after 60000 -> error(start_timeout)
            end,
            Lats = [
                timed_call(Mode, Target, Table, R, SQL, Batch)
             || R <- Job
            ],
            Parent ! {Key, done, self(), Lats}
        end)
     || Job <- Assignments
    ],
    ready(Key, Concurrency),
    erlang:garbage_collect(),
    {IdleRss, IdleThreads, Native} = snapshot(),
    SampleKey = make_ref(),
    Sampler = spawn(fun() -> sample(Parent, SampleKey, IdleRss, IdleThreads) end),
    Cpu0 = cpu(os:getpid()),
    DbCpu0 = cpu(os:getenv("BENCH_SERVER_PID")),
    Start = erlang:monotonic_time(microsecond),
    lists:foreach(fun({Pid, _}) -> Pid ! {Key, go} end, Workers),
    Latencies = done(Key, Workers, []),
    case Mode of
        stream -> ok = greptimedb_rs:stream_close(Target);
        _ -> ok
    end,
    %% Include stream finish so queued writes are acknowledged before reporting TPS.
    Elapsed = erlang:monotonic_time(microsecond) - Start,
    CpuMs = (cpu(os:getpid()) - Cpu0) * 1000 div get(clock_ticks),
    DbCpuMs = (cpu(os:getenv("BENCH_SERVER_PID")) - DbCpu0) * 1000 div get(clock_ticks),
    Sampler ! stop,
    {PeakRss, PeakThreads} =
        receive
            {SampleKey, Metrics} -> Metrics
        after 1000 -> error(sampler_timeout)
        end,
    case Mode of
        query ->
            ok;
        _ ->
            Expected = Ops * Batch,
            {ok, [[Expected]]} = greptimedb_rs:query(
                Client, <<"SELECT count(*) FROM ", Table/binary, " WHERE ts>=1800000000000">>
            )
    end,
    Sorted = lists:sort(Latencies),
    {PoolRss, PoolThreads, PoolNative} = get(pool_idle),
    io:format(
        "RESULT,~s,~s,~B,~B,~B,~B,~.3f,~.3f,"
        "~B,~B,~B,~B,~B,~B,~B,~B,~B,~B,~B,~B,~B~n",
        [
            Name,
            Mode,
            Batch,
            Concurrency,
            Ops,
            Elapsed,
            Ops * 1000000 / Elapsed,
            Ops * Batch * 1000000 / Elapsed,
            percentile(Sorted, 50),
            percentile(Sorted, 95),
            percentile(Sorted, 99),
            CpuMs,
            DbCpuMs,
            IdleRss,
            PeakRss,
            IdleThreads,
            PeakThreads,
            Native,
            PoolRss,
            PoolThreads,
            PoolNative
        ]
    ),
    {ok, _} = greptimedb_rs:query(Client, <<"DROP TABLE ", Table/binary>>),
    ok.
rows(Base, N) ->
    [
        #{
            timestamp => Base + I,
            tags => #{<<"sensor_id">> => integer_to_binary(I rem 100)},
            fields => #{<<"temperature">> => 18.5, <<"sensor_status">> => 1}
        }
     || I <- lists:seq(1, N)
    ].
timed_call(Mode, Target, Table, Rows, SQL, Expected) ->
    Start = erlang:monotonic_time(microsecond),
    call(Mode, Target, Table, Rows, SQL, Expected),
    erlang:monotonic_time(microsecond) - Start.

call(query, C, _T, _Rows, SQL, Expected) ->
    {ok, R} = greptimedb_rs:query(C, SQL),
    Expected = length(R),
    ok;
call(insert, C, T, Rows, _SQL, Expected) ->
    {ok, Expected} = greptimedb_rs:insert(C, T, Rows),
    ok;
call(insert_async, C, T, Rows, _SQL, Expected) ->
    Self = self(),
    Ref = make_ref(),
    {ok, _} = greptimedb_rs:insert_async(C, T, Rows, {fun(R) -> Self ! {Ref, R} end, []}),
    receive
        {Ref, {ok, Expected}} -> ok;
        {Ref, Other} -> error({async_failed, Other})
    after 60000 -> error(callback_timeout)
    end;
call(stream, St, _T, Rows, _SQL, _Expected) ->
    ok = greptimedb_rs:stream_write(St, Rows).
assign(Payload, C) ->
    [
        [R || {R, I} <- lists:zip(Payload, lists:seq(0, length(Payload) - 1)), I rem C =:= J]
     || J <- lists:seq(0, C - 1)
    ].
ready(_, 0) ->
    ok;
ready(K, N) ->
    receive
        {K, ready, _} -> ready(K, N - 1);
        {'DOWN', _, process, _, R} -> error({worker_failed, R})
    after 60000 -> error(ready_timeout)
    end.
done(_, [], Acc) ->
    lists:append(Acc);
done(K, Workers, Acc) ->
    receive
        {K, done, Pid, L} ->
            {value, {Pid, Monitor}, Rest} = lists:keytake(Pid, 1, Workers),
            erlang:demonitor(Monitor, [flush]),
            done(K, Rest, [L | Acc]);
        {'DOWN', _, process, _, R} ->
            error({worker_failed, R})
    after 60000 -> error(worker_timeout)
    end.
percentile(S, P) -> lists:nth(max(1, (length(S) * P + 99) div 100), S).
cpu(Pid) ->
    {ok, B} = file:read_file("/proc/" ++ Pid ++ "/stat"),
    Ts = string:tokens(binary_to_list(B), " "),
    list_to_integer(lists:nth(14, Ts)) + list_to_integer(lists:nth(15, Ts)).
memory() ->
    {ok, B} = file:read_file("/proc/self/status"),
    {match, [R]} = re:run(B, <<"VmRSS:\\s+(\\d+)">>, [{capture, [1], binary}]),
    {match, [T]} = re:run(B, <<"Threads:\\s+(\\d+)">>, [{capture, [1], binary}]),
    {binary_to_integer(R), binary_to_integer(T)}.
snapshot() ->
    {R, T} = memory(),
    {R, T, T - get(initial_threads)}.
sample(P, K, R, T) ->
    receive
        stop -> P ! {K, {R, T}}
    after 20 ->
        {R1, T1} = memory(),
        sample(P, K, max(R, R1), max(T, T1))
    end.

integer_env(Name, Default) ->
    case os:getenv(Name) of
        false -> Default;
        Value -> list_to_integer(Value)
    end.

ports() ->
    {ok, Rpc} = gen_tcp:listen(0, [{ip, {127, 0, 0, 1}}]),
    {ok, Http} = gen_tcp:listen(0, [{ip, {127, 0, 0, 1}}]),
    {ok, {_, RpcPort}} = inet:sockname(Rpc),
    {ok, {_, HttpPort}} = inet:sockname(Http),
    io:format("~B ~B~n", [RpcPort, HttpPort]),
    gen_tcp:close(Rpc),
    gen_tcp:close(Http),
    halt(0).
