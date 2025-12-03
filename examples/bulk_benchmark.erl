-module(bulk_benchmark).
-compile(export_all).
-compile(nowarn_export_all).

-define(CNT, 1000).
-define(BATCH_COUNT, ?CNT).
-define(ROWS_PER_BATCH, ?CNT).
-define(DB_NAME, <<"public">>).

main() ->
    application:ensure_all_started(gproc),
    application:ensure_all_started(ecpool),
    application:ensure_all_started(greptimedb_rs),

    io:format("\n  === GreptimeDB Ingester Benchmark (Erlang With Rust NIF) ==="),
    io:format("\n  === Start Running benchmarks ==="),
    io:format("\n  === BatchCount: ~p , RowsPerBatch: ~p", [?BATCH_COUNT, ?ROWS_PER_BATCH]),
    io:format(lists:duplicate(3, "\n")),

    Results = [
        run_benchmark(insert, 1),
        run_benchmark(insert, 16),
        run_benchmark(insert_async, 1),
        run_benchmark(insert_async, 16),
        run_benchmark(stream_write, 1),
        run_benchmark(stream_write, 16),
        run_benchmark(stream_write_async, 1),
        run_benchmark(stream_write_async, 16)
    ],

    print_summary(Results),
    ok.

run_benchmark(Type, PoolSize) ->
    Name = io_lib:format("~p (pool=~p)", [Type, PoolSize]),
    TableName = iolist_to_binary(io_lib:format("bench_~p_~p", [Type, PoolSize])),
    io:format("Running ~s...\n", [Name]),

    {ok, Client} = connect(PoolSize),
    create_table(Client, TableName),

    {_Res, Duration} = time_fun(fun() -> do_benchmark(Type, Client, TableName) end),
    RowCount = ?BATCH_COUNT * ?ROWS_PER_BATCH,
    Throughput = RowCount / Duration,

    io:format("Done. ~p rows in ~.3fs (~.2f rows/s)\n\n", [RowCount, Duration, Throughput]),

    greptimedb_rs:stop_client(Client),
    {Name, Duration, Throughput}.

%% 1. Insert (Sync) - loops over its batch range
do_benchmark(insert, Client, Table) ->
    Parent = self(),
    Ref = make_ref(),

    lists:foreach(
        fun(BatchId) ->
            Rows = generate_rows(BatchId, ?ROWS_PER_BATCH),
            {ok, _} = greptimedb_rs:insert(Client, Table, Rows),
            progress_bar(BatchId, ?BATCH_COUNT),
            Parent ! {Ref, done}
        end,
        lists:seq(1, ?BATCH_COUNT)
    ),
    ensure_count(Ref, ?BATCH_COUNT);

%% 2. Insert Async - Single process firing async requests
do_benchmark(insert_async, Client, Table) ->
    Parent = self(),
    Ref = make_ref(),

    lists:foreach(
        fun(BatchId) ->
            Rows = generate_rows(BatchId, ?ROWS_PER_BATCH),
            Callback = {fun(_Res) -> Parent ! {Ref, done} end, []},
            ok = greptimedb_rs:insert_async(Client, Table, Rows, Callback),
            progress_bar(BatchId, ?BATCH_COUNT)
        end,
        lists:seq(1, ?BATCH_COUNT)
    ),
    ensure_count(Ref, ?BATCH_COUNT);

%% 3. Stream Write (Sync) - loops over its batch range
do_benchmark(stream_write, Client, Table) ->
    Parent = self(),
    Ref = make_ref(),

    {ok, Stream} = greptimedb_rs:stream_start(Client, Table, hd(generate_rows(0, 1))),
    lists:foreach(
        fun(BatchId) ->
            Rows = generate_rows(BatchId, ?ROWS_PER_BATCH),
            ok = greptimedb_rs:stream_write(Stream, Rows),
            progress_bar(BatchId, ?BATCH_COUNT),
            Parent ! {Ref, done}
        end,
        lists:seq(1, ?BATCH_COUNT)
    ),
    ensure_count(Ref, ?BATCH_COUNT),
    greptimedb_rs:stream_close(Stream);

%% 4. Stream Write Async - Single client firing async stream writes
do_benchmark(stream_write_async, Client, Table) ->
    Parent = self(),
    Ref = make_ref(),

    {ok, Stream} = greptimedb_rs:stream_start(Client, Table, hd(generate_rows(0, 1))),
    lists:foreach(
        fun(BatchId) ->
            Rows = generate_rows(BatchId, ?ROWS_PER_BATCH),
            Callback = {fun(_Res) -> Parent ! {Ref, done} end, []},
            ok = greptimedb_rs:stream_write_async(Stream, Rows, Callback),
            progress_bar(BatchId, ?BATCH_COUNT)
        end,
        lists:seq(1, ?BATCH_COUNT)
    ),
    ensure_count(Ref, ?BATCH_COUNT),
    greptimedb_rs:stream_close(Stream).

%% Helpers

connect(PoolSize) ->
    Endpoint =
        case os:getenv("GREPTIMEDB_TCP_ADDR") of
            false -> <<"127.0.0.1:4001">>;
            Addr -> iolist_to_binary([Addr, ":4001"])
        end,
    Opts = #{
        endpoints => [Endpoint],
        dbname => ?DB_NAME,
        pool_size => PoolSize,
        pool_type => random
    },
    greptimedb_rs:start_client(Opts).

create_table(Client, Table) ->
    DropSql = iolist_to_binary(io_lib:format("DROP TABLE IF EXISTS ~s", [Table])),
    {ok, _} = greptimedb_rs:query(Client, DropSql),
    Sql = iolist_to_binary(
        io_lib:format(
            "CREATE TABLE IF NOT EXISTS ~s (" ++
                "ts TIMESTAMP TIME INDEX, " ++
                "sensor_id STRING, " ++
                "temperature DOUBLE, " ++
                "sensor_status INT64, " ++
                "PRIMARY KEY(sensor_id)" ++
                ") ENGINE=mito",
            [Table]
        )
    ),
    case greptimedb_rs:query(Client, Sql) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            io:format("Failed to create table: ~p\n", [Reason]),
            exit(failed_create_table)
    end.

generate_rows(BatchId, Count) ->
    BaseTs = os:system_time(millisecond),
    [
        begin
            GlobalIdx = BatchId * Count + I,
            #{
                fields => #{
                    <<"temperature">> => 18.0 + (GlobalIdx * 0.03),
                    <<"sensor_status">> =>
                        case GlobalIdx rem 100 of
                            0 -> 0;
                            _ -> 1
                        end
                },
                tags => #{
                    <<"sensor_id">> => iolist_to_binary(
                        io_lib:format("sensor_~6..0b", [GlobalIdx rem 1000])
                    )
                },
                timestamp => BaseTs + (GlobalIdx * 50)
            }
        end
     || I <- lists:seq(0, Count - 1)
    ].

progress_bar(Done, Total) ->
    Percent = trunc((Done / Total) * 100),
    Filled = trunc((Done / Total) * 100),
    Empty = 100 - Filled,
    Bar = lists:duplicate(Filled, $%) ++ lists:duplicate(Empty, $.),
    io:format("\r[~s] ~3.B%", [Bar, Percent]),
    if
        Done == Total -> io:format("\n");
        true -> ok
    end.

ensure_count(Ref, Total) ->
    io:format("Ensuring writing all done...\n"),
    ensure_count(Ref, Total, Total).

ensure_count(_Ref, 0, _Total) ->
    ok;
ensure_count(Ref, Rem, Total) ->
    receive
        {Ref, done} -> ensure_count(Ref, Rem - 1, Total)
    after 300000 ->
        io:format("Timeout waiting for async callbacks\n"),
        exit(timeout)
    end.

print_summary(Results) ->
    io:format("\n  === Summary ===\n"),
    io:format(" ~-45s | ~-15s | ~-20s\n", ["Benchmark", "Duration", "Throughput"]),
    io:format(" ~-45s | ~-15s | ~-20s\n", [
        "---------------------------------------------", "---------------", "-------------------"
    ]),
    lists:foreach(
        fun({Name, Duration, Throughput}) ->
            io:format(" ~-45s | ~14.4fs | ~12.2f rows/s \n", [Name, Duration, Throughput])
        end,
        Results
    ).

time_fun(Fun) ->
    Start_us = erlang:monotonic_time(micro_seconds),
    Result = try Fun() of
                 R -> R
             catch
                 Type:Reason:Stack ->
                    End_Exc = erlang:monotonic_time(micro_seconds),
                    Time_Exc = End_Exc - Start_us,
                    io:format("Function failed after ~p micro_seconds with ~p:~p~n",
                              [Time_Exc, Type, Reason]),
                    erlang:raise(Type, Reason, Stack)
            end,
    End_us = erlang:monotonic_time(micro_seconds),
    Time_us = End_us - Start_us,
    %% microsecond to second, min 1ms
    {Result, max(0.001, Time_us / 1000_000.0)}.
