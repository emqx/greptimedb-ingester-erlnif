-module(row_insert_bench).

-export([run/0]).

run() ->
    application:ensure_all_started(gproc),
    application:ensure_all_started(ecpool),
    application:ensure_all_started(greptimedb_rs),
    Mode = list_to_atom(os:getenv("BENCH_MODE")),
    Label = list_to_binary(os:getenv("BENCH_LABEL")),
    Endpoint = list_to_binary(os:getenv("BENCH_ENDPOINT")),
    {ok, C} = greptimedb_rs:start_client(#{
        endpoints => [Endpoint],
        dbname => <<"public">>,
        pool_size => 1
    }),
    Table = <<"perf_", Label/binary>>,
    {ok, _} = q(C, <<"DROP TABLE IF EXISTS ", Table/binary>>),
    {DefaultN, Warm, Iters} =
        case Mode of
            row_single -> {1, 10, 500};
            row_batch -> {1000, 2, 10};
            row_async -> {1000, 2, 10};
            row_mixed -> {1000, 2, 10};
            bulk_dense -> {2048, 0, 1};
            bulk_sparse -> {2048, 0, 1}
        end,
    N =
        case Mode of
            row_batch -> env_n(DefaultN);
            row_async -> env_n(DefaultN);
            row_mixed -> env_n(DefaultN);
            _ -> DefaultN
        end,
    Columns = iolist_to_binary([
        lists:join(",", [io_lib:format("s~p STRING", [I]) || I <- lists:seq(1, 20)]),
        ",v BIGINT"
    ]),
    {ok, _} = q(
        C,
        <<"CREATE TABLE ", Table/binary, " (ts TIMESTAMP TIME INDEX,", Columns/binary,
            ") ENGINE=mito">>
    ),
    Strings = maps:from_list([{key(I), <<"abcdefgh">>} || I <- lists:seq(1, 20)]),
    Gap =
        case Mode of
            bulk_sparse -> 3600000;
            _ -> 1
        end,
    Batches = [
        [row(I, Mode, Strings, Gap) || I <- lists:seq(B * N + 1, (B + 1) * N)]
     || B <- lists:seq(0, Warm + Iters - 1)
    ],
    {WarmBatches, Measured} = lists:split(Warm, Batches),
    lists:foreach(fun(Rows) -> write(Mode, C, Table, Rows, N) end, WarmBatches),
    Us =
        case Mode of
            bulk_dense ->
                bulk(C, Table, Mode, hd(Measured), N);
            bulk_sparse ->
                bulk(C, Table, Mode, hd(Measured), N);
            _ ->
                {Elapsed, ok} = timer:tc(fun() ->
                    lists:foreach(fun(Rows) -> write(Mode, C, Table, Rows, N) end, Measured)
                end),
                Elapsed
        end,
    TotalRows = (Warm + Iters) * N,
    verify(C, Table, Mode, TotalRows),
    io:format("RESULT mode=~p rows=~p us=~p~n", [Mode, N * Iters, Us]),
    {ok, _} = q(C, <<"DROP TABLE ", Table/binary>>),
    ok = greptimedb_rs:stop_client(C).

row(I, Mode, Strings, Gap) ->
    Fields =
        case Mode of
            row_mixed when I rem 2 =:= 0 -> maps:remove(key(20), Strings);
            _ -> Strings
        end,
    #{
        timestamp => 1700000000000 + I * Gap,
        fields => Fields#{<<"v">> => I}
    }.

key(I) ->
    iolist_to_binary(io_lib:format("s~p", [I])).

write(row_async, Client, Table, Rows, N) ->
    Self = self(),
    Ref = make_ref(),
    Callback = {fun(Result) -> Self ! {Ref, Result} end, []},
    {ok, _} = greptimedb_rs:insert_async(Client, Table, Rows, Callback),
    receive
        {Ref, {ok, N}} -> ok;
        {Ref, Other} -> error({unexpected_async_result, Other})
    after 30000 -> error(async_timeout)
    end;
write(_, Client, Table, Rows, N) ->
    {ok, N} = greptimedb_rs:insert(Client, Table, Rows),
    ok.

bulk(Client, Table, Mode, Rows, N) ->
    {ok, Stream} = greptimedb_rs:stream_start(Client, Table, #{}),
    {Us, ok} = timer:tc(fun() ->
        ok = greptimedb_rs:stream_write(Stream, Rows),
        ok = greptimedb_rs:stream_close(Stream)
    end),
    verify(Client, Table, Mode, N),
    Us.

verify(Client, Table, Mode, TotalRows) ->
    ExpectedSum = TotalRows * (TotalRows + 1) div 2,
    {ok, [[TotalRows, 1, TotalRows, ExpectedSum]]} = q(
        Client,
        <<"SELECT count(*),min(v),max(v),sum(v) FROM ", Table/binary>>
    ),
    ExpectedS20 =
        case Mode of
            row_mixed -> TotalRows - (TotalRows div 2);
            _ -> TotalRows
        end,
    {ok, [[TotalRows, ExpectedS20, <<"abcdefgh">>, <<"abcdefgh">>]]} = q(
        Client,
        <<"SELECT count(s1),count(s20),min(s1),max(s20) FROM ", Table/binary>>
    ),
    ok.

q(Client, Sql) ->
    greptimedb_rs:query(Client, Sql).

env_n(Default) ->
    case os:getenv("BENCH_N") of
        false -> Default;
        Value -> list_to_integer(Value)
    end.
