-module(greptimedb_io_probe).
-export([main/0]).

main() ->
    logger:set_primary_config(level, warning),
    {ok, _} = application:ensure_all_started(greptimedb_rs),
    Port = list_to_integer(os:getenv("PROBE_CONTROL_PORT")),
    {ok, Control} = gen_tcp:connect(
        "127.0.0.1", Port, [binary, {packet, line}, {active, false}], 5000
    ),
    Endpoint = list_to_binary(os:getenv("PROBE_ENDPOINT")),
    Mode = list_to_existing_atom(os:getenv("PROBE_MODE")),
    Sizes = [list_to_integer(S) || S <- string:split(os:getenv("PROBE_POOL_SIZES", "4"), ",", all)],
    File = os:getenv("PROBE_FILE"),
    ok = file:write_file(File, <<"dirty-io-probe">>),
    {Base, {ok, _}} = timer:tc(file, read_file, [File]),
    Clients = [
        begin
            {ok, C} = greptimedb_rs:start_client(#{
                endpoints => [Endpoint],
                dbname => <<"public">>,
                pool_name => {io_probe, I},
                pool_size => S,
                pool_type => round_robin,
                request_timeout => 2000,
                stream_timeout => 2000
            }),
            C
        end
     || {I, S} <- lists:zip(lists:seq(1, length(Sizes)), Sizes)
    ],
    {ok, Healthy} = greptimedb_rs:start_client(#{
        endpoints => [list_to_binary(os:getenv("PROBE_BACKEND"))],
        dbname => <<"public">>,
        pool_name => io_probe_healthy,
        pool_size => 1,
        request_timeout => 2000,
        stream_timeout => 2000
    }),
    put(healthy_client, Healthy),
    Rows = [
        #{
            timestamp => 1700000000000,
            tags => #{<<"sensor_id">> => <<"probe">>},
            fields => #{<<"reading">> => 7}
        }
    ],
    Targets =
        case Mode of
            stream_write -> setup_streams(Clients, Rows);
            stream_write_async -> setup_streams(Clients, Rows);
            _ -> Clients
        end,
    event(Control, #{
        event => ready, baseline_us => Base, otp => list_to_binary(erlang:system_info(otp_release))
    }),
    {ok, <<"go\n">>} = gen_tcp:recv(Control, 0, 10000),
    Parent = self(),
    Start = erlang:monotonic_time(microsecond),
    lists:foreach(
        fun({Target, S}) ->
            lists:foreach(
                fun(_) ->
                    spawn(fun() ->
                        Result = invoke(Mode, Target, Rows),
                        Parent ! {result, Result, erlang:monotonic_time(microsecond) - Start}
                    end)
                end,
                lists:seq(1, S)
            )
        end,
        lists:zip(Targets, Sizes)
    ),
    loop(Control, File, Clients, Rows, [], pending).

setup_streams(Clients, Rows) ->
    C = hd(Clients),
    {ok, _} = greptimedb_rs:query(C, <<"DROP TABLE IF EXISTS io_probe_stream">>),
    {ok, _} = greptimedb_rs:query(
        C,
        <<"CREATE TABLE io_probe_stream (ts TIMESTAMP TIME INDEX, sensor_id STRING, reading INT64, PRIMARY KEY(sensor_id)) ENGINE=mito">>
    ),
    [
        begin
            {ok, S} = greptimedb_rs:stream_start(Client, <<"io_probe_stream">>, hd(Rows)),
            S
        end
     || Client <- Clients
    ].

invoke(query, C, _) ->
    greptimedb_rs:query(C, <<"SELECT 1">>);
invoke(query_async, C, _) ->
    await(fun(Cb) -> greptimedb_rs:query_async(C, <<"SELECT 1">>, Cb) end);
invoke(insert, C, Rows) ->
    greptimedb_rs:insert(C, <<"io_probe_insert">>, Rows);
invoke(insert_async, C, Rows) ->
    await(fun(Cb) -> greptimedb_rs:insert_async(C, <<"io_probe_insert">>, Rows, Cb) end);
invoke(stream_write, S, Rows) ->
    greptimedb_rs:stream_write(S, Rows);
invoke(stream_write_async, S, Rows) ->
    await(fun(Cb) -> greptimedb_rs:stream_write_async(S, Rows, Cb) end).

await(Fun) ->
    P = self(),
    Ref = make_ref(),
    case Fun({fun(R) -> P ! {Ref, R} end, []}) of
        {ok, _} ->
            receive
                {Ref, R} -> R
            end;
        Error ->
            Error
    end.

loop(Control, File, Clients, Rows, Results, FileResult) ->
    {R, F} = drain(Results, FileResult),
    case gen_tcp:recv(Control, 0, 50) of
        {error, timeout} ->
            loop(Control, File, Clients, Rows, R, F);
        {ok, <<"measure\n">>} ->
            P = self(),
            spawn(fun() ->
                {Elapsed, Res} = timer:tc(fun() ->
                    ok = file:write_file(File, <<"dirty-io-probe">>),
                    file:read_file(File)
                end),
                P ! {file_result, Elapsed, Res}
            end),
            Healthy = get(healthy_client),
            spawn(fun() ->
                Started = erlang:monotonic_time(microsecond),
                Insert = greptimedb_rs:insert(Healthy, <<"io_probe_healthy">>, Rows),
                Query = greptimedb_rs:query(
                    Healthy, <<"SELECT reading FROM io_probe_healthy WHERE sensor_id = 'probe'">>
                ),
                P ! {healthy, Insert, Query, erlang:monotonic_time(microsecond) - Started}
            end),
            event(Control, #{event => measuring}),
            loop(Control, File, Clients, Rows, R, F);
        {ok, <<"snapshot\n">>} ->
            {R1, F1} = drain(R, F),
            event(Control, #{
                event => snapshot, results => R1, file => F1, resources => resources(Clients)
            }),
            loop(Control, File, Clients, Rows, R1, F1);
        {ok, <<"recover\n">>} ->
            P = self(),
            spawn(fun() ->
                C = hd(Clients),
                Insert = greptimedb_rs:insert(C, <<"io_probe_recovery">>, Rows),
                Query = greptimedb_rs:query(
                    C, <<"SELECT reading FROM io_probe_recovery WHERE sensor_id = 'probe'">>
                ),
                Mode = list_to_existing_atom(os:getenv("PROBE_MODE")),
                case Mode of
                    M when M =:= stream_write; M =:= stream_write_async ->
                        {ok, Stream} = greptimedb_rs:stream_start(
                            C, <<"io_probe_stream">>, hd(Rows)
                        ),
                        Write = greptimedb_rs:stream_write(Stream, Rows),
                        Close = greptimedb_rs:stream_close(Stream),
                        Read = greptimedb_rs:query(
                            C, <<"SELECT reading FROM io_probe_stream WHERE sensor_id = 'probe'">>
                        ),
                        P ! {stream_recovery, Write, Close, Read};
                    _ ->
                        ok
                end,
                P ! {recovery, Insert, Query}
            end),
            loop(Control, File, Clients, Rows, R, F);
        {ok, <<"stop\n">>} ->
            event(Control, #{event => stopped}),
            halt(0);
        {error, closed} ->
            halt(1)
    end.

drain(Results, FileResult) ->
    receive
        {result, R, Elapsed} ->
            drain([#{result => classify(R), elapsed_us => Elapsed} | Results], FileResult);
        {file_result, Elapsed, Res} ->
            drain(Results, #{elapsed_us => Elapsed, result => classify(Res)});
        {healthy, Insert, Query, Elapsed} ->
            put(healthy, #{insert => classify(Insert), query => Query, elapsed_us => Elapsed}),
            drain(Results, FileResult);
        {stream_recovery, Write, Close, Read} ->
            put(stream_recovery, #{write => Write, close => Close, read => Read}),
            drain(Results, FileResult);
        {recovery, Insert, Query} ->
            put(recovery, #{insert => classify(Insert), query => Query}),
            drain(Results, FileResult)
    after 0 -> {Results, FileResult}
    end.

classify({ok, _}) -> ok;
classify(ok) -> ok;
classify({error, R}) -> #{error => iolist_to_binary(io_lib:format("~p", [R]))};
classify(R) -> iolist_to_binary(io_lib:format("~p", [R])).

event(Control, M) ->
    Recovery =
        case get(recovery) of
            undefined -> #{};
            V -> #{recovery => V}
        end,
    Healthy =
        case get(healthy) of
            undefined -> #{};
            H -> #{healthy => H}
        end,
    Stream =
        case get(stream_recovery) of
            undefined -> #{};
            S -> #{stream_recovery => S}
        end,
    gen_tcp:send(Control, [
        json:encode(convert(maps:merge(Stream, maps:merge(maps:merge(M, Recovery), Healthy)))), "\n"
    ]).
convert(M) when is_map(M) -> maps:map(fun(_, V) -> convert(V) end, M);
convert(L) when is_list(L) -> [convert(V) || V <- L];
convert(T) when is_tuple(T) -> convert(tuple_to_list(T));
convert(A) when is_atom(A) -> atom_to_binary(A);
convert(V) -> V.

resources(_Clients) ->
    {Runtimes, Operations} = greptimedb_rs_nif:resource_counts(),
    #{runtimes => Runtimes, operations => Operations}.
