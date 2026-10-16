-module(greptimedb_rs_regression_SUITE).
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-compile([export_all, nowarn_export_all]).

suite() ->
    [{timetrap, {seconds, 45}}].

all() ->
    [{group, Name} || {Name, _} <- network_cases()] ++
        [{group, lifecycle}, {group, codec}, {group, failures}].

groups() ->
    [{Name, [], [t_network]} || {Name, _} <- network_cases()] ++
        [
            {lifecycle, [], [
                t_queued, t_caller_cancel, t_pool_stop, t_socket_exit, t_library_stop, t_owner_exit
            ]},
            {codec, [], [
                t_codec_cancel, t_codec_timeout, t_codec_stop, t_query_cancel, t_query_reply_bounds
            ]},
            {failures, [], [
                t_startup_failure,
                t_startup_pressure,
                t_callback_exception,
                t_callback_timeout,
                t_conversion_errors,
                t_insert_bounds,
                t_stream_bounds,
                t_request_timeout,
                t_request_timeout_expiry,
                t_stream_timeout,
                t_stream_response_drain,
                t_stream_response_error,
                t_stream_response_faults,
                t_queued_close_timeout,
                t_queued_close_cancel,
                t_rejected_close,
                t_close_during_start,
                t_close_after_queued_start,
                t_submit_rejection,
                t_pool_stop_flush,
                t_pool_stop_flush_busy,
                t_pool_stop_flush_timeout,
                t_pool_stop_flush_killed,
                t_library_stop_flush,
                t_library_stop_flush_timeout,
                t_owner_exit_no_flush,
                t_owner_error_no_flush,
                t_library_stop_active_close,
                t_library_stop_active_write
            ]}
        ].

network_cases() ->
    Modes = [query, query_async, insert, insert_async, stream_write, stream_write_async],
    Cases =
        [
            {Mode, Sizes, Fault}
         || Mode <- Modes,
            Sizes <- [[4], [2, 2]],
            Fault <- [release, disconnect]
        ] ++ [{Mode, [4], restart} || Mode <- Modes] ++
            [{query, [3], release}],
    [
        {
            list_to_atom(
                lists:flatten(
                    io_lib:format("~s_~s_~s", [
                        Mode,
                        string:join([integer_to_list(Size) || Size <- Sizes], "x"),
                        Fault
                    ])
                )
            ),
            Case
        }
     || Case = {Mode, Sizes, Fault} <- Cases
    ].

init_per_suite(Config) ->
    case os:getenv("GREPTIME_BIN") of
        false ->
            {skip, "Set GREPTIME_BIN or run make regression"};
        Binary ->
            {ok, Server} = greptimedb_test_server:start(Binary, ?config(priv_dir, Config)),
            [{server, Server} | Config]
    end.

end_per_suite(Config) ->
    greptimedb_test_server:stop(?config(server, Config)).

init_per_group(codec, Config) ->
    [{codec_workers, 1} | Config];
init_per_group(Name, Config) ->
    case lists:keyfind(Name, 1, network_cases()) of
        {Name, Case} -> [{network_case, Case} | Config];
        false -> Config
    end.

end_per_group(_, _) ->
    ok.

init_per_testcase(Testcase, Config) ->
    logger:set_primary_config(level, warning),
    application:load(greptimedb_rs),
    application:set_env(
        greptimedb_rs, codec_workers, proplists:get_value(codec_workers, Config, 2)
    ),
    ShutdownTimeout =
        case Testcase of
            t_library_stop_flush_timeout -> 200;
            T when T =:= t_owner_exit_no_flush; T =:= t_owner_error_no_flush -> 10000;
            _ -> 5000
        end,
    application:set_env(greptimedb_rs, shutdown_timeout, ShutdownTimeout),
    {ok, _} = application:ensure_all_started(greptimedb_rs),
    Config.

end_per_testcase(_, Config) ->
    lists:foreach(
        fun(Name) ->
            try
                greptimedb_rs:stop_client({regression, Name})
            catch
                _:_ -> ok
            end
        end,
        [1, 2, healthy]
    ),
    application:stop(greptimedb_rs),
    greptimedb_test_server:disconnect(?config(server, Config)),
    ok.

client(Server, Name, Size, Timeout, Direct) ->
    Endpoint =
        case Direct of
            true -> greptimedb_test_server:backend(Server);
            false -> greptimedb_test_server:endpoint(Server)
        end,
    Opts = #{
        endpoints => [Endpoint],
        dbname => <<"public">>,
        pool_name => {regression, Name},
        pool_size => Size,
        pool_type => round_robin,
        auto_reconnect => 1,
        request_timeout => Timeout,
        stream_timeout => Timeout
    },
    {ok, Client} = greptimedb_rs:start_client(Opts),
    {ok, [[1]]} = greptimedb_rs:query(Client, <<"SELECT 1">>),
    Client.

row() ->
    #{timestamp => 1700000000000, fields => #{<<"reading">> => 7}}.

start_request(Fun) ->
    Parent = self(),
    Ref = make_ref(),
    Pid = spawn(fun() ->
        Monitor = erlang:monitor(process, Parent),
        {Elapsed, Result} = timer:tc(Fun),
        Parent ! {Ref, Result, Elapsed},
        watch_callbacks(Parent, Monitor, Ref)
    end),
    {Pid, Ref}.

%% Keep callback recipients alive so a second completion is observable.
watch_callbacks(Parent, Monitor, Ref) ->
    receive
        {'DOWN', Monitor, process, Parent, _} ->
            ok;
        {_, Result} ->
            Parent ! {Ref, {duplicate_callback, Result}, 0},
            watch_callbacks(Parent, Monitor, Ref)
    after 60000 ->
        erlang:demonitor(Monitor, [flush]),
        ok
    end.

result({_, Ref}, Timeout) ->
    receive
        {Ref, Result, Elapsed} -> {Result, Elapsed}
    after Timeout -> ct:fail({missing_result, Ref})
    end.

no_result({_, Ref}) ->
    receive
        {Ref, Result, _} -> ct:fail({duplicate_result, Result})
    after 200 -> ok
    end.

wait(Fun, Timeout) ->
    wait_until(Fun, erlang:monotonic_time(millisecond) + Timeout).

wait_until(Fun, Deadline) ->
    Value = Fun(),
    case Value of
        true ->
            ok;
        _ ->
            case erlang:monotonic_time(millisecond) < Deadline of
                true ->
                    timer:sleep(10),
                    wait_until(Fun, Deadline);
                false ->
                    ct:fail({condition_timeout, Value})
            end
    end.

counts(Expected) ->
    wait(fun() -> greptimedb_rs_nif:resource_counts() =:= Expected end, 5000).

kill_callers(Requests) ->
    lists:foreach(fun({Pid, _}) -> exit(Pid, kill) end, Requests).

%% Keep all API variants and pool layouts in one table; assertions are shared.

t_network(Config) ->
    Server = ?config(server, Config),
    {Mode, Sizes, Fault} = ?config(network_case, Config),
    Clients = [
        client(Server, I, Size, 2000, false)
     || {I, Size} <- lists:zip(lists:seq(1, length(Sizes)), Sizes)
    ],
    Healthy = client(Server, healthy, 1, 2000, true),
    counts({1, 0}),
    Targets =
        case Mode of
            M when M =:= stream_write; M =:= stream_write_async ->
                C = hd(Clients),
                {ok, _} = greptimedb_rs:query(C, <<"DROP TABLE IF EXISTS regression_stream">>),
                {ok, _} = greptimedb_rs:query(
                    C,
                    <<"CREATE TABLE regression_stream (ts TIMESTAMP TIME INDEX, reading INT64)">>
                ),
                [
                    start_stream(Client)
                 || Client <- Clients
                ];
            _ ->
                Clients
        end,
    File = filename:join(?config(priv_dir, Config), "dirty-io.data"),
    ok = file:write_file(File, <<"dirty-io">>),
    ok = greptimedb_test_server:pause(Server),
    Requests = [
        start_request(fun() -> invoke(Mode, Target) end)
     || {Target, Size} <- lists:zip(Targets, Sizes), _ <- lists:seq(1, Size)
    ],
    wait(fun() -> greptimedb_test_server:headers(Server) >= length(Requests) end, 1000),
    Io = start_request(fun() ->
        ok = file:write_file(File, <<"dirty-io">>),
        file:read_file(File)
    end),
    {{ok, <<"dirty-io">>}, IoElapsed} = result(Io, 1000),
    ?assert(IoElapsed =< 1000000),
    Progress = start_request(fun() -> recover(Healthy, <<"regression_healthy">>) end),
    {ok, ProgressElapsed} = result(Progress, 1000),
    ?assert(ProgressElapsed =< 1000000),
    lists:foreach(
        fun(Request) ->
            {{error, timeout}, Elapsed} = result(Request, 3000),
            ?assert(Elapsed =< 3000000)
        end,
        Requests
    ),
    counts({1, 0}),
    case Fault of
        release -> greptimedb_test_server:release(Server);
        disconnect -> greptimedb_test_server:disconnect(Server);
        restart -> greptimedb_test_server:restart(Server)
    end,
    C1 = hd(Clients),
    ok = recover(C1, <<"regression_recovered">>),
    case Mode of
        M1 when M1 =:= stream_write; M1 =:= stream_write_async ->
            {ok, Stream1} = greptimedb_rs:stream_start(C1, <<"regression_stream">>, row()),
            ok = greptimedb_rs:stream_write(Stream1, [row()]),
            ok = greptimedb_rs:stream_close(Stream1),
            {ok, [[7]]} = greptimedb_rs:query(C1, <<"SELECT reading FROM regression_stream">>);
        _ ->
            ok
    end,
    lists:foreach(fun no_result/1, Requests).

start_stream(Client) ->
    {ok, Stream} = greptimedb_rs:stream_start(Client, <<"regression_stream">>, row()),
    Stream.

invoke(query, Client) ->
    greptimedb_rs:query(Client, <<"SELECT 1">>);
invoke(query_async, Client) ->
    async(fun(Cb) -> greptimedb_rs:query_async(Client, <<"SELECT 1">>, Cb) end);
invoke(insert, Client) ->
    greptimedb_rs:insert(Client, <<"regression_insert">>, [row()]);
invoke(insert_async, Client) ->
    async(fun(Cb) ->
        greptimedb_rs:insert_async(Client, <<"regression_insert">>, [row()], Cb)
    end);
invoke(stream_write, Stream) ->
    greptimedb_rs:stream_write(Stream, [row()]);
invoke(stream_write_async, Stream) ->
    async(fun(Cb) ->
        greptimedb_rs:stream_write_async(Stream, [row()], Cb)
    end).

async(Fun) ->
    async(Fun, 30000).

async(Fun, Timeout) ->
    Parent = self(),
    Ref = make_ref(),
    {ok, _} = Fun({fun(Result) -> Parent ! {Ref, Result} end, []}),
    receive
        {Ref, Result} -> Result
    after Timeout -> ct:fail(callback_timeout)
    end.

recover(Client, Table) ->
    {ok, 1} = greptimedb_rs:insert(Client, Table, [row()]),
    {ok, [[7]]} = greptimedb_rs:query(Client, <<"SELECT reading FROM ", Table/binary>>),
    ok.

t_queued(Config) ->
    lifecycle(queued, Config).

t_caller_cancel(Config) ->
    lifecycle(caller_cancel, Config).

t_pool_stop(Config) ->
    lifecycle(pool_stop, Config).

t_socket_exit(Config) ->
    lifecycle(socket_exit, Config).

t_library_stop(Config) ->
    lifecycle(library_stop, Config).

t_owner_exit(Config) ->
    lifecycle(owner_exit, Config).

lifecycle(Mode, Config) ->
    Server = ?config(server, Config),
    Timeout =
        case Mode of
            queued -> 1000;
            _ -> 10000
        end,
    Client = client(Server, 1, 1, Timeout, false),
    Healthy = client(Server, healthy, 1, 10000, true),
    counts({1, 0}),
    greptimedb_test_server:pause(Server),
    Request = start_request(fun() ->
        case Mode of
            caller_cancel -> invoke(query, Client);
            _ -> invoke(query_async, Client)
        end
    end),
    wait(fun() -> greptimedb_test_server:headers(Server) >= 1 end, 1000),
    case Mode of
        queued ->
            Opts = maps:get(conn_opts, Client),
            Queued = start_request(fun() ->
                invoke(
                    query_async,
                    Client#{conn_opts := Opts#{request_timeout := 200}}
                )
            end),
            {{error, timeout}, QueuedElapsed} = result(Queued, 500),
            ?assert(QueuedElapsed =< 500000),
            ?assertEqual({1, 1}, greptimedb_rs_nif:resource_counts()),
            {{error, timeout}, _} = result(Request, 2000);
        caller_cancel ->
            kill_callers([Request]);
        pool_stop ->
            ok = greptimedb_rs:stop_client(Client);
        socket_exit ->
            [{_, Worker}] = ecpool:workers(maps:get(pool_name, Client)),
            {ok, Conn} = ecpool_worker:client(Worker),
            exit(Conn, kill);
        library_stop ->
            ok = application:stop(greptimedb_rs);
        owner_exit ->
            exit(whereis(greptimedb_rs_runtime), kill)
    end,
    case Mode of
        library_stop -> counts({0, 0});
        _ -> counts({1, 0})
    end,
    case Mode of
        M when M =:= pool_stop; M =:= library_stop; M =:= owner_exit ->
            {{error, closed}, _} = result(Request, 1000);
        socket_exit ->
            kill_callers([Request]);
        _ ->
            ok
    end,
    greptimedb_test_server:release(Server),
    {Recovered, RecoveredHealthy} =
        case Mode of
            library_stop ->
                ok = greptimedb_rs:stop_client(Client),
                ok = greptimedb_rs:stop_client(Healthy),
                {ok, _} = application:ensure_all_started(greptimedb_rs),
                {client(Server, 1, 1, 10000, false), client(Server, healthy, 1, 10000, true)};
            pool_stop ->
                {client(Server, 1, 1, 10000, false), Healthy};
            _ ->
                wait_connected(Client),
                wait_connected(Healthy),
                {Client, Healthy}
        end,
    ok = recover(RecoveredHealthy, <<"regression_healthy">>),
    ok = recover(Recovered, <<"regression_lifecycle">>),
    no_result(Request),
    case Mode of
        pool_stop -> pool_cycles(Server, Recovered, RecoveredHealthy);
        _ -> ok
    end.

wait_connected(Client) ->
    wait(
        fun() ->
            try greptimedb_rs:query(Client, <<"SELECT 1">>) of
                {ok, [[1]]} -> true;
                _ -> false
            catch
                exit:_ -> false
            end
        end,
        5000
    ).

pool_cycles(Server, Client, Healthy) ->
    ok = greptimedb_rs:stop_client(Client),
    ok = greptimedb_rs:stop_client(Healthy),
    lists:foreach(
        fun(_) ->
            C = client(Server, 1, 1, 10000, true),
            ok = greptimedb_rs:stop_client(C),
            counts({1, 0})
        end,
        lists:seq(1, 20)
    ),
    ok = application:stop(greptimedb_rs),
    counts({0, 0}),
    {ok, _} = application:ensure_all_started(greptimedb_rs),
    C1 = client(Server, 1, 1, 10000, true),
    ok = greptimedb_rs:stop_client(C1),
    counts({1, 0}).

t_codec_cancel(Config) ->
    codec(cancel, Config).

t_codec_timeout(Config) ->
    codec(timeout, Config).

t_codec_stop(Config) ->
    codec(library_stop, Config).

t_query_cancel(Config) ->
    codec(query_cancel, Config).

t_query_reply_bounds(Config) ->
    Client = client(?config(server, Config), 1, 1, 10000, true),
    Table = <<"regression_reply_bounds">>,
    {ok, _} = greptimedb_rs:query(
        Client,
        <<"CREATE TABLE regression_reply_bounds (ts TIMESTAMP TIME INDEX, reading BIGINT)">>
    ),
    Rows = [(row())#{timestamp := 1700000000000 + I} || I <- lists:seq(1, 257)],
    {ok, 257} = greptimedb_rs:insert(Client, Table, Rows),
    lists:foreach(
        fun(N) ->
            SQL = iolist_to_binary([
                "SELECT reading FROM regression_reply_bounds LIMIT ", integer_to_binary(N)
            ]),
            ?assertEqual({ok, lists:duplicate(N, [7])}, greptimedb_rs:query(Client, SQL)),
            ?assertEqual(
                {ok, lists:duplicate(N, [7])},
                async(fun(Cb) -> greptimedb_rs:query_async(Client, SQL, Cb) end)
            )
        end,
        [256, 257]
    ),
    lists:foreach(
        fun(N) ->
            SQL = iolist_to_binary([
                "SELECT ", lists:join(",", [integer_to_binary(I) || I <- lists:seq(1, N)])
            ]),
            ?assertEqual({ok, [lists:seq(1, N)]}, greptimedb_rs:query(Client, SQL))
        end,
        [8, 9]
    ),
    lists:foreach(
        fun(Size) ->
            SQL = iolist_to_binary(["SELECT repeat('x',", integer_to_binary(Size), ")"]),
            {ok, [[Payload]]} = greptimedb_rs:query(Client, SQL),
            ?assert(Payload =:= binary:copy(<<"x">>, Size))
        end,
        [16383, 16384, 16385]
    ),
    %% A large single-cell result must use codec workers through both public APIs.
    SQL = <<"SELECT repeat('x',67108864)">>,
    lists:foreach(
        fun(Mode) ->
            {Result, Peak} = query_codec_activity(fun() ->
                reply_query(Mode, Client, SQL)
            end),
            {ok, [[Payload]]} = Result,
            ?assert(Payload =:= binary:copy(<<"x">>, 67108864)),
            ?assert(Peak > 0)
        end,
        [sync, async]
    ),
    counts({1, 0}),
    ?assertEqual({1, 2, 0, 0, 0}, greptimedb_rs_nif:codec_counts()).

reply_query(sync, Client, SQL) ->
    greptimedb_rs:query(Client, SQL);
reply_query(async, Client, SQL) ->
    async(fun(Cb) -> greptimedb_rs:query_async(Client, SQL, Cb) end).

query_codec_activity(Call) ->
    Parent = self(),
    Ref = make_ref(),
    Sampler = spawn_link(fun() -> query_codec_activity(Parent, Ref, 0) end),
    Result =
        try
            Call()
        after
            Sampler ! stop
        end,
    receive
        {Ref, Peak} -> {Result, Peak}
    after 1000 -> error(codec_sampler_timeout)
    end.

query_codec_activity(Parent, Ref, Peak) ->
    {_, _, Active, _, _} = greptimedb_rs_nif:codec_counts(),
    Next = max(Peak, Active),
    receive
        stop -> Parent ! {Ref, Next}
    after 1 -> query_codec_activity(Parent, Ref, Next)
    end.

codec(Mode, Config) ->
    Server = ?config(server, Config),
    Timeout =
        case Mode of
            timeout -> 150;
            _ -> 10000
        end,
    Client = client(Server, 1, 4, Timeout, true),
    {ok, _} = greptimedb_rs:query(
        Client,
        <<"CREATE TABLE IF NOT EXISTS regression_codec (ts TIMESTAMP TIME INDEX, reading INT64)">>
    ),
    ?assertEqual({1, 2, 0, 0, 0}, greptimedb_rs_nif:codec_counts()),
    {ok, 0} = greptimedb_rs:insert(Client, <<"regression_codec">>, []),
    Request =
        case Mode of
            query_cancel ->
                WarmRows = [(row())#{timestamp := 1700000000000 + I} || I <- lists:seq(1, 60000)],
                {ok, 60000} = greptimedb_rs:insert(Client, <<"regression_codec">>, WarmRows),
                fun() ->
                    greptimedb_rs:query(
                        Client,
                        <<
                            "SELECT a.reading FROM regression_codec a "
                            "CROSS JOIN regression_codec b LIMIT 600000"
                        >>
                    )
                end;
            _ ->
                Rows = lists:duplicate(600000, row()),
                fun() -> greptimedb_rs:insert(Client, <<"regression_codec">>, Rows) end
        end,
    Requests = [start_request(Request) || _ <- lists:seq(1, 4)],
    wait(
        fun() ->
            {Workers, Capacity, Active, Queued, Waiting} = greptimedb_rs_nif:codec_counts(),
            ?assert(Active =< Workers),
            ?assert(Queued =< Capacity),
            Active > 0 andalso
                (Mode =:= timeout orelse Mode =:= query_cancel orelse
                    (Queued > 0 andalso Waiting > 0))
        end,
        5000
    ),
    case Mode of
        timeout -> lists:foreach(fun(R) -> {{error, timeout}, _} = result(R, 5000) end, Requests);
        library_stop -> ok = application:stop(greptimedb_rs);
        _ -> kill_callers(Requests)
    end,
    wait(
        fun() ->
            {_, Operations} = greptimedb_rs_nif:resource_counts(),
            {Workers, Capacity, Active, Queued, Waiting} = greptimedb_rs_nif:codec_counts(),
            ?assert(Active =< Workers),
            ?assert(Queued =< Capacity),
            ?assert(Operations >= Active),
            {Operations, Active, Queued, Waiting} =:= {0, 0, 0, 0}
        end,
        5000
    ),
    Recovered =
        case Mode of
            library_stop ->
                counts({0, 0}),
                ?assertEqual({0, 0, 0, 0, 0}, greptimedb_rs_nif:codec_counts()),
                ok = greptimedb_rs:stop_client(Client),
                {ok, _} = application:ensure_all_started(greptimedb_rs),
                client(Server, 1, 4, 10000, true);
            _ ->
                Client
        end,
    ok = recover(Recovered, <<"regression_codec_recovered">>),
    ok = application:stop(greptimedb_rs),
    counts({0, 0}),
    ?assertEqual({0, 0, 0, 0, 0}, greptimedb_rs_nif:codec_counts()).

t_startup_failure(Config) ->
    startup_child(invalid_config, Config).

t_startup_pressure(Config) ->
    case {os:type(), os:find_executable("prlimit")} of
        {{unix, linux}, Executable} when is_list(Executable) -> startup_child(pressure, Config);
        _ -> {skip, "Requires Linux prlimit"}
    end.

startup_child(Scenario, Config) ->
    Argument = #{
        scenario => Scenario,
        endpoint => greptimedb_test_server:backend(?config(server, Config))
    },
    Log = filename:join(?config(priv_dir, Config), atom_to_list(Scenario) ++ ".log"),
    greptimedb_test_server:run_beam(?MODULE, startup_probe, Argument, Log).

startup_probe(#{scenario := Scenario, endpoint := Endpoint}) ->
    logger:set_primary_config(level, error),
    try
        {ok, _} = application:ensure_all_started(ecpool),
        {module, greptimedb_rs_nif} = code:ensure_loaded(greptimedb_rs_nif),
        application:load(greptimedb_rs),
        startup_scenario(Scenario),
        {ok, _} = application:ensure_all_started(greptimedb_rs),
        counts({1, 0}),
        {ok, Client} = greptimedb_rs:start_client(#{
            endpoints => [Endpoint],
            dbname => <<"public">>,
            pool_name => startup_probe,
            pool_size => 1
        }),
        ok = recover(Client, <<"regression_startup_recovered">>),
        ok = greptimedb_rs:stop_client(Client),
        ok = application:stop(greptimedb_rs),
        counts({0, 0}),
        halt(0)
    catch
        Class:Reason:Stack ->
            logger:error(#{
                msg => startup_probe_failed,
                exception => Class,
                reason => Reason,
                stacktrace => Stack
            }),
            halt(1)
    end.

startup_scenario(invalid_config) ->
    lists:foreach(
        fun({Key, Value}) ->
            application:set_env(greptimedb_rs, Key, Value),
            ?assertMatch({error, _}, application:ensure_all_started(greptimedb_rs)),
            ?assertEqual(undefined, whereis(greptimedb_rs_runtime)),
            counts({0, 0}),
            ?assertEqual({0, 0, 0, 0, 0}, greptimedb_rs_nif:codec_counts()),
            application:set_env(greptimedb_rs, Key, 2)
        end,
        [
            {runtime_workers, 65},
            {codec_workers, 0},
            {shutdown_timeout, 0},
            {shutdown_timeout, 30001}
        ]
    );
startup_scenario(pressure) ->
    Saved = string:trim(prlimit(["--nproc", "--output", "SOFT", "--noheadings"])),
    Upper =
        case Saved of
            "unlimited" -> 4096;
            _ -> min(4096, list_to_integer(Saved))
        end,
    try
        %% Only this child VM's thread limit changes; the database and parent keep theirs.
        {Boundary, Panicked} = startup_boundary(1, Upper, false),
        Outcomes = lists:map(
            fun(Limit) -> pressure_attempt(max(1, Limit)) end,
            lists:seq(Boundary - 4, Boundary + 2)
        ),
        ?assert(Panicked orelse lists:member(panic, Outcomes))
    after
        thread_limit(Saved)
    end.

startup_boundary(Low, High, Panicked) when Low > High ->
    case Low of
        1 -> halt(77);
        _ -> {Low, Panicked}
    end;
startup_boundary(Low, High, Panicked) ->
    Middle = (Low + High) div 2,
    case pressure_attempt(Middle) of
        ok -> startup_boundary(Low, Middle - 1, Panicked);
        error -> startup_boundary(Middle + 1, High, Panicked);
        panic -> startup_boundary(Middle + 1, High, true)
    end.

pressure_attempt(Limit) ->
    thread_limit(integer_to_list(Limit)),
    Result =
        try
            greptimedb_rs_nif:runtime_start(1, 1)
        catch
            Class:Reason -> {exception, Class, Reason}
        end,
    Outcome =
        case Result of
            Runtime when is_reference(Runtime) ->
                ok = greptimedb_rs_nif:runtime_stop(Runtime),
                ok;
            {error, <<"runtime initialization panicked:", _/binary>>} ->
                panic;
            {error, _} ->
                error;
            _ ->
                error({unexpected_startup_result, Result})
        end,
    ?assertEqual({0, 0}, greptimedb_rs_nif:resource_counts()),
    ?assertEqual({0, 0, 0, 0, 0}, greptimedb_rs_nif:codec_counts()),
    Outcome.

thread_limit(Soft) ->
    _ = prlimit(["--nproc=" ++ Soft ++ ":"]),
    timer:sleep(20).

prlimit(Args) ->
    Port = open_port(
        {spawn_executable, os:find_executable("prlimit")},
        [binary, exit_status, stderr_to_stdout, {args, ["--pid", os:getpid() | Args]}]
    ),
    prlimit_output(Port, <<>>).

prlimit_output(Port, Acc) ->
    receive
        {Port, {data, Data}} -> prlimit_output(Port, <<Acc/binary, Data/binary>>);
        {Port, {exit_status, 0}} -> binary_to_list(Acc);
        {Port, {exit_status, Status}} -> error({prlimit_failed, Status, Acc})
    after 5000 -> error(prlimit_timeout)
    end.

t_callback_exception(Config) ->
    Client = client(?config(server, Config), 1, 1, 10000, true),
    [{_, Worker}] = ecpool:workers(maps:get(pool_name, Client)),
    {ok, Conn} = ecpool_worker:client(Worker),
    lists:foreach(
        fun(Class) ->
            Ref = make_ref(),
            Callback = failing_callback(Ref, Class),
            {ok, _} = greptimedb_rs:query_async(Client, <<"SELECT 1">>, Callback),
            ?assertEqual({ok, [[1]]}, callback_result(Ref)),
            no_callback(Ref),
            ?assert(is_process_alive(Conn)),
            ?assertEqual({ok, [[1]]}, greptimedb_rs:query(Client, <<"SELECT 1">>))
        end,
        [error, throw, exit]
    ).

t_callback_timeout(Config) ->
    Server = ?config(server, Config),
    Client = client(Server, 1, 1, 200, false),
    Ref = make_ref(),
    greptimedb_test_server:pause(Server),
    {ok, _} = greptimedb_rs:query_async(Client, <<"SELECT 1">>, failing_callback(Ref, error)),
    ?assertEqual({error, timeout}, callback_result(Ref)),
    counts({1, 0}),
    no_callback(Ref),
    greptimedb_test_server:release(Server),
    ?assertEqual({ok, [[1]]}, greptimedb_rs:query(Client, <<"SELECT 1">>)).

failing_callback(Ref, Class) ->
    Parent = self(),
    {
        fun(Result) ->
            Parent ! {Ref, Result},
            erlang:raise(Class, callback_failure, [])
        end,
        []
    }.

callback_result(Ref) ->
    receive
        {Ref, Result} -> Result
    after 5000 -> error(callback_timeout)
    end.

no_callback(Ref) ->
    receive
        {Ref, Result} -> error({duplicate_callback, Result})
    after 200 -> ok
    end.

t_conversion_errors(Config) ->
    Client = client(?config(server, Config), 1, 1, 10000, true),
    Table = <<"regression_int8">>,
    {ok, _} = greptimedb_rs:query(
        Client,
        <<"CREATE TABLE regression_int8 (ts TIMESTAMP TIME INDEX, reading INT8)">>
    ),
    BadRow = (row())#{fields := #{<<"reading">> => 256}},
    RangeError = {error, {nif_error, <<"Value 256 out of range for type Int8">>}},
    ?assertEqual(RangeError, greptimedb_rs:insert(Client, Table, [BadRow])),
    ?assertEqual(
        RangeError,
        async(fun(Cb) ->
            greptimedb_rs:insert_async(Client, Table, [BadRow], Cb)
        end)
    ),
    {ok, Stream} = greptimedb_rs:stream_start(Client, Table, row()),
    ?assertEqual(RangeError, greptimedb_rs:stream_write(Stream, [BadRow])),
    {ok, NewStream} = greptimedb_rs:stream_start(Client, Table, row()),
    ?assertEqual(
        RangeError,
        async(fun(Cb) ->
            greptimedb_rs:stream_write_async(NewStream, [BadRow], Cb)
        end)
    ),
    WrongType = (row())#{fields := #{<<"reading">> => <<"not an integer">>}},
    ?assertEqual(
        {error, {nif_error, <<"Invalid value for type Int8: expected integer">>}},
        greptimedb_rs:insert(Client, Table, [WrongType])
    ),
    ?assertEqual(
        {error, {nif_error, task_panic}},
        greptimedb_rs:insert(Client, Table, [BadRow | bad_tail])
    ),
    ?assertEqual({ok, 1}, greptimedb_rs:insert(Client, Table, [row()])),
    ?assertEqual(
        {ok, [[7]]}, greptimedb_rs:query(Client, <<"SELECT reading FROM regression_int8">>)
    ),
    counts({1, 0}).

t_insert_bounds(Config) ->
    Client = client(?config(server, Config), 1, 1, 10000, true),
    Table = <<"bounded_insert">>,
    {ok, _} = greptimedb_rs:query(
        Client,
        <<"CREATE TABLE bounded_insert (ts TIMESTAMP TIME INDEX, reading INT64, payload STRING)">>
    ),
    %% Exercise both sides of the row limit through sync and async APIs.
    Total = lists:foldl(
        fun(N, Offset) ->
            Rows = bounded_rows(Offset, N),
            ?assertEqual({ok, N}, greptimedb_rs:insert(Client, Table, Rows)),
            NextRows = bounded_rows(Offset + N, N),
            ?assertEqual(
                {ok, N},
                async(fun(Cb) -> greptimedb_rs:insert_async(Client, Table, NextRows, Cb) end)
            ),
            Offset + 2 * N
        end,
        0,
        [1, 100, 128, 129, 512]
    ),
    ?assertEqual(
        {ok, [[Total, Total * (Total + 1) div 2]]},
        greptimedb_rs:query(Client, <<"SELECT count(*), sum(reading) FROM bounded_insert">>)
    ),
    %% A single row can exceed the byte limit, independent of its row count.
    Large = #{
        timestamp => 1700000020000,
        fields => #{
            <<"reading">> => 20000, <<"payload">> => binary:copy(<<"x">>, 32768)
        }
    },
    ?assertEqual({ok, 1}, greptimedb_rs:insert(Client, Table, [Large])),
    ?assertEqual(
        {ok, [[20000, 32768]]},
        greptimedb_rs:query(
            Client, <<"SELECT reading, length(payload) FROM bounded_insert WHERE reading = 20000">>
        )
    ),
    %% Check both sides of the binary budget, including unaligned binaries.
    BaseBytes = byte_size(Table) + byte_size(<<"reading">>) + byte_size(<<"payload">>),
    lists:foreach(
        fun(Extra) ->
            Size = 16384 - BaseBytes + Extra,
            Bits = <<0:1, (binary:copy(<<"x">>, Size))/binary, 0:7>>,
            <<_:1, Payload:Size/binary, _:7>> = Bits,
            I = 21000 + Extra,
            Row = #{
                timestamp => 1700000000000 + I,
                fields => #{
                    <<"reading">> => I, <<"payload">> => Payload
                }
            },
            ?assertEqual({ok, 1}, greptimedb_rs:insert(Client, Table, [Row])),
            SQL = iolist_to_binary([
                "SELECT reading, length(payload) FROM bounded_insert WHERE reading = ",
                integer_to_binary(I)
            ]),
            ?assertEqual({ok, [[I, Size]]}, greptimedb_rs:query(Client, SQL))
        end,
        [0, 1]
    ),
    %% Complex scalar values must fall back without an unbounded normal-NIF walk.
    [Nested] = bounded_rows(22000, 1),
    Bad = Nested#{fields := #{<<"reading">> => lists:duplicate(8192, 1)}},
    ?assertEqual(
        {error, {nif_error, <<"Invalid value for type Int64: expected integer">>}},
        greptimedb_rs:insert(Client, Table, [Bad])
    ),
    %% A small input against a wide server schema must retain general conversion.
    lists:foreach(
        fun(I) ->
            Name = integer_to_binary(I),
            {ok, _} = greptimedb_rs:query(
                Client, <<"ALTER TABLE bounded_insert ADD COLUMN extra", Name/binary, " INT64">>
            ),
            case I of
                5 ->
                    ?assertEqual(
                        {ok, 1}, greptimedb_rs:insert(Client, Table, bounded_rows(30000, 1))
                    );
                _ ->
                    ok
            end
        end,
        lists:seq(1, 6)
    ),
    ?assertEqual({ok, 1}, greptimedb_rs:insert(Client, Table, bounded_rows(30001, 1))),
    ?assertEqual(
        {ok, [[30001, <<"value">>], [30002, <<"value">>]]},
        greptimedb_rs:query(
            Client,
            <<"SELECT reading, payload FROM bounded_insert ",
                "WHERE reading >= 30000 ORDER BY reading">>
        )
    ),
    counts({1, 0}).

t_stream_bounds(Config) ->
    Client = client(?config(server, Config), 1, 1, 10000, true),
    Table = <<"bounded_stream">>,
    {ok, _} = greptimedb_rs:query(
        Client,
        <<"CREATE TABLE bounded_stream (ts TIMESTAMP TIME INDEX, reading INT64, payload STRING)">>
    ),
    {ok, Stream} = greptimedb_rs:stream_start(Client, Table, row()),
    Total = lists:foldl(
        fun(N, Offset) ->
            ok = greptimedb_rs:stream_write(Stream, bounded_rows(Offset, N)),
            ?assertEqual(
                ok,
                async(fun(Cb) ->
                    greptimedb_rs:stream_write_async(Stream, bounded_rows(Offset + N, N), Cb)
                end)
            ),
            Offset + 2 * N
        end,
        0,
        [1, 100, 128, 129, 512]
    ),
    %% Stream arguments contain a writer reference, so only row binaries count.
    BaseBytes = byte_size(<<"reading">>) + byte_size(<<"payload">>),
    lists:foreach(
        fun(Extra) ->
            Size = 16384 - BaseBytes + Extra,
            Bits = <<0:1, (binary:copy(<<"x">>, Size))/binary, 0:7>>,
            <<_:1, Payload:Size/binary, _:7>> = Bits,
            I = 21000 + Extra,
            Row = #{
                timestamp => 1700000000000 + I,
                fields => #{<<"reading">> => I, <<"payload">> => Payload}
            },
            ok = greptimedb_rs:stream_write(Stream, [Row])
        end,
        [0, 1]
    ),
    ok = greptimedb_rs:stream_close(Stream),
    ?assertEqual(
        {ok, [[Total, Total * (Total + 1) div 2]]},
        greptimedb_rs:query(
            Client,
            <<"SELECT count(*), sum(reading) FROM bounded_stream WHERE reading < 20000">>
        )
    ),
    ?assertEqual(
        {ok, [[21000, 16384 - BaseBytes], [21001, 16385 - BaseBytes]]},
        greptimedb_rs:query(
            Client,
            <<"SELECT reading, length(payload) FROM bounded_stream ",
                "WHERE reading >= 21000 ORDER BY reading">>
        )
    ),
    BadRow = #{
        timestamp => 1700000040000,
        fields => #{<<"reading">> => 40000, <<"payload">> => <<1:1>>}
    },
    ExpectedError = {error, {nif_error, <<"Invalid value for type String">>}},
    {ok, BadStream} = greptimedb_rs:stream_start(Client, Table, row()),
    ?assertEqual(ExpectedError, greptimedb_rs:stream_write(BadStream, [BadRow])),
    {ok, AsyncBadStream} = greptimedb_rs:stream_start(Client, Table, row()),
    ?assertEqual(
        ExpectedError,
        async(fun(Cb) ->
            greptimedb_rs:stream_write_async(AsyncBadStream, [BadRow], Cb)
        end)
    ),
    %% Reopen against schemas on both sides of the inline-conversion limit.
    lists:foreach(
        fun(I) ->
            Name = integer_to_binary(I),
            {ok, _} = greptimedb_rs:query(
                Client,
                <<"ALTER TABLE bounded_stream ADD COLUMN extra", Name/binary, " INT64">>
            )
        end,
        lists:seq(1, 5)
    ),
    {ok, EightColumns} = greptimedb_rs:stream_start(Client, Table, row()),
    ok = greptimedb_rs:stream_write(EightColumns, bounded_rows(30000, 1)),
    ok = greptimedb_rs:stream_close(EightColumns),
    {ok, _} = greptimedb_rs:query(
        Client,
        <<"ALTER TABLE bounded_stream ADD COLUMN extra6 INT64">>
    ),
    {ok, NineColumns} = greptimedb_rs:stream_start(Client, Table, row()),
    ok = greptimedb_rs:stream_write(NineColumns, bounded_rows(30001, 1)),
    ok = greptimedb_rs:stream_close(NineColumns),
    ?assertEqual(
        {ok, [[30001, <<"value">>], [30002, <<"value">>]]},
        greptimedb_rs:query(
            Client,
            <<"SELECT reading, payload FROM bounded_stream ",
                "WHERE reading >= 30000 ORDER BY reading">>
        )
    ),
    counts({1, 0}).

bounded_rows(Offset, N) ->
    [
        #{
            timestamp => 1700000000000 + I,
            fields => #{
                <<"reading">> => I, <<"payload">> => <<"value">>
            }
        }
     || I <- lists:seq(Offset + 1, Offset + N)
    ].

%% Hold initial response headers to exercise the SDK's transport timeout.
t_request_timeout(Config) ->
    Server = ?config(server, Config),
    QueryClient = client(Server, 1, 1, 20000, false),
    InsertClient = client(Server, 2, 1, 20000, false),
    Table = <<"regression_long_timeout">>,
    {ok, _} = greptimedb_rs:query(
        QueryClient,
        <<"CREATE TABLE regression_long_timeout (ts TIMESTAMP TIME INDEX, reading INT64)">>
    ),
    ok = greptimedb_test_server:pause_headers(Server),
    Query = start_request(fun() -> invoke(query, QueryClient) end),
    Insert = start_request(fun() ->
        async(fun(Cb) -> greptimedb_rs:insert_async(InsertClient, Table, [row()], Cb) end)
    end),
    wait(fun() -> greptimedb_test_server:headers(Server) >= 2 end, 1000),
    receive
        {Ref, Result, _} -> ct:fail({premature_result, Ref, Result})
    after 11000 -> ok
    end,
    ok = greptimedb_test_server:release(Server),
    {{ok, [[1]]}, _} = result(Query, 5000),
    {{ok, 1}, _} = result(Insert, 5000),
    counts({1, 0}),
    ?assertEqual(
        {ok, [[7]]}, greptimedb_rs:query(QueryClient, <<"SELECT reading FROM ", Table/binary>>)
    ),
    lists:foreach(fun no_result/1, [Query, Insert]).

t_stream_response_drain(Config) ->
    Server = ?config(server, Config),
    Client = client(Server, 1, 2, 10000, true),
    Tables = [<<"response_drain_a">>, <<"response_drain_b">>],
    Streams = [response_stream(Client, Table) || Table <- Tables],
    Tag = make_ref(),
    Parent = self(),
    lists:foreach(
        fun(I) ->
            lists:foreach(
                fun(Stream) ->
                    response_write(Stream, I, Parent, Tag)
                end,
                Streams
            )
        end,
        lists:seq(1, 4096)
    ),
    lists:foreach(fun(Stream) -> ok = greptimedb_rs:stream_close(Stream) end, Streams),
    lists:foreach(
        fun(Table) ->
            ?assertEqual(
                {ok, [[4096, 8390656]]},
                greptimedb_rs:query(Client, <<"SELECT count(*), sum(reading) FROM ", Table/binary>>)
            )
        end,
        Tables
    ),
    receive
        {Tag, _, Result} -> ct:fail({duplicate_callback, Result})
    after 200 -> ok
    end,
    counts({1, 0}).

response_write(Stream, I, Parent, Tag) when I rem 2 =:= 0 ->
    {ok, _} = greptimedb_rs:stream_write_async(
        Stream,
        [response_row(I)],
        {fun(Result) -> Parent ! {Tag, I, Result} end, []}
    ),
    receive
        {Tag, I, ok} -> ok;
        {Tag, Other, Result} -> ct:fail({unexpected_callback, Other, Result})
    after 5000 -> ct:fail(callback_timeout)
    end;
response_write(Stream, I, _Parent, _Tag) ->
    ok = greptimedb_rs:stream_write(Stream, [response_row(I)]).

t_stream_response_error(Config) ->
    Client = client(?config(server, Config), 1, 1, 5000, true),
    Table = <<"response_error">>,
    Stream = response_stream(Client, Table),
    warm_responses(Stream),
    wait(
        fun() ->
            greptimedb_rs:query(Client, <<"SELECT count(*) FROM ", Table/binary>>) =:= {ok, [[512]]}
        end,
        5000
    ),
    %% The active stream still refers to the dropped table's regions.
    {ok, _} = greptimedb_rs:query(Client, <<"DROP TABLE ", Table/binary>>),
    Result = response_burst(Stream),
    ?assertMatch({error, _}, Result),
    ?assertNotEqual({error, timeout}, Result),
    {ok, _} = greptimedb_rs:query(
        Client,
        <<"CREATE TABLE ", Table/binary, " (ts TIMESTAMP TIME INDEX, reading INT64 NOT NULL)">>
    ),
    recover_stream(Client, Table).

t_stream_response_faults(Config) ->
    Server = ?config(server, Config),
    lists:foreach(
        fun(Fault) ->
            Client = client(Server, 1, 1, 1000, false),
            Table = atom_to_binary(Fault),
            Stream = response_stream(Client, Table),
            warm_responses(Stream),
            ok = greptimedb_test_server:pause(Server),
            Request = start_request(fun() -> response_burst(Stream) end),
            wait(fun() -> greptimedb_test_server:headers(Server) > 0 end, 1000),
            case Fault of
                response_timeout -> ok;
                response_disconnect -> ok = greptimedb_test_server:disconnect(Server);
                response_restart -> ok = greptimedb_test_server:restart(Server)
            end,
            {Result, _} = result(Request, 3000),
            ?assertMatch({error, _}, Result),
            case Fault of
                response_timeout -> ?assertEqual({error, timeout}, Result);
                _ -> ok
            end,
            counts({1, 0}),
            ok = greptimedb_test_server:release(Server),
            wait_connected(Client),
            recover_stream(Client, Table),
            no_result(Request),
            kill_callers([Request]),
            ok = greptimedb_rs:stop_client(Client)
        end,
        [response_timeout, response_disconnect, response_restart]
    ).

response_stream(Client, Table) ->
    {ok, _} = greptimedb_rs:query(
        Client,
        <<"CREATE TABLE ", Table/binary, " (ts TIMESTAMP TIME INDEX, reading INT64 NOT NULL)">>
    ),
    {ok, Stream} = greptimedb_rs:stream_start(Client, Table, row()),
    Stream.

response_row(I) ->
    #{timestamp => 1700000000000 + I, fields => #{<<"reading">> => I}}.

warm_responses(Stream) ->
    lists:foreach(
        fun(I) -> ok = greptimedb_rs:stream_write(Stream, [response_row(I)]) end,
        lists:seq(1, 512)
    ).

response_burst(Stream) ->
    response_burst(Stream, lists:seq(10001, 10008)).

response_burst(_Stream, []) ->
    ct:fail(expected_stream_failure);
response_burst(Stream, [I | Rest]) ->
    case greptimedb_rs:stream_write(Stream, [response_row(I)]) of
        ok -> response_burst(Stream, Rest);
        Error -> Error
    end.

recover_stream(Client, Table) ->
    {ok, Stream} = greptimedb_rs:stream_start(Client, Table, row()),
    ok = greptimedb_rs:stream_write(Stream, [response_row(20000)]),
    ok = greptimedb_rs:stream_close(Stream),
    ?assertEqual(
        {ok, [[20000]]},
        greptimedb_rs:query(
            Client, <<"SELECT reading FROM ", Table/binary, " WHERE reading = 20000">>
        )
    ).

t_request_timeout_expiry(Config) ->
    Server = ?config(server, Config),
    Clients = [client(Server, I, 1, 12000, false) || I <- [1, 2]],
    ok = greptimedb_test_server:pause_headers(Server),
    Requests = [
        start_request(fun() -> invoke(Mode, Client) end)
     || {Mode, Client} <- lists:zip([query, query_async], Clients)
    ],
    lists:foreach(
        fun(Request) ->
            {{error, timeout}, Elapsed} = result(Request, 14000),
            ?assert(Elapsed >= 11500000 andalso Elapsed =< 14000000)
        end,
        Requests
    ),
    counts({1, 0}),
    ok = greptimedb_test_server:release(Server),
    lists:foreach(
        fun(Client) -> ?assertEqual({ok, [[1]]}, greptimedb_rs:query(Client, <<"SELECT 1">>)) end,
        Clients
    ),
    lists:foreach(fun no_result/1, Requests).

%% All SDK waits must respect a stream timeout longer than its former 30s limit.
t_stream_timeout(Config) ->
    Server = ?config(server, Config),
    Clients = [client(Server, Name, 1, 40000, false) || Name <- [1, 2, healthy]],
    Tables = [<<"stream_handshake">>, <<"stream_backpressure">>, <<"stream_finish">>],
    Streams = lists:zipwith(
        fun(Client, Table) ->
            {ok, _} = greptimedb_rs:query(
                Client,
                <<"CREATE TABLE ", Table/binary, " (ts TIMESTAMP TIME INDEX, reading INT64)">>
            ),
            {ok, Stream} = greptimedb_rs:stream_start(Client, Table, row()),
            Stream
        end,
        Clients,
        Tables
    ),
    [Handshake, Backpressure, Finish] = Streams,
    lists:foreach(fun(Stream) -> ok = greptimedb_rs:stream_write(Stream, [row()]) end, [
        Backpressure, Finish
    ]),
    ok = greptimedb_test_server:pause(Server),
    ok = greptimedb_rs:stream_write(Finish, [(row())#{timestamp := 1700000000001}]),
    Requests = [
        start_request(fun() ->
            async(fun(Cb) -> greptimedb_rs:stream_write_async(Handshake, [row()], Cb) end, 40000)
        end),
        start_request(fun() ->
            lists:foreach(
                fun(I) ->
                    ok = greptimedb_rs:stream_write(Backpressure, [
                        (row())#{timestamp := 1700000000000 + I}
                    ])
                end,
                lists:seq(1, 8)
            )
        end),
        start_request(fun() -> greptimedb_rs:stream_close(Finish) end)
    ],
    wait(fun() -> greptimedb_rs_nif:resource_counts() =:= {1, 3} end, 2000),
    receive
        {Ref, Result, _} -> ct:fail({premature_result, Ref, Result})
    after 31000 -> ok
    end,
    ok = greptimedb_test_server:release(Server),
    lists:foreach(fun(Request) -> {ok, _} = result(Request, 5000) end, Requests),
    lists:foreach(fun(Stream) -> ok = greptimedb_rs:stream_close(Stream) end, Streams),
    lists:foreach(
        fun({Client, Table, Count}) ->
            ?assertEqual(
                {ok, [[Count]]},
                greptimedb_rs:query(Client, <<"SELECT count(*) FROM ", Table/binary>>)
            )
        end,
        lists:zip3(Clients, Tables, [1, 9, 2])
    ),
    counts({1, 0}),
    lists:foreach(fun no_result/1, Requests).

%% Rejected native submissions must not release another request's client slot.
t_submit_rejection(Config) ->
    Server = ?config(server, Config),
    Client = client(Server, 1, 1, 10000, false),
    Healthy = client(Server, 2, 1, 10000, true),
    [{_, Worker}] = ecpool:workers(maps:get(pool_name, Client)),
    {ok, Conn} = ecpool_worker:client(Worker),
    NativeClient = element(2, sys:get_state(Conn)),
    Deadline = erlang:monotonic_time(microsecond) + 10000000,
    Token = make_ref(),
    ok = greptimedb_test_server:pause(Server),
    {ok, Operation} = greptimedb_rs_nif:submit(
        NativeClient, execute, [<<"SELECT 1">>], Token, Deadline
    ),
    wait(fun() -> greptimedb_test_server:headers(Server) >= 1 end, 1000),
    %% A second rejection checks that cleaning up the first kept the occupied slot.
    lists:foreach(
        fun(_) ->
            ?assertEqual(
                {error, busy},
                greptimedb_rs_nif:submit(
                    NativeClient, execute, [<<"SELECT 1">>], make_ref(), Deadline
                )
            ),
            ?assert(greptimedb_rs_nif:operation_active(Operation)),
            counts({1, 1})
        end,
        [first, second]
    ),
    ok = greptimedb_rs_nif:cancel(Operation),
    receive
        {greptimedb_nif, Token, {error, cancelled}} -> ok
    after 1000 -> error(missing_cancelled_reply)
    end,
    counts({1, 0}),
    ok = greptimedb_test_server:release(Server),
    ?assertEqual({ok, [[1]]}, greptimedb_rs:query(Client, <<"SELECT 1">>)),
    ok = greptimedb_rs_nif:disconnect(NativeClient),
    ?assertEqual(
        {error, closed},
        greptimedb_rs_nif:submit(NativeClient, execute, [<<"SELECT 1">>], make_ref(), Deadline)
    ),
    counts({1, 0}),
    ?assertEqual({ok, [[1]]}, greptimedb_rs:query(Healthy, <<"SELECT 1">>)).

%% Closing must release the writer even when the native close never starts.
t_queued_close_timeout(Config) ->
    queued_close(timeout, Config).

t_queued_close_cancel(Config) ->
    queued_close(cancel, Config).

queued_close(Mode, Config) ->
    Server = ?config(server, Config),
    {Client, Conn, Stream} = close_client(Server),
    ok = greptimedb_test_server:pause_headers(Server),
    Query = start_request(fun() -> greptimedb_rs:query(Client, <<"SELECT 1">>) end),
    wait(fun() -> greptimedb_test_server:headers(Server) >= 1 end, 1000),
    Opts = maps:get(conn_opts, Client),
    Timeout =
        case Mode of
            timeout -> 200;
            cancel -> 10000
        end,
    CloseClient = Client#{conn_opts := Opts#{stream_timeout := Timeout}},
    Close = start_request(fun() ->
        greptimedb_rs:stream_close({stream_client, CloseClient, <<"regression_close">>})
    end),
    wait(fun() -> map_size(element(8, sys:get_state(Conn))) =:= 2 end, 1000),
    case Mode of
        timeout -> {ok, _} = result(Close, 1000);
        cancel -> kill_callers([Close])
    end,
    wait(fun() -> map_size(element(8, sys:get_state(Conn))) =:= 1 end, 1000),
    ?assertEqual(#{}, element(4, sys:get_state(Conn))),
    ok = greptimedb_test_server:release(Server),
    {{ok, [[1]]}, _} = result(Query, 1000),
    ?assertEqual({error, no_writer}, greptimedb_rs:stream_write(Stream, [row()])),
    {ok, Fresh} = greptimedb_rs:stream_start(Client, <<"regression_close">>, row()),
    ?assertEqual(ok, greptimedb_rs:stream_write(Fresh, [row()])),
    ?assertEqual(ok, greptimedb_rs:stream_close(Fresh)),
    counts({1, 0}).

t_rejected_close(Config) ->
    Server = ?config(server, Config),
    {Client, Conn, Stream} = close_client(Server),
    NativeClient = element(2, sys:get_state(Conn)),
    Token = make_ref(),
    ok = greptimedb_test_server:pause_headers(Server),
    {ok, Operation} = greptimedb_rs_nif:submit(
        NativeClient,
        execute,
        [<<"SELECT 1">>],
        Token,
        erlang:monotonic_time(microsecond) + 10000000
    ),
    wait(fun() -> greptimedb_test_server:headers(Server) >= 1 end, 1000),
    ?assertEqual(ok, greptimedb_rs:stream_close(Stream)),
    ?assertEqual(#{}, element(4, sys:get_state(Conn))),
    ?assert(greptimedb_rs_nif:operation_active(Operation)),
    ok = greptimedb_rs_nif:cancel(Operation),
    receive
        {greptimedb_nif, Token, {error, cancelled}} -> ok
    after 1000 -> error(missing_cancelled_reply)
    end,
    ok = greptimedb_test_server:release(Server),
    ?assertEqual({error, no_writer}, greptimedb_rs:stream_write(Stream, [row()])),
    ?assertEqual({ok, [[1]]}, greptimedb_rs:query(Client, <<"SELECT 1">>)),
    counts({1, 0}).

close_client(Server) ->
    Client = client(Server, 1, 1, 10000, false),
    {ok, _} = greptimedb_rs:query(Client, <<"DROP TABLE IF EXISTS regression_close">>),
    {ok, _} = greptimedb_rs:query(
        Client, <<"CREATE TABLE regression_close (ts TIMESTAMP TIME INDEX, reading INT64)">>
    ),
    {ok, Stream} = greptimedb_rs:stream_start(Client, <<"regression_close">>, row()),
    [{_, Worker}] = ecpool:workers(maps:get(pool_name, Client)),
    {ok, Conn} = ecpool_worker:client(Worker),
    ClientState = sys:get_state(Conn),
    ?assertEqual(1, map_size(element(4, ClientState))),
    {Client, Conn, Stream}.

%% A start submitted before close must not publish a writer after close expires.
t_close_during_start(Config) ->
    close_during_start(active, Config).

t_close_after_queued_start(Config) ->
    close_during_start(queued, Config).

close_during_start(Mode, Config) ->
    Server = ?config(server, Config),
    {Client, Conn, Stream} = close_client(Server),
    ok = greptimedb_test_server:pause_headers(Server),
    Query =
        case Mode of
            active ->
                undefined;
            queued ->
                Request = start_request(fun() -> greptimedb_rs:query(Client, <<"SELECT 1">>) end),
                wait(fun() -> map_size(element(8, sys:get_state(Conn))) =:= 1 end, 1000),
                Request
        end,
    Start = start_request(fun() ->
        greptimedb_rs:stream_start(Client, <<"regression_close">>, row())
    end),
    Count =
        case Mode of
            active -> 1;
            queued -> 2
        end,
    wait(fun() -> map_size(element(8, sys:get_state(Conn))) =:= Count end, 1000),
    Opts = maps:get(conn_opts, Client),
    Short = Client#{conn_opts := Opts#{stream_timeout := 200}},
    ?assertEqual(
        ok,
        greptimedb_rs:stream_close(
            {stream_client, Short, <<"regression_close">>}
        )
    ),
    ok = greptimedb_test_server:release(Server),
    case Query of
        undefined -> ok;
        _ -> {{ok, [[1]]}, _} = result(Query, 1000)
    end,
    {{ok, _}, _} = result(Start, 2000),
    ?assertEqual(#{}, element(4, sys:get_state(Conn))),
    ?assertEqual({error, no_writer}, greptimedb_rs:stream_write(Stream, [row()])),
    {ok, Fresh} = greptimedb_rs:stream_start(Client, <<"regression_close">>, row()),
    ?assertEqual(ok, greptimedb_rs:stream_write(Fresh, [row()])),
    ?assertEqual(ok, greptimedb_rs:stream_close(Fresh)),
    counts({1, 0}).

%% Stopping a pool must finish batches already accepted by its stream writers.
t_pool_stop_flush(Config) ->
    pool_stop_flush(idle, Config).

t_pool_stop_flush_busy(Config) ->
    pool_stop_flush(busy, Config).

pool_stop_flush(Mode, Config) ->
    Server = ?config(server, Config),
    Client = client(Server, 1, 1, 10000, false),
    Healthy = client(Server, healthy, 1, 10000, true),
    Tables = [
        iolist_to_binary(["regression_stop_", atom_to_binary(Mode), Suffix])
     || Suffix <- ["_a", "_b"]
    ],
    Rows = [(row())#{timestamp := 1700000000000 + I} || I <- lists:seq(1, 200000)],
    lists:foreach(
        fun(Table) ->
            {ok, _} = greptimedb_rs:query(
                Client,
                <<"CREATE TABLE ", Table/binary, " (ts TIMESTAMP TIME INDEX, reading INT64)">>
            ),
            {ok, Stream} = greptimedb_rs:stream_start(Client, Table, row()),
            ok = greptimedb_rs:stream_write(Stream, Rows)
        end,
        Tables
    ),
    Query =
        case Mode of
            idle ->
                undefined;
            busy ->
                ok = greptimedb_test_server:pause_headers(Server),
                Request = start_request(fun() -> greptimedb_rs:query(Client, <<"SELECT 1">>) end),
                wait(fun() -> greptimedb_test_server:headers(Server) >= 1 end, 1000),
                Request
        end,
    ?assertEqual(ok, greptimedb_rs:stop_client(maps:get(pool_name, Client))),
    case Query of
        undefined -> ok;
        _ -> {{error, closed}, _} = result(Query, 1000)
    end,
    ok = greptimedb_test_server:release(Server),
    lists:foreach(
        fun(Table) ->
            ?assertEqual(
                {ok, [[200000]]},
                greptimedb_rs:query(
                    Healthy, <<"SELECT count(*) FROM ", Table/binary>>
                )
            )
        end,
        Tables
    ),
    counts({1, 0}).

%% An unresponsive stream must not prevent pool shutdown or occupy dirty I/O.
t_pool_stop_flush_timeout(Config) ->
    pool_stop_flush_fault(timeout, Config).

t_pool_stop_flush_killed(Config) ->
    pool_stop_flush_fault(killed, Config).

pool_stop_flush_fault(Mode, Config) ->
    Server = ?config(server, Config),
    Timeout =
        case Mode of
            timeout -> 200;
            killed -> 10000
        end,
    Client = client(Server, 1, 1, Timeout, false),
    Healthy = client(Server, healthy, 1, 10000, true),
    {ok, _} = greptimedb_rs:query(Client, <<"DROP TABLE IF EXISTS regression_stop_timeout">>),
    {ok, _} = greptimedb_rs:query(
        Client,
        <<"CREATE TABLE regression_stop_timeout (ts TIMESTAMP TIME INDEX, reading INT64)">>
    ),
    {ok, Stream} = greptimedb_rs:stream_start(Client, <<"regression_stop_timeout">>, row()),
    ok = greptimedb_rs:stream_write(Stream, [row()]),
    wait(
        fun() ->
            greptimedb_rs:query(Healthy, <<"SELECT reading FROM regression_stop_timeout">>) =:=
                {ok, [[7]]}
        end,
        2000
    ),
    ok = greptimedb_test_server:pause(Server),
    ok = greptimedb_rs:stream_write(Stream, [(row())#{timestamp := 1700000000001}]),
    Stop = start_request(fun() -> greptimedb_rs:stop_client(Client) end),
    wait(fun() -> greptimedb_rs_nif:resource_counts() =:= {1, 1} end, 1000),
    File = filename:join(?config(priv_dir, Config), "stop-dirty-io.data"),
    Io = start_request(fun() -> file:write_file(File, <<"stop">>) end),
    {ok, IoElapsed} = result(Io, 1000),
    ?assert(IoElapsed < 1000000),
    ?assertEqual({ok, [[1]]}, greptimedb_rs:query(Healthy, <<"SELECT 1">>)),
    {ok, Elapsed} = result(Stop, 1000),
    ?assert(Elapsed >= 150000 andalso Elapsed < 1000000),
    counts({1, 0}),
    ok = greptimedb_test_server:release(Server).

%% Application shutdown must finish idle writers before dropping the shared runtime.
t_library_stop_flush(Config) ->
    Server = ?config(server, Config),
    Client = client(Server, 1, 1, 10000, true),
    {ok, _} = greptimedb_rs:query(
        Client,
        <<"CREATE TABLE regression_app_flush (ts TIMESTAMP TIME INDEX, reading INT64)">>
    ),
    {ok, Stream} = greptimedb_rs:stream_start(Client, <<"regression_app_flush">>, row()),
    Rows = [(row())#{timestamp := 1700000000000 + I} || I <- lists:seq(1, 200000)],
    ok = greptimedb_rs:stream_write(Stream, Rows),
    ok = application:stop(greptimedb_rs),
    counts({0, 0}),
    ok = greptimedb_rs:stop_client(Client),
    {ok, _} = application:ensure_all_started(greptimedb_rs),
    Reader = client(Server, healthy, 1, 10000, true),
    ?assertEqual(
        {ok, [[200000]]},
        greptimedb_rs:query(
            Reader, <<"SELECT count(*) FROM regression_app_flush">>
        )
    ).

t_library_stop_flush_timeout(Config) ->
    library_stop_fault(timeout, Config).

t_owner_exit_no_flush(Config) ->
    library_stop_fault(killed, Config).

t_owner_error_no_flush(Config) ->
    library_stop_fault(error, Config).

library_stop_fault(Mode, Config) ->
    Server = ?config(server, Config),
    Client = client(Server, 1, 1, 10000, false),
    Healthy = client(Server, healthy, 1, 10000, true),
    Tables = [
        <<"regression_app_fault_a">>, <<"regression_app_fault_b">>, <<"regression_app_fault_c">>
    ],
    Streams = lists:map(
        fun(Table) ->
            {ok, _} = greptimedb_rs:query(Client, <<"DROP TABLE IF EXISTS ", Table/binary>>),
            {ok, _} = greptimedb_rs:query(
                Client,
                <<"CREATE TABLE ", Table/binary, " (ts TIMESTAMP TIME INDEX, reading INT64)">>
            ),
            {ok, Stream} = greptimedb_rs:stream_start(Client, Table, row()),
            ok = greptimedb_rs:stream_write(Stream, [row()]),
            wait(
                fun() ->
                    greptimedb_rs:query(Healthy, <<"SELECT reading FROM ", Table/binary>>) =:=
                        {ok, [[7]]}
                end,
                2000
            ),
            Stream
        end,
        Tables
    ),
    ok = greptimedb_test_server:pause(Server),
    lists:foreach(
        fun(Stream) ->
            ok = greptimedb_rs:stream_write(Stream, [(row())#{timestamp := 1700000000001}])
        end,
        Streams
    ),
    Owner = whereis(greptimedb_rs_runtime),
    {Elapsed, ok} = timer:tc(fun() ->
        case Mode of
            timeout ->
                ok = application:stop(greptimedb_rs),
                counts({0, 0});
            _ ->
                case Mode of
                    killed -> exit(Owner, kill);
                    error -> gen_server:stop(Owner, owner_crash, infinity)
                end,
                wait(
                    fun() ->
                        New = whereis(greptimedb_rs_runtime),
                        is_pid(New) andalso New =/= Owner andalso
                            greptimedb_rs_nif:resource_counts() =:= {1, 0}
                    end,
                    1000
                )
        end
    end),
    ?assert(Elapsed < 1000000),
    case Mode of
        timeout -> ?assert(Elapsed >= 150000 andalso Elapsed < 500000);
        _ -> ok
    end,
    ok = greptimedb_test_server:release(Server).

t_library_stop_active_close(Config) ->
    library_stop_active(close, Config).

t_library_stop_active_write(Config) ->
    library_stop_active(write, Config).

%% In-flight stream work must not discard older batches during graceful shutdown.
library_stop_active(Mode, Config) ->
    Server = ?config(server, Config),
    Client = client(Server, 1, 1, 10000, false),
    Healthy = client(Server, healthy, 1, 10000, true),
    Table = iolist_to_binary(["regression_app_active_", atom_to_binary(Mode)]),
    {ok, _} = greptimedb_rs:query(
        Client,
        <<"CREATE TABLE ", Table/binary, " (ts TIMESTAMP TIME INDEX, reading INT64)">>
    ),
    {ok, Stream} = greptimedb_rs:stream_start(Client, Table, row()),
    ok = greptimedb_rs:stream_write(Stream, [row()]),
    wait(
        fun() ->
            greptimedb_rs:query(Healthy, <<"SELECT count(*) FROM ", Table/binary>>) =:=
                {ok, [[1]]}
        end,
        2000
    ),
    ok = greptimedb_test_server:pause(Server),
    ok = greptimedb_rs:stream_write(Stream, [(row())#{timestamp := 1700000000001}]),
    Active =
        case Mode of
            close ->
                start_request(fun() -> greptimedb_rs:stream_close(Stream) end);
            write ->
                Rows = lists:duplicate(600000, (row())#{timestamp := 1700000000002}),
                start_request(fun() -> greptimedb_rs:stream_write(Stream, Rows) end)
        end,
    wait(
        fun() ->
            case Mode of
                close -> greptimedb_rs_nif:resource_counts() =:= {1, 1};
                write -> element(3, greptimedb_rs_nif:codec_counts()) > 0
            end
        end,
        2000
    ),
    Stop = start_request(fun() -> application:stop(greptimedb_rs) end),
    no_result(Stop),
    ok = greptimedb_test_server:release(Server),
    {ok, _} = result(Stop, 5000),
    {Reply, _} = result(Active, 1000),
    ?assert(lists:member(Reply, [ok, {error, closed}])),
    counts({0, 0}),
    ok = greptimedb_rs:stop_client(Client),
    ok = greptimedb_rs:stop_client(Healthy),
    {ok, _} = application:ensure_all_started(greptimedb_rs),
    Reader = client(Server, healthy, 1, 10000, true),
    {ok, [[Count]]} = greptimedb_rs:query(Reader, <<"SELECT count(*) FROM ", Table/binary>>),
    ?assert(Count >= 2).
