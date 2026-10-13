-module(greptimedb_rs_regression_SUITE).
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-compile([export_all, nowarn_export_all]).

suite() ->
    [{timetrap, {seconds, 45}}].

all() ->
    [{group, Name} || {Name, _} <- network_cases()] ++ [{group, lifecycle}, {group, cpu}].

groups() ->
    [{Name, [], [t_network]} || {Name, _} <- network_cases()] ++
        [
            {lifecycle, [], [
                t_queued, t_caller_cancel, t_pool_stop, t_socket_exit, t_library_stop, t_owner_exit
            ]},
            {cpu, [], [t_cpu_cancel, t_cpu_timeout, t_cpu_stop, t_query_cancel]}
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

init_per_group(cpu, Config) ->
    [{cpu_workers, 1} | Config];
init_per_group(Name, Config) ->
    case lists:keyfind(Name, 1, network_cases()) of
        {Name, Case} -> [{network_case, Case} | Config];
        false -> Config
    end.

end_per_group(_, _) ->
    ok.

init_per_testcase(_, Config) ->
    logger:set_primary_config(level, warning),
    application:load(greptimedb_rs),
    application:set_env(greptimedb_rs, cpu_workers, proplists:get_value(cpu_workers, Config, 2)),
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
    Parent = self(),
    Ref = make_ref(),
    {ok, _} = Fun({fun(Result) -> Parent ! {Ref, Result} end, []}),
    receive
        {Ref, Result} -> Result
    after 10000 -> ct:fail(callback_timeout)
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

t_cpu_cancel(Config) ->
    cpu(cancel, Config).

t_cpu_timeout(Config) ->
    cpu(timeout, Config).

t_cpu_stop(Config) ->
    cpu(library_stop, Config).

t_query_cancel(Config) ->
    cpu(query_cancel, Config).

cpu(Mode, Config) ->
    Server = ?config(server, Config),
    Timeout =
        case Mode of
            timeout -> 150;
            _ -> 10000
        end,
    Client = client(Server, 1, 4, Timeout, true),
    {ok, _} = greptimedb_rs:query(
        Client,
        <<"CREATE TABLE IF NOT EXISTS regression_cpu (ts TIMESTAMP TIME INDEX, reading INT64)">>
    ),
    ?assertEqual({1, 2, 0, 0, 0}, greptimedb_rs_nif:cpu_counts()),
    {ok, 0} = greptimedb_rs:insert(Client, <<"regression_cpu">>, []),
    Request =
        case Mode of
            query_cancel ->
                WarmRows = [(row())#{timestamp := 1700000000000 + I} || I <- lists:seq(1, 60000)],
                {ok, 60000} = greptimedb_rs:insert(Client, <<"regression_cpu">>, WarmRows),
                fun() ->
                    greptimedb_rs:query(
                        Client,
                        <<
                            "SELECT a.reading FROM regression_cpu a "
                            "CROSS JOIN regression_cpu b LIMIT 600000"
                        >>
                    )
                end;
            _ ->
                Rows = lists:duplicate(600000, row()),
                fun() -> greptimedb_rs:insert(Client, <<"regression_cpu">>, Rows) end
        end,
    Requests = [start_request(Request) || _ <- lists:seq(1, 4)],
    wait(
        fun() ->
            {Workers, Capacity, Active, Queued, Waiting} = greptimedb_rs_nif:cpu_counts(),
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
            {Workers, Capacity, Active, Queued, Waiting} = greptimedb_rs_nif:cpu_counts(),
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
                ?assertEqual({0, 0, 0, 0, 0}, greptimedb_rs_nif:cpu_counts()),
                ok = greptimedb_rs:stop_client(Client),
                {ok, _} = application:ensure_all_started(greptimedb_rs),
                client(Server, 1, 4, 10000, true);
            _ ->
                Client
        end,
    ok = recover(Recovered, <<"regression_cpu_recovered">>),
    ok = application:stop(greptimedb_rs),
    counts({0, 0}),
    ?assertEqual({0, 0, 0, 0, 0}, greptimedb_rs_nif:cpu_counts()).
