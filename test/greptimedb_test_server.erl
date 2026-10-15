-module(greptimedb_test_server).
-behaviour(gen_server).
-export([run_beam/4]).
-export([
    start/2,
    stop/1,
    endpoint/1,
    backend/1,
    pause/1,
    pause_headers/1,
    release/1,
    disconnect/1,
    headers/1,
    restart/1
]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

run_beam(Module, Function, Argument, LogPath) ->
    Eval = lists:flatten(io_lib:format("~p:~p(~p).", [Module, Function, Argument])),
    Args =
        ["+S", "1:1", "+SDcpu", "1:1", "+SDio", "1", "-noshell", "-pa"] ++
            [filename:absname(Path) || Path <- code:get_path()] ++ ["-eval", Eval],
    {ok, Log} = file:open(LogPath, [write]),
    Port = open_port(
        {spawn_executable, os:find_executable("erl")},
        [binary, exit_status, stderr_to_stdout, {args, Args}]
    ),
    {os_pid, Pid} = erlang:port_info(Port, os_pid),
    try
        beam_result(Port, Log, erlang:monotonic_time(millisecond) + 30000)
    after
        stop_backend(#{backend_port => Port, backend_pid => Pid, log => Log}),
        file:close(Log)
    end.

beam_result(Port, Log, Deadline) ->
    receive
        {Port, {data, Data}} ->
            file:write(Log, Data),
            beam_result(Port, Log, Deadline);
        {Port, {exit_status, 0}} ->
            ok;
        {Port, {exit_status, 77}} ->
            {skip, "OS thread limits are not enforced"};
        {Port, {exit_status, Status}} ->
            error({beam_exit, Status})
    after max(0, Deadline - erlang:monotonic_time(millisecond)) -> error(beam_timeout)
    end.

start(Binary, Directory) ->
    gen_server:start(?MODULE, [Binary, Directory], []).

stop(Server) ->
    gen_server:stop(Server, normal, 15000).

endpoint(Server) ->
    gen_server:call(Server, endpoint).

backend(Server) ->
    gen_server:call(Server, backend).

pause(Server) ->
    gen_server:call(Server, pause).

pause_headers(Server) ->
    gen_server:call(Server, pause_headers).

release(Server) ->
    gen_server:call(Server, release).

disconnect(Server) ->
    gen_server:call(Server, disconnect).

headers(Server) ->
    gen_server:call(Server, headers).

restart(Server) ->
    gen_server:call(Server, restart, 40000).

init([Binary, Directory]) ->
    process_flag(trap_exit, true),
    {RpcListener, RpcPort} = listen(),
    {HttpListener, HttpPort} = listen(),
    gen_tcp:close(RpcListener),
    gen_tcp:close(HttpListener),
    Config = filename:join(Directory, "greptime.toml"),
    ok = file:write_file(
        Config,
        io_lib:format(
            "[grpc]\nbind_addr = \"127.0.0.1:~B\"\nruntime_size = 4\n"
            "[http]\naddr = \"127.0.0.1:~B\"\n"
            "[mysql]\nenable = false\n[postgres]\nenable = false\n",
            [RpcPort, HttpPort]
        )
    ),
    {ok, Log} = file:open(filename:join(Directory, "greptime.log"), [write]),
    Args = [
        "standalone",
        "start",
        "-c",
        Config,
        "--data-home",
        filename:join(Directory, "data"),
        "--log-dir",
        filename:join(Directory, "logs"),
        "--log-level",
        "warn"
    ],
    State = #{
        binary => Binary,
        args => Args,
        log => Log,
        rpc => RpcPort,
        http => HttpPort,
        sockets => [],
        held => [],
        headers => #{},
        paused => none
    },
    Started = start_backend(State),
    {Listener, Port} = listen(),
    Server = self(),
    Acceptor = spawn_link(fun() -> accept(Listener, RpcPort, Server) end),
    {ok, Started#{listener => Listener, port => Port, acceptor => Acceptor}}.

handle_call(endpoint, _, State) ->
    {reply, address(maps:get(port, State)), State};
handle_call(backend, _, State) ->
    {reply, address(maps:get(rpc, State)), State};
handle_call(pause, _, State) ->
    {reply, ok, State#{paused := body, headers := #{}}};
handle_call(pause_headers, _, State) ->
    {reply, ok, State#{paused := headers, headers := #{}}};
handle_call(headers, _, State) ->
    {reply, map_size(maps:get(headers, State)), State};
handle_call(release, _, State) ->
    lists:foreach(
        fun({Socket, Packet}) -> gen_tcp:send(Socket, Packet) end,
        lists:reverse(maps:get(held, State))
    ),
    {reply, ok, State#{paused := none, held := []}};
handle_call(disconnect, _, State) ->
    {reply, ok, close_sockets(State)};
handle_call(restart, _, State) ->
    stop_backend(State),
    {reply, ok, start_backend(close_sockets(State))};
%% Keep connection control frames flowing while holding RPC responses.
%% Body mode forwards initial headers; headers mode holds those too.

handle_call({frame, Socket, Stream, Type, Flags, Packet}, _, #{paused := Paused} = State) when
    Paused =/= none andalso Stream =/= 0 andalso
        (Type =:= 0 orelse Type =:= 1 orelse Type =:= 9)
->
    Headers = (maps:get(headers, State))#{{Socket, Stream} => true},
    Next = State#{headers := Headers},
    case Paused =:= body andalso Type =:= 1 andalso Flags band 1 =:= 0 of
        true ->
            gen_tcp:send(Socket, Packet),
            {reply, ok, Next};
        false ->
            {reply, ok, Next#{held := [{Socket, Packet} | maps:get(held, State)]}}
    end;
handle_call({frame, Socket, _, _, _, Packet}, _, State) ->
    gen_tcp:send(Socket, Packet),
    {reply, ok, State}.

handle_cast({sockets, Client, Backend}, State) ->
    {noreply, State#{sockets := [Client, Backend | maps:get(sockets, State)]}}.

handle_info({Port, {data, Data}}, #{backend_port := Port} = State) ->
    file:write(maps:get(log, State), Data),
    {noreply, State};
handle_info({Port, {exit_status, Status}}, #{backend_port := Port} = State) ->
    {stop, {database_exit, Status}, State};
handle_info({'EXIT', Pid, Reason}, #{acceptor := Pid} = State) ->
    {stop, {acceptor_exit, Reason}, State};
handle_info(_, State) ->
    {noreply, State}.

terminate(_, State) ->
    gen_tcp:close(maps:get(listener, State)),
    exit(maps:get(acceptor, State), shutdown),
    close_sockets(State),
    stop_backend(State),
    file:close(maps:get(log, State)).

listen() ->
    {ok, Socket} = gen_tcp:listen(0, [binary, {active, false}, {ip, {127, 0, 0, 1}}]),
    {ok, {_, Port}} = inet:sockname(Socket),
    {Socket, Port}.

address(Port) ->
    iolist_to_binary(io_lib:format("127.0.0.1:~B", [Port])).

start_backend(State) ->
    Port = open_port(
        {spawn_executable, maps:get(binary, State)},
        [binary, exit_status, use_stdio, stderr_to_stdout, {args, maps:get(args, State)}]
    ),
    {os_pid, Pid} = erlang:port_info(Port, os_pid),
    try
        await_backend(Port, State, erlang:monotonic_time(millisecond) + 30000),
        State#{backend_port => Port, backend_pid => Pid}
    catch
        Class:Reason:Stack ->
            stop_backend(State#{backend_port => Port, backend_pid => Pid}),
            erlang:raise(Class, Reason, Stack)
    end.

await_backend(Port, State, Deadline) ->
    receive
        {Port, {exit_status, Status}} -> error({database_exit, Status});
        {Port, {data, Data}} -> file:write(maps:get(log, State), Data)
    after 0 -> ok
    end,
    case gen_tcp:connect("127.0.0.1", maps:get(http, State), [binary, {active, false}], 200) of
        {ok, Socket} ->
            gen_tcp:send(
                Socket, <<"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n">>
            ),
            Response = gen_tcp:recv(Socket, 0, 500),
            gen_tcp:close(Socket),
            case Response of
                {ok, <<"HTTP/1.1 200", _/binary>>} -> ok;
                _ -> retry_backend(Port, State, Deadline)
            end;
        _ ->
            retry_backend(Port, State, Deadline)
    end.

retry_backend(Port, State, Deadline) ->
    true = erlang:monotonic_time(millisecond) < Deadline,
    timer:sleep(50),
    await_backend(Port, State, Deadline).

stop_backend(#{backend_port := Port, backend_pid := Pid, log := Log}) ->
    case erlang:port_info(Port) of
        undefined ->
            ok;
        _ ->
            signal(Pid, "-TERM"),
            case wait_exit(Port, Log, 5000) of
                ok ->
                    ok;
                timeout ->
                    signal(Pid, "-KILL"),
                    wait_exit(Port, Log, 5000)
            end,
            try
                port_close(Port)
            catch
                error:badarg -> ok
            end
    end.

signal(Pid, Signal) ->
    Port = open_port(
        {spawn_executable, os:find_executable("kill")},
        [exit_status, {args, [Signal, integer_to_list(Pid)]}]
    ),
    receive
        {Port, {exit_status, _}} -> ok
    after 1000 ->
        try
            port_close(Port)
        catch
            error:badarg -> ok
        end
    end.

wait_exit(Port, Log, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    receive
        {Port, {exit_status, _}} ->
            ok;
        {Port, {data, Data}} ->
            file:write(Log, Data),
            wait_exit(Port, Log, max(0, Deadline - erlang:monotonic_time(millisecond)))
    after Timeout -> timeout
    end.

close_sockets(State) ->
    lists:foreach(fun gen_tcp:close/1, maps:get(sockets, State)),
    State#{sockets := [], held := [], paused := none}.

accept(Listener, RpcPort, Server) ->
    case gen_tcp:accept(Listener) of
        {ok, Client} ->
            case gen_tcp:connect("127.0.0.1", RpcPort, [binary, {active, false}], 1000) of
                {ok, Backend} ->
                    gen_server:cast(Server, {sockets, Client, Backend}),
                    reader(Client, Backend, request, Server),
                    reader(Backend, Client, response, Server);
                _ ->
                    gen_tcp:close(Client)
            end,
            accept(Listener, RpcPort, Server);
        {error, closed} ->
            ok
    end.

reader(Source, Destination, Direction, Server) ->
    Pid = spawn(fun() ->
        receive
            start ->
                try
                    case Direction of
                        request ->
                            Preface = recv(Source, 24),
                            <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">> = Preface,
                            ok = gen_tcp:send(Destination, Preface);
                        response ->
                            ok
                    end,
                    frames(Source, Destination, Direction, Server)
                catch
                    error:disconnected -> ok
                after
                    gen_tcp:close(Source),
                    gen_tcp:close(Destination)
                end
        after 5000 ->
            gen_tcp:close(Source),
            gen_tcp:close(Destination)
        end
    end),
    case gen_tcp:controlling_process(Source, Pid) of
        ok -> Pid ! start;
        {error, closed} -> exit(Pid, shutdown)
    end.

frames(Source, Destination, Direction, Server) ->
    case gen_tcp:recv(Source, 9) of
        {ok, Header = <<Length:24, Type:8, Flags:8, _:1, Stream:31>>} ->
            Payload =
                case Length of
                    0 ->
                        <<>>;
                    _ ->
                        recv(Source, Length)
                end,
            Packet = <<Header/binary, Payload/binary>>,
            case Direction of
                request ->
                    gen_tcp:send(Destination, Packet);
                response ->
                    gen_server:call(Server, {frame, Destination, Stream, Type, Flags, Packet})
            end,
            frames(Source, Destination, Direction, Server);
        {error, _} ->
            ok
    end.

recv(Socket, Length) ->
    case gen_tcp:recv(Socket, Length) of
        {ok, Data} -> Data;
        {error, _} -> error(disconnected)
    end.
