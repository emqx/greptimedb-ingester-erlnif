-module(greptimedb_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-compile([export_all, nowarn_export_all]).

-define(conn_opts(Config), ?config(conn_opts, Config)).
-define(database(Config), ?config(database, Config)).
-define(table(Config), ?config(table, Config)).

all() ->
    [
        {group, tcp}
    ].

groups() ->
    TCs = [
        t_connect_tcp,
        t_connect_tls,
        t_connect_with_auth,
        t_insert_test,
        t_async_insert_query_test
    ],
    [
        {tcp, TCs}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(tcp, Config) ->
    Host = get_host_addr("GREPTIMEDB_TCP_ADDR"),
    ConnOpts = #{
        endpoints => [<<Host/binary, ":4001">>],
        dbname => <<"public">>
    },
    [{conn_opts, ConnOpts} | Config].

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    Database = <<(atom_to_binary(TestCase))/binary, "_db">>,
    Table = <<(atom_to_binary(TestCase))/binary, "_table">>,
    [{database, Database}, {table, Table} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

t_connect_tcp(Config) ->
    {ok, Client} = greptimedb:start_client(?conn_opts(Config)),
    ?assert(is_pid(Client)),
    ?assertMatch({ok, [_ | _]}, greptimedb:query(Client, <<"SELECT 1">>)),
    ok = greptimedb:stop_client(Client).

t_connect_with_auth(_Config) ->
    Host = get_host_addr("GREPTIMEDB_AUTH_ADDR"),
    ConnOpts = #{
        endpoints => [<<Host/binary, ":4001">>],
        dbname => <<"public">>,
        username => <<"greptime_user">>,
        password => <<"greptime_pwd">>
    },
    {ok, Client} = greptimedb:start_client(ConnOpts),
    ?assert(is_pid(Client)),
    ?assertMatch({ok, [_ | _]}, greptimedb:query(Client, <<"SELECT 1">>)),
    ok = greptimedb:stop_client(Client).

t_connect_tls(_Config) ->
    Host = get_host_addr("GREPTIMEDB_TLS_ADDR"),
    Dir = code:lib_dir(greptimedb),
    DataDir = filename:join(Dir, "test/data/certs"),
    CaCert = filename:join(DataDir, "ca.crt"),
    ClientCert = filename:join(DataDir, "server.crt"),
    ClientKey = filename:join(DataDir, "server.key"),

    %% Ensure files exist
    ?assert(filelib:is_file(CaCert)),
    ?assert(filelib:is_file(ClientCert)),
    ?assert(filelib:is_file(ClientKey)),

    ConnOpts = #{
        endpoints => [<<Host/binary, ":4001">>],
        dbname => <<"public">>,
        tls => true,
        ca_cert => list_to_binary(CaCert),
        client_cert => list_to_binary(ClientCert),
        client_key => list_to_binary(ClientKey)
    },

    {ok, Client} = greptimedb:start_client(ConnOpts),
    ?assert(is_pid(Client)),
    ?assertMatch({ok, [_ | _]}, greptimedb:query(Client, <<"SELECT 1">>)),
    ok = greptimedb:stop_client(Client).

t_insert_test(Config) ->
    {ok, Client} = greptimedb:start_client(?conn_opts(Config)),

    Table = ?table(Config),
    Ts = erlang:system_time(millisecond),
    Rows = [
        #{
            <<"ts">> => Ts,
            <<"value">> => 1.0
        }
    ],

    ?assertMatch({ok, _}, greptimedb:write(Client, Table, Rows)),

    % Verify with query

    % Wait for eventual consistency if needed
    timer:sleep(1000),
    Sql = iolist_to_binary(io_lib:format("SELECT * FROM ~s WHERE ts = ~p", [Table, Ts])),
    ?assertMatch({ok, [_ | _]}, greptimedb:query(Client, Sql)),

    ok = greptimedb:stop_client(Client).

t_async_insert_query_test(Config) ->
    {ok, Client} = greptimedb:start_client(?conn_opts(Config)),
    Table = ?table(Config),
    Ts = erlang:system_time(millisecond),
    Rows = [
        #{
            <<"ts">> => Ts,
            <<"async_val">> => 100
        }
    ],

    Self = self(),
    Callback = {fun erlang:send/2, [Self]},

    ok = greptimedb:async_write(Client, Table, Rows, Callback),

    receive
        {ok, _} -> ok;
        {error, Reason} -> ct:fail({async_write_failed, Reason})
    after 5000 ->
        ct:fail(async_write_timeout)
    end,

    timer:sleep(1000),

    Sql = iolist_to_binary(io_lib:format("SELECT * FROM ~s WHERE ts = ~p", [Table, Ts])),
    ok = greptimedb:async_query(Client, Sql, Callback),

    receive
        {ok, Result} ->
            ?assertMatch([_ | _], Result);
        {error, Reason2} ->
            ct:fail({async_query_failed, Reason2})
    after 5000 ->
        ct:fail(async_query_timeout)
    end,

    ok = greptimedb:stop_client(Client).

%% ================================================================================
%% Helpers
%% ================================================================================

get_host_addr(Env) ->
    case os:getenv(Env) of
        false -> <<"127.0.0.1">>;
        Host -> iolist_to_binary(Host)
    end.
