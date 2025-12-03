-module(greptimedb_rs_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-compile([export_all, nowarn_export_all]).

-define(conn_opts(Config), ?config(conn_opts, Config)).
-define(database(Config), ?config(database, Config)).
-define(table(Config), ?config(table, Config)).

all() ->
    [
        {group, tcp},
        {group, tls},
        t_connect_with_auth
    ].

groups() ->
    TCs = [
        t_connect,
        t_sync_insert,
        t_sync_query,
        t_async_insert,
        t_async_query
    ],
    [
        {tcp, [], TCs},
        {tls, [], TCs}
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
    [{conn_opts, ConnOpts} | Config];
init_per_group(tls, Config) ->
    Host = get_host_addr("GREPTIMEDB_TLS_ADDR"),
    Dir = code:lib_dir(greptimedb_rs),
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
    [{conn_opts, ConnOpts} | Config].

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    UniqueSuffix = integer_to_binary(erlang:unique_integer([positive])),
    Database = <<(atom_to_binary(TestCase))/binary, "_db">>,
    Table = <<(atom_to_binary(TestCase))/binary, "_table_", UniqueSuffix/binary>>,
    [{database, Database}, {table, Table} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

%% ----------------------------------------
%% Test Cases

t_connect(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    ?assert(is_pid(Client)),
    ?assertMatch({ok, [_ | _]}, greptimedb_rs:query(Client, <<"SELECT 1">>)),
    ok = greptimedb_rs:stop_client(Client).

t_connect_with_auth(_Config) ->
    Host = get_host_addr("GREPTIMEDB_AUTH_ADDR"),
    ConnOpts = #{
        endpoints => [<<Host/binary, ":4001">>],
        dbname => <<"public">>,
        username => <<"greptime_user">>,
        password => <<"greptime_pwd">>
    },
    {ok, Client} = greptimedb_rs:start_client(ConnOpts),
    ?assert(is_pid(Client)),
    ?assertMatch({ok, [_ | _]}, greptimedb_rs:query(Client, <<"SELECT 1">>)),
    ok = greptimedb_rs:stop_client(Client).

t_sync_insert(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),

    % Drop table if exists
    DropTableSql = iolist_to_binary(io_lib:format("DROP TABLE IF EXISTS ~s", [Table])),
    greptimedb_rs:query(Client, DropTableSql),

    % Create table explicitly with full schema
    CreateTableSql = iolist_to_binary(
        io_lib:format(
            "CREATE TABLE IF NOT EXISTS ~s ("
            "ts TIMESTAMP TIME INDEX, "
            "temperature DOUBLE, "
            "pressure INT64, "
            "active BOOLEAN, "
            "sensor_location STRING, "
            "sensor_id INT64, "
            "insert_val DOUBLE, "
            "PRIMARY KEY (sensor_location, sensor_id)"
            ") ENGINE=mito",
            [Table]
        )
    ),
    ?assertMatch({ok, _}, greptimedb_rs:query(Client, CreateTableSql)),

    Ts = erlang:system_time(millisecond),
    % Use ATOMS for keys here
    Rows = [
        #{
            fields => #{
                <<"temperature">> => 25.5,
                <<"pressure">> => 1013,
                <<"active">> => true,
                <<"insert_val">> => 1.0
            },
            tags => #{
                <<"sensor_location">> => <<"room1">>,
                <<"sensor_id">> => 12345
            },
            timestamp => Ts
        }
    ],
    ?assertMatch({ok, _}, greptimedb_rs:insert_bulk(Client, Table, Rows)),
    ok = greptimedb_rs:stop_client(Client).

t_sync_query(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),

    % Drop table if exists
    DropTableSql = iolist_to_binary(io_lib:format("DROP TABLE IF EXISTS ~s", [Table])),
    greptimedb_rs:query(Client, DropTableSql),

    % Create table explicitly
    CreateTableSql = iolist_to_binary(
        io_lib:format(
            "CREATE TABLE IF NOT EXISTS ~s ("
            "ts TIMESTAMP TIME INDEX, "
            "temperature DOUBLE, "
            "pressure INT64, "
            "active BOOLEAN, "
            "sensor_location STRING, "
            "sensor_id INT64, "
            "query_val INT64, "
            "PRIMARY KEY (sensor_location, sensor_id)"
            ") ENGINE=mito",
            [Table]
        )
    ),
    ?assertMatch({ok, _}, greptimedb_rs:query(Client, CreateTableSql)),

    Ts = erlang:system_time(millisecond),
    % Use BINARIES for keys here
    Rows = [
        #{
            <<"fields">> => #{
                <<"temperature">> => 25.5,
                <<"pressure">> => 1013,
                <<"active">> => true,
                <<"query_val">> => 123
            },
            <<"tags">> => #{
                <<"sensor_location">> => <<"room1">>,
                <<"sensor_id">> => 12345
            },
            <<"timestamp">> => Ts
        }
    ],
    ?assertMatch({ok, _}, greptimedb_rs:insert_bulk(Client, Table, Rows)),

    timer:sleep(1000),
    Sql = iolist_to_binary(io_lib:format("SELECT * FROM ~s", [Table])),
    ?assertMatch({ok, [_ | _]}, greptimedb_rs:query(Client, Sql)),
    ok = greptimedb_rs:stop_client(Client).

t_async_insert(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),

    % Drop table if exists
    DropTableSql = iolist_to_binary(io_lib:format("DROP TABLE IF EXISTS ~s", [Table])),
    greptimedb_rs:query(Client, DropTableSql),

    % Create table explicitly
    CreateTableSql = iolist_to_binary(
        io_lib:format(
            "CREATE TABLE IF NOT EXISTS ~s ("
            "ts TIMESTAMP TIME INDEX, "
            "temperature DOUBLE, "
            "pressure INT64, "
            "active BOOLEAN, "
            "sensor_location STRING, "
            "sensor_id INT64, "
            "async_insert INT64, "
            "PRIMARY KEY (sensor_location, sensor_id)"
            ") ENGINE=mito",
            [Table]
        )
    ),
    ?assertMatch({ok, _}, greptimedb_rs:query(Client, CreateTableSql)),

    Ts = erlang:system_time(millisecond),
    % Use ATOMS for keys here
    Rows = [
        #{
            fields => #{
                <<"temperature">> => 25.5,
                <<"pressure">> => 1013,
                <<"active">> => true,
                <<"async_insert">> => 100
            },
            tags => #{
                <<"sensor_location">> => <<"room1">>,
                <<"sensor_id">> => 12345
            },
            timestamp => Ts
        }
    ],
    Self = self(),
    Ref = make_ref(),
    CallbackFun = fun(P, R, Res) -> P ! {R, Res} end,
    Callback = {CallbackFun, [Self, Ref]},

    ok = greptimedb_rs:insert_bulk_async(Client, Table, Rows, Callback),
    receive
        {Ref, {ok, _}} -> ok;
        {Ref, {error, Reason}} -> ct:fail({async_write_failed, Reason})
    after 5000 ->
        ct:fail(async_write_timeout)
    end,
    ok = greptimedb_rs:stop_client(Client).

t_async_query(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),

    % Drop table if exists
    DropTableSql = iolist_to_binary(io_lib:format("DROP TABLE IF EXISTS ~s", [Table])),
    greptimedb_rs:query(Client, DropTableSql),

    % Create table explicitly
    CreateTableSql = iolist_to_binary(
        io_lib:format(
            "CREATE TABLE IF NOT EXISTS ~s ("
            "ts TIMESTAMP TIME INDEX, "
            "temperature DOUBLE, "
            "pressure INT64, "
            "active BOOLEAN, "
            "sensor_location STRING, "
            "sensor_id INT64, "
            "async_query INT64, "
            "PRIMARY KEY (sensor_location, sensor_id)"
            ") ENGINE=mito",
            [Table]
        )
    ),
    ?assertMatch({ok, _}, greptimedb_rs:query(Client, CreateTableSql)),

    Ts = erlang:system_time(millisecond),

    % Use BINARIES for keys here
    Rows = [
        #{
            <<"fields">> => #{
                <<"temperature">> => 25.5,
                <<"pressure">> => 1013,
                <<"active">> => true,
                <<"async_query">> => 200
            },
            <<"tags">> => #{
                <<"sensor_location">> => <<"room1">>,
                <<"sensor_id">> => 12345
            },
            <<"timestamp">> => Ts
        }
    ],
    {ok, _} = greptimedb_rs:insert_bulk(Client, Table, Rows),
    timer:sleep(1000),

    Self = self(),
    Ref = make_ref(),
    CallbackFun = fun(P, R, Res) -> P ! {R, Res} end,
    Callback = {CallbackFun, [Self, Ref]},

    Sql = iolist_to_binary(io_lib:format("SELECT * FROM ~s", [Table])),
    ok = greptimedb_rs:query_async(Client, Sql, Callback),

    receive
        {Ref, {ok, Result}} ->
            ?assertMatch([_ | _], Result);
        {Ref, {error, Reason}} ->
            ct:fail({async_query_failed, Reason})
    after 5000 ->
        ct:fail(async_query_timeout)
    end,
    ok = greptimedb_rs:stop_client(Client).

%% ================================================================================
%% Helpers
%% ================================================================================

get_host_addr(Env) ->
    case os:getenv(Env) of
        false -> <<"127.0.0.1">>;
        Host -> iolist_to_binary(Host)
    end.
