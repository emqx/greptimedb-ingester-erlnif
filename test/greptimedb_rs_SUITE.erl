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
        t_metadata_queries,
        t_insert_sync,
        t_insert_sync_existing_table,
        t_insert_sync_schema_conflict,
        t_query_sync,
        t_insert_async,
        t_insert_async_existing_table,
        t_insert_async_schema_conflict,
        t_query_async,
        t_stream_write,
        t_stream_write_async
    ],
    [
        {tcp, [], TCs},
        {tls, [], TCs}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(gproc),
    application:ensure_all_started(ecpool),
    application:ensure_all_started(greptimedb_rs),
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

end_per_testcase(_TestCase, Config) ->
    case ?config(conn_opts, Config) of
        undefined ->
            ok;
        ConnOpts ->
            Table = ?table(Config),

            %% Ensure client is available to clean up
            Client =
                case greptimedb_rs:start_client(ConnOpts) of
                    {ok, C} -> C;
                    {error, {already_started, C}} -> C
                end,

            %% Drop the table
            DropTableSql = iolist_to_binary(io_lib:format("DROP TABLE IF EXISTS ~s", [Table])),
            greptimedb_rs:query(Client, DropTableSql),

            %% Ensure client is stopped to prevent 'already_started' in next test
            catch greptimedb_rs:stop_client(Client),
            ok
    end.

%% ----------------------------------------
%% Test Cases

t_connect(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    ?assertMatch(#{pool_name := greptimedb_rs_pool}, Client),
    ?assertMatch({ok, [_ | _]}, greptimedb_rs:query(Client, <<"SELECT 1">>)),
    ok = greptimedb_rs:stop_client(Client).

t_metadata_queries(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),

    %% 1. Show Databases
    {ok, Dbs} = greptimedb_rs:query(Client, <<"SHOW DATABASES">>),
    %% Result is [[<<"db1">>], [<<"db2">>], ...], check if "public" is in one of the rows
    ?assert(lists:any(fun([DbName]) -> DbName =:= <<"public">> end, Dbs)),

    %% 2. Setup Table
    DropTableSql = iolist_to_binary(io_lib:format("DROP TABLE IF EXISTS ~s", [Table])),
    greptimedb_rs:query(Client, DropTableSql),

    CreateSql = iolist_to_binary(
        io_lib:format(
            "CREATE TABLE ~s ("
            "ts TIMESTAMP TIME INDEX, "
            "val INT32, "
            "PRIMARY KEY (val)"
            ") ENGINE=mito",
            [Table]
        )
    ),
    {ok, _} = greptimedb_rs:query(Client, CreateSql),

    %% 3. Show Tables
    {ok, Tables} = greptimedb_rs:query(Client, <<"SHOW TABLES">>),
    %% Check if our table is in the list
    ?assert(lists:any(fun([TableName]) -> TableName =:= Table end, Tables)),

    %% 4. Insert Data
    Ts = erlang:system_time(millisecond),
    Rows = [
        #{
            tags => #{<<"val">> => 10},
            timestamp => Ts
        },
        #{
            tags => #{<<"val">> => 20},
            timestamp => Ts + 1000
        }
    ],
    {ok, _} = greptimedb_rs:insert(Client, Table, Rows),
    timer:sleep(1000),

    %% 5. Select *
    %% Select specific column to match specific values
    SelectSql = iolist_to_binary(io_lib:format("SELECT val FROM ~s ORDER BY val", [Table])),
    {ok, Result} = greptimedb_rs:query(Client, SelectSql),
    ?assertEqual([[10], [20]], Result),

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
    ?assertMatch(#{pool_name := greptimedb_rs_pool}, Client),
    ?assertMatch({ok, [_ | _]}, greptimedb_rs:query(Client, <<"SELECT 1">>)),
    ok = greptimedb_rs:stop_client(Client).

t_insert_sync(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),

    %% Drop table if exists
    DropTableSql = iolist_to_binary(io_lib:format("DROP TABLE IF EXISTS ~s", [Table])),
    greptimedb_rs:query(Client, DropTableSql),

    Ts = erlang:system_time(millisecond),
    %% Use ATOMS for keys here
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
    ?assertMatch({ok, _}, greptimedb_rs:insert(Client, Table, Rows)),

    timer:sleep(1000),
    Sql = iolist_to_binary(io_lib:format("SELECT count(*) FROM ~s", [Table])),
    {ok, [[Count]]} = greptimedb_rs:query(Client, Sql),
    ?assertEqual(1, Count),

    ok = greptimedb_rs:stop_client(Client).

t_insert_sync_existing_table(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),

    %% Drop table if exists
    DropTableSql = iolist_to_binary(io_lib:format("DROP TABLE IF EXISTS ~s", [Table])),
    greptimedb_rs:query(Client, DropTableSql),

    %% Create table explicitly with schema that differs from default inference
    %% e.g. use INT32 for pressure (default inference might prefer INT64)
    CreateTableSql = iolist_to_binary(
        io_lib:format(
            "CREATE TABLE ~s ("
            "ts TIMESTAMP TIME INDEX, "
            "temperature DOUBLE, "
            "pressure INT32, "
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
    Rows = [
        #{
            fields => #{
                <<"temperature">> => 25.5,
                %% Fits in INT32
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
    %% Should succeed by using the existing schema
    ?assertMatch({ok, _}, greptimedb_rs:insert(Client, Table, Rows)),

    timer:sleep(1000),
    Sql = iolist_to_binary(io_lib:format("SELECT count(*) FROM ~s", [Table])),
    {ok, [[Count]]} = greptimedb_rs:query(Client, Sql),
    ?assertEqual(1, Count),

    ok = greptimedb_rs:stop_client(Client).

t_insert_sync_schema_conflict(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),

    %% Drop table if exists
    DropTableSql = iolist_to_binary(io_lib:format("DROP TABLE IF EXISTS ~s", [Table])),
    greptimedb_rs:query(Client, DropTableSql),

    %% Create table with INT8 Field and INT32 PK
    CreateTableSql = iolist_to_binary(
        io_lib:format(
            "CREATE TABLE ~s ("
            "ts TIMESTAMP TIME INDEX, "
            "val_int8 INT8, "
            "val_pk INT32, "
            "PRIMARY KEY (val_pk)"
            ") ENGINE=mito",
            [Table]
        )
    ),
    ?assertMatch({ok, _}, greptimedb_rs:query(Client, CreateTableSql)),

    Ts = erlang:system_time(millisecond),

    %% Case 1: Overflow INT8 (1000 doesn't fit in i8) -> Should Error
    Rows1 = [
        #{
            %% Overflow -> Error
            fields => #{<<"val_int8">> => 1000},
            tags => #{<<"val_pk">> => 1},
            timestamp => Ts
        }
    ],
    %% NIF should return error tuple {error, {nif_error, Reason}}
    ?assertMatch({error, {nif_error, _}}, greptimedb_rs:insert(Client, Table, Rows1)),

    %% Case 2: Type Conflict INT8 ("bad" string) -> Should Error
    Rows2 = [
        #{
            %% Type mismatch -> Error
            fields => #{<<"val_int8">> => <<"bad">>},
            tags => #{<<"val_pk">> => 2},
            timestamp => Ts + 1000
        }
    ],
    ?assertMatch({error, {nif_error, _}}, greptimedb_rs:insert(Client, Table, Rows2)),

    timer:sleep(1000),

    %% Case 3: PK Conflict (Insert String for Int32 PK) -> Should Error
    RowsPK = [
        #{
            fields => #{<<"val_int8">> => 10},
            tags => #{<<"val_pk">> => <<"bad_pk">>},
            timestamp => Ts + 2000
        }
    ],
    ?assertMatch({error, {nif_error, _}}, greptimedb_rs:insert(Client, Table, RowsPK)),

    %% Verify no rows inserted
    Sql = iolist_to_binary(io_lib:format("SELECT count(*) FROM ~s", [Table])),
    {ok, [[Count]]} = greptimedb_rs:query(Client, Sql),
    ?assertEqual(0, Count),

    ok = greptimedb_rs:stop_client(Client).

t_insert_async(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),

    %% Drop table if exists
    DropTableSql = iolist_to_binary(io_lib:format("DROP TABLE IF EXISTS ~s", [Table])),
    greptimedb_rs:query(Client, DropTableSql),

    Ts = erlang:system_time(millisecond),
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

    {ok, _WorkerPid} = greptimedb_rs:insert_async(Client, Table, Rows, Callback),
    receive
        {Ref, {ok, _}} -> ok;
        {Ref, {error, Reason}} -> ct:fail({async_write_failed, Reason})
    after 5000 ->
        ct:fail(async_write_timeout)
    end,

    timer:sleep(1000),
    Sql = iolist_to_binary(io_lib:format("SELECT count(*) FROM ~s", [Table])),
    {ok, [[Count]]} = greptimedb_rs:query(Client, Sql),
    ?assertEqual(1, Count),

    ok = greptimedb_rs:stop_client(Client).

t_insert_async_existing_table(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),

    %% Drop table if exists
    DropTableSql = iolist_to_binary(io_lib:format("DROP TABLE IF EXISTS ~s", [Table])),
    greptimedb_rs:query(Client, DropTableSql),

    %% Create table explicitly with schema that differs from default inference
    %% e.g. use INT32 for pressure
    CreateTableSql = iolist_to_binary(
        io_lib:format(
            "CREATE TABLE ~s ("
            "ts TIMESTAMP TIME INDEX, "
            "temperature DOUBLE, "
            "pressure INT32, "
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

    {ok, _WorkerPid} = greptimedb_rs:insert_async(Client, Table, Rows, Callback),
    receive
        {Ref, {ok, _}} -> ok;
        {Ref, {error, Reason}} -> ct:fail({async_write_failed, Reason})
    after 5000 ->
        ct:fail(async_write_timeout)
    end,

    timer:sleep(1000),
    Sql = iolist_to_binary(io_lib:format("SELECT count(*) FROM ~s", [Table])),
    {ok, [[Count]]} = greptimedb_rs:query(Client, Sql),
    ?assertEqual(1, Count),

    ok = greptimedb_rs:stop_client(Client).

t_insert_async_schema_conflict(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),

    %% Drop table if exists
    DropTableSql = iolist_to_binary(io_lib:format("DROP TABLE IF EXISTS ~s", [Table])),
    greptimedb_rs:query(Client, DropTableSql),

    %% Create table explicitly with INT32 PK
    CreateTableSql = iolist_to_binary(
        io_lib:format(
            "CREATE TABLE ~s ("
            "ts TIMESTAMP TIME INDEX, "
            "val_int8 INT8, "
            "val_pk INT32, "
            "PRIMARY KEY (val_pk)"
            ") ENGINE=mito",
            [Table]
        )
    ),
    ?assertMatch({ok, _}, greptimedb_rs:query(Client, CreateTableSql)),

    Ts = erlang:system_time(millisecond),
    Rows1 = [
        #{
            %% Overflow -> Error
            fields => #{<<"val_int8">> => 1000},
            tags => #{<<"val_pk">> => 1},
            timestamp => Ts
        }
    ],
    Self = self(),
    Ref = make_ref(),
    CallbackFun = fun(P, R, Res) -> P ! {R, Res} end,
    Callback = {CallbackFun, [Self, Ref]},

    {ok, _} = greptimedb_rs:insert_async(Client, Table, Rows1, Callback),
    receive
        {Ref, {error, {nif_error, _}}} -> ok;
        {Ref, Result1} -> ct:fail({expected_error, got, Result1})
    after 5000 ->
        ct:fail(async_write_timeout)
    end,

    timer:sleep(1000),

    Rows2 = [
        #{
            %% Type mismatch -> Error
            fields => #{<<"val_int8">> => <<"bad">>},
            tags => #{<<"val_pk">> => 2},
            timestamp => Ts + 1000
        }
    ],

    {ok, _} = greptimedb_rs:insert_async(Client, Table, Rows2, Callback),
    receive
        {Ref, {error, {nif_error, _}}} -> ok;
        {Ref, Result2} -> ct:fail({expected_error, got, Result2})
    after 5000 ->
        ct:fail(async_write_timeout)
    end,

    timer:sleep(1000),

    %% Case 3: PK Conflict Async
    RowsPK = [
        #{
            fields => #{<<"val_int8">> => 10},
            tags => #{<<"val_pk">> => <<"bad_pk">>},
            timestamp => Ts + 2000
        }
    ],
    {ok, _} = greptimedb_rs:insert_async(Client, Table, RowsPK, Callback),
    receive
        {Ref, {error, {nif_error, _}}} -> ok;
        {Ref, ResultPK} -> ct:fail({expected_error, got, ResultPK})
    after 5000 ->
        ct:fail(async_write_timeout)
    end,

    timer:sleep(1000),

    %% Verify no rows
    Sql = iolist_to_binary(io_lib:format("SELECT count(*) FROM ~s", [Table])),
    {ok, [[Count]]} = greptimedb_rs:query(Client, Sql),
    ?assertEqual(0, Count),
    ok = greptimedb_rs:stop_client(Client).

t_query_sync(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),

    %% Drop table if exists
    DropTableSql = iolist_to_binary(io_lib:format("DROP TABLE IF EXISTS ~s", [Table])),
    greptimedb_rs:query(Client, DropTableSql),

    Ts = erlang:system_time(millisecond),
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
    ?assertMatch({ok, _}, greptimedb_rs:insert(Client, Table, Rows)),

    timer:sleep(1000),
    Sql = iolist_to_binary(io_lib:format("SELECT * FROM ~s", [Table])),
    ?assertMatch({ok, [_ | _]}, greptimedb_rs:query(Client, Sql)),
    ok = greptimedb_rs:stop_client(Client).

t_query_async(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),

    %% Drop table if exists
    DropTableSql = iolist_to_binary(io_lib:format("DROP TABLE IF EXISTS ~s", [Table])),
    greptimedb_rs:query(Client, DropTableSql),

    Ts = erlang:system_time(millisecond),

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
    {ok, _} = greptimedb_rs:insert(Client, Table, Rows),
    timer:sleep(1000),

    Self = self(),
    Ref = make_ref(),
    CallbackFun = fun(P, R, Res) -> P ! {R, Res} end,
    Callback = {CallbackFun, [Self, Ref]},

    Sql = iolist_to_binary(io_lib:format("SELECT * FROM ~s", [Table])),
    {ok, _} = greptimedb_rs:query_async(Client, Sql, Callback),

    receive
        {Ref, {ok, Result}} ->
            ?assertMatch([_ | _], Result);
        {Ref, {error, Reason}} ->
            ct:fail({async_query_failed, Reason})
    after 5000 ->
        ct:fail(async_query_timeout)
    end,
    ok = greptimedb_rs:stop_client(Client).

t_stream_write(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),

    %% Drop table if exists
    DropTableSql = iolist_to_binary(io_lib:format("DROP TABLE IF EXISTS ~s", [Table])),
    greptimedb_rs:query(Client, DropTableSql),

    %% Create table explicitly
    CreateTableSql = iolist_to_binary(
        io_lib:format(
            "CREATE TABLE IF NOT EXISTS ~s ("
            "ts TIMESTAMP TIME INDEX, "
            "temperature DOUBLE, "
            "pressure INT64, "
            "active BOOLEAN, "
            "sensor_location STRING, "
            "sensor_id INT64, "
            "stream_val INT64, "
            "PRIMARY KEY (sensor_location, sensor_id)"
            ") ENGINE=mito",
            [Table]
        )
    ),
    ?assertMatch({ok, _}, greptimedb_rs:query(Client, CreateTableSql)),

    Ts = erlang:system_time(millisecond),
    Rows = [
        #{
            fields => #{
                <<"temperature">> => 26.0 + I,
                <<"pressure">> => 1015,
                <<"active">> => true,
                <<"stream_val">> => I
            },
            tags => #{
                <<"sensor_location">> => <<"lab">>,
                <<"sensor_id">> => 5000 + I
            },
            timestamp => Ts + (I * 100)
        }
     || I <- lists:seq(0, 9)
    ],

    {ok, StreamClient} = greptimedb_rs:stream_start(Client, Table, hd(Rows)),
    ok = greptimedb_rs:stream_write(StreamClient, Rows),
    ok = greptimedb_rs:stream_close(StreamClient),

    %% Wait for data to be visible
    timer:sleep(1000),

    Sql = iolist_to_binary(io_lib:format("SELECT count(*) FROM ~s", [Table])),
    {ok, [[Count]]} = greptimedb_rs:query(Client, Sql),
    %% Verify result contains "10" (row count)
    ?assertEqual(10, Count),

    ok = greptimedb_rs:stop_client(Client).

t_stream_write_async(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),

    %% Drop table if exists
    DropTableSql = iolist_to_binary(io_lib:format("DROP TABLE IF EXISTS ~s", [Table])),
    greptimedb_rs:query(Client, DropTableSql),

    %% Create table explicitly
    CreateTableSql = iolist_to_binary(
        io_lib:format(
            "CREATE TABLE IF NOT EXISTS ~s ("
            "ts TIMESTAMP TIME INDEX, "
            "temperature DOUBLE, "
            "pressure INT64, "
            "active BOOLEAN, "
            "sensor_location STRING, "
            "sensor_id INT64, "
            "stream_val_async INT64, "
            "PRIMARY KEY (sensor_location, sensor_id)"
            ") ENGINE=mito",
            [Table]
        )
    ),
    ?assertMatch({ok, _}, greptimedb_rs:query(Client, CreateTableSql)),

    Ts = erlang:system_time(millisecond),
    Rows = [
        #{
            fields => #{
                <<"temperature">> => 26.0 + I,
                <<"pressure">> => 1015,
                <<"active">> => true,
                <<"stream_val_async">> => I
            },
            tags => #{
                <<"sensor_location">> => <<"lab">>,
                <<"sensor_id">> => 6000 + I
            },
            timestamp => Ts + (I * 100)
        }
     || I <- lists:seq(0, 9)
    ],

    {ok, StreamClient} = greptimedb_rs:stream_start(Client, Table, hd(Rows)),

    Self = self(),
    Ref = make_ref(),
    CallbackFun = fun(P, R, Res) -> P ! {R, Res} end,
    Callback = {CallbackFun, [Self, Ref]},

    {ok, _} = greptimedb_rs:stream_write_async(StreamClient, Rows, Callback),

    receive
        {Ref, ok} -> ok;
        {Ref, {error, Reason}} -> ct:fail({async_stream_write_failed, Reason})
    after 5000 ->
        ct:fail(async_stream_write_timeout)
    end,

    ok = greptimedb_rs:stream_close(StreamClient),

    %% Wait for data to be visible
    timer:sleep(1000),

    Sql = iolist_to_binary(io_lib:format("SELECT count(*) FROM ~s", [Table])),
    {ok, [[Count]]} = greptimedb_rs:query(Client, Sql),
    ?assertEqual(10, Count),

    ok = greptimedb_rs:stop_client(Client).

%% ================================================================================
%% Helpers
%% ================================================================================

get_host_addr(Env) ->
    case os:getenv(Env) of
        false -> <<"127.0.0.1">>;
        Host -> iolist_to_binary(Host)
    end.
