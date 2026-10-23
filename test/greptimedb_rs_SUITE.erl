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
        {group, tls_ciphers},
        t_connect_with_auth,
        t_schema_discovery_error_propagation
    ].

groups() ->
    CommonTCs = [
        t_connect,
        t_metadata_queries,
        t_insert_sync,
        t_insert_sync_custom_ts_column,
        t_insert_sync_ttl_hint,
        t_insert_sync_existing_table,
        t_insert_sync_quoted_existing_table,
        t_insert_sync_quoted_auto_create,
        t_insert_sync_omitted_column_defaults,
        t_insert_sync_server_rendered_defaults,
        t_insert_sync_catalog_qualified_dbname,
        t_insert_sync_recreated_table_defaults,
        t_insert_sync_altered_default_invalidates_cache,
        t_insert_sync_mixed_batch_last_row_order,
        t_insert_sync_mixed_batch_atomicity,
        t_insert_unsupported_column_isolation,
        t_stream_rejects_unsupported_columns,
        t_insert_sync_schema_conflict,
        t_insert_sync_new_field_existing_table,
        t_insert_sync_new_tag_existing_table,
        t_insert_sync_new_field_mixed_batch,
        t_insert_sync_new_field_null_first,
        t_insert_sync_new_field_all_null,
        t_insert_sync_new_duplicate_key_prefers_field,
        t_insert_sync_json2_auto_create,
        t_insert_sync_json2_existing_table,
        t_insert_sync_json2_new_field_existing_table,
        t_query_sync,
        t_insert_async,
        t_insert_async_existing_table,
        t_insert_async_schema_conflict,
        t_insert_async_new_field_existing_table,
        t_insert_async_json2,
        t_query_async,
        t_stream_legacy_json,
        t_stream_legacy_json_rejects_invalid,
        t_legacy_json_deep_nesting,
        t_stream_rejects_json2,
        t_stream_multiple_time_windows,
        t_stream_write,
        t_stream_write_async
    ],
    TlsOnlyTCs = [
        t_connect_tls_without_client_certfiles,
        t_connect_tls_without_any_certfiles,
        t_connect_tls_verify_none_without_any_certfiles,
        t_connect_tls_verify_undefined_fallback_to_peer,
        t_connect_tls_invalid_verify_returns_error
    ],
    CipherTCs = [
        t_cipher_tls13_aes256_gcm,
        t_cipher_tls13_aes128_gcm,
        t_cipher_tls12_ecdhe_rsa_aes256,
        t_cipher_tls12_ecdhe_rsa_aes128,
        t_cipher_mixed_tls12_tls13,
        t_cipher_single_suite,
        t_cipher_empty_list_uses_defaults,
        t_cipher_all_invalid_returns_error,
        t_cipher_partial_invalid_skips_bad,
        t_fips_status_returns_boolean
    ],
    [
        {tcp, [], CommonTCs},
        {tls, [], CommonTCs ++ TlsOnlyTCs},
        {tls_ciphers, [], CipherTCs}
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
    [{conn_opts, ConnOpts} | Config];
init_per_group(tls_ciphers, Config) ->
    Host = get_host_addr("GREPTIMEDB_TLS_ADDR"),
    Dir = code:lib_dir(greptimedb_rs),
    DataDir = filename:join(Dir, "test/data/certs"),
    CaCert = filename:join(DataDir, "ca.crt"),
    ClientCert = filename:join(DataDir, "server.crt"),
    ClientKey = filename:join(DataDir, "server.key"),

    ?assert(filelib:is_file(CaCert)),
    ?assert(filelib:is_file(ClientCert)),
    ?assert(filelib:is_file(ClientKey)),

    %% Base TLS opts without cipher_suites; each TC adds its own
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

t_connect_tls_without_client_certfiles(_Config) ->
    Host = get_host_addr("GREPTIMEDB_TLS_ADDR"),
    Dir = code:lib_dir(greptimedb_rs),
    DataDir = filename:join(Dir, "test/data/certs"),
    CaCert = filename:join(DataDir, "ca.crt"),
    ?assert(filelib:is_file(CaCert)),
    ConnOpts = #{
        endpoints => [<<Host/binary, ":4001">>],
        dbname => <<"public">>,
        tls => true,
        ca_cert => list_to_binary(CaCert),
        username => <<"greptime_user">>,
        password => <<"greptime_pwd">>
    },
    {ok, Client} = greptimedb_rs:start_client(ConnOpts),
    ?assertMatch({ok, [_ | _]}, greptimedb_rs:query(Client, <<"SELECT 1">>)),
    ok = greptimedb_rs:stop_client(Client).

t_connect_tls_without_any_certfiles(_Config) ->
    Host = get_host_addr("GREPTIMEDB_TLS_ADDR"),
    ConnOpts = #{
        endpoints => [<<Host/binary, ":4001">>],
        dbname => <<"public">>,
        tls => true,
        username => <<"greptime_user">>,
        password => <<"greptime_pwd">>
    },
    case greptimedb_rs:start_client(ConnOpts) of
        {ok, Client} ->
            case greptimedb_rs:query(Client, <<"SELECT 1">>) of
                {ok, [_ | _]} ->
                    ok;
                {error, QueryReason} ->
                    QueryReasonBin = reason_to_binary(QueryReason),
                    ?assertEqual(
                        nomatch,
                        binary:match(QueryReasonBin, <<"Invalid config file path">>)
                    ),
                    ?assertEqual(
                        nomatch,
                        binary:match(QueryReasonBin, <<"No such file or directory">>)
                    )
            end,
            ok = greptimedb_rs:stop_client(Client);
        {error, Reason} ->
            ReasonBin = reason_to_binary(Reason),
            ?assertEqual(nomatch, binary:match(ReasonBin, <<"Invalid config file path">>)),
            ?assertEqual(nomatch, binary:match(ReasonBin, <<"No such file or directory">>))
    end.

t_connect_tls_verify_none_without_any_certfiles(_Config) ->
    Host = get_host_addr("GREPTIMEDB_TLS_ADDR"),
    ConnOpts = #{
        endpoints => [<<Host/binary, ":4001">>],
        dbname => <<"public">>,
        tls => true,
        verify => verify_none,
        username => <<"greptime_user">>,
        password => <<"greptime_pwd">>
    },
    {ok, Client} = greptimedb_rs:start_client(ConnOpts),
    ?assertMatch({ok, [_ | _]}, greptimedb_rs:query(Client, <<"SELECT 1">>)),
    ok = greptimedb_rs:stop_client(Client).

t_connect_tls_verify_undefined_fallback_to_peer(_Config) ->
    Host = get_host_addr("GREPTIMEDB_TLS_ADDR"),
    Dir = code:lib_dir(greptimedb_rs),
    DataDir = filename:join(Dir, "test/data/certs"),
    CaCert = filename:join(DataDir, "ca.crt"),
    ?assert(filelib:is_file(CaCert)),
    ConnOpts = #{
        endpoints => [<<Host/binary, ":4001">>],
        dbname => <<"public">>,
        tls => true,
        verify => undefined,
        ca_cert => list_to_binary(CaCert),
        username => <<"greptime_user">>,
        password => <<"greptime_pwd">>
    },
    {ok, Client} = greptimedb_rs:start_client(ConnOpts),
    ?assertMatch({ok, [_ | _]}, greptimedb_rs:query(Client, <<"SELECT 1">>)),
    ok = greptimedb_rs:stop_client(Client).

t_connect_tls_invalid_verify_returns_error(_Config) ->
    Host = get_host_addr("GREPTIMEDB_TLS_ADDR"),
    ConnOpts = #{
        endpoints => [<<Host/binary, ":4001">>],
        dbname => <<"public">>,
        tls => true,
        verify => bad_verify
    },
    case greptimedb_rs:start_client(ConnOpts) of
        {error, Reason} ->
            ReasonBin = reason_to_binary(Reason),
            ?assertNotEqual(nomatch, binary:match(ReasonBin, <<"invalid verify option">>)),
            ?assertEqual(nomatch, binary:match(ReasonBin, <<"nif_error">>));
        {ok, Client} ->
            ok = greptimedb_rs:stop_client(Client),
            ct:fail(expected_invalid_verify_error)
    end.

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

t_insert_sync_custom_ts_column(Config) ->
    ConnOpts = ?conn_opts(Config),
    ConnOpts1 = ConnOpts#{ts_column => <<"event_time">>},
    {ok, Client} = greptimedb_rs:start_client(ConnOpts1),
    Table = ?table(Config),

    %% Drop table if exists
    DropTableSql = iolist_to_binary(io_lib:format("DROP TABLE IF EXISTS ~s", [Table])),
    greptimedb_rs:query(Client, DropTableSql),

    Ts = erlang:system_time(millisecond),
    Rows = [
        #{
            fields => #{
                <<"temperature">> => 25.5
            },
            tags => #{
                <<"sensor_id">> => 12345
            },
            timestamp => Ts
        }
    ],
    ?assertMatch({ok, _}, greptimedb_rs:insert(Client, Table, Rows)),
    timer:sleep(1000),

    %% If ts_column works for auto-created tables, this query should succeed.
    SelectSql = iolist_to_binary(io_lib:format("SELECT event_time FROM ~s LIMIT 1", [Table])),
    ?assertMatch({ok, [[_]]}, greptimedb_rs:query(Client, SelectSql)),

    ok = greptimedb_rs:stop_client(Client).

t_insert_sync_ttl_hint(Config) ->
    ConnOpts = ?conn_opts(Config),
    ConnOpts1 = ConnOpts#{ttl => <<"3 days">>},
    {ok, Client} = greptimedb_rs:start_client(ConnOpts1),
    Table = ?table(Config),

    %% Drop table if exists
    DropTableSql = iolist_to_binary(io_lib:format("DROP TABLE IF EXISTS ~s", [Table])),
    greptimedb_rs:query(Client, DropTableSql),

    Ts = erlang:system_time(millisecond),
    Rows = [
        #{
            fields => #{
                <<"temperature">> => 25.5
            },
            tags => #{
                <<"sensor_id">> => 12345
            },
            timestamp => Ts
        }
    ],
    ?assertMatch({ok, _}, greptimedb_rs:insert(Client, Table, Rows)),
    timer:sleep(1000),

    CreateOptionsSql = iolist_to_binary(
        io_lib:format(
            "SELECT create_options FROM information_schema.tables WHERE table_name='~s'",
            [Table]
        )
    ),
    {ok, [[CreateOptions]]} = greptimedb_rs:query(Client, CreateOptionsSql),
    ?assert(is_binary(CreateOptions)),
    ?assertNotEqual(nomatch, binary:match(string:lowercase(CreateOptions), <<"ttl=">>)),

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

t_insert_sync_omitted_column_defaults(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),
    ok = drop_table_if_exists(Client, Table),
    CreateTableSql = iolist_to_binary(
        io_lib:format(
            "CREATE TABLE ~s ("
            "ts TIMESTAMP TIME INDEX, "
            "payload JSON, "
            "temperature DOUBLE, "
            "default_label STRING NOT NULL DEFAULT 'sensor', "
            "nullable_default STRING DEFAULT 'nd', "
            "default_tag STRING DEFAULT 'tag-default', "
            "PRIMARY KEY (default_tag)"
            ") ENGINE=mito",
            [Table]
        )
    ),
    ?assertMatch({ok, _}, greptimedb_rs:query(Client, CreateTableSql)),

    Ts = erlang:system_time(millisecond),
    %% The payload and every defaulted column are omitted. They must stay out
    %% of the request schema so GreptimeDB applies the column defaults.
    ?assertMatch(
        {ok, _},
        greptimedb_rs:insert(Client, Table, [
            #{fields => #{<<"temperature">> => 25.0}, timestamp => Ts}
        ])
    ),
    %% Explicit values must still override the defaults.
    ?assertMatch(
        {ok, _},
        greptimedb_rs:insert(Client, Table, [
            #{
                fields => #{
                    <<"temperature">> => 26.0,
                    <<"default_label">> => <<"explicit">>,
                    <<"nullable_default">> => <<"explicit-nd">>
                },
                tags => #{<<"default_tag">> => <<"explicit-tag">>},
                timestamp => Ts + 1
            }
        ])
    ),
    %% Mixed batch: some rows provide a defaulted column and others omit it.
    %% Each presence set must become its own request so omitted columns still
    %% receive the server-side default.
    ?assertMatch(
        {ok, _},
        greptimedb_rs:insert(Client, Table, [
            #{fields => #{<<"temperature">> => 27.0}, timestamp => Ts + 2},
            #{
                fields => #{
                    <<"temperature">> => 28.0,
                    <<"default_label">> => <<"mixed-explicit">>
                },
                tags => #{<<"default_tag">> => <<"mixed-tag">>},
                timestamp => Ts + 3
            },
            #{
                fields => #{<<"temperature">> => 29.0, <<"nullable_default">> => <<"mixed-nd">>},
                timestamp => Ts + 4
            }
        ])
    ),
    timer:sleep(500),

    Sql = iolist_to_binary(
        io_lib:format(
            "SELECT default_label, nullable_default, default_tag, payload IS NULL, temperature "
            "FROM ~s ORDER BY ts",
            [Table]
        )
    ),
    {ok, Rows} = greptimedb_rs:query(Client, Sql),
    ?assertEqual(
        [
            [<<"sensor">>, <<"nd">>, <<"tag-default">>, true, 25.0],
            [<<"explicit">>, <<"explicit-nd">>, <<"explicit-tag">>, true, 26.0],
            [<<"sensor">>, <<"nd">>, <<"tag-default">>, true, 27.0],
            [<<"mixed-explicit">>, <<"nd">>, <<"mixed-tag">>, true, 28.0],
            [<<"sensor">>, <<"mixed-nd">>, <<"tag-default">>, true, 29.0]
        ],
        Rows
    ),
    ok = greptimedb_rs:stop_client(Client).

t_insert_sync_server_rendered_defaults(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),
    ok = drop_table_if_exists(Client, Table),
    CreateTableSql = iolist_to_binary(
        io_lib:format(
            "CREATE TABLE ~s ("
            "ts TIMESTAMP TIME INDEX, "
            "s_empty STRING DEFAULT '', "
            "s_sql_null STRING DEFAULT NULL, "
            "s_cap_null STRING DEFAULT 'Null', "
            "s_null_text STRING DEFAULT 'NULL', "
            "s_lower_null STRING DEFAULT 'null', "
            "s_space STRING DEFAULT ' sensor ', "
            "s_quote STRING DEFAULT '''quoted''', "
            "s_paren STRING DEFAULT 'a(b', "
            "s_cast STRING DEFAULT 'x::y', "
            "d DATE DEFAULT '2022-06-15', "
            "ts_zero TIMESTAMP DEFAULT 0"
            ") ENGINE=mito",
            [Table]
        )
    ),
    ?assertMatch({ok, _}, greptimedb_rs:query(Client, CreateTableSql)),

    Ts = erlang:system_time(millisecond),
    %% The first row supplies every column so they are all present in the
    %% request schema; the second row omits them and must receive the defaults.
    ?assertMatch(
        {ok, _},
        greptimedb_rs:insert(Client, Table, [
            #{
                fields => #{
                    <<"s_empty">> => <<"explicit-empty">>,
                    <<"s_sql_null">> => <<"explicit-null">>,
                    <<"s_cap_null">> => <<"explicit-cap">>,
                    <<"s_null_text">> => <<"explicit-NULL">>,
                    <<"s_lower_null">> => <<"explicit-null">>,
                    <<"s_space">> => <<"explicit-space">>,
                    <<"s_quote">> => <<"explicit-quote">>,
                    <<"s_paren">> => <<"explicit-paren">>,
                    <<"s_cast">> => <<"explicit-cast">>,
                    <<"d">> => 19000,
                    <<"ts_zero">> => 123
                },
                timestamp => Ts
            },
            #{fields => #{}, timestamp => Ts + 1}
        ])
    ),
    timer:sleep(500),

    Sql = iolist_to_binary(
        io_lib:format(
            "SELECT s_empty, s_null_text, s_lower_null, s_space, s_quote, "
            "s_paren, s_cast, s_sql_null IS NULL, s_cap_null, d, ts_zero "
            "FROM ~s ORDER BY ts",
            [Table]
        )
    ),
    {ok, [Explicit, Defaults]} = greptimedb_rs:query(Client, Sql),
    ?assertEqual(
        [
            <<"explicit-empty">>,
            <<"explicit-NULL">>,
            <<"explicit-null">>,
            <<"explicit-space">>,
            <<"explicit-quote">>,
            <<"explicit-paren">>,
            <<"explicit-cast">>,
            false,
            <<"explicit-cap">>,
            19000,
            123
        ],
        Explicit
    ),
    ?assertEqual(
        [
            <<>>,
            <<"NULL">>,
            <<"null">>,
            <<" sensor ">>,
            <<"'quoted'">>,
            <<"a(b">>,
            <<"x::y">>,
            true,
            <<"Null">>,
            19158,
            0
        ],
        Defaults
    ),
    ok = greptimedb_rs:stop_client(Client).

t_insert_sync_catalog_qualified_dbname(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),
    ok = drop_table_if_exists(Client, Table),
    CreateTableSql = iolist_to_binary(
        io_lib:format(
            "CREATE TABLE ~s ("
            "ts TIMESTAMP TIME INDEX, "
            "temperature DOUBLE, "
            "default_label STRING NOT NULL DEFAULT 'sensor'"
            ") ENGINE=mito",
            [Table]
        )
    ),
    ?assertMatch({ok, _}, greptimedb_rs:query(Client, CreateTableSql)),
    ok = greptimedb_rs:stop_client(Client),

    CatalogOpts = (?conn_opts(Config))#{dbname => <<"greptime-public">>},
    {ok, CatalogClient} = greptimedb_rs:start_client(CatalogOpts),
    Ts = erlang:system_time(millisecond),
    ?assertMatch(
        {ok, _},
        greptimedb_rs:insert(CatalogClient, Table, [
            #{fields => #{<<"temperature">> => 1.0}, timestamp => Ts},
            #{
                fields => #{<<"temperature">> => 2.0, <<"default_label">> => <<"explicit">>},
                timestamp => Ts + 1
            }
        ])
    ),
    timer:sleep(500),

    Sql = iolist_to_binary(
        io_lib:format("SELECT default_label, temperature FROM ~s ORDER BY ts", [Table])
    ),
    ?assertEqual(
        {ok, [[<<"sensor">>, 1.0], [<<"explicit">>, 2.0]]},
        greptimedb_rs:query(CatalogClient, Sql)
    ),
    ok = greptimedb_rs:stop_client(CatalogClient).

t_insert_sync_recreated_table_defaults(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),
    Create = fun(Default) ->
        DropSql = iolist_to_binary(io_lib:format("DROP TABLE IF EXISTS ~s", [Table])),
        _ = greptimedb_rs:query(Client, DropSql),
        CreateSql = iolist_to_binary(
            io_lib:format(
                "CREATE TABLE ~s ("
                "ts TIMESTAMP TIME INDEX, "
                "temperature DOUBLE, "
                "default_label STRING NOT NULL DEFAULT '~s'"
                ") ENGINE=mito",
                [Table, Default]
            )
        ),
        ?assertMatch({ok, _}, greptimedb_rs:query(Client, CreateSql))
    end,
    Insert = fun(Ts) ->
        ?assertMatch(
            {ok, _},
            greptimedb_rs:insert(Client, Table, [
                #{fields => #{<<"temperature">> => 1.0}, timestamp => Ts},
                #{
                    fields => #{<<"temperature">> => 2.0, <<"default_label">> => <<"explicit">>},
                    timestamp => Ts + 1
                }
            ])
        )
    end,
    Select = fun() ->
        Sql = iolist_to_binary(
            io_lib:format("SELECT default_label, temperature FROM ~s ORDER BY ts", [Table])
        ),
        {ok, Rows} = greptimedb_rs:query(Client, Sql),
        Rows
    end,

    Create("one"),
    Insert(erlang:system_time(millisecond)),
    timer:sleep(500),
    ?assertEqual([[<<"one">>, 1.0], [<<"explicit">>, 2.0]], Select()),

    %% Recreate the table with a different default. The next mixed batch must
    %% observe the current server default, not a value cached from the old table.
    Create("two"),
    Insert(erlang:system_time(millisecond)),
    timer:sleep(500),
    ?assertEqual([[<<"two">>, 1.0], [<<"explicit">>, 2.0]], Select()),
    ok = greptimedb_rs:stop_client(Client).

t_insert_sync_altered_default_invalidates_cache(Config) ->
    Opts = (?conn_opts(Config))#{pool_size => 1},
    {ok, Client} = greptimedb_rs:start_client(Opts),
    Table = ?table(Config),
    CreateTableSql = iolist_to_binary(
        io_lib:format(
            "CREATE TABLE ~s ("
            "ts TIMESTAMP TIME INDEX, "
            "temperature DOUBLE, "
            "default_label STRING NOT NULL DEFAULT 'one'"
            ") ENGINE=mito",
            [Table]
        )
    ),
    ?assertMatch({ok, _}, greptimedb_rs:query(Client, CreateTableSql)),
    Insert = fun(Ts) ->
        ?assertMatch(
            {ok, _},
            greptimedb_rs:insert(Client, Table, [
                #{fields => #{<<"temperature">> => 1.0}, timestamp => Ts},
                #{
                    fields => #{<<"temperature">> => 2.0, <<"default_label">> => <<"explicit">>},
                    timestamp => Ts + 1
                }
            ])
        )
    end,
    Ts = erlang:system_time(millisecond),
    Insert(Ts),
    AlterSql = iolist_to_binary(
        io_lib:format("ALTER TABLE ~s MODIFY COLUMN default_label SET DEFAULT 'two'", [Table])
    ),
    ?assertMatch({ok, _}, greptimedb_rs:query(Client, AlterSql)),
    Insert(Ts + 2),
    timer:sleep(500),
    Sql = iolist_to_binary(
        io_lib:format("SELECT default_label, temperature FROM ~s ORDER BY ts", [Table])
    ),
    ?assertEqual(
        {ok, [
            [<<"one">>, 1.0],
            [<<"explicit">>, 2.0],
            [<<"two">>, 1.0],
            [<<"explicit">>, 2.0]
        ]},
        greptimedb_rs:query(Client, Sql)
    ),
    ok = greptimedb_rs:stop_client(Client).

t_insert_sync_mixed_batch_last_row_order(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),
    ok = drop_table_if_exists(Client, Table),
    CreateTableSql = iolist_to_binary(
        io_lib:format(
            "CREATE TABLE ~s ("
            "ts TIMESTAMP TIME INDEX, "
            "sensor_id INT64, "
            "v BIGINT, "
            "x BIGINT, "
            "PRIMARY KEY (sensor_id)"
            ") ENGINE=mito",
            [Table]
        )
    ),
    ?assertMatch({ok, _}, greptimedb_rs:query(Client, CreateTableSql)),

    Ts = erlang:system_time(millisecond),
    %% Different field sets must still be sent as one request in input order.
    %% With merge_mode=last_row this row must win over the preceding row.
    ?assertMatch(
        {ok, 2},
        greptimedb_rs:insert(Client, Table, [
            #{fields => #{<<"v">> => 1}, tags => #{<<"sensor_id">> => 1}, timestamp => Ts},
            #{
                fields => #{<<"v">> => 2, <<"x">> => 22},
                tags => #{<<"sensor_id">> => 1},
                timestamp => Ts
            }
        ])
    ),
    timer:sleep(500),

    Sql = iolist_to_binary(io_lib:format("SELECT v, x FROM ~s", [Table])),
    ?assertEqual({ok, [[2, 22]]}, greptimedb_rs:query(Client, Sql)),
    ?assertEqual([[1]], select_count(Client, Table, <<"COUNT(*)">>)),
    ok = greptimedb_rs:stop_client(Client).

t_insert_sync_mixed_batch_atomicity(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),
    ok = drop_table_if_exists(Client, Table),
    CreateTableSql = iolist_to_binary(
        io_lib:format(
            "CREATE TABLE ~s ("
            "ts TIMESTAMP TIME INDEX, "
            "v BIGINT NOT NULL, "
            "x BIGINT"
            ") ENGINE=mito",
            [Table]
        )
    ),
    ?assertMatch({ok, _}, greptimedb_rs:query(Client, CreateTableSql)),

    Ts = erlang:system_time(millisecond),
    %% The second row violates the NOT NULL constraint. Because both rows are
    %% in one request, the whole request must be rejected without persisting
    %% the first row.
    Result = greptimedb_rs:insert(Client, Table, [
        #{fields => #{<<"v">> => 1}, timestamp => Ts},
        #{fields => #{<<"v">> => nil, <<"x">> => 22}, timestamp => Ts + 1}
    ]),
    ?assertMatch({error, _}, Result),

    timer:sleep(500),
    ?assertEqual([[0]], select_count(Client, Table, <<"COUNT(*)">>)),
    ok = greptimedb_rs:stop_client(Client).

t_insert_sync_schema_conflict(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),

    %% Drop table if exists
    DropTableSql = iolist_to_binary(io_lib:format("DROP TABLE IF EXISTS ~s", [Table])),
    greptimedb_rs:query(Client, DropTableSql),

    %% Create table with a TINYINT (Int8) field and INT32 PK.
    %% Note: `INT8` is a PostgreSQL alias for BIGINT, not an 8-bit type.
    CreateTableSql = iolist_to_binary(
        io_lib:format(
            "CREATE TABLE ~s ("
            "ts TIMESTAMP TIME INDEX, "
            "val_int8 TINYINT, "
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

    %% Create table with a TINYINT (Int8) field and INT32 PK.
    %% Note: `INT8` is a PostgreSQL alias for BIGINT, not an 8-bit type.
    CreateTableSql = iolist_to_binary(
        io_lib:format(
            "CREATE TABLE ~s ("
            "ts TIMESTAMP TIME INDEX, "
            "val_int8 TINYINT, "
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

%%------------------------------------------------------------------------------
%% New-column regression tests for emqx/emqx#18499
%%
%% Inserting a record with fields/tags that are not present in the existing
%% table schema must not silently drop them: the inferred columns must be merged
%% into the outgoing request schema so that GreptimeDB can add them.
%%------------------------------------------------------------------------------

t_insert_sync_new_field_existing_table(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),
    ok = create_existing_table(Client, Table),
    Ts = erlang:system_time(millisecond),
    Rows = [
        #{
            fields => #{
                <<"temperature">> => 26.0,
                %% New field: absent from the existing table schema.
                <<"humidity">> => 60.0
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
    ok = assert_column(Client, Table, <<"humidity">>, <<"Float64">>, <<"FIELD">>),
    ?assertEqual([60.0], select_column(Client, Table, <<"humidity">>)),
    %% Existing columns keep being written.
    ?assertEqual([26.0], select_column(Client, Table, <<"temperature">>)),
    ok = greptimedb_rs:stop_client(Client).

t_insert_sync_new_tag_existing_table(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),
    ok = create_existing_table(Client, Table),
    Ts = erlang:system_time(millisecond),
    Rows = [
        #{
            fields => #{<<"temperature">> => 26.0},
            tags => #{
                <<"sensor_id">> => 12345,
                %% New tag: absent from the existing table schema.
                <<"room">> => <<"room1">>
            },
            timestamp => Ts
        }
    ],
    ?assertMatch({ok, _}, greptimedb_rs:insert(Client, Table, Rows)),
    timer:sleep(1000),
    ok = assert_column(Client, Table, <<"room">>, <<"String">>, <<"TAG">>),
    ?assertEqual([<<"room1">>], select_column(Client, Table, <<"room">>)),
    ok = greptimedb_rs:stop_client(Client).

t_insert_sync_new_field_mixed_batch(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),
    ok = create_existing_table(Client, Table),
    Ts = erlang:system_time(millisecond),
    %% Only the second row carries the new field: inference must consider all
    %% rows in the batch, not just the first one.
    Rows = [
        #{
            fields => #{<<"temperature">> => 25.0},
            tags => #{<<"sensor_location">> => <<"room1">>, <<"sensor_id">> => 12345},
            timestamp => Ts
        },
        #{
            fields => #{<<"temperature">> => 26.0, <<"humidity">> => 60.0},
            tags => #{<<"sensor_location">> => <<"room1">>, <<"sensor_id">> => 12345},
            timestamp => Ts + 1
        }
    ],
    ?assertMatch({ok, _}, greptimedb_rs:insert(Client, Table, Rows)),
    timer:sleep(1000),
    ok = assert_column(Client, Table, <<"humidity">>, <<"Float64">>, <<"FIELD">>),
    ?assertEqual([nil, 60.0], select_column(Client, Table, <<"humidity">>)),
    ok = greptimedb_rs:stop_client(Client).

t_insert_sync_new_duplicate_key_prefers_field(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),
    ok = create_existing_table(Client, Table),
    Ts = erlang:system_time(millisecond),
    %% The same unknown key shows up in both maps. Sending two columns with the
    %% same name is rejected by GreptimeDB ("Duplicated column name in gRPC
    %% requests"), so one of them must win: fields take precedence.
    Rows = [
        #{
            fields => #{<<"temperature">> => 26.0, <<"dup">> => 9.0},
            tags => #{
                <<"sensor_location">> => <<"room1">>,
                <<"sensor_id">> => 12345,
                <<"dup">> => <<"as-tag">>
            },
            timestamp => Ts
        }
    ],
    ?assertMatch({ok, _}, greptimedb_rs:insert(Client, Table, Rows)),
    timer:sleep(1000),
    ok = assert_column(Client, Table, <<"dup">>, <<"Float64">>, <<"FIELD">>),
    ?assertEqual([9.0], select_column(Client, Table, <<"dup">>)),
    ok = greptimedb_rs:stop_client(Client).

t_insert_sync_new_field_null_first(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),
    ok = create_existing_table(Client, Table),
    Ts = erlang:system_time(millisecond),
    %% A NULL must not decide the type of a new column: the numeric value in the
    %% second row has to win, otherwise the second row fails to encode.
    Rows = [
        #{
            fields => #{<<"temperature">> => 25.0, <<"humidity">> => nil},
            tags => #{<<"sensor_location">> => <<"room1">>, <<"sensor_id">> => 12345},
            timestamp => Ts
        },
        #{
            fields => #{<<"temperature">> => 26.0, <<"humidity">> => 60.0},
            tags => #{<<"sensor_location">> => <<"room1">>, <<"sensor_id">> => 12345},
            timestamp => Ts + 1
        }
    ],
    ?assertMatch({ok, _}, greptimedb_rs:insert(Client, Table, Rows)),
    timer:sleep(1000),
    ok = assert_column(Client, Table, <<"humidity">>, <<"Float64">>, <<"FIELD">>),
    ?assertEqual([nil, 60.0], select_column(Client, Table, <<"humidity">>)),
    ok = greptimedb_rs:stop_client(Client).

t_insert_sync_new_field_all_null(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),
    ok = create_existing_table(Client, Table),
    Ts = erlang:system_time(millisecond),
    %% Every value of the unknown key is NULL: no column should be created for
    %% it, so that a later non-null write can pick the right type.
    Rows = [
        #{
            fields => #{<<"temperature">> => 25.0, <<"humidity">> => nil},
            tags => #{<<"sensor_location">> => <<"room1">>, <<"sensor_id">> => 12345},
            timestamp => Ts
        }
    ],
    ?assertMatch({ok, _}, greptimedb_rs:insert(Client, Table, Rows)),
    timer:sleep(1000),
    ?assertEqual(not_found, describe_column(Client, Table, <<"humidity">>)),
    ?assertEqual([25.0], select_column(Client, Table, <<"temperature">>)),
    ok = greptimedb_rs:stop_client(Client).

t_insert_async_new_field_existing_table(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),
    ok = create_existing_table(Client, Table),
    Ts = erlang:system_time(millisecond),
    Rows = [
        #{
            fields => #{
                <<"temperature">> => 26.0,
                <<"humidity">> => 60.0
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
    ok = assert_column(Client, Table, <<"humidity">>, <<"Float64">>, <<"FIELD">>),
    ?assertEqual([60.0], select_column(Client, Table, <<"humidity">>)),
    ok = greptimedb_rs:stop_client(Client).

t_insert_sync_json2_auto_create(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),
    ok = drop_table_if_exists(Client, Table),

    Ts = erlang:system_time(millisecond),
    Rows = [
        #{
            fields => #{
                <<"payload">> =>
                    {json2,
                        <<"{\"name\":\"alice\",\"nested\":{\"ok\":true},\"items\":[1,\"two\",null]}">>}
            },
            timestamp => Ts
        },
        #{
            fields => #{<<"payload">> => {json2, <<"null">>}},
            timestamp => Ts + 1
        }
    ],
    ?assertMatch({ok, _}, greptimedb_rs:insert(Client, Table, Rows)),

    timer:sleep(1000),
    ok = assert_column_type_contains(Client, Table, <<"payload">>, <<"Json2">>, <<"FIELD">>),
    ?assertEqual([<<"alice">>, nil], select_column(Client, Table, <<"payload.name">>)),
    ?assertEqual([<<"two">>, nil], select_column(Client, Table, <<"payload.items[1]">>)),
    ?assertEqual([[2]], select_count(Client, Table, <<"COUNT(*)">>)),
    %% GreptimeDB 1.2.1 counts NULL rows in COUNT(json2_column), so count a
    %% subfield to verify that the JSON null document did not produce a value.
    ?assertEqual([[1]], select_count(Client, Table, <<"COUNT(payload.name)">>)),

    ok = greptimedb_rs:stop_client(Client).

t_insert_sync_json2_existing_table(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),
    ok = create_json2_table(Client, Table),

    Ts = erlang:system_time(millisecond),
    Rows = [
        #{
            fields => #{<<"payload">> => <<"{\"name\":\"bob\",\"active\":true}">>},
            timestamp => Ts
        },
        #{
            fields => #{<<"payload">> => {json2, <<"{\"name\":\"carol\",\"active\":false}">>}},
            timestamp => Ts + 1
        },
        #{
            fields => #{<<"payload">> => nil},
            timestamp => Ts + 2
        }
    ],
    ?assertMatch({ok, _}, greptimedb_rs:insert(Client, Table, Rows)),

    timer:sleep(1000),
    ok = assert_column_type_contains(Client, Table, <<"payload">>, <<"Json2">>, <<"FIELD">>),
    ?assertEqual(
        [<<"bob">>, <<"carol">>, nil], select_column(Client, Table, <<"payload.name">>)
    ),
    ?assertEqual([[3]], select_count(Client, Table, <<"COUNT(*)">>)),
    %% See above: COUNT(json2_column) includes SQL NULL rows on GreptimeDB 1.2.1.
    ?assertEqual([[2]], select_count(Client, Table, <<"COUNT(payload.name)">>)),

    ok = greptimedb_rs:stop_client(Client).

t_insert_unsupported_column_isolation(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),
    Create = <<
        "CREATE TABLE ",
        Table/binary,
        " (ts TIMESTAMP TIME INDEX, payload JSON2, extra DECIMAL(10,2))"
        " WITH (append_mode='true')"
    >>,
    ?assertMatch({ok, _}, greptimedb_rs:query(Client, Create)),
    Json = <<"{\"name\":\"bob\"}">>,
    Row = fun(Ts, Fields) -> #{timestamp => Ts, fields => Fields} end,
    ?assertEqual({ok, 1}, greptimedb_rs:insert(Client, Table, [Row(1, #{<<"payload">> => Json})])),
    %% Adding a supported column also exercises the inferred-column fallback.
    ?assertEqual(
        {ok, 2},
        greptimedb_rs:insert(Client, Table, [
            Row(2, #{<<"payload">> => Json, <<"v">> => 2}),
            Row(3, #{<<"v">> => 3})
        ])
    ),
    Self = self(),
    Ref = make_ref(),
    Callback = {fun(Result) -> Self ! {Ref, Result} end, []},
    ?assertMatch(
        {ok, _},
        greptimedb_rs:insert_async(Client, Table, [Row(4, #{<<"payload">> => Json})], Callback)
    ),
    receive
        {Ref, Result} -> ?assertEqual({ok, 1}, Result)
    after 5000 ->
        ct:fail(async_write_timeout)
    end,
    %% An unsupported column in a later row must reject the entire batch.
    ?assertEqual(
        {error, <<"Unsupported type `Decimal(10, 2)` for column `extra`">>},
        greptimedb_rs:insert(Client, Table, [
            Row(5, #{<<"payload">> => Json}),
            Row(6, #{<<"payload">> => Json, <<"extra">> => 1.25})
        ])
    ),
    ?assertEqual(
        {ok, [[1, <<"bob">>], [2, <<"bob">>], [3, nil], [4, <<"bob">>]]},
        greptimedb_rs:query(
            Client, <<"SELECT ts, payload.name FROM ", Table/binary, " ORDER BY ts">>
        )
    ),
    ok = greptimedb_rs:stop_client(Client).

t_insert_sync_quoted_existing_table(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    try
        lists:foreach(
            fun(Suffix) ->
                Table = <<(?table(Config))/binary, Suffix/binary>>,
                Quoted = quote_table_identifier(Table),
                try
                    ?assertMatch(
                        {ok, _},
                        greptimedb_rs:query(Client, <<
                            "CREATE TABLE ",
                            Quoted/binary,
                            " (ts TIMESTAMP TIME INDEX, v DOUBLE, label STRING DEFAULT 'Null')"
                        >>)
                    ),
                    Ts = erlang:system_time(millisecond),
                    %% Mixed presence forces default lookup and SHOW CREATE TABLE.
                    ?assertEqual(
                        {ok, 2},
                        greptimedb_rs:insert(Client, Table, [
                            #{
                                fields => #{<<"v">> => 1.0, <<"label">> => <<"explicit">>},
                                timestamp => Ts
                            },
                            #{fields => #{<<"v">> => 2.0}, timestamp => Ts + 1}
                        ])
                    ),
                    ?assertEqual(
                        {ok, [[1.0, <<"explicit">>], [2.0, <<"Null">>]]},
                        greptimedb_rs:query(Client, <<
                            "SELECT v, label FROM ", Quoted/binary, " ORDER BY ts"
                        >>)
                    )
                after
                    greptimedb_rs:query(Client, <<"DROP TABLE IF EXISTS ", Quoted/binary>>)
                end
            end,
            [<<"-name">>, <<"-MiXeD">>, <<".name">>]
        )
    after
        greptimedb_rs:stop_client(Client)
    end.

t_insert_sync_quoted_auto_create(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    try
        lists:foreach(
            fun(Suffix) ->
                Table = <<(?table(Config))/binary, Suffix/binary>>,
                Quoted = quote_table_identifier(Table),
                Ts = erlang:system_time(millisecond),
                try
                    lists:foreach(
                        fun(Offset) ->
                            ?assertEqual(
                                {ok, 1},
                                greptimedb_rs:insert(Client, Table, [
                                    #{fields => #{<<"v">> => Offset}, timestamp => Ts + Offset}
                                ])
                            )
                        end,
                        [1, 2]
                    ),
                    ?assertEqual(
                        {ok, [[1], [2]]},
                        greptimedb_rs:query(
                            Client, <<"SELECT v FROM ", Quoted/binary, " ORDER BY ts">>
                        )
                    )
                after
                    greptimedb_rs:query(Client, <<"DROP TABLE IF EXISTS ", Quoted/binary>>)
                end
            end,
            [<<"-name">>, <<"-MiXeD">>, <<".name">>]
        ),
        %% Quoting must reach table-name validation rather than fail SQL parsing.
        InvalidTable = <<(?table(Config))/binary, "\"name">>,
        {error, Reason} = greptimedb_rs:insert(Client, InvalidTable, [
            #{fields => #{<<"v">> => 1}, timestamp => erlang:system_time(millisecond)}
        ]),
        ?assertNotEqual(nomatch, binary:match(Reason, <<"Invalid table name">>))
    after
        greptimedb_rs:stop_client(Client)
    end.

t_stream_rejects_unsupported_columns(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),
    ?assertMatch(
        {ok, _},
        greptimedb_rs:query(Client, <<
            "CREATE TABLE ",
            Table/binary,
            " (ts TIMESTAMP TIME INDEX, v BIGINT, extra DECIMAL(10,2))"
        >>)
    ),
    %% Bulk needs the complete schema and must not silently skip unsupported columns.
    ?assertEqual(
        {error, <<"Unsupported type `Decimal(10, 2)` for column `extra`">>},
        greptimedb_rs:stream_start(Client, Table, #{})
    ),
    ?assertEqual([[0]], select_count(Client, Table, <<"COUNT(*)">>)),
    ok = greptimedb_rs:stop_client(Client).

t_schema_discovery_error_propagation(Config) ->
    Host = get_host_addr("GREPTIMEDB_AUTH_ADDR"),
    ConnOpts = #{
        endpoints => [<<Host/binary, ":4001">>],
        dbname => <<"public">>,
        username => <<"greptime_user">>,
        password => <<"incorrect_password">>
    },
    {ok, Client} = greptimedb_rs:start_client(ConnOpts),
    try
        Table = ?table(Config),
        {error, Reason} = greptimedb_rs:query(Client, <<"DESCRIBE ", Table/binary>>),
        ?assertNotEqual(nomatch, binary:match(Reason, <<"password">>)),
        %% Return the authentication error before attempting to encode these rows.
        ?assertEqual(
            {error, Reason},
            greptimedb_rs:insert(Client, Table, [#{fields => not_a_map}])
        )
    after
        greptimedb_rs:stop_client(Client)
    end.

t_insert_sync_json2_new_field_existing_table(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),
    ok = create_append_only_existing_table(Client, Table),

    Ts = erlang:system_time(millisecond),
    Rows = [
        #{
            fields => #{
                <<"temperature">> => 26.0,
                <<"payload">> => {json2, <<"{\"name\":\"dave\",\"nested\":{\"ok\":true}}">>}
            },
            timestamp => Ts
        }
    ],
    ?assertMatch({ok, _}, greptimedb_rs:insert(Client, Table, Rows)),

    timer:sleep(1000),
    ok = assert_column_type_contains(Client, Table, <<"payload">>, <<"Json2">>, <<"FIELD">>),
    ?assertEqual([<<"dave">>], select_column(Client, Table, <<"payload.name">>)),
    ?assertEqual([26.0], select_column(Client, Table, <<"temperature">>)),

    ok = greptimedb_rs:stop_client(Client).

t_insert_async_json2(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),
    ok = drop_table_if_exists(Client, Table),

    Ts = erlang:system_time(millisecond),
    Rows = [
        #{
            fields => #{<<"payload">> => {json2, <<"{\"name\":\"async\",\"ok\":true}">>}},
            timestamp => Ts
        }
    ],
    Self = self(),
    Ref = make_ref(),
    Callback = {fun(P, R, Res) -> P ! {R, Res} end, [Self, Ref]},
    {ok, _} = greptimedb_rs:insert_async(Client, Table, Rows, Callback),

    receive
        {Ref, {ok, _}} -> ok;
        {Ref, Error} -> ct:fail({async_json2_insert_failed, Error})
    after 5000 ->
        ct:fail(async_json2_insert_timeout)
    end,

    timer:sleep(1000),
    ok = assert_column_type_contains(Client, Table, <<"payload">>, <<"Json2">>, <<"FIELD">>),
    ?assertEqual([<<"async">>], select_column(Client, Table, <<"payload.name">>)),

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
            fields => #{
                <<"temperature">> => 25.5,
                <<"pressure">> => 1013,
                <<"active">> => true,
                <<"query_val">> => 123
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
            fields => #{
                <<"temperature">> => 25.5,
                <<"pressure">> => 1013,
                <<"active">> => true,
                <<"async_query">> => 200
            },
            tags => #{
                <<"sensor_location">> => <<"room1">>,
                <<"sensor_id">> => 12345
            },
            timestamp => Ts
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

t_stream_legacy_json(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),
    run_legacy_json_test(Client, Table, fun check_legacy_json_stream/2).

run_legacy_json_test(Client, Table, Fun) ->
    Result = with_legacy_json_table(Client, Table, Fun),
    ok = greptimedb_rs:stop_client(Client),
    case Result of
        {fail, Reason} -> ct:fail(Reason);
        Other -> Other
    end.

with_legacy_json_table(Client, Table, Fun) ->
    ok = drop_table_if_exists(Client, Table),
    CreateTableSql = iolist_to_binary(
        io_lib:format(
            "CREATE TABLE ~s ("
            "ts TIMESTAMP TIME INDEX, "
            "payload JSON, "
            "sensor_id INT64, "
            "PRIMARY KEY (sensor_id)"
            ") ENGINE=mito",
            [Table]
        )
    ),
    case greptimedb_rs:query(Client, CreateTableSql) of
        {ok, _} ->
            case describe_column(Client, Table, <<"payload">>) of
                {<<"Json">>, _} -> Fun(Client, Table);
                {<<"Json2">>, _} -> {skip, "this GreptimeDB version aliases JSON to JSON2"};
                Other -> {fail, {unexpected_legacy_json_type, Other}}
            end;
        {error, Reason} ->
            {skip, {legacy_json_not_supported, Reason}}
    end.

check_legacy_json_stream(Client, Table) ->
    Ts = erlang:system_time(millisecond),
    Rows = [
        #{
            fields => #{<<"payload">> => <<"{\"name\":\"legacy-1\",\"value\":1}">>},
            tags => #{<<"sensor_id">> => 1},
            timestamp => Ts
        },
        #{
            fields => #{<<"payload">> => <<"{\"name\":\"legacy-2\",\"value\":2}">>},
            tags => #{<<"sensor_id">> => 2},
            timestamp => Ts + 1
        }
    ],
    {ok, Stream} = greptimedb_rs:stream_start(Client, Table, hd(Rows)),
    ok = greptimedb_rs:stream_write(Stream, Rows),
    ok = greptimedb_rs:stream_close(Stream),

    timer:sleep(1000),
    %% `COUNT(payload)` would be 0 if the bulk builder encoded the legacy JSON
    %% values as NULL (the regression this test guards against).
    ?assertEqual([[2]], select_count(Client, Table, <<"COUNT(payload)">>)),
    ?assertEqual([[2]], select_count(Client, Table, <<"COUNT(*)">>)).

t_stream_legacy_json_rejects_invalid(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),
    run_legacy_json_test(Client, Table, fun check_invalid_legacy_json_stream/2).

check_invalid_legacy_json_stream(Client, Table) ->
    Ts = erlang:system_time(millisecond),
    ValidDoc = <<"{\"name\":\"valid\"}">>,
    InvalidRows = [
        #{
            fields => #{<<"payload">> => <<"not-json">>},
            tags => #{<<"sensor_id">> => 1},
            timestamp => Ts + 2
        }
    ],
    ValidRows = [
        #{
            fields => #{<<"payload">> => ValidDoc},
            tags => #{<<"sensor_id">> => 2},
            timestamp => Ts + 3
        }
    ],
    {ok, Stream} = greptimedb_rs:stream_start(Client, Table, hd(ValidRows)),
    %% The bulk builder copies strings into a JSON binary column without
    %% server-side validation, so malformed documents must be rejected here.
    ?assertMatch({error, _}, greptimedb_rs:stream_write(Stream, InvalidRows)),
    ?assertEqual(ok, greptimedb_rs:stream_write(Stream, ValidRows)),
    ok = greptimedb_rs:stream_close(Stream),

    timer:sleep(1000),
    ?assertEqual([[1]], select_count(Client, Table, <<"COUNT(*)">>)),
    ?assertEqual([ValidDoc], select_column(Client, Table, <<"json_to_string(payload)">>)).

t_legacy_json_deep_nesting(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),
    run_legacy_json_test(Client, Table, fun check_deep_legacy_json/2).

check_deep_legacy_json(Client, Table) ->
    Ts = erlang:system_time(millisecond),
    %% 150 nested arrays are valid JSON and accepted by GreptimeDB, but exceed
    %% serde_json's default 128-level recursion limit.
    Doc = iolist_to_binary([
        lists:duplicate(150, <<"[">>),
        <<"1">>,
        lists:duplicate(150, <<"]">>)
    ]),
    ?assertMatch(
        {ok, _},
        greptimedb_rs:insert(Client, Table, [
            #{
                fields => #{<<"payload">> => Doc},
                tags => #{<<"sensor_id">> => 1},
                timestamp => Ts
            }
        ])
    ),

    {ok, Stream} = greptimedb_rs:stream_start(Client, Table, #{}),
    ok = greptimedb_rs:stream_write(Stream, [
        #{
            fields => #{<<"payload">> => Doc},
            tags => #{<<"sensor_id">> => 2},
            timestamp => Ts + 1
        }
    ]),
    ok = greptimedb_rs:stream_close(Stream),

    timer:sleep(1000),
    ?assertEqual([Doc, Doc], select_column(Client, Table, <<"json_to_string(payload)">>)),
    ?assertEqual([[2]], select_count(Client, Table, <<"COUNT(*)">>)).

t_stream_rejects_json2(Config) ->
    Table = ?table(Config),
    {ok, ClientRef} = greptimedb_rs_nif:connect(?conn_opts(Config)),

    CreateTableSql = iolist_to_binary(
        io_lib:format(
            "CREATE TABLE ~s ("
            "ts TIMESTAMP TIME INDEX, "
            "payload JSON2"
            ") ENGINE=mito WITH (append_mode = 'true')",
            [Table]
        )
    ),
    {ok, _} = greptimedb_rs_nif:execute(ClientRef, CreateTableSql),

    {error, Reason} = greptimedb_rs_nif:stream_start(ClientRef, Table, #{}),
    ?assertNotEqual(
        nomatch, binary:match(reason_to_binary(Reason), <<"does not support JSON2">>)
    ),

    %% The public wrapper must propagate the same rejection instead of
    %% returning {ok, Stream} and failing later on stream_write.
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    {error, PublicReason} = greptimedb_rs:stream_start(Client, Table, #{}),
    ?assertNotEqual(
        nomatch, binary:match(reason_to_binary(PublicReason), <<"does not support JSON2">>)
    ),
    ok = greptimedb_rs:stop_client(Client),

    ok = greptimedb_rs_nif:disconnect(ClientRef).

t_stream_multiple_time_windows(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),
    ok = drop_table_if_exists(Client, Table),
    CreateTableSql = iolist_to_binary(
        io_lib:format(
            "CREATE TABLE ~s (ts TIMESTAMP TIME INDEX, metric_value INT64) ENGINE=mito",
            [Table]
        )
    ),
    ?assertMatch({ok, _}, greptimedb_rs:query(Client, CreateTableSql)),

    Ts = erlang:system_time(millisecond),
    Rows = [
        #{fields => #{<<"metric_value">> => 1}, timestamp => Ts},
        #{fields => #{<<"metric_value">> => 2}, timestamp => Ts + 3600000},
        #{fields => #{<<"metric_value">> => 3}, timestamp => Ts + 7200000}
    ],
    {ok, Stream} = greptimedb_rs:stream_start(Client, Table, hd(Rows)),
    ok = greptimedb_rs:stream_write(Stream, Rows),
    ok = greptimedb_rs:stream_close(Stream),

    timer:sleep(1000),
    ?assertEqual([1, 2, 3], select_column(Client, Table, <<"metric_value">>)),
    ?assertEqual([[3]], select_count(Client, Table, <<"COUNT(*)">>)),
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
%% TLS Cipher Suite Test Cases
%% ================================================================================

%% TLS 1.3 cipher: AES-256-GCM
t_cipher_tls13_aes256_gcm(Config) ->
    ConnOpts = (?conn_opts(Config))#{
        cipher_suites => [<<"TLS_AES_256_GCM_SHA384">>]
    },
    {ok, Client} = greptimedb_rs:start_client(ConnOpts),
    ?assertMatch({ok, [_ | _]}, greptimedb_rs:query(Client, <<"SELECT 1">>)),
    ok = greptimedb_rs:stop_client(Client).

%% TLS 1.3 cipher: AES-128-GCM
t_cipher_tls13_aes128_gcm(Config) ->
    ConnOpts = (?conn_opts(Config))#{
        cipher_suites => [<<"TLS_AES_128_GCM_SHA256">>]
    },
    {ok, Client} = greptimedb_rs:start_client(ConnOpts),
    ?assertMatch({ok, [_ | _]}, greptimedb_rs:query(Client, <<"SELECT 1">>)),
    ok = greptimedb_rs:stop_client(Client).

%% TLS 1.2 cipher: ECDHE-RSA-AES256
t_cipher_tls12_ecdhe_rsa_aes256(Config) ->
    ConnOpts = (?conn_opts(Config))#{
        cipher_suites => [<<"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384">>]
    },
    {ok, Client} = greptimedb_rs:start_client(ConnOpts),
    ?assertMatch({ok, [_ | _]}, greptimedb_rs:query(Client, <<"SELECT 1">>)),
    ok = greptimedb_rs:stop_client(Client).

%% TLS 1.2 cipher: ECDHE-RSA-AES128
t_cipher_tls12_ecdhe_rsa_aes128(Config) ->
    ConnOpts = (?conn_opts(Config))#{
        cipher_suites => [<<"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256">>]
    },
    {ok, Client} = greptimedb_rs:start_client(ConnOpts),
    ?assertMatch({ok, [_ | _]}, greptimedb_rs:query(Client, <<"SELECT 1">>)),
    ok = greptimedb_rs:stop_client(Client).

%% Mixed TLS 1.2 + 1.3 ciphers
t_cipher_mixed_tls12_tls13(Config) ->
    ConnOpts = (?conn_opts(Config))#{
        cipher_suites => [
            <<"TLS_AES_256_GCM_SHA384">>,
            <<"TLS_AES_128_GCM_SHA256">>,
            <<"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384">>,
            <<"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256">>
        ]
    },
    {ok, Client} = greptimedb_rs:start_client(ConnOpts),
    ?assertMatch({ok, [_ | _]}, greptimedb_rs:query(Client, <<"SELECT 1">>)),
    ok = greptimedb_rs:stop_client(Client).

%% Single cipher suite
t_cipher_single_suite(Config) ->
    ConnOpts = (?conn_opts(Config))#{
        cipher_suites => [<<"TLS_AES_128_GCM_SHA256">>]
    },
    {ok, Client} = greptimedb_rs:start_client(ConnOpts),
    ?assertMatch({ok, [_ | _]}, greptimedb_rs:query(Client, <<"SELECT 1">>)),
    ok = greptimedb_rs:stop_client(Client).

%% Empty cipher list should behave the same as no cipher_suites key (default)
t_cipher_empty_list_uses_defaults(Config) ->
    ConnOpts = (?conn_opts(Config))#{
        cipher_suites => []
    },
    {ok, Client} = greptimedb_rs:start_client(ConnOpts),
    ?assertMatch({ok, [_ | _]}, greptimedb_rs:query(Client, <<"SELECT 1">>)),
    ok = greptimedb_rs:stop_client(Client).

%% All invalid cipher names should cause a connection error
t_cipher_all_invalid_returns_error(Config) ->
    ConnOpts = (?conn_opts(Config))#{
        cipher_suites => [<<"INVALID_CIPHER_1">>, <<"NONSENSE_CIPHER">>]
    },
    case greptimedb_rs:start_client(ConnOpts) of
        {error, _Reason} ->
            ok;
        {ok, Client} ->
            %% Connection might succeed lazily; query should fail
            case greptimedb_rs:query(Client, <<"SELECT 1">>) of
                {error, _} -> ok;
                {ok, _} -> ct:fail(expected_cipher_error)
            end,
            ok = greptimedb_rs:stop_client(Client)
    end.

%% Mix of valid + invalid cipher names: invalid ones are skipped, valid ones work
t_cipher_partial_invalid_skips_bad(Config) ->
    ConnOpts = (?conn_opts(Config))#{
        cipher_suites => [
            <<"INVALID_CIPHER">>,
            <<"TLS_AES_256_GCM_SHA384">>,
            <<"ANOTHER_BAD_ONE">>
        ]
    },
    {ok, Client} = greptimedb_rs:start_client(ConnOpts),
    ?assertMatch({ok, [_ | _]}, greptimedb_rs:query(Client, <<"SELECT 1">>)),
    ok = greptimedb_rs:stop_client(Client).

%% FIPS status should return a boolean
t_fips_status_returns_boolean(_Config) ->
    FipsStatus = greptimedb_rs_nif:fips_status(),
    ?assert(is_boolean(FipsStatus)),
    %% cached version should return the same value
    CachedStatus = greptimedb_rs_nif:cached_fips_status(),
    ?assertEqual(FipsStatus, CachedStatus).

%% ================================================================================
%% Helpers
%% ================================================================================

%%------------------------------------------------------------------------------
%% Helpers for the new-column regression tests
%%------------------------------------------------------------------------------

%% Pre-creates a table whose schema intentionally differs from what the driver
%% would infer (e.g. sensor_id is INT64, temperature is DOUBLE).
create_existing_table(Client, Table) ->
    DropSql = iolist_to_binary(io_lib:format("DROP TABLE IF EXISTS ~s", [Table])),
    _ = greptimedb_rs:query(Client, DropSql),
    CreateSql = iolist_to_binary(
        io_lib:format(
            "CREATE TABLE ~s ("
            "ts TIMESTAMP TIME INDEX, "
            "temperature DOUBLE, "
            "sensor_location STRING, "
            "sensor_id INT64, "
            "PRIMARY KEY (sensor_location, sensor_id)"
            ") ENGINE=mito",
            [Table]
        )
    ),
    case greptimedb_rs:query(Client, CreateSql) of
        {ok, _} -> ok;
        Error -> ct:fail({create_table_failed, Table, Error})
    end.

drop_table_if_exists(Client, Table) ->
    DropSql = iolist_to_binary(io_lib:format("DROP TABLE IF EXISTS ~s", [Table])),
    case greptimedb_rs:query(Client, DropSql) of
        {ok, _} -> ok;
        Error -> ct:fail({drop_table_failed, Table, Error})
    end.

create_json2_table(Client, Table) ->
    ok = drop_table_if_exists(Client, Table),
    CreateSql = iolist_to_binary(
        io_lib:format(
            "CREATE TABLE ~s ("
            "ts TIMESTAMP TIME INDEX, "
            "payload JSON2"
            ") ENGINE=mito WITH (append_mode = 'true')",
            [Table]
        )
    ),
    case greptimedb_rs:query(Client, CreateSql) of
        {ok, _} -> ok;
        Error -> ct:fail({create_json2_table_failed, Table, Error})
    end.

create_append_only_existing_table(Client, Table) ->
    ok = drop_table_if_exists(Client, Table),
    CreateSql = iolist_to_binary(
        io_lib:format(
            "CREATE TABLE ~s ("
            "ts TIMESTAMP TIME INDEX, "
            "temperature DOUBLE"
            ") ENGINE=mito WITH (append_mode = 'true')",
            [Table]
        )
    ),
    case greptimedb_rs:query(Client, CreateSql) of
        {ok, _} -> ok;
        Error -> ct:fail({create_append_only_table_failed, Table, Error})
    end.

assert_column(Client, Table, Column, Type, SemanticType) ->
    case describe_column(Client, Table, Column) of
        not_found ->
            ct:fail({column_missing_from_table, Table, Column});
        {Type, SemanticType} ->
            ok;
        {OtherType, OtherSemanticType} ->
            ct:fail({
                unexpected_column_definition,
                Table,
                Column,
                {expected, Type, SemanticType},
                {got, OtherType, OtherSemanticType}
            })
    end.

assert_column_type_contains(Client, Table, Column, TypeFragment, SemanticType) ->
    case describe_column(Client, Table, Column) of
        not_found ->
            ct:fail({column_missing_from_table, Table, Column});
        {Type, SemanticType} ->
            case binary:match(Type, TypeFragment) of
                nomatch ->
                    ct:fail({
                        unexpected_column_type,
                        Table,
                        Column,
                        {expected_fragment, TypeFragment},
                        {got, Type}
                    });
                _ ->
                    ok
            end;
        {OtherType, OtherSemanticType} ->
            ct:fail({
                unexpected_column_definition,
                Table,
                Column,
                {expected, TypeFragment, SemanticType},
                {got, OtherType, OtherSemanticType}
            })
    end.

describe_column(Client, Table, Column) ->
    Sql = iolist_to_binary(io_lib:format("DESCRIBE ~s", [Table])),
    {ok, Rows} = greptimedb_rs:query(Client, Sql),
    %% DESCRIBE columns: Field, Type, Null, Key, Default, Semantic Type
    case [Row || [Name | _] = Row <- Rows, Name =:= Column] of
        [[_, Type, _Null, _Key, _Default, SemanticType] | _] -> {Type, SemanticType};
        [] -> not_found
    end.

select_column(Client, Table, Column) ->
    Sql = iolist_to_binary(
        io_lib:format("SELECT ~s FROM ~s ORDER BY ts", [Column, Table])
    ),
    {ok, Rows} = greptimedb_rs:query(Client, Sql),
    [Value || [Value] <- Rows].

select_count(Client, Table, Expression) ->
    Sql = iolist_to_binary(io_lib:format("SELECT ~s FROM ~s", [Expression, Table])),
    {ok, Rows} = greptimedb_rs:query(Client, Sql),
    Rows.

quote_table_identifier(Name) ->
    Escaped = binary:replace(Name, <<"\"">>, <<"\"\"">>, [global]),
    <<"\"", Escaped/binary, "\"">>.

get_host_addr(Env) ->
    case os:getenv(Env) of
        false -> <<"127.0.0.1">>;
        Host -> iolist_to_binary(Host)
    end.

reason_to_binary(Reason) when is_binary(Reason) ->
    Reason;
reason_to_binary(Reason) when is_list(Reason) ->
    iolist_to_binary(Reason);
reason_to_binary(Reason) ->
    iolist_to_binary(io_lib:format("~0p", [Reason])).
