-module(greptimedb_rs_types_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-compile([export_all, nowarn_export_all]).

-define(conn_opts(Config), ?config(conn_opts, Config)).
-define(table(Config), ?config(table, Config)).

all() ->
    [
        {group, all_types}
    ].

groups() ->
    [
        {all_types, [], [
            t_insert_all_types,
            t_insert_async_all_types,
            t_stream_all_types,
            t_stream_async_all_types
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(gproc),
    application:ensure_all_started(ecpool),
    application:ensure_all_started(greptimedb_rs),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(all_types, Config) ->
    Host = get_host_addr("GREPTIMEDB_TCP_ADDR"),
    ConnOpts = #{
        endpoints => [<<Host/binary, ":4001">>],
        dbname => <<"public">>
    },
    [{conn_opts, ConnOpts} | Config].

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    UniqueSuffix = integer_to_binary(erlang:unique_integer([positive])),
    Table = <<(atom_to_binary(TestCase))/binary, "_table_", UniqueSuffix/binary>>,
    [{table, Table} | Config].

end_per_testcase(_TestCase, _Config) ->
    ConnOpts = #{pool_name => greptimedb_rs_pool},
    catch greptimedb_rs:stop_client(ConnOpts),
    ok.

%% ----------------------------------------
%% Test Cases

t_insert_all_types(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),

    setup_table(Client, Table),

    Rows = [generate_full_row(1)],

    ?assertMatch({ok, _}, greptimedb_rs:insert(Client, Table, Rows)),

    verify_data(Client, Table, 1),

    ok = greptimedb_rs:stop_client(Client).

t_insert_async_all_types(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),

    setup_table(Client, Table),

    Rows = [generate_full_row(2)],

    Self = self(),
    Ref = make_ref(),
    Callback = {fun(P, R, Res) -> P ! {R, Res} end, [Self, Ref]},

    {ok, _} = greptimedb_rs:insert_async(Client, Table, Rows, Callback),

    receive
        {Ref, {ok, _}} -> ok;
        {Ref, Error} -> ct:fail({async_insert_failed, Error})
    after 5000 ->
        ct:fail(async_insert_timeout)
    end,

    verify_data(Client, Table, 1),
    ok = greptimedb_rs:stop_client(Client).

t_stream_all_types(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),

    setup_table(Client, Table),

    Rows = [generate_full_row(3), generate_full_row(4)],

    {ok, Stream} = greptimedb_rs:stream_start(Client, Table, hd(Rows)),
    ok = greptimedb_rs:stream_write(Stream, Rows),
    ok = greptimedb_rs:stream_close(Stream),

    verify_data(Client, Table, 2),
    ok = greptimedb_rs:stop_client(Client).

t_stream_async_all_types(Config) ->
    {ok, Client} = greptimedb_rs:start_client(?conn_opts(Config)),
    Table = ?table(Config),

    setup_table(Client, Table),

    Rows = [generate_full_row(5)],

    {ok, Stream} = greptimedb_rs:stream_start(Client, Table, hd(Rows)),

    Self = self(),
    Ref = make_ref(),
    Callback = {fun(P, R, Res) -> P ! {R, Res} end, [Self, Ref]},

    ok = greptimedb_rs:stream_write_async(Stream, Rows, Callback),

    receive
        {Ref, ok} -> ok;
        {Ref, Error} -> ct:fail({async_stream_write_failed, Error})
    after 5000 ->
        ct:fail(async_stream_write_timeout)
    end,

    ok = greptimedb_rs:stream_close(Stream),

    verify_data(Client, Table, 1),
    ok = greptimedb_rs:stop_client(Client).

%% ----------------------------------------
%% Helpers

setup_table(Client, Table) ->
    DropSql = iolist_to_binary(io_lib:format("DROP TABLE IF EXISTS ~s", [Table])),
    greptimedb_rs:query(Client, DropSql),

    CreateSql = iolist_to_binary(
        io_lib:format(
            "CREATE TABLE ~s ("
            "ts TIMESTAMP TIME INDEX, "
            "tag_str STRING, "
            "v_i8 INT8, "
            "v_i16 INT16, "
            "v_i32 INT32, "
            "v_i64 INT64, "
            "v_u8 UINT8, "
            "v_u16 UINT16, "
            "v_u32 UINT32, "
            "v_u64 UINT64, "
            "v_f32 FLOAT32, "
            "v_f64 FLOAT64, "
            "v_bool BOOLEAN, "
            "v_string STRING, "
            "v_binary BINARY, "
            "v_date DATE, "
            "v_datetime DATETIME, "
            "PRIMARY KEY (tag_str)"
            ") ENGINE=mito",
            [Table]
        )
    ),
    {ok, _} = greptimedb_rs:query(Client, CreateSql).

generate_full_row(Id) ->
    Ts = os:system_time(millisecond),
    #{
        timestamp => Ts,
        tags => #{
            <<"tag_str">> => list_to_binary("tag_" ++ integer_to_list(Id))
        },
        fields => #{
            <<"v_i8">> => 127,
            <<"v_i16">> => 32767,
            <<"v_i32">> => 2147483647,
            <<"v_i64">> => 9223372036854775807,
            <<"v_u8">> => 255,
            <<"v_u16">> => 65535,
            <<"v_u32">> => 4294967295,
            <<"v_u64">> => 18446744073709551615,
            <<"v_f32">> => 1.23 + Id,
            <<"v_f64">> => 4.56789 + Id,
            <<"v_bool">> => true,
            <<"v_string">> => <<"hello">>,
            <<"v_binary">> => <<"binary_data">>,
            % days since epoch
            <<"v_date">> => 19000 + Id,
            <<"v_datetime">> => 1600000000000 + Id
        }
    }.

verify_data(Client, Table, ExpectedCount) ->
    timer:sleep(2000),
    Sql = iolist_to_binary(io_lib:format("SELECT count(*) FROM ~s", [Table])),
    {ok, [ResultStr]} = greptimedb_rs:query(Client, Sql),
    ExpectedStr = integer_to_list(ExpectedCount),
    case string:find(ResultStr, ExpectedStr) of
        nomatch -> ct:fail({verification_failed, ResultStr, ExpectedStr});
        _ -> ok
    end.

get_host_addr(Env) ->
    case os:getenv(Env) of
        false -> <<"127.0.0.1">>;
        Host -> iolist_to_binary(Host)
    end.
