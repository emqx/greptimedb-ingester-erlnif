%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(greptimedb_rs).

-include("greptimedb_rs.hrl").

%% Connection and Disconnection
-export([
    start_client/1,
    stop_client/1
]).

%% Write - Batch Write, onshot streaming write
-export([
    insert/3,
    insert/4,
    insert_async/4,
    insert_async/5
]).

%% Write - Execute Query
-export([
    query/2,
    query_async/3
]).

%% Write - Streaming Write with persistent stream client
-export([
    stream_start/3,
    stream_close/1,
    stream_write/2,
    stream_write_async/3
]).

-export_type([
    client/0,
    opts/0,
    stream_client/0,
    time_unit/0,
    native_error/0,
    error_kind/0,
    error_source/0,
    error_code/0,
    error_stage/0,
    write_outcome/0,
    sql/0,
    result/0,
    reason/0,
    pool_name/0,
    callback/0
]).

%% ===================================================================
%% Type defs
%% ===================================================================

-define(POOL_NAME(PoolName), #{pool_name := PoolName}).

-type opts() :: #{
    endpoints := [binary()],
    dbname := binary(),
    username => binary(),
    password => binary(),
    ts_column => binary(),
    ttl => binary(),
    tls => boolean(),
    verify => verify_peer | verify_none | binary(),
    ca_cert => binary(),
    client_cert => binary(),
    client_key => binary(),
    cipher_suites => [binary()],
    pool_name => pool_name(),
    pool_size => pool_size(),
    pool_type => pool_type(),
    request_timeout => pos_integer(),
    stream_timeout => pos_integer(),
    any() => term()
}.
-type client() :: #{
    pool_name := pool_name(),
    pool_size := pool_size(),
    pool_type := pool_type(),
    conn_opts := opts()
}.
-type stream_client() :: {stream_client, client(), table()}.
-type time_unit() :: s | ms | us | ns.
-type error_kind() ::
    transport
    | server
    | invalid_argument
    | configuration
    | protocol
    | internal
    | timeout
    | cancelled
    | closed.
-type error_source() ::
    server
    | create_channel
    | request_timeout
    | stream_ended
    | stream_ended_with_pending_requests
    | send_data
    | invalid_tls_config
    | invalid_config_file_path
    | illegal_grpc_client_state
    | missing_field
    | illegal_database_response
    | invalid_tonic_metadata_value
    | serde_json
    | create_record_batch
    | unsupported_data_type
    | serialize_metadata
    | schema_mismatch
    | invalid_column_count
    | invalid_column_index
    | empty_rows
    | add_row_to_built_batch
    | unflushed_rows
    | unexpected_flight_message
    | local
    | table_not_found.
-type error_code() ::
    none
    | unknown
    | cancelled
    | invalid_argument
    | deadline_exceeded
    | not_found
    | already_exists
    | permission_denied
    | resource_exhausted
    | failed_precondition
    | aborted
    | out_of_range
    | unimplemented
    | internal
    | unavailable
    | data_loss
    | unauthenticated.
-type error_stage() ::
    connect
    | metadata
    | query
    | insert
    | stream_start
    | stream_write
    | stream_close.
-type write_outcome() :: not_sent | unknown.
-type native_error() ::
    {greptimedb_error, #{
        domain := greptimedb,
        kind := error_kind(),
        source := error_source(),
        code := error_code(),
        stage := error_stage(),
        write_outcome := write_outcome(),
        retry_hint := boolean(),
        message := binary()
    }}.
-type table() :: binary().
-type sql() :: binary().
-type result() :: term().
-type reason() :: native_error() | term() | binary().

-type pool_name() :: term().
-type pool_type() :: random | hash.
-type pool_size() :: pos_integer().

-type callback() :: {function(), list()}.

%% ===================================================================
%% Connection and Disconnection
%% ===================================================================

%% @doc Start the connection pool (using ecpool).
%% Returns a Client (logic level) that holds the connection pool.
-spec start_client(opts()) -> {ok, client()} | {error, {already_started, client()} | term()}.
start_client(Opts) ->
    PoolName = maps:get(pool_name, Opts, greptimedb_rs_pool),
    PoolSize = maps:get(pool_size, Opts, 8),
    PoolType = maps:get(pool_type, Opts, random),
    AutoReconnect = maps:get(auto_reconnect, Opts, undefined),

    PoolOpts = lists:flatten([
        {pool_size, PoolSize},
        {pool_type, PoolType},
        {conn_opts, Opts},
        [{auto_reconnect, AutoReconnect} || is_integer(AutoReconnect)]
    ]),

    Client = #{
        pool_name => PoolName,
        pool_size => PoolSize,
        pool_type => PoolType,
        conn_opts => Opts
    },

    case ecpool:start_sup_pool(PoolName, ?SOCK_MODULE, PoolOpts) of
        {ok, _} ->
            {ok, Client};
        {error, {already_started, _}} ->
            {error, {already_started, Client}};
        Error ->
            Error
    end.

%% @doc Stop the connection pool and release resources.
-spec stop_client(client() | pool_name()) -> ok.
stop_client(?POOL_NAME(PoolName)) ->
    ecpool:stop_sup_pool(PoolName);
stop_client(PoolName) ->
    ecpool:stop_sup_pool(PoolName).

%% ===================================================================
%% Write - Batch Write
%% ===================================================================

%% @doc Batch write data with millisecond timestamps (blocking).
%%
%% New callers that need an explicit timestamp unit should use `insert/4'.
-spec insert(client(), binary(), [map()]) -> {ok, integer()} | {error, reason()}.
insert(Client, Table, Rows) ->
    call_sync(Client, ?CMD_INSERT, [Table, Rows]).

%% @doc Batch write data (blocking).
%%
%% `Precision' describes the unit of integer timestamps in `Rows'. It is used
%% when the target table is auto-created; existing tables keep their schema.
-spec insert(client(), binary(), [map()], time_unit()) ->
    {ok, integer()} | {error, reason()}.
insert(Client, Table, Rows, Precision) ->
    call_sync(Client, ?CMD_INSERT, [Table, Rows, Precision]).

%% @doc Batch write data with millisecond timestamps (asynchronous).
%%
%% New callers that need an explicit timestamp unit should use `insert_async/5'.
-spec insert_async(client(), binary(), [map()], callback()) ->
    {ok, pid()} | {error, reason()}.
insert_async(Client, Table, Rows, ResultCallback) ->
    call_async(Client, ?CMD_INSERT, [Table, Rows], ResultCallback).

%% @doc Batch write data (asynchronous).
-spec insert_async(client(), binary(), [map()], time_unit(), callback()) ->
    {ok, pid()} | {error, reason()}.
insert_async(Client, Table, Rows, Precision, ResultCallback) ->
    call_async(Client, ?CMD_INSERT, [Table, Rows, Precision], ResultCallback).

%% ===================================================================
%% Write - Execute Query
%% ===================================================================

%% @doc Execute SQL query (blocking).
-spec query(client(), sql()) -> {ok, result()} | {error, reason()}.
query(Client, Sql) ->
    call_sync(Client, ?CMD_EXECUTE, [Sql]).

%% @doc Execute SQL query (asynchronous).
-spec query_async(client(), sql(), callback()) -> {ok, pid()} | {error, reason()}.
query_async(Client, Sql, ResultCallback) ->
    call_async(Client, ?CMD_EXECUTE, [Sql], ResultCallback).

%% ===================================================================
%% Write - Streaming Write
%% ===================================================================

%% @doc Start a stream for a specific table.
%% Returns a StreamClient handle that binds the Table context to the Client.
%% It attempts to initialize the stream on all workers (best effort).
-spec stream_start(client(), binary(), map()) -> {ok, stream_client()}.
stream_start(?POOL_NAME(PoolName) = Client, Table, FirstRow) ->
    Deadline = deadline(Client, ?CMD_STREAM_START),
    Workers = ecpool:workers(PoolName),
    %% Attempt to pre-warm all workers. Ignore errors/ignored returns.
    lists:foreach(
        fun({_Name, Worker}) ->
            try
                {ok, Conn} = ecpool_worker:client(Worker),
                greptimedb_rs_sock:sync_command(
                    Conn, ?CMD_STREAM_START, [Table, FirstRow], Deadline
                )
            catch
                _:_ -> ok
            end
        end,
        Workers
    ),
    {ok, {stream_client, Client, Table}}.

%% @doc Stop the stream for a specific table.
%% Releases stream resources on all workers in the pool.
-spec stream_close(stream_client()) -> ok.
stream_close({stream_client, ?POOL_NAME(PoolName) = Client, Table}) ->
    Deadline = deadline(Client, ?CMD_STREAM_CLOSE),
    Workers = ecpool:workers(PoolName),
    lists:foreach(
        fun({_Name, Worker}) ->
            try
                {ok, Conn} = ecpool_worker:client(Worker),
                greptimedb_rs_sock:sync_command(Conn, ?CMD_STREAM_CLOSE, [Table], Deadline)
            catch
                _:_ -> ok
            end
        end,
        Workers
    ).

%% @doc Write data to the stream (blocking).
%% Uses ecpool to pick a connection from the pool.
%% The worker lazily initializes the stream writer if needed.
-spec stream_write(stream_client(), [map()]) -> ok | {error, term()}.
stream_write({stream_client, Client, Table}, Rows) ->
    call_sync(Client, ?CMD_STREAM_WRITE, [Table, Rows]).

%% @doc Write data to the stream (asynchronous).
-spec stream_write_async(stream_client(), [map()], callback()) ->
    {ok, pid()} | {error, reason()}.
stream_write_async({stream_client, Client, Table}, Rows, Callback) ->
    call_async(Client, ?CMD_STREAM_WRITE, [Table, Rows], Callback).

%% ===================================================================
%% Helpers
%% ===================================================================

call_sync(?POOL_NAME(PoolName) = Client, Cmd, Args) ->
    Deadline = deadline(Client, Cmd),
    ecpool:with_client(
        PoolName,
        fun(Conn) ->
            greptimedb_rs_sock:sync_command(Conn, Cmd, Args, Deadline)
        end
    ).

call_async(?POOL_NAME(PoolName) = Client, Cmd, Args, Callback) ->
    Deadline = deadline(Client, Cmd),
    ecpool:with_client(
        PoolName,
        fun(Conn) ->
            erlang:send(Conn, {async, Cmd, Args, Callback, Deadline}),
            {ok, Conn}
        end
    ).

deadline(Client, Cmd) ->
    Opts = maps:get(conn_opts, Client, #{}),
    Timeout =
        case Cmd of
            ?CMD_STREAM_START -> maps:get(stream_timeout, Opts, 30000);
            ?CMD_STREAM_WRITE -> maps:get(stream_timeout, Opts, 30000);
            ?CMD_STREAM_CLOSE -> maps:get(stream_timeout, Opts, 30000);
            _ -> maps:get(request_timeout, Opts, 10000)
        end,
    erlang:monotonic_time(microsecond) + Timeout * 1000.
