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
    insert_async/4
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
    opts/0
]).

%% ===================================================================
%% Type defs
%% ===================================================================

-define(pool_name(PoolName), #{pool_name := PoolName}).

-type opts() :: #{
    endpoints := [binary()],
    dbname := binary(),
    username => binary(),
    password => binary(),
    tls => boolean(),
    ca_cert => binary(),
    client_cert => binary(),
    client_key => binary(),
    pool_name => pool_name(),
    pool_size => pool_size(),
    pool_type => pool_type(),
    any() => term()
}.
-type client() :: #{
    pool_name := pool_name(),
    pool_size := pool_size(),
    pool_type := pool_type(),
    conn_opts := opts()
}.
-type stream_client() :: {stream_client, client(), table()}.
-type table() :: binary().
-type sql() :: binary().
-type result() :: term().
-type reason() :: term() | binary().

-type pool_name() :: term().
-type pool_type() :: random | hash.
-type pool_size() :: pos_integer().

-type callback() :: {function(), list()}.

%% ===================================================================
%% Connection and Disconnection
%% ===================================================================

-doc """
Start the connection pool (using ecpool).
Returns a Client (logic level) that holds the connection pool.
""".
-spec start_client(opts()) -> {ok, client()} | {error, {already_started, client()} | term()}.
start_client(Opts) ->
    PoolName = maps:get(pool_name, Opts, greptimedb_rs_pool),
    PoolSize = maps:get(pool_size, Opts, 8),
    PoolType = maps:get(pool_type, Opts, random),

    PoolOpts = [{pool_size, PoolSize}, {pool_type, PoolType}, {conn_opts, Opts}],

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

-doc """
Stop the connection pool and release resources.
""".
-spec stop_client(client() | pool_name()) -> ok.
stop_client(?pool_name(PoolName)) ->
    ecpool:stop_sup_pool(PoolName);
stop_client(PoolName) ->
    ecpool:stop_sup_pool(PoolName).

%% ===================================================================
%% Write - Batch Write
%% ===================================================================

-doc """
Batch write data (blocking).
Picks a connection from the pool to do the write operation.
""".
-spec insert(client(), binary(), [map()]) -> {ok, integer()} | {error, reason()}.
insert(Client, Table, Rows) ->
    call_sync(Client, ?cmd_insert, [Table, Rows]).

-doc """
Batch write data (asynchronous).
""".
-spec insert_async(client(), binary(), [map()], callback()) -> ok.
insert_async(Client, Table, Rows, ResultCallback) ->
    call_async(Client, ?cmd_insert, [Table, Rows], ResultCallback).

%% ===================================================================
%% Write - Execute Query
%% ===================================================================

-doc """
Execute SQL query (blocking).
""".
-spec query(client(), sql()) -> {ok, result()} | {error, reason()}.
query(Client, Sql) ->
    call_sync(Client, ?cmd_execute, [Sql]).

-doc """
Execute SQL query (asynchronous).
""".
-spec query_async(client(), sql(), callback()) -> ok.
query_async(Client, Sql, ResultCallback) ->
    call_async(Client, ?cmd_execute, [Sql], ResultCallback).

%% ===================================================================
%% Write - Streaming Write
%% ===================================================================

-doc """
Start a stream for a specific table.
Returns a StreamClient handle that binds the Table context to the Client.
It attempts to initialize the stream on all workers (best effort).
""".
-spec stream_start(client(), binary(), map()) -> {ok, stream_client()}.
stream_start(?pool_name(PoolName) = Client, Table, FirstRow) ->
    Workers = ecpool:workers(PoolName),
    %% Attempt to pre-warm all workers. Ignore errors/ignored returns.
    lists:foreach(
        fun({_Name, Worker}) ->
            try
                {ok, Conn} = ecpool_worker:client(Worker),
                greptimedb_rs_sock:sync_command(Conn, ?cmd_stream_start, [Table, FirstRow])
            catch
                _:_ -> ok
            end
        end,
        Workers
    ),
    {ok, {stream_client, Client, Table}}.

-doc """
Stop the stream for a specific table.
Releases stream resources on all workers in the pool.
""".
-spec stream_close(stream_client()) -> ok.
stream_close({stream_client, ?pool_name(PoolName), Table}) ->
    Workers = ecpool:workers(PoolName),
    lists:foreach(
        fun({_Name, Worker}) ->
            try
                {ok, Conn} = ecpool_worker:client(Worker),
                greptimedb_rs_sock:sync_command(Conn, ?cmd_stream_close, [Table])
            catch
                _:_ -> ok
            end
        end,
        Workers
    ).

-doc """
Write data to the stream (blocking).
Uses ecpool to pick a connection from the pool.
The worker lazily initializes the stream writer if needed.
""".
-spec stream_write(stream_client(), [map()]) -> ok | {error, term()}.
stream_write({stream_client, ?pool_name(PoolName), Table}, Rows) ->
    ecpool:with_client(PoolName, fun(Conn) ->
        greptimedb_rs_sock:sync_command(Conn, ?cmd_stream_write, [Table, Rows])
    end).

-doc """
Write data to the stream (asynchronous).
""".
-spec stream_write_async(stream_client(), [map()], callback()) -> ok.
stream_write_async({stream_client, ?pool_name(PoolName), Table}, Rows, Callback) ->
    ecpool:with_client(PoolName, fun(Conn) ->
        erlang:send(Conn, ?ASYNC_REQ(?cmd_stream_write, [Table, Rows], Callback)),
        ok
    end).

%% ===================================================================
%% Helpers
%% ===================================================================

call_sync(?pool_name(PoolName), Cmd, Args) ->
    ecpool:with_client(
        PoolName,
        fun(Conn) ->
            greptimedb_rs_sock:sync_command(Conn, Cmd, Args)
        end
    ).

call_async(?pool_name(PoolName), Cmd, Args, Callback) ->
    ecpool:with_client(
        PoolName,
        fun(Conn) ->
            erlang:send(Conn, ?ASYNC_REQ(Cmd, Args, Callback)),
            ok
        end
    ).
