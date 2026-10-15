# GreptimeDB Ingester Erlang Library (NIF)

High-performance Erlang client for [GreptimeDB](https://greptime.com/), built on top of the official Rust Ingester SDK using NIFs (Native Implemented Functions). This library is designed to offer superior performance by leveraging the Rust SDK's efficiency while providing a friendly Erlang API.

## Features

- **High Performance**: Direct binding to the Rust Ingester SDK with one shared Tokio runtime and asynchronous network tasks.
- **Async & Sync API**: Flexible API supporting both blocking calls and non-blocking async callbacks.
- **Stream Ingestion**: Efficient streaming support for high-throughput data ingestion.
- **Connection Pooling**: Robust connection management implemented in Erlang using ecpool.
- **SQL Execution**: Execute DDL and SQL queries directly.
- **Full Type Support**: Supports all GreptimeDB data types including Int8-64, Float32/64, Boolean, String, Binary, Date, Datetime, and Timestamps with various precisions.

## Installation

Add `greptimedb_rs` to your `rebar.config` dependencies:

```erlang
{deps, [
    {greptimedb_rs, {git, "https://github.com/emqx/greptimedb-ingester-erlnif", {branch, "main"}}}
]}.
```

## Basic Usage

### 1. Start the Application

Ensure the application is started before using it:

```erlang
application:ensure_all_started(greptimedb_rs).
```

### 2. Connect to GreptimeDB

Initialize the client with your GreptimeDB endpoint configuration.

**Basic Connection:**

```erlang
Opts = #{
    endpoints => [<<"127.0.0.1:4001">>], % List of gRPC endpoints
    dbname => <<"public">>,
    pool_size => 8,       % Optional: Number of pooled client instances
    pool_type => random   % Optional: 'random' or 'round_robin'
},
{ok, Client} = greptimedb_rs:start_client(Opts).
```

**Additional Insert Options:**

`start_client/1` also accepts:

- `ts_column` (optional): Timestamp column name used when auto-creating tables. Default is `<<"ts">>`.
- `ttl` (optional): TTL hint for auto-created tables. Example: `<<"3 days">>`.

**Operation Timeouts:**

- `request_timeout`: Positive milliseconds for query and insert; default `10000`.
- `stream_timeout`: Positive milliseconds for stream start, write and close; default `30000`.

RPC timeouts are controlled by the operation deadline rather than the SDK's default 10-second request timeout. The SDK retains its connection establishment timeout. The streaming writer's internal timeout follows `stream_timeout`; the operation deadline also includes queueing and conversion time.

The deadline starts when the public API accepts the call and includes library queueing, schema discovery, RPC and response consumption. Query/insert/write timeouts return `{error, timeout}` or invoke the existing asynchronous callback with that result. Stream start and close retain their existing best effort public return semantics.

Timeouts cancel local work and do not undo writes already accepted by the server. Uncertain writes are not automatically retried. After a streaming write timeout or error, establish a new writer using `stream_start/3` before writing again.

### Shared runtime

All pools share one Tokio runtime. Defaults are two Tokio workers, one owner thread for non-`Send` SDK tasks, and two codec workers for row conversion and large query result encoding.

Configure worker counts before starting the application:

```erlang
application:load(greptimedb_rs),
application:set_env(greptimedb_rs, runtime_workers, 2),
application:set_env(greptimedb_rs, codec_workers, 2),
application:ensure_all_started(greptimedb_rs).
```

Both settings accept 1–64 and take effect on application restart. The codec pool accepts at most `2 * codec_workers` queued and running jobs; additional operations wait asynchronously within their deadline. Running codec work finishes before its client slot is released after cancellation.

Application startup returns an error when native runtime creation fails. A poisoned native client state lock closes that client and cancels its active request while retaining codec work until reclamation. The affected pool connection must be recreated. Async callback exceptions are logged as `greptimedb_callback_failed`; the worker continues processing requests.

Stopping a pool cancels its active request and attempts to finish previously submitted
stream batches before releasing writers. This wait uses `stream_timeout`, but ecpool
can force termination earlier (currently about 300ms per connection). Call
`stream_close/1` explicitly before stopping when more time is needed; its existing
public return value still suppresses finish errors.

Normal application shutdown finishes remaining stream writers concurrently before
closing the runtime. The application setting `shutdown_timeout` provides one total
deadline starting when native shutdown is signalled (default `5000`ms, range
`1`–`30000`ms). In-flight stream write/close operations share this deadline;
queries and inserts are cancelled. Expiry cancels unfinished drains; failed writes
are not retried.
Running codec jobs still need to finish safely, so this setting does not impose a
hard limit on total shutdown time. Abnormal runtime owner exits skip writer draining.
Clients with `auto_reconnect` can reconnect after a runtime owner restart.

**Connection with Authentication:**

```erlang
Opts = #{
    endpoints => [<<"127.0.0.1:4001">>],
    dbname => <<"public">>,
    username => <<"greptime_user">>,
    password => <<"greptime_pwd">>
},
{ok, Client} = greptimedb_rs:start_client(Opts).
```

**Connection with TLS:**

```erlang
Opts = #{
    endpoints => [<<"127.0.0.1:4001">>],
    dbname => <<"public">>,
    tls => true,
    % Optional: Paths to certificates
    ca_cert => <<"/path/to/ca.crt">>,
    client_cert => <<"/path/to/client.crt">>,
    client_key => <<"/path/to/client.key">>
},
{ok, Client} = greptimedb_rs:start_client(Opts).
```

Notes:
- `ca_cert`, `client_cert`, and `client_key` are optional configuration fields.
- `ca_cert` may still be required to verify the server certificate when the server uses a self-signed certificate or a private CA. If the server certificate chains to a CA already trusted by the system, `ca_cert` can usually be omitted.
- `client_cert` and `client_key` must be provided together when enabling mTLS.

### 3. Prepare Data

Rows are represented as maps with specific atom keys:
- **Required**: `timestamp` (or `ts`) - timestamp value
- **Optional**: `tags` - map of tag columns
- **Required**: `fields` - map of field columns

**Important**: The top-level keys (`:timestamp` or `:ts`, `:tags`, `:fields`) must be atoms for optimal performance. Column names within `:tags` and `:fields` should be binaries.

```erlang
Row = #{
    timestamp => os:system_time(millisecond),  % or use 'ts' instead
    tags => #{
        <<"host">> => <<"server-01">>,
        <<"region">> => <<"us-west">>
    },
    fields => #{
        <<"cpu_usage">> => 85.5,
        <<"memory_usage">> => 1024,
        <<"is_active">> => true
    }
}.
```

### 4. Insert Data

#### Synchronous Insert
Blocks the calling process until the insert is confirmed.

```erlang
Table = <<"system_metrics">>,
Rows = [Row], % Insert a list of rows
{ok, AffectedRows} = greptimedb_rs:insert(Client, Table, Rows).
```

#### Asynchronous Insert
Returns immediately with the connection pid. The provided callback is executed upon completion.

```erlang
% Define a callback function
% Callback signature: fun(CallbackArgs..., Result)
Callback = {fun(Ref, Result) ->
    io:format("Insert finished for ~p: ~p~n", [Ref, Result])
end, [make_ref()]},

% Returns {ok, ConnPid} where ConnPid is the connection process handling the request
{ok, ConnPid} = greptimedb_rs:insert_async(Client, Table, Rows, Callback).
```

### 5. Schema-less Insertion & Safety

The library leverages the schema-less API of the Rust SDK to simplify data writing while ensuring data integrity.

-   **Automatic Schema Inference**: If the target table does not exist, the SDK automatically infers the schema from the provided data and creates the table.
-   **Strict Type Validation**: If the table exists, the provided data is strictly validated against the server's schema.
-   **Conflict Handling**:
    -   **Type Mismatch**: Inserting incompatible types (e.g., a String into an Integer column) will return an explicit error.
    -   **Integer Overflow**: Values exceeding the range of the target column (e.g., inserting `1000` into an `Int8` column) will strictly return an error `{error, {nif_error, Reason}}`, preventing silent data corruption or `nil` insertion.

## Streaming Usage

Streaming is recommended for high-volume data ingestion. It establishes a persistent stream to the server.

### 1. Start a Stream
Initialize a stream. The schema is automatically inferred from the `FirstRow` provided (or fetched from the server if the table exists).

```erlang
{ok, Stream} = greptimedb_rs:stream_start(Client, Table, Row).
```

### 2. Write to Stream

```erlang
% Synchronous Write - blocks until complete
ok = greptimedb_rs:stream_write(Stream, Rows).

% Asynchronous Write - returns immediately with connection pid
{ok, ConnPid} = greptimedb_rs:stream_write_async(Stream, Rows, Callback).
```

### 3. Close Stream
Always close the stream to flush any buffered data and release resources.

```erlang
greptimedb_rs:stream_close(Stream).
```

## Executing SQL

You can execute SQL statements (like `CREATE TABLE`, `DROP TABLE`, or `SELECT`) using the `query/2` function. The API ensures that the returned result set is formatted into correct Erlang terms (e.g., integers, floats, binaries) that strictly match the database column types.

### Synchronous Query

**DDL (Data Definition Language):**

```erlang
Sql = <<"CREATE TABLE IF NOT EXISTS system_metrics (
    ts TIMESTAMP TIME INDEX,
    host STRING,
    cpu_usage DOUBLE,
    PRIMARY KEY(host)
) ENGINE=mito">>,

{ok, _} = greptimedb_rs:query(Client, Sql).
```

**DQL (Data Query Language):**

```erlang
Query = <<"SELECT * FROM system_metrics LIMIT 10">>,
{ok, Result} = greptimedb_rs:query(Client, Query).
```

### Asynchronous Query

Returns immediately with the connection pid. The provided callback is executed upon completion.

```erlang
Callback = {fun(Ref, Result) ->
    io:format("Query finished for ~p: ~p~n", [Ref, Result])
end, [make_ref()]},

{ok, ConnPid} = greptimedb_rs:query_async(Client, Sql, Callback).
```

## Supported Data Types

The library supports automatic mapping from Erlang terms to GreptimeDB types based on the table schema.

| GreptimeDB Type  | Erlang Type                                 | Example                   |
|:-----------------|:--------------------------------------------|:--------------------------|
| `String`         | Binary / String                             | `<<"hello">>` / `"hello"` |
| `Boolean`        | Boolean                                     | `true`, `false`           |
| `Int8/16/32/64`  | Integer                                     | `123`, `-456`             |
| `UInt8/16/32/64` | Integer                                     | `123`                     |
| `Float32/64`     | Float                                       | `123.45`                  |
| `Binary`         | Binary                                      | `<<1, 2, 3>>`             |
| `Date`           | Integer (Days since epoch)                  | `19700`                   |
| `Datetime`       | Integer (Milliseconds since epoch)          | `1678888888000`           |
| `Timestamp`      | Integer (Units depend on column definition) | `1678888888000`           |

## Performance Tips

For optimal performance, consider these best practices:

1. **Use Large Batches**: Batch 5,000-10,000 rows per insert for best throughput. Larger batches reduce NIF boundary crossing overhead.

2. **Use Async Operations**: For high-throughput scenarios, use `insert_async/4` or `stream_write_async/3` with connection pooling to achieve maximum parallelism.

3. **Use Atom Keys**: Always use atom keys (`:timestamp`, `:ts`, `:tags`, `:fields`) in row maps. Binary keys are no longer supported and atom keys provide ~50% faster map lookups.

4. **Connection Pooling**: Each pool worker owns a client instance; all pools share the library Tokio runtime. `pool_size` controls client concurrency (default: 8); the SDK manages the underlying TCP/gRPC connections.

5. **Stream for Bulk Ingestion**: For continuous high-volume writes, use streaming API (`stream_start/3`, `stream_write/2`) which maintains persistent connections and reduces connection overhead.

### Benchmark Results

Typical performance on a modern system (using 1M rows total):

| Operation                      | Throughput (rows/s) |
|:-------------------------------|--------------------:|
| `insert` (pool=1)              |            260,000+ |
| `insert_async` (pool=16)       |            935,000+ |
| `stream_write_async` (pool=16) |            818,000+ |

**Note**: Performance varies based on network latency, server capacity, data complexity, and batch size.

## Cleanup

Stop the client to close all connections.

```erlang
greptimedb_rs:stop_client(Client).
```

## Development checks

`make fmt` formats Erlang code and checks Rust formatting. `make lint` runs Elvis
against Erlang sources, headers, tests and `rebar.config`; rules are in
`elvis.config`. GitHub CI checks both formatting and Elvis rules.

## Regression tests

Run the real network fault, runtime lifecycle and codec cancellation tests using a GreptimeDB executable:

```bash
make regression GREPTIME_BIN=/path/to/greptime
```

The Common Test suite starts and stops its own local database and transparent HTTP/2 fault proxy. Results use the standard Common Test report. Existing TCP, TLS and authentication suites run with `make ct-docker`.
