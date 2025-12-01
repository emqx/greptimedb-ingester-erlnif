# GreptimeDB Ingester Erlang Library

An Erlang library for GreptimeDB using the Rust Ingester SDK via NIFs.

## Building

```bash
rebar3 compile
```

## Usage

### Connecting

```erlang
Opts = #{
    endpoints => [<<"127.0.0.1:4001">>],
    dbname => <<"public">>
},
{ok, Client} = greptimedb:start_client(Opts).
```

### Writing (Stubbed)

```erlang
% Metric/Table name, and list of maps (rows)
Table = <<"my_table">>,
Rows = [
  #{<<"column1">> => 1, <<"column2">> => <<"value">>}
],
{ok, _} = greptimedb:write(Client, Table, Rows).
```
