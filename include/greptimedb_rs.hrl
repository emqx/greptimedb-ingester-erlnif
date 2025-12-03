-type opts() :: #{
    endpoints := [binary()],
    dbname := binary(),
    username => binary(),
    password => binary(),
    tls => boolean(),
    ca_cert => binary(),
    client_cert => binary(),
    client_key => binary()
}.
-type conn() :: pid().
-type client() :: #{pool_name := atom() | binary(), conn_opts := map()}.
-type stream_client() :: {stream_client, client(), table()}.
-type client_ref() :: reference().
-type table() :: binary().
-type sql() :: binary().
-type result() :: term().
-type reason() :: term() | binary().

-type args() :: term().
-type callback() :: {function(), list()}.

%% The commands are also the names of the NIF functions.
-define(cmd_connect, connect).
-define(cmd_execute, execute).
-define(cmd_insert, insert).
-define(cmd_stream_start, stream_start).
-define(cmd_stream_write, stream_write).
-define(cmd_stream_close, stream_close).

-type command() ::
    ?cmd_connect
    | ?cmd_execute
    | ?cmd_insert
    | ?cmd_stream_start
    | ?cmd_stream_write
    | ?cmd_stream_close.

-define(SOCK_MODULE, greptimedb_rs_sock).
-define(NIF_MODULE, greptimedb_rs_nif).
-define(REQ(Func, Args), {sync, Func, Args}).
-define(ASYNC_REQ(Func, Args, Callback), {async, Func, Args, Callback}).
