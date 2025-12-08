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
