%% Commands handled by the connection process.
-define(CMD_CONNECT, connect).
-define(CMD_DISCONNECT, disconnect).
-define(CMD_EXECUTE, execute).
-define(CMD_INSERT, insert).
-define(CMD_STREAM_START, stream_start).
-define(CMD_STREAM_WRITE, stream_write).
-define(CMD_STREAM_CLOSE, stream_close).

-define(SOCK_MODULE, greptimedb_rs_sock).
-define(NIF_MODULE, greptimedb_rs_nif).
-define(REQ(Func, Args), {sync, Func, Args}).
-define(ASYNC_REQ(Func, Args, Callback), {async, Func, Args, Callback}).
