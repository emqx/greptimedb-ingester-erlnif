-module(greptimedb_rs_nif).

-export([
    init_runtime/0,
    connect/1,
    execute/2,
    insert/3,
    stream_start/3,
    stream_write/2,
    stream_close/1
]).

-export([init/0]).
-on_load(init/0).

init() ->
    NifName = "libgreptimedb_nif",
    Niflib = filename:join(priv_dir(), NifName),
    case erlang:load_nif(Niflib, none) of
        ok ->
            %% Initialize the Rust runtime immediately after loading
            case init_runtime() of
                true -> ok;
                %% already initialized
                false -> ok;
                Error -> Error
            end;
        {error, _Reason} = Res ->
            Res
    end.

%% =================================================================================================
%% NIFs

init_runtime() ->
    not_loaded(?LINE).

connect(_Opts) ->
    not_loaded(?LINE).

execute(_Client, _Sql) ->
    not_loaded(?LINE).

insert(_Client, _Table, _Rows) ->
    not_loaded(?LINE).

stream_start(_Client, _Table, _FirstRow) ->
    not_loaded(?LINE).

stream_write(_Writer, _Rows) ->
    not_loaded(?LINE).

stream_close(_Writer) ->
    not_loaded(?LINE).

%% =================================================================================================
%% Helpers

not_loaded(Line) ->
    erlang:nif_error({error, {not_loaded, [{module, ?MODULE}, {line, Line}]}}).

priv_dir() ->
    case code:priv_dir(?MODULE) of
        {error, _} ->
            EbinDir = filename:dirname(code:which(?MODULE)),
            AppPath = filename:dirname(EbinDir),
            filename:join(AppPath, "priv");
        Path ->
            Path
    end.
