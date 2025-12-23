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

-module(greptimedb_rs_nif).

-export([
    connect/1,
    disconnect/1,
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
    erlang:load_nif(Niflib, none).

%% =================================================================================================
%% NIFs

connect(_Opts) ->
    not_loaded(?LINE).

disconnect(_Client) ->
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
