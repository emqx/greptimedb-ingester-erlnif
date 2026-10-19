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
    runtime_start/2,
    runtime_stop/1,
    runtime_stop/2,
    connect/1,
    disconnect/1,
    drain/1,
    submit/5,
    cancel/1,
    operation_active/1,
    resource_counts/0,
    codec_counts/0,
    discard_writer/1,
    fips_status/0,
    cached_fips_status/0
]).

-export([init/0]).
-on_load(init/0).

init() ->
    NifName = "libgreptimedb_nif",
    Niflib = filename:join(priv_dir(), NifName),
    case erlang:load_nif(Niflib, none) of
        ok -> ok;
        {error, {already_loaded, _}} -> ok
    end.

%% =================================================================================================
%% NIFs

runtime_start(_Workers, _CodecWorkers) ->
    not_loaded(?LINE).

runtime_stop(Runtime) ->
    runtime_stop(Runtime, 5000).

runtime_stop(_Runtime, _Timeout) ->
    not_loaded(?LINE).

connect(_Opts) ->
    not_loaded(?LINE).

disconnect(_Client) ->
    not_loaded(?LINE).

drain(_Client) ->
    not_loaded(?LINE).

submit(Client, Command, Args, Token, DeadlineUs) ->
    case small_submission(Args, Token) of
        true -> submit_small(Client, Command, Args, Token, DeadlineUs);
        false -> submit_dirty(Client, Command, Args, Token, DeadlineUs)
    end.

%% Check structure without copying binary contents, then bound their total
%% size with constant-time byte_size/1 before copying input on a normal scheduler.
small_submission(Args, Token) ->
    case small_token(Token) andalso bounded_submission(Args) of
        false -> false;
        Binaries -> small_binaries(Binaries, 16384)
    end.

small_binaries([], _) ->
    true;
small_binaries([Binary | Rest], Budget) ->
    Size = byte_size(Binary),
    Size =< Budget andalso small_binaries(Rest, Budget - Size).

small_token(Token) when is_reference(Token) -> true;
small_token({Generation, Ref}) -> is_reference(Generation) andalso is_reference(Ref);
small_token(_) -> false.

bounded_submission(_Args) ->
    not_loaded(?LINE).

submit_small(_Client, _Command, _Args, _Token, _DeadlineUs) ->
    not_loaded(?LINE).

submit_dirty(_Client, _Command, _Args, _Token, _DeadlineUs) ->
    not_loaded(?LINE).

cancel(_Operation) ->
    not_loaded(?LINE).

operation_active(_Operation) ->
    not_loaded(?LINE).

codec_counts() ->
    not_loaded(?LINE).

resource_counts() ->
    not_loaded(?LINE).

discard_writer(_Writer) ->
    not_loaded(?LINE).

fips_status() ->
    not_loaded(?LINE).

%% =================================================================================================
%% Helpers

-define(FIPS_CACHE_KEY, {?MODULE, fips_status}).
%% 1 hour
-define(FIPS_CACHE_TTL_MS, 3600000).

cached_fips_status() ->
    Now = erlang:system_time(millisecond),
    case persistent_term:get(?FIPS_CACHE_KEY, undefined) of
        {Value, Timestamp} when Now - Timestamp < ?FIPS_CACHE_TTL_MS ->
            Value;
        _ ->
            Value = fips_status(),
            persistent_term:put(?FIPS_CACHE_KEY, {Value, Now}),
            Value
    end.

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
