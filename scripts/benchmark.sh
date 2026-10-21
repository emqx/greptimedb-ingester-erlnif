#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
root=$PWD
: "${GREPTIME_BIN:?Set GREPTIME_BIN to the GreptimeDB executable}"
greptime_bin=$(realpath "$GREPTIME_BIN")
bench_app=$(realpath "${BENCH_APP:-_build/default/lib/greptimedb_rs}")
baseline_app=${BASELINE_APP:+$(realpath "$BASELINE_APP")}
rounds=${BENCH_ROUNDS:-3}
[[ $(uname -s) == Linux ]] || { echo 'Benchmark metrics require Linux /proc' >&2; exit 1; }
[[ $rounds =~ ^[1-9]$|^10$ ]] || { echo 'BENCH_ROUNDS must be 1–10' >&2; exit 1; }
out=${BENCH_OUT:-$(mktemp -d /tmp/greptimedb-benchmark.XXXXXX)}
mkdir -p "$out"
out=$(realpath "$out")
[[ ! -e $out/results.csv ]] || { echo "Results already exist: $out" >&2; exit 1; }
erl=${ERL:-erl}
erlc=${ERLC:-erlc}
"$erlc" -o "$out" test/greptimedb_benchmark.erl
export BENCH_CLOCK_TICKS=$(getconf CLK_TCK)
export TOKIO_WORKER_THREADS=${TOKIO_WORKER_THREADS:-$(getconf _NPROCESSORS_ONLN)}

cases=(query_small_serial query_small_concurrent insert_single insert100_serial
       insert100_concurrent insert5000 insert_async100 stream100 stream5000 query20000)
if [[ -n ${BENCH_CASE:-} ]]; then
    [[ " ${cases[*]} " == *" $BENCH_CASE "* ]] || { echo 'Unknown BENCH_CASE' >&2; exit 1; }
    cases=("$BENCH_CASE")
fi
printf '%s\n' 'variant,round,case,mode,batch,concurrency,ops,elapsed_us,ops_s,rows_s,p50_us,p95_us,p99_us,client_cpu_ms,server_cpu_ms,ready_rss_kb,peak_rss_kb,ready_threads,peak_threads,native_threads,pool_idle_rss_kb,pool_idle_threads,pool_idle_native_threads' > "$out/results.csv"
{
    printf 'driver_commit=%s\n' "$(git rev-parse HEAD)"
    printf 'driver_diff_sha256=%s\n' "$(git diff HEAD | sha256sum | cut -d ' ' -f 1)"
    printf 'greptime_bin=%s\nrounds=%s\n' "$greptime_bin" "$rounds"
    printf 'pool=%s runtime_workers=%s codec_workers=%s\n' "${BENCH_POOL_SIZE:-4}" "${BENCH_RUNTIME_WORKERS:-2}" "${BENCH_CODEC_WORKERS:-2}"
    printf 'vm_flags=+S 4:4 +SDcpu 2:2 +SDio 4\nTOKIO_WORKER_THREADS=%s\n' "$TOKIO_WORKER_THREADS"
    sha256sum "$bench_app/priv/libgreptimedb_nif.so"
    if [[ -n $baseline_app ]]; then sha256sum "$baseline_app/priv/libgreptimedb_nif.so"; fi
} > "$out/metadata.txt"

server_pid=
cleanup() {
    if [[ -n $server_pid ]]; then
        kill "$server_pid" 2>/dev/null || true
        for ((attempt=0; attempt<100; attempt++)); do
            kill -0 "$server_pid" 2>/dev/null || break
            sleep 0.1
        done
        if kill -0 "$server_pid" 2>/dev/null; then kill -KILL "$server_pid" 2>/dev/null || true; fi
        wait "$server_pid" 2>/dev/null || true
        server_pid=
    fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

run_case() {
    local variant=$1 round=$2 scenario=$3 app=$4 directory rpc http attempt ready=0
    directory="$out/$round-$scenario-$variant"
    mkdir "$directory"
    read -r rpc http < <(ERL_FLAGS= ERL_AFLAGS= "$erl" +S 1:1 +SDcpu 1:1 +SDio 1 -noshell -pa "$out" -s greptimedb_benchmark ports)
    cat > "$directory/greptime.toml" <<EOF
[grpc]
bind_addr = "127.0.0.1:$rpc"
runtime_size = 4
[http]
addr = "127.0.0.1:$http"
[mysql]
enable = false
[postgres]
enable = false
EOF
    "$greptime_bin" standalone start -c "$directory/greptime.toml" \
        --data-home "$directory/data" --log-dir "$directory/logs" --log-level warn \
        > "$directory/server.log" 2>&1 &
    server_pid=$!
    for ((attempt=0; attempt<300; attempt++)); do
        kill -0 "$server_pid" 2>/dev/null || { cat "$directory/server.log" >&2; return 1; }
        if curl --noproxy '*' --max-time 1 -fsS "http://127.0.0.1:$http/health" >/dev/null 2>&1; then
            ready=1
            break
        fi
        sleep 0.1
    done
    [[ $ready == 1 ]] || { echo "Database startup timed out: $directory" >&2; return 1; }
    BENCH_ENDPOINT="127.0.0.1:$rpc" BENCH_SERVER_PID=$server_pid BENCH_CASE=$scenario \
        ERL_FLAGS= ERL_AFLAGS= timeout 240 "$erl" +S 4:4 +SDcpu 2:2 +SDio 4 -noshell \
        -pa "$out" "$app/ebin" "$root/_build/default/lib/ecpool/ebin" \
        "$root/_build/default/lib/gproc/ebin" -s greptimedb_benchmark run \
        > "$directory/client.log" 2>&1 || { cat "$directory/client.log" >&2; return 1; }
    [[ $(grep -c '^RESULT,' "$directory/client.log") == 1 ]]
    awk -v variant="$variant" -v round="$round" '/^RESULT,/ {sub(/^RESULT,/, ""); print variant "," round "," $0}' \
        "$directory/client.log" | tee -a "$out/results.csv"
    cleanup
}

for ((round=1; round<=rounds; round++)); do
    index=0
    for scenario in "${cases[@]}"; do
        if [[ -z $baseline_app ]]; then
            run_case current "$round" "$scenario" "$bench_app"
        elif (( (round + index) % 2 )); then
            run_case baseline "$round" "$scenario" "$baseline_app"
            run_case current "$round" "$scenario" "$bench_app"
        else
            run_case current "$round" "$scenario" "$bench_app"
            run_case baseline "$round" "$scenario" "$baseline_app"
        fi
        index=$((index + 1))
    done
done
printf 'Results: %s/results.csv\n' "$out"
