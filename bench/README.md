# Performance regression benchmark

`row_insert_bench.erl` exercises the public Erlang API:

- `row_single`, `row_batch`, `row_async`, `row_mixed`
- `bulk_dense`, `bulk_sparse`

It verifies row counts, counter sums, and mixed-presence NULL counts before
reporting a result.

## Run

Start GreptimeDB 1.2.1, build the revision as a release NIF, then compile and
run the benchmark:

```bash
export PATH="$HOME/.local/share/mise/installs/erlang/27.3.4.2-8/bin:$PATH"
erlc -o /tmp bench/row_insert_bench.erl
ERL_LIBS="$PWD/_build/default/lib" \
BENCH_MODE=row_batch \
BENCH_LABEL=local_1 \
BENCH_ENDPOINT=127.0.0.1:4001 \
erl +S 4:4 -noshell -pa /tmp \
  -eval 'row_insert_bench:run(), halt().'
```

Set `BENCH_N=5000` for `row_batch`, `row_async`, or `row_mixed` to increase the
per-batch signal; the default is 1,000 rows.

## Comparison method

1. Build the base and candidate revisions in separate worktrees.
2. Use the same GreptimeDB instance, `pool_size=1`, and `+S 4:4`.
3. Run at least three independent Erlang VMs per revision/workload, alternating
   old/new order.
4. Report medians and raw samples, not a single run.
5. Keep the correctness checks enabled; a benchmark failure is not a valid
   performance sample.

Suggested acceptance budgets relative to the base:

- `row_batch`, `row_async`: <= 10%
- `row_mixed`: <= 15%
- `row_single`: within +/-5%
- `bulk_dense`: <= 5%

`bulk_sparse` is tracked separately in
GreptimeTeam/greptimedb-ingester-rust#63 and is not part of the ordinary
row-insert budget.
