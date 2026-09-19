# Local frontier: checkpoint retirement and query-cache lifetime

**No Timescale win, Railway qualification, production SLO or independent certification is claimed.** These are macOS real-service measurements, not dedicated-CPU/cgroup-matched tests. The broad pre-existing worktree was preserved; no commit, push, package publication or cloud activation occurred in this campaign.

## What changed

- Bounded opt-in ingest traces correlate durable receipt sequences with writer phases. Trace-off remains the default.
- Frozen-prefix checkpoint retirement releases the writer/state gates before obsolete-WAL directory sync and private destruction. The directory barrier, dependency pins and append acknowledgment barriers remain. A deterministic blocked-writer regression failed before the fix and passes afterward.
- Reusable DuckDB input cache admission is separate from the unchanged 128-MiB one-shot input limit. Its allowance is `min(128 MiB, worker_memory / 4)`; this is an estimate, not an allocator guarantee.
- A live-byte limit alone was insufficient: repeated raw/dynamic replacement could leave a child alive through unbounded cumulative materialization. A second ledger retains these charges until child retirement. Admission selects a fresh child before that ledger is exceeded; no failed SQL is retried. Validated unchanged hits incur no new charge. Both replacement regressions were witnessed red/green.

## Final optimized probes

Same profile: 256 MiB per DuckDB worker, two query threads/workers, retained inputs enabled, unchanged ingestion/durability/maintenance settings. Batch 1,000; four writers; 30-second independently scheduled write interval. Every successful receipt requires `local_fsync`. Queries jointly check changing raw and rollup aggregates. Independent count/sum/min/max checks cover the acknowledged dataset after drain.

| Probe | Final checked rows | Offered mixed rows | Acknowledged | Driver-dropped before submission | Failed/ambiguous | Verdict |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 5,000 rows/s, 307,200 preload, tracing on | 457,200 | 150,000 | 150,000 | 0 | 0 | pass |
| 15,000 rows/s, 102,400 preload, tracing off | 546,400 | 450,000 | 444,000 | 6,000 | 0 | **overloaded — not a pass** |

The first probe crossed the earlier final-query OOM size without increasing memory. The second completed aggregate checks without SQL errors, but did not sustain its offered workload. Dropped work was load-generator queue shedding, not acknowledged data loss. All submitted writes drained and owned services were killed/reaped. Trace-on and trace-off timings are not compared as an optimization A/B.

The remaining cost is explicit: the two mixed intervals recorded **25.20 s / 24.77 s of `query_build`**, versus **1.94 s / 1.89 s of `query_run`**. These are elapsed phase totals, not exclusive CPU accounting. Repeated input staging/materialization remains the dominant query-boundary target; correct bounded retirement does not make that work cheap.

Write sample counts were 150 and 444; read counts were 26 and 24. Neither qualifies p99. Reads are one closed-loop reader with a think interval, **not independent scheduled read arrivals**. The compound fresh query also exercises broader planning/catalog exposure than the prefix probe; this is not an assertion of workload parity with earlier Railway runs.

## Qualification and identity

Full locked/offline fault-injection workspace suite, strict Clippy with/without fault injection, formatting, the service metric witness and actual Rust SDK/server smoke passed. Fifty query unit tests passed; 31 Python driver tests passed. Logs/exit receipts are under `checks/`.

- Optimized binary SHA256: `ebed1b9e75255fd4c98ac6855fb9b250fee998803584586a90dbdc762c41c922`
- Profile SHA256: `23ae83270630d881127817f0a39547cd327144635a99597c476374a6401b675c`
- Source manifest SHA256: `84db9e135661f76584d0bc911cb0d707afd26fa7b02a06572ba2171d50b5bd1c`
- Source archive SHA256: `b9555e1e9ca06e640329cee9004fd0cef993b0b713b368e72cb9237b56e2b248`

The archive contains 133 candidate input files, not binaries or database data. Compiler/build identity is recorded. Source equality was checked before/after final probes. Witness scripts preserve actual local paths; use the archived parameterized frontier driver for another environment and disclose path/resource differences.

`manifest.json` hashes the included artifacts. Verify with:

```sh
node docs/evidence/frontier-jszmov/verify.mjs
```

Historical failed/overloaded reports remain under `runs/`. `baseline-http-01` timing is explicitly rejected because oracle setup was initially charged inside the arrival interval. Cache-disabled diagnosis is not a fair speed comparison. Debug probes are not compared with optimized binaries. The earlier planned fresh A–B–B–A comparison stopped at baseline OOM; it was not completed or relabeled successful.

**Next gate:** reduce query input rematerialization, test independently offered mixed reads/writes and repeated matched workloads, then fresh Linux/runtime and source-bound Railway qualification. Old cloud approvals/roots remain unusable for this changed source.
