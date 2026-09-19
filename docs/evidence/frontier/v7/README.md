# v7: sediment candidate, unchanged v6 regression profile

**Outcome: not a throughput-and-latency win.** The baseline passed; the fixed 20k-row/s stress case overloaded its paired queue. This is one pilot per case, not repeated/counterbalanced validation or a standalone saturation estimate. Negative cells and all raw samples are retained.

## What changed

Source-qualified candidate `12643365fa352b4303019cba7dd9a994d2d1b433327674cb4b29da7dff4c0646`: conservative nested input pruning, bounded typed small inputs, off-lock prepared-root work, one live application per accepted grouped item and reuse of canonical WAL proof bytes. Large inputs still use request-local NDJSON; the CLI pool does not retain their relations. The new `pressure_only` policy is implemented and locally tested, but **not enabled in this regression profile**. Durability, serving residency, checkpointing and eviction remain distinct design responsibilities.

Runtime configuration bytes match v6 (`20c310dea827fc4b98199acdd4eab4c660660909a223c9155a3688be16b4aeed`): default five-second age flush, 100k hot-row limit, 128 MiB hot budget, 256 MiB WAL, v2 derived pages explicitly enabled with 512 MiB derived budget. No durability relaxation or raised service cap. Exact driver/Python/PostgreSQL/Timescale versions and driver file hashes match v4/v6. Both databases received fresh isolated directories, 2 CPUs and 2 GB; driver 2 CPUs and 1 GB, same Railway region. Timescale fsync, synchronous_commit and full_page_writes stayed on. Timings and load ran only on Railway.

## Measured results

| Metric | Baseline Varve / Timescale | Stress Varve / Timescale |
| --- | ---: | ---: |
| Initial durable rows/s | 59,790 / 88,266 | 48,797 / 89,319 |
| Initial acknowledgement p95 ms | 93.31 / 125.25 | 127.18 / 133.00 |
| Concurrent acknowledgement p95 ms | 69.88 / 40.86 | 90.58 / 49.43 |
| Concurrent read p95 ms | 50.22 / 17.68 | 630.86 / 23.11 |

Initial durable ingest alone is not durable-plus-fresh readiness; the report retains refresh/ANALYZE preparation and independent raw/aggregate oracles. No throughput win is claimed under either definition.

All query cells below are p95 milliseconds, Varve / Timescale:

| Query / stage | Baseline | Stress |
| --- | ---: | ---: |
| Recent series/time, automatic tiers | 39.90 / 2.91 | 25.41 / 3.11 |
| Full count/sum, automatic tiers | 153.62 / 22.27 | 112.42 / 79.70 |
| Cross-series group, automatic tiers | 25.47 / 39.71 | 65.90 / 146.71 |
| Minute aggregate, automatic tiers | 28.89 / 2.91 | 50.92 / 3.17 |
| Window top five, automatic tiers | 31.65 / 3.13 | 41.93 / 2.95 |
| Recent series/time, checkpointed columnar | 17.54 / 4.62 | 27.07 / 3.21 |
| Full count/sum, checkpointed columnar | 18.55 / 22.88 | 23.62 / 60.10 |
| Cross-series group, checkpointed columnar | 26.78 / 41.14 | 45.20 / 104.10 |
| Minute aggregate, checkpointed columnar | 32.32 / 2.93 | 44.26 / 2.67 |
| Window top five, checkpointed columnar | 26.72 / 2.94 | 38.80 / 2.64 |

Baseline acknowledged all 300,000 offered mixed rows at 5k rows/s, with no drops or ambiguous writes. Stress acknowledged 542,000 of 600,000 offered mixed rows at 20k rows/s; **58,000 dropped before admission**, zero failed/ambiguous acknowledged rows. The paired queue does not identify either backend's independent saturation limit. There were 1,600 oracle-verified timed query samples across both reports, plus retained mixed-read checks.

## Diagnostics, recovery and cleanup

Worker reuse was observed, but reuse is not retained input data: baseline 561 reuses/23 spawns, stress 326 reuses/27 spawns. Overlapping phase counters show remaining WAL sync, lock-held publication and query work; their sums are **not exclusive CPU or wall-time attribution**. End-of-baseline counters were captured about 163.4 seconds after workload completion and stress counters about 50.1 seconds after completion, so idle maintenance is included. No database metrics were polled during timed workload execution. Memory peaks are process-cgroup observations, not proven global memory bounds; no OOM kill was recorded.

After actual process restarts on the same images and volumes, both backends retained matching raw count/sum/min/max/exact timestamp-sum and independently retained aggregate fingerprints for **2,092,000 committed rows per backend** (550,000 baseline + 1,542,000 stress). Process identities and PostgreSQL start times changed, while Varve database identity stayed unchanged. Local fsync/restart recovery is not synchronous S3 replication or HA.

All three exact benchmark deployments were removed and zero active benchmark deployments verified. Benchmark volumes, prior datasets and this evidence remain retained. The original service was unchanged. Local qualification records 321 Rust and 19 TypeScript checks; one live-S3 check remains ignored. Source/runtime receipts and arithmetic checks are evidence of this candidate, not production certification.

## Reproduce the evidence checks

```sh
node docs/evidence/frontier/v7/witness/verify.mjs
```

This verifies checksums, frozen source archive, configuration/runtime receipts, recorded percentiles, query-oracle counts, recovery fingerprints and cleanup. It does not execute a local database workload. `driver/` and `witness/` preserve the workload and probes; `MANIFEST.json` covers archived files. Do not overwrite v4/v5/v6 or turn this negative pilot into a win claim.
