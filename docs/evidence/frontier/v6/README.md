# v6: checkpoint-safe reusable execution and derived state

**Baseline passed; the large 20k-row/s target overloaded.** Both database processes actually restarted on their retained volumes and preserved **2,072,000 common rows per backend**, with identical raw/aggregate counts, sums, extrema and timestamp sums. This is a single-node pilot, not a general Timescale win or production certificate.

## What changed from the retained failure

[v5](../v5/README.md) remains intact: its 256 MiB derived budget failed during large initial ingest and exposed missing future checkpoint headroom. The v6 source rejects live growth before WAL publication unless a zero-contention checkpoint can fit its cloned maps, replacement indexes and page workspace. Historical replay keeps its existing actual-memory limits rather than retroactively enforcing the new live-admission policy.

This run explicitly uses **512 MiB for conservative derived resident/working accounting**, rather than v5's 256 MiB. That is a recorded sizing change, not a hidden tuning retry. No preexisting Varve limit, database cgroup cap or durability setting was relaxed. Both database containers retained 2 CPUs / 2,000,000,000 configured bytes; the driver retained 2 CPUs / 1,000,000,000 bytes. Actual page-aligned limits and region are in the runtime/resource witnesses. The driver files, Python/PostgreSQL versions and TimescaleDB 2.30.0 match v4; its previous image was reused.

Local qualification: **297 Rust checks, 18 TypeScript unit checks and one actual TypeScript service check**, strict workspace lint and formatting. One live-S3 test remains ignored. Selected-source identity: `f15f16a8998e0921ec45975d936fdacc1e217891c348c851945b04fb95abe399`. All Rust and real-client gates reran after the headroom fix; the unchanged TS unit source/config was byte-verified before reusing that result.

## Unchanged workloads and results

Batch 1,000; four writers; fresh isolated datasets; same fixed driver/generator. Database metrics were collected outside timed work, not polled inside it. Varve acknowledged `local_fsync`; PostgreSQL `fsync`, `synchronous_commit` and `full_page_writes` remained on. No synchronous S3/replicated durability is measured.

| Metric | Varve | Timescale |
| --- | ---: | ---: |
| 250k initial ingest, rows/s | 46,321 | 65,253 |
| 250k durable data + fresh aggregate, seconds | 5.40 | 4.80 |
| 5k-target mixed ACK p95, ms | 73.10 | 45.22 |
| 5k-target mixed read p95, ms | 77.16 | 19.27 |
| 1M initial ingest, rows/s | 39,925 | 61,557 |
| 1M durable data + fresh aggregate, seconds | 25.05 | 19.86 |
| 20k-target mixed ACK p95, ms | 95.24 | 82.10 |
| 20k-target mixed read p95, ms | 1,411.11 | 45.56 |

Ready-to-query times include Timescale refresh and ANALYZE, unlike ingest-only rates. `reuse003` passed with all 300k mixed rows acknowledged and a 550k common watermark. `reuse004` was **overloaded**: 600k offered, 522k acknowledged, 78k dropped before submission, zero failed/ambiguous rows; common watermark 1,522,000. Its mixed read distribution has only 24 samples. The paired queue cannot identify either backend's standalone saturation point.

### Large-case query p95, milliseconds

| Query | Varve automatic tiers | Timescale rowstore | Varve checkpointed | Timescale columnar |
| --- | ---: | ---: | ---: | ---: |
| Full count/sum | 547.86 | 79.81 | 34.06 | 25.72 |
| Cross-series grouping | 73.87 | 138.99 | 31.48 | 24.22 |
| Named minute aggregate | 480.73 | 3.14 | 85.14 | 2.81 |
| Selective series/time | 100.78 | 3.12 | 23.45 | 3.07 |
| Window top five | 698.80 | 3.09 | 304.38 | 5.87 |

The favorable automatic-tier grouping cell does not erase the unfavorable cells or mixed-load tail. Across-run differences are not controlled causal speedups: matched caps do not establish identical physical I/O conditions, and source/working-budget changes are explicit.

## Reuse and remaining costs

- Baseline: **570 reuses + 13 spawns for 583 executions**. Large case: **322 reuses + 29 spawns for 351 executions**. Total observed reuse: 892/934 executions. These are real execution-worker counters, not just network keep-alive.
- End-of-large conservative derived resident charge: **156,573,676 bytes**. Published control root: **35,269 bytes**; referenced derived pages: **37,691,000 bytes**; equivalent live logical metadata: **37,507,362 bytes**. Maps/index remain resident; a small root is not out-of-core storage or a claim of total-state shrinkage.
- Varve cgroup memory peak before restart was **1,026,699,264 bytes**, with no OOM. Outstanding working gauge was zero at the snapshot, not a measured working-set peak.
- State-lock hold/wait, query execution, WAL synchronization and whole-set checkpoint work remain material. Reusing a process does not eliminate input staging/scanning or all catalog/index replacement.
- Metric snapshots were **72.90 and 50.70 seconds after their reports finished**. Their deltas include untimed background work; nested/concurrent intervals overlap. Do not sum them into exclusive CPU time or attribute all their cost to timed ingestion.

## Recovery, audit and cleanup

Exact before/after fingerprints and PostgreSQL start times are retained. Immediate pre/post restart process identities changed for both databases; Varve's database identity remained unchanged. This covers process restart on the same volumes, not power loss, lost-volume restore, HA or an S3 disaster-recovery drill.

`MANIFEST.json` inventories the raw reports/logs/launch arguments, all latency samples/oracles, source archive/receipt, exact configuration, runtime evidence and witness scripts. Known benchmark credential values were scanned out; this is not an exhaustive secret-detection claim. Both v5's failure and v6's overload remain available.

All three exact v6 deployments were observed **REMOVED**, with zero active benchmark deployments; the original service stayed unchanged and successful. Benchmark volumes were retained. Keep the Experimental label.
