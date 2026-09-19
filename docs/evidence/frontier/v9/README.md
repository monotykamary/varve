# Frontier v9 — frozen-prefix pilot and resumed stress, no win

This immutable record tests proof-pruned resident batches and explicitly enabled frozen-prefix checkpoints. It is **not a frontier win, a fresh uninterrupted acceptance trial, or production qualification**. Actual database restarts retained matching raw, aggregate and exact-timestamp fingerprints for **2,034,000 committed rows per backend**. Both operational attempts ended with the exact three benchmark deployments removed; volumes and the original service were preserved.

## Candidate and fixed comparison

S10 adds immutable batch time bounds and omits only planner-proven disjoint inputs. S11 prepares a durable prefix outside the state mutex and preserves later live rows, aggregates, receipts and idempotency floors when publishing it. Structural invalidation, bounded pre-publication retries, pins, budgets and ambiguous-failure fencing remain mandatory. Direct-write pressure retains the locked fallback; this is not a lock-free design.

`checkpoint_frozen_prefix: true` is the sole runtime-profile change versus v8; `previous-profile.json` and `varve-config.json` bind that comparison. The source also changes pruning, so this does not isolate a checkpoint-only effect. The 100k hot-row / 128 MiB hot / 32 MiB decoded / 256 MiB WAL budgets, five-second age policy, two SQL workers and v2/512 MiB derived-state budget remain unchanged. This is not a separately residency-tuned profile. Checkpoint durability and serving eviction remain separate decisions.

Driver bytes, Python, PostgreSQL, TimescaleDB 2.30.0, DuckDB v2, region and resource caps are checked against preceding evidence. Databases have two CPU / 2 GB caps; driver two CPU / 1 GB. `fsync`, `synchronous_commit` and `full_page_writes` are on. These are **local durable acknowledgements**, not synchronous S3 replication or HA. SQL remains DuckDB; no benchmark-pattern dispatch or raw-to-rollup substitution is used.

## Results

Baseline `prefix001`: 250k initial rows, 50 samples/query/stage/backend, then 60 seconds at 5k offered rows/s. All 300k mixed rows acknowledged, with no drops, failures or ambiguity. Initial durable throughput **63.6k vs 99.3k rows/s** loses. Concurrent-read p95 is **100.9 vs 17.3 ms** (Varve / Timescale).

Resumed stress `prefix002`: 1M initial rows, 30 samples/query/stage/backend, then 30 seconds at 20k offered rows/s. Initial durable throughput is **46.2k vs 91.2k rows/s**. **116,000 of 600,000 offered rows dropped before admission**; 484,000 acknowledged, zero failed/ambiguous rows. Mixed acknowledgement p95 is **439.4 vs 57.0 ms**, concurrent read **322.4 vs 27.1 ms**. The paired queue does not identify either backend's standalone saturation limit.

Broad group scans and some count cells favor Varve. Selective, aggregate and window cells still lose; the stress checkpointed count also loses. All cells and raw samples are retained below, not just the favorable ones. Timescale's changed throughput versus v8 demonstrates host/campaign variability; no causal cross-campaign speedup is claimed. At least three fresh counterbalanced trials and every acceptance gate remain necessary for the final goal.

## Controller incident and explicit resume

The first controller completed the baseline but its local unpacker rejected the new run ID because an old regex remained. This was a controller error, not a failed database workload. It removed all owned compute promptly; the completed raw report, error, rejected unpacker and cleanup receipts remain under `initial/`. No after-baseline phase snapshot or planned recovery probe was captured; those missing observations are not reconstructed.

A separate controller then reused the exact removed images and retained database volumes. **It did not rebuild source or rerun the baseline.** Redeployment restarted the databases between the two workloads; this affects caches and residency and prevents calling the pair an uninterrupted fresh trial. A small recovery-only reference, bound to the original full baseline report's SHA256, restored the ephemeral driver's baseline namespace/watermark metadata. The complete report remained preserved locally. Before stress, both backends' 550k baseline fingerprints were verified; after stress those fingerprints were unchanged.

The revised controller was syntax-checked, its actual unpacker exercised against the completed baseline fixture, and its actual cleanup/redeploy helpers exercised in 8 / 5 mocked guard cases before deployment. It captures phase metrics before formatting the local result. After stress, both database process identities changed on restart and all 2.034M common committed rows plus raw/aggregate/exact-timestamp fingerprints survived. The resumed controller's final cleanup verifies zero active benchmark compute, retained volumes and unchanged original service. These are artifact-backed recovery observations, not repeated performance acceptance.

## Observations, not exclusive attribution

`resume/analysis-stress.json` spans 00:01:37–00:03:42 UTC on 2026-09-17, including boundary delay and idle maintenance. **Timers overlap and cannot be added as exclusive CPU or wall time.** It records 34.34 seconds of state-lock hold, 21.14 root preparation, 18.99 checkpoint preparation, 14.63 WAL write, 14.45 WAL sync, 9.88 compaction preparation, 6.39 query build and 4.06 query execution. Manifest commit accounts for 2.02 cumulative seconds. These do not identify the cause of a particular tail sample.

The interval records 954 groups for 1,484 submitted requests, 23 full resident loads, one delta and 331 unchanged-input hits. 1,291,224 raw rows / 237,000,147 logical staged bytes still moved into workers. Observed cgroup memory peak was 854,577,152 bytes with no OOM events; logical resident/working counters are not a hard process-RSS bound. Five pre-stress empty-query diagnostic samples are not benchmark tails, and the copied probe's old `before fixture creation` label does not mean the resumed databases were empty.

## Source, correctness and reproducibility

Frozen qualification: **384 Rust checks + 19 TypeScript checks**, strict workspace Clippy and formatting. Eighteen unchanged-source TypeScript unit checks reuse their verified prior log; the actual service check reran. One live-S3 test remains ignored. The qualified 83-input digest is `9ed8187afb8b8f9846ee62e80a8ddf32b442c6ca188d1dd330d07728018dca81`. Review-driven grouped retry/idempotency-floor regressions are covered; qualification is not performance certification.

The 54-file build receipt and all 83 locally qualified inputs are archived separately. Runtime config SHA256 is `215f100b13ba1ba3d012d39117fa1895ed7978657434057a93390c6f359aa089`; build-manifest SHA256 is `ab8c46ad7670557d2abe93b702658484d44a09f88068bdd93801708dfd146a5a`. Both runtime probes confirm source/config/binary identity. `MANIFEST.json` binds public artifacts, with known credential values checked before copying. Run `node docs/evidence/frontier/v9/witness/verify.mjs` from the repository root for stored arithmetic, source/archive, runtime, recovery and cleanup verification without local DB load or timing.

## All paired p95 cells (milliseconds)

| Run | Stage | Pattern | Varve p95 | Timescale p95 |
| --- | --- | --- | ---: | ---: |
| prefix001 | columnar_checkpointed | cross_series_group | 14.80 | 33.27 |
| prefix001 | columnar_checkpointed | full_count_sum | 10.08 | 17.97 |
| prefix001 | columnar_checkpointed | minute_continuous_aggregate | 14.52 | 1.29 |
| prefix001 | columnar_checkpointed | recent_series_time_filter | 5.64 | 1.55 |
| prefix001 | columnar_checkpointed | window_top_five | 22.68 | 4.07 |
| prefix001 | default_tiers | cross_series_group | 20.83 | 37.69 |
| prefix001 | default_tiers | full_count_sum | 10.18 | 19.93 |
| prefix001 | default_tiers | minute_continuous_aggregate | 13.32 | 3.64 |
| prefix001 | default_tiers | recent_series_time_filter | 7.31 | 1.52 |
| prefix001 | default_tiers | window_top_five | 24.35 | 2.11 |
| prefix002 | columnar_checkpointed | cross_series_group | 23.69 | 121.59 |
| prefix002 | columnar_checkpointed | full_count_sum | 86.55 | 58.42 |
| prefix002 | columnar_checkpointed | minute_continuous_aggregate | 73.27 | 1.22 |
| prefix002 | columnar_checkpointed | recent_series_time_filter | 7.76 | 1.58 |
| prefix002 | columnar_checkpointed | window_top_five | 35.91 | 1.37 |
| prefix002 | default_tiers | cross_series_group | 32.83 | 135.03 |
| prefix002 | default_tiers | full_count_sum | 20.43 | 82.58 |
| prefix002 | default_tiers | minute_continuous_aggregate | 17.25 | 3.14 |
| prefix002 | default_tiers | recent_series_time_filter | 9.35 | 1.69 |
| prefix002 | default_tiers | window_top_five | 36.20 | 4.00 |

| Run | Initial rows/s Varve / Timescale | Mixed ack p95 Varve / Timescale | Concurrent read p95 Varve / Timescale |
| --- | ---: | ---: | ---: |
| prefix001 | 63572 / 99345 | 65.73 / 36.04 | 100.87 / 17.26 |
| prefix002 | 46244 / 91238 | 439.43 / 56.97 | 322.40 / 27.06 |
