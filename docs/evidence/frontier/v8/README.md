# Frontier v8 — retained inputs, no overall win

This immutable Railway pilot tests the explicitly enabled retained-input adapter and checkpoint residency support. It is **not a frontier win or production qualification**. Both databases retained matching raw, aggregate and exact-timestamp fingerprints for **2,062,000 committed rows each** after actual process restarts. All three exact benchmark deployments were removed; volumes and the original service were preserved.

## What changed

- Immutable, charged hot batches shared by SQL snapshots; checkpoint output can stay in the existing bounded decoded cache instead of being automatically evicted from serving memory.
- Disposable typed DuckDB CLI input tables, snapshot-compatible append deltas, reused schema, current rollup/catalog refresh, and safe invalidation on inversion/removal/cutoff/path changes.
- Explicit `query_retained_inputs: true`. `previous-profile.json` proves all other v7 settings are unchanged, including 100k hot rows, 128 MiB hot budget, 32 MiB decoded cache, five-second age policy, two SQL workers and v2/512 MiB derived-state budget. This is an unchanged-pressure/age regression pilot, not a separately residency-tuned profile.

The exact driver, Python, PostgreSQL, TimescaleDB 2.30.0 and DuckDB v2 binary are pinned to prior evidence. Both databases use two CPU / 2 GB caps; the driver uses two CPU / 1 GB. Local durable acknowledgements still require fsync; this is not synchronous S3 replication or HA. SQL remains DuckDB, not a workload-pattern shortcut or substitution of historical rollups for raw data.

## Results

Baseline `resident001`: 250k initial rows, 50 samples per query/stage/backend, then 60 seconds at 5k offered rows/s. All 300k mixed rows acknowledged; zero drops or ambiguous/failed rows.

Stress `resident002`: 1M initial rows, 30 samples per query/stage/backend, then 30 seconds at 20k offered rows/s. **88,000 of 600,000 offered rows dropped before admission**; 512,000 acknowledged, zero failed/ambiguous rows. The paired queue does not measure either backend's standalone saturation limit.

Stress initial Varve ingest was about 5% faster, below the 10% target and not repeated. Baseline ingest lost. Broad group/count scans won their cells, but rollup, selective and window tails largely did not. Stress concurrent-read p95 was about 1.56 seconds for Varve versus 35 ms for Timescale. Different host conditions are evident in Timescale's changed results versus v7; cross-campaign numbers do not isolate the effect of this code change. At least three fresh counterbalanced trials and all acceptance cells are still required for a final claim.

## Runtime observations

Baseline / stress phase deltas report 21 / 25 full resident loads, 54 / 19 delta loads, and 506 / 305 unchanged-input hits. They staged 2,014,262 / 2,421,060 raw rows. This witnesses reuse **and** nontrivial churn; it does not prove the cause of an individual latency sample. After both phases, observed cgroup memory peak was 973,709,312 bytes and no OOM events occurred.

`analysis-*.json` contains overlapping cumulative phase intervals, not exclusive CPU or wall-time attribution. Snapshots are untimed and arrive several seconds after each workload finishes; idle maintenance can contribute. Idle resident gauges are not total process-memory bounds. Five pre-fixture empty-query diagnostic observations are neither a workload benchmark nor a tail estimate.

## Method and controller incident

All load ran remotely in `asia-southeast1-eqsg3a`, under a non-root benchmark identity, with fresh database directories. The original controller mistook the driver's in-progress JSON for a completed result and stopped monitoring. The remote baseline kept running, was neither interrupted nor relaunched, and monitoring resumed with its original deadline. `campaign-error.json`, both controller logs, and the intermediate metrics snapshot preserve this incident. The corrected controller waits for `finished_at`; final phase metrics were collected after actual completion. No timed workload or driver code changed.

## Source, correctness and reproducibility

The build receipt covers 54 files; the local qualification snapshot covers all 80 selected source/config/test inputs, including the configuration documentation used by compile-time tests. Selected-source SHA256: `9090bcb0d26eb1cb0b2fcdbfd273607c9e2b54bd0d3881a0eaa4bb223782c947`. Runtime configuration SHA256: `a1ba9bc95589a40c490b2bcad26359b0c62ff103ae3bdc121d969d8e02775e34`.

Qualification: **341 Rust checks + 19 TypeScript checks**, strict Clippy and formatting. Eighteen unchanged-source TS unit checks reuse their verified prior log; the real-service check reran. One live-S3 test remains ignored. `config-docs-check.log` is an additional focused rerun, not extra checks added to 341. Independent source review found no concrete correctness defect; these tests and reviews are not performance certification.

`source.tar.gz` is the exact build snapshot (Dockerfile adds only its runtime source receipt); `local-qualified-source.tar.gz` includes all locally checked inputs. `MANIFEST.json` binds every public artifact; known credential values were scanned before copying. Run `node docs/evidence/frontier/v8/witness/verify.mjs` from the repository root to verify hashes, raw latency arithmetic, profile isolation, source/test receipts, recovery and cleanup without rerunning a workload.

## All paired p95 cells (milliseconds)

| Run | Stage | Pattern | Varve p95 | Timescale p95 |
| --- | --- | --- | ---: | ---: |
| resident001 | columnar_checkpointed | cross_series_group | 12.80 | 31.98 |
| resident001 | columnar_checkpointed | full_count_sum | 7.00 | 21.10 |
| resident001 | columnar_checkpointed | minute_continuous_aggregate | 11.50 | 3.18 |
| resident001 | columnar_checkpointed | recent_series_time_filter | 5.60 | 4.48 |
| resident001 | columnar_checkpointed | window_top_five | 22.55 | 3.74 |
| resident001 | default_tiers | cross_series_group | 15.67 | 43.04 |
| resident001 | default_tiers | full_count_sum | 12.83 | 22.27 |
| resident001 | default_tiers | minute_continuous_aggregate | 13.38 | 3.81 |
| resident001 | default_tiers | recent_series_time_filter | 7.72 | 8.35 |
| resident001 | default_tiers | window_top_five | 21.61 | 4.83 |
| resident002 | columnar_checkpointed | cross_series_group | 20.76 | 30.52 |
| resident002 | columnar_checkpointed | full_count_sum | 18.53 | 26.95 |
| resident002 | columnar_checkpointed | minute_continuous_aggregate | 11.35 | 3.07 |
| resident002 | columnar_checkpointed | recent_series_time_filter | 6.06 | 2.84 |
| resident002 | columnar_checkpointed | window_top_five | 17.99 | 8.71 |
| resident002 | default_tiers | cross_series_group | 19.22 | 145.93 |
| resident002 | default_tiers | full_count_sum | 13.57 | 85.35 |
| resident002 | default_tiers | minute_continuous_aggregate | 12.01 | 4.48 |
| resident002 | default_tiers | recent_series_time_filter | 5.70 | 4.32 |
| resident002 | default_tiers | window_top_five | 75.20 | 6.03 |

| Run | Initial rows/s Varve / Timescale | Mixed ack p95 Varve / Timescale | Concurrent read p95 Varve / Timescale |
| --- | ---: | ---: | ---: |
| resident001 | 43011 / 52864 | 91.27 / 58.36 | 243.59 / 23.07 |
| resident002 | 42720 / 40680 | 105.69 / 112.25 | 1555.36 / 35.37 |
