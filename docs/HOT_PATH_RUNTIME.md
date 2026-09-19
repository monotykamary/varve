# Hot-path runtime and Railway qualification

Date: 2026-09-17. **Correctness/recovery gates passed; no overall Timescale performance win.** This follows the historical database-free [structural rework](HOT_PATH_REWORK.md). No package publication, GitHub write, production-app deployment, resource tuning or durability relaxation was performed.

## Local qualification

- **268 selected Rust runtime/fault tests** across the library and 13 integration targets. Real DuckDB, direct/group/retry/reopen, prefix staleness, publication, residency, cancellation and HTTP lifecycle paths ran. This is not a claim that every repository test or hosted CI ran.
- **21 driver tests**, two default-binary HTTP/restart smoke checks, strict workspace/all-target Clippy in default and all-feature modes, formatting and whitespace checks passed.
- Runtime repairs restore direct exact-WAL admission/error order, timed-ID same-frontier reclamation, group-only metrics and bounded no-progress behavior without weakening publication barriers.
- Independent review accepted production changes and caught a root-staleness test that also changed the checkpoint frontier. The corrected test holds sequence, checkpoint frontier, control epoch and live floors constant; only root epoch changes. It passed for inline/paged roots. Its focused rerun and feature Clippy supplement—not add to—the 268-test count. Mechanical comparison proves this private test is the only source change after the full gate.
- Final frozen source: `bce40d9452c153d7e243e031318226096d30e37ec6e172e4e370c4e86eca2747`. The preceding full-gate source was `f2b18507d7f83b9680c9ceba90e47c091e62563e501ee51594667dfdf5b41967`; the exact test-only bridge is recorded in the qualification receipt.

## Railway contract

- Existing benchmark services only, Singapore, one replica, restart NEVER. Varve/Timescale each have 2 CPU / 2,000,000,000-byte nominal memory limits; actual cgroups read `200000 100000` and `1999998976`. Driver: 2 CPU / 1,000,000,000 nominal bytes (`999997440` actual). These are limits, not dedicated cores or pristine-host guarantees.
- Qualified Rust 1.98.1 Linux release image, checksum-pinned DuckDB `v2.0.0-alpha41533` / `10de957379`, exact source/config/driver hashes and actual nonroot processes verified. The Linux binary is not claimed byte-identical to the tested macOS binary.
- Same v2 benchmark profile: paged derived state, retained inputs, frozen-prefix checkpointing, 128 MiB / 100k-row hot limits, 512 MiB derived budget, 5s flush / 1s maintenance, two 256 MiB/two-thread query workers. No age, cache, grouping, query, resource or fsync tuning between trials. S3 is disabled.
- Timescale 2.30.0 / PostgreSQL 17.11 with fsync, synchronous_commit and full_page_writes ON. Existing extension SELECT-verified; no extension installation or new cluster bootstrap. A startup guard refuses missing initialized PGDATA. Existing PostgreSQL memory/worker settings were preserved and SHOW-verified.
- Each trial uses a fresh empty Varve root and fresh benchmark names. Timescale retains its initialized historical cluster: **fresh namespace, not pristine-cluster parity**. Historical data/cache/maintenance can affect comparison.
- Identical deterministic quarter-valued data, 250k initial rows, 1k-row batches, four writers. Initial ingestion includes the runner’s charged freshness barriers. Short trials: 20 query samples, 10s mixed at 5k rows/s. Sustained: 50 query samples, 60s mixed at 5k rows/s. HTTP/PG clients run remotely as UID10001. All workload invocations use the unchanged frozen driver.

## Every measured trial

Values are **Varve / Timescale**, not pooled favorable cells. p95 is the runner’s sample quantile, not an SLA. Short IDs/order were selected before load to include both backend orders.

| Trial | Initial order | Ingest k rows/s | Mixed write p95 ms | Read median ms | Read p95 ms | Read samples |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| runtime_e9ihkn_p1 | varve → timescale | 64.6 / 101.8 | 85.8 / 28.3 | 10.5 / 13.8 | 284.5 / 17.4 | 9 |
| runtime_e9ihkn_p2 | varve → timescale | 61.9 / 100.1 | 60.4 / 29.7 | 10.8 / 15.8 | 343.6 / 19.0 | 9 |
| runtime_e9ihkn_p4 | timescale → varve | 59.8 / 99.0 | 65.7 / 32.2 | 11.0 / 15.9 | 307.3 / 17.6 | 9 |
| runtime_e9ihkn_s1 | timescale → varve | 67.1 / 86.6 | 72.4 / 34.8 | 10.7 / 14.2 | 254.7 / 18.1 | 58 |

All four runs passed raw/aggregate oracles and tier-conversion checks with **zero dropped or failed/ambiguous rows**: 300k/backend in each short trial, 550k/backend sustained, **1,450,000 total acknowledged rows/backend** across separate datasets. The sustained run lasted 87.826s, crossing the prior ~65s disconnect window, and recorded zero HTTP connection timeouts. This does not prove the historical socket-level failure’s cause or indefinite stability.

**Verdict:** Varve consistently wins the measured concurrent-read median and wins the sustained broad count/sum and cross-series scans below. It still loses initial ingestion, mixed write/read tails, selective/top-N queries and continuous-aggregate query latency. The latest sustained pair is 67.1k versus 86.6k rows/s; write p95 72.4 versus 34.8ms. Historical S12/S13 comparisons are different campaigns, not isolated causal speedups.

### Sustained query-stage detail

| Stage / query | Varve median / p95 ms | Timescale median / p95 ms |
| --- | ---: | ---: |
| columnar_checkpointed / cross_series_group | 13.0 / 15.7 | 30.9 / 37.3 |
| columnar_checkpointed / full_count_sum | 6.8 / 9.7 | 17.8 / 20.1 |
| columnar_checkpointed / minute_continuous_aggregate | 11.4 / 14.5 | 0.8 / 1.1 |
| columnar_checkpointed / recent_series_time_filter | 5.5 / 9.3 | 0.9 / 4.2 |
| columnar_checkpointed / window_top_five | 17.3 / 21.8 | 0.8 / 1.1 |
| default_tiers / cross_series_group | 11.9 / 17.1 | 35.4 / 46.0 |
| default_tiers / full_count_sum | 6.0 / 9.4 | 19.7 / 23.8 |
| default_tiers / minute_continuous_aggregate | 12.4 / 24.3 | 0.8 / 1.3 |
| default_tiers / recent_series_time_filter | 6.7 / 9.3 | 1.0 / 3.7 |
| default_tiers / window_top_five | 21.8 / 35.6 | 0.8 / 2.5 |

## What still costs work

- Short trial p4 recorded 185 WAL publication attempts, 370 file/directory sync operations and 3.249s inside those sync timers; enclosing WAL publication took 3.283s. `src/wal.rs::append_encoded` still writes an immutable temporary, file-syncs, renames, then directory-syncs each publication. These nested times must not be added. Encoding was 0.077s and group preparation 0.861s.
- Sustained residency counters: 561 hits, 7 delta loads, 19 full loads and 17 invalidations. 2,007,951 raw rows were staged for a 550k-row dataset. Warm reuse works, but misses still rebuild substantial input.
- These are measured cost classes, **not isolated causal attribution** for every tail sample. Timers overlap and do not cover all CPU. The next focused investigations are publication/grouping amortization that preserves existing corruption/acknowledgment guarantees, and tracing/removing avoidable full reloads across tier/scope changes. Do not simply drop a WAL barrier, forgive a damaged tail, disable maintenance or pick a favorable cache profile. The earlier segmented-WAL rejection remains valid until an explicit recovery proof addresses its ambiguity.

## Recovery and cleanup

- Restarted only the exact owned Varve and Timescale deployments. Railway retained instance UUIDs; an initial wait for new instance IDs timed out. No second restart was issued. Reconciliation proved a new Varve OS process start tick and new PostgreSQL postmaster time instead.
- Preserved Varve database identity, **550k rows and 550 receipts per backend**. Independent integer timestamp sum, timestamp-square sum and timestamp/quarter-value cross-product matched the generated oracle before and after. The unchanged driver then checked raw and retained aggregate count/sum/min/max **without refreshing views**. These moments are not a collision-free cryptographic dataset proof.
- This was process restart recovery, not a power-cut, lost-volume, replicated or HA test. No stress/saturation frontier was claimed.
- Original reports/logs were copied and byte hashes verified before cleanup. The three benchmark deployments are confirmed REMOVED with zero active instances. Services, data roots and volumes remain; retained storage still costs money. The original Varve app remained on its original successful deployment.

## Evidence and reproduction

[Evidence bundle](evidence/hot-path-runtime/e9ihkn/) contains original five reports, metrics, source/binary/config/deployment receipts, local logs/reviews, recovery moments, cleanup proof and exact staged source archive. `summary.json` retains numeric values and query breakdowns; `SHA256SUMS` binds the bundle. Private credential-bearing API responses are deliberately excluded.

- `build/qualified-varve-stage.tar.gz`: exact upload context, not a published package. `build/stages.json` and embedded source manifest distinguish the source closure from the metadata/restart-policy overlays. Rebuilding can produce different machine bytes; preserve a new image/binary receipt.
- Verify the bundle offline from `varve/` with `node docs/evidence/hot-path-runtime/e9ihkn/witness/verify-evidence.mjs`. It checks all 81 artifact hashes, archive/source equivalence, the exact private-test-only refinement, original latency quantiles, row counts, process markers and cleanup. It does not launch databases or contact Railway.
- Other `witness/` files are **archival provenance, not activation authorization**. Some contain historical absolute paths. Do not execute old cloud controls or reuse these run IDs to launch a new campaign.
- Local gate commands and target counts are in `local/qualification.json`, `local/runtime-final.log` and `witness/run-final.sh`. Full gate used `cargo test --offline --locked --features fault-injection` for the library plus the 13 recorded targets; the focused corrected test and both feature-mode lint receipts are separate.
- A new comparison needs a fresh source/runtime gate, workload intent, empty owned roots/names, exact deployment/image/resource/extension/durability readbacks and explicit bounded load scope. No production-readiness, S3, distributed, zero-copy or hard tail-latency guarantee follows from this checkpoint.
