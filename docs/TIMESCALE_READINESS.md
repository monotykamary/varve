# Production readiness and Timescale comparison

**Decision: no production cutover yet.** This is an engineering and qualification plan, not a production certification or a claim that Varve beats Timescale. The first milestone removes avoidable query-side work without changing the WAL, acknowledgement, persistent format, or aggregate arithmetic.

Varve's intended production contract can remain fixed-schema, append-only and single-node. PostgreSQL feature parity, arbitrary SQL incremental views and automatic HA are not prerequisites unless they are promised. Correctness, bounded resources, recovery, operational SLOs and upgrade/rollback qualification **within the supported contract** are prerequisites. Compatibility with an existing Timescale deployment is an additional migration gate, not the definition of production readiness.

The T-ledger below records the original query-projection milestone. Current results, fixes and the 243-test v4 qualification are tracked in [FRONTIER.md](FRONTIER.md); historical trial results are not silently promoted into passes.

## Historical acceptance ledger for the query-projection milestone

| ID | Requirement | Evidence / state |
| --- | --- | --- |
| T1 | Never clone receipt maps or whole aggregate maps just to form SQL/native read snapshots | `src/engine.rs`: borrowed catalog traversal, projection before cloning; independent source review found no projection-specific correctness regression; see the separate remote-vacuum risk below |
| T2 | Conservative tenant/series selection, physical shard pruning, named aggregate width selection | `src/plan.rs` tests; `tests/query_projection.rs` passes against real DuckDB, including a counted cold-store fixture |
| T3 | No semantic change for Unicode, quotes, backslashes, collations, disjunctions, late data, hot/cold mixing or reopen | Differential queries compare with deliberately unpruned CTE exposure; passing local regressions, not general SQL equivalence proof |
| T4 | Direct blocked cold SQL and native reads preserve snapshots without stalling local writes/status/checkpoint | New `tests/background_io.rs` cases passed in the rebuilt 212-test workspace suite; expiration plus remote vacuum is not covered by this guarantee |
| T5 | Reproducible, correctness-checked performance evidence, without extrapolation | Local after-measurement cancelled at user request; [isolated Railway comparison](TIMESCALE_BENCHMARK.md) completed: 550k-row baseline and restart verification passed; larger run hit projected-checkpoint admission headroom |
| T6 | Preserve corruption, retention, aggregate, admission and durability regressions | 212 Rust tests passed in the rebuilt workspace/all-targets/fault-injection suite; one live-S3 test ignored; strict workspace Clippy passed |
| T7 | Prove compatibility with the user's actual Timescale workload | BLOCKED: schemas, continuous aggregate definitions, queries, versions, volume/rate/hardware and recovery objectives not supplied |
| T8 | Establish a fair Timescale comparison and safe migration | Matched 2-vCPU/2-GB single-node comparison completed; all three temporary deployments removed and test volumes retained. Timescale was faster in this trial; actual workload compatibility and migration remain unproven. |

## What is actually implemented

The write coordinator is a bounded **Crossbeam channel plus a single batching writer**, not an LMAX Disruptor implementation. Saturation comes from amortizing durable publications and bounded backpressure; a different queue alone is not a durability or throughput guarantee.

Read snapshots previously cloned a complete `Table`, including all request receipts and rollups, before throwing away unneeded state. They now borrow the catalog under the existing lock and clone only selected hot rows, rollups and immutable segment descriptors. Pins are installed before releasing that lock; remote materialization remains outside it. This reduces copied state, but does not make snapshot construction lock-free or eliminate its remaining linear scans.

For supported single-relation SQL, binary string equality on `tenant` and `series` can filter hot/aggregate rows. Both exact keys are required to choose a physical hash shard. A named aggregate additionally selects its configured width. Contradictory equalities require no raw files. Unknown expressions and complex relation shapes retain conservative fallback; no inference passes through OR, NOT, casts, functions, parameters or explicit collations. Raw timestamp bounds are never applied as aggregate bucket bounds.

The cold-store regression proves one selected shard is fetched out of eight, while a named aggregate and a contradictory predicate fetch zero raw segments. Unfiltered SQL still exposes all eight. This is a work-reduction assertion, not an eightfold latency claim.

## Cold-reader reclamation follow-up

Independent source review found a pre-existing cold-reader/remote-vacuum race: expiration could publish removal and delete an object before an older pinned snapshot completed its GET. The [frontier follow-up](FRONTIER.md) now reproduces the failure for both native and SQL readers and fixes vacuum reachability to include current catalog references and per-object reader pins. Both regressions pass, including unrelated reclamation and later reclamation after pin release. State/pin locks are released before remote I/O. This establishes deterministic single-node/FileStore behavior, not live-S3 qualification or a durable-data-loss claim. The original local-durability Railway comparison did not exercise S3.

## Compatibility gates for an actual migration

| Requirement | Current Varve boundary | Needed before cutover |
| --- | --- | --- |
| Time-series schema | Fixed `(timestamp_us, tenant, series, value, tags)` rows; finite f64 values; append-only | Map every actual column, timestamp precision/timezone, null, decimal and uniqueness rule. Wide typed tables require real schema/format work, not an undocumented JSON workaround. |
| SQL/driver compatibility | DuckDB dialect behind an additional conservative parser; PostgreSQL wire offers a limited simple-query surface | Replay actual application SQL and driver flows. PostgreSQL prepared statements, COPY/INSERT ingestion, casts/functions and full transactions are not established replacement contracts. |
| Continuous aggregates | Named built-in count/sum/min/max/average/first/last/OHLC with fixed microsecond widths | Verify grouping dimensions, refresh/freshness behavior, late data, duplicate/tie policy and independent raw/derived retention. Arbitrary aggregate SQL, joins, calendar/DST buckets, gapfill and mutation invalidation are not implied. |
| Analytical capacity | Process-per-query DuckDB v2 alpha; copied hot/view NDJSON; selected whole cold files; bounded output; spill disabled | Qualify an exact supported DuckDB build and the real join/window/aggregation working sets. Query limits and total RSS must meet the workload, not only small fixtures. |
| Durability/availability | Local fsync acknowledgement, asynchronous remote publication, single owner | Agree explicit local-disk-loss RPO and recovery RTO. Async S3 is not synchronous replication or automatic HA. Resolve the documented restart incident and drill actual recovery. |
| Operations | Admission, auth, lifecycle jobs and crash tests exist | Multi-day mixed-load soak, disk full/provider outage/restart drills, actionable alerts, supported upgrade/rollback and external review remain release gates. |

Supply sanitized table DDL, continuous aggregate definitions and 5–10 important queries, plus Postgres/Timescale versions, driver/protocol, current compression/retention policies, row/series/tag cardinalities, sustained/burst write rate, query concurrency, hardware and RPO/RTO. Also state whether old data is updated/deleted or corrected and whether aggregates must remain queryable after raw retention.

Read-only discovery examples (run against your database yourself; do not share credentials or unsanitized output):

```sql
SHOW server_version;
SELECT extversion FROM pg_extension WHERE extname = 'timescaledb';
SELECT view_schema, view_name, view_definition, materialized_only
FROM timescaledb_information.continuous_aggregates;
```

## Optimization roadmap, in dependency order

The [original remote comparison](TIMESCALE_BENCHMARK.md) exposed conservative projected-checkpoint admission before the hot-row threshold. The [frontier follow-up](FRONTIER.md) adds bounded pre-publication checkpoint/replanning and crash/replay coverage without removing the guard. Candidate v3 subsequently completed one million initial rows, then exposed mixed-load overload. V4 addresses the flush-cadence bug, no-op maintenance work and duplicate exact metadata projection; remote qualification is tracked separately. Neither the original 403k partial table nor a later single passing trial defines a universal capacity ceiling or production frontier.

1. **Projection-first snapshots and proven series/shard selection** — this milestone. Avoid moving unnecessary data before optimizing instructions. Add finer series indexes/zone maps only with measurable benefit and corruption/false-negative tests.
2. **Columnar hot batches and isolated persistent query workers** — evaluate immutable Arrow batches / Arrow IPC and pinned, independently replaceable DuckDB workers. Measure copying, parse/setup and spawn costs separately. Preserve per-query snapshots, exact file allowlists, credential isolation, cancellation/reaping, memory ceilings and worker reset behavior. Out-of-process IPC is not automatically zero-copy.
3. **Workload-sized Parquet row groups and compaction** — tune sorted layout, dictionary/encoding choices, row groups and target file sizes together. An Arrow ingestion batch is not a Parquet row group. Avoid both tiny-file amplification and a giant single row group that prevents parallel scans. Measure selective reads and full scans, storage ratio, write amplification and compaction debt.
4. **Shorter commit critical sections** — background compaction with generation-checked publication; measured incremental disk accounting with startup/reconciliation and ENOSPC coverage; allocation/ownership profiling for group-commit staging. Keep single-writer ordering and acknowledgement semantics. Do not replace exact accounting with an optimistic counter that forgets garbage, pinned files or reservations.
5. **Segmented/preallocated WAL if syscall profiling warrants it** — amortize file and directory publication costs, but first specify frame boundaries, committed tails, rotation/checkpoint rules and versioned recovery. Crash at every boundary. io_uring/direct I/O/custom lock-free structures are later, platform-qualified options, not prerequisites for production.
6. **Scalable aggregate and metadata storage** — indexed, partitioned aggregate state and receipt lifecycle; mergeable partial states only where algebra and tie semantics permit. Arbitrary continuous aggregate SQL needs a supported operator/state/invalidation model. Floating-point reassociation/SIMD must not silently change the promised result semantics.
7. **Selective remote range reads** — only after designing integrity for partial reads, bounded cache admission, immutable object identity, publication/GC coordination and cancellation. Do not bypass today's whole-file BLAKE3 verification merely to issue fewer GET bytes.

Pgrust is useful inspiration for batching, fusion and memory locality, **not a production dependency**: its own README says it is not production ready. Its illustrated large analytical speedups use a particular workload/build/hardware and a PostgreSQL baseline with parallel gather disabled. They are not a comparison with tuned Timescale columnstore and continuous aggregates. DuckDB already supplies vectorized execution; Varve's first task is to stop undermining it at the storage/worker boundary.

Primary references:

- [pgrust status](https://github.com/malisper/pgrust) and [batching/fusion/SIMD article and benchmark conditions](https://pgrust.com/blog/how-we-made-postgres-hundreds-of-times-faster-the-query-engine/).
- [DuckDB file formats, row groups and parallelism](https://duckdb.org/docs/current/guides/performance/file_formats) and [workload tuning](https://duckdb.org/docs/current/guides/performance/how_to_tune_workloads).
- [Datadog's production Rust time-series engine](https://www.datadoghq.com/blog/engineering/rust-timeseries-engine/): data layout, aggregation-buffer ownership, indexing and compaction are useful precedents, not guarantees for Varve.
- [Timescale architecture](https://docs.timescale.com/about/latest/whitepaper/): the relevant opponent includes hybrid columnar storage, vectorized execution and continuous aggregates, not only PostgreSQL heap scans.

## A benchmark that could support a Timescale claim

Use explicitly provisioned disposable databases, not the production Timescale instance. Pin both versions, artifact/source hashes, configuration, instance CPU/RAM/storage, filesystem and region. Match logical schemas, precision, retention and aggregate semantics. Use production-appropriate ingestion on each side (including PostgreSQL COPY/batching where appropriate); never spawn a fresh `psql` per batch to make the competitor slower. Report transport/encoding differences.

Compare equal durability: Varve local fsync versus logged PostgreSQL with fsync and synchronous commit enabled. Do not use unlogged tables, disable synchronous commit, or compare local acknowledgement with a synchronous replicated acknowledgement without labeling a separate durability class. Match aggregate freshness, including late-data/refresh work; do not compare a precomputed Varve answer with a Timescale raw scan and call it an engine win.

Run separate and mixed ingest/query/maintenance cases across selective recent series, broad history, cross-series aggregates, continuous aggregates, late arrivals, high-cardinality groups/receipts, joins and window functions actually used. Include tuned Timescale rowstore and columnstore where appropriate, with realistic indexes, statistics, compression/segment ordering and chunk sizes. Use small, memory-sized and larger-than-RAM datasets, warm/cold cache runs and long enough duration to include steady compaction/archive work.

Retain raw samples, independent correctness oracles, errors/rejections/retries, acknowledged rows/sec, end-to-end p50/p95/p99, aggregate freshness lag, CPU/RSS, disk/remote bytes, write amplification, compaction debt and recovery times. Specify offered load and arrival timestamps to avoid hiding overload through coordinated omission. Publish regressions as well as wins. Claims apply only to the measured workload/hardware/durability; seven samples in a local probe cannot establish production tail latency.

## Safe cutover sequence

1. Inventory and classify every required feature/query; unsupported requirements block migration rather than being silently approximated.
2. Perform a bounded backfill through a defined watermark into an isolated Varve instance, with stable IDs and tested timestamp/type conversion. Shadow ingest from a durable replayable application log/outbox or explicit CDC design; naive dual HTTP writes are not atomic.
3. Shadow reads and compare raw counts/checksums plus bucket/group aggregates, tie cases, late/corrected events, nulls and numerical tolerance. Compare at the same logical data watermark.
4. Soak under peak mixed load and complete restart, corruption, disk-full, lost-volume, object-store outage, restore and upgrade drills against agreed SLO/RPO/RTO. Require independent review.
5. Canary read traffic, then a deliberate write cutover at a fenced watermark. Keep the former system or a tested replay path current for a defined rollback window. Never retire Timescale backups just because smoke tests passed.
