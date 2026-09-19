# Execution and state observability

`Database::performance()` returns a serializable `metrics::PerformanceSnapshot`. `Database::query_worker_stats()` returns bounded-pool lifecycle counters. The existing authenticated `/metrics` endpoint exposes both alongside service, ingestion and storage metrics; no per-tenant metric label is introduced. Opt-in bounded group diagnostics are separately exposed at authenticated `/v1/diagnostics/ingest`.

## Fixed phases

`varve_phase_duration_seconds` is a Prometheus histogram with exactly the 38 names in `metrics::Phase::ALL`:

- State: `state_lock_wait`, `state_lock_hold`, `snapshot`.
- Write: `write_prepare`, `wal_encode`, `wal_write`, `wal_sync`.
- Query: `query_wait`, `query_spawn`, `query_build`, `query_run`, `query_reset`.
- Maintenance: `checkpoint_prepare`, `checkpoint_publish`, `compaction_prepare`, `compaction_publish`.
- Storage: `disk_account`, `remote_io`.
- Checkpoint attribution: `checkpoint_capture`, `checkpoint_locked`, `root_prepare`, `manifest_commit`.
- Admission attribution: `disk_lock_wait`, `disk_lock_hold`, `wal_disk_lock_wait`, `group_prepare`.
- Dependency output: `derived_verify`, `derived_publish`, `raw_verify`, `raw_publish`.
- Append accounting: `append_accounting` (appended after the original 30 indices).
- Commit boundary: `commit_lock_wait`, `commit_detach`, `commit_install` (indices 31–33). The first measures writer/structural gate acquisition, including failed poisoned-lock acquisition; it is not reader-state wait. Detach and install measure touched-delta movement while state is held, not WAL synchronization. Install timing excludes subsequent scalar frontier updates. These attempted phases overlap state-hold and must not be added to it.

All four checkpoint-attribution labels are wired into checkpoint execution. They report attempts, including failures; registration is checked across all 38 phases. Their presence is not performance qualification. They distinguish state-held capture, the complete synchronous fallback, dependency/root preparation, and the atomic manifest write. Existing `checkpoint_publish` includes derived preparation on the synchronous fallback path and must not be interpreted as root fsync alone.

The disk guard measures its actual scope, including tier publication intent. Append publication now releases that guard after writing the fully charged WAL temporary and releases reader state before both synchronization barriers; control/root paths can retain their original wider scope. Thus append `wal_sync` is no longer contained in `state_lock_hold` or `disk_lock_hold`. Phase IDs and units are unchanged. `disk_lock_wait` surrounds acquisition; `disk_lock_hold` starts only after acquisition returns and records before unlock. Standard poisoned-lock ownership remains an error, but its acquired guard is also measured until dropped. `wal_disk_lock_wait` is the append/control publisher's subset of generic wait; it does not identify the competing holder. Initial acquisition ends before budget admission and `wal_write`. Append rename reacquires admission inside `wal_write`; returned-error temporary cleanup also reacquires when necessary. A plain successful append therefore records two WAL-specific wait/hold scopes but still one publication and two syncs. A budget rejection before I/O records only the initial acquisition. These nested waits must not be added to enclosing publication time.

`group_prepare` covers state-held planning/admission intervals, including nested `wal_encode`. It ends before publication, checkpoint preparation, retry undo and state release; resumed planning starts a new interval. Counts are not request/group counts. Input hashing outside state is excluded. The legacy `write_prepare` slot is not wired to production preparation; zero observations there do not mean zero preparation cost.

`append_accounting` measures the one-use scalar receipt/rollup sizing projection, including failed attempts, not aggregate construction, row/digest validation, admission guards or publication. Direct writes normally project twice and must reproject after checkpoint-capable preparation; grouped staging projects per new item/attempt. Durable duplicates do not project. Database-open WAL replay uses the same database metric sink; internal verification/reconciliation replay without that sink remains uninstrumented. Counts therefore are not durable-request counts. It nests within state-held/group preparation where applicable; its wall time overlaps those phases. No existing phase index or meaning changed.

Dependency verification covers existing-object bounded reads and integrity checks. Publication covers the existing atomic write, file fsync, rename and directory fsync after budget admission. These remain under the original disk guard; no lock narrowing or durability change is implied. `raw_*` applies to raw **output** dependencies, not every query/cache/recovery read. Counts include failed attempts and cannot be called successful object reuse/publication or used as certified byte counts.

Observations are monotonic-clock wall time of **attempts**, including errors. Nested and concurrent phases overlap: their totals must not be summed into an exclusive CPU profile or a critical-path percentage. A lock wait is not lock hold time. WAL sync observations include file and directory synchronization; encode counts do not count acknowledged transactions.

The Rust/JSON snapshot stores `count`, `total_ns`, `max_ns` and **disjoint** bucket counts. Prometheus emits cumulative buckets, `_count`, `_sum` in seconds and the separate lifetime `_max` gauge. Bounds are 1 µs, 10 µs, 100 µs, 1 ms, 10 ms, 100 ms, 1 s, 10 s, 60 s and infinity. Atomic counters saturate; concurrent snapshots are approximate, not a transactional audit log.

For one uninterrupted database lifetime, counter deltas can describe attempted work in an interval. A maximum cannot be differenced into an interval maximum. Detect restarts before subtracting counters. These coarse histogram buckets do not replace the benchmark's raw request-latency observations.

`checkpoint_reclaim` (index 35) measures frozen-prefix obsolete-WAL directory synchronization and private allocation retirement after releasing state and the writer gate. `checkpoint_publish` now ends before that reclamation; historical measurements included it and must not be compared as an unchanged timer scope. Namespace removal and live WAL-byte accounting still occur under their original locks. The directory barrier remains required before successful checkpoint return.

`wal_file_sync` and `wal_directory_sync` (indices 36–37) distinguish the two acknowledgment barriers. Both remain nested in the original `wal_sync` and `wal_write` phases; each successful publication records one of each. Do not add child and parent totals.

## Group-correlated attribution

`admission_checkpoint` is appended as phase index 34. It encloses checkpoint calls initiated by write pressure, including preparation-gate wait; it overlaps checkpoint subphases and must not be added to them. Scheduled checkpoints are not mislabeled as admission checkpoints.

Opt-in `Ingestor::traces()` records phase observations only on the coordinator thread and for that database instance. The bounded authenticated diagnostic endpoint returns successful receipt sequences so a client can associate slow writes with the actual publication/checkpoint phases. See [INGESTION.md](INGESTION.md#opt-in-causal-diagnostics). Global sums remain useful for aggregate work but are not substitutes for these group records.

## Worker lifecycle

Fixed names are `varve_query_workers_active`, `varve_query_workers_idle` and `varve_query_workers_{spawned,reused,resets,discarded}_total`. Their exact definitions and fresh-only query behavior are in [QUERY_WORKERS.md](QUERY_WORKERS.md). An open network connection alone is not evidence that DuckDB execution was reused. Conversely, deliberate fresh execution or immutable-scope replacement is not automatically a failure.

The retained adapter also exports `varve_query_resident_dynamic_loads_total` and `varve_query_resident_dynamic_staged_bytes_total`. They count acknowledged, cleaned-up changes to the canonical non-raw payload; changing it to empty counts one load and zero bytes. They do not redefine `resident_hits`, which only means no missing raw batches. A planner-proven storage-only query may omit unreachable catalog rows, but metadata and storage-planner fallback queries must still install current coherent rows. Worker-fresh-only execution is a separate decision: it may install a schema-only catalog when the opt-in request has a positive storage proof. Later user-SQL failure does not undo an already acknowledged adapter installation.

`varve_query_resident_idle_materialized_bytes` is the sum of lifetime materialized-input charges in idle workers, including replaced raw/dynamic values and selected-ID bindings. Unlike `varve_query_resident_idle_bytes`, deletion does not refund it; retirement does. True no-op installs do not increase it. It excludes active workers and is not DuckDB allocated memory or RSS. Both ledgers share the reusable-cache allowance described in [QUERY_WORKERS.md](QUERY_WORKERS.md).

## Publication fencing

`Status.fenced` reports an existing State fence reason first, or a publication-gate poison reason requiring reopen/recovery when an armed epoch is dropped without installation. Gate-only fencing does not require mutating or locking State during drop. Readiness already rejects either condition; this status fallback makes that rejection observable without changing publication authority or automatically clearing a fence.

## Storage sizes

Status and Prometheus separately expose `control_root_bytes`, `derived_encoded_bytes`, `derived_resident_bytes` and `derived_working_bytes` (Prometheus names have the `varve_` prefix). Their format/frontier and conservative-accounting definitions are in [DERIVED_STATE.md](DERIVED_STATE.md).

`metadata_bytes` retains the logical inline-catalog footprint; moving maps into page files must not make it falsely appear that resident state vanished. Encoded bytes, resident estimates and outstanding temporary reservations are different quantities. These limits do not constitute a total-process-RSS guarantee.

The Rust and TypeScript clients expose the four new status fields as optional, so an older server's absent measurement is not invented as a zero. Existing integer-preserving decoding remains unchanged.

## Raw allocation ownership

`Status.raw_memory` (also `/v1/status`) is one coherent per-engine budget snapshot. Prometheus exposes these fixed, unlabelled names:

| JSON field | Prometheus name | Meaning |
| --- | --- | --- |
| `limit_bytes` | `varve_raw_memory_limit_bytes` | Configured regular raw ownership pool |
| `working_limit_bytes` | `varve_raw_memory_working_limit_bytes` | Separate maintenance/recovery pool |
| `reserved_bytes` | `varve_raw_memory_reserved_bytes` | All outstanding credits in both pools, including live rows and pre-allocation/workspace credits |
| `live_bytes` | `varve_raw_memory_live_bytes` | Credits attached to immutable row allocations, including retired hot/cache rows still owned by a capture/query |
| `working_bytes` | `varve_raw_memory_working_bytes` | Outstanding maintenance/recovery credits, a subset of reserved |
| `pinned_bytes` | `varve_raw_memory_pinned_bytes` | Unique immutable allocations held by query/checkpoint pin roles, a subset of live; multiple roles do not multiply this gauge |
| `peak_bytes` | `varve_raw_memory_peak_bytes` | Lifetime high-water mark of reserved bytes across both pools |
| `rejections` | `varve_raw_memory_rejections_total` | Saturating count of failed quota acquisitions/resizes, including transient-pressure retries and optional retained-copy skips; not failed requests |

All but rejections are gauges in bytes. `reserved_bytes - working_bytes <= limit_bytes`; `working_bytes <= working_limit_bytes`. Live/pinned/working overlap and must not be summed. Input and codec reservations can be outstanding without a live immutable row allocation. Estimates and independent role reservations intentionally overcount some simultaneous workspace; each linear reservation refunds exactly once. Pins can remain nonzero with zero hot/cache designation bytes. Eviction does not refund a pinned allocation, and opaque worker identity tokens retain neither credit nor weak control blocks. The two charged raw Arc containers are freed before payload/credit destruction, including concurrent last-alias drops.

The raw allocation gauge and ingestion `pending_bytes`/group-byte ceilings are intentionally **different budgets**: queue/group accounting retains the existing admission estimate; raw credit includes retained payload capacities, the input vector, and transient encoding workspace. Do not sum them or substitute one for the other. Transfer/split changes designation to `live_bytes` without reacquiring or briefly refunding credit. `live_bytes` includes excess capacities of moved strings/maps, not merely logical row lengths. Transient envelope credit falls only after terminal buffers/inputs are destroyed, including failed or panicking materialization after borrowed WAL encoding. Raw-only waits contribute to existing ingestion admission-wait diagnostics; a raw quota rejection counter increment can be a successful request's transient retry, not dropped data.

Native scanner fixed workspace, selected batch pin metadata and dynamically allocated callback-owner credits contribute to regular `reserved_bytes`, not `working_bytes`; original shared row allocations remain counted once in `live_bytes`. Native callback buffer usage is bounded by one query-wide thread-slot pool, while repeated bindings acquire independently charged owner credits. Focused test diagnostics record actual batch/column/buffer capacities, peak simultaneous callbacks and dynamic-owner high-water marks; these are not new public Prometheus gauges. Cancellation joins, database teardown, fixed scanner metadata and final scratch Arc deallocation precede release of fixed scratch credit. Callback-box credit is refunded after box backing deallocation, not merely after its final field destructor.

These metrics reset on reopen and are not allocator/RSS measurements. Existing `hot_bytes`, `decoded_cache_bytes` and derived metrics retain their designation meanings. See [CONFIGURATION.md](CONFIGURATION.md#raw-ownership-admission) for estimates, admission behavior and exclusions.

## Evidence boundary

Metric units, saturation and histogram conversion have deterministic unit tests. The real service regression checks WAL observations, two scalar queries reusing a child, reset counts, idle capacity and released working reservations. These are correctness checks, not local load benchmarks. Performance effects require the pinned, matched-cap Railway comparison described in [REUSE.md](REUSE.md).
