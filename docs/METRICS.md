# Execution and state observability

`Database::performance()` returns a serializable `metrics::PerformanceSnapshot`. `Database::query_worker_stats()` returns bounded-pool lifecycle counters. The existing authenticated `/metrics` endpoint exposes both alongside service, ingestion and storage metrics; no new endpoint or per-tenant label is introduced.

## Fixed phases

`varve_phase_duration_seconds` is a Prometheus histogram with exactly the 18 names in `metrics::Phase::ALL`:

- State: `state_lock_wait`, `state_lock_hold`, `snapshot`.
- Write: `write_prepare`, `wal_encode`, `wal_write`, `wal_sync`.
- Query: `query_wait`, `query_spawn`, `query_build`, `query_run`, `query_reset`.
- Maintenance: `checkpoint_prepare`, `checkpoint_publish`, `compaction_prepare`, `compaction_publish`.
- Storage: `disk_account`, `remote_io`.

Observations are monotonic-clock wall time of **attempts**, including errors. Nested and concurrent phases overlap: their totals must not be summed into an exclusive CPU profile or a critical-path percentage. A lock wait is not lock hold time. WAL sync observations include file and directory synchronization; encode counts do not count acknowledged transactions.

The Rust/JSON snapshot stores `count`, `total_ns`, `max_ns` and **disjoint** bucket counts. Prometheus emits cumulative buckets, `_count`, `_sum` in seconds and the separate lifetime `_max` gauge. Bounds are 1 µs, 10 µs, 100 µs, 1 ms, 10 ms, 100 ms, 1 s, 10 s, 60 s and infinity. Atomic counters saturate; concurrent snapshots are approximate, not a transactional audit log.

For one uninterrupted database lifetime, counter deltas can describe attempted work in an interval. A maximum cannot be differenced into an interval maximum. Detect restarts before subtracting counters. These coarse histogram buckets do not replace the benchmark's raw request-latency observations.

## Worker lifecycle

Fixed names are `varve_query_workers_active`, `varve_query_workers_idle` and `varve_query_workers_{spawned,reused,resets,discarded}_total`. Their exact definitions and fresh-only query behavior are in [QUERY_WORKERS.md](QUERY_WORKERS.md). An open network connection alone is not evidence that DuckDB execution was reused. Conversely, deliberate fresh execution or immutable-scope replacement is not automatically a failure.

## Storage sizes

Status and Prometheus separately expose `control_root_bytes`, `derived_encoded_bytes`, `derived_resident_bytes` and `derived_working_bytes` (Prometheus names have the `varve_` prefix). Their format/frontier and conservative-accounting definitions are in [DERIVED_STATE.md](DERIVED_STATE.md).

`metadata_bytes` retains the logical inline-catalog footprint; moving maps into page files must not make it falsely appear that resident state vanished. Encoded bytes, resident estimates and outstanding temporary reservations are different quantities. These limits do not constitute a total-process-RSS guarantee.

The Rust and TypeScript clients expose the four new status fields as optional, so an older server's absent measurement is not invented as a zero. Existing integer-preserving decoding remains unchanged.

## Evidence boundary

Metric units, saturation and histogram conversion have deterministic unit tests. The real service regression checks WAL observations, two scalar queries reusing a child, reset counts, idle capacity and released working reservations. These are correctness checks, not local load benchmarks. Performance effects require the pinned, matched-cap Railway comparison described in [REUSE.md](REUSE.md).
