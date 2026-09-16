# Varve acceptance ledger

This is the initial v0.1 baseline, verified locally on 2026-09-15. Its counts and not-yet-run cloud/CI checks are historical, not the current status. See [VERIFICATION.md](VERIFICATION.md) for that baseline, [EVALUATION.md](EVALUATION.md) for subsequent hardening and cloud evidence, and [PUBLICATION.md](PUBLICATION.md) for the GitHub publication gate. Passing these checks establishes the stated behavior, not production readiness.

| ID | Guarantee | Executable evidence | Status |
| --- | --- | --- | --- |
| A1 | Rust library + CLI; append-only time-series schema | `tests/model.rs`, `tests/engine.rs`, HTTP typed ingestion and direct default-binary CLI probe | pass |
| A2 | Local fsync, atomic batches, idempotent replay | WAL corruption/version tests; process crash matrix; HTTP kill/restart; conflicting retry; exact-float probes | pass |
| A3 | Stable hash shards/event windows | Golden FNV vectors; negative epochs/lateness; HTTP i64 extremes through checkpoint and restore | pass |
| A4 | RAM → sorted ZSTD Parquet; coherent snapshots | Typed segment tests; concurrent query/flush/compaction; snapshot pin/GC tests; mixed workload | pass |
| A5 | Real DuckDB v2 SQL, hot and cold | `tests/query.rs`, conservative planner/admission tests, EXPLAIN/ANALYZE probes, output/time/concurrency limits | pass |
| A6 | Incremental count/sum/min/max/average/OHLC | Late arrivals, deterministic ties, replay, raw-vs-derived retention and workload assertions | pass |
| A7 | Remote WAL/checkpoint publication and restore | Filesystem outage/corruption/CAS/restore tests; blocking-store concurrency; process-crash prefix rebinding; explicit first-publication namespace recovery | pass locally; live S3 not run |
| A8 | Evict only protected files; bounded caches | Failed-upload no-eviction, cold restore/cache pressure, individual pins and unrelated-cache eviction | pass |
| A9 | Scheduled flush/compaction/archive/expiration/GC | Deterministic maintenance clocks; partial-file retention; local/remote reachability GC and restore-lock tests | pass within documented retry limits |
| A10 | Admission/observability | Batch/hot/WAL/disk/query caps; amplified rollup metadata; pre-decompression admission; recovery gap and status during blocked upload | pass for logical budgets, not OS quotas |
| A11 | Ownership and crash publication | Process lock; staged-file/manifest crash points; snapshot lifetime; divergent publisher fencing and proven-prefix recovery | pass |
| A12 | Real HTTP process lifecycle | `tests/service.rs`: ingest/query, scheduler, abrupt death/restart, SIGINT drain, public-bind refusal, JSON numeric errors | pass for trusted loopback |
| A13 | Due diligence/reproducibility | Strict Clippy in both feature modes; fmt; 67 passing tests across full + focused runs; pinned Cargo/DuckDB; RustSec 0 findings; CI configured; independent review; retained binary smoke probe | pass locally; hosted CI not run |

## Deliberate exclusions

Distributed writes/failover, distributed transactions, mutable arbitrary relational schemas, general SQL incremental maintenance, zero-copy/native DuckDB integration, unbounded cold SQL streaming and hard latency/RSS guarantees. SQL is trusted-local, not a hostile multi-tenant sandbox. First remote publication without a durable prior binding requires explicit namespace recovery after an ambiguous crash. Live cloud conformance and long-duration production qualification remain mandatory future work.
