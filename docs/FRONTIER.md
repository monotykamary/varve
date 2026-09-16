# First-principles frontier work

This ledger retains the v3/v4 stage. The subsequent [reusable execution and storage ledger](REUSE.md) records implemented follow-ups, the v5 admission failure and the v6 overload/restart qualification; historical next-step statements below describe the v4 boundary.

This is an active engineering ledger, not a claim that Varve beats Timescale or is production-ready. The goal is a narrow time-series frontier, not PostgreSQL feature parity. Explicit non-goals can remain non-goals in a production release; operational and correctness qualification of the supported contract is the gate. Preserve the original [Railway evidence](TIMESCALE_BENCHMARK.md); subsequent candidates get separate source identities and reports. All performance/load measurements remain remote. Small local correctness tests are allowed.

## Edition and release labels

`edition = "2024"` selects language compatibility rules, not the age of the compiler. Rust 2024 is the latest stable edition; this workspace is currently checked with Rust 1.98.1. `rust-version = "1.90"` is the package's declared minimum compiler, not a compiler pin. See the [official edition guide](https://doc.rust-lang.org/edition-guide/editions/).

The user authorizes removing Experimental from `varve/README.md`, the GitHub repository description and `monotykamary/README.md` **only after readiness gates pass**. Leave all three unchanged until then. No single microbenchmark or passing correctness suite satisfies that condition.

## Acceptance ledger

| ID | Concrete check | State |
| --- | --- | --- |
| F1 | Preserve edition 2024; distinguish edition, current compiler and minimum compiler | verified against Cargo metadata, local compiler and official Rust docs |
| F2 | Grouped writes resolve reclaimable checkpoint headroom before publication, with at most one retry; never checkpoint provisional rows or weaken metadata/recovery bounds | passed old-fail/new-pass fixture, timed-floor crossing, exact guard and replay checks; one headroom retry per bounded physical group |
| F3 | Disk-only/empty SQL avoids the JSON scanner; nonempty input avoids per-row JSON object allocation without changing types, escaping or isolation | passed typed-empty/borrowed-input probes; actual Database::query reaches bounded 32KiB catalog literals with NUL/size/script-headroom fallback |
| F4 | Blocked native and SQL cold snapshots survive expiration plus vacuum; unpinned objects still reclaim and released pins eventually reclaim | both native and SQL regressions failed before the fix and pass afterward; unrelated objects reclaim and released pins eventually reclaim |
| F5 | Apply bounded-ownership ideas without replacing a proven runtime/queue speculatively | Crossbeam batching retained; turnloop source reviewed as inspiration, not dependency |
| F6 | Full affected correctness, recovery, format and Clippy checks; independent source review | final v4: 243 workspace tests passed, one live-S3 test ignored; strict Clippy/formatting passed; 14 headroom crash/I/O cases and sequence 9→10 replay covered; independent source review caught a locked-GET regression, now fixed with old-fail/new-pass tests |
| F7 | Matched-cap remote comparison with independent oracles, unchanged durability and retained failure data | [v3](evidence/frontier/v3/README.md) and [v4](evidence/frontier/v4/README.md) retained separately; v4 baseline passed at 550k, 1M preparation completed, but paired 20k target overloaded (532k acknowledged, 68k pre-submit drops, zero failed/ambiguous); actual restarts preserved 2.082M rows/backend; 1,600 timed query oracles and 11,000 raw latency samples mechanically audited |
| F8 | Remove Experimental only once release/operational gates pass across all three requested locations | blocked; labels preserved |
| F9 | Respect flush cadence, avoid no-op catalog clones/cold GETs, and reuse exact metadata projection work without changing bounds | local correctness passed and frozen source 74a0f2e… was measured remotely; observed baseline/mixed write results improved over v3 but not every query improved; the 20k target still failed; no performance-frontier claim |

## Cost model and chosen work

The prior trial measured initial ingest at 41.6k versus Timescale's 53.0k rows/sec, and columnar-stage selective p95 at 148 versus 7.82 ms. These are application-path measurements, not attribution to queueing, process startup, parsing or storage alone.

- **Admission:** single writes already checkpoint on projected metadata pressure; grouped writes checked the same exact reservation but lacked that fallback. Fix the asymmetry with bounded pre-publication rollback/checkpoint/replanning. No catalog clone, no raised cap and no publication retry.
- **SQL boundary:** do not invoke a JSON scanner for a sentinel when a snapshot contains no payload rows. Serialize borrowed typed fields rather than allocating a JSON object tree per input row. These are mechanical work reductions; remote results must establish whether they matter for latency.
- **Reclamation:** a snapshot's ownership extends through pending remote GET. A checkpoint dropping a segment does not mean an older reader has released it. Remote reachability must account for that lifetime.
- **Next architectural frontier:** isolate fixed subprocess/setup cost from snapshot selection, materialization, encoding and DuckDB execution before designing persistent workers. Reuse must preserve per-query data/reset, exact file allowlists, stripped credentials, cancellation/reaping and bounded memory. Keep the current fresh-process boundary until a replacement proves those properties.

## What v4 establishes about the architecture

The full [v4 report and source](evidence/frontier/v4/README.md) preserve the negative result. One million rows reached durable storage plus a fresh aggregate in 20.46 seconds for Varve versus 13.81 for Timescale, including Timescale's explicit refresh/ANALYZE. Raw ingestion alone compares different aggregate-maintenance schedules. Larger-run columnar cross-series grouping p95 favored Varve (83.70 vs 121.79 ms), but selective, named-aggregate and window queries remained much slower. Single trials do not establish general superiority or a saturation frontier.

Source-confirmed costs, not percentages attributed by a profiler:

- `src/query.rs::execute_with_catalog` creates a fresh process and in-memory DuckDB database for each query; selected hot/view data still crosses a JSON materialization boundary. Five empty-database observations show tens of milliseconds of fixed boundary cost, not a complete explanation of workload latency.
- `src/engine.rs::checkpoint_locked` and `src/policy.rs::compact_locked` perform substantial local Parquet/catalog work under the shared state lock. Correct single-writer ordering does not require every expensive operation to occupy that critical section.
- `src/engine.rs::commit_record` encodes a record to measure it; `src/wal.rs::append` encodes it again and publishes a separate file with file and directory fsync. `ensure_budget` recursively accounts the data directory. These are conservative correctness choices with avoidable allocation, filesystem and scaling costs; durability must not be weakened to remove them.
- Rollups and receipts live in the manifest's table state. Checkpoint clones/serializes that catalog, and SQL rollup selection scans/filter-copies its map. Growing derived data therefore burdens metadata publication and query preparation. Indexed, independently scalable derived state is a structural change, not a larger metadata cap.
- The comparator uses persistent PostgreSQL connections, transactional COPY and an explicit tenant/series/time index, plus Timescale columnstore and materialized views. Removing PostgreSQL feature breadth does not replace its optimized access paths, buffered WAL and concurrency machinery.

Next steps should be phase-resolved profiling, then bounded off-lock checkpoint/compaction with generation-checked publication, a persistent isolated typed query boundary, indexed hot/derived state, and measured WAL/accounting redesign. Keep one durable commit order, snapshot isolation, exact admission, cancellation and recovery proofs. Neither a custom queue nor merely switching languages solves these costs. S3 was not involved in this benchmark.

Both restarted processes and all committed fingerprints verified. [Cleanup proof](evidence/frontier/CLEANUP.json) confirms zero active benchmark deployments, the original service unchanged, and both data volumes retained. Storage remains allocated; compute is stopped.

## turnloop research

Read [PerryTS/turnloop](https://github.com/PerryTS/turnloop) at commit `164e8e444f7f5d7331ae079026263b794d5599b6`, particularly `DESIGN.md` D4–D8 and `crates/turnloop/src/notifier.rs`. Its README explicitly says **Pre-alpha**. No code is vendored and no dependency is added.

Useful transfers are bounded work turns, host-owned state, precise acceptance/completion/cancellation distinctions, capacity checks and notification only when a consumer is parked. Its notifier uses RUNNING/PARKED/NOTIFIED state and coalesces wakes. Its design also records that reducing reactor turn overhead can increase total work when outer pumps run more often—optimizing an inner loop is not automatically an application win.

Varve's existing Crossbeam consumer already blocks on queue receipt and uses deadline-bounded batching. Neither Crossbeam nor turnloop is a durability primitive. A custom ring, reactor, io_uring path or lock-free rewrite requires evidence of the corresponding bottleneck and a simpler demonstrable ownership/recovery contract.
