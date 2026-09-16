# Initial local verification record

This is the historical baseline before the subsequent hardening and cloud work. The current 138-test/14-Python-test harness and live deployment results are in [EVALUATION.md](EVALUATION.md). Counts, disk cleanup and exclusions below describe that earlier checkpoint, not the current workspace.

Date: 2026-09-15 UTC. Platform: macOS/Darwin arm64.

- Rust `1.98.1 (48a229cea 2026-09-01)`.
- Cargo `1.98.1`.
- Real DuckDB `v2.0.0-alpha41533 (Cyanoptera)`, commit `10de957379`.
- Cargo.lock: 282 packages; RustSec fetched 1,246 advisories and reported no vulnerabilities or warnings.

## Checks actually run

- `scripts/install-duckdb.sh`: downloaded the pinned CLI, verified SHA-256/layout/version, installed it, cleaned its temporary; a second invocation correctly reused it.
- `scripts/verify.sh`: formatting, strict Clippy, all targets with fault injection, self-checking 2,000-row workload and Cargo audit passed.
- That complete test run passed 66 tests and ignored the single explicitly opt-in live-S3 test. One subsequently added first-publication crash/namespace-recovery regression passed in an exact focused run: **67 passing tests total**. Production source was unchanged by that test addition.
- Strict Clippy with `--all-targets --features fault-injection -- -D warnings` and again without the feature passed after the final test addition. Formatting and default `cargo build --locked` passed.
- A stripped default-feature executable was copied to `.tools/varve` and independently exercised through init/create/write, hot SQL, real Parquet checkpoint, filesystem remote ship, lazy cold restore and SQL. It returned `[{"count":2,"total":30.0}]`.
- Setting `VARVE_FAILPOINT=wal_renamed` during that default-binary write did not terminate it: production/default builds do not activate failure injection.
- Shell syntax checks passed. Public admin command registrations and every configuration entry are mechanically checked by tests. Primary architecture reference URLs were checked.

The final suite spans library/planner/WAL/segment units, admission, background I/O, storage engine, model, query, remote store and service integration tests. Crash worker tests are exercised by parent tests in separate processes.

## Requested storage/query retry

After the delayed agent report about missing `src/tier.rs` and `src/policy.rs`, the user requested an actual retry. Both modules and their registrations were confirmed present. With `.tools/` on PATH, `cargo test --locked --lib --test query -- --test-threads=2` rebuilt successfully and passed **16 library tests plus 6 real-DuckDB query tests**, with zero failures. The missing-module blocker did not recur; no implementation changes were necessary. These are reruns of existing tests, not 22 additional unique tests. Disposable retry build artifacts were cleaned while preserving `.tools/varve` and `.tools/duckdb`.

## Small mixed-workload probe

This is **not a throughput benchmark**. It used an unoptimized build, 2,000 rows in 256-row batches, four resulting segments and self-cleaning temporary storage.

| Phase | Observed microseconds |
| --- | ---: |
| Ingest | 145146 |
| Hot SQL | 437393 |
| Checkpoint | 80356 |
| Archive | 69754 |
| Cold SQL | 262496 |

The probe asserted local fsync receipts, expected hot/cold SQL, rollups and restore. Its reported remaining local data footprint after archive/cold query was 25,814 bytes. SQL timings include subprocess startup and are diagnostic only.

## Disk hygiene

No bundled DuckDB source build, container images or large generated fixtures. Cargo debug symbols/incremental compilation were disabled and builds used two compiler jobs. Temporary fixtures and installer archives were cleaned. After preserving and smoke-testing the default executable, this project's disposable `target/` was removed with explicit-target `cargo clean`, reclaiming approximately 3 GiB of build artifacts.

Ignored local tools remain: approximately 47 MiB for stripped Varve and 61 MiB for the pinned DuckDB CLI. Source/docs/tests are small. Re-running Cargo tests rebuilds disposable artifacts; none are part of the source repository.

## Not claimed at this initial checkpoint

Live S3 was not accessed; hosted CI was configured but not run; no release-optimized, sustained-load, p95/p99, power-loss or distributed correctness qualification was performed. No GitHub publication, push or commit was made. See [DUE_DILIGENCE.md](DUE_DILIGENCE.md) and [REMOTE.md](REMOTE.md) for operational boundaries.
