# Due diligence and production boundary

## Evidence model

A successful build does not establish database correctness. The acceptance ledger ties guarantees to real behavior. Tests use deterministic event-time clocks, actual file sync/rename, the installed DuckDB v2, typed Parquet, a filesystem object-store implementation, TCP HTTP and separate killed/crashed processes. Live S3 is a separate explicitly enabled test. CI is configured, not claimed to have run remotely merely because its workflow exists.

## Failure model

| Event | Expected result |
| --- | --- |
| Invalid batch / conflicting request ID | Reject before WAL publication, no partial rows or view updates |
| Crash after WAL temporary fsync but before rename | Ignore unpublished temporary; retry may create the batch |
| Crash after WAL rename+directory fsync, before response | Replay committed batch once; retry returns the same receipt |
| Crash during segment write or after segment publication | Ignore/collect unreferenced output; replay WAL |
| Crash after manifest publication, before WAL cleanup | Checkpoint already includes rows, receipts and view state; do not replay them twice |
| Corrupt committed WAL/manifest/segment | Fail closed; do not silently truncate committed records or skip data |
| Upload succeeds but remote-head publication fails | Local data remains authoritative; uploaded orphans are not a committed remote snapshot |
| Remote-head publication succeeds, process dies before local binding | Exact durable intent permits first or later head adoption on reopen; later local WAL remains intact. Mismatched/corrupt intent and divergent history fail closed |
| Failed upload / outage | Never evict unprotected local data; recovery gap may increase |
| Lost local disk | Recover only the last published remote checkpoint plus its explicitly referenced WAL tail |
| Crash during restore / remote vacuum | Lock fails closed. Resume owned GC or explicitly recover an abandoned lock after stopping its owner |
| Raw expiration | Raw rows disappear and obsolete files are reclaimed; independently retained rollups and request receipts may remain |

Fsync correctness depends on the filesystem, OS and device honoring flushes. Process-kill tests are not power-loss/device-controller tests, nor are they Jepsen verification. Supported scope is a single POSIX filesystem owner and a single remote publisher, not concurrent active clones.

## Independent review checkpoint

Sol independently reviewed WAL/replay, manifest publication, view state, remote CAS/restore/GC, snapshot lifetimes and resource admission. Five concrete findings were addressed:

1. Global cache pins prevented a cold query from evicting unrelated cached files: use per-segment pins and conservative relation/time pruning. Regression: `tests/admission.rs`.
2. Rollup width/tag amplification bypassed raw memory accounting: bound update-map bytes and projected serialized metadata before WAL acknowledgment, reserve future segment-reference space. Regression: `tests/admission.rs`.
3. Remote size validation happened after unbounded allocation: bounded metadata-first filesystem/S3 reads, bounded collision checks and exact expected-size core reads. Regressions: `tests/remote_store.rs` and corrupt-restore tests.
4. Crash temporary files were counted before cleanup or never cleaned: remove recognized WAL/staging/manifest temporaries under the process lock before accounting. Added `segment_written` failpoint.
5. Segment working-set checks happened after decompression: persist conservative decoded-size metadata and check per-file/compaction budgets first. Regression: `tests/admission.rs`.

An additional local review rejects overlapping local/object-store filesystem roots, bounds CLI input, tests JSON integer extremes/non-finite encodings through HTTP and restore, pins partition hash vectors, and probes 10,000 finite float encodings. A direct DuckDB v2 probe exposed EXPLAIN output that bypasses JSON mode; the adapter now wraps it explicitly.

A second Sol implementation/review pass isolated background remote I/O from the state mutex. Blocking-store tests prove local writes/status proceed during uploads and maintenance prefetch, older prefixes publish coherently, failed uploads do not evict or advance the ship timer, and subsequent publication catches up. A killed-process test proves prefix-checked rebinding preserves concurrently acknowledged local rows. Rebinding checks complete bidirectional table/receipt history, immutable config/creation sequences, digests, owner identity, sequence continuity and monotonic cutoffs; UUID equality alone is not trusted.

Contour was invoked at the structural checkpoint. Its current analysis supports JS/TS, not Rust: its zero findings were explicitly treated as a coverage gap, not approval. Rust source review, Clippy and behavioral tests carry the evidence here.

## Resource and performance limits

- No bundled DuckDB C++ build. Cargo debug symbols/incremental builds are disabled, compiler parallelism is two, fixtures are small and self-cleaning.
- Metadata/group/request-ID admission intentionally refuses growth instead of pretending it can retain unbounded state in RAM. Checks use conservative estimates/serialized bytes, not an allocator-wide RSS guarantee.
- Parquet files have an explicit decoded-size estimate. Reopening with smaller budgets can deliberately reject scans/recovery until limits are raised; it must not allocate the full oversized working set first.
- The DuckDB subprocess has memory/thread/output/time limits and no disk spilling. Output is native DuckDB JSON; unsigned/128-bit numeric types can be strings to preserve precision.
- Whole cold SQL snapshots currently materialize into the bounded local cache. Safe simple-table and timestamp pruning avoids unrelated downloads; complex SQL falls back to complete referenced exposure. This is not an unbounded streaming S3 SQL implementation.
- Local WAL is one immutable frame per API batch, not per row. There is no cross-request group-commit coordinator. Batch sensibly; per-event requests create excessive filesystem/API/receipt overhead.
- Ship/maintenance calls are synchronous to their caller, but remote upload/prefetch/vacuum and cold-query reads release ingestion state. Native flush/compaction and WAL snapshot capture retain serialization costs. Paged remote vacuum and durable locks/intents support safe retries; restore-lock recovery remains explicit administration.
- Query startup, hot-row copying, metadata cloning, native flush/compaction and synchronous cold materialization are known performance costs. No competitive throughput or tail-latency claims are made.

The workload example verifies 2,000 rows in 256-row batches through hot SQL, checkpoint, archive, cold SQL, aggregate state and restore. Timings are from an unoptimized development build and include subprocess startup, so they are diagnostics rather than product benchmarks.

## Remote operations

S3 Standard/compatible immutable object segments are supported with conditional head create/update; no assumption of S3 Express append is required. Express One Zone does not become multi-AZ merely by using this adapter. Credentials use the object_store/AWS chain; no credentials are embedded or logged. HTTP endpoints require explicit opt-in.

Remote storage must support strong conditional writes. No unconditional-overwrite fallback exists. A filesystem adapter tests the protocol but cannot establish every S3-compatible vendor's conditional-write, consistency or failure behavior. Retain current objects; do not configure external bucket lifecycle deletion to remove live manifests, WAL or segments.

Remote GC is reachability based and protects only the current head, not arbitrary historical snapshots. Restore is an exclusive ownership transfer. Abandoned locks require operator confirmation that the old owner has stopped; blindly breaking a live lock invalidates safety. Deletion is logical/filesystem/object deletion, not forensic secure erasure of devices or independent backups.

## Release checklist still required before production use

- Run the opt-in suite against the actual S3 vendor/region/credentials policy, including real outages, throttling, ambiguous responses and cold restores.
- Repeated crash/power-loss testing on the intended filesystem and storage device; restore drills on lost hosts.
- Long-duration workload-specific benchmarks, large-cardinality/retention tests, allocator/RSS measurements and p95/p99 latency under compaction and cold-query pressure.
- Stabilize the on-disk format and migration policy; qualify a stable DuckDB v2 release before promising compatibility.
- Qualify TLS, token rotation/access control, platform containment and resource limits for the intended deployment. Authenticated public opt-in, bounded clients and negative SQL tests are implemented; these are not a hostile multi-tenant OS sandbox.
- Qualify the timed request-receipt window for the intended retry/outage policy. Native query integration, disk-backed metadata, streaming cold SQL, mutable schemas, replication and general SQL IVM remain separate scope decisions with their own acceptance requirements.
