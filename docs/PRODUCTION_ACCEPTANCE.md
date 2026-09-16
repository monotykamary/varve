# Production-hardening acceptance ledger

This extends the initial v0.1 ledger. User goal: exhaustive review, accessible scheduling/continuous aggregates, and an isolated Railway example/stress test in the **Tom** workspace. Production readiness is an evidence-backed release decision, not the result of one deploy or a finite smoke test.

| ID | Acceptance criterion | Evidence needed | State |
| --- | --- | --- | --- |
| P1 | Safe public operator API while loopback remains default | Auth before body reads, explicit public opt-in, request/connection limits, SIGTERM/SIGINT, negative security tests; readiness now bypasses full worker queue with a nonblocking state check | passed local harness and live authenticated HTTPS/Linux negative probes; restrictions are not an OS sandbox |
| P2 | Restricted DuckDB workers | Filesystem/network deny probes, environment isolation, exact selected paths, normal SQL/Parquet, large-script argv regression; eight adversarial tests include dynamic COPY/DDL/SET, selected-file integrity and deadlines | passed local harness and live authenticated HTTPS/Linux negative probes; restrictions are not an OS sandbox |
| P3 | Durable lifecycle controls | SQL CALL + Rust APIs, policy listing, validation, replay/checkpoint/remote restore, immutable physical schema | passed local harness and HTTP probe |
| P4 | Named continuous aggregates | Create/drop/list/query, new-width backfill, late writes, independent raw expiration, no double counting, admission | passed local harness and HTTP probe |
| P5 | Inspectable durable scheduling | Define/alter/pause/run/drop interval jobs; non-overlap, bounded retries/history, restart behavior, metadata SQL | passed; private runtime journal avoids authoritative WAL amplification |
| P6 | Preserve durability under control-plane changes | Explicit format handling, WAL-before-ack, metadata admission, owned-prefix reconciliation, divergent writers, crash boundaries | passed local fault/ENOSPC suites; hardware/outage qualification remains |
| P7 | Independent production safety review | Current source witnesses, regression fixes and clearly enumerated release blockers | 138 Rust + 14 Python tests passed; reviewed fixes plus remaining source/performance gaps are recorded in FINAL_REVIEW.md and AUDIT_RECONCILIATION.md |
| P8 | Reproducible isolated deployment | Remote release build; pinned DuckDB; non-root runtime; persistent volume; one replica; auth; observed deployment SUCCESS | current dbd59af2-5c4c-4e8e-a452-72914ca69528 SUCCESS + live probes; identical verified artifact/volume |
| P9 | Real cloud backing qualification | Dedicated S3-compatible bucket/prefix, actual conditional-write conformance, bounded reads, remote ship and isolated restore drill | passed real-bucket 4,000-row protocol/archive/restore/retention drill, including same-payload token freshness |
| P10 | Bounded cloud stress and persistence | Exact row/aggregate oracles, idempotency, late events, scheduled jobs, p50/p95/p99/errors/RSS/CPU, restart recovery and retention | 100,000 + 20,000 rows passed; same-artifact recreate preserved exact data; direct-restart incident unresolved |
| P11 | Operator documentation | Runnable SQL/HTTP/CLI examples, backup/restore drills, auth/secret handling, alerts, capacity limits, failure and upgrade procedures | written with observed results, provenance and incident caveats; see EVALUATION.md |

## Remaining source/performance work

The delayed earlier audit was reconciled against current code in [AUDIT_RECONCILIATION.md](AUDIT_RECONCILIATION.md). Streaming WAL recovery still checks decoded-state limits after applying each operation; recursive disk accounting and some shipping/local I/O remain state-serialized. Dedicated blocked cold-query/scan, high-cardinality longevity, million-key GC and general format-migration qualification are not complete. The 100,000-row run must not be represented as a 100,000-request-ID/group test.

## Release decision

The implemented evaluation checks pass, including live recovery by replica recreation, but **production qualification is not granted**. The direct Railway restart left an exited instance while reporting stale SUCCESS; recovery required redeploying the same artifact. Its root cause and broader availability qualification remain open. See `EVALUATION.md` for the complete evidence and limitations.

## Scope and safety

Only new Varve evaluation resources in workspace `18857f84-0af6-4379-9217-e62e2d14b48f` (Tom) may be mutated. Existing unrelated projects and workspace spending limits remain untouched. Stress generation must have fixed row/time/concurrency ceilings and abort on correctness failures. Secrets never belong in source, transcripts, images or command arguments. No public deployment before the authentication gate is verified.

The current dependency is a DuckDB v2 alpha; stable-release qualification, long-duration soak/power-loss testing, independent external audit, HA/distributed failover, general arbitrary-SQL IVM and OS-hard memory quotas cannot be inferred from these checks. Record any remaining blockers explicitly rather than relabeling experimental code as production-certified.
