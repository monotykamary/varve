# Clients, persistent transport, ingestion and Railway acceptance

Experimental scope remains explicit. Every row requires implementation and direct executable evidence, not merely a build. Local gate completed on 2026-09-16; see [review and evidence](CLIENT_RELEASE_REVIEW.md).

| ID | Requirement | Evidence | State |
| --- | --- | --- | --- |
| C1 | Standalone Rust client | 22 unit/protocol/type tests, doctest, actual server auth/write/SQL/i64/error/deadline/disconnect test; retained outbound-credit regression | passed locally |
| C2 | Node/browser TypeScript client | 18 unit tests, exact real-service oracle, typecheck/build/pack/audit; native Chromium: 19 rows and exact `9007199254740993n`; backpressure and ambiguity regressions | passed locally |
| C3 | WebSocket v1 | Auth/origin/limit/correlation/heartbeat/drain tests in the 17-test transport suite | passed locally |
| C4 | Standard database protocol | Real tokio-postgres client exercises simple-query/SCRAM, read-only SQL guard and persistent idle session; loopback-only subset documented | passed locally |
| C5 | Manual atomic batches | Rust/TypeScript insert-batch APIs/examples, CLI/HTTP batch docs and idempotency tests; examples typechecked | passed locally |
| C6 | Bounded multi-producer ingestion | Crossbeam queue with byte/row budgets, one batching writer, durable completion, overload/drain tests | passed locally |
| C7 | Durable group commit | 13 group tests plus library/admission/fault/restore regressions; ordered whole-record proofs survive checkpoint and reject divergence | passed locally |
| C8 | Measured single-insert benefit | 512-request self-checking local burst: 512 → 4 WAL frames, 3856.8 → 197.2 ms; no throughput/SLO claim | passed locally |
| C9 | Railway service + volume + bucket template | Tom-only resources created and wired; pinned-source live deployment, soaks, redeploy, rendered route/button and cleanup still required | pending live release |
| C10 | Integrated verification | `scripts/verify.sh` exit 0: 181 storage/server tests, 24 Rust SDK tests including live/doctest, 18 TS units plus integration, 14 Python tests; fmt/Clippy/audits; public exports confirmed | passed locally |
| C11 | crates.io and npm publication | `cargo publish` for `varve-storage` and `varve-client`; `bun publish` for `@monotykamary/varve`; clean package builds, registry readback and install/import smoke tests | pending publication |

The local suite deliberately leaves one real-S3 contract test opt-in; the local object-store probe is not cloud evidence. Only new task-owned Railway validation resources may be mutated; the existing evaluation is not a scratchpad. No enqueue-time acknowledgment, silent mutation replay, or unbounded buffers. Template publication remains gated on actual live success and advertises the accepted Varve/DuckDB-alpha experimental scope.
