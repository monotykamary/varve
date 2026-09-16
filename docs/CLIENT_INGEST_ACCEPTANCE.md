# Clients, persistent transport, ingestion and Railway acceptance

Experimental scope remains explicit. Every row requires implementation and direct executable evidence, not merely a build. Local gate completed on 2026-09-16; see [review and evidence](CLIENT_RELEASE_REVIEW.md).

| ID | Requirement | Evidence | State |
| --- | --- | --- | --- |
| C1 | Standalone Rust client | 22 unit/protocol/type tests, doctest, actual server auth/write/SQL/i64/error/deadline/disconnect test; retained outbound-credit regression | passed locally |
| C2 | Node/browser TypeScript client | 18 unit tests, exact real-service oracle, typecheck/build/pack/audit; native Chromium: 19 rows and exact `9007199254740993n`; backpressure and ambiguity regressions | passed locally |
| C3 | WebSocket v1 | Auth/origin/limit/correlation/heartbeat/drain tests in the 18-test transport suite | passed locally |
| C4 | Standard database protocol | Real tokio-postgres client exercises simple-query/SCRAM, read-only SQL guard and persistent idle session; loopback-only subset documented | passed locally |
| C5 | Manual atomic batches | Rust/TypeScript insert-batch APIs/examples, CLI/HTTP batch docs and idempotency tests; examples typechecked | passed locally |
| C6 | Bounded multi-producer ingestion | Crossbeam queue with byte/row budgets, one batching writer, durable completion, overload/drain tests | passed locally |
| C7 | Durable group commit | 13 group tests plus library/admission/fault/restore regressions; ordered whole-record proofs survive checkpoint and reject divergence | passed locally |
| C8 | Measured single-insert benefit | 512-request self-checking local burst: 512 → 4 WAL frames, 3856.8 → 197.2 ms; no throughput/SLO claim | passed locally |
| C9 | Railway service + volume + bucket template | [Published template](https://railway.com/deploy/varve): exact 37-row/rollup/bigint/receipt oracle, real S3 restore/lifecycle contract, two 121s soaks, persistence redeploy, rendered route/button, secret-safe config and deletion readback; [evidence](evidence/railway-template.json) | passed |
| C10 | Integrated verification | Original local gate: 181 storage/server tests; corrected [Linux CI](https://github.com/monotykamary/varve/actions/runs/35055893569): 182; 24 Rust SDK tests including live/doctest, 18 TS units plus integration, 14 Python tests; fmt/Clippy/audits; public exports confirmed | passed |
| C11 | crates.io and npm publication | [`@monotykamary/varve@0.1.0`](https://www.npmjs.com/package/@monotykamary/varve) published with `bun publish`; registry SHA/SRI, fresh install, strict types, browser bundle and 19-row/recovery probe passed. Both Rust crates package/verify; crates.io rejects upload until the account email is verified. [Receipts and unblock steps](REGISTRY_RELEASE.md) | partial: npm passed; Cargo account blocked |

Linux CI exposed a transport-fixture readiness race after the original local gate. A deterministic new test failed before the helper fix; all 18 transport tests and strict targeted Clippy now pass. The helper waits for every configured listener, without changing production code. The corrected full Linux run passed on `45d7c014ea9070e6a2f299a752c121c748434762`.

The local suite deliberately leaves one real-S3 contract test opt-in; the local object-store probe is not cloud evidence. Only new task-owned Railway validation resources may be mutated; the existing evaluation is not a scratchpad. No enqueue-time acknowledgment, silent mutation replay, or unbounded buffers. Template publication remains gated on actual live success and advertises the accepted Varve/DuckDB-alpha experimental scope.
