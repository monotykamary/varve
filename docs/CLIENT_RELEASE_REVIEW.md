# Client and ingestion release review

## Executed local gate — 2026-09-16

`scripts/verify.sh` passed on the corrected source:

- 181 storage/server tests, including fault injection and 17 HTTP/WS/pgwire transport tests. One external-S3 test remains explicitly opt-in.
- 24 Rust client tests: 22 unit/protocol/type tests, one doctest and one actual-service integration test.
- 18 TypeScript unit tests and one actual-service integration test; strict typecheck/build, example compilation, package inventory and zero production dependency vulnerabilities.
- 14 Python entrypoint/stress regressions; shell syntax, workspace formatting and strict all-feature/all-target Clippy.
- RustSec scanned 341 locked dependencies without a reported vulnerability.
- Deterministic workload and local object-store contract/restore/lifecycle probes passed. These do not substitute for Railway evidence.

A separate native Chromium browser probe used the real bundled TypeScript SDK and native WebSocket against a disposable Varve service. It authenticated, created a table, inserted one row plus a two-row atomic batch and 16 concurrent individual rows, then asserted an exact 19-row SQL result and the exact bigint timestamp `9007199254740993n`. The server explicitly used a 64-slot request queue. The owned tab, processes and temporary data were closed/removed.

## Findings fixed before release

1. **Database lease lifetime.** Relying only on closing the lock descriptor allowed a duplicated/fork-inherited open-file description to prolong the lock. A deterministic duplicate-descriptor test failed before the fix. `Inner::drop` now explicitly unlocks only at the last database owner; surviving database clones still retain the lease, and closing an old descriptor cannot unlock its replacement.
2. **Divergent grouped remote history.** Per-item sequence/count/digest checks did not bind group order or membership. New durable whole-record fingerprints bind sequence, ordered items/rows and clocks. Reordered tied-OHLC and pruned-subset regressions failed before the fix and now reject reconciliation, including after local WAL checkpoint removal. Proof bytes are included in admission and undo accounting; legacy single-write metadata remains compatible.
3. **Client cancellation/backpressure.** Removing a pending correlation previously released byte credit while outbound frames remained retained. Rust reservations now follow queued/writing payloads through flush/discard. TypeScript admission includes native buffered bytes and rejects invalid counters. Its 512-byte/100-cancel regression changed from 100 retained frames (7,965 bytes) to six (474 bytes), rejecting the other attempts.
4. **Mutation ambiguity.** TypeScript treats SQL as potentially mutating because management CALLs are supported. Interrupted SQL and malformed matching-ID write errors preserve uncertain outcomes and protocol causes. Neither client reconnects or replays mutations automatically.
5. **PostgreSQL subset.** The simple-query path rejects management mutations through AST validation; authenticated idle sessions are not confused with incomplete-packet deadlines.

The independent Astra review witnessed the grouped-history and client defects. A separate bounded re-review found no additional concrete blocker in those four fixes. It inspected source/regression assertions rather than claiming another full test execution. Two earlier extension-enabled review launches failed in infrastructure and were not counted as reviews.

Contour reported advisory TypeScript validation/dispatch complexity and test-to-public-entrypoint coupling, with explicit unsupported-language/extraction gaps for Rust and other files. Exact selected witnesses were inspected; defensive branching and public API tests were retained. Structural exposure is not a correctness certification.

## Measured batching

`cargo run --locked --quiet --example ingest_bench -- 512` independently checked recovered data and reported:

- Individual path: 512 WAL frames, 3856.7785 ms.
- Queued burst: four WAL frames, 197.215666 ms.
- 512 completions, no failed/rejected requests, zero pending bytes/requests after drain.
- Peak in-flight accounting: 128 requests, 193,792 bytes.

Each frame uses a file plus directory sync. This is one local, in-process burst workload, not a production throughput guarantee, an always-sequential SDK loop, or an S3 durability measurement. See [INGESTION.md](INGESTION.md) and [machine-readable evidence](evidence/clients.json).

## Linux CI fixture correction

The first Linux CI run reached the transport suite and exposed a startup race: the fixture waited for the HTTP TCP socket but immediately connected to PostgreSQL before its listener was bound. Production initialization binds both before serving requests; a TCP handshake alone was not application readiness.

A deterministic fixture regression reserves a PG address without listening and proves HTTP-only readiness is insufficient. It failed before the helper correction. The helper now waits for every configured listener; all 18 transport tests and strict targeted Clippy pass. This adds one test to the inventory and does not change production code or weaken frame, authentication or durability assertions. A new full Linux CI run verifies the corrected candidate.

## Remaining gates

Railway source/build, live S3/protocol checks, both two-minute soaks, persistence redeploy, rendered template/button and cleanup are recorded separately in the template repository. Registry publication and clean installed-package probes remain pending until explicitly recorded in [CLIENT_INGEST_ACCEPTANCE.md](CLIENT_INGEST_ACCEPTANCE.md). Experimental single-node and DuckDB-alpha limitations remain unchanged.
