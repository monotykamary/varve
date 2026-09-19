# Lossless frontier campaign

Status: in progress, 2026-09-18. This campaign does not claim a Timescale win. Fresh artifacts: `/tmp/varve-no-loss.whNPeE`. Historical source-qualified baseline: `84db9e135661f76584d0bc911cb0d707afd26fa7b02a06572ba2171d50b5bd1c`; optimized binary `ebed1b9e75255fd4c98ac6855fb9b250fee998803584586a90dbdc762c41c922`. Prior source and evidence are immutable. Existing dirty work is preserved. Workspace re-entry is an observation baseline, not a clean verdict.

## Where we must win

Durable ingestion-only throughput; durable data plus equally fresh aggregates; mixed-load intended-arrival write/read latency at matched offered rates; useful hot, checkpointed and retained-tier SQL. Show throughput, freshness and maintenance together, not only an in-memory queue microbenchmark. Historical Railway p1/p2: Varve60–61k versus Timescale106–112k ingestion rows/s, and60–61k versus75–78k including Timescale freshness/ANALYZE. Current code needs a new comparison; local hardware does not predict its rank.

## Non-negotiable semantics

No silently discarded work. Bounded capacity propagates producer backpressure; unlimited arrival rate with finite storage cannot be guaranteed. Explicit validation/capacity/deadline errors and ambiguous post-submission outcomes remain visible; no retry of unknown writes with a new identity. Admission is not acknowledgment. Local acknowledgment follows the existing file and directory fsync barriers and atomic raw/rollup/receipt publication. Independent recovery checks retain corruption/gap/idempotency assertions. Queue waiting retains the original intended arrival clock, and drain time/backlog must fail overload rather than manufacture a win. No budget increases, disabled maintenance, benchmark SQL special cases, stale aggregates, or unbounded queues.

| Gate | Concrete check | State |
| --- | --- | --- |
| L1 | Recover real win metrics and frozen baseline identities; trace write/query paths before edits | baseline hash verified; paths traced |
| L2 | Lossless bounded write scheduling, independently scheduled bounded reads, complete cancellation/drain accounting and regression tests | implementation delegated |
| L3 | Query setup work removed for changing data at unchanged budgets; exact raw/rollup/catalog scope and lifetime accounting | implementation delegated |
| L4 | Bounded ingestion backpressure through public API/transports, preserving explicit not-admitted versus unknown-outcome errors; deterministic saturation/shutdown/cancellation tests | Main investigating |
| L5 | Repeat real-service optimized tests with identical profile/data, sufficient samples, zero missing offered work, independent exact final oracles, maintenance and abrupt restart | pending |
| L6 | Local throughput/latency sweep and counterbalanced baseline/candidate evidence; retain failures and qualify p95/p99 separately | pending |
| L7 | Full fault/recovery suite, strict lint, SDKs, independent scoped review and frozen source evidence | pending |
| L8 | After local gate: fresh Linux runtime qualification; matched bounded Railway tests on existing benchmark services only, equal resource/durability settings and both backend orders | pending; no cloud activation |
| L9 | Preserve all reports and restart data oracles, stop exact-owned compute, report wins/losses without cherry-picking | pending |

Main owns all builds/measurements and non-query Rust code. Scoped agents own only the frontier driver and query adapter respectively. No publication, git rewrite, original-app mutation or old cloud authorization/root reuse.
