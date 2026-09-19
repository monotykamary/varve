# Performance frontier acceptance ledger

Status: source qualification complete; performance frontier incomplete. No new Timescale win or cloud qualification claimed. Verified local results and failures are preserved in [frontier-jszmov evidence](evidence/frontier-jszmov/README.md). Baseline is the existing dirty worktree including the locally tested resident-hit fix. Prior frozen pilot evidence stays immutable. Private campaign artifacts: `/tmp/varve-frontier-gate.JsZmOV`.

## Completion contract

Report ingestion-only and durable-data-plus-fresh-aggregates separately. The old p1/p2 reports show 60–61k vs 106–112k ingestion rows/s, but 60–61k vs 75–78k rows/s using their full fresh-aggregate barrier (including Timescale ANALYZE). Neither is a new measurement. Compare latency at identical offered rates, batch sizes, concurrency and resource/durability settings. Count intended-arrival delay, failures, rejections and final drain; no unbounded queue is a throughput win.

| Gate | Required evidence | Status |
| --- | --- | --- |
| P1 | Preserve baseline source/binary identities and source-bound results | baseline input manifest, prior patch and release binaries preserved |
| P2 | Bounded real-HTTP local probe, deterministic data/oracles, explicit profile, fresh root, owned process cleanup | corrected baseline and instrumentation-only release pilots passed; 152,400 rows each |
| P3 | Scheduled arrivals, independent acknowledgment timing, overload/drop accounting and drain; throughput–latency curve, not just closed-loop throughput | accounting/cancellation tests pass; independently scheduled writes expose overload, but reads remain closed-loop; matched rate sweep and independent read arrivals pending |
| P4 | Correlate writes/groups with phase costs and admission-pressure checkpoints; phase totals alone cannot explain tails | all 50 mixed-write receipts matched retained traces; checkpoint and commit-gate stalls witnessed |
| P5 | Retained query setup validated at useful size across hot/checkpointed data and mixed writes; budget failures remain failures | baseline and earlier candidates exposed OOM; final optimized candidate passes 457,200-row aggregate checks with zero drops/failures at 5k/s offered; 15k/s checks 546,400 rows without SQL/write errors but drops 6,000 offered rows, so that point fails performance admission |
| P6 | Optimize measured boundary while retaining local fsync, corruption detection, atomic visibility and dedup recovery | reclamation lock and query-cache lifetime fixes have deterministic red/green proof; final release measured with unchanged resource/durability settings; no isolated speedup or Timescale claim |
| P7 | Counterbalanced baseline/candidate real-service measurements over multiple maintenance cycles with sufficient percentile samples | first fresh A–B–B–A stopped on baseline OOM; no comparative speed claim; all attempts preserved |
| P8 | Targeted regression/fault tests, strict lint, direct crash/reopen oracles and source review | final source passed full fault workspace suite, both strict lint modes, fmt, 50 query units, real-service metric witness and actual release SDK smoke; 31 Python tests pass; source/artifact hashes verified; no independent review certification |
| P9 | Fresh Linux/runtime qualification and bounded matched Timescale comparison; preserve complete results and clean up exact-owned compute | not started: local overload remains; all local controllers/services finished, old cloud approvals/roots not reused |

## Rules

No container builds or broad local dependency installs. Do not alter prior benchmark receipts, old cloud approvals, retained data or the original app. Do not weaken fsync/corruption guarantees, suppress maintenance, raise budgets to disguise admission failures, or treat microbenchmarks as cloud results. Moving WAL I/O outside the reader mutex does not eliminate serialized commit service time. Releasing locks around a pressure checkpoint does not make its initiating writer nonblocking. No eager tier cascade is introduced.
