# Integrated Railway qualification — 2026-09-18

**In progress. No production or performance approval.** All executable work ran on Railway. `scope.json` records the owned services, volumes, bucket/prefix and sandbox; excluded services were not changed.

## Evidence boundaries

- `candidate-source-manifest.sha256` identifies the earlier deployed candidate, not every subsequent working-tree change.
- `integrated-tests.log`, `integrated-remaining-tests.log`, `final-repair-checks.log`, `rebuilt-stack-checks.log`, native residency/Clippy logs and SDK logs preserve actual checks and failures.
- `rebuilt-live-s3.json` records the live S3 journal restore, deduplication, archive/native query, retention and vacuum probe. This is not a cloud durability SLA.
- `authority-native-source.sha256` identifies the later authority/startup repair files; `authority-native-review.log` records **9 journal-engine + 12 native tests passing**. Those older smoke runs predate these repairs; the later source-bound deployment is recorded in `raw-safety-runtime/`.

## Diagnostic comparison

`ior6_smoke_02.json` used single-row COPY on Timescale. That adds avoidable protocol round trips; its relative ingestion timing is not an ordinary-insert win.

`ior6_smoke_03.json` uses a single atomic autocommit SQL statement for one Timescale event plus its durable idempotency receipt; larger batches still use COPY. It offered 1,024 initial single rows with four writers, then 50 paired late rows over two seconds. Both databases had matching observed 2-CPU and approximately 2-GB cgroup limits. This is a short diagnostic, not a sustained frontier result.

Its final state is **overloaded**: conservation and freshness checks passed, but the strict intended-arrival scheduling gate failed. Initial diagnostic rates were approximately 290 rows/s for Varve and 452 rows/s for Timescale. These are not qualified capacity or comparative-tail results. No errors, pending rows or ambiguous rows were hidden to obtain a passing verdict.

## Exact conservation and restart

- `ior6_smoke_03-exact-before.json`: preserved failed verifier attempt; SQL returned Varve tags as JSON text and Timescale tags as a decoded object.
- `ior6_smoke_03-exact-before-v2.json`: all **1,074 raw identities and 1,074 aggregate groups** matched after strict representation normalization.
- `ior6_smoke_03-exact-after-restart.json`: the same complete comparison passed after both owned database services restarted, with unchanged database identities. Both existing deployment IDs were observed `SUCCESS` afterward.
- `review-0918/exact-tests-tags.log`: 8 tests, including middle-row substitutions, missing/duplicate rows, wrong aggregate groups and strict empty-map normalization.
- `review-0918/single-row-green.log`: atomic single-row comparator contract test.
- `driver-unit-final.log`: all **41 core, runner and exact-verifier tests passed** together.

The verifier makes no writes or refreshes, uses bounded sequential pages, and never promotes an overloaded benchmark. It does not claim first/last/OHLC tie equivalence, sustained load, or crash-under-load coverage from this restart check.

`driver-smoke-03/` freezes the actual driver dependencies, verifier and focused tests. Its four driver hashes match the report artifact. Keep these bytes when rerunning the exact verifier; current benchmark source may evolve independently.

## Larger paging/aggregation diagnostic

`ior6_smoke_04.json` passed its short batch-128 workload (8,192 initial rows plus 256 late rows). `ior6_smoke_04-exact.json` independently compared **8,448 raw rows across three pages and 4,352 aggregate groups across two pages**, including non-singleton groups. The current shipping source also passed all 41 affected tests and Python compilation on the driver (`review-0918-next/unit.log`). This diagnostic does not qualify sustained capacity or repair the ordinary single-row scheduling failure. Its driver differs from smoke 03 by the Dockerfile including `verify_exact.py`; preserve that source identity when reproducing it.

## Journal lifecycle handoff reconciliation

The delayed original journal handoff is already integrated. `journal-lifecycle.log` records **45 tests passed on Railway**; `journal-engine-integration.log` records the initial bridge checks. Current journal source contains those 45 tests plus two later read-only sealed-file validation tests. Both current journal file hashes match `candidate-source-manifest.sha256` and the later frozen overlay source. The 45-test log is historical coverage, not a claim that all 47 tests ran together on the latest engine. `journal-remote-integration-v2.log` separately records 11 passing remote lifecycle tests and six ingestion tests.

## Later source qualification and review

`raw-budget/` preserves the ownership implementation's source manifests, 13 ownership tests, related ingestion/CLI/native/publication tests and both strict Clippy runs. A read-only frozen-source audit found no actionable reservation/owner defect. This is conservative engine-raw accounting, not an RSS quota or a guarantee that arbitrary quota/profile combinations make progress.

`private-overlay/` records the next source checkpoint: live append preparation no longer mutates/undoes committed maps. Final-source engine/raw/boundary (92), native (12), both Clippy modes and formatting passed. The 97 selected integration tests passed **before** the final test-only `derived.rs` annotation; they are not relabeled as exact-final-source runs. The independent review found one inherited early-pressure-checkpoint durable-retry classification defect, now assigned to the epoch-handoff owner. See `private-overlay/review.md`. This is still serialized preparation, not independent partition ownership or completed pipeline P/D/V wiring.

Independent verifier review found missing source-accounting/run binding and unbounded cleanup. These were tightened; `review-0918-next/reviewed-unit.log` records **44 passing tests**, including malformed conservation, namespace/base alignment and stalled-close cases. `ior6_smoke_04-exact-reviewed.json` repeats the complete 8,448-row/4,352-group verification with the revised verifier; its exact source is retained in `review-0918-next/`.

`ior6_atomic_02.json` and the retained atomic probe witness actual PostgreSQL event-error rollback of the receipt, concurrent identical IDs producing one fresh/one duplicate result, and concurrent conflicting IDs producing exactly one event. `ior6_atomic_01.json` preserves an earlier probe-fixture field-name error, not a database failure. Durability settings were observed enabled.

`native-construction/` is a tiny pinned-library phase diagnostic, **not matched-resource benchmarking**. Eight fresh empty sessions observed approximately 23–32 ms in database open and 5–15 ms in teardown; environment creation and connection were much smaller. The sandbox exposed eight online processors but no readable cgroup ceilings. Pre-open settings do not remove this per-query construction work. Any reuse optimization must preserve snapshot lifetimes and exact file authority; these timings grant no permission to weaken isolation.

## Owned epoch handoff

`owned-epoch/acceptance.md` and its manifests bind the completed four-stage handoff to source digest `86c317d4e103be5f69e55de0852885ecd9ae4f6be5d064a00c0c45d7db538c5d`. Final-source checks passed: **98 engine/raw/boundary, 12 flow, 6 ingestion-unit, 12 native and 119 selected integration tests**, both strict Clippy modes and formatting. The consuming prepared/durable types retain exclusive publication authority across workers and fence uninstalled durable drops or publication panics. Ring progress is a terminal-slot prefix, not a public WAL D frontier.

The earlier private-overlay review's checkpoint-failure finding and the adjacent successful-checkpoint receipt-pruning defect both have retained red/green regressions. Those repairs are part of this final manifest, not retroactive claims about the older overlay source. Independent frozen-source review found no actionable correctness defect in the scoped patch; additional post-sync/checkpoint-boundary tests and gate-only status reporting were assigned separately. See `owned-epoch/review.md`. Preparation remains globally serialized; this is not partition-owner or performance qualification.

## Transferable raw credit and bounded native scratch

`raw-pressure/acceptance.md` binds final source manifest `5244039ae491f1ccd3481b630967b7127435572cf35a775e13e21ad26b5b33e3` (128 paths). Final-source checks passed **107 engine/raw/boundary, 12 flow, 10 ingestion-unit, 19 native and 121 selected integration tests**, both strict Clippy modes and formatting. The public red-03 fixture reproduces the old empty-scan quota failure and admitted-burst second-copy rejection; unchanged assertions pass on the candidate. The raw transfer retains oversized string/tag capacities and transient frame/vector credit, preserves logical queue/group limits and does not acquire a second production conversion reservation.

Native scratch now charges fixed metadata/global callback buffers and each actual callback-owner allocation, including repeated bindings; source payload is not charged again. Regular credit survives cancellation joining and private-session teardown. Dedicated maintenance credit remains separate. The parent non-native audit found no blocker in its scope. Independent final review found native owner/frame credit-release ordering defects not covered by those end-state checks (`raw-pressure/review-independent.md`). The later `raw-safety/` checkpoint repaired them, passed 282 tests plus both strict Clippy modes and formatting, and closed independent review with no findings in scope. Root integrated the ten owned paths and verified all 128 source hashes. The qualified repair was subsequently deployed with all 128 source hashes matched to the runtime manifest; both short smokes and complete row/view comparisons passed (`raw-safety-runtime/`). Full-load qualification remains open; see `raw-safety/review.md` for the source-review scope. Follow-up status/failure-boundary work has a separate source/evidence directory and does not overwrite this checkpoint.

`hot-reuse/` records a successful isolated pinned-C connection/authority diagnostic, not production pooling or Varve performance.

## Open gates

Ownership-backed raw allocation leases, private append preparation, owned prepared/durable handoff and cleanup safety have source-qualified evidence. The reviewed safety repair is deployed; `raw-safety-runtime/` records matched-limit short smokes and complete row/view conservation, including the previous dataset after upgrade. Batched initial ingestion won that short comparison; single-row throughput and SQL query latency still trail Timescale. Independent partition ownership, production native-session reuse, sustained mixed/maintenance capacity and coherent final production qualification remain open.