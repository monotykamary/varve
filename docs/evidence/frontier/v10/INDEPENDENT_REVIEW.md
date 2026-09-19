# Independent read-only S12 correctness review

## Result and scope

**No critical/high-severity finding or demonstrated S12 result/durability defect found in this bounded review. One low-severity documentation/contract ambiguity is reproduced below.** This is not full qualification or a security/performance certificate.

First command verified exact git root `/Users/monotykamary/VCS/working-remote/open-source/varve`. Read AGENTS, ARCHITECTURE, ACCEPTANCE and the four supplied contract/handoff documents. Reviewed integrated engine/tier/service, not the owners' obsolete “integration pending” status. No agents, network/HTTP tests, Railway, load, performance run, Cargo build, source/test/doc edit or commit. Only this report is retained; the tiny private probe/fixtures were temporary.

Compared S12 with `docs/evidence/frontier/v9/local-qualified-source.tar.gz`, NOT HEAD's accumulated v6 diff. All **83 archive inputs matched** `v9/local-qualification.json` hashes. Of those inputs, precisely nine differ now: engine, metrics, plan, resident, resident_tests, workers, service, tier and tests/service. Two additional Rust tests are new. Other archived inputs, including query.rs, WAL, model, derived/root, segment, Cargo files and configuration, are unchanged. Reviewed documentation separately; it is not all included in that source archive.

## Finding F1 — low: disambiguate planner fallback from worker-fresh-only execution

Witnesses: `src/engine.rs:1740-1750`, `src/plan.rs:69-99`, `src/query.rs:1118-1153,1248-1255`, `src/query/workers.rs:257-282,325-326,392`; wording at `docs/QUERY_WORKERS.md:62-76`, `docs/QUERY.md:29`, `docs/ARCHITECTURE.md:48`, `docs/METRICS.md:36`, and CONTRACT's “every ... fresh fallback”.

`retained && nonempty ScanPlan.table` does **not** establish pooling eligibility. `sqrt` is absent from the reusable-function allowlist, but the planner accepts it over a single storage table. The runtime constructs a resident request before eligibility selection; fresh-only execution still installs that request into a newly spawned, subsequently discarded child. Thus a fresh-only query can receive schema-only catalogs.

Deterministic reproducer: private database, `query_retained_inputs=true`, `query_workers=1`, pressure-only flush; create default `metrics`; write one row `(timestamp_us=0, tenant=t, series=s, value=4)` with `now_us=0`; execute `SELECT sqrt(value) AS v FROM metrics` twice. The independent probe observed:

```text
result each time: [{"v":2.0}] (equal to query_retained_inputs=false)
spawned=2 reused=0 discarded=2 idle=0
resident_full_loads=2 resident_dynamic_loads=0 resident_dynamic_staged_bytes=0
```

This refutes an unconditional “all fresh-only/unsupported queries get full catalog rows” interpretation, **not** the narrower no-catalog-access claim. `sqrt` cannot inspect the omitted rows. No actual catalog-reachability/result counterexample was found. Dynamic SQL reading `varve_status()` and a scalar catalog subquery both retained current rows in the probe; both matched the non-retained oracle (`sequence=2`). A nested storage-only `sqrt` query also matched.

Recommended owner action: qualify “unsupported/fallback” as **no positive storage planner proof**, distinguish non-retained/standalone from pooling-ineligible fresh children, and explicitly say that the latter may omit proven-unreachable catalog rows. Do not describe the storage-source flag as a pooling/effect/security proof. No foreign-code repair was made or is implied by this finding.

## Acceptance ledger — source witnesses and bounded conclusions

| Gate | Review evidence / result |
| --- | --- |
| Q1 positive no-catalog-access proof | `plan.rs:29-95,242-312,365-435`: exactly one parsed query statement; single plain source or derived-table chain; no joins/CTEs; nested query count must equal chain length; unresolved/dynamic sources fail closed; exactly one raw/rollup/alias candidate. Engine uses only the returned plan, not arbitrary public-field construction. No reachable-catalog counterexample found; not an exhaustive SQL effect proof. |
| Q2 shape and aliases | `engine.rs:1852-1862,1950-2039`: SchemaOnly does not advance row-producing iterators; exact five catalog schemas remain. Alias source/name/width selection is unchanged. `query.rs:940-956` generates aliases only over the source rollup relation with exact width, not metadata. |
| Q3 coherent fallback | `engine.rs:1590-1601,1736-1756`: planning uses shape under State; final Full materializes current rows under the same lock when no storage proof or retained mode is disabled. Scalar/storage-free sentinel is empty-table, hence not positive. Worker-fresh distinction is F1. |
| Q4 raw hit / dynamic no-op | `resident.rs:185-204,257-264,298-331`: canonical vector equality remains exact; unchanged dynamic bytes do not stage/install again. `resident_hits` remains `!full && additions == 0`, independent of dynamic replacement. New tests assert disjoint appends preserve raw/dynamic counts. |
| Q5 rollup/exact data | `tests/query_catalog_exposure.rs:251-328`: relevant rollup refresh, irrelevant tenant stability, standalone fresh oracle and signed-zero first/last bits. `query.rs:557-626` serialization is byte-identical to base. New counters do not change payloads. |
| Q6 scope / limits / cleanup | `resident.rs:21-147,155-163,298-319`; `workers.rs:242-326,367-393`: existing namespace/sequence/schema/weak-identity compatibility, 128-MiB accounting, deadlines, acknowledgment/cleanup/reset and disposal retained. `query.rs` unchanged against archive; no sandbox, path, authorization or limit changes in S12. This is delta review, not renewed exhaustive security qualification. |
| Q7 fresh equivalence | Inspected the two new catalog tests and their raw/rollup standalone oracles. Independently ran 15 planner cases and five retained/non-retained query pairs in private one-row fixtures; all assertions passed. |
| O1 measured disk scopes | `metrics.rs:187-204,239-251`; `engine.rs:2651-2655`; `tier.rs:563`: wait ends after acquisition, hold starts then, `_hold` drops before `_guard`; poison remains Err with owned guard and remains poisoned. All 14 production helper call sites route through this wrapper; no direct acquisition bypass remains. Existing frozen-prefix drop before reacquisition stays at `engine.rs:4485-4487`. |
| O2 WAL attribution | `engine.rs:3274-3279` ends WAL-specific wait before budget scan/append; `wal.rs:381` starts WalWrite inside append_encoded. No hold-by-subtraction. WAL encoding/fsync/order and failure fencing unchanged. |
| O3 grouped planning | All three locked checkpoints, three prepared checkpoints and three State releases traced individually below. Every explicit rollback/checkpoint/publication transition closes the timer first. Retry revalidation is a new interval; clocks/floors/undo statements unchanged. |
| O4 dependency paths | Both derived emit callbacks (`engine.rs:3687-3710,3954-3974`) distinguish bounded verification from new atomic publication. Raw output (`4835-4870`) does likewise and retains the disk guard through descriptor/copy/result construction. No corruption healing, budget bypass, changed pin order or altered output bytes. |
| O5 errors/drop/isolation | Inspected `metrics.rs:420-563` poison, early-error, unwind and barrier tests; `engine_metrics_tests.rs:52-334,348-470` actual-path counts, pressure/hook boundaries and corrupt reuse rejection. No elapsed-time thresholds; no softened durability/corruption assertion. Tests' existing passes are owner/Main evidence, not reruns here. |
| O6 public registration/export/docs | Mechanically confirmed 30 enum/ALL/as_str entries in identical order, unchanged first 22 and eight appended names. Module registered at `engine.rs:5698-5700`. `workers.rs:147-168,348-355` propagates both counters with saturating addition. `service.rs:786-788,1045-1046,1095-1096` exposes both alongside all phases; no dependency/config knob added. Documentation is accurate about overlapping attempted wall time, subject to F1. |

## GroupPrepare path audit

Line numbers below are current `src/engine.rs`. Entry is after State acquisition at 1028; RAII closes early errors before State drops.

| Transition | Stop / excluded work / restart |
| --- | --- |
| Initial pressure, frozen | stop 1084; State drop 1089, prepared checkpoint 1090, hook/reacquire/validation excluded; restart 1105 |
| Initial pressure, locked | stop 1084; checkpoint_locked 1103 excluded; restart 1105 |
| Metadata headroom, frozen | stop 1197 before undo 1200/floor restore; State drop 1206, prepared checkpoint 1207 excluded; continue restarts at 1113 |
| Metadata headroom, locked | same stop/undo; checkpoint_locked 1220 excluded; continue restarts at 1113 |
| Encoded remaining-WAL pressure, locked | stop 1271 before undo 1273/floors/checkpoint_locked 1276; restart 1277 before revalidation/staging |
| Encoded remaining-WAL pressure, frozen sentinel | publication closure returns OffLockCheckpoint; unconditional stop 1343 before undo 1346/input recovery; State drop 1367 and prepared checkpoint 1368 excluded; restart at 1113 after reacquisition |
| Successful publication | stop 1331 before GroupBeforePublish hook and publish_record 1335; disk wait, admission, filesystem publication and post-publication updates excluded |
| Other publication/encoding/admission error | unconditional stop 1343 before rollback/floor restoration; already-stopped success is not counted twice |
| All duplicate/invalid/no-new-items | stop 1391 before duplicate-floor commit; outer error-floor handling stays outside |

Thus counts denote completed **planning intervals**, not groups/requests; failed planning counts; canonical WalEncode is nested. Initial-pressure paths can have two intervals. This excludes retry *undo/checkpoint/off-State time*, not resumed retry planning itself. Integration-only diff versus `engine-before-integration.rs` contained instrumentation/import/helper/module registration, not reordered storage/control statements.

## Derived/raw timers, counters and public tests

- Verify spans the existing bounded read plus integrity/collision check, including failure. Publish starts only after budget admission and spans `wal::atomic_write`, including its existing temp cleanup on error and file-sync/rename/directory-sync path (`wal.rs:336-363`). Both derived publication timers end before `derived_page_published`; post-guard hooks remain outside. Encoding and path-existence tests are not redefined as verification/publication.
- Raw timers apply only to output dependencies, not query/recovery/cold-cache reads. Raw `_disk` is not shortened. The new counts are attempted operations, neither certified bytes nor successful reuse counts.
- Dynamic counters are returned only after adapter acknowledgment, private staging close and deadline check (`resident.rs:302-331`); later SQL/reset failure does not erase the accepted install. Changed-to-empty counts one load and zero bytes. Full empty initial setup does not count a dynamic change. Both pool totals saturate (`workers.rs:348-355`), with public stats preserving them via `..pool.resident`.
- `tests/service.rs:281-380` asserts exported zero dynamic work for raw proof, then positive loads/bytes and current sequence for metadata. `383-431` asserts positive admission/group counts and presence of four dependency labels. `metric_count`/`phase_count` (`173-189`) fail on missing labels, not silently defaulting to zero. Dependency execution, not merely registration, is separately covered by engine tests.

## Independently executed checks and qualification limits

- Archive: 83/83 SHA256 matches; current 83-input hash manifest rechecked unchanged at end. New test hashes also unchanged. Scoped `git diff --check` passed.
- Private Rust probe: compiled exact current `src/plan.rs` and linked existing `libvarve-a62bcdd46b1a7d55.rlib`, using `rustc --edition=2024 -C opt-level=0 -C debuginfo=0 -L dependency=target/debug/deps` and existing sqlparser rlib; no Cargo/dependency/DuckDB build. Actual `.tools/duckdb` was used by the adapter, with private databases and explicit clock zero.
- Planner matrix: 4 positives (raw sqrt, nested sqrt, aggregate alias, quoted table) and 11 negatives (scalar, metadata macro, scalar/order/EXISTS subqueries, mixed join, CTE, query_table, query, internal input, alias-as-function). Five paired engine results passed, including dynamic/scalar metadata fallback. The linked library is an existing artifact, not a new source-qualified full build.
- Library artifact SHA256: `9dfe2f3e0285b60b316329473a43ddafed30b0c8923a10b4961862dcbb65e825`; temporary probe source SHA256: `d85c32cf80bcf8d03a5b30c657f68deb735693431699e8604280cbd494b796d7`.
- Did **not** rerun unchanged passing suites. User/Main reports metrics 9, engine 6 default/7 fault-feature and two HTTP targets passing; Sol reports planner 20, resident 11, catalog 2 plus query/security gates. Those results are supplied evidence, not independent executions in this review.
- Coverage gaps: no exhaustive DuckDB grammar/builtin proof; no new fault-injection/crash/corruption matrix, remote/tier integration execution, full strict lint/client qualification or all-feature rebuild. Rare metadata-headroom and remaining-WAL timer transitions were source-traced, not independently forced here. Frozen-callback corrupt reuse was source-reviewed; the new corruption fixture directly exercises ordinary prepared-root reuse. Saturation of the new worker totals was checked in source, not forced to u64::MAX at runtime.
- Fovea sketch and Contour extension actions were unavailable through Fabric discovery/call. Grep supplied some Fovea navigation only. Contour v0.1 does not model Rust in any event; neither a clean structural score nor a Rust certificate is claimed.
- **Qualification stage: integrated S12, bounded independent source review + tiny correctness probe; full post-integration source-bound qualification remains Main's gate.** The archived 384-Rust/19-TS receipt qualifies only v9's old digest, not these edits. No local or remote performance conclusion; v9 negative evidence is unchanged.

## Source witnesses (SHA256)

Base archive: `ece31c43a4bafa44803e34358d6315e6931c166fb0d636b8bb6bbf8b74aebb57`.
Base qualification source digest: `9ed8187afb8b8f9846ee62e80a8ddf32b442c6ca188d1dd330d07728018dca81`.
Review's current-83 manifest digest: `08ab38add9723dcea9e7d110d9d739b66e1212278317ff83b9e8ad0f60ebfc19` (SHA256 of `sha256sum`-style lines in receipt file order; different definition, **not** a qualification digest).

| Path | Reviewed SHA256 |
| --- | --- |
| src/engine.rs | 794cd89a0ab0fef36cc48d098cf4315d7ada5642bd92e1c46d59f81ec698593d |
| src/plan.rs | e692905a371e75ce057a9a38fe7c54f14faefe94562847ab9f84a82eb7293eb5 |
| src/metrics.rs | 88e4b3180fe490c755b1d891d200f7b4c2e6dce4ed0adcb4a4f2ea5d0d98db2d |
| src/query.rs (unchanged) | 46a4885722eec69f2840b76165d782c5f953890aefa2de4a935555b94c754d4a |
| src/query/resident.rs | 7531897b404682f722c645c3a965de5767ba9bcdb7d0c8cb6899427585e6bd44 |
| src/query/workers.rs | c6318a8caf2db765bcad8380fc3c76774f2025257d3df8af3417bfa717e7e542 |
| src/query/resident_tests.rs | 06778af5866b101ba13c89daf1ff06525ab1c2303f5443d562c1bd2c19a95ae1 |
| src/tier.rs | 10ce07836c5d012e3a8393fe7b25ffc331a9e7955df23dbc87a614023311a0bc |
| src/service.rs | 1558f835bf60b966551178122f43353b4d6e1858c8d0d572fa2f53f1d990fb13 |
| tests/service.rs | e0da4a335a345ce173d24abc555acd73a2e1031bfbf43584f6386699cc23028d |
| src/engine_metrics_tests.rs (new) | 9451f203fab16bc3743bd0957bab703440519c6fff3e20b594b247d0ba628dd5 |
| tests/query_catalog_exposure.rs (new) | f31bdb56bf76c338aa06a0109ece000a882fb40e8e65339958c237eda1057c34 |
| docs/METRICS.md | a96d9c929f2e8bdc878d79863d1e69e9f44687e098063fd621a0e8a2d025d0da |
| docs/QUERY.md | d1d1c5080d4f80d9829a80d214a51451c8ca9b381939f6eb86210db4fa9a9a4b |
| docs/QUERY_WORKERS.md | 45d60a409ff38977239b53cbc7c06630ed7d4d92499c7a97d124864a0a141a04 |
| docs/ARCHITECTURE.md | e33e9e2db94f1945f68d40deb94a8148458fe9e16bab132cff4dbce94d9e12bf |

Provided engine-before-integration snapshot: `d48810f77297f864e8a79c8d4f5abb6607fedc09a9f2da34398c0b0bee97a16d`; it already contains Sol's query work and was used only to isolate Main's metrics integration.
