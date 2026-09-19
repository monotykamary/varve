# Hot-path structural rework

**Historical database-free gate.** The subsequent [runtime and Railway qualification](HOT_PATH_RUNTIME.md) ran on newer, source-bound bytes on 2026-09-17: 268 selected Rust tests, three short comparisons, one sustained comparison and verified process recovery. Its evidence supersedes the pending-runtime status below, not this original gate’s provenance. There is still no overall Timescale win.

This checkpoint implements larger architectural improvements validated with small local probes, **without running databases**. No Varve/DuckDB/Postgres instance, server, container, Railway operation or load benchmark was run for this gate. Existing dirty-tree work and immutable benchmark archives are preserved. Historical qualification applies to historical bytes, not this rewrite.

## Selected architecture

1. **One owned-input / commit pipeline.** Private `AdmittedWrite` moves static validation and its conservative charge through queue admission and batching. `PreparedWrite` computes canonical row identity with a streaming hasher and at most 8 KiB of scratch space, rather than materializing a full JSON vector. Buffering coalesces Serde's small tokens instead of issuing one hash update per token. Live replanning borrows the immutable proof; untrusted WAL recovery independently validates digest and static properties. Direct and grouped writes share staging, bounded undo, publication and generation advancement. Direct writes still emit legacy `Append` frames and use exact hot-row admission outside the conservative group envelope. Inputs remain owned across checkpoint retries; borrowed typed WAL views replace move-out/reconstruct logic.
2. **Durable dependency batches.** Raw/derived preparation owns pins until file barriers and coalesced directory barriers complete. Only consuming a successful batch produces `DurableDependencies`, required by prepared roots. A raw-GC registration barrier protects off-lock writers. Existing objects are verified/re-synced so a failed or concurrent prior publisher cannot supply a false durability assumption. There is no WAL format migration or weakened WAL/root fsync.
3. **Typed retained query input / relation deltas.** Exact current scope/content determines reuse, including float bits and catalog changes, NOT state generation alone. Job-runtime, remote-status and fence changes outside that generation remain visible. Immutable schema/relation descriptors avoid rebuilding unchanged schema and serializing unchanged payloads; only changed relations enter the install plan. Request-wide borrowed preflight admits scope, metadata, descriptors and relation-map identities before payload cloning. Interned descriptors remain charged. Indexed identity checks and relation lookups avoid quadratic scans. Existing coherent snapshot capture remains authoritative; there is no SQL result cache or benchmark dispatch.

## What the local probes establish

- The original actual WAL path published three one-row groups using six sync operations. Committed checksum/truncation/gap and ambiguous post-rename failure behavior were exercised without linking an engine. Discarding a damaged append-log tail cannot distinguish an interrupted append from corrupt acknowledged data; a segmented-WAL migration was therefore not selected.
- New preparation performs one admission traversal and one canonical row-identity pass. Three live replans perform zero additional identity passes. Recovery independently revalidates. A 512-row witness confirms exact canonical bytes/digest and coalesced hash updates. These are work-removal counts, not measured throughput.
- Borrowed direct/group frames are byte-identical to old owned-record encoding, including group proof and absent clocks. Pure live/replay equivalence covers rows, ordered aggregates, receipts, accounting, rollback, signed zero and forged inner digests.
- A paged-v2, 50-series/64-KiB control-root witness preserves direct admission while retaining the group-only undo ceiling. Both modes require undo to fit the actual held derived working reservation; dropping it releases that charge.
- Three new dependencies in one directory require three file barriers plus one directory barrier: four instead of six dependency barriers. Root publication retains its original two barriers. WAL sync count is unchanged. Reuse explicitly establishes durability and can add work. Batches are per derived-root preparation or partition-writing invocation, not globally coalesced across all tables.
- The original tiny unchanged query reconstructed two batch keys, 1,454 bytes of setup SQL and 390 identical dynamic bytes. New pure tests show zero schema/payload rebuilds on unchanged content, raw append planning and relation-specific changes/deletes. Exact current-content comparisons still scan rows.
- Small-limit actual-constructor tests reject oversized relations, large text and combined individually fitting relations before payload cloning. Empty relation identities and interned descriptors consume the retained budget. A 1,024-batch/131-relation witness preserves an empty warm install plan, zero payload clones and early duplicate rejection. Indexed complexity is a source-structure claim, not a timing measurement.

## Acceptance ledger

| ID | Check | Evidence/state |
| --- | --- | --- |
| H1 | Trace actual write, WAL/dependency and query-input paths | Complete; production entry points and module/runner registrations checked |
| H2 | Bounded deterministic probes, no database instance/process | 35 integrated offline tests pass |
| H3 | Representation/boundary change, no queue/tier/resource tuning | Shared writer, durable dependency guard, typed query descriptors implemented |
| H4 | Atomicity/order/receipts/exact floats/replay/local fsync/immutable remote frames | Pure-state/input/codec and low-level fault checks pass; full engine/process tests not run |
| H5 | Preserve hot/cache serving; no eager checkpoint eviction or result cache | Serving/partition/retention policy unchanged; dependency protection retained |
| H6 | Compile consumers and exercise behavior | Workspace/all-target Clippy with `-D warnings` passes in default and all-feature modes; formatting/diff checks pass |
| H7 | Independent coherent-patch review | Write/publication, query admission and final indexed-preflight reviews report no remaining scoped blocker; discovered admission regressions fixed |

Final offline suites:

| Prefix | Passed |
| --- | ---: |
| `engine::hot_path_tests` | 5 |
| `engine::write_input::tests` | 4 |
| `wal::tests` | 5 |
| `wal::dependency_publication::tests` | 8 |
| `query::workers::resident::descriptor_delta_tests` | 13 |
| **Total** | **35** |

Counts overlap original probes and must not be added as independent coverage. The final receipt binds unchanged pre/post `src/`, Cargo files and runner inputs to SHA-256 `3e940b64095578302074adb8472c6e333d2baa2c6d8ddbd5949d7444861b3869`. Documentation is outside that digest.

Contour's broader working-tree report includes earlier archived benchmark work and unsupported-language gaps; its JS/TS advisory metrics do not certify this Rust patch. Independent source reviews and executable probes supply the scoped evidence.

## Repeatable loop

```sh
cd varve
node scripts/verify-hot-path.mjs
```

The runner compiles the library test binary once, lists actual tests, and executes only the five explicit database-free prefixes. It refuses empty/incomplete selections and source drift, and emits a source-bound receipt plus logs in a temporary evidence directory. Requires Node and cached Cargo dependencies (`--offline --locked`); it does not download dependencies. This is a narrow test-selection contract, not an execution sandbox. The tests exercise domain state, serialization, planning and low-level temporary-file publication, not a running database.

## Remaining limits

WAL fsync and state-held publication remain; stored-row conversion still clones and aggregate planning still performs work. Engine snapshot capture and physical-file verification remain. Budgets are logical, not hard process-RSS quotas. Pure-state and filesystem-boundary tests do not replace real engine concurrency, crash/restart, query execution, restore or performance qualification. No Timescale win, p95 improvement, power-loss simulation or production/distributed guarantee follows. Run those integration gates only under a later explicit database-execution scope. This checkpoint was not deployed or published.

Private provenance: `/tmp/varve-hotpath-main/before` preserves 104 pre-rework source/test/document files. Investigation and handoffs live under `/tmp/varve-hotpath-wal` and `/tmp/varve-hotpath-query`; independent reviews under `/tmp/varve-hotpath-review-write` and `/tmp/varve-hotpath-review-query`. Final gate logs are under `/tmp/varve-hotpath-main`; the final runner receipt is in `/var/folders/hc/3k4lvfw90yv7kghsnynghcvm0000gn/T/varve-hot-path-HdZSYk`. Old S14 pilot controls and authorizations must not be used for these new source bytes.
