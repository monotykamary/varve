# Independent write-fix review

## Verdict

**BLOCKED on one test-evidence defect.** I found no demonstrated production-code defect in the scoped `engine.rs` repair, but the newly added private test does not establish the requested root-epoch-only stale-candidate case. This is a concrete coverage blocker, not a request for broader redesign.

No source was edited. No build, test, Clippy, runtime, network, cloud, or delegated action was run. Main retains the final real runtime gate and Clippy run.

## Scope and provenance

- Verified Git root exactly: `/Users/monotykamary/VCS/working-remote/open-source/varve`.
- Read `AGENTS.md`, `docs/ARCHITECTURE.md`, `docs/ACCEPTANCE.md`, `docs/HOT_PATH_REWORK.md`, the parent runtime `ACCEPTANCE.md`, and `write-fix/FIX.md`.
- Compared all three changed files to `write-fix/before/`.
- Current source matches `write-fix/after/` and the entries frozen in `source-qualified.json`:
  - `src/engine.rs`: `0f1fc4b996f184a03fb4001d4ee41cc9baf0099bd6812bc1aa7c2a6f27e1d8d2`
  - `src/append_accounting_tests.rs`: `2d6a665efa0c537c4241be9d6b24084e58f61cb0310724b0067ea505741d46dd`
  - `tests/prefix_checkpoint.rs`: `73c84ee8a95f9a952ecae40c194171e35e3384d2b2f4b557c05c88d66199c544`
- The unchanged witnesses also match the handoff: `src/engine_metrics_tests.rs` `9451f203...` and `src/hot_path_tests.rs` `257c9739...`.

## Blocker

### B1 — The alleged root-only stale test also changes the prior checkpoint frontier

`src/engine.rs:5682-5723` adds `competing_same_sequence_root_stales_frozen_candidate_without_control_change`, but it starts from an uncheckpointed hot write and invokes `checkpoint_locked` as the competing publisher. The test records only `(sequence, control_epoch, idempotency_floors)` at `src/engine.rs:5700-5706`; it does **not** record `s.catalog.checkpoint_sequence`.

That omitted value is an independent frozen-candidate stamp:

- capture stores `prior_checkpoint_sequence: s.catalog.checkpoint_sequence` at `src/engine.rs:4018-4022`;
- currency rejects a mismatch before/alongside the root epoch at `src/engine.rs:4116-4121`;
- the competing `checkpoint_locked` unconditionally sets `next.checkpoint_sequence = s.sequence` for this non-no-op checkpoint at `src/engine.rs:4757-4789`.

Therefore the test's stale error is explained by the changed prior checkpoint frontier even if the `root_epoch` comparison were removed. It does not isolate or prove the requested same-sequence, same-frontier, root-only stale defense.

A minimal sound witness can remain private and use existing machinery: create a timed table, write and checkpoint it so `sequence == checkpoint_sequence`, advance only the live timed floor with a durable duplicate so a same-frontier floor checkpoint is due, block a frozen candidate at `RootPrepare`, and publish that same-frontier floor root through `checkpoint_locked`. Assert before/after equality of **sequence, checkpoint_sequence, control_epoch, and live idempotency floors**, plus `root_epoch + 1`; then require the blocked candidate's exact stale error and receipt/reopen integrity. In that setup `src/engine.rs:4118` is the only scalar stale predicate that changes.

This is source-demonstrated and does not require a new runtime repro to establish.

## Production-path review

No additional blocker was found in the repaired write path:

- **Typed preflight/finish and cached direct encoding:** `PreparedAppend<Result<DerivedProjection>>::finish` at `src/engine.rs:3080-3099` cleanly defers derived reservation from `preflight_append` at `src/engine.rs:3102`. Direct mode encodes its borrowed `PreparedWrite` once per attempt, stores it at `src/engine.rs:1085`, and consumes that exact frame at `src/engine.rs:1158-1165`. Recovery still enters through `ValidatedAppend::recovered` in `prepare_group_append`; no trusted live proof is reused for WAL replay.
- **Caps and error order:** direct initial pressure uses exact resident bytes while grouped mode retains its conservative admission charge (`src/engine.rs:896-906`). Preflight performs hot/receipt/rollup and projected metadata checks before direct encoding; exact own-frame and remaining-WAL checks occur at `src/engine.rs:1064-1084`; deferred derived reservation follows at `src/engine.rs:1086`. Publication still performs disk admission before WAL append and fences ambiguous append errors in `publish_record`.
- **Timed floor and bounded retry:** direct pressure applies the validated clock before checkpoint capture at `src/engine.rs:943-952`. The newly clock-eligible full-registry branch is direct-only at `src/engine.rs:1025-1037`; metadata-headroom and direct-checkpoint retries converge at `src/engine.rs:1106-1146`. `offlock_checkpoint_used` prevents a second frozen retry, and exact remaining WAL failure after that retry is explicit at `src/engine.rs:1079-1083`.
- **No unacknowledged staged write state:** rows, receipts, rollups, metadata, and derived reservations are installed only after complete preparation. Retry and publication-error paths drain `undo` before checkpoint/return (`src/engine.rs:1113-1121`, `src/engine.rs:1266-1272`). Publication remains under the state lock, and sequence/generation advance only after WAL publication. Direct retry-floor preservation is intentional and scoped; publication failures restore the captured floor baseline.
- **Group defenses and metrics:** grouped conservative pressure remains selected by mode, while the oversized-direct guard no longer suppresses private grouped defensive/no-progress paths (`src/engine.rs:912-939`). The original no-progress tests remain present at `src/engine.rs:5731` and `src/engine.rs:5788`. `GroupPrepare` is now instantiated only for grouped mode at `src/engine.rs:881-883` and on its restart sites, so direct writes do not emit group-only timing.
- **Shared immutable pipeline:** `AdmittedWrite` still owns rows, `PreparedWrite::validated` lends a typed immutable view, retries retain the owner, and direct/group writes continue through the shared apply/undo/publication/install path. No resource setting, durability barrier, or fsync path was relaxed.

## Changed test expectations

Accepted as behaviorally justified:

1. `append_accounting_phase_covers_direct_group_and_open_replay_not_retries` changes direct accounting from two projections to one and cumulative direct-plus-two-grouped projections from four to three. This matches the new preflight/finish split and cached frame; reopen still independently accounts three replayed appends.
2. `direct_reprojects_after_wal_pressure_prunes_at_same_sequence` now exercises inline/paged and synchronous/frozen combinations, requires two state projections across the real checkpoint, and proves only one admission and one canonical identity pass. Its receipt/floor/accounting/reopen assertions remain strict.
3. `direct_new_clock_reclaims_full_receipt_registry_at_same_frontier` covers the new direct-only idempotency-headroom branch in all four root modes and verifies floor, capacity, rejection, duplicate, rows, and reopen.
4. Rewriting the formerly hanging third case in `control_floor_and_competing_root_changes_stale_the_candidate` is sound: explicit frozen checkpoints serialize on the maintenance-preparation gate (`src/engine.rs:4418-4424`), so a direct pressure write cannot be the competing publisher while the old candidate is blocked. The synchronous aggregate-control path legitimately produces sequence 3, after which the tail is sequence 4 and is verified after reopen.
5. `direct_pressure_waits_for_inflight_frozen_prefix_then_reopens_exactly` is a real threaded pressure witness. `performance()` snapshots do not themselves acquire the state mutex, so the observed completed `state_lock_hold` is attributable to the writer releasing state before waiting on the preparation gate. The test also checks noncompletion while blocked, no additional locked checkpoint, zero group timing, exact sequence/frontier, duplicate behavior, rows, and reopen.

Not accepted:

- The new private `competing_same_sequence_root_stales_frozen_candidate_without_control_change` expectation is insufficient for the claimed root-only edge for B1 above.

## Gate disposition

Fix B1's witness, then Main should run its already-owned final real runtime gate and Clippy concurrently against the unchanged source-qualified inputs. The fix-owner's `engine 54 / prefix 12 / admission 13` and targeted Clippy results are handoff claims only; this read-only review did not rerun them.
