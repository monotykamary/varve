# Write runtime regression fix

## Scope and provenance

Exact git root verified: `/Users/monotykamary/VCS/working-remote/open-source/varve`.
Read AGENTS.md, docs/ARCHITECTURE.md, docs/ACCEPTANCE.md, docs/HOT_PATH_REWORK.md and the parent runtime ACCEPTANCE.md. Compared legacy admission against `/tmp/varve-hotpath-main/before/src/engine.rs`.

`before/` preserves exact pre-edit copies of all five owned files. Only `src/engine.rs`, `src/append_accounting_tests.rs`, and `tests/prefix_checkpoint.rs` changed in this task. `src/engine_metrics_tests.rs` and `src/hot_path_tests.rs` remain byte-identical to the task snapshot. `after/`, `SHA256SUMS`, and per-file patches bind the scoped handoff. Existing unrelated dirty work remains untouched.

No cloud/network/load timings, containers, delegation, new Cargo target, resource/profile tuning, commits or unowned formatting. Main retains final integrated gate and review ownership.

## Implementation

- Keep the immutable input and shared staging/undo/publication/install pipeline. Split state-dependent preflight from the derived-reservation finish step using a private typed preparation state. Group/recovery finish immediately; Single first checks its exact borrowed legacy Append frame and remaining WAL capacity. Metadata headroom still precedes WAL failure, and WAL failure precedes deferred derived accounting errors. Normal Single encodes once and projects once. Replay still constructs independently validated input.
- Keep GroupPrepare timing and group-only conservative-pressure behavior in Group mode. The oversized Single hot-input guard no longer disables defensive group pressure/no-progress paths. Group rollback and recovery ceilings are unchanged.
- Direct checkpoint retries preserve their validated timed-ID floor, so same-frontier pruning can publish a new root, then reproject against reclaimed receipts. A related actual-runtime probe exposed that a new direct clock could not reclaim a full receipt registry; that is fixed before preflight, with bounded retry and no immutable-row revalidation. Direct hot-pressure checkpoints also retain the validated input clock. Exact remaining WAL pressure after the retry fails explicitly before publication.
- Frozen Single writes wait for the maintenance-preparation gate; they do not bypass it with an exact locked root. No production concurrency bypass was added.

## Explicit expectation changes

1. `append_accounting_phase_covers_direct_group_and_open_replay_not_retries`: direct count 2 -> 1; durable duplicate remains 1; two further grouped inputs produce cumulative 3 instead of 4. This is truthful removal of redundant projection, not reduced durability. Reopen accounting remains 3.
2. Rename the same-frontier test from `direct_reprojects_after_prepare_record_prunes_at_same_sequence` to `direct_reprojects_after_wal_pressure_prunes_at_same_sequence`: Single no longer calls prepare_record. It still requires two projections across the state-changing checkpoint. Expanded to inline/paged x synchronous/frozen modes and asserts exactly one immutable admission and canonical identity pass across the real retry.
3. Third case of `control_floor_and_competing_root_changes_stale_the_candidate`: the old direct-write-before-release expectation deadlocks under the shared preparation gate. Use the existing aggregate-creation synchronous control checkpoint, detach future hook visits, release the old hook, require the exact stale-frozen-prefix error, then verify tail/receipt/reopen. Additional aggregate WAL operation means tail sequence 4 instead of 3 in that scenario.
4. Add `competing_same_sequence_root_stales_frozen_candidate_without_control_change`: private existing checkpoint_locked path publishes a competing root while sequence/control/floors stay identical. Require root_epoch +1, old candidate staleness, and exact receipt/row recovery in both root modes. This preserves isolated root-staleness coverage rather than relying only on a structural control change.
5. Add `direct_pressure_waits_for_inflight_frozen_prefix_then_reopens_exactly`: real pressure writer runs in another thread. While RootPrepare is blocked, the only writer's completed state-lock-hold count proves pressure admission released state; its completion channel must remain empty. Release the hook, join both, verify sequence 3/frontier 2, rows/duplicate/reopen, zero group timing and zero *additional* locked checkpoints. Watchdog clocks bound deadlocks only; no latency assertion. The locked-checkpoint metric uses a before/after comparison because paged-root setup may already perform one.

No exact cap, corruption, durability, staleness or no-progress assertion was relaxed. Both original no-progress tests and the direct WAL error-precedence test now pass unchanged.

## Acceptance ledger

| Check | Evidence | State |
| --- | --- | --- |
| W1 Shared immutable/borrowed path; independent recovery; no repeated row work | engine hot-path/input/replay tests; real retry counter assertions | pass |
| W2 Direct exact hot/WAL/disk budgets and failure order; group-only metrics/limits | append_accounting and engine_metrics prefixes | pass |
| W3 Same-frontier direct clock pruning and full-registry reclamation | four mode combinations each; exact metadata/derived accounting, reopen, rejection floor | pass |
| W4 Group defensive no-progress and durable duplicate/floor preservation | original prefix_review tests unchanged | pass |
| W5 Frozen root/control/floor staleness and explicit direct gate waiting | private root-only test plus all 12 prefix_checkpoint tests | pass |
| W6 Scoped lint, format and test registration | targeted Clippy -D warnings, rustfmt skip_children check, diff check, binary --list | pass |
| Final integrated gate/review | Main-owned | not claimed |

## Executed evidence

All Cargo commands use `--offline --locked --features fault-injection`, existing target and configured profiles. Tool deadlines bounded every invocation.

- `engine-1.log`: affected `--lib engine::` prefix, 52 passed after initial fixes.
- `prefix-1.log`: prefix_admission 13 passed; then prefix_checkpoint 10 passed and old revised third case still hung. 240-second tool deadline killed the processes; process listing confirmed none remained. This exposed the preparation gate, not merely the shared hook. Superseded by final root/gate tests.
- `reprojects-2.log`, `root-only.log`, `stale-2.log`: individually verified expanded same-frontier, isolated root-only and corrected former-hang tests.
- `receipt-regression.log`: intentionally captured failing new direct-clock/full-registry probe before fixing source (registry-full rejection).
- `engine-2.log`: affected `--lib engine::` prefix, **54 passed**, including original seven failures, added root-only and receipt tests, runtime fault subprocesses, hot-path and independent recovery witnesses.
- `direct-pressure.log`: initial new concurrency test completed but had an incorrect absolute metric assertion for paged setup; fixed to require no additional synchronous checkpoint, not remove the check.
- `reprojects-final.log`: strengthened real retry admission/identity counter test passed on final source.
- `prefix-final.log`: **12 passed**, including original former hang, explicit direct gate waiting, crash/recovery and concurrent prefix cases.
- `admission-final.log`: final-source prefix_admission rerun, **13 passed**, including stale capacity, same-frontier receipt pressure, rollback floors and manifest ambiguity.
- `clippy.log`: targeted `cargo clippy --lib --test prefix_checkpoint -- -D warnings` passed.
- Final owned-file rustfmt check and tracked engine diff whitespace check passed; new tests confirmed in actual built test binary lists.

Fovea/Contour extension actions were unavailable from the current tool registry (lookup yielded no action); scoped source/diff inspection supplies this task's review evidence. No independent/final integrated review is claimed.
