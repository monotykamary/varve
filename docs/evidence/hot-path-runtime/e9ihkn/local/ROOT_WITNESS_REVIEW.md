# Root witness final review

**B1 resolved.** Git root verified exactly as `/Users/monotykamary/VCS/working-remote/open-source/varve`.

## Source witness

Read B1 in `WRITE_FIX_REVIEW.md`, the current `src/engine.rs` test `competing_same_sequence_root_stales_frozen_candidate_without_control_change` (starting at line 5682), and `frozen_prefix_is_current` (4111–4139).

The corrected test checkpoints the seed before advancing the live timed floor via a duplicate at clock 20. It explicitly requires `sequence == checkpoint_sequence` and a due idempotency checkpoint before freezing at `RootPrepare`. The competing `checkpoint_locked` must succeed while preserving the complete tuple `(sequence, checkpoint_sequence, control_epoch, idempotency_floors)` and incrementing `root_epoch` by exactly one. Thus the prior-frontier confound in B1 is removed. With the seed already checkpointed and no new rows/control changes, the hot-prefix/config checks introduce no competing stale cause; the root-epoch comparison at line 4118 is the isolated changing currency stamp.

After release, the frozen worker must return an error containing `stale frozen-prefix`. The test requires checkpoint frontier 2 and one row, then reopens and requires floor `Some(-80)`, the retained seed receipt's duplicate response, and still one row. Both inline and paged roots are covered by `[false, true]`.

## Limits and gate disposition

This is a narrow source-witness review, not a new execution or general engine certification. Read `source-qualified2.json` (recorded engine SHA-256 `3ae158559dbd02902fb97621e2c2a8178a8aa4d3cbd39d716219ef4046c56299`); repository-wide hash comparison and proof that only this cfg(test, fault-injection) function body changed remain Main's mechanical qualification, not independently established here.

Focused inline/paged success, feature all-target Clippy, and the previously green 268-runtime/both-lint/default-2-HTTP/21-driver gates are supplied execution evidence, not rerun here. No unchanged passing tests need rerunning for B1. No builds, tests, source edits, network/cloud operations, or delegation performed; only this requested review artifact was written.
