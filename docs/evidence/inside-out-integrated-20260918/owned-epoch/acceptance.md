# Owned epoch acceptance ledger

Root verified; AGENTS.md, ARCHITECTURE.md and ACCEPTANCE.md read. Frozen private-overlay manifest: 654c5e7a114a9ffbab4bfda1e1bbd3e0a13c13a8523f64055bbc1760514fad3f. Only authorized sandbox 1dea8336-2d0c-4de4-bcf9-f1dfd63ef2f3; source-only owned-epoch and separate evidence. Frozen remote/local reviewer sources untouched.

## Design / authority trace
- Replace only Inner.commit with an owned standard-library gate; every existing lock_commit call retains the same exclusion domain and lock order (maintenance/remote -> commit -> state -> disk).
- Append/create table: engine. Control definitions, jobs, policies, root/control replay: control.rs lock_commit sites. Checkpoint exact/frozen publication and failure cleanup: engine.rs. Retention, archive, maintenance floors and GC: policy.rs. Compaction, remote publication/vacuum and archive transitions: tier.rs. Startup mutations precede sharing. Reader cache/pin changes may run under State only; PendingAppend never replaces those fields.
- PreparedEpoch owns original inputs, outcomes, encoded frame, private overlay/reservations and lease. No borrowed state/guard crosses threads. Static PreparedWrite is only validation.
- PreparedEpoch -> DurableEpoch -> results consume ownership. Authority armed before possible I/O, cleared only for proven pre-I/O rejection or completed atomic install. Normal durable-token drop and panic fence without locking State.
- Stage indices 0 validation, 1 real private preparation/grouping, 2 sync+install, 3 completion. Ring progress is contiguous terminal-slot progress including rejected, duplicate and no-WAL slots, not WAL maxima. No separate public D frontier. Typestate and pre-install hook distinguish D from V internally.
- One head owns epoch; members own no raw data. Stage 1 awaits publication through a fence-aware condition so a retained abandoned head cannot deadlock cleanup. All consumer credits retained through completion.
- Preparation remains globally serialized. Concurrent partition owners, dependency stamps and cross-owner prepare/commit coordination remain future work.

## Checks (final formatted-source qualification)
- [x] Send+'static prepared epoch moved across threads; committed raw/views/receipts unchanged while held.
- [x] Sync blocked: no ack/visibility, real prepared stage progressed, credit retained.
- [x] D/V barrier and atomic multi-table/partition visibility + exact replay.
- [x] Durable drop, failure, caught panic fencing; safe pre-I/O drop; all authority mutators blocked.
- [x] Existing private overlay semantics, floors, accepted ordinal order, independent/same-epoch duplicates.
- [x] Ring terminal progress; group bounds/order; drain, full/closed/cancel/drop receiver and worker panic cleanup.
- [x] Final-source engine/raw/flow/ingest/boundary/native targeted suites; both Clippy modes; fmt.
- [x] Manifest comparison: only owned sources changed, raw_memory/native/journal framing/config preserved, frozen base verified.

## Final evidence
- engine-final.log: 98 tests (original 92 engine/raw/boundary + six owned-epoch tests).
- flow-final.log: 12 tests; ingest-unit-final.log: 6 tests; native-final.log: 12 tests.
- integrations-final.log: 119 top-level tests: audit_regressions 10, engine 14, group_commit 13, headroom_recovery 4, prefix_admission 13, prefix_checkpoint 13, prepared_publication 5, publication 13, rebuilt_engine 3, journal_engine 9, ingest 16, ingest_flush 3, ingest_traces 3. Child-process executions are separate, not added to this count. Exact commands in final.sh.
- clippy-default-final.log and clippy-fault-final.log: --locked --all-targets -- -D warnings, respective default/fault-injection modes. fmt-final.log: cargo fmt --all -- --check. final-status.txt: FINAL_MATRIX_PASS.
- Reviewer early-failure fix: terminal revalidation is restricted to existing durable receipts when INITIAL pressure fails before overlay acceptance; pending hypothetical clocks have no authority. Digest, own-clock and committed floors remain checked. Root/locked-preparation failure tests cover inline/paged + frozen/synchronous, valid seed floor170, conflicts, expired IDs and uncommitted IDs. reviewer-red.log removes only this repair in the mutable candidate and reproduces the expired-seed failure; source was restored before final tests.
- Adjacent successful-root defect was reproduced in successful-checkpoint-red.log before repair. Checkpoint ceilings now protect independently valid durable receipts for later ordered rechecking, never grant unconditional success. successful-checkpoint-green.log and final engine run cover valid/expired/digest-conflict/legitimate prior-clock-expiry cases in all four root modes, then exact replay and expired-ID rejection.
- First qualification matrix stopped at default Clippy: try_lock existed only for fault-mode test callers. Its cfg now matches those callers. Previous matrix preserved in pre-ceiling/; it does not qualify final ceiling-fix bytes. Earlier failed probe logs preserved: initial probe incorrectly expected live raw reservations to vanish and used obsolete staging for locked checkpoints; corrected tests assert reserved == live after install and inject actual locked preparation failure without weakening durability assertions.
- All formatting performed remotely with rustfmt --edition 2024 --config skip_children=true, only owned paths synchronized. source-files.sha256 binds final source; owned-files.sha256 binds changed/new files; base-differences.txt mechanically excludes non-owned edits. Frozen manifest checked before and after.
- Manual review: consuming typestate and lease drop order, armed durable-drop fencing without State, private inputs/reservations, atomic BatchDelivery dependency advance, fence-aware stage-1 wait, no-WAL/error slot progress, direct/group shared path, and all existing structural publication gate callers. Contour/Fovea extension actions were unavailable in registered tool discovery; no automated structural certification is claimed.

## Final provenance
- Final source-files.sha256 digest: 86c317d4e103be5f69e55de0852885ecd9ae4f6be5d064a00c0c45d7db538c5d (125 paths).
- owned-files.sha256 digest: ee03030880c901b5d95982732f12613aea572a9749494e0b76b111a2ba7b58b4 (nine paths: seven modifications, two new internal files).
- Remote frozen base verified in full (123 paths). Local reviewer copy is intentionally sparse: all 103 present manifest-covered files match; 20 absent SDK/example/.cargo paths listed in frozen-local-omitted.txt. The initial whole-manifest check's missing-file diagnostics are preserved, not treated as content changes. No frozen path was written.
- Local owned files match final remote hashes (local-owned-verification.log). append_overlay, raw_memory, journal and journal tests, native FFI/scanner/startup and all non-owned manifest entries are unchanged. No journal-source edit or claim of rerunning the 47 journal unit tests: journal_engine's nine regressions and new owned handoff tests exercise both backends.
- Only authorized sandbox used. No services, deployments, resources, credentials or S3 operations. Final commands finished; shared Cargo target no longer in use by this implementation.

Known next qualification concern is unchanged: default raw owned 128 MiB can explicitly reject submit_wait before queue admission; 16 MiB output scan reserves 8x + 256 > 128 MiB. No quota loosening or production/distributed claim.
