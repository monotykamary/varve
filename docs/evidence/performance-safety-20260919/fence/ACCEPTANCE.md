# Baseline remote-fence safety backport acceptance

**SOURCE-ONLY CHECKPOINT — UNTESTED / UNQUALIFIED. Remote execution and independent review BLOCKED / PENDING.** No builds, tests, rustfmt, Cargo, fixtures, repository source execution, cloud/resources, delegation, commits or Root edits were performed. Only source inspection, isolated edits/copying and static hash/diff/text checks occurred. No test results or passing-test counts are claimed.

Source13 was the deployed qualified baseline, but this asynchronous remote-fence schedule was not qualified. This backport is separate from partition b73 (whose conservative admission compatibility remains open) and WALv2.

## Identity and scope

Baseline: exactly128 paths from `raw-safety-repair-evidence/source-files.sha256`, digest `13e9198e00e6d11a26c89def8fd775a2a326236f7bd844cfdef288883e4441c0`.

Fresh source: `.tools/rebuild-20260918/baseline-fence-repair`. Evidence: sibling `baseline-fence-repair-evidence`. Both destinations were required absent. The exact128 copy was verified before edits; the final source set has no extra files or symlinks/artifacts.

Only `src/commit_boundary.rs`, `src/owned_epoch_tests.rs`, and expressly authorized test instrumentation in `src/publication_gate.rs` differ. Other125 paths are byte-identical to source13. No engine/model/catalog Arc/owner/quota, journal/native/raw_memory, Cargo/profile or Root/control publication changes. Exact checkpoint hashes are in HANDOFF.md and checkpoint.sha256.

## Acceptance ledger

| Check | Source/evidence | Status |
| --- | --- | --- |
| Exact Git top and mandatory contributor/architecture/acceptance docs | First command verified `/Users/monotykamary/VCS/working-remote/open-source/varve`; docs read | verified before edits |
| Baseline hash13, refuse existing destinations, exact128 copy | baseline-source128.sha256, copy-before-check.log, actual/expected-files.txt | static verified |
| Some(publication) pre-arm health; State dropped before arm/I/O; fail(false) | commit_boundary.rs:58-70; and_then closure owns/drops guard | source inspected; not executed |
| Install health under State before publication.take(); drop State before fail(true) | commit_boundary.rs:138-145 | source inspected; not executed |
| Unchanged failure demotion/floors, field/frame-before-credit destruction, no rollback/truncation | static-delta-review.txt; exact baseline remainder retained | text verified; behavior untested |
| Real competing FileStore CAS through tier, gated after ship capture and before epoch lease | FenceRaceStore / pause_fence_ship / observe_remote_fence | regression source added; not run |
| Before-sync fence: identical WAL/journal names and bytes, append count, no new receipt/sequence | Split-stage regression, legacy and segmented | regression source added; not run |
| After-sync/pre-install fence: durable bytes retained, no new visible install/ACK, new/same-epoch errors, gate poison | Split-stage regression, both modes; full committed snapshot, generation/accounting and receipt assertions | regression source added; not run |
| Independent old receipt and timed floor preserved, same-epoch clock demoted | Old retry270/new200/same-epoch retry290; terminal floor170 not190; exact old receipt entry retained | regression source added; not run |
| Exact all-duplicate/no-publication behavior after observed fence | Dedicated duplicate regression, both modes | regression source added; not run |
| Sender/drain termination without owner runtime | Existing EpochBeforeInstall hook, normal release, real CAS fence; both modes; errors, barrier counts, reclaimed ring/credit | regression source added; not run |
| Deterministic fence observation; no sleeps/polling or added synthetic State fence | cfg(all(test, feature = "fault-injection")) waiter hook only; no production fence() port | text inspected; not executed |
| Existing module registration and inherited tests unchanged | symbols-and-scope.log, static-delta-review.txt | static verified |
| Full128/owned3 manifests and three-file patch | source/owned-after-check.log, changed-files.txt, checkpoint.sha256 | static verified |
| Original repair/review + live-root128 remain hash13; partition reference remains b73 | *-after-check.log; root-status before/after equal | static verified; no reference/frozen edits |
| Compile, targeted tests, default/fault Clippy, fmt and independent review | Unexecuted remote plan in HANDOFF.md | BLOCKED pending permission and review |

No additional source-level correctness blocker was identified in limited static inspection. Rust type checking, formatting, behavior and liveness remain unverified. Do not integrate/deploy on this evidence alone. See REVIEW.md for the asynchronous observation limit and preserved old-receipt semantics. No distributed or production guarantee is claimed.
