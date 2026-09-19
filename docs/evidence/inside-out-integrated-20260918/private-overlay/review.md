# Frozen private-overlay source review

Read-only review by Sol (`cc3adb99b0b84248afdf6a4bda7dfbdf`); Main witnessed the finding against the same frozen files. No local build/test execution.

## Open finding: early pressure-checkpoint failure skips durable-retry correction

The conservative `resolve_group_durable` prepass precedes the fallible initial pressure checkpoint. Its same-epoch-conflict correction occurs only in the later private overlay loop. On an earlier checkpoint error, fallback can therefore return a false error for an already-durable retry and omit its live clock-floor advance.

Reproduction target: durable `v1:180:seed`; then new `v1:200:a` at clock200, conflicting same-ID data at clock290, and identical durable seed retry at clock270. Force initial hot/WAL pressure and checkpoint failure. The seed retry must remain a successful duplicate and advance its live floor to170, without publishing new raw rows, rollups or receipts. The existing mixed-epoch test injects failure later, at `GroupBeforePublish`, and does not cover this path.

Frozen source locations: `engine.rs:1103,1160-1194,1224-1240,1541-1550,3265-3281`; existing test `commit_boundary_tests.rs:660`. This is an inherited prepass limitation, not identified as a new overlay regression. Repair/regression assigned to the active owned-epoch implementer; not marked fixed here.

## Reviewed properties

The reviewer found no other concrete regression in the six-file overlay delta. Preparation owns touched forward maps, ordered raw batches, scalar projections and reservations without modifying committed state; installation follows successful WAL publication. Same-epoch receipts fail with publication, accepted ordinals preserve request order, and recovery independently validates decoded records. Old undo helpers are test-only. These findings are scoped source review, not production certification.

## Evidence precision

The final manifest digest is `654c5e7a114a9ffbab4bfda1e1bbd3e0a13c13a8523f64055bbc1760514fad3f`. All six reviewed changed-file hashes verified. The initial local review copy lacked20 of123 manifest paths (`.cargo`, Rust client and examples). Main subsequently retrieved the exact source-only manifest set from the frozen Railway directory and mechanically verified **all123 paths**, with zero mismatches, in `.tools/rebuild-20260918/private-overlay-complete-review`. The original review snapshot remains untouched; this completeness repair does not extend the semantic audit's scope.

The final source passed92 engine/raw/boundary and12 native tests, both strict Clippy modes and formatting. The97 selected integration tests ran before the final test-only `#[cfg(test)]` annotation on `RollupIndex::remove`; their logs retain that distinction. The annotation is covered by final-source compilation/lint and the final unit/native runs, not retroactively by those integration runs.
