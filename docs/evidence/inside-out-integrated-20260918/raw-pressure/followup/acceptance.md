# Owned-epoch reviewer follow-ups — qualified

Base qualified raw-pressure manifest128: `5244039ae491f1ccd3481b630967b7127435572cf35a775e13e21ad26b5b33e3`.
Final follow-up source manifest128: `0658a56988a24534b5b61f268274a71f5ed7327d42e92495d88f969dddb61c7f`.
Final follow-up owned manifest4: `d6eeb5449056d1b0ccb20256f9de30385140289e9eb6cc7d1bb6e5694809aa81`.

Exact local root/AGENTS/ARCHITECTURE/ACCEPTANCE reverified. Original128source was copied to raw-pressure-qualified before any follow-up edits. The first TEST-ONLY red run briefly used raw-pressure; after Main requested a distinct workdir, those edits moved to raw-pressure-followup and raw-pressure was restored byte-for-byte, verified, and confirmed safe to Main before fetch. All subsequent follow-up work is isolated in raw-pressure-followup. Both original raw-pressure and raw-pressure-qualified witnesses still verify against the original manifest; original logs/manifests were not overwritten.

- [x] Ingestion EpochBeforeInstall failure after actual sync: legacy WAL and segmented journal; four accepted writes plus two interleaved flush barriers all explicitly terminate; ring6/6slots and zero charged bytes, raw credits zero, no lost completion channels, publication authority fenced, reopen recovers exactly one durable request. This is not merely direct durable-drop or worker-entry panic coverage.
- [x] Frozen prefix root succeeds before GroupCheckpointComplete fails: legacy/journal and inline/derived-page variants, valid/expired/conflicting durable retries. While blocked, tests witness advanced durable root, exact180/200 persisted floor, receipt pruning and no new rows. Completion error rechecks current root, preserves valid duplicates, rejects expired/conflicting IDs, and does not lower floor; live terminal floor250 is persisted and recovered. No floor algorithm change was needed.
- [x] Gate-only poison is observable via Status.fenced with explicit publication/reopen/recovery reason. Existing State reason wins. Normal armed-token drop while State is held does not mutate/acquire State; reopen clears process fencing. Publication-gate implementation and exclusion semantics unchanged.
- [x] Existing raw13 and both reproduced checkpoint-floor tests retained. Final engine/raw/boundary109, flow12, ingest11, native19 and relevant integrations121 passed. Default/fault strict Clippy and fmt passed.
- [x] Every one of8final qualification commands checks all128source hashes BEFORE and AFTER, recording the exact same manifest hash. Whole-matrix frozen-baseline checks also passed. Formatting is a separate intentional mutation step before final manifest capture.
- [x] Only4owned formatted paths synced back: engine.rs, owned_epoch_tests.rs, ingest_flow_tests.rs and docs/METRICS.md. Full128localmanifest and owned4hashes verify. Production diff is solely Status.fenced fallback (engine.diff); no publication_gate, WAL/journal, raw accounting, native ABI, benchmark/config/profile/SDK/Docker or overview-doc changes.

## Evidence interpretation

followup-red.log reproduces the real gate-only Status.fenced=null defect. Its prefix failure was a fixture grouping error: five requested rows exceeded a four-row group ceiling, splitting away the clock donor, so the root correctly stayed150. The fixture was corrected to keep four inputs in one bounded epoch and separately select valid/expired/conflicting retry cases. This is not labeled a production-floor bug or hidden by weaker assertions. Followup-green-01 passed all3regressions; final-source matrix includes them. All failure logs retained. No local build/test/fixtures or service/resource/credential/S3 mutation was performed.

Remaining partition-owner, production/deployment/SDK/live-S3/full matched-benchmark and RSS/distributed qualification gaps from the base remain unchanged. Main owns diagnostic deployments and overview docs. Shared /workspace/varve-rebuild/target is explicitly RELEASED again; no pending Cargo job remains.
