# Native qualification + Journal owner-release handoff

**Remote qualification completed; await Main review/integration and hosted CI. Native reuse remains default OFF. Cargo ownership is released to Main at this handoff. No Root integration, commit, deployment or resource/config change.**

## Final source and review patches

Local source `.tools/performance-safety-20260919/native-qualification`, evidence sibling `native-qualification-evidence`. Remote source/evidence use those names under `/workspace/varve-rebuild/performance-safety-20260919`.

- Full132 `source-files-qualified.sha256`: **438499613a28b058a562cf1788a073ee100f2a54bafcd085055bc99eb3662aa8**.
- Owned14 `owned-files-qualified.sha256`: **c9930b4bdbca7fb662de7302ba18cc4d7225c82bdd005130d0166e43a1319663**. Inventory `owned-files14.txt`.
- `combined-from-f884.patch`: **c92a34fdaa199d97bc17cecaf8789db4e690be486a2dbe82e0e18c347430a0fe**. Twelve changed paths from authenticated Mainf884/full129; three additions yield132. Dry-run against untouched Main fence passed.
- `journal-lifecycle.patch`: **357d502ecf28bd4a09e622755d390116e2d89acc3bd070bc719c5053f073f039**.
- Native-only corrected checkpoint retained as `source-files-native-final.sha256`: **000badde6f2be5b38c0f80c99ce24af6c3932d26cbdb72497890a91e8e03f033**, owned12 **789188354033ef5cb64f0e772845bebc9098bc6edbdb2278c03150c030599036**, plus source archive.

Original native132/full6bb9 and owned12/06b5, original128/full6f9, and Mainf884 copies were reverified unchanged. No unspecified closure code fix imported: README and tuple formatting were already present. Remote rustfmt changed six original owned files. Semantic qualification edits are only the explicitly authorized EXPLAIN test/doc correction and Journal production/test repair. Native renderer, ABI bindings, dependencies, quotas, profiles and durability formats remain unchanged.

## Final combined-source results

Full suites used **--test-threads=2**, without retries, newly ignored tests or added global serialization.

| Gate | Result | Evidence |
| --- | --- | --- |
| Full workspace default | **579 passed,0 failed,1 existing live-S3 ignored** | `34-combined-workspace-default.*` |
| Full workspace fault-injection | **679 passed,0 failed,1 existing live-S3 ignored** | `35-combined-workspace-fault.*` |
| Storage library | **297 default /337 fault passed** | same logs |
| All39 storage integration targets | **251 default /311 fault passed** | same logs |
| Storage CLI units | **8 each mode passed** | same logs |
| Rust client library +3 integration targets | **23 each mode passed** | same logs |
| Strict workspace/all-target Clippy | **PASS default and fault** | cases36/37 |
| Workspace fmt | **PASS** | case38 |

Counts are per-mode executions, not unique tests or performance statistics. `*.top-level-results.txt` separates parent summaries from child-test output. Commands, exact flags/exits, logs, source/runtime before/after gates, owners, binary hashes and depinfo are retained. Full suites exercise current inputs, default fresh and opt-in reuse, cancellation/migration, owner credits, cold pins/GC, namespace/files/settings, model/status/config, workers, security and services.

## ABI and EXPLAIN evidence

Case02 first compiled from the new root after package cleanup and passed exactly `transaction_rollback_detaches_catalog_callbacks_without_closing_session`: live registrations before rollback, zero callback owners/functions/views afterward while the session stays open. Safe close was never substituted for reuse. The unchanged130-refresh test requires exactly3 constructions/127reuses/130resets and passes in original and final suites.

Case03 originally returned8pass/1fail only on the new invalid cross-backend EXPLAIN equality oracle; cases08/13/15 preserve the same wider default/fault failures. CLI stages `__varve_input` and produces scan/join plans; native uses `__VARVE_RAW_SCAN_0`. The public contract is opaque backend-rendered single-plan text, not byte-identical physical plans.

Main authorized correcting this oracle. All actual SQL data-result cases retain exact CLI JSON equality. EXPLAIN now requires exact fresh-native versus repeated real reused-native equality for identical inputs, nonempty single-plan shape and native scan/projection markers; CLI independently requires its shape/staged-input scan. Same-key reused-session plan overflow must discard without successful reset, release all owners/credit, and permit fresh successful plan recovery. Namespace/table/security/quota and130/3/127 assertions remain intact. Corrected focused case21 passed1; native groups22/23 passed33 each. `docs/NATIVE_REUSE.md` documents the distinction; production rendering is untouched.

## Journal repair, red/green and CI limitation

Main supplied CI35444232886 on192562d:324/325 library tests passed; checkpoint-successor reopen failed with ownership EAGAIN. The actual interfering CI child identity was not captured.

Journal lacked explicit flock release. Duplicated/inherited handles retain its open-file description after one File closes. `Drop for Journal` now takes/drops `active` before `FileExt::unlock(&_lock)`. Remaining fields have no I/O destructors or consuming field-move requirements. Teardown does not seal, sync, unlink LOCK, change bytes or acknowledge writes. Drop cannot return unlock failure; descriptor close remains fallback, possibly leaving ownership unavailable while inherited descriptions survive, never creating an acknowledgment.

Two new deterministic regressions retain the lock through `try_clone` and an actual Unix sh child with the cloned lock on stdin. Duplex UnixStream ready/release gates have10second socket deadlines and RAII kill/wait; no unsafe/sleeps/retries. They require live-owner exclusion, unchanged bytes on drop, successor reopen/replay while the old descriptor lives, continued successor exclusion after old-descriptor/child closure, and durable sequence2 replay.

- Case30 expected negative: **0pass/2fail**, EAGAIN at successor reopen against old no-unlock production, two threads.
- Case32 positive: **2pass**, exact unchanged test SHA **c876f1546b691b21af775d03e9bf5dd32d411223ae27137d5c3b90b951b9a4c1**.
- Case33: **49 journal tests passed** at2threads, including the CI-named regression.
- Cases34/35: complete combined default/fault suites passed at2threads.

This proves the controlled retention/release mechanism, not identification of the actual CI child or a hosted CI rerun. All red source archives/manifests and failures remain preserved.

## Runtime, cache and evidence controls

Only assigned sandbox18b148a9-5d1c-4ddc-a80f-2c6c9668dada/project8caffa15-0158-4822-a6c2-cb405bddc62d/env5ec35c82-c1ea-4aa4-b7a5-b89e4c4b9ed1 used. Every CLI call carried required telemetry. rustc/cargo1.98.1, target `/workspace/varve-rebuild/target`, jobs2, debug0/incrementalfalse, locked/offline. No fetches, bundled C++, containers, large fixtures or local source execution.

Case01 cleaned only varve-storage/varve-client from the prior fence root, preserving dependencies, before switching source root. `.d` CARGO_MANIFEST_DIR values were checked against the exact qualification root or Rust-client subdirectory; root never changed again. Supervisors acquired existing cargo-owner.lock, and tested child commands closed FD9. Lock never deleted/replaced.

Only remote source link `.tools/duckdb -> /workspace/varve-rebuild/baseline/.tools/duckdb` is excluded from source hashes and included in runtime gates. CLI SHA **1dd0a1596505613a439dced4fb5f5800471badec82b2dc3b2c0efbd46fafbf6d**. Native library `/workspace/varve-rebuild/baseline/.tools/duckdb-native-v2.0.0-alpha41533-linux-amd64/libduckdb.so`, SHA **69bdd44e0d2426e7ba44ed14644b54d8bd99a9703e5cbb4aec3f8beef40f817d**. Gates verify exact132 paths/directories and reject unexpected links/types. Local source has no runtime link.

Versioned checkpoints and source snapshots were promptly retrieved as source/evidence-only ustar streams and locally hash-verified; no binaries/fixtures downloaded. Earlier wrappers captured package library/CLI identities; later wrappers also capture storage integration artifacts/depinfo. Client integration identities are supplemented separately. `cargo-release-final.txt` and empty `processes-after-combined.txt` witness available flock and no remaining Cargo/rustc/owned child. Cargo is released to Main; no sandbox lifecycle mutation was performed.

## Remaining review and exclusions

Main must review exact combined/native-oracle/journal diffs, integrate and rerun hosted CI. No Root edits/commits were made here. Sol's original132 independent review reports no actionable findings and links pinned ResultWrapperV2 transaction/catalog/destroy source; new authorized oracle/journal changes still require Main review. Sequential native handle migration has bounded runtime evidence, not a broad upstream guarantee.

The pre-existing live-S3 test remains ignored without external credentials/opt-in. No hosted CI, live cloud conformance, production/distributed or sustained performance guarantee is claimed. Candidate source docs retain conservative initial handoff qualification wording; this evidence handoff is the current runtime record, and Main owns public qualification/enabling decisions.
