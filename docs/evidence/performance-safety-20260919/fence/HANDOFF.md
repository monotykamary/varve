# Isolated baseline remote-fence backport handoff

**SOURCE ONLY — UNTESTED / UNQUALIFIED. Execution, integration and deployment remain blocked pending replacement sandbox permission and independent review.** No local builds/tests/rustfmt/Cargo/fixtures/source execution/cloud/resources/delegation/commits or Root edits. No passing-test counts claimed.

## Checkpoint paths and hashes

- Source: `.tools/rebuild-20260918/baseline-fence-repair` (exact128, no artifacts).
- Evidence: `.tools/rebuild-20260918/baseline-fence-repair-evidence`.
- Baseline128 / `baseline-source128.sha256`: `13e9198e00e6d11a26c89def8fd775a2a326236f7bd844cfdef288883e4441c0`.
- Full128 / `source-files.sha256`: `6f9a1f288089082f010a5d74f9a8463d4c6d376b2a9ba5be3216a5c72e351c60`.
- Owned3 / `owned-files.sha256`: `0f57da983ae979adb908b6cc05a74719954ef6050234ee979b07755bd8f1f94f`.
- `repair.patch`: `8ec16f3556bc90388d651b8e3042a8f9995103d3cec6cf617c0d64029500d154`.

| Only changed paths | SHA-256 |
| --- | --- |
| src/commit_boundary.rs | a35eb39a12e0d77798bb22c25c9e3636705288b79ddcf1e737ebf5b5a0170da4 |
| src/owned_epoch_tests.rs | f701de44119f4771a620ba7e38a6264b1a348a21af42debe4713fdf743099eef |
| src/publication_gate.rs | df5423627d6d947cf25a5a116cddf4607220427d97358363e9648e1e8a05e8d3 |

The patch has a/src/... and b/src/... labels and was generated directly against verified source13. Path lists, manifests, static-delta-review.txt, symbols-and-scope.log and after-check logs retain exact scope and source evidence. Evidence is outside the128 source tree.

Both original raw-safety-repair and raw-safety-repair-review, plus live-root128, were reverified against hash13. Partition reference full130 remains `b73c83a0a740f7fb399837feaf1c2563b5e37246510dd356db83959079e467a2`, with every manifest entry checked. No frozen/partition work was edited. Root status before/after is identical. This is not partition b73 integration or WALv2; b73's open conservative-admission compatibility remains separate.

## Minimal repair and authorized test delta

Before arming Some(publication), reacquire State/check healthy; the guard ends inside and_then before I/O or fail(false). At install, check healthy under the State lock before taking pending publication; drop State before existing fail(true). Existing fail/demotion/floor logic, fields/destruction order and pending install remain byte-identical. There is no truncation/rollback of durable bytes, nor a State lock over fsync. No-publication outcomes are unchanged.

Source13 lacked the reference wait hook. The explicitly authorized publication_gate.rs addition is entirely cfg(all(test, feature = "fault-injection")): optional one-shot waiter sender, notification before condvar wait and notify_next_wait setter. No production fence() API or gate semantics were ported. All non-test/default gate logic is unchanged. No engine/model/catalog/Arc ownership/quota, journal/native/raw_memory, Cargo/profile or registration changes.

## Regression source, NOT RUN

- real_remote_cas_fence_before_sync_and_after_sync_preserves_only_old_receipts: both log modes; initial ship succeeds, next ship captures then gates at CAS before preparing a mixed old+new+same-epoch-duplicate epoch. A competing actual FileStore CAS writes valid but nonidentical head bytes, making the original CAS stale. The real tier path records State.fenced, then its outer handler waits on the epoch lease; the waiter hook observes that boundary. Pre-sync asserts exact WAL/journal namespace+bytes and append count unchanged. Post-sync requires real changed durable bytes, no new visible install/ACK and no byte rollback. Both assert old receipt/floor survival, new-result demotion and released private credit.
- real_remote_cas_fence_does_not_change_prepared_no_publication_outcomes: both modes, prepared old duplicate receipts/floor remain exact even after real CAS fencing; no WAL change. A separate observer lease exists only to synchronize the ship handler.
- real_remote_cas_fence_drains_ingestor_senders_and_barrier_without_install: both modes, existing EpochBeforeInstall hook released normally, not with an injected error. After real fence observation, enqueue later work and a FIFO barrier. Source checks every accepted sender terminates with an error, barrier failure counters/sequence, unchanged committed snapshot, retained durable bytes, reclaimed flow slots/credit. A local guard releases the hook before ingestor destruction on assertion unwind.

No sleeps/polling or synthetic State.fenced assignment were added. Channel/condvar events order the schedule; timeouts only bound waits. Later ingestion cannot steal ship's one-shot probe because submission occurs after observing it. TempDirs are scoped and workers joined on the normal path. All inherited tests/helpers are unchanged, including the baseline's historical synthetic gate test.

## Remote check plan — DO NOT EXECUTE NOW

Requires explicit replacement sandbox permission and independent review of this exact patch. Use only approved existing remote resources/scratch and provisioned pinned toolchain/dependency cache. Transfer the exact128 source plus sibling evidence; no artifacts/fixtures/containers/new resources/bundled DuckDB build/profile changes. Preserve configured dev/test profiles (debug=0, incremental=false) and jobs=2. Put CARGO_TARGET_DIR and logs outside the128 source tree. If required cached dependencies or pinned runtime tools are absent, stop; do not fetch/build substitutes without approval.

For EACH command below: verify manifest-file digests against the full128/owned3 hashes above, then check every source and owned entry BEFORE and AFTER (including command failures). In the isolated source directory use `sha256sum -c ../baseline-fence-repair-evidence/source-files.sha256` and the analogous owned-files.sha256 check. Verify the exact source path set too. Save command, selected tests, exit status/stdout/stderr and both source gates. Recheck original/live-root source gates where accessible. Do not qualify zero-selected-test runs, drift, missing logs or silent flag changes.

The following is an unexecuted plan, never a local instruction:

```sh
# Fault-only owned and boundary modules must select real tests.
cargo test --locked --offline -p varve-storage --lib --features fault-injection owned_epoch_tests::
cargo test --locked --offline -p varve-storage --lib --features fault-injection commit_boundary

# Coordinator unit coverage, default then fault.
cargo test --locked --offline -p varve-storage --lib ingest::
cargo test --locked --offline -p varve-storage --lib --features fault-injection ingest::

# Filesystem integration coverage, no cloud.
cargo test --locked --offline -p varve-storage --test group_commit --test ingest --test ingest_flush --test ingest_traces --test journal_engine --test journal_remote --test remote_store
cargo test --locked --offline -p varve-storage --features fault-injection --test group_commit --test ingest --test ingest_flush --test ingest_traces --test journal_engine --test journal_remote --test remote_store

# Strict lint, both configurations; fmt checks only.
cargo clippy --locked --offline --workspace --all-targets -- -D warnings
cargo clippy --locked --offline --workspace --all-targets --features fault-injection -- -D warnings
cargo fmt --all -- --check
```

Use an explicit outer command timeout for liveness diagnostics, not a timing-based schedule. Inspect failures; never weaken durability/corruption assertions. Corrections need a new manifest/patch/review checkpoint and fresh gates. Separately authorized negative-control validation may remove each new health check in a disposable copy while retaining test instrumentation to show the corresponding baseline race assertion fails; never mutate this checkpoint for a red run. Expand coverage only for evidence/review needs.

## Unresolved

No additional source-level correctness blocker identified in this narrow static review; compile/format/behavior/liveness remain unverified. A fence after the pre-arm observation may leave durable bytes; install rejects when it observes the recorded fence. Earlier-installed commits and independent old receipts are not revoked. These tests make no arbitrary competing-ownership recovery/reopen, distributed, or production claim. Remote permission and independent review remain blockers.
