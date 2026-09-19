# Inside-out replacement: first component checkpoint

Date: 2026-09-18. **Not a production-engine migration or performance result.** All compilation, executable tests, model checks and probes ran on an isolated Railway sandbox. No local build or benchmark data was created.

## Witnesses

- `component-recipe.log` / `.exit`: checked-in recipe completed with status 0; **12 flow + 22 journal tests**, strict Clippy and rustfmt, the pipeline proof and the native scanner probe passed.
- The two Loom checks reproduce the old reclamation counterexample and check corrected inspection ordering with preemption bound two. They do not model the whole queue, wakeups, journal or database.
- `pipeline.json`: 4,096 individual events from four producers, all 4,096 acknowledged after durable visibility and recovered with exact identities. 128 journal groups, six segments, six namespace barriers, 139 file syncs, 82,624 encoded bytes, zero remaining flow charge. No offered events were lost **in this component probe**.
- `native-final-source.log`: DuckDB `v2.0.0-alpha41533` C scanner probe passed projection, caller-buffer refresh and destructor checks. No Rust adapter, cancellation, mixed Parquet, private-database isolation or performance qualification.
- `flow-review-red.log`: two actual pre-fix failures (swallowed capacity wake; coalescing after fencing). These are documentary pre-fix logs; the archive contains the corrected implementation, not the old buggy sources.
- `baseline-checks.log` / `baseline-path-checks.log`: 51 baseline query tests passed; one failed because default `duckdb` was missing from PATH, then passed after environment correction. Other SQL fixtures explicitly selected `.tools/duckdb`, not silent availability skips. Nine baseline ingestion tests also passed.

A late C probe comment-only change was caught by the source hash comparison. Only that probe was recompiled/reexecuted on Railway; the final source archive and `native-final-source.log` contain the corrected source identity. Rust source hashes were unchanged.

Independent scoped source review closed all three reported flow issues and found no additional concrete regression. Remote results were supplied evidence, not independently rerun by the reviewer. Contour's JS/TS-only metrics do not qualify the Rust/C patch; broader worktree advisories were not rewritten merely to improve a score.

## Source binding and reproduction

`source.tar.gz` is the final source-only build snapshot, not a Git-history archive. No toolchain, native library, build cache, credentials or database fixtures are bundled. `source-inputs.sha256` identifies relevant live repository inputs; `SHA256SUMS` binds the retained artifacts. The evidence is about 540 KiB, not another multi-gigabyte benchmark tree.

From this directory, `shasum -a 256 -c SHA256SUMS` verifies artifact integrity. From the repository root, `shasum -a 256 -c docs/evidence/inside-out-components-20260918/source-inputs.sha256` checks live input equality at this checkpoint. Later edits may intentionally differ; the archived source remains the witness.

On Railway, with Rust 1.98.1, rustfmt, Clippy, gcc, curl, unzip, GNU timeout and the normal crate build prerequisites:

```sh
mkdir -p replay
# Run on Railway, not the developer laptop.
tar -xzf source.tar.gz -C replay
cd replay
export RUSTUP_TOOLCHAIN=1.98.1
export VARVE_REMOTE_QUALIFICATION=railway
export CARGO_TARGET_DIR=/workspace/varve-rebuild/target
bash scripts/verify-rebuild-components.sh
```

The explicit environment flag is operator intent, not cloud attestation. The recipe installs the hash-pinned native library. It does not install Rust or qualify the legacy SQL baseline.

## Cloud ownership and resume

- Project: `8caffa15-0158-4822-a6c2-cb405bddc62d`; environment: `5ec35c82-c1ea-4aa4-b7a5-b89e4c4b9ed1`.
- Owned qualification sandbox: `d61734d7-61eb-4658-aa85-bae3a52e6373`.
- One checkpoint: `varve-inside-out-20260918-d61734d7`, created synchronously at `2026-09-18T10:41:41.173Z`, retaining the 1.4-GiB build cache remotely.
- Candidate checkout: `/workspace/varve-rebuild/candidate`; legacy baseline: `/workspace/varve-rebuild/baseline`; shared target: `/workspace/varve-rebuild/target`.
- Recreate with `railway sandbox create --checkpoint varve-inside-out-20260918-d61734d7` in the explicit project/environment. Use no private network or production service variables. Revalidate source equality and stream new source before testing further changes.
- Explicit destruction succeeded; the subsequent sandbox list was empty and exactly the one checkpoint above remained (`cleanup.json`).
- The checkpoint is storage, not running compute. This checkpoint will need explicit deletion after the migration or when no longer useful. Sandbox cleanup status is recorded separately; existing app/benchmark services were not deployed, restarted or reconfigured.

## Remaining acceptance work

See [the replacement ledger](../../INSIDE_OUT_REBUILD.md). Actual engine/public-API wiring, time-series rows and rollups, idempotency, native Rust SQL integration, checkpoint reclamation, S3/object publication, lifecycle policies, complete failure qualification and a matched Timescale comparison remain pending. Flow payload charges are not a whole-database RSS bound. No publish, default-path switch, readiness label change or performance-win claim follows from this checkpoint.
