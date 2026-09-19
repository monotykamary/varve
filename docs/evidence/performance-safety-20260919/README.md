# Performance and safety checkpoint — 2026-09-19

**Qualified safety checkpoint, not a new performance result or production certification.** All builds, tests and fixtures ran in one Railway sandbox. No local build/load ran and no database-service configuration was changed. The three-file remote-fence fix is integrated into the repository; deployment and sustained comparison are separate gates.

## Final results

| Check | Result |
| --- | --- |
| Storage library, fault-injection | 325 passed |
| Storage integration, fault-injection | 311 passed across 39 targets; 1 live-S3 test ignored |
| Rust client suite including real service | 24 passed |
| TypeScript client unit suite | 18 passed |
| TypeScript actual CLI-backed service | 1 passed |
| TypeScript actual native/segmented service | 1 passed |
| Benchmark/oracle/frontier unit suite | 57 passed |
| Strict workspace Clippy | default and fault-injection passed |
| Rustfmt | passed |
| Negative controls | Removing either new health check separately caused the unchanged real-CAS oracle to fail, as required |

The integration count excludes four nested subprocess reruns from the parent-target total. Earlier 178 targeted executions overlap this coverage and are not added again. Tests are scoped single-node evidence; the ignored live-S3 test and broader production/long-duration gates remain open.

## Safety boundary and source identity

Before arming a publication, the engine rechecks recorded health. Under the install State lock, it rechecks again before installing pending state. An already-started sync may leave durable bytes, but an observed fence prevents new installation/ACK. Independent old receipts and no-publication duplicate outcomes remain valid; there is no truncation of durable bytes.

Final 129 compile-input manifest: `f884d3592e240c77ef4458ca8fbac073cbc972bdc736e2ec0e6637b3071a359c` (`fence/source-files-final.sha256`). Owned 3 manifest: `91b3caec81b4cf809a48d35c4786f68268e671e1d87b37e20caf122a50a87935`. Exact source is in `fence/final-source.tar.gz`. The repository matched all 129 final inputs after integration.

The original independently reviewed 128-file candidate and its three-file patch remain retained. Qualification added the unchanged Rust client README required by `include_str!` and reflowed one test tuple; no additional production logic changed. Historical `fence/HANDOFF.md`, `ACCEPTANCE.md`, and source-review notes describe their earlier unexecuted checkpoint, not the final runtime status above.

Client/benchmark 29-input manifest: `b8a09ec509dd0d8196b960e6fbdb480c416221e9c014e0a6ccec2ef6b803596d`. Exact inputs and before/after gates are retained in `clients-benchmark/`.

## Reproduction and retained failures

Rust/Cargo 1.98.1, jobs 2, configured small profiles, no bundled DuckDB build. Node 24.21.0; Python 3.13.15 with unchanged pinned benchmark requirements. The CLI and native library are the pinned v2 alpha artifacts; scripts record exact paths/hashes. Only the declared `.tools/duckdb` symlink was admitted as a runtime binding, separately checked from source.

Retained failures explain the progression rather than being hidden:

- The restored checkpoint lacked three locked crates; an isolated remote locked fetch filled the cache without changing versions.
- Workspace lint required the Rust client README omitted from the original source closure.
- One formatting-only correction was required.
- Broad CLI tests initially lacked their hardcoded runtime path.
- A shared Cargo target reused a test binary with `CARGO_MANIFEST_DIR` from a negative-control tree. That filtered attempt is NOT final-candidate evidence. Only our package artifacts were invalidated; final storage library and all integrations were rebuilt/rerun, with dep-info showing the correct source directory. Future worktree changes must not trust source hashes alone as binary identity.
- A forced-stop transport fixture left an orphaned gated shell holding inherited lock FD9. Its exact PID/command/descriptor were verified before terminating it. The supervisor now closes that FD in child commands. This harness cleanup does not establish general descendant-process cleanup guarantees.
- Python 3.14 lacked wheels for pinned requirements; Python 3.13 matching the driver minor was used instead of changing dependencies.

The decisive final logs are `fence/storage-lib-final.log`, `integrations-all-final.log`, `rust-client-service-final.log`, the two `clippy-*-final.log` files and `fmt-final.log`. `final-status.txt` records completion after the retained earlier failed matrices. Each operation's command, exit and source gates remain alongside its log. `fence/runtime-SHA256SUMS` and `clients-benchmark/SHA256SUMS` authenticate their complete archived artifact sets. Large compiled binaries are not committed.

## Performance remains unproven beyond the prior smokes

Latest [retained comparison](../inside-out-integrated-20260918/raw-safety-runtime/README.md): single rows 291.82 vs 699.82 rows/s; batch 128: 34,285.86 vs 21,520.93 rows/s (Varve 1.59× in fewer than one second); default-tier query samples 60–99 vs 1.9–6.0 ms. Varve still lacks a sustained or overall Timescale win. Native reuse, bounded partition preparation and fixed-extent WAL candidates are not integrated or certified by this checkpoint.

A local fsync ACK is not an S3 ACK. Complete coordinated storage rollback needs independent monotonic authority. Keep these limits separate from the no-silent-loss and fail-closed assertions tested here.
