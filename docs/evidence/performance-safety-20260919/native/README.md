# Native reuse and journal ownership — 2026-09-19

**Remotely qualified and integrated; native reuse remains opt-in. Hosted CI and live performance comparison are separate gates.** No local builds/tests/load or service/resource/limit changes were performed.

## Results

| Gate | Result |
| --- | --- |
| Full workspace, default | 579 passed; 1 existing live-S3 test ignored |
| Full workspace, fault injection | 679 passed; 1 existing live-S3 test ignored |
| Strict workspace Clippy | default and fault passed |
| Rustfmt | passed |
| Live callback detachment | passed without closing/replacing the native session |
| 130 current-input refreshes | exactly 3 constructions, 127 reuses, 130 resets |
| Journal ownership negative controls | 2 failures before fix → 2 passes after; identical test bytes |
| Actual Rust SDK, native reuse | passed |
| Actual TypeScript SDK, native reuse | passed |
| Positive public-service probe | 32 durable writes and current results; 1 construction, 31 reuses, 32 resets; no CLI executable available |

Default totals contain 297 library, 251 storage integration, 8 CLI and 23 Rust client tests; fault totals contain 337, 311, 8 and 23 respectively. All 39 storage integration targets ran. Counts are per-mode executions, not distinct tests or performance measurements. Earlier targeted executions overlap these totals.

## Journal ownership repair

[CI35444232886](https://github.com/monotykamary/varve/actions/runs/35444232886) for `192562d` failed the checkpoint-successor reopen test with EAGAIN after `drop(journal)`. An inherited or duplicated file description can retain `flock` after the original File closes. Journal destruction now closes its active writable handle, then explicitly unlocks its directory lease. It does not seal, sync, unlink, modify durable bytes or acknowledge data.

Two deterministic tests retain the description via `try_clone` and an actual Unix child. They require exclusion while the original owner lives, successor reopen/replay while the old description remains, and successor exclusion after that stale description/child closes. Both unchanged oracles fail old production and pass the repair. This proves the controlled mechanism; the original interfering CI child was not captured. No retries, sleeps, new ignores or global serialization hide the failure. Full suites still use two test threads.

## EXPLAIN is not a data-result equality oracle

The retained original failure compared different physical plans: CLI staged input/joins versus the native custom scan. Actual SQL result cases still require exact native/CLI JSON equality. EXPLAIN now requires exact fresh-native/reused-native equality for identical inputs, independent CLI plan shape, real reuse, bounded overflow/discard, owner/credit release and fresh recovery. Production rendering did not change; the documented plan string is backend-rendered and opaque.

## Reproduce and inspect

- Frozen qualified132 manifest: `438499613a28b058a562cf1788a073ee100f2a54bafcd085055bc99eb3662aa8`.
- Owned14 manifest: `c9930b4bdbca7fb662de7302ba18cc4d7225c82bdd005130d0166e43a1319663`.
- Combined patch from the earlier f884 checkpoint: `c92a34fdaa199d97bc17cecaf8789db4e690be486a2dbe82e0e18c347430a0fe`.
- Root matched all 132 inputs immediately after integration. The only subsequent difference within that closure is current qualification wording in `docs/NATIVE_REUSE.md`; `source-files-integrated.sha256` records it. All implementation/configuration/test bytes remain identical to the executed candidate.

`evidence.tar.gz` contains the exact source snapshots, original failures, commands/exits, source/runtime before/after gates, binary hashes/dep-info, complete logs and service probe inputs. After extraction, verify `native-qualification-evidence/main-SHA256SUMS` and `native-service-input/INPUTS.sha256` in their respective directories. Large executables and generated Node compiler caches are not included. The closeout's initial empty-fixture check found only an owned 1.9MiB Node compile cache; it was identified and removed, with no owned service children remaining.

Decisive logs inside the bundle: cases34/35 full workspaces,36/37 Clippy,38 fmt,30/32 journal red/green,33 journal suite and39–42 actual service build/tests/probe. `independent-final-review.md` is a separate authenticated source review; it reports no actionable findings within this patch, not whole-repository approval.

## Limits

Pinned DuckDB alpha library/header and Rust1.98.1; sequential handle migration has bounded runtime evidence, not a blanket upstream guarantee. The existing live-S3 test remains ignored. Local fsync is not S3 durability. No non-Unix inheritance, arbitrary power-loss, distributed, production or sustained Timescale victory claim. Partition preparation and fixed-extent WALv2 are not included.
