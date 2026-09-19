# Native deployment, failed setup and recovery — 2026-09-19

**No new performance result. The comparison failed before measurement.** Recovery restored availability and the two retained datasets passed exact checks, but the control-admission defect is still open.

## Authenticated deployment

Native/journal-lock code `9c82b0f` passed hosted CI. `7cbd207` enables `query_native_reuse` only in the rebuild benchmark profile; its CI also passed. Every other profile value is unchanged, including the default 64 MiB derived-state budget. Native reuse remains disabled by default outside the explicit benchmark opt-in.

Only the existing rebuild Varve service was deployed, as `92c52e70-0691-4c09-8884-835347829afa`. All **151** staged files match the runtime source manifest. The pinned DuckDB library/header remain unchanged. Database identity `1e006339-802c-4ed7-ac03-cced48430e15` and sequence 1555 survived deployment. Actual database cgroups remain 2 CPU / 1,999,998,976 bytes; the unchanged driver has 2 CPU / 999,997,440 bytes. Timescale/driver deployment identities and all platform limits match the before/after snapshots. No unrelated service was mutated.

The actual driver is **Python 3.12.14**, not the sandbox's 3.13.15. The unchanged 57-test hermetic suite passed on the driver as UID/GID 10001, without installing dependencies. `unit-inputs.sha256` identifies those inputs relative to `benchmarks/timescale/` at `7cbd207`.

## Passing conservation, failed new workload

The frozen driver/oracle source hashes are in `driver.sha256`. Both retained reports passed after deployment and again after recovery:

| Existing report | Exact raw rows | Exact minute aggregate groups |
| --- | ---: | ---: |
| `io13_b128_01` | 8,448 | 4,352 |
| `io13_b1_01` | 1,074 | 1,074 |

These checks cover every deterministic raw identity, exact quarter value, empty tags, multiplicity and named minute count/sum/min/max group in those reports. They do not certify unrelated tables, equal-timestamp first/last/OHLC ordering, long-duration performance or independent-storage disaster recovery.

`native01_b128_01` was intended to run 8,192 initial rows, batch128, 50 query samples and 60 seconds of 512 rows/s mixed load. It passed preflight, then failed during schema setup at **15:22:50 UTC**:

> apply committed control WAL; reopen for recovery: derived resident/working byte budget exceeded

The engine correctly stopped further operation by fencing itself, but the avoidable failure occurred **after** a control WAL commit. Readiness became HTTP503; sequence1557/checkpoint1556, derived resident22,679,768 bytes, working0, no pinned raw memory and no OOM. Native workers held no resident input bytes. This is not evidence that native reuse caused the failure. The source witness is the post-control index reservation in `engine.rs`, while aggregate creation retains its backfill and projected-catalog reservations in `control.rs`.

The failed report and its failed supplemental oracle are preserved in `results1/`. The latter correctly refuses a report without the completed workload manifest. Neither is a benchmark approval.

## Recovery without concealing the defect

Before restarting, the exact deployed binary and unchanged config successfully reopened an isolated **6,139,571-byte remote copy**. Every original file hash remained unchanged. Only recovery metadata—not the database copy—was retrieved here.

The same deployment was then restarted without rebuilding, changing limits/configuration or deleting namespaces. At **15:53:02 UTC**, readiness was HTTP200, identity/sequence1557 were unchanged and the fence was clear. Both retained exact oracles passed again. The previously committed, unacknowledged control operation was replayed; recovery is not proof that the original HTTP400 meant rejection.

No new workload retry, quota increase or source bypass was used. A repair must admit all necessary control/index/accounting working memory before WAL commitment, preserve real ownership accounting and reject genuine resource shortages without committing or fencing. That repair and sustained comparisons remain pending.

## Evidence

`deploy-source.sha256` and `runtime-source.sha256` authenticate the upload/runtime closure; `effective-benchmark-config.json` captures the sole opt-in. Runtime and platform snapshots distinguish deployment health from the later fence/recovery. `run-native.sh` records the finite non-root workload and exact-check commands. `results1/`, `after-recovery-exact/` and `copied-recovery-metadata/` retain outcomes, including failures. `SHA256SUMS` covers this directory except itself.
