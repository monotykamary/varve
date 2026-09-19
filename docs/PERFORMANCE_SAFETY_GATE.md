# Performance and data-safety gate

## Objective

Improve durable single-row ingestion, batched ingestion and hot/cold SQL without weakening ACK, replay, corruption, bounded-memory or lifecycle semantics. An overall Timescale win is a goal, not an established result. Builds, fixtures and load run on Railway only; local work is restricted to source and small evidence artifacts.

## Acceptance ledger — 2026-09-19 continuation

| Gate | Required evidence | Status |
| --- | --- | --- |
| Fence safety | Real remote CAS before sync and before install; no new install after observed fence; old durable receipts preserved; both log modes; sender/barrier drain | Qualified on Railway and integrated: 325 library +311 integration tests, real-CAS negative controls, strict lint/format; see current evidence |
| WAL improvement | Preserve v1; exact durable authority and independent inner validation; two ordered barriers before ACK; fail closed on unknown/corrupt suffix; seal and remote exact bytes; crash/reopen matrix | Conservative recovery and migration contract amended; isolated opt-in implementation underway, unqualified |
| Partition preparation | Reserve before allocation, cover live clone/overlay lifetimes, avoid repeated-key overreservation; explicit error-order compatibility; concurrent owner and quota regressions | Isolated candidate unqualified; admission redesign required |
| Native SQL reuse | Bounded reusable native runtime, exact per-query authority/data, no stale callbacks/snapshots, cancellation/teardown safety, budget admission, output parity | Default-off implementation qualified and9c82b0f CI green; owned benchmark opt-in7cbd207 deployed with all151 source hashes matched; see live failure boundary below |
| Control admission | Admit catalog/backfill/index/accounting ownership before WAL; genuine budget rejection must not commit or fence; recovery and all acknowledged state preserved | Live failure also reproduced by an unchanged native-independent baseline test; repair remains unqualified; work paused at a safe checkpoint |
| Sustained comparison | Matched declared resources/durability; repeated single/batched and mixed hot/cold/maintenance workloads; exact conservation; throughput, latency, failures, backlog and resource evidence | New native comparison failed before measurement; both retained datasets passed exact checks after upgrade and recovery; no new performance result |
| Release checkpoint | Source-bound tests, direct probes, strict lint/format, independent review, exact committed artifact scope, commit and push verification | 192562d,9c82b0f and7cbd207 pushed; hosted CI green; live control-admission defect blocks the next performance/release gate |

Prior baseline (before native reuse): four writers, database limits 2 CPUs/~2 GB. Single rows: Varve291.82 vs Timescale699.82 rows/s, ACK p99 50.19 vs27.89 ms. Batch128:34285.86 vs21520.93 rows/s, ACK p99 28.51 vs54.92 ms. Batched ingestion lasted under one second with64 requests; no sustained/tail-SLA claim. Five default-tier queries had one sample each, Varve60–99 ms vs Timescale1.9–6.0 ms. Matching resource limits does not establish identical disk performance. See [retained runtime evidence](evidence/inside-out-integrated-20260918/raw-safety-runtime/README.md).

No dropped/ambiguous rows were observed in the short conservation checks; that does not establish arbitrary-crash, distributed or production guarantees. Local fsync ACK does not promise S3 durability. Full coordinated storage rollback requires independent monotonic authority.

The [native live follow-up](evidence/performance-safety-20260919/native-live/README.md) preserved an important failure: schema setup committed a control WAL operation before a later derived-index working reservation failed. The engine fenced; the same image/config reopened successfully after a bounded copied-recovery probe. Exact checks passed again for the two retained datasets. Recovery does not fix control admission or make the original HTTP400 a definitive rejection. No limits were raised and no namespaces were deleted.

The [native-independent negative control](evidence/performance-safety-20260919/control-admission-negative/README.md) now reproduces the exact post-commit budget failure on the unchanged baseline. This diagnoses the defect; it does not qualify the candidate repair. At the user's requested wrap-up, the recovered service remains healthy and remote Cargo is idle.

## Execution order

Restore one build sandbox from the existing toolchain checkpoint. Qualify the narrow fence backport first, retaining before/after source gates. Keep subsequent native/query, owner-admission and WAL work isolated until their own gates pass. One Cargo owner, jobs2, configured small profiles, no bundled DuckDB build. Reuse cache; do not erase preserved evidence or mutate unrelated services. Repeat sustained comparisons only on qualified deployed bytes. Commit/push completed work with explicit remaining limitations; never promote a diagnostic or unexecuted patch to a win.
