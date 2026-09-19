# Reviewed raw-safety runtime — 2026-09-18

**Source-bound short cloud smokes, not sustained-capacity or production qualification.**

Deployment `cb0fca35-a170-4192-a4ba-d43b41a75443` reached `SUCCESS`. All **128 qualified source paths** from `../raw-safety/` match the deployed **148-entry runtime manifest** after path normalization. The qualified manifest digest is `13e9198e00e6d11a26c89def8fd775a2a326236f7bd844cfdef288883e4441c0`; runtime-manifest file hashes need not equal that subset digest.

Both database containers report `cpu.max = 200000 100000` and `memory.max = 1999998976`, before and after. Deployment identities stayed unchanged and recorded OOM counters are zero. Matched limits are not a claim of identical physical storage performance. Post-run snapshots include idle time and the disposable-file diagnostic, so they are not isolated workload CPU/I/O deltas. No local build or load test was run.

## Measured results

Four writers, unchanged driver/durability/profile, separate initial-ingest phases:

| Initial ingestion | Varve rows/s | Timescale rows/s | Varve ACK p50 / p99 | Timescale ACK p50 / p99 |
| --- | ---: | ---: | ---: | ---: |
| 1,024 single-row requests | 291.82 | 699.82 | 10.88 / 50.19 ms | 4.32 / 27.89 ms |
| 8,192 rows, batches of 128 | 34,285.86 | 21,520.93 | 11.84 / 28.51 ms | 20.36 / 54.92 ms |

The batched rate is **1.59×** Timescale in this short observation. Varve still loses single-row throughput. The batch run has only 64 request samples and lasts under one second; its tail statistics are descriptive, not a latency-SLA qualification.

Both two-second mixed smokes passed unchanged conservation/scheduling checks with zero dropped, pending, failed or ambiguous rows. Offered load was only 25 rows/s for single rows and 128 rows/s for batches: **50 and 256 total mixed rows**, respectively. These are correctness smokes, not capacity wins.

Queries still trail Timescale. There is **one recorded sample per query**, not a meaningful percentile distribution. In the batch-128 default-tier stage, the five queries took approximately **60–99 ms** in Varve versus **1.9–6.0 ms** in Timescale. `summary.json` retains both query stages and both runs without raw-sample noise; full reports remain alongside it.

## Complete conservation checks

- Previous `ior6_smoke_04` dataset after the binary upgrade: **8,448 raw rows / 4,352 minute groups** matched.
- New `io13_b1_01`: **1,074 raw rows / 1,074 minute groups** matched.
- New `io13_b128_01`: **8,448 raw rows / 4,352 minute groups** matched.

The supplemental verifier checks every deterministic identity, exact quarter value, empty tags, multiplicity, and minute count/sum/min/max group. It excludes tie-dependent first/last/OHLC. It performs no writes or refresh and does not promote benchmark performance. Source/report hashes are embedded in each exact report; the unchanged workload and verifier source are retained in `../driver-smoke-04/` and `../review-0918-next/`.

## Where single-row time went

`b1-phase-deltas.json` uses the existing `after_isolated_setup` → `after_initial_drain` snapshots, not another load run:

- 1,024 completed requests / **429 physical groups**: 2.39 requests per group.
- WAL-write span: **2.5599 s**, approximately 5.97 ms/group.
- Group preparation: **0.0493 s**; WAL encoding: **0.0102 s**; installation: **0.0158 s**.
- Initial Varve ingestion wall time: **3.5090 s**.

The WAL-write span includes more than the sync syscall. Nested metric spans overlap; do not sum them into a wall-time breakdown or subtract cumulative maxima. Parallel preparation alone cannot remove this observed WAL cost. Ingestion delay/group/trace environment overrides were unset; source defaults apply, including the 2 ms grouping delay.

## Disposable-file sync diagnostic

`wal-sync-probe.py` ran as the Varve service UID on its Railway volume, outside database files. It used 64 rotating-order writes of 1 KiB per mode, at most **256 KiB** of fixture data. Preallocated files were fully zero-written and synced before timing. All four files passed exact readback and were removed; the temporary script was also removed.

| Mode | Mean sync time |
| --- | ---: |
| Growing append + fsync | 4.591 ms |
| Growing append + fdatasync | 4.317 ms |
| Preallocated overwrite + fsync | 4.026 ms |
| Preallocated overwrite + fdatasync | 0.180 ms |

Read-only PostgreSQL settings confirm `fdatasync`, 16 MB WAL segments, zero initialization/recycling enabled, and `fsync`, `synchronous_commit`, `full_page_writes` enabled. No database setting or durability barrier was changed.

This isolates a promising filesystem mechanism, **not** a Varve WAL implementation, crash test, PostgreSQL filesystem measurement or database speedup. Preallocation must preserve committed-prefix authority, fail-closed corruption handling, logical versus physical offsets, rotation, disk quotas and recovery. A simple `sync_all` → `sync_data` substitution did not reproduce the preallocation benefit in this diagnostic. Production changes remain subject to implementation, recovery tests and independent review.

A later `two-barrier-sync-probe.py` diagnostic retained **two ordered data-sync calls** per operation: frame write/sync, then an opaque 128-byte marker-stub write/sync on alternating aligned pages. It is explicitly **not a WAL encoding or recovery prototype**. All 64 operations passed full-file readback and cleanup of the **76 KiB** fixture. Total observed mean was **1.237 ms**, median **0.099 ms**, p95 **14.916 ms**, maximum **18.535 ms**. The marker-sync phase had substantial outliers. This is not a matched comparison with the earlier probe and does not establish a tail-latency win. Initialization remained outside timing; resource after-snapshots predate this later diagnostic.
