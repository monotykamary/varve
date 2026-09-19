# Frontier v10 — scoped catalog pilot, mixed workload failed

**No win. No completed baseline/stress pair. No recovery qualification.** The S12 candidate completed its initial ingest and all static query checks, then the 5k-row/s mixed baseline failed after 47.50 seconds. Stress and the planned restart checks did not run. All three benchmark deployments were removed; the original application and retained volumes were verified unchanged.

## Candidate and controls

S12 omits only catalog rows proven unreachable by the SQL AST in retained storage-only queries, preserving full schema/aliases and conservative fallback. It adds eight fixed timing phases (30 total) plus actual non-raw worker-install count/byte counters. It does not remove disk locks, change WAL semantics, alter batching, tune residency budgets or change tiering age.

The runtime profile is byte-identical to v9. Database caps remain two CPU / 2 GB; the driver two CPU / 1 GB, same Singapore region, same driver bytes/Python/PostgreSQL/TimescaleDB 2.30.0 and DuckDB v2. Timescale fsync, synchronous_commit and full_page_writes were verified on. Acknowledgements are local durable commits, not synchronous S3 replication or HA. Resource caps are not a claim of dedicated physical hardware.

## Observed result

| Metric | Varve | Timescale |
| --- | ---: | ---: |
| Initial durable ingest, rows/s | 45,859 | 47,246 |
| Partial mixed write-ack p95, ms | 89.58 | 132.08 |

Varve's initial throughput is about 2.9% lower. Partial mixed tails are from a failed, truncated interval and **cannot establish an advantage**. Of 238,000 rows offered before abort, the driver recorded 237,000 paired acknowledgements, zero queue drops and 1,000 failed/ambiguous rows. Its reported common watermark is 487,000 rows including the initial 250,000. This is the driver's acknowledgement ledger, **not a recovered-data verification**. Ambiguity does not establish data loss or identify which backend committed the last batch.

All 1,000 timed static queries passed their result oracles. The complete raw report retains every static latency distribution. Broad group/count cells favor Varve; selective/aggregate/window results remain mixed or losing. Timescale timings differ substantially from v9, so cross-campaign changes cannot be attributed to this code alone. Concurrent-read samples are absent: the unchanged driver drops those arrays from its failure report.

## Failure evidence and limits

The recorded cause is `MixedWorkloadError` wrapping `unhandled errors in a TaskGroup (1 sub-exception)`. The driver's failure path serializes the exception group's summary rather than its nested causes. Existing backend logs do not recover that missing leaf exception. The root cause is **not established**.

Varve boundary metrics record zero HTTP application errors, request timeouts, connection rejections or ingest rejections; one HTTP connection-lifetime timeout; 487 submitted/completed ingest requests; and no OOM events. A transport/connection-retirement failure is a hypothesis, not a demonstrated cause. The next diagnostic must retain nested failures and partial read samples without changing scheduling, retries, acceptance thresholds or workload semantics.

The local formatter also assumed the successful report's read-latency fields and crashed on this failure report. It had already saved the raw report, and the controller captured after-baseline metrics **before** formatting. The controller then performed scoped abort cleanup, verified zero active benchmark compute and did not launch stress. Neither failure is omitted from this record.

## Phase attribution

The 102.85-second observation window includes boundary delay and maintenance. Timers overlap; do not sum them as exclusive CPU/wall time. Cumulative disk-lock hold was 13.92 seconds, state-lock hold 13.77, WAL write 6.27, WAL sync 6.19, checkpoint preparation 4.89, query execution 4.04, query build 3.86, root preparation 3.43 and group preparation 2.73. Disk-lock wait was 0.322 seconds and WAL-specific disk wait 0.106; derived verification only 0.052 seconds. These observations do not support assuming disk-lock wait or reused-page verification dominated this interval.

Workers recorded 16 full raw loads, 3 deltas, 548 raw unchanged-input hits, 9 non-raw installs / 8,342 logical bytes, and 1,466,705 raw staged rows / 268,120,952 logical bytes. These whole-interval counters do not isolate individual query patterns. Observed cgroup memory peak was 425,058,304 bytes, with zero OOM events; logical gauges are not RSS bounds.

## Pre-workload monitor incident

The first source upload succeeded once. The captured shell override ignored the requested background option, so the first local monitor was terminated by the executor deadline while Varve was still building. A second monitor attempt was refused by the existing-start guard; its output redirect overwrote the first monitor's log. That initial log is unavailable. No database workload or comparator/driver redeployment had begun.

After checking the old PID was dead and all pre-workload transition receipts absent, the identical controller resumed through explicit shell-level detachment. It did not rebuild or rerun a workload. Both the initial incorrect upload-timeout inference and the corrected incident record are preserved. This incident is separate from the later mixed-workload failure.

## Source qualification and reproduction

The exact candidate passed **400 Rust checks + 19 TypeScript checks**, strict formatting/workspace Clippy and independent review; one live-S3 test remains ignored. Eighteen unchanged-source TypeScript unit checks reuse their verified prior log; the actual service check reran. The 85-input source digest is `572bc1423656e266d81c8fe7ce1388e0ed8954e40277d5537b2cf6cb9090731c`. The 55-file build and all locally qualified inputs are archived. Local qualification did not predict or certify remote success.

`MANIFEST.json` binds the artifacts; known credential values were checked before copying. Run `node docs/evidence/frontier/v10/witness/verify.mjs` from the repository root to verify stored hashes, source archives, raw latency arithmetic, phase deltas and scoped cleanup without running database load. Further diagnostics must preserve this failed attempt, not overwrite or reclassify it.
