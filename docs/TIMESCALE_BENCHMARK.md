# Railway Timescale comparison

**Completed bounded evaluation, not production qualification.** The 550,000-row baseline and database-restart checks passed. A larger run stopped at Varve's configured projected-checkpoint admission budget. Timescale was faster in this trial. All three temporary deployments are confirmed removed; two test volumes remain.

## Acceptance ledger

| Check | Evidence / outcome |
| --- | --- |
| Isolate local activity and existing services | Three new private Railway services in Singapore; generation and timing ran remotely; existing `varve` service unchanged |
| Match resource caps | Both databases: 2 vCPU / 1,999,998,976-byte observed memory cap; generator: 2 vCPU / 999,997,440 bytes |
| Match acknowledged durability | Varve `local_fsync`; logged PostgreSQL with `fsync`, `synchronous_commit`, `full_page_writes` on; S3 disabled |
| Independent deterministic answers | Integer-quarter count/sum/min/max, group, bucket and window oracles; all baseline timed query samples passed |
| Columnar and mixed workload | Both baseline query stages passed; 300,000 late rows offered at 5,000/sec, zero drops or failed/ambiguous writes |
| Preserve failure and capacity evidence | Four attempts retained, including transport failure, generator failure and projected-checkpoint rejection |
| Recovery | Both databases restarted; baseline retained-oracle verification and per-backend raw/aggregate fingerprints passed without refresh |
| Cleanup | Exact three deployment IDs `REMOVED`; original service unchanged; two database volumes retained and may still incur storage charges |

## Reproducible setup

The runner and commands are in [`benchmarks/timescale/`](../benchmarks/timescale/README.md). App configuration is [`varve-config.json`](../benchmarks/varve-config.json); [`varve-service.json`](../benchmarks/varve-service.json) is Railway API input, not deprecated config-as-code.

- TimescaleDB **2.30.0**, PostgreSQL **17.11**, official full image digest `sha256:3113d12b78392c064aa7475caf7a52b447b29ddd4f9bfd23526733fcb03e3459`.
- DuckDB **v2.0.0-alpha41533**, commit `10de957379`.
- Varve: 128 MiB hot budget, 100,000 hot rows, 64 MiB metadata, two query workers with 256 MiB/two threads each. Single owner; no S3 durability in this test.
- PostgreSQL: 512 MiB shared buffers, 32 MiB work memory, two parallel workers per gather; transactional COPY plus receipt rows, covering tenant/series/time index, ANALYZE and explicit aggregate-freshness barriers.
- Baseline: 250,000 initial rows, batches of 1,000, four writers, 50 timed samples/query, then 60 seconds at 5,000 offered late rows/sec. One excluded warmup per query. Generator UID/GID **10001**, separate 20-minute hard ceiling.
- Larger case: 1,000,000 initial rows, same batches/writers, 30 samples/query and a planned 30 seconds at 20,000 offered rows/sec; separate 10-minute hard ceiling. It failed during initial ingestion, so its query and 20,000/sec phases were **not reached**.

The first suite is **Varve automatic tiering versus Timescale rowstore**, not hot-only Varve. Automatic Parquet flushing was already active. The second explicitly checkpoints/compacts Varve and converts Timescale chunks through introspected native columnstore procedures; actual commands are retained. Neither suite drops caches or measures cold-disk/S3 behavior.

## Passing baseline: `railway002`

2026-09-16, 09:18:49–09:21:36 UTC. Both backends independently verified **550,000 committed rows**: 250,000 initial plus 300,000 late/out-of-order rows.

| Initial ingestion | Varve | Timescale |
| --- | ---: | ---: |
| Acknowledged rows/sec | 41,560 | 52,998 |
| Durable data plus fresh aggregate | 6.016 s | 5.880 s |

Freshness includes each backend's immediate barrier, including Timescale ANALYZE and refresh. Encoding/transport implementations differ; these are client-observed application paths, not isolated engine-kernel timings.

Query **p95 milliseconds**, 50 timed samples per cell:

| Query | Varve automatic | Timescale rowstore | Varve checkpointed | Timescale columnstore |
| --- | ---: | ---: | ---: | ---: |
| Recent tenant/series/time filter | 136.92 | 5.98 | 148.01 | 7.82 |
| Full count/sum | 141.83 | 30.83 | 148.30 | 26.50 |
| Cross-series grouping | 154.70 | 46.06 | 192.20 | 43.75 |
| Minute continuous aggregate | 150.53 | 10.75 | 166.22 | 6.43 |
| Window top five | 295.80 | 8.59 | 298.53 | 7.82 |

During mixed load, all **300,000 offered rows** were acknowledged on both sides, with **zero drops and zero failed/ambiguous rows**. Elapsed writer/drain time was 60.179 seconds. Batch acknowledgement p95 was **721.15 ms Varve / 94.85 ms Timescale**. Concurrent stable-watermark read p95 was **1,098.52 ms / 35.58 ms**, with 38 read pairs.

The mixed generator pairs writes to both backends. Shared queue delay/end-to-end latency is not either database's independent maximum capacity. Reads and isolated query samples are closed-loop; they do not establish fixed-query-QPS tail SLAs. This is one completed baseline, not repeated-trial statistical evidence.

## Failures retained, not tuned away

### `railway001`: active HTTP connection retirement

Both backends acknowledged and verified 250,000 rows; first-tier queries and conversion completed. Varve then returned `ServerDisconnectedError`. The server wrapped the entire Hyper connection in a **65-second** timeout, dropping active requests at expiry.

`src/service.rs` now retires keepalive gracefully and bounds response draining by request timeout plus header timeout. `connection_lifetime_drains_an_active_write` failed against the old code, then passed with a complete fsync receipt and durable reopen. All **28 affected security/service/transport tests passed**. The timeout and no-retry policy were not relaxed. The failed report remains evidence, not a completed performance result. Successful active-request draining and reopen are tested; the forced cutoff for a stalled reader is enforced in code but still lacks a dedicated slow-reader regression.

### `railway003`: generator event-loop starvation

The first million-row attempt wrote **zero rows** to either database. Synchronous oracle construction blocked the event loop long enough for an idle pooled connection to close. A remote read-only probe reproduced `ServerDisconnectedError` after 8.09 seconds of blocking preparation; offloaded preparation took 6.82 seconds and the same query succeeded.

Oracle preparation now runs off-loop with cooperative cancellation/deadline checks. Redacted exception-group leaf causes are retained instead of only the generic group wrapper. **17 offline runner tests passed.** This was a harness defect, not database capacity evidence; the same workload was then rerun.

### `railway004`: projected-checkpoint admission headroom

Timescale loaded all **1,000,000 initial rows**. Varve rejected writes with:

```text
Varve HTTP 400: projected checkpoint exceeds metadata byte budget
```

A subsequent read observed **403,000 rows** in Varve's new table. This is a persisted observation, **not a reconstructed acknowledgement ledger**: cancellation can leave in-flight outcomes unknown. The run did not reach the larger query or 20,000/sec phases.

The guard reserves approximately `projected_catalog_bytes + 512 * prospective_hot_rows` against the configured **64 MiB** metadata budget. It is not simply a check of current stored metadata. After automatic maintenance, status showed **28,658,011 metadata bytes**, zero hot rows, 70,656 rollup groups, no fence and no maintenance error. The Varve cgroup recorded zero OOM events and a 482,115,584-byte lifetime peak before restart.

Both databases already retained **800,000 rows from earlier attempts**; the empty third namespace also remained. This is not a clean-database claim that Varve can hold only 403,000 rows. The result exposes conservative checkpoint reservations and their interaction with the 100,000-hot-row threshold and growing catalog. Limits were not raised to manufacture a pass.

## Recovery and cleanup

Both database services were restarted without rebuilding or deleting volumes. [`railway002.verify.json`](evidence/timescale/railway002.verify.json) passed retained-oracle verification at 550,000 rows **without refreshing aggregates**. Before/after fingerprints also matched each backend's observed partial-stress state: raw count/sum/min/max/exact timestamp sum and aggregate count/sum/min/max. PostgreSQL's process-start timestamp changed.

These are process-restart/persistent-volume checks, not lost-volume/S3 recovery, a cryptographic full-row equality proof, or an RTO SLA. All three exact benchmark deployment IDs are now `REMOVED`; the original service was unchanged. Only temporary compute was stopped. **The two test volumes remain and may incur storage charges.**

## Evidence and next gates

[`evidence/timescale/`](evidence/timescale/README.md) contains raw reports/samples, source manifests, driver artifact hashes, platform resource series, the preparation repro, observed stress state and recovery/removal proofs. [`MANIFEST.json`](evidence/timescale/MANIFEST.json) records SHA-256 digests. Actual runtime credential values were checked absent before copying.

Source v1 manifest: `15aa3f5887a830943eb516d26a3f4d53fe963634148224bcb3baaa8e2cd8a61f`. Corrected v2: `18e4a1311f6b12181557e95118b5db7e8c56ab8612f93ecc82627c0cd22b7f4c`. Effective config: `135b331eb502807a6a5be13524dbfe07ba7aab25d29338066f097ee1328e3ac6`. Manifests were copied into the images and checked at runtime.

Railway caps are not exclusive physical hosts. Metrics use coarse 30-second buckets, can have zero-filled edges, and cannot attribute per-query CPU. Volume gauges include filesystem overhead, not just table bytes. PostgreSQL receipt IDs do not implement Varve's payload-fingerprint conflict contract; no automatic write retry is performed.

Immediate priorities are **checkpoint-headroom-aware admission/flushing** and measuring **persistent isolated query workers/columnar handoff** against the current process-per-query/NDJSON boundary. Preserve durability, cancellation, worker isolation and conservative admission proofs. Do not attribute every latency difference to process startup without profiling.

This bounded memory-sized fixture does not establish multi-day stability, larger-than-RAM operation, HA, S3 recovery or compatibility with an actual user schema. The cold-reader/expiration/remote-vacuum risk identified during this trial is tracked in [TIMESCALE_READINESS.md](TIMESCALE_READINESS.md). The [subsequent frontier work](FRONTIER.md) fixes and regression-tests that single-node race and grouped checkpoint-headroom admission. [Candidate v3 evidence](evidence/frontier/v3/README.md) retains the next comparison, overload and actual restart results separately; this historical report is not rewritten into a pass.
