# v5: reusable execution, manifest v2, 256 MiB derived budget

This candidate **passed the baseline but failed the large case**. It is retained as negative evidence, not a new qualified frontier or production claim.

## Protocol and identity

Same driver file hashes, Python/PostgreSQL versions, TimescaleDB 2.30.0, region and runtime caps as v4. Database containers: 2 CPUs / 2,000,000,000 configured bytes (1,999,998,976 effective); driver: 2 CPUs / 1,000,000,000 configured bytes. Local fsync acknowledgments, PostgreSQL `fsync`, `synchronous_commit` and `full_page_writes` remained on. No synchronous S3 acknowledgment was measured.

All previous Varve configuration limits remained unchanged. This candidate explicitly enabled manifest-v2 derived pages and added a **268,435,456-byte resident/working derived budget**. That new limit is not equivalent to the old encoded-only metadata limit. Source receipt, exact runtime configuration, frozen build inputs and binary hash are included. The driver reused its previous image; its source Docker tag alone is not an immutable image pin.

The first upload omitted the runtime source receipt. It was cancelled while building, reached REMOVED, and measured no workload. The final image embedded the receipt and passed exact source/config, v2 magic, empty-table/zero-sequence and runtime-cap checks before measurement.

## Outcomes

| Case | Result |
| --- | --- |
| `reuse001`: 250k initial + 60s at 5k rows/s, batch 1k, 4 writers, 50 query samples | Passed; 550,000 common durable/fresh rows; all 300,000 mixed rows acknowledged; no drops or ambiguous writes |
| `reuse002`: 1M initial + planned 30s at 20k rows/s, batch 1k, 4 writers, 30 query samples | Failed during Varve initial ingest at the derived working budget; no large query/mixed result or common watermark is claimed |

Baseline initial ingest was 38.4k Varve versus 80.0k Timescale rows/s. These ingest-only figures do not include Timescale refresh/ANALYZE; consult the raw report for readiness phases. Mixed ACK p95 was 125.24 versus 38.77 ms; mixed read p95 was 64.26 versus 16.02 ms. This is one pilot, not a backend saturation estimate or a controlled causal speedup over v4.

## What the new measurements showed

- During the baseline, the actual DuckDB pool recorded 15 spawns and **569 reuses for 584 executions**, rather than merely keeping a network connection open.
- At the baseline boundary, logical metadata was 8,741,718 bytes, published control root 9,633 bytes, referenced derived pages 8,783,545 bytes, and conservative resident derived charge 36,577,886 bytes. The small root does **not** mean the state became out-of-core or disappeared.
- At the failed large boundary, derived resident charge was 140,640,681 bytes and outstanding working charge was zero. No cgroup OOM occurred. A cloned catalog plus replacement indexes and page workspace requires roughly **twice resident derived charge plus page workspace** for checkpoint publication. The 256 MiB budget cannot cover that peak.
- More importantly, live append admission did not preserve that future checkpoint headroom. Accepted state could become uncheckpointable without a larger budget even after concurrent queries drained. This is a liveness defect, not a reason to silently widen a limit; its regression/fix must qualify separately.
- The after-baseline and after-failure metric snapshots were taken **163.74 and 180.76 seconds after their reports finished**. Counter deltas therefore include untimed idle/background work. In particular, the large-case delta captures checkpoint retry churn after ingest had already failed; it is not an exclusive timed-ingest cost. Histogram intervals also overlap, so sums are neither exclusive CPU time nor an additive wall-time profile. `derived_working_bytes` is a point-in-time gauge, not a peak.

The v5 source passed 291 Rust and 19 TypeScript local checks plus strict lint/format before cloud measurement; one live-S3 test was ignored. Those checks did not expose this high-resident checkpoint boundary. A subsequent fix is not covered automatically by that source qualification.

## Preservation and cleanup

Both complete driver reports, logs, launch arguments, all raw latency arrays/oracles, untimed metrics, resource observations and witness scripts are retained. Source inputs are byte-verified against the embedded receipt; artifact checksums are in `MANIFEST.json`. Exact known benchmark credential values were scanned out before publication. This is not a claim of exhaustive secret detection.

All three exact benchmark deployments were observed REMOVED, with zero active benchmark deployments. The original service remained unchanged and successful; retained benchmark volumes were not deleted. There is **no actual-restart qualification for this failed candidate** and no durability claim for a common frontier in its partial large ingest. Keep the Experimental label.
