# Frontier v11 — complete S12 diagnostic, still no win

The baseline completed; stress overloaded the paired offered-load queue. **No frontier win or production qualification.** Both actual database restarts retained matching raw, aggregate and exact-timestamp fingerprints for **2,047,000 committed rows per backend**. Cleanup verified zero active benchmark deployments; volumes and the original application were preserved.

## Same database, explicit diagnostic-only driver change

The database binaries/source, DuckDB v2, runtime configuration, budgets, tiering ages, two-CPU/2-GB database caps and two-CPU/1-GB driver cap are unchanged from v10. Exact image reuse and fresh directories are recorded. Runtime config is the same byte-level SHA256 `215f100b13ba1ba3d012d39117fa1895ed7978657434057a93390c6f359aa089`. TimescaleDB 2.30.0/PostgreSQL 17.11, Python and durability settings were attested. These are single-node local durable acknowledgements, not synchronous S3 replication or HA. Caps do not imply dedicated physical hosts.

The base driver remains archived separately. The effective driver changes only error reporting: preserve the original nested exception as a cause, redact bounded causal graphs safely, and retain completed read/generation samples on failure. No successful-workload scheduling, SQL, retry, limit or acceptance rule changed; a full-module AST normalization and unchanged dependency-byte proof are included. The hash-verified effective copy was installed non-root into its own directory and explicitly attested at both launches. This is **not** a claim that driver bytes are identical to v10.

Twenty-one fake-client/offline driver tests passed. Independent review found no demonstrated blocker and two low test-coverage gaps; both were closed without changing the reviewed production driver bytes. The new regression exercises actual mixed failure through run_fresh, main, persisted/emitted redaction, failed exit and cleanup. The old v10 failure did not reproduce in this run; that does not establish its cause or that the database defect, if any, is fixed.

## Results

| Metric | Baseline Varve / Timescale | Stress Varve / Timescale |
| --- | ---: | ---: |
| Initial durable rows/s | 63,876 / 91,640 | 50,337 / 88,973 |
| Mixed acknowledgement p95, ms | 51.65 / 33.72 | 77.81 / 44.92 |
| Concurrent read p95, ms | 259.93 / 21.26 | 107.07 / 23.85 |

Baseline acknowledged all 300,000 offered mixed rows with no drops or ambiguity. Stress acknowledged 497,000 of 600,000 offered rows and dropped **103,000 before admission**, with zero failed/ambiguous rows. This paired queue does not identify either database's standalone saturation frontier. All static query checks passed; every latency cell/raw sample is retained below. Broad counts and default-tier groups favor Varve, but selective/aggregate/window patterns still lose and the stress checkpointed group also loses. Cross-campaign differences are not isolated causal speedups; at least three fresh counterbalanced trials and every target gate remain necessary.

## Attribution, not exclusive CPU accounting

The baseline/stress observation windows include boundary delay and maintenance; timers overlap and must not be added as exclusive CPU/wall time. Stress records 28.80s state-lock hold, 13.88s WAL sync, 7.26s grouped planning, 18.96s root preparation, 10.17s derived publication, 1.56s disk-lock wait and 0.54s WAL-specific disk wait. Query build/execution total 3.13s/4.09s across 356 observations. These do not prove which operation caused an individual latency tail.

Stress records 23 full raw resident loads, one delta, 332 raw unchanged-input hits, 1,169,882 raw staged rows / 212,379,272 logical bytes and 8 non-raw installs / 35,367 logical bytes. Baseline records 20 full loads, five deltas and 2,139,299 raw staged rows. The measurements do not support assuming unreachable catalog reloads, shared disk waiting or reused-page verification dominate remaining costs. Observed cgroup memory peak was 860,098,560 bytes with zero OOM events; logical counters are not RSS bounds.

## Preserved pre-workload configuration incident

A preceding diagnostic deployment was halted by the strict runtime-config hash guard before any workload or effective-driver installation. Its helper compacted JSON while attestation expected the original pretty-printed bytes. Parsed settings were equivalent; the startup command writes the environment payload verbatim. The prior receipts, error, original helper and verified cleanup remain under `config-preflight/`.

The corrected attempt uses new directories and run IDs, preserves the original bytes, and checks the actual trimmed environment payload hash before deployment. Negative fixtures exercise that exact readiness source and reject semantically equivalent compact JSON. The strict runtime guard was not weakened. The corrected baseline and stress ran uninterrupted; there was no database restart between those workloads. Later restart checks are separately recorded.

## Source, qualification and reproduction

The same S12 database source passed **400 Rust + 19 TypeScript checks**, strict formatting/workspace Clippy and independent review. One live-S3 test remains ignored; 18 unchanged-source TypeScript unit checks reuse their verified prior log and the real-service test reran. Its 85-input digest remains `572bc1423656e266d81c8fe7ce1388e0ed8954e40277d5537b2cf6cb9090731c`. The exact 55-file build and locally qualified source archives are reused byte-for-byte from v10, not rebuilt or falsely requalified by a different run.

`MANIFEST.json` binds every artifact, and known credential values were checked before copying. With Node, Python 3.11+ and tar installed, run `node docs/evidence/frontier/v11/witness/verify.mjs` to check source/driver archives, workload arithmetic, raw percentiles, phase deltas, actual restart evidence and scoped cleanup without database load. Read-only structural review tools are not correctness or performance certification.

## All static query p95 cells (milliseconds)

| Run | Stage | Pattern | Varve p95 | Timescale p95 |
| --- | --- | --- | ---: | ---: |
| diag101 | columnar_checkpointed | cross_series_group | 13.04 | 35.37 |
| diag101 | columnar_checkpointed | full_count_sum | 7.93 | 17.27 |
| diag101 | columnar_checkpointed | minute_continuous_aggregate | 13.38 | 1.11 |
| diag101 | columnar_checkpointed | recent_series_time_filter | 6.20 | 1.68 |
| diag101 | columnar_checkpointed | window_top_five | 22.40 | 1.34 |
| diag101 | default_tiers | cross_series_group | 14.90 | 47.99 |
| diag101 | default_tiers | full_count_sum | 10.17 | 27.26 |
| diag101 | default_tiers | minute_continuous_aggregate | 11.14 | 1.94 |
| diag101 | default_tiers | recent_series_time_filter | 6.97 | 4.02 |
| diag101 | default_tiers | window_top_five | 24.47 | 2.61 |
| diag102 | columnar_checkpointed | cross_series_group | 47.10 | 32.79 |
| diag102 | columnar_checkpointed | full_count_sum | 15.51 | 32.46 |
| diag102 | columnar_checkpointed | minute_continuous_aggregate | 11.76 | 2.83 |
| diag102 | columnar_checkpointed | recent_series_time_filter | 10.87 | 2.09 |
| diag102 | columnar_checkpointed | window_top_five | 42.00 | 1.19 |
| diag102 | default_tiers | cross_series_group | 25.64 | 152.25 |
| diag102 | default_tiers | full_count_sum | 16.52 | 82.38 |
| diag102 | default_tiers | minute_continuous_aggregate | 12.20 | 1.67 |
| diag102 | default_tiers | recent_series_time_filter | 6.31 | 2.41 |
| diag102 | default_tiers | window_top_five | 25.55 | 2.38 |
