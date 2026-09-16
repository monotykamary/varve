# Candidate v3 — retained Railway evidence

Single-trial, matched-cap comparison on 2026-09-16. Both databases: 2 CPU / 2 GB; driver: 2 CPU / 1 GB. All actual runtime regions are Singapore. Database durability remained local fsync; S3 was not exercised.

- `frontier001`: **passed**, 550,000-row common watermark. Initial ingest: Varve 39.6k vs Timescale 107.4k rows/sec. Columnar selective p95: 69.85 vs 1.53 ms (50 samples each).
- `frontier002`: **overloaded**, not passed. One million initial rows ingested successfully. The paired 20k-row/sec generator offered 600k mixed rows: 402k acknowledged, 198k dropped before submission, zero failed/ambiguous. The 1,402,000-row common watermark and query oracles verified. Varve mixed ACK p95 1,216 ms versus Timescale 51.25 ms. This paired queue does not independently establish Timescale's saturation limit.
- Same-volume restarts preserved both watermarks: **1,952,000 rows total per backend**, exact raw/aggregate statistics and timestamp sums. PostgreSQL start time and both native process identities changed. This is not a volume-loss, power-loss or S3 recovery drill.
- Pre-restart cgroup lifetime peaks: Varve 739,856,384 bytes; Timescale 974,266,368; driver 60,182,528. No OOM kills. These are lifetime peaks, not per-phase RSS measurements.

`source.tar.gz` contains the exact 43 staged source files plus `SOURCE_MANIFEST.json`. The source manifest SHA-256 is `64ca1e2e5db4f9358a4555b878f150a9a779a79bd5ffbd474a7ed54aa4915f86`. Runtime binary/config hashes, driver source and witness programs are retained. The manifest records a dirty source snapshot over its Git base; it is not a claim that the base commit alone produced the binary.

`MANIFEST.json` checksums every retained evidence file except itself. Exact deployment credential values were checked absent before publication. Timestamp-sum witnesses normalize PostgreSQL's decimal zero suffix using exact Decimal/integer equality, never floating point.

The small empty-database diagnostic in `runtime-varve.json` is not a workload/tail benchmark. DuckDB detected two threads correctly; bare SELECT 1 took 25.8–29.9 ms in five observations, configured CLI 30.6–45.3 ms and loopback HTTP 34.2–41.8 ms. It does not attribute the full workload gap to subprocess creation.

Do not attribute differences from earlier deployments solely to code: this is not a repeated, counterbalanced same-host A/B experiment. Varve is not at a performance frontier and Experimental labels remain. Subsequent maintenance fixes are candidate v4, not these measurements.
