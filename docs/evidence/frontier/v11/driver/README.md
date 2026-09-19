# Varve / Timescale bounded comparison

This directory is a synthetic capability benchmark, not a production certification or evidence that either database fits an unsupplied user workload. Provision isolated services in one Railway region: 2 CPU / 2 GB for each database and 2 CPU / 1 GB for this generator. TimescaleDB must come from an official Timescale image with the extension already installed. The runner never creates or drops the extension, changes system settings, deletes data, or operates outside its new per-run namespace.

The image intentionally idles so both databases can become ready before execution. `/results` is created and owned by the unprivileged runtime user; it works as ephemeral writable storage without a third volume. Copy reports out before stopping or redeploying the driver. The same live driver can retain the report while only the two databases are restarted for the verification drill.

```sh
docker build -t varve-timescale-bench .
# Railway SSH, after both databases are ready:
python benchmark.py --run-id run001 --output /results/run001.json
# After restarting both databases:
python benchmark.py --verify-only /results/run001.json
```

Runtime credentials are accepted **only** through:

- `VARVE_URL`, `VARVE_API_TOKEN`
- `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`

Do not put credentials in arguments. Reports and progress events omit connection fields and redact credential values from errors. Varve uses one persistent `aiohttp` session. Timescale uses persistent psycopg 3 async connections and transactional `COPY` batches; a receipt row is committed in the same transaction so stable batch IDs deduplicate retries. The runner performs no automatic retry after an ambiguous acknowledgement.

The baseline records the supplied official full/non-OSS TimescaleDB 2.30.0 / PostgreSQL 17 image digest `sha256:3113d12b78392c064aa7475caf7a52b447b29ddd4f9bfd23526733fcb03e3459` and the supplied Varve benchmark limits (10,000 maximum configured batch rows, 128 MiB hot, 64 MiB metadata, 256 MiB/query worker, two query threads, two workers, 30 seconds). Runtime preflight independently records actual database versions and requires `fsync`, `synchronous_commit`, and `full_page_writes` to be on. Timescale receives an idiomatic `(tenant, series, ts DESC) INCLUDE (value)` index and `ANALYZE`.

Defaults are 250,000 initial rows, 1,000 rows/batch, 4 writers, 50 timed samples per query, a 60 second mixed phase at 5,000 offered rows/s, and a 1,200 second outer ceiling. Hard maxima are enforced by `--help`. Encoding/generation time is reported separately from acknowledgement latency. The mixed phase records intended-arrival queue delay, end-to-end latency, drops, failed/ambiguous batch rows, and offered/acknowledged rows. Queue drops produce an `overloaded` report and nonzero exit; unavailable/failed Timescale conversion produces `partial_unsupported` and nonzero exit rather than a fabricated columnar result.

The deterministic quarter-valued dataset uses 1,024 series, four tenants, empty tags, and a minute-aligned base about 24 hours old. Python integer oracles independently verify count/sum/min/max, tenant groups, selected series groups, minute buckets, and a window query. Varve's 60-second built-in aggregate is eager. Timescale's native continuous aggregate is explicitly refreshed at every comparison barrier; ingest acknowledgement and durable-data-plus-fresh-aggregate times are reported separately.

The first suite compares Varve's default automatic tiering with Timescale rowstore, and records the observed hot-row/segment counts. It is not a hot-only Varve claim: the configured thresholds and scheduler can flush rows before the first query. Freshness elapsed time includes each backend's immediate barrier (including Timescale ANALYZE), not a delayed sum that hides intervening work. The runner then explicitly checkpoints and compacts its Varve table and selects a Timescale columnstore/compression path by Timescale procedure introspection. It records the exact path/configuration. If conversion is unavailable or fails, the columnar suite is marked unsupported/failed rather than reported as a pass. No cache-dropping, S3, or local Parquet “cold disk” claim is made.

Oracle preparation runs off the event loop with cooperative deadline/cancellation checks, so a large fixture cannot starve pooled-connection cleanup. Redacted exception-group leaves and explicit causal chains are retained with cycle detection, a 16-description cap and a 2,000-character output cap. Failed mixed runs preserve completed acknowledgement/read/generation samples; those truncated tails are diagnostic, not successful-run latency claims. No retries, pacing, query or acceptance rules change on this reporting path. A failed initial ingestion does not reconstruct every in-flight acknowledgement; distinguish observed stored rows from known acknowledgements. Mixed writes share a paired queue, while reads are closed-loop, so neither is an independent database-capacity or fixed-query-QPS SLA measurement.

The 21 offline tests require the pinned runtime dependencies but make no network connections:

```sh
python -m unittest discover -s tests -v
python -m py_compile core.py benchmark.py
```

Synthetic data is retained. After copying reports, stop only the owned deployments; retain volumes unless their owner authorizes deletion. Retained volumes can still incur storage charges. Do not teach this runner to drop relations.

The completed Railway run, retained failures and recovery evidence are documented in [TIMESCALE_BENCHMARK.md](../../docs/TIMESCALE_BENCHMARK.md).
