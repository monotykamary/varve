# Varve / Timescale bounded comparison

This is a synthetic single-node capability benchmark, not production certification or evidence for an unsupplied workload. Provision isolated services in one Railway region: 2 CPU / 2,000,000,000 bytes for each database and 2 CPU / 1,000,000,000 bytes for the driver. Use fresh volumes and restart policy `NEVER`. TimescaleDB must use the supplied official full image with the extension already installed. The runner never creates or drops extensions, changes system settings, deletes data, or leaves its unique per-run namespace.

Run from the already deployed driver after all services are ready:

```sh
python benchmark.py \
  --run-id run001 \
  --output /results/run001.json \
  --require-rebuilt \
  --attestation '{"candidate_image":"...","candidate_binary":"...","config":"...","resources":"..."}'
```

After Main performs the explicit restart durability drill, run against the same retained volumes and source report:

```sh
python benchmark.py \
  --verify-only /results/run001.json \
  --output /results/run001.verify.json \
  --require-rebuilt \
  --attestation '{"before_after_process_identity":"..."}'
```

`--attestation` accepts a JSON object of at most 64 KiB. It is stored verbatim as **externally supplied, unverified metadata** and is never described as runtime proof. Main owns image, binary, source/config, resource, platform CPU/memory/disk, and before/after process-identity attestations. Timestamped `/v1/status`, `/metrics`, and `pg_stat_database` snapshots plus phase durations are included so Main can join that evidence; the driver does not claim to have measured remote cgroups.

Runtime credentials are accepted only through `VARVE_URL`, `VARVE_API_TOKEN`, `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, and `PGDATABASE`. Reports and progress events omit connection fields and redact credential values from failures. Varve uses one persistent `aiohttp` session. Timescale uses persistent psycopg 3 async connections: one atomic autocommit statement for a single event plus its receipt, and transactional `COPY` for multi-row batches.

## Strict contracts

With `--require-rebuilt`, preflight reads actual Varve `/v1/status` and requires the persisted `segmented_journal` marker plus exact native DuckDB identity: version `v2.0.0-alpha41533`, Linux library SHA-256 `69bdd44e0d2426e7ba44ed14644b54d8bd99a9703e5cbb4aec3f8beef40f817d`, and `duckdb_v2.h` SHA-256 `62ad0df66b9f4193a657540d2429ba23cb32c41c5c98ea9f3f4e7a3b64e30544`. Actual status is retained even when the gate fails. Timescale preflight independently records versions, database identity, and requires `fsync`, `synchronous_commit`, and `full_page_writes` to be on.

Every initial and mixed Varve receipt is checked for exact row count, `duplicate=false`, `durability=local_fsync`, and a positive integer sequence. Normal writes are never retried after an ambiguous acknowledgement. The Timescale-owned receipt relation stores row count and a canonical SHA-256 of the ordered logical payload. For one row, a CTE inserts its receipt and event atomically in one autocommit statement; duplicate-only reads verify the existing immutable receipt digest. Multi-row receipt insertion and `COPY` share one explicit transaction with a locked duplicate check. Both paths reject conflicting payloads and avoid inserting duplicate events.

Before final verification, both backends receive explicit identical and conflicting retries of the same initial stable ID. Identical retries must report duplicate, conflicting retries must be rejected, and raw fingerprints must remain unchanged. Fingerprints contain count/sum/extrema, exact microsecond first/last moments, and persistent database identity. `--verify-only` requires the source fingerprint and identities after restart, repeats the identical retry on both backends, and again requires unchanged fingerprints.

## Supplemental exact verification

Run the shipped read-only verifier before and after an explicit restart, using distinct output files:

```sh
python verify_exact.py \
  --report /results/run001.json \
  --output /results/run001.exact-before.json \
  --max-seconds 600
```

It compares every deterministic raw identity, quarter value, empty tag map and multiplicity, plus every named minute count/sum/min/max group, using sequential bounded pages. JSON text and decoded-object tags must represent the same empty map; malformed or nonempty values fail. It checks database identities and frozen driver hashes, performs no writes or refreshes, and refuses incomplete conservation or existing output paths. Keep the original driver dependencies with the report when source changes. It can diagnose data-complete `overloaded` reports but never promotes their performance verdict. First/last/OHLC equal-timestamp tie equivalence is explicitly excluded.

## Workload and boundedness

The frozen `config/railway-rebuild-benchmark.json` profile remains external to this harness: 128 MiB hot, 64 MiB metadata, 256 MB/query worker (DuckDB units), two query threads/workers, 10,000 rows and 4 MiB per batch, and 200,000 idempotency keys. The harness does not modify it. CLI bounds additionally cap initial plus intended mixed rows at 1,000,000 and intended request IDs at 200,000.

Defaults are 250,000 initial rows, 1,000 rows/batch, four writers, 50 query samples, and a finite 60-second mixed trace at 5,000 intended rows/s. Initial ingest order is hash-counterbalanced; query backend order is recorded and reversed to counterbalance it. Query generation/encoding is reported separately from acknowledgement latency.

Mixed writes use a bounded lossless cursor and backpressure, never queue-full shedding. The complete intended trace remains the denominator through drain. Reports conserve paired acknowledgements, per-backend observed acknowledgements, failed/ambiguous rows, pending rows, never-submitted rows, queue peaks, original intended-arrival latency, and drain time. A missed scheduling gate is `overloaded` with nonzero exit even if the drain eventually stores every row.

Mixed reads have their own finite intended clock, bounded queue, and workers. Their end-to-end arrival latency is distinct from backend service latency; closed-loop samples are not substituted. `--mixed-read-interval`, `--mixed-readers`, and `--drain-seconds` are bounded. Short-run p50/p95/p99 values are observational pilot summaries, not p99 qualification.

The deterministic quarter-valued dataset uses 1,024 series, four tenants, empty tags, and a minute-aligned base about 24 hours old. Independent Python integer oracles verify count/sum/min/max, tenant groups, selected series groups, minute buckets, and a deterministic window. Varve's aggregate is eager. Timescale's continuous aggregate is explicitly refreshed at comparison barriers. Conversion unavailability/failure is non-passing rather than fabricated support. No cache-dropping, S3, distributed, high-availability, or power-loss claim is made.

Failed or cancelled initial, query, and mixed phases retain completed diagnostic samples, bounded error descriptions, and conserved pending/ambiguous work; they never become passes. Synthetic data is retained. Copy reports before stopping services, and do not delete relations or volumes without owner authorization.

## Offline validation

The offline tests require the pinned Python dependencies but make no network connections:

```sh
cd benchmarks/timescale
python -m unittest discover -s tests -v
python -m py_compile core.py benchmark.py verify_exact.py tests/test_core.py tests/test_runner.py tests/test_exact.py
```

Main runs executable validation remotely. The completed Railway comparison history is documented in [TIMESCALE_BENCHMARK.md](../../docs/TIMESCALE_BENCHMARK.md).
