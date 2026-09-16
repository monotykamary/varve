# Configuration

`--config runtime.json` reads `Config`; unknown fields fail and omitted fields use defaults. `create NAME --table-config table.json` reads `TableConfig`. Runtime resource limits may change on reopen; lowering below existing state can intentionally fail recovery/admission. Partition shape remains immutable; lifecycle fields and named aggregates use the durable control plane in `CONTROL.md`.

## Runtime defaults

| Entry | Default | Meaning |
| --- | --- | --- |
| `hot_max_bytes` | 16 MiB | Estimated hot rows and native segment/compaction working set |
| `metadata_max_bytes` | 32 MiB | Projected catalog bytes plus future segment-reference reservation; 1 KiB..64 MiB |
| `hot_max_rows` | 100,000 | Hot rows; checkpoint before overflow |
| `wal_max_bytes` | 64 MiB | Committed local WAL; checkpoint before overflow |
| `max_disk_bytes` | 512 MiB | Local directory admission budget |
| `decoded_cache_bytes` | 8 MiB | Retained native decoded cache; zero disables caching |
| `disk_cache_bytes` | 32 MiB | Downloaded cold Parquet cache; pinned files cannot be evicted |
| `max_batch_rows` | 10,000 | Rows per API batch |
| `max_batch_bytes` | 4 MiB | Canonical serialized row payload |
| `max_idempotency_keys` | 100,000 | Durable request registry cap across tables; never silently forgotten |
| `max_rollup_groups` | 100,000 | Aggregate group cap in addition to byte admission |
| `max_tables` | 128 | Named table cap |
| `segment_rows` | 16,384 | Maximum target rows per output file |
| `compact_min_segments` | 4 | Minimum small files in one shard/window to compact |
| `flush_interval_us` | 5,000,000 | Age-based hot flush interval |
| `ship_interval_us` | 1,000,000 | Remote publication interval, not a durability deadline |
| `maintenance_interval_ms` | 1,000 | Service scheduler tick |
| `query_executable` | `duckdb` | DuckDB v2 executable |
| `query_memory_mb` | 128 | DuckDB worker memory setting |
| `query_threads` | 2 | Threads per query worker |
| `query_workers` | 2 | Concurrent queries; also bounds service handlers |
| `query_timeout_ms` | 30,000 | DuckDB subprocess deadline |
| `query_max_output_bytes` | 8 MiB | SQL output/native scan result budget |

These are logical admission estimates, not allocator-wide RSS or filesystem quotas. Serialization/copies, Arrow/Parquet overhead, metadata and query workers coexist. Native scans read files sequentially but bound their returned rows. A cold SQL snapshot must fit the downloaded cache because this adapter uses local Parquet paths.

Service-only WebSocket, PostgreSQL-wire and ingestion settings are documented in [TRANSPORTS.md](TRANSPORTS.md). They are CLI/environment settings, not extra fields in the persisted `Config` JSON. The library also exposes [IngestConfig](INGESTION.md).

## Table defaults

| Entry | Default | Meaning |
| --- | --- | --- |
| `shards` | 8 | Stable FNV-1a hash over length-prefixed tenant/series; 1..1024 |
| `window_us` | 3,600,000,000 | Event-time file window (one hour) |
| `late_after_us` | `null` | Reject new rows older than `now_us - age`, if configured |
| `retention_us` | `null` | Raw expiration age, disabled unless selected |
| `archive_after_us` | `null` | Local eviction age, conditional on remote protection |
| `rollup_widths_us` | `[60000000]` | Up to 16 unique positive widths |
| `rollup_retention_us` | `null` | Independently expire aggregate buckets by bucket end |
| `idempotency_window_us` | `null` | Opt into `v1:<issued_us>:<nonce>` IDs; checkpoint/maintenance prune receipts below a durable monotonic floor, and expired IDs are rejected |

Durations/widths are positive microseconds. Windows use Euclidean floor division for negative epochs. A window start outside i64 is rejected before acknowledgment; width 1 supports the entire signed timestamp domain. Raw expiration removes `timestamp_us < cutoff`; scans use `[start, end)`. Cutoffs never move backward with the supplied clock.

Rollup keys include width, bucket, tenant, series and the complete tag map. Shards use tenant/series; tags do not create physical partitions. First/last order by `(timestamp_us, sequence, ordinal)` for deterministic late arrivals/ties. Sums are finite f64 with normal floating-point rounding; overflow rejects before WAL publication. Average derives from sum/count.

Raw expiration need not expire rollups or request receipts. These are independent historical state, not strict materialized views over only currently retained raw rows. New rows older than the raw cutoff fail. Valid in-horizon retries return their original receipt without resurrecting data; expired timed IDs are rejected. Request issue time and event time are independent. The accepted request floor never moves backward, and enabling a window is a one-way cutover; see `CONTROL.md`.
