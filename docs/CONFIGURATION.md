# Configuration

`--config runtime.json` reads `Config`; unknown fields fail and omitted fields use defaults. `create NAME --table-config table.json` reads `TableConfig`. Runtime resource limits may change on reopen; lowering below existing state can intentionally fail recovery/admission. Partition shape remains immutable; lifecycle fields and named aggregates use the durable control plane in `CONTROL.md`.

## Runtime defaults

| Entry | Default | Meaning |
| --- | --- | --- |
| `hot_max_bytes` | 16 MiB | Hot designation and decoded segment/compaction logical-size cap; retiring this designation does not release shared raw allocation credit |
| `raw_memory_max_bytes` | 128 MiB | Positive finite pool for engine-owned raw inputs, immutable hot/cache allocations, query copies and staging reservations; includes retired allocations until their final strong owner drops |
| `raw_working_max_bytes` | 512 MiB | Separate positive finite maintenance/recovery pool for partition copies, decode, retention, compaction and codec workspaces; cannot be borrowed for optional retained cache copies. The sum of both raw limits must fit `usize`. |
| `metadata_max_bytes` | 32 MiB | Projected catalog bytes plus future segment-reference reservation; 1 KiB..64 MiB |
| `checkpoint_frozen_prefix` | `false` | Opt-in append-tolerant prefix checkpoints for explicit/scheduled work and bounded grouped-admission retry. No WAL/root format, durability, or age/pressure policy change; direct-write pressure remains locked. Default retains exact-stamp checkpoints. |
| `derived_pages` | false | Explicit opt-in to immutable derived-state pages and manifest v2; v1 remains readable, and a published v2 root never silently downgrades. See `docs/DERIVED_STATE.md` for migration/recovery boundaries. |
| `derived_max_bytes` | 64 MiB | Conservative derived resident/working and encoded/hydration limit; 4 KiB..512 MiB. A smaller root does not imply smaller resident memory. |
| `derived_page_bytes` | 256 KiB | Writer page target, 4 KiB..1 MiB and no larger than `derived_max_bytes`; readers validate historical pages against the 1 MiB format cap, not the current writer target. |
| `hot_max_rows` | 100,000 | Hot rows; checkpoint before overflow |
| `wal_max_bytes` | 64 MiB | Committed local WAL; checkpoint before overflow |
| `segmented_journal` | `false` | Explicit opt-in to append-only segmented commit logging. Persisted format authority controls later replay; legacy migration requires a covered checkpoint and never silently downgrades. |
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
| `flush_policy` | `"age_or_pressure"` | Legacy age/pressure behavior; explicit `"pressure_only"` disables elapsed-age flushing, not admission, explicit checkpoints, retention or durable timed-ID floor maintenance |
| `flush_interval_us` | 5,000,000 | Age-based hot flush interval when `flush_policy` is `"age_or_pressure"`; remains a positive configured interval in either mode |
| `ship_interval_us` | 1,000,000 | Remote publication interval, not a durability deadline |
| `maintenance_interval_ms` | 1,000 | Service scheduler tick |
| `query_executable` | `duckdb` | Legacy DuckDB v2 CLI executable; not executed when the native library is selected |
| `duckdb_library` | `null` | Absolute path to the exact checksum-pinned DuckDB v2 shared library. Activates native shared-batch SQL with private per-query environments by default; currently qualified on Linux x86_64/x86-64-v3. No automatic CLI fallback. |
| `query_native_reuse` | `false` | Explicit opt-in to bounded exclusive native sessions; fresh current inputs and proven callback detachment per execution. Inert without `duckdb_library`. Source-only/unqualified; see [NATIVE_REUSE.md](NATIVE_REUSE.md). |
| `query_memory_mb` | 128 | DuckDB worker memory setting |
| `query_threads` | 2 | Threads per query worker |
| `query_workers` | 2 | Concurrent queries; also bounds service handlers |
| `query_retained_inputs` | `false` | Explicit opt-in to snapshot-validated disposable SQL input caches and checkpoint-to-decoded-cache promotion; 128 MiB logical input/metadata cap per worker, not an RSS or durability setting |
| `query_timeout_ms` | 30,000 | Native interrupt/join or legacy subprocess deadline; snapshot capture and teardown are not a hard wall-clock guarantee |
| `query_max_output_bytes` | 8 MiB | SQL output/native scan result budget |

## Raw ownership admission

`RawMemoryBudget` issues non-cloneable `RawReservation` credits before engine allocation/copy. `SharedRawRows` owns both immutable rows and their credit: query/native-scanner pins, checkpoint captures and `retired_hot` keep it alive after hot/cache designation retirement. Opaque CLI identity handles (`WeakRawRows`, with no upgrade operation) now carry checked, nonreused process-wide allocation IDs, not weak Arc references. Both raw-handle and row-allocation Arc containers are deallocated before their payload destructors can refund row credit; concurrent aliases all use the same ordered destruction path. Identity-only handles retain neither row credit nor an uncharged control block. Hot, cache, metadata and ingestion caps remain additional designations/admission checks, never authority to refund raw allocation credit.

The estimate for a copied row collection is `R = 8 * sum(Row::estimated_bytes()) + 256` bytes. This covers vector growth, `StoredRow` layout, bounded string/map slack and owner metadata, not allocator usage. A moved collection additionally retains **every** tenant/series/tag-key/tag-value `capacity - len` byte; moving is not normalization. Terminal conversion explicitly allocates a `Vec<StoredRow>` of row-count capacity and consumes the input with `into_iter`, preserving payload pointers but preventing excess caller vector capacity from surviving its credit.

Ingestion has **two different byte measures**. Existing `admission_bytes`, group ceilings and pending-byte gauges retain their prior conservative logical/escaped-payload meanings. The raw pool instead admits the whole lifetime envelope before transfer: retained charge `R + excess_string_capacity`, plus `input_rows.capacity() * size_of::<Row>()`, table/ID string capacities, `max(4096, 3 * F)`, and `8192 + 512` bytes. Here `F = exact_canonical_row_JSON_length + 6 * (table.len() + request_id.len()) + 512`. Row JSON length is counted without allocating; six is JSON's maximum byte escape expansion; the fixed bound covers field names, digest, numeric clocks/sequence, frame and per-request handle/index metadata. The unchanged WAL encoder starts with 4096 bytes and geometrically grows its single frame, hence a three-times bound including old+new backing during growth (summed across grouped items). The 8192 bytes cover the bounded streaming digest buffer. Checked arithmetic rejects overflow. The row estimate's 128-byte base and 64 bytes per tag, multiplied by eight, conservatively cover the actual fixed Rust row layouts, vector capacity, and bounded B-tree node slack (at most 32 tags); tests bind retained capacities and layouts. Allocator bookkeeping/rounding and stack memory are not measured.

Checkpoint/capacity retries borrow rows and compute only accepted plans/ordinals. After the final borrowed frame is encoded and all retry checks pass, the owned publication lease excludes stale retries: payloads move once and original admission credit splits into immutable-row credit and transient frame/input credit, without a free-credit gap or another whole-pool acquisition. An encoded-first owner holds every input before materialization starts. It detaches every accepted frame/transient portion before consuming any rows, so partial split/conversion failure and unwind destroy the encoded frame before releasing those portions. Only retained capacities remain after publication. Rejected/duplicate inputs and failed private attempts release their credit. This does not change WAL bytes, digest rules or durable publication/fencing.

`submit_wait` registers raw release and flow closure/capacity notifications before checking admission. `Pressure` waits; `TooLarge` is permanent; checked-arithmetic failures are also immediate terminal admission errors. A flow-full refund waits only for flow capacity, never its own raw release notification. Closure is checked even when raw credit is exhausted; cancellation before enqueue owns no pending work. Synchronous `submit` and direct writes still reject pressure immediately. No ingestion path borrows the maintenance pool or awaits while holding state/publication/flow-book locks.

`Database.scan` starts with only the empty-owner charge and grows before each selected row clone/push, separately enforcing `query_max_output_bytes`. Empty/small scans therefore do not pre-reserve eight times the configured maximum output. Quota failure discards the entire private output. Public sequence/ordinal ordering is total, allowing allocation-free unstable sorting. Returned vectors leave accounting at handoff.

Codec workspace reserves `16 * logical_bytes + 2 * encoded_bound + 8 MiB` before Arrow/Parquet work. Decode checks row counts/uncompressed metadata before decompression and validates exact decoded-size metadata. Recovery additionally reserves bounded tail decode workspace. SQL copies/staging have separate admission; native Rust scanner metadata/callback accounting is distinct from DuckDB arenas and returned SQL output. No engine row copy is justified by the caller-input exclusion.

Native execution admits its Rust scanner workspace from the **regular owned pool**, before scanner allocations and database open; it no longer reserves eight times all selected logical row bytes. Engine snapshot batch-vector/pin metadata is separately precharged by selected handle counts and ID lengths. The fixed scanner plan precharges requested `RawBatch` and column/name layouts, `ScannerSpec`/Arc metadata and one reusable callback slot per configured query thread. Covered variable arrays use exact boxed-slice layouts with unwind-safe partial initialization, and fixed columns use boxed arrays; no postallocation Vec-capacity check is treated as admission. Allocator-internal usable-size padding remains excluded. Each slot owns 1024 positions (`Position` is two `usize`, 16 KiB on 64-bit) plus the maximum exact escaped tag JSON length. A nonallocating JSON counter computes raw tag maxima once when each immutable row owner is built; snapshots inspect only cached maxima. Selected rollup tag maxima are counted outside State during native preparation. Empty sources use zero tag bytes; arithmetic overflow or insufficient quota fails before allocation.

The slot pool is global to the query, not multiplied independently per relation. Execution callbacks check out a slot only for callback duration, returning it on success, error or caught panic. The pinned API receives a per-scanner thread ceiling, database open receives the query-wide thread limit, and slot checkout also enforces the global bound. Actual fixed capacities, peak callback concurrency and dynamic-owner high-water marks have focused deterministic tests. Repeated UNION/self-join bindings may create more retained Bind/Global/Local states than threads or registered sources: every actual UserData/Bind/Global/Local box acquires its own checked reservation before allocation. Pre-handoff RAII and opaque-pointer destruction both keep the guard outside the box while destroying its fields and deallocating its backing storage; only then is credit refunded. No static source-count estimate substitutes for those owners.

The fixed scanner lease is owned by the outer execution scope, outside the scratch Arc. It outlives cancellation join, callback detachment (or complete private session teardown), prepared scanner metadata and the final scratch Arc backing allocation. Shared budget/notification bookkeeping, allocator-internal padding and stack storage are not included in these per-allocation estimates; charged scanner/row container headers are not excluded. Source payload is charged only to its original shared row owner. Rust scanner metadata/buffers are not DuckDB arenas: DuckDB vectors/arenas retain the separate query memory limit, Rust returned SQL output retains its existing output quota, and general catalog/snapshot identity/file-authority and SQL/view construction remain under their existing independent bounds. None of these is a hard process RSS guarantee. The separately opted-in native reuse candidate requires complete callback detachment before any session enters idle; see [NATIVE_REUSE.md](NATIVE_REUSE.md).

Maintenance uses independent headroom and never waits for quota while holding state. Insufficient mandatory workspace fails cleanly for retry with larger limits or less concurrent work. Configure enough workspace for the selected hot/compaction size plus simultaneous partition and codec reservations; a positive but very small setting need not admit a checkpoint or reopen with a WAL tail. Optional checkpoint-to-cache copies are skipped **before allocation** when the owned pool is full; this does not prevent a durable checkpoint. Active queries can still cause bounded write/query rejection even after hot/cache retirement. Dropping the last owner, not checkpoint success or eviction, makes that allocation's credit available.

Caller-owned input allocations before transfer and returned vectors after handoff are excluded; engine-owned lifetimes between those boundaries are charged. DuckDB's allocator, process RSS, general catalog/derived/control allocations (their existing budgets apply), and transport/journal framing are not a unified part of this raw-row quota. This is single-engine raw ownership accounting, not independent partition ownership or a total-RSS guarantee.

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
