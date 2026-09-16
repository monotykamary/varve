# Architecture

## Scope

Varve v0.1 is a single-node modular Rust database for append-only numeric time-series measurements. Named tables have fixed typed rows: signed UTC epoch `timestamp_us`, `tenant`, `series`, finite f64 `value`, and ordered string `tags`. Table settings select hash shards, event-time windows, lateness, raw retention, archive age and continuous-aggregate widths. A committed global sequence and row ordinal break equal-timestamp ties. A timestamp/series pair is not unique; request IDs deduplicate entire batches.

## Write and recovery path

1. Lock state and validate table, timestamps, finite values, request ID, batch size and conflicts.
2. Precompute affected aggregate groups. Bound their working set and projected encoded metadata, including future segment-reference reservations. Check hot/WAL/disk budgets and checkpoint first when necessary.
3. Encode a versioned, checksummed immutable WAL frame. Direct `Database::write` uses one append; the shared ingestion coordinator can collect independent requests into one `AppendGroup` publication with per-request receipts and a common physical sequence. Sync a temporary, atomically rename it in the same filesystem, then sync the directory.
4. Apply rows, aggregate updates and receipt under the same state lock. Only then acknowledge `local_fsync`.

HTTP and WebSocket writes share a bounded Crossbeam coordinator with one batching writer. Request/row/byte/time ceilings bound each flush; pending bytes include queued and in-flight work. `Database::write_group` stages affected rows/aggregates/receipts behind the state lock with a bounded undo log, restores the pre-publication state, publishes the grouped WAL and applies it durably. It does not clone the full catalog per group; existing checkpoints still can. Global row ordinals preserve deterministic ties within a shared physical sequence. Admission is not acknowledgment. Invalid/conflicting work never publishes a partial application batch. Ambiguous local publication errors fence the instance; reopen replays or rejects the committed frame rather than guessing. `float_roundtrip` is enabled for JSON persistence so typed finite values survive serialization exactly. Floating-point aggregation itself is still approximate f64 arithmetic.

An exclusive OS file lock enforces one process per data directory. Startup removes recognized unpublished temporaries, loads the verified manifest and replays exactly the contiguous WAL tail above its checkpoint. Committed corruption, unknown formats and sequence gaps fail closed. Idempotency receipts are independent of raw expiration. Legacy receipts remain durable; opted-in timed-ID windows atomically prune receipts with a monotonic rejection floor at checkpoint, so old retries cannot silently reinsert data.

## Checkpoints and snapshots

The manifest atomically contains table definitions, segment references, derived state, request receipts, retention cutoffs and replay position. Checkpointing writes/syncs sorted immutable Parquet files, publishes the complete manifest, and only then retires hot rows and covered WAL. Compaction replaces file references, never query-visible state piecemeal. Unreferenced output is garbage, not committed data.

Snapshots copy bounded hot/view state and pin individual immutable file identities. Compaction/expiration can publish replacement manifests while existing queries keep their old files. Cache eviction skips only pinned entries, not the entire cache. Per-file decoded-size metadata is checked before Parquet allocation/decompression; legacy zero estimates take a conservative bounded fallback.

Local native maintenance and cold materialization still serialize with commits. Remote publication uses a distinct operation gate and frozen, pinned dependencies; network uploads do not need to hold the ingestion state lock. See the remote tests and due-diligence record for the exercised slow-store boundary. No lock-free or hard-latency claim is made.

## Physical partitioning and tiers

`shard_for(tenant, series, shard_count)` is length-prefixed FNV-1a with pinned golden vectors. Event windows use Euclidean floor division, including negative epochs. Files are grouped by table/shard/window and sorted by tenant, series, timestamp, sequence and ordinal; their hashes, sizes, row counts and bounds are in the manifest. Shards are physical partitioning/routing units, not separately replicated distributed owners in this version. Table settings are immutable.

CPU caches are hardware managed. Varve uses batching and sorted columnar layouts, not fictional L1/L2 durability tiers. RAM holds recent rows plus a bounded decoded cache; disk holds WAL, current Parquet and bounded downloaded cold files. S3-compatible storage provides optional asynchronous recovery/archive backing. Filesystem object storage implements the same protocol for hermetic tests.

## Derived state and lifecycle

Rollup keys are `(table, width, bucket, tenant, series, tags)`. Updates maintain count, sum, min, max and event-time first/last; average derives from sum/count and OHLC aliases first/max/min/last. Raw and derived changes share a WAL operation and checkpoint. This is constrained continuous aggregation, not general SQL incremental maintenance or a planner that substitutes rollups for raw SQL automatically.

Scheduler ticks drive age/size flush, small-file compaction, remote publication, protected local eviction, raw expiration, aggregate expiration and garbage collection. Cutoffs are monotonic and persisted. Raw expiration can filter partial files, reclaim fully obsolete files, and preserve independently retained derived history. These rollups are historical summaries, not necessarily strict views of only the remaining raw rows.

## Remote publication

Immutable manifests/segments/WAL have digest-derived keys. A conditional head publishes a complete checkpoint plus contiguous tail only after dependencies upload. A separate publication/GC gate prevents vacuum from deleting in-flight dependencies. Remote CAS locks exclude restore and remote vacuum; locks fail closed on crashes and are never blindly time-stolen. Restore transfers ownership and verifies metadata/WAL immediately, then verifies cold segments on demand. See [REMOTE.md](REMOTE.md) for operational recovery and caveats.

A local acknowledgment is not an S3 acknowledgment. Losing the local disk can lose the unshipped tail; an interval cannot bound that gap during an outage. S3-compatible providers must support strong conditional writes. The adapter uses ordinary immutable objects, not an assumption that Express append provides multi-AZ durability.

## SQL boundary

The real DuckDB v2 CLI is a bounded subprocess per query. Startup files and extension autoload/install are disabled. Hot/view rows are copied over stdin into temporary in-memory tables, and cold data uses verified pinned local Parquet paths. Disk spilling is disabled. Memory, thread, worker, timeout and output limits are explicit. This avoids a multi-GB bundled C++ build, but is not zero-copy Arrow ingestion or a persistent worker pool.

A conservative SQL AST planner proves relation/time pruning only for safe simple cases; complex/dynamic syntax falls back without guessing. SQL execution remains DuckDB's job. Read-only lexical guards are deliberately conservative, not an untrusted filesystem/network sandbox. EXPLAIN's CLI-rendered plans receive a JSON wrapper. See [QUERY.md](QUERY.md).

## Module boundaries

- `model.rs`: typed domain, validation, fixed routing and public configuration.
- `wal.rs`: durable frames, sync/rename and opt-in process failpoints.
- `engine.rs`: state/admission, replay, checkpoints, coherent snapshots and native scans.
- `segment.rs`: reusable typed Arrow/Parquet read/write.
- `plan.rs` / `query.rs`: conservative exposure planning and replaceable SQL adapter.
- `remote.rs`: reusable synchronous RemoteStore abstraction, filesystem and AWS adapters, bounded object I/O.
- `tier.rs`: publication ownership, restore, remote locks and reachable-object vacuum.
- `policy.rs`: deterministic-clock lifecycle orchestration.
- `ingest.rs`: bounded many-producer queue, batching writer, completion receipts and drain.
- `service.rs` / `transport.rs` / `pg_transport.rs`: authenticated HTTP/WebSocket service and opt-in loopback SCRAM PostgreSQL-wire simple queries.
- `main.rs`: JSON CLI and service lifecycle.
- `clients/rust` / `clients/typescript`: independent network clients; no embedded storage dependency.

The interfaces are reusable without the HTTP server. Runtime/backend configuration, user-facing commands, tests and explicit production exclusions are linked from the README.
