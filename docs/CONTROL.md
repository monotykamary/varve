# Control plane

Varve's control plane is local, durable metadata for the fixed time-series engine. It is not arbitrary SQL DDL and continuous aggregates are not general SQL incremental view maintenance.

## Rust API

The public methods on `Database` are:

```rust
fn create_table(&self, name: &str, config: TableConfig) -> Result<u64>;
fn set_policy(&self, table: &str, policy: LifecyclePolicy) -> Result<u64>;
fn policy(&self, table: &str) -> Result<LifecyclePolicy>;
fn idempotency_floor_us(&self, table: &str) -> Result<Option<i64>>;

fn create_continuous_aggregate(
    &self,
    name: &str,
    source: &str,
    width_us: i64,
) -> Result<u64>;
fn drop_continuous_aggregate(&self, name: &str) -> Result<u64>;
fn continuous_aggregates(&self) -> Result<Vec<ContinuousAggregate>>;

fn create_job(&self, name: &str, kind: JobKind, interval_us: i64) -> Result<u64>;
fn alter_job(&self, name: &str, alter: JobAlter) -> Result<u64>;
fn pause_job(&self, name: &str) -> Result<u64>;
fn resume_job(&self, name: &str) -> Result<u64>;
fn drop_job(&self, name: &str) -> Result<u64>;
fn run_job_now(&self, name: &str, now_us: i64) -> Result<()>;
fn jobs(&self) -> Result<Vec<JobDefinition>>;
fn tick(&self, now_us: i64) -> Result<()>;
fn is_ready(&self) -> bool;
```

A successful mutating method returns the committed global WAL sequence. Lifecycle policy updates replace the five mutable fields: `late_after_us`, `retention_us`, `archive_after_us`, `rollup_retention_us`, and `idempotency_window_us`. JSON `null` disables an ordinary lifecycle field. Timed idempotency is a one-way cutover: after `idempotency_window_us` becomes non-null it cannot return to null. Shard count, event window, and the full creation configuration identity remain immutable.

`JobAlter` accepts `interval_us` and/or `paused`. Job kinds are `checkpoint`, `compact`, `ship`, `maintain`, and `vacuum_remote`. `varve_maintenance` is created from `Config::maintenance_interval_ms`, is inspectable and pausable, and cannot be dropped. Direct `Database::maintain(now_us)` remains available and does not depend on the scheduler.

## SQL controls

`Database::query` intercepts these calls before the read-only DuckDB guard:

```sql
CALL varve_create_table(name, config_json);
CALL varve_set_policy(table, policy_json);
CALL varve_create_continuous_aggregate(name, source, width_us);
CALL varve_drop_continuous_aggregate(name);
CALL varve_create_job(name, kind, interval_us);
CALL varve_alter_job(name, config_json);
CALL varve_drop_job(name);
CALL varve_run_job(name);
CALL varve_run_job(name, now_us);
```

Exactly one statement is accepted. `sqlparser` must parse it as `CALL`, the function must be on the whitelist, and every argument must be a string or signed integer literal. Expressions, subqueries, trailing statements, and unknown functions are rejected before mutation. Results are JSON objects such as `{"sequence": 12}`.

The following zero-argument DuckDB table macros are read-only:

```sql
SELECT * FROM varve_tables();
SELECT * FROM varve_policies();
SELECT * FROM varve_continuous_aggregates();
SELECT * FROM varve_jobs();
SELECT * FROM varve_status();
```

Their columns use only `VARCHAR`, `BIGINT`, `UBIGINT`, `DOUBLE`, and `BOOLEAN`. Metadata-only plans do not materialize raw Parquet. A named continuous aggregate is exposed as a read-only view over `source__rollup` filtered to its exact width.

## Bounded idempotency windows

`TableConfig::idempotency_window_us` and `LifecyclePolicy::idempotency_window_us` are explicit opt-in settings. `None` preserves the v0.1 behavior: any non-empty, non-NUL request ID up to 256 bytes is accepted and its receipt is retained for the table lifetime. Once a table is changed to `Some(positive_us)`, the change cannot be disabled. Extending the configured window does not lower the already accepted floor and therefore cannot resurrect forgotten IDs.

An opted-in table accepts only `v1:<issued_us>:<nonce>`. `issued_us` is a signed decimal i64 UTC epoch in microseconds. The nonce is at least one byte; it has no character restriction beyond the whole request ID's 256-byte/non-NUL limit, and may contain `:`. New IDs more than `IDEMPOTENCY_MAX_FUTURE_SKEW_US` (300,000,000 microseconds) ahead of the write call's `now_us` are rejected. A retry with an existing receipt is exempt from that future check so a wall-clock rollback does not invalidate an already accepted request, but it must still be at or above the monotonic floor and have the same body digest.

The request issue time is not an event timestamp. Event timestamps remain the row `timestamp_us` values used for lateness, retention, windows, and queries. The request issue time is used only for idempotency admission and receipt lifetime. The effective floor advances monotonically as `max(previous_floor, now_us - idempotency_window_us)`. IDs below it are rejected even if their receipt was already purged. `Database::idempotency_floor_us(table)` returns the current effective floor. `varve_policies()` exposes both `idempotency_window_us` and the last checkpointed `idempotency_floor_us`.

Checkpoint and maintenance atomically publish the advanced floor and remove legacy receipts plus timed receipts below it. A crash before that publication replays the still-present WAL/receipts; a crash after it recovers the floor and cannot re-ingest a forgotten ID. Receipt pruning does not remove raw rows or rollups. The caller supplies `now_us`; correctness requires a trustworthy UTC clock within the fixed future-skew bound. This is a bounded single-node deduplication window, not a globally synchronized request clock.

## Continuous aggregate limits

Each alias selects Varve's fixed grouping `(tenant, series, tags)` and fixed state: count, sum, minimum, maximum, average, first/last, and OHLC aliases. Creating the first alias for a new width performs a synchronous, sequence-barrier backfill. It reads one bounded segment at a time plus the coherent hot snapshot and admits the complete derived metadata before publishing the WAL record. If a concurrent write moves the barrier, creation fails and can be retried; no partial alias is accepted.

Backfill uses retained raw rows only. Data already removed by raw retention cannot be reconstructed and is not represented as if it had been read. Future writes maintain every unique active width once, regardless of how many aliases share it. A table may have at most 16 unique active widths. Dropping an alias preserves a width used by another alias or present in the immutable creation configuration; an unshared dynamic width and its derived rows may be removed.

Rollups remain historical summaries with their own retention policy. They are not automatically substituted for arbitrary raw SQL and are not strict views of only currently retained raw rows.

## Durability and scheduling

Policy, alias, and job-definition changes are versioned checksummed authoritative WAL operations. Mutation admission clones the future manifest, validates it, and checks the metadata budget before WAL publication. New job starts and finishes do not use the authoritative WAL. Recovery requires contiguous sequences and revalidates every operation. Checkpoints and remote restore include the same state.

Control mutations carry a deterministic state digest. The manifest retains a bounded sequence/digest history (4096 entries) used by owned-head prefix reconciliation. If an older remote prefix falls outside that proof window, reconciliation fails closed rather than accepting owner equality. Table prefix proof compares immutable creation configuration, not mutable policy. Every new WAL variant is handled explicitly.

A job start is durable before its action and completion is durable afterward in the checksummed private `job-runtime.bin` journal. The journal is capped at 4 MiB and retains only the latest run per authoritative job-definition generation. It is not part of the authoritative catalog, control digest, WAL sequence, checkpoint, or remote manifest. `jobs()` and `varve_jobs()` overlay generation-matched journal state on authoritative definitions without mutating catalog job fields. A crash after start leaves the local run active and the next `tick` retries the same run. Built-in actions are idempotent and execution is at least once, not exactly once. An in-process guard prevents overlap. Failures record only the latest run, use bounded exponential backoff capped by the interval, and stop immediate retry after five attempts before returning to the normal interval. Error text is capped at 2 KiB; there is no per-tick history. Dropped/recreated job names have a new generation, so stale journal state is ignored. Remote restore has no private journal and may safely rerun an idempotent action. Legacy `JobStarted` and `JobFinished` WAL records remain explicitly replayable for format compatibility.

Altering or dropping a job commits its authoritative WAL mutation before updating the private runtime journal. If that later journal update fails, the error explicitly reports the committed sequence and the database fences; reopen and inspect the durable definition before retrying. Stale or dropped journal generations are ignored during recovery. A reported post-commit error must not be interpreted as rollback.

`tick(now_us)` runs all due jobs and reports combined failures after durable completion records are written. Callers should use an explicit deterministic clock in tests. The persisted maintenance job performs routine checkpoint, compaction, publication, local garbage collection, and remote reachability vacuum when shipping is due even when no retention policy is configured. Remote vacuum consumes bounded listing pages and batch deletes; a partial sweep remains idempotently retryable under the durable GC lock.

Startup preflights the active WAL tail against `wal_max_bytes`, validates contiguous filenames, and decodes/replays one frame at a time. Hot rows, receipts, rollup groups, and metadata are checked after every frame rather than after allocating the whole tail. SQL and native cold segment materialization use coherent per-file pins and perform remote reads outside the state mutex. Status snapshots scalars under the mutex and uses the cached exact encoded-manifest byte count; it does not clone or serialize the catalog. Recursive disk accounting still runs after releasing the state mutex. `is_ready()` is the constant-cost readiness path: it only attempts the state mutex and checks fencing, returning false when busy or poisoned without serialization or filesystem access.

## Format compatibility and exclusions

The manifest and WAL envelope remain explicit v0.1 `VARVEM01`/`VARVEW01`, format version 1. Newly added manifest fields use a documented backward migration: missing aliases, jobs, and control history decode as empty, and missing `creation_config` uses the prior table config as immutable identity. Missing `idempotency_window_us`, `idempotency_floor_us`, and receipt `issued_us` decode as disabled/absent legacy state. Opening such a manifest durably adds the built-in maintenance job. Unknown fields, unknown WAL operations, future format versions, bad checksums, and sequence gaps fail closed. There is no forward-format guessing.

This implementation is single-node and does not claim distributed scheduling, arbitrary SQL IVM, hostile multi-tenant SQL sandboxing, or production readiness. Remote durability still depends on explicit shipping; an interval cannot guarantee an outage-bounded recovery point. Before remote CAS, publication durably records a checksummed intent for the exact planned head and physically reserves binding space under shared disk admission. Reopen accepts an ambiguous CAS outcome only when the remote head exactly matches that intent; divergent heads fail closed. Fault-injection coverage exercises intent, reservation, binding, WAL, and manifest publication errors. Append metadata admission uses an exact cached encoded-manifest size plus serializer-equivalent deltas for the changed receipt, changed/new rollup groups, and checkpoint-sequence digit width; it does not clone or serialize the total catalog per append. Descriptor/hot-row reserve remains conservative. Legacy-mode idempotency receipts still retain the lifetime configured cap, while opted-in tables prune only at checkpoint/maintenance. Rollup groups retain their configured lifetime cap. Disk admission serializes Varve-managed writers, but this is not an OS quota, strict RSS bound, distributed scheduler, or production-readiness claim.
