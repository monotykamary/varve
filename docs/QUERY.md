# Query adapter

Varve v0.1 runs analytical SQL in a fresh, bounded DuckDB CLI subprocess. The exercised engine is DuckDB `v2.0.0-alpha41533` (`Cyanoptera`, revision `10de957379`). `query::execute_with_catalog` performs an in-worker v2 gate before exposing relations, and `query::version` rejects other major versions. `query::execute` is the compatibility wrapper with an empty `QueryCatalog`.

## Worker boundary

The CLI starts with `-no-init`, an in-memory database and a private temporary directory. Varve writes setup plus user SQL to a mode-private file and invokes DuckDB with native `-f`; SQL and long Parquet lists are not process arguments. The script is capped at 8 MiB and removed with the worker directory. Standard input remains dedicated to hot NDJSON ingestion. The database layer currently applies its own smaller public SQL request limit.

Before reading input, the script applies settings verified against the v2 engine:

- `autoinstall_known_extensions=false`, `autoload_known_extensions=false`, `allow_community_extensions=false` and `allow_unsigned_extensions=false`;
- `allowed_directories=[]`, and `allowed_paths` containing only `/dev/stdin` and the immutable Parquet files selected for this query;
- `enable_external_access=false` after the exact path allowlist is installed;
- private home and temporary directories, with disk spilling disabled; and
- `lock_configuration=true` after all worker settings are established.

A missing or rejected security setting fails the query closed. The child environment is cleared; only `PATH`, required Windows runtime roots when present, and private `HOME`/temporary-directory values are supplied. Inherited credentials are therefore not available through DuckDB `getenv`.

These controls are the filesystem/network boundary; the single-statement lexical read-only guard is defense in depth, not the sandbox. Tests against the real engine deny `/etc/passwd`, `/proc/1/environ`, remote URLs, and unselected Parquet/database-shaped files while selected cold files and `/dev/stdin` continue to work.

This is still an alpha CLI subprocess boundary, not an OS sandbox: Varve does not add seccomp, namespaces, a chroot, or a separate service account. A DuckDB native-code vulnerability could exceed SQL-level settings, and selected Parquet files are intentionally readable by the query. Production qualification must pin and retest the exact DuckDB build and should add an operating-system containment layer appropriate to the deployment.

## Exposed relations

Hot rows and rollups are encoded as newline-delimited JSON on stdin and copied into a typed temporary in-memory staging table. This is copied ingestion, not zero-copy Arrow integration. Immutable cold files are native typed Parquet with ZSTD compression. Raw tables use their configured names; rollups use `<table>__rollup`.

`QueryCatalog` adds read-only feature metadata without interpolating values into SQL. Each `CatalogRelation` becomes a quoted zero-argument DuckDB table macro, for example `varve_tables()`. Its rows travel as tagged JSON payloads on the same stdin stream. Columns and macro names are quoted, and declared types are restricted to `VARCHAR`, `BIGINT`, `UBIGINT`, `DOUBLE`, and `BOOLEAN`. Rows must match the declared nullable types.

Each `AggregateAlias` becomes a quoted view over `<source>__rollup` with an exact `width_us` predicate. Alias `count` is exposed as `BIGINT` for normal JSON numeric output; the underlying rollup relation retains `UBIGINT`. It does not create or maintain an aggregate. Core configuration and schema validation remain authoritative, and Varve still does not provide arbitrary SQL incremental maintenance, DDL, or mutable relational SQL.

`<table>__rollup` exposes `count`, `sum`, `average`, `min`, `max`, `first`, `last`, `open`, `high`, `low`, `close`, timestamps and tie-breakers. OHLC aliases map to `first`/`max`/`min`/`last`. DuckDB JSON can encode unsigned and 128-bit integers as strings to preserve precision; cast to `BIGINT` where its range suffices.

## Admission and planning

The adapter accepts one `SELECT`, `WITH`, or `EXPLAIN` statement and rejects additional statements, CLI dot commands, and mutating SQL keywords. Adapter-owned identifiers, literals and file paths are quoted. Empty results are `[]`. DuckDB v2 renders EXPLAIN specially even in JSON mode, so Varve returns `[{"plan":"rendered plan text"}]` as structured JSON.

The optional AST planner in `src/plan.rs` proves timestamp pruning only for safe, simple single-table queries. Scalar queries and queries whose FROM sources are exclusively known zero-argument catalog macros receive a storage-free plan. A named aggregate alias resolves to its source table with `rollup=true`; raw files are omitted and raw timestamp pruning is never applied to that alias. Unknown or dynamic table functions, nested queries, CTEs, unsupported syntax, ambiguous names and mixed joins conservatively fall back to all candidate storage rather than guessing.

Each query has explicit memory, thread, elapsed-time, script-size and output-size limits. Standard input, output and error are serviced concurrently. A worker that exceeds elapsed time or output size is killed and reaped before the adapter returns.
