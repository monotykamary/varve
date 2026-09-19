# Query adapter

Varve runs analytical SQL through bounded, isolated DuckDB CLI workers. `Database::query` owns a resident `QueryRuntime`; its exact reuse, reset, eligibility, cancellation and replacement contract is in [QUERY_WORKERS.md](QUERY_WORKERS.md). Standalone `query::execute_with_catalog` retains a fresh subprocess per call, and `query::execute` wraps it with an empty `QueryCatalog`. The exercised engine is DuckDB `v2.0.0-alpha41533` (`Cyanoptera`, revision `10de957379`); an in-worker v2 gate precedes relation exposure, and `query::version` rejects other major versions.

The worker-boundary details below describe the standalone compatibility path. Resident workers instead use unique private staging files, deny direct stdin reads, and retain locked immutable execution scopes; they do not inherit the standalone stdin exception.

## Standalone compatibility boundary

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

The default/standalone path uses bounded typed SQL for snapshot-wide inputs up to 128 rows / 32 KiB encoded SQL, otherwise copied NDJSON with explicit types (stdin standalone, private files in pooled workers). Payload-free snapshots use typed empty tables. With `query_retained_inputs` explicitly enabled, immutable batches feed reusable typed relations through copied Arrow/Parquet staging. Selected-ID views separate current exposure from bounded inactive cached batches. Coherent full lineage and logical raw stamps permit proven complete data to survive row-preserving checkpoint/cache/compaction transitions without reload; changes reconcile or safely miss. Reachable current rollup/catalog payloads refresh before user SQL. Native/direct scanners retain exact selected-file authority, and oversized cold imports fall back rather than weakening budgets. The total retained logical input plus identity/schema metadata is capped at 128 MiB per worker. Neither mode is zero-copy Arrow or a total-RSS guarantee. Checkpointed segments can remain in the bounded decoded cache; each contributes resident rows or Parquet, never both. Immutable cold files are native typed Parquet with ZSTD compression. Raw tables use their configured names; rollups use `<table>__rollup`.

`QueryCatalog` adds read-only feature metadata. Each `CatalogRelation` becomes a quoted zero-argument DuckDB table macro, for example `varve_tables()`. Small catalogs use escaped, explicitly typed SQL literals within a 32 KiB limit. Embedded NUL, excessive literal size or insufficient script headroom conservatively selects the bounded tagged-NDJSON transport instead; transport selection never omits relations or rows merely to force the fast path. Separately, the retained engine path can omit catalog rows only when its AST plan proves one storage source and no catalog access; complete catalog schema and applicable aliases remain. Planning uses schema only, while metadata, mixed/unsupported-by-the-planner, planner-miss and non-retained queries keep full coherent catalog rows. Storage exposure is independent of pooling eligibility: a positively proven opt-in request can still use a fresh-only disposable child with unreachable catalog rows omitted. Columns and macro names are quoted, and declared types are restricted to `VARCHAR`, `BIGINT`, `UBIGINT`, `DOUBLE`, and `BOOLEAN`. Rows must match the declared nullable types, including exact signed/unsigned integer bounds.

Each `AggregateAlias` becomes a quoted view over `<source>__rollup` with an exact `width_us` predicate. Alias `count` is exposed as `BIGINT` for normal JSON numeric output; the underlying rollup relation retains `UBIGINT`. It does not create or maintain an aggregate. Core configuration and schema validation remain authoritative, and Varve still does not provide arbitrary SQL incremental maintenance, DDL, or mutable relational SQL.

`<table>__rollup` exposes `count`, `sum`, `average`, `min`, `max`, `first`, `last`, `open`, `high`, `low`, `close`, timestamps and tie-breakers. OHLC aliases map to `first`/`max`/`min`/`last`. DuckDB JSON can encode unsigned and 128-bit integers as strings to preserve precision; cast to `BIGINT` where its range suffices.

## Admission and planning

The adapter accepts one `SELECT`, `WITH`, or `EXPLAIN` statement and rejects additional statements, CLI dot commands, and mutating SQL keywords. Adapter-owned identifiers, literals and file paths are quoted. Empty results are `[]`. DuckDB v2 renders EXPLAIN specially even in JSON mode, so Varve returns `[{"plan":"rendered plan text"}]` as structured JSON.

The optional AST planner in `src/plan.rs` proves timestamp pruning for safe single-source queries, including supported derived-table chains whose exposure predicates come only from the innermost source; it does not push outer predicates past windows, limits or aggregation barriers. Scalar queries and queries whose FROM sources are exclusively known zero-argument catalog macros receive a storage-free plan. A named aggregate alias resolves to its source table with `rollup=true`; raw files are omitted and raw timestamp pruning is never applied to that alias. Unknown or dynamic table functions, unsupported nested forms, CTEs, ambiguous names and mixed joins conservatively fall back to candidate storage rather than guessing.

Each query has explicit memory, thread, elapsed-time, script-size and output-size limits. Standard input, output and error are serviced concurrently. A worker that exceeds elapsed time or output size is killed and reaped before the adapter returns.
