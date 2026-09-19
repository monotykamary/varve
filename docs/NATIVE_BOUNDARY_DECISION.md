# Native DuckDB boundary decision

Status: Rust adapter integrated with `Database` and HTTP on 2026-09-18. Pinned-library primitive/temporal/nested/JSON parity, scoped lifetime/cancellation regressions and native-only checkpoint/archive/restore checks passed on Railway. Broad regression, coherent review and matched-resource/production qualification remain separate gates; see [INSIDE_OUT_REBUILD.md](INSIDE_OUT_REBUILD.md).

Date: 2026-09-18

## Decision

Use DuckDB's pinned **v2 C ABI table-function API** to expose a query-scoped Varve snapshot. Rust remains the owner of immutable hot batches, verified Parquet identities, rollups, durability, and reclamation. DuckDB receives:

- hot raw rows through a registered table function that fills DuckDB-owned output vectors from pinned Rust batches;
- rollup rows through a second fixed-schema table function;
- cold rows through DuckDB's built-in `read_parquet` over the exact verified, pinned local paths selected by Varve; and
- generated connection-local views that union the hot scanner and selected Parquet without creating or populating a shadow table.

Do not use the legacy Arrow relation APIs, do not retain the CLI resident database as the target architecture, and do not stage hot rows as NDJSON or temporary Parquet. The first implementation remains behind an explicit native-mode gate and must not replace the public query path until the remote ABI probe and result-compatibility oracle pass.

This is deliberately a narrow boundary for Varve's fixed raw schema and existing fixed rollups. It is not a design for arbitrary mutable schemas or arbitrary SQL incremental materialized views.

## Evidence labels

- **Verified** means observed in the current Varve source, the exact pinned DuckDB commit/header, or the exact staged binary artifact.
- **Hypothesis** means it still needs execution on Railway. Header presence and exported symbols do not establish callback correctness, loader compatibility, performance, memory containment, or cancellation behavior.

## Pinned API and artifact findings

### Identity and availability

The current installer pins:

- DuckDB version: `v2.0.0-alpha41533`
- commit: `10de9573794001c649621013bdd93553b54e00c9`
- Varve source: [`scripts/install-duckdb.sh`](../scripts/install-duckdb.sh)

**Verified:** the commit exists at [duckdb/duckdb@10de9573794001c649621013bdd93553b54e00c9](https://github.com/duckdb/duckdb/commit/10de9573794001c649621013bdd93553b54e00c9). The version has no public GitHub release or tag: the GitHub release and tag API endpoints both returned HTTP 404 on 2026-09-18. The artifacts are staging artifacts, not a normal immutable public release contract.

The pinned DuckDB workflow explicitly publishes `duckdb-shared-libs-<platform>.tar.gz` and `libduckdb-src.zip`; see [the pinned Linux release workflow](https://github.com/duckdb/duckdb/blob/10de9573794001c649621013bdd93553b54e00c9/.github/workflows/Main.yml#L655-L805) and [staged upload workflow](https://github.com/duckdb/duckdb/blob/10de9573794001c649621013bdd93553b54e00c9/.github/workflows/StagedUpload.yml#L16-L65).

**Verified by bounded HTTP range/HEAD reads:** these exact artifacts are available:

| Artifact | HTTP/content | Size | SHA-256 observed by streaming the complete artifact |
| --- | --- | ---: | --- |
| [Linux amd64 shared library](https://duckdb-staging.duckdb.org/10de957379/v2.0.0-alpha41533/duckdb/duckdb/github_release/duckdb-shared-libs-linux-amd64.tar.gz) | 200, `application/gzip` | 25,657,420 bytes | `934507594428409754d3e76d162ee5763df3df53b570bb770b2b38082c3795da` |
| [macOS arm64 shared library](https://duckdb-staging.duckdb.org/10de957379/v2.0.0-alpha41533/duckdb/duckdb/github_release/duckdb-shared-libs-osx-arm64.tar.gz) | 200, `application/gzip` | 18,354,331 bytes | not downloaded in full |
| [public headers](https://duckdb-staging.duckdb.org/10de957379/v2.0.0-alpha41533/duckdb/duckdb/github_release/libduckdb-src.zip) | 200, `application/zip` | 153,793 bytes | `83442d176f9a52e6b70e90204ce2acdefcc5eaf2bffe3dbeb298cd864680c25e` |

The Linux archive contains only `libduckdb.so`, `duckdb.h`, `duckdb_v2.h`, `duckdb_extension.h`, and `duckdb_extension_v2.h`. Streamed member hashes are below. The packaged `duckdb.h` and `duckdb_v2.h` hashes also exactly match those files at the pinned GitHub commit:

| Member | SHA-256 |
| --- | --- |
| `libduckdb.so` | `69bdd44e0d2426e7ba44ed14644b54d8bd99a9703e5cbb4aec3f8beef40f817d` |
| `duckdb.h` | `9ff8bfb6f88ed1be4f845399ed7ff35980a65e011848d027cef88467fe9e0886` |
| `duckdb_v2.h` | `62ad0df66b9f4193a657540d2429ba23cb32c41c5c98ea9f3f4e7a3b64e30544` |
| `duckdb_extension.h` | `a67312ee4c88b1c3ede25af0a74c253ce39354efe1c3beaf576c441451f84a36` |
| `duckdb_extension_v2.h` | `45b19dd8588fd2cfc32dabe72fbe83f5926734bedeead7128a31a57575981807` |

The producing workflow checks `-march=x86-64-v3` for amd64 canonical builds. **Limitation:** the Linux artifact requires an x86-64-v3 CPU. Its Railway CPU and glibc loader compatibility remain hypotheses until the remote probe loads it. A successful CLI run does not prove the separate shared object is loadable.

### Verified scanner interface

The exact [`duckdb_v2.h`](https://github.com/duckdb/duckdb/blob/10de9573794001c649621013bdd93553b54e00c9/src/include/duckdb_v2.h) declares its v2 table-function surface as stable in v2.0.0. The relevant region is [table-function registration and callbacks](https://github.com/duckdb/duckdb/blob/10de9573794001c649621013bdd93553b54e00c9/src/include/duckdb_v2.h#L11744-L12805). It includes:

- creation against a connection or extension;
- name, signature, user-data, bind, global-init, local-init, exec, and progress callbacks;
- projection pushdown;
- a filter-pushdown callback with explicit accept semantics;
- bind/global/local state carried by opaque pointers with destructor callbacks;
- a maximum scan-thread declaration;
- projected-column index discovery;
- a borrowed output data chunk for each exec callback; and
- function registration followed by destruction of the builder handle without unregistering the function.

The exact Linux `libduckdb.so` was streamed through `strings`; it contains the names of all symbols needed for this seam, including:

- `duckdb_v2_table_function_create_with_connection`
- `duckdb_v2_table_function_set_bind_callback`
- `duckdb_v2_table_function_set_init_global_callback`
- `duckdb_v2_table_function_set_init_local_callback`
- `duckdb_v2_table_function_set_exec_callback`
- `duckdb_v2_table_function_set_projection_pushdown`
- `duckdb_v2_table_function_set_filter_pushdown_callback`
- `duckdb_v2_table_function_exec_get_output_chunk`
- `duckdb_v2_table_function_register`
- `duckdb_v2_connection_interrupt`
- `duckdb_v2_statement_execute`
- `duckdb_v2_result_fetch_chunk`
- `duckdb_v2_library_version`

This establishes source and artifact availability. A string-table match does not prove ELF dynamic export, loader compatibility, or successful calls; those remain explicit checks in the Railway probe.

### Arrow conclusion

Do not build the boundary around `duckdb_arrow_scan` or `duckdb_arrow_array_scan`.

**Verified:** in the pinned legacy [`duckdb.h`](https://github.com/duckdb/duckdb/blob/10de9573794001c649621013bdd93553b54e00c9/src/include/duckdb.h#L2855-L3111), those relation APIs are marked deprecated since v1.0.0 and are gated by `DUCKDB_API_ALLOW_DEPRECATED` for current API versions.

**Verified:** the new `duckdb_v2.h` has public, stable-v2 Arrow conversion APIs such as `duckdb_v2_arrow_importer_create` and `duckdb_v2_result_to_arrow_stream`; see [the Arrow module](https://github.com/duckdb/duckdb/blob/10de9573794001c649621013bdd93553b54e00c9/src/include/duckdb_v2.h#L4738-L5050). It does not expose a `duckdb_v2_arrow_scan` relation-registration function. The importer converts Arrow arrays to DuckDB chunks; it is not itself a catalog scanner.

Therefore the verified table-function API is the production seam. Arrow import plus `duckdb_v2_vector_reference` is a possible later optimization only after lifetimes and copy measurements are proven. It is not required for the first scanner and must not be described as zero-copy today.

## Current Varve contracts that the seam must preserve

### Query and worker behavior

The current source establishes these contracts:

- [`src/query.rs`](../src/query.rs) validates one conservative read-only statement, applies explicit memory/thread/timeout/output bounds, disables spill and extension autoload/install, quotes adapter-owned identifiers and paths, and returns `serde_json::Value`.
- Empty results are `[]`. `EXPLAIN` is wrapped as `[{"plan":"..."}]`. CLI JSON output is decoded directly, including DuckDB's string treatment of some unsigned and 128-bit values.
- [`src/query/workers.rs`](../src/query/workers.rs) has bounded, non-queuing admission; cancellation and deadline checks; exact-file worker keys; and teardown that kills and joins all I/O before releasing capacity.
- Selected Parquet files must remain immutable and pinned until execution returns.
- [`src/query/resident_types.rs`](../src/query/resident_types.rs) already identifies immutable raw batches as `Arc<Vec<StoredRow>>`, with immutable IDs, charges, and timestamp bounds. `ResidentSnapshot` records namespace, sequence, tables, file identities, and complete raw lineage. Its sequence is query identity, not a durable frontier.
- [`src/engine.rs`](../src/engine.rs) captures batch arcs, selected segment identities, decoded-cache arcs, rollups, catalog rows, raw stamps, and file pins under one coherent state snapshot, then resolves files and executes outside the state lock.
- [`src/segment.rs`](../src/segment.rs) defines the cold raw schema as non-null `timestamp_us: Int64`, `tenant: Utf8`, `series: Utf8`, `value: Float64`, `tags: Utf8`, `sequence: UInt64`, and `ordinal: UInt32`.
- [`src/model.rs`](../src/model.rs) defines the fixed `StoredRow` and fixed `RollupRow` fields and validation limits.

The current retained-worker optimization is specifically what must be removed from the target path: [`src/query/resident.rs`](../src/query/resident.rs) writes memory batches to temporary Parquet, may hard-link or copy selected files, inserts them into `__varve_input`, stages dynamic NDJSON, and retains a materialized DuckDB relation. This is a copied shadow database, even when worker reuse amortizes it.

### Public SQL shape

The native path must preserve these relation contracts before it can become default:

Raw table columns, in order:

| Column | DuckDB type | Nullability |
| --- | --- | --- |
| `timestamp_us` | `BIGINT` | not null |
| `tenant` | `VARCHAR` | not null |
| `series` | `VARCHAR` | not null |
| `value` | `DOUBLE` | not null; Varve admits only finite values |
| `tags` | `VARCHAR` | not null; canonical JSON object text |
| `sequence` | `UBIGINT` | not null |
| `ordinal` | `UINTEGER` | not null |

`<table>__rollup` remains the fixed current rollup schema: `width_us`, `bucket_us`, tenant/series/tags, count/sum/average/min/max/first/last, OHLC aliases, first/last timestamps, and sequence/ordinal tie-breakers. Named aggregate aliases remain views with a fixed `width_us` predicate. They do not become arbitrary SQL incremental views.

## Proposed native architecture

### Stable implementation seam

The smallest seam is a sibling adapter behind the existing call boundary:

`QueryRuntime::execute_resident_with_catalog_cancellable(tables, snapshot, sql, options, catalog, cancelled)`

The engine already supplies nearly all required ownership at that call site. In the current synchronous path, the engine-local `Pin` remains alive across the runtime call; the first seam need not move it. Any later asynchronous native request must move the pin guard into its query-owned object. The native adapter should consume an object assembled from the existing arguments:

```text
NativeQuerySnapshot
  namespace + sequence + lineage
  tables[]
    name
    raw batches: Arc<ResidentBatch>[]
    selected verified files: path + identity + decoded charge
    cutoff and already-proven plan bounds
    rollups: immutable owned/Arc slice
  catalog: immutable query catalog
  file pin guard (or a caller-held guard in the initial synchronous seam)
  cancellation flag
```

Do not begin by changing engine storage layout or replacing `StoredRow`. The first scanner can walk the existing typed immutable batch arcs. A later independently measured change may make the resident batch physically columnar and pre-encode canonical tag JSON, but API feasibility and correctness do not depend on that churn.

Add a separate explicit `duckdb_library` configuration when implementation starts. Do not reinterpret the existing CLI executable path as a shared-library path. Keep the old CLI adapter as the controlled rollback path until R3 evidence passes.

### Runtime ownership

One `NativeDuckDbRuntime` owns, in destruction order:

1. the loaded exact library for process lifetime;
2. one DuckDB v2 environment;
3. bounded query admission equal to the configured worker capacity; and
4. process-wide immutable ABI/version metadata and metrics.

The baseline opens a fresh in-memory database and connection for each admitted query and registers `varve_raw_scan` and `varve_rollup_scan` on that private database. The registration user-data points directly to that query's immutable Rust snapshot. This is intentionally stricter than merely opening a fresh connection on one shared database: the scope of `allowed_paths`, `lock_configuration`, catalogs, and caches across concurrent connections has not been behaviorally verified for this alpha. The private database is intended to isolate exact allowlists, locked settings, function registrations, and temporary views, but the Railway probe must prove disjoint configuration under concurrency. If settings leak across databases in one environment, use one environment per query or retain process isolation; do not weaken the allowlist. This still removes hot-input materialization. Database/connection pooling is a later optimization only after configuration-isolation and reset tests.

The library and environment must never be unloaded while a query database, connection, result, function registration, callback state, or DuckDB-created thread can exist. The simplest rule is no `dlclose`; unload only at process exit after orderly native runtime destruction.

### Query flow

1. Under the existing engine snapshot protocol, capture the batch arcs, rollups, selected verified segment descriptors, raw lineage, and file pins. Transfer their complete logical charges to query/pin accounting; cloning an `Arc` does not make memory free.
2. Open a fresh in-memory database and connection under bounded admission. Register the raw and rollup table functions with query-owned user-data holding an `Arc<NativeQuerySnapshot>`.
3. Apply the current memory/thread/no-spill/no-extension settings, set the exact selected Parquet paths as the only allowed paths, disable external access, and lock configuration. No query temp directory is needed for hot input. If DuckDB requires a path-valued setting, use a private service-owned directory but require that the query creates no files.
4. Create connection-local views with adapter-generated, quoted SQL. For raw table `metrics`, the shape is conceptually:

   ```sql
   CREATE TEMP VIEW "metrics" AS
     SELECT timestamp_us, tenant, series, value, tags, sequence, ordinal
     FROM varve_raw_scan('metrics')
     UNION ALL
     SELECT timestamp_us, tenant, series, value, tags, sequence, ordinal
     FROM read_parquet([<exact selected paths>]);
   ```

   Apply the captured retention cutoff to both branches. Empty branches are typed, not sentinel rows. The generated view never depends on rewriting user SQL.
5. Create `<table>__rollup` from `varve_rollup_scan` and retain the current derived columns and aggregate aliases. Small catalog relations may remain bounded typed SQL literals initially; larger catalogs should use a fixed-schema catalog scanner rather than NDJSON.
6. Execute the already validated user statement through the v2 statement/result API. Stream result chunks into the bounded Varve JSON adapter.
7. Destroy the result, close the connection and private database, join cancellation/watchdog work, and only then drop snapshot/file pins and release query admission. Every error path follows the same order.

There is no `__varve_input`, `INSERT`, hot NDJSON, hot temporary Parquet, copied selected Parquet, or persistent DuckDB shadow relation in this path.

### Raw scanner behavior

The bind callback:

- accepts only the generated table-name argument type;
- resolves the table against the query snapshot in function user-data and clones its `Arc<NativeQuerySnapshot>` into bind data;
- declares exactly the seven fixed raw columns and an exact/estimated cardinality; and
- does not accept caller-supplied file paths, schema, SQL, or arbitrary pointers.

The global-init callback:

- constructs immutable work descriptors over whole batch/range pairs;
- records projected column indices;
- owns one `Arc` to the snapshot for the entire scan;
- initializes an atomic next-work index and progress counters; and
- sets maximum threads to `min(query_threads, nonempty_work_items)`, with a minimum of one.

The local-init callback creates thread-local cursor state. It claims work through the global atomic index; it never mutates a batch and never holds an engine/state mutex.

The exec callback obtains the borrowed DuckDB output chunk, copies at most one vector-sized slice, sets the first output vector's size, and returns an empty batch only at end of scan. Projection pushdown should be enabled so unused columns are not converted.

For numeric columns, use the verified typed mutable-vector path. For strings, copy bytes into DuckDB's vector arena and write its public v2 byte representation. Rust `String` storage cannot simply be pointed at DuckDB unless the exact vector-reference lifetime is proven. `tags` currently requires canonical JSON serialization; cache that representation in a future typed columnar batch only after accounting it. The baseline must measure and report these copies and must not claim zero-copy.

Do not accept DuckDB filter pushdown in the baseline. Varve's existing conservative planner may preselect batches/files and capture exact safe bounds. An accepted v2 filter becomes a promise that the scanner itself applies it exactly; accepting one without a complete typed expression evaluator can silently return wrong rows. Filter pushdown is a later, separately tested optimization.

### Parquet behavior

Do not decode verified Parquet through Rust merely to feed the scanner. DuckDB's built-in `read_parquet` is the native cold scanner. Varve remains responsible for:

- selecting files without false negatives;
- verifying content identity before exposure;
- providing only absolute immutable local paths;
- exact allowlisting on the query connection;
- retaining file pins through result destruction and cancellation; and
- ensuring a selected checkpointed segment contributes resident decoded rows or its Parquet file, never both.

Remote objects are still downloaded, verified, and pinned under Varve's existing cold-file protocol before SQL. This decision does not authorize DuckDB network access or direct S3 credentials.

## Threading, cancellation, and lifetime rules

These are mandatory FFI invariants:

1. No Rust panic may cross an `extern "C"` callback. Every callback and destructor catches panic, converts ordinary callback failures to a DuckDB error, and aborts only if invariants make safe recovery impossible.
2. Callback pointers refer only to heap allocations owned through `duckdb_v2_opaque` destructors. No pointer into a movable Rust stack frame crosses the ABI.
3. Bind data is read-only after bind. Global state uses only atomics or a narrowly scoped mutex for work assignment/progress. Local state is touched only by its DuckDB worker thread.
4. The engine state lock, disk-admission lock, and decoded-cache lock are never held while calling DuckDB or while a callback can run.
5. The query thread owns the connection and result. A separate bounded cancellation watcher may call `duckdb_v2_connection_interrupt`; the pinned header explicitly documents this as safe from another thread while a result is being stepped. The watcher must be joined before disconnect/free.
6. The scanner also checks Varve's cancellation flag between work slices so interruption does not depend solely on DuckDB reaching an interrupt poll.
7. Timeout, caller cancellation, output overflow, shutdown, and callback error all interrupt, drain/destroy the result as permitted, close the connection and private database, join all owned threads, and then release pins. No detached native query or callback may outlive its snapshot.
8. Shutdown closes admission first, waits for admitted query teardown, then destroys the registered database/environment. A timeout may be reported as exceeded, but the service must not claim teardown completed while native work remains.
9. Function, bind, global, and local destructor callbacks are idempotent with respect to Rust ownership and never re-enter DuckDB on the handle being destroyed.

## SQL result compatibility

Replacing CLI `-json` with native chunks changes the output adapter and is a compatibility project, not a formatting detail.

The required public behavior is:

- a JSON array of row objects;
- empty result `[]`;
- SQL `NULL` as JSON `null`;
- exact current column names and duplicate-name behavior;
- exact finite double behavior;
- current DuckDB CLI treatment of unsigned, 128-bit, and decimal values, including string encoding where the CLI preserves values that JSON numbers cannot represent safely;
- current nested/list/struct/temporal/blob behavior for any query that remains advertised as supported;
- the current `EXPLAIN` wrapper and rendered plan text contract;
- output charging before unbounded allocation and the same configured output-byte rejection; and
- PostgreSQL-wire behavior, which currently derives text columns from the returned JSON objects.

The native result adapter should read the v2 result schema and chunks and append directly to a bounded JSON writer. It must not first materialize an unbounded DuckDB result and then serialize it. The output bound includes JSON punctuation and escaping.

**Baseline support gate:** enable native mode only for result types with an exact pinned-CLI parity oracle. The minimum useful set is null, boolean, signed integers, `UBIGINT`/`UINTEGER`, finite double, and UTF-8 varchar as produced by raw/rollup queries and their ordinary count/sum/min/max/average projections. Unsupported logical types must fail closed or remain on the old adapter during migration; they must not be guessed.

This restriction is about result serialization, not DuckDB's ability to execute SQL. Before the native path replaces the public adapter, add parity cases for empty results, aliases, duplicate names, Unicode/escaping, `u64::MAX`, signed extremes, nulls, aggregates, raw/Parquet unions, rollups, `EXPLAIN`, and each additional supported DuckDB logical type.

## Memory, performance, and security trade-offs

### Benefits

- Removes per-query process spawn from the target path.
- Removes copied NDJSON, hot temporary Parquet, hard-link/copy fallback, `INSERT` into `__varve_input`, and retained shadow relations.
- Keeps one coherent Rust-owned snapshot and bounded DuckDB vector conversion.
- Allows DuckDB projection pushdown to avoid converting unused hot columns.
- Lets DuckDB scan selected Parquet directly with its native vectorized reader.

### Costs and limits

- Native does not mean zero-copy. Fixed-width hot values are copied into output vectors in the baseline; tenant, series, tags, and result JSON require byte copies. DuckDB operators can materialize additional state.
- Query pins add to live Rust memory. DuckDB's `memory_limit` does not include every Rust allocation, file mapping, allocator overhead, result buffer, or library-global cache. Admission must charge pinned batches, output buffers, and per-query native overhead separately.
- Multiple connections in one process can make process RSS exceed one connection's configured limit. Bound concurrency and qualify aggregate RSS on Railway.
- A DuckDB assertion, allocator failure, native crash, ABI mismatch, or FFI memory bug now affects the Varve process, unlike the current child-process boundary. Native embedding weakens failure isolation.
- `duckdb_v2_connection_interrupt` is cooperative cancellation, not an OS kill. Shutdown and cancellation latency require measurement.
- In-process SQL is not a hostile multi-tenant sandbox. Keep the conservative statement validator, disable extension install/autoload and external access, use exact file allowlists, expose no credentials, and continue to document SQL as trusted-local.
- A private per-query database prevents scanner user-data and temporary views from being reused by another query. It is still not a security boundary against arbitrary code execution in-process.
- The staged alpha artifact is not a public release guarantee. Deployment must mirror or otherwise retain the exact reviewed bytes under an operator-controlled artifact policy before calling the mode production-capable.

## Schema and feature limitations

The baseline supports only:

- Varve's one fixed append-only numeric time-series row schema;
- canonical tags exposed as JSON `VARCHAR`, not DuckDB `MAP`;
- the current exact rollup schema and named fixed-width aliases;
- local verified/pinned Parquet with the matching seven-column schema;
- the current single-statement read-only `SELECT`/`WITH`/`EXPLAIN` surface; and
- existing conservative table/time/series planning without false-negative pruning.

It does not establish:

- arbitrary SQL incremental materialized views;
- arbitrary aggregate definitions, joins as maintained views, calendar/DST buckets, gap fill, or mutation invalidation;
- mutable relational tables, schema evolution, DDL/DML, or user-defined storage schemas;
- a public zero-copy Arrow relation API;
- direct S3 scanning or DuckDB access to object-store credentials;
- process isolation, a hard RSS ceiling, hard cancellation latency, distributed execution, or failover; or
- compatibility with a different DuckDB v2 alpha, a system `libduckdb`, musl, arm64 Linux, or CPUs below x86-64-v3.

Ordinary read-only DuckDB SQL may still compose over the exposed fixed relations. That is distinct from promising that Varve incrementally maintains arbitrary SQL.

## Binary and header binding policy

A production native mode must bind all of these identities together:

1. exact full commit `10de9573794001c649621013bdd93553b54e00c9`;
2. exact version string `v2.0.0-alpha41533` from `duckdb_v2_library_version`;
3. exact `duckdb_v2.h` content hash `62ad0df66b9f4193a657540d2429ba23cb32c41c5c98ea9f3f4e7a3b64e30544` used to generate checked-in Rust FFI bindings;
4. exact Linux `libduckdb.so` hash `69bdd44e0d2426e7ba44ed14644b54d8bd99a9703e5cbb4aec3f8beef40f817d`; and
5. exact deployment platform/architecture compatibility proven by the remote loader probe.

Do not use a semver-compatible Rust DuckDB wrapper that silently selects another C library. Generate only the needed v2 declarations from the pinned header, review the resulting ABI types, and link/load only the configured absolute shared-library path. Startup fails closed on absent library, digest mismatch, missing required symbol, or version mismatch. The service status should report the resolved path, artifact digest, API/header digest, and library version without reporting secrets.

The artifact archive digest is useful for installation; the member digest is authoritative after extraction. Installation must extract to a new immutable versioned directory, verify before publication, and atomically switch the configured path. Never replace a loaded shared object in place.

## Smallest production-capable sequence

1. **API probe only.** Run the remote probe below. Make no Varve Rust/Cargo changes if it fails.
2. **FFI shell behind a disabled mode.** Add the exact library/header binding, startup identity checks, environment plus private query-database lifecycle, bounded admission, and one scanner returning an in-memory fixed raw batch. No public routing.
3. **Existing snapshot seam.** Route only `execute_resident_with_catalog_cancellable` into the native adapter. Use current `ResidentSnapshot`, `QueryTable`, catalog, cancellation, and pins. Do not redesign WAL, checkpoints, or hot storage in this step.
4. **Mixed hot/Parquet and rollups.** Add generated views over raw scanner plus exact `read_parquet`, then the fixed rollup scanner and catalog exposure. Assert no hot-input staging files and no shadow relation.
5. **Result parity and bounds.** Implement the bounded chunk-to-JSON adapter and pass CLI-vs-native exact oracles for every enabled result type, cancellation, timeout, output overflow, pins, and teardown.
6. **Railway qualification.** Measure conversion bytes, query setup, execution, peak process RSS, cancellation latency, hot/cold exactness, and mixed concurrent ingestion. Only then consider native mode default.
7. **Retirement later.** Remove resident staging/CLI paths only after R3/R7 acceptance, migration/rollback evidence, and one released deployment can revert to the old adapter.

`src/query/native.rs` as a sibling of the current adapter is the likely first code boundary. Avoid moving unrelated query planning or engine state until this seam is proven.

## Stage A result and remaining decisive remote probe

The bounded Stage A probe passed on Railway using the exact artifact below: `projection=0x6`, opaque destructor counts `1/2/2/2`, four exec callbacks. Two aggregate queries observed sums 3 and 13 and maximum sequences 9 and 42 after caller-owned input changed between queries. No shadow relation or hot staging SQL is used, and no working-directory file was created. This is a C ABI feasibility witness, not a Rust lifetime, cancellation, mixed-Parquet, multi-query isolation or performance qualification. The broader probe that follows remains pending.


Main should run this on the owned Railway qualification sandbox; this document does not authorize an agent to mutate Railway.

Use a fresh service-owned working directory, not `/tmp`, with explicit byte/time limits and cleanup. Download the exact Linux shared archive, require archive SHA-256 `934507594428409754d3e76d162ee5763df3df53b570bb770b2b38082c3795da`, extract only the five expected members, and require the member hashes above.

Compile a small C probe directly against the extracted `duckdb_v2.h` and `libduckdb.so`; C avoids spending time on Rust wrapper design before the ABI is known to work. The probe must:

1. call `duckdb_v2_library_version` and require exactly `v2.0.0-alpha41533`;
2. open an in-memory database and register a table function with bind/global-init/local-init/exec/destructor callbacks;
3. emit two or more chunks of the seven-column Varve raw schema, including Unicode/quotes, `i64` boundaries, `u64::MAX`, and more than one tenant;
4. enable projection pushdown and execute a grouped/order-sensitive query that reads only a subset of columns;
5. union the scanner with one small service-owned Parquet fixture through an exact allowlisted path and verify row/count/sum/tie-break results;
6. run two private databases concurrently in one environment with disjoint locked path allowlists; require each to read its own fixture and reject the other fixture, with no setting or catalog leakage;
7. start a deliberately slow multi-chunk scan, call `duckdb_v2_connection_interrupt` from a second thread, require a cancelled/interrupt result within a fixed deadline, join both threads, and verify every opaque destructor ran exactly once;
8. snapshot the work directory before and after the query and require no unplanned database, spill, or hot-input staging file; and
9. record loader diagnostics, CPU flags, elapsed time, maximum RSS, and all exact result values.

The probe passes only if loading, version binding, registration, projected vector writes, mixed hot/Parquet SQL, disjoint configuration isolation, interruption, and teardown all pass in one bounded run. A missing symbol, `SIGILL`, loader/glibc failure, wrong value, callback error, destructor mismatch, cancellation hang, or unplanned file is a stop signal. Do not respond by tuning the CLI cache or designing more resident-table machinery.

After that pass, the next probe should be a Rust FFI lifetime harness and the CLI-vs-native JSON parity matrix. Those are not substitutes for this first ABI behavior check.

## Acceptance ledger for this decision

| Check | Evidence | State |
| --- | --- | --- |
| Repository identity | `git rev-parse --show-toplevel` exactly `/Users/monotykamary/VCS/working-remote/open-source/varve` | verified |
| Required design context | `AGENTS.md`, `docs/ARCHITECTURE.md`, `docs/ACCEPTANCE.md`, `docs/INSIDE_OUT_REBUILD.md` read | verified |
| Actual pin | installer, exact GitHub commit, exact version | verified |
| Shared library/header availability | bounded staging HEAD/range/full streaming reads; hashes and members above | verified |
| Required v2 scanner/cancel/result API names | pinned header plus exact shared object; Stage A calls | scanner/statement/version calls executed on Railway; interruption and full adapter calls remain pending |
| Legacy Arrow relation status | deprecated/gated in pinned `duckdb.h`; no invented v2 Arrow scan | verified |
| Current query/worker/snapshot contracts | source inspection cited above | verified |
| Runtime loader/callback behavior | `probes/native_scan.c`, Railway Stage A | passed for three numeric rows, projected aggregate, caller-buffer refresh and exact destructor counts; full probe below still pending |
| Rust FFI unwind/lifetime/cancellation behavior | remote Rust harness | pending |
| Exact public JSON/EXPLAIN/pgwire compatibility | pinned CLI-vs-native oracle | pending |
| Configuration isolation | concurrent private databases with disjoint locked allowlists on Railway | pending; decisive |
| Memory/thread/security qualification | bounded Railway mixed workload | pending |
| No hot staging or shadow database | filesystem and source witness after implementation | pending |

## Sources

Varve:

- [`docs/INSIDE_OUT_REBUILD.md`](INSIDE_OUT_REBUILD.md)
- [`docs/ARCHITECTURE.md`](ARCHITECTURE.md)
- [`docs/ACCEPTANCE.md`](ACCEPTANCE.md)
- [`scripts/install-duckdb.sh`](../scripts/install-duckdb.sh)
- [`src/query.rs`](../src/query.rs)
- [`src/query/workers.rs`](../src/query/workers.rs)
- [`src/query/resident.rs`](../src/query/resident.rs)
- [`src/query/resident_types.rs`](../src/query/resident_types.rs)
- [`src/engine.rs`](../src/engine.rs)
- [`src/segment.rs`](../src/segment.rs)
- [`src/model.rs`](../src/model.rs)

Pinned DuckDB:

- [commit `10de9573794001c649621013bdd93553b54e00c9`](https://github.com/duckdb/duckdb/commit/10de9573794001c649621013bdd93553b54e00c9)
- [`duckdb_v2.h` at the exact commit](https://github.com/duckdb/duckdb/blob/10de9573794001c649621013bdd93553b54e00c9/src/include/duckdb_v2.h)
- [`duckdb.h` at the exact commit](https://github.com/duckdb/duckdb/blob/10de9573794001c649621013bdd93553b54e00c9/src/include/duckdb.h)
- [Linux release artifact construction](https://github.com/duckdb/duckdb/blob/10de9573794001c649621013bdd93553b54e00c9/.github/workflows/Main.yml#L655-L805)
- [staged artifact publication](https://github.com/duckdb/duckdb/blob/10de9573794001c649621013bdd93553b54e00c9/.github/workflows/StagedUpload.yml#L16-L65)
- [exact Linux shared-library artifact](https://duckdb-staging.duckdb.org/10de957379/v2.0.0-alpha41533/duckdb/duckdb/github_release/duckdb-shared-libs-linux-amd64.tar.gz)
- [exact public-header artifact](https://duckdb-staging.duckdb.org/10de957379/v2.0.0-alpha41533/duckdb/duckdb/github_release/libduckdb-src.zip)
