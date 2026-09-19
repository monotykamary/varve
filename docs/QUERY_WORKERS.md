# Reusable isolated DuckDB query workers

## Public contract

```rust
QueryRuntime::new(capacity: usize) -> QueryRuntime
QueryRuntime::with_metrics(capacity: usize, metrics: Arc<Metrics>) -> QueryRuntime

runtime.execute_with_catalog(
    tables: &[QueryTable],
    sql: &str,
    options: &QueryOptions,
    catalog: &QueryCatalog,
) -> anyhow::Result<serde_json::Value>

runtime.execute_with_catalog_cancellable(
    tables: &[QueryTable],
    sql: &str,
    options: &QueryOptions,
    catalog: &QueryCatalog,
    cancelled: &AtomicBool,
) -> anyhow::Result<serde_json::Value>

runtime.stats() -> QueryWorkerStats
```

The runtime is synchronous and thread-safe. A database owns one runtime and shares its `Arc<crate::metrics::Metrics>` through `with_metrics`. `new` creates private metrics. Use `query_workers` for process capacity. Process reuse remains the default and requires no new dependencies or bundled C++. The separate `query_retained_inputs` flag, default `false`, opts database queries into the disposable input cache described below; existing public runtime entry points keep per-request materialization. Zero capacity rejects all requests. Once the admission mutex is acquired, occupied capacity rejects rather than accumulating a SQL-work queue. Contention on the key-check/retirement critical section can delay that check; this is not a wait-free admission guarantee.

The existing standalone `query::execute` and `query::execute_with_catalog` functions remain available with per-call subprocess behavior. They share the SQL guard and bounded input serializer. The resident runtime requires absolute selected file paths; the engine supplies pinned absolute paths.

## Exact reuse boundary

All workers are keyed by canonical executable path/generation and configured memory/thread limits. Native per-request materialization and direct scanners additionally require the exact sorted selected-file set. These paths retain the original pinned absolute file allowlist; a key mismatch requires another child.

Eligible retained logical queries use a separate file-independent key with **no original-file allowlist**. Verified and pinned cold files enter through unpredictable aliases inside the child's private input directory, using a hardlink when possible and a bounded copy fallback otherwise. Aliases are removed before user SQL. Changing DuckDB's locked allowlist, accumulating old file permissions or widening to the data directory is not required. Native/direct-scanner behavior is preserved rather than rewriting user SQL. Oversized retained cold imports use the original bounded native-scanner fallback.

Matching a process key alone never proves input freshness. Retained reuse also requires namespace/sequence checks and coherent logical lineage, values, charges, scope and current dynamic relations. Capacity includes active plus idle children and is never an unbounded cache of keys. An incompatible idle child is preferred for eviction under pressure; a spare compatible child is not evicted merely to construct a fresh-only request. Failed reset/cancel/install paths still reap the child.

Executable generation on Unix includes device, inode, size and nanosecond mtime/ctime. In-place replacement or mutation of the executable/installation is unsupported; coordinate atomic replacement with admission or recreate the runtime. This discriminator is not content authentication; wrappers/libraries remain trusted. Non-Unix pooling retains its conservative fresh execution fallback. All selected sources remain immutable and pinned until execution returns. DuckDB configuration stays locked throughout execution; native/fresh scanner authority and reusable logical-input authority are distinct.

## Conservative pooling eligibility

The read-only lexical guard remains an admission restriction, **not** a pooling proof. `ROLLBACK` does not reset DuckDB PRNG state: the pinned actual CLI returns `0.24376333656300356` from `random()` after `BEGIN; SELECT "setseed"(0.25); ROLLBACK;`. Bare, quoted and dynamically evaluated forms therefore cannot execute on reusable children.

The existing `sqlparser` DuckDB parser classifies a deliberately small subset of SELECT/CTE/EXPLAIN syntax. Parsed function identifiers include quoted names. The visitor checks nested expressions, function arguments, filters, windows and subqueries. A short explicit non-mutating builtin list covers basic numeric/string aggregates, date bucketing/conversion, ranking/lag/lead and expressions; locked-setting/catalog introspection (`current_setting`, `duckdb_tables`, `duckdb_functions`) and `range` are explicit exceptions to a purely deterministic-expression subset. Generated zero-argument catalog table macros also qualify: their bodies are generated internally, and the entire catalog is validated before any request SQL executes. Qualified/unknown functions, stateful calls (including `random` itself), dynamic SQL (`query`), direct scanners, unrecognized AST forms and parse failures are **fresh-only**, not newly rejected SQL.

Fresh-only means a newly constructed worker that is **never acquired from or returned to the pool**, even on success. It still uses the runtime's admission slot, private setup/staging, output framing, deadline, cancellation polling, rollback acknowledgement and kill/reap cleanup; it does not delegate to standalone execution. Spare capacity leaves matching reusable idle workers untouched. Under capacity pressure, an incompatible idle key is preferred for eviction; eviction of a matching key may be necessary if it is the only idle candidate.

This classifier is a conservative reuse policy for a trusted local DuckDB installation, not a comprehensive SQL effect system or an SQL/OS sandbox. Existing executable, filesystem and environment trust boundaries still apply. Unsupported syntax and functions remain available through the fresh path subject to the existing read-only/security limits. Tests assert process identity, disposal and stats; they do not use probabilistic random-output inequality.

## Opt-in retained inputs

`Config.query_retained_inputs = true` makes database SQL snapshots share immutable, ID'd hot batches and available decoded segment rows. The engine chooses resident rows **or** a pinned Parquet file for each selected segment. Checkpoint publication can offer exact sorted chunks to the existing `decoded_cache_bytes` LRU without rereading Parquet. Disabled, zero-cache and non-fitting chunks do not create retained output copies. Eviction still follows bounded cache/lifecycle policy; WAL/root/Parquet formats and acknowledgements do not change.

A worker retains typed input tables outside the user transaction, with raw rows tagged by table and batch identity. A separate selected-ID binding defines the current generated views. Inactive immutable batches remain charged within the same budget, allowing full-to-selective-to-full requests without reimporting unchanged data. Namespace mismatch and snapshot inversion cannot reuse a resident worker. Removed or remapped identities are reconciled explicitly; schema, cutoffs, catalog macros and aggregate aliases are transactionally rebound rather than assumed unchanged.

The engine captures complete live lineage for every table together with a process-local raw stamp under the same snapshot lock, before resolving pinned paths. Logical append/retention/replacement/recreation changes the stamp; proven row-preserving checkpoint/cache/compaction transitions do not. Only proven complete raw coverage may use equal-stamp physical replacement as evidence to retain previous materialized rows. Subsequent additions need a valid lineage-superset proof; changed/removed data cannot be inferred equivalent from row counts or filenames. These stamps are neither durable acknowledgments nor replacements for root/publication validation. Reopen starts fresh stamps and workers.

Planning uses catalog shape and aliases without materializing its rows. For retained engine queries, a positive single-storage-source AST proof also permits omission of catalog rows that the statement cannot reach. Every catalog relation name/type and applicable aggregate alias remains. Metadata/storage-free, mixed, unsupported-by-the-storage-planner, planner-miss and non-retained queries keep full coherent catalog rows; absence of proof never permits omission. This storage proof is separate from worker reuse eligibility. An opt-in retained query such as `SELECT sqrt(value) FROM metrics` can have a positive storage proof but require a disposable fresh-only child; it may still omit proven-unreachable catalog rows. The proof does not certify pooling eligibility, arbitrary SQL effects or sandbox security. This is input exposure pruning, not SQL-result caching or a raw-to-rollup substitution.

Ordinary hot batches still require their immutable memory identity; reusing a name for a different `Arc` causes reconciliation, not a cache hit. Only the engine's verified content-addressed segment cache marks decoded rows as the same immutable segment as its file. With matching ID, row count and charge, file/decoded/new-decoded representations can then reuse the existing SQL copy. A hash-looking string alone grants no such provenance. Partial coverage without a logical-completeness proof can still require importing a rewritten physical ID.

Missing retained raw batches use typed Arrow/Parquet staging, with the same finite-float/timestamp/tag schema as immutable segments. Existing cold segments use verified private aliases, and newly materialized hot batches use temporary Parquet. Imports copy into DuckDB's disposable table; this is not a shared allocator, native embedding or zero-copy. Staging cleanup completes before user SQL. Exact raw hits stage zero raw bytes; appends stage only new batches. Rollup tables and individual catalog relations use immutable typed descriptors built from the exact current rows. Float identity uses bits, including signed zero; signed/unsigned integer types and all catalog status/job fields remain distinct. Admission scans the current typed rows to intern an equal descriptor before serialization. The install plan deletes and inserts only changed rollup/catalog relations in the existing adapter transaction; unchanged relations generate no payload, and changed-to-empty relations issue only their targeted delete. Historical rollups never answer current raw queries. Configuration, initial schema and data installation are acknowledged before user SQL starts; `ROLLBACK` then removes user-transaction changes without discarding the committed disposable input cache. Every partial install, SQL/reset/output/cancellation failure still discards and reaps the worker. Fresh-only SQL gets a new exact-snapshot child, never an old cached state.

The **total** admitted resident logical input, selected and inactive raw batches, complete-coverage bookkeeping, typed descriptors, generated schema and identity metadata is capped at the smaller of 128 MiB and one quarter of the configured DuckDB worker memory budget, not just each delta. This is a conservative reusable-cache allowance, not an exact bound on DuckDB allocations: typed storage, mutation versions and query operators need execution headroom, and DuckDB's unchanged memory cap remains authoritative. The one-shot request input ceiling remains 128 MiB. A separate lifetime ledger charges each inserted raw batch, changed dynamic relation and installed selected-ID binding until that child is retired. Deletion/replacement does not refund that ledger: SQL `DELETE` plus `COMMIT` is not proof that DuckDB reclaimed physical storage. Admission rejects an over-age idle worker before staging, then uses a fresh child under the existing active-plus-idle capacity bound; it does not retry a failed SQL statement. Unchanged validated hits incur no new materialization charge. Both live metadata/input and cumulative materialized-input ledgers must fit; neither measures actual allocator RSS. Inactive immutable entries are evicted before rejecting an active set; cache selections that cannot fit use disposable selected-only execution, with cold files scanned natively. All-hot cache misses use the same disposable path without reducing the one-shot input admission limit. An unknown decoded size at the adapter boundary cannot be treated as free. Current manifest validation requires positive segment decoded-size metadata; adapter fallback is not a promise to open or migrate roots missing that required metadata. Borrowed preflight debits one request-wide budget before payload cloning: scope, batch metadata, new or interned descriptors, and separate relation-map entry/key charges all count. Reused `Arc` descriptors are not free. Indexed identity checks and relation lookups avoid quadratic prefix scans. Changed-relation payloads and staging are also bounded. The old persistent canonical dynamic byte vector is not retained alongside typed rows. Engine row/batch charges are logical estimates and batches are nonempty, so node growth is bounded by hot-row admission. Partitioning copies, pinned snapshots, buffers, DuckDB overhead and concurrent workers coexist; this is not an allocator-wide or aggregate RSS guarantee. This path is not native/zero-copy Arrow and does not make the DuckDB relation authoritative or durable. The engine still constructs request-local rollup/catalog rows, and exact descriptor matching scans their current content; therefore this phase does not claim zero snapshot-capture cost or O(delta) total query work.

An unchanged retained hit now skips the adapter setup exchange entirely only after complete install-plan validation proves unchanged schema, raw identities, non-raw relations and installed selected-ID membership. Selection is a boolean on already-reserved loaded-batch metadata, not another retained copy of IDs. A raw hit alone is insufficient: switching partial selections or changing catalog/rollup data still installs the necessary state. Even a no-op install refreshes the frontier, lineage and logical accounting and still executes user SQL plus acknowledged rollback. See [BOUNDARY_REDESIGN.md](BOUNDARY_REDESIGN.md) for the explicitly local-only follow-up measurements.

## Snapshot staging and reset

The remaining per-request materialization description applies to the default compatibility path; the retained-input differences are above.

Each child has a private temporary home/current directory and a separate `inputs` subdirectory. Only that input subdirectory and exact selected immutable files are allowed through DuckDB's external-access configuration. The home root, data root, parent directories and inherited working directory are not allowlisted. Startup files, inherited credentials, network reads, extension auto-install/loading and disk spilling remain disabled. The executable and its installed contents are trusted; this is not an OS sandbox for an adversarial executable.

Hot/rollup/catalog payloads use **bounded typed SQL construction** when the whole snapshot fits 128 rows and 32 KiB encoded SQL, with final-script headroom rechecked by the resident worker. Larger inputs or unsupported literals use **copied NDJSON staging**. These are not typed Arrow ingestion or zero-copy; both default materialization paths rebuild request relations. The separately enabled retained-input path stages missing immutable batches once and retains typed relations. The existing borrowed Rust serializers produce newline-delimited JSON; the CLI scanner has an explicit fixed schema and integer widths. A unique request filename avoids stale scanner/cache identities. Payload-free requests use typed empty tables; suitable small catalogs retain the existing bounded SQL-literal path. Catalog NUL strings use scanner fallback. Domain rows retain their existing validation, including rejecting NUL where the model prohibits it.

Fresh temporary input tables, views and catalog macros are created inside a transaction. User output is acknowledged first, then a separate `ROLLBACK` is sent and acknowledged. The current staging file is removed before an eligible child can return to the pool. A fresh-only child is killed/reaped even after successful rollback and staging cleanup. Any error before that point evicts the worker. Rollback cleans up generated request relations; it is not treated as a general connection-state reset. No SQL scripts, protocol tokens, credentials or result files are stored in the allowlisted input directory.

This retains existing JSON result behavior, including DuckDB's UBIGINT string representation, exact supported signed/unsigned integer values, round-trip finite doubles, nullable catalog columns and structured EXPLAIN wrapping. It does not change floating-point aggregation into exact arithmetic.

## Bounds, framing and cancellation

- Admission and idle/live child counts are bounded by runtime capacity.
- SQL is checked against the existing 8 MiB script ceiling before lexical scanning and again after setup construction.
- Encoded NDJSON is capped at **128 MiB per admitted request**, checked before serializer bytes are appended. This also applies to standalone calls. It bounds that encoded buffer, not all caller-owned snapshots, JSON value allocations or process RSS.
- Output uses 8 KiB pipe chunks, a four-message bounded channel and the per-request `max_output_bytes` ceiling. Diagnostics are capped at 256 KiB. Framing has a small additional fixed-size allowance.
- Each pipe exchange ends with a fresh unpredictable UUID acknowledgement emitted by a generated CLI `.print` command. It is not an SQL literal, catalog value, file or persisted script, so user SQL cannot inspect its token through `current_query()` or staged inputs. User output must not be treated as trusted control text; parsing verifies the line boundary and complete suffix, then JSON decoding verifies the result.
- Errors, unexpected EOF, malformed framing/output, cancellation, timeout or output overflow kill/reap the child. Teardown drops the bounded-channel receiver before joining I/O threads so blocked sends cannot deadlock cleanup. Partially constructed workers also own child cleanup.
- The request deadline starts on entry. Cancellation/deadline checks surround staging and parsing and run in the pipe loop at a nominal 2 ms polling interval. Filesystem operations, serialization, thread scheduling and process reaping are cooperative boundaries, not hard real-time guarantees.
- Dropping an async caller waiting on a synchronous task does **not** automatically cancel that task. Its owner must set the explicit `AtomicBool` cancellation token, or rely on the deadline. Runtime destruction happens when its last owner is dropped; active calls retaining an `Arc` finish or cancel before final pool destruction.

DuckDB's `memory_limit` is a logical engine budget. Parent staging, JSON decoding, CLI overhead and aggregate pool RSS are additional: **total RSS and hard OS I/O deadlines are not guaranteed**. No throughput or latency improvement is claimed from correctness probes.

## Observability

`QueryWorkerStats` is serializable and has `spawned`, `reused`, `resets`, `discarded` (`u64`), plus `active` and `idle` (`usize`). Counts saturate rather than wrap:

- `spawned`: successful child constructions.
- `reused`: matching idle-worker acquisitions, including executions that subsequently fail.
- `resets`: successful acknowledged rollback and staging cleanup, including successful fresh-only requests. This does not count or promise a reset of nontransactional connection state.
- `discarded`: workers reaped for capacity-pressure eviction, request failure, or successful fresh-only execution. Final runtime destruction is not observable through a later stats call and is not included.

Phase timers are non-overlapping: `QueryWait` covers admission, filesystem generation/eligibility checks, key selection and capacity-pressure child retirement; `QuerySpawn` covers OS child/I/O construction; `QueryBuild` covers staging/setup construction and, in retained mode, acknowledged adapter data/schema installation; `QueryRun` covers execution and result decoding; `QueryReset` covers rollback acknowledgement and input deletion. The first execution also runs locked configuration/version setup. These are attempted-phase timers, not success counters or hard timing guarantees.

Retained-mode fields add `resident_full_loads`, `resident_delta_loads`, `resident_hits`, `resident_invalidations`, `resident_raw_staged_rows`, `resident_raw_staged_bytes`, `resident_dynamic_loads`, and `resident_dynamic_staged_bytes` counters, plus `resident_idle_rows` and `resident_idle_bytes` gauges. The service exports `varve_query_resident_*` names (counters end in `_total`). A hit means no missing raw batches, not necessarily unchanged catalog/rollup descriptors. Raw staged row counters include copied hot and imported cold rows. Raw staged byte counters now measure temporary/imported Parquet transport sizes on the retained path; historical NDJSON byte counts are a different representation and must not be treated as directly comparable allocation/RSS measurements. `resident_dynamic_loads` counts requests with acknowledged changed non-raw relations, and `resident_dynamic_staged_bytes` counts only bytes encoded for those changed relations. A changed-to-empty relation counts one load and zero bytes; an unchanged empty relation does not count a change. Staging counters count acknowledged, cleaned-up adapter installations; later user SQL failure does not erase that work. Invalidations include retained workers discarded on eviction/failure/fresh-only completion. Idle gauges include charged typed descriptor/schema memory, exclude active workers and are not RSS observations.

Retained-mode witnesses include the no-database exact-content and work-count prefix `query::workers::resident::descriptor_delta_tests`, `src/query/resident_tests.rs`, `src/query/resident_process_tests.rs`, `tests/query_residency.rs`, `tests/query_catalog_exposure.rs`, `tests/residency.rs` and the real HTTP service reuse/delta test. They exercise large-input hit/delta behavior, snapshot inversion/remapping, current derived/catalog state, exact numeric boundaries, permission/cancellation/EOF/reset behavior, checkpoint/cache pressure, compaction/retention, reopen and FileStore WAL-only restoration. See `SEDIMENT_FRONTIER.md` for current integrated qualification; the ledger below is the historical original process-reuse checkpoint, not a new performance claim.

## Acceptance ledger

Verified with small actual-CLI correctness tests, not local load tests or benchmarks. Repair verification: 12 worker integration tests, 7 query unit tests, 15 existing query tests, 8 security tests and 2 projection tests passed (44 checks across focused runs). The strengthened concurrent regression also passed with wrapper-entry PID observations. Owned-file `rustfmt --check` and whitespace checks passed. Other modules were being edited concurrently; this is not whole-repository qualification.

Commands used the configured test profile, `--no-default-features`, and `PATH="$PWD/.tools:$PATH"`: `cargo test --lib query::tests`, `cargo test --test query_workers -- --test-threads=2`, and the existing `--test query --test query_security --test query_projection` targets. Initial worker failures from old child-count/missing-file expectations were inspected: assertions now count the disposable scanner and require permission denial for stale staging access.

| Check | Executable evidence | Status |
| --- | --- | --- |
| Public runtime/stats/metrics contract | `tests/query_workers.rs`, symbol checks | pass |
| Actual exec PID reuse; fresh/absent hot tables and catalogs | `actual_child_reuse_refreshes_exact_hot_catalog_and_empty_relations` | pass |
| Exact rollup/catalog/null/NUL/numeric/cutoff compatibility | `rollups_cutoffs_and_catalog_types_match_standalone_across_reuse` | pass |
| Settings, executable and file-set mismatch replacement | `immutable_file_set_and_settings_changes_replace_child_without_stale_access` | pass |
| Bare/quoted/dynamic stateful SQL and unknown function/grammar use distinct, reaped children without touching a spare matching idle worker | `stateful_quoted_dynamic_and_unknown_sql_use_disposable_children`; AST eligibility unit test | pass |
| Actual same-path atomic executable replacement runs the new wrapper generation, reaps the old child, then reuses the new child | `atomic_same_path_executable_replacement_changes_worker_generation` | pass |
| Capacity-two A/B/A/B reuse, fresh-only eviction preference, concurrent key replacement/admission/cancellation | `capacity_two_keeps_alternating_keys_and_prefers_other_key_for_fresh_eviction`, `concurrent_key_replacement_and_fresh_admission_never_exceed_capacity` | pass |
| Fresh-only output/deadline enforcement and cleanup preserve a matching pooled child | `disposable_workers_share_output_deadline_and_failure_cleanup` | pass |
| Output/diagnostic bounds, marker-like strings, EXPLAIN, errors and EOF | `framing_output_errors_explain_and_eof_never_poison_next_request` | pass |
| Fail-fast admission, explicit cancellation and query deadline | `admission_cancellation_deadline_and_concurrent_drop_cleanup` | pass |
| Concurrent snapshot isolation and last-owner child/temp cleanup | `concurrent_snapshots_are_isolated_and_last_arc_drop_reaps_both_children` | pass |
| Host/network/stdin/traversal/mutation denial; deleted staging not recoverable | `reused_worker_security_denies_host_network_mutation_and_old_staging_paths` | pass |
| Serializer bound checked before mutation; SQL guards/build behavior | seven `query::tests` unit tests | pass |
| Existing standalone query and security behavior | `tests/query.rs` (15), `tests/query_security.rs` (8) | pass |
| Existing engine projection behavior | `tests/query_projection.rs` (2) | pass |

Direct scanners are fresh-only. The prior worker's staging file and private home have been removed before the next call; `read_blob` and `read_json` cannot access that prior allowlist and error. The regression requires denial, not merely an empty result. The original same-worker missing-file `read_blob` behavior (empty array) is no longer the execution path.

This ledger establishes single-node correctness evidence only. Independent integration review, root linting and broader project acceptance remain separate; no cloud, distributed, benchmark or production qualification is claimed.
