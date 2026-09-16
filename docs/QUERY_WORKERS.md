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

The runtime is synchronous and thread-safe. A database owns one runtime and shares its `Arc<crate::metrics::Metrics>` through `with_metrics`. `new` creates private metrics. Use the existing `query_workers` configuration for capacity; no new opt-in, options fields, dependencies, bundled C++, global singleton or service dependency is required. Zero capacity rejects all requests. Once the admission mutex is acquired, occupied capacity rejects rather than accumulating a SQL-work queue. Contention on the key-check/retirement critical section can delay that check; this is not a wait-free admission guarantee.

The existing standalone `query::execute` and `query::execute_with_catalog` functions remain available with per-call subprocess behavior. They share the SQL guard and bounded input serializer. The resident runtime requires absolute selected file paths; the engine supplies pinned absolute paths.

## Exact reuse boundary

A worker's immutable key contains:

- The resolved, canonical executable path and, on Unix, its filesystem generation: device, inode, size, and nanosecond mtime/ctime. No per-query binary hashing is performed.
- `QueryOptions.memory_mb` and `QueryOptions.threads`.
- The sorted, deduplicated set of exact selected immutable file paths.

Only an eligible request may acquire an idle child with an identical key. A key miss uses spare capacity without evicting another key: capacity two can retain A and B across A/B/A/B requests. An idle child is evicted only when the projected active-plus-idle population would exceed capacity. Admission/key selection and necessary retirement are serialized through reaping, so a racing admission cannot spawn against a not-yet-reaped eviction. Active reservations also cover construction and failure teardown. The total child population is bounded by capacity; this is not an unbounded cache of keys. Timeout and output limits are applied anew on every request and do not require replacement. Hot rows, rollups, cutoffs, table names, catalog rows/macros and aggregate aliases are rebuilt on every request, not cached in the key.

The executable and installation must remain immutable during execution; **in-place mutation is unsupported**. Deploy replacements atomically, coordinated with request admission (no concurrent replacement between stat/key selection and process spawn). An atomic replacement completed before admission changes the Unix key even at the same pathname. This is a cheap generation discriminator, not content authentication or an atomic executable-open/exec protocol. Wrapper targets, shared libraries and other installed dependencies are trusted and are not recursively fingerprinted; drain/recreate the runtime when those change. Non-Unix platforms currently use the same bounded fresh-worker execution path but disable pooling because this Unix generation contract is unavailable. Non-Unix behavior has not been exercised by the Unix process tests.

Selected files must remain immutable and pinned until execution returns. No assumption is made that a reused pathname denotes new mutable contents: replacing files in place violates this caller contract. An idle child never receives another request without matching its key. Other idle children can retain their own locked keys but cannot execute a request with a different selection.

This is deliberately narrower than unconditional persistent CLI reuse. A small probe using the actual `.tools/duckdb` (`v2.0.0-alpha41533`) executed:

```sql
SET allowed_paths = [];
SET enable_external_access = false;
SET lock_configuration = true;
SET allowed_paths = ['/tmp/not-an-exposed-input'];
```

DuckDB rejected the final statement: `Cannot change configuration option ""allowed_paths"" - the configuration has been locked`. Unlocking or accumulating previous allowlists is not a safe refresh strategy.

Private hardlinks were considered but are not used. Replacing original Parquet paths with private aliases would break supported direct reads of selected paths unless SQL were rewritten or the original allowlist retained; hardlinks also require same-filesystem support. Keeping exact original paths and replacing children on key mismatch preserves that behavior without broadening filesystem authority or copying cold segments.

## Conservative pooling eligibility

The read-only lexical guard remains an admission restriction, **not** a pooling proof. `ROLLBACK` does not reset DuckDB PRNG state: the pinned actual CLI returns `0.24376333656300356` from `random()` after `BEGIN; SELECT "setseed"(0.25); ROLLBACK;`. Bare, quoted and dynamically evaluated forms therefore cannot execute on reusable children.

The existing `sqlparser` DuckDB parser classifies a deliberately small subset of SELECT/CTE/EXPLAIN syntax. Parsed function identifiers include quoted names. The visitor checks nested expressions, function arguments, filters, windows and subqueries. A short explicit non-mutating builtin list covers basic numeric/string aggregates, date bucketing/conversion, ranking/lag/lead and expressions; locked-setting/catalog introspection (`current_setting`, `duckdb_tables`, `duckdb_functions`) and `range` are explicit exceptions to a purely deterministic-expression subset. Generated zero-argument catalog table macros also qualify: their bodies are generated internally, and the entire catalog is validated before any request SQL executes. Qualified/unknown functions, stateful calls (including `random` itself), dynamic SQL (`query`), direct scanners, unrecognized AST forms and parse failures are **fresh-only**, not newly rejected SQL.

Fresh-only means a newly constructed worker that is **never acquired from or returned to the pool**, even on success. It still uses the runtime's admission slot, private setup/staging, output framing, deadline, cancellation polling, rollback acknowledgement and kill/reap cleanup; it does not delegate to standalone execution. Spare capacity leaves matching reusable idle workers untouched. Under capacity pressure, an incompatible idle key is preferred for eviction; eviction of a matching key may be necessary if it is the only idle candidate.

This classifier is a conservative reuse policy for a trusted local DuckDB installation, not a comprehensive SQL effect system or an SQL/OS sandbox. Existing executable, filesystem and environment trust boundaries still apply. Unsupported syntax and functions remain available through the fresh path subject to the existing read-only/security limits. Tests assert process identity, disposal and stats; they do not use probabilistic random-output inequality.

## Snapshot staging and reset

Each child has a private temporary home/current directory and a separate `inputs` subdirectory. Only that input subdirectory and exact selected immutable files are allowed through DuckDB's external-access configuration. The home root, data root, parent directories and inherited working directory are not allowlisted. Startup files, inherited credentials, network reads, extension auto-install/loading and disk spilling remain disabled. The executable and its installed contents are trusted; this is not an OS sandbox for an adversarial executable.

Hot/rollup/catalog payloads use **copied NDJSON staging**, not typed Arrow ingestion and not zero-copy. The existing borrowed Rust serializers produce newline-delimited JSON; the CLI scanner has an explicit fixed schema and integer widths. A unique request filename avoids stale scanner/cache identities. Payload-free requests use typed empty tables; suitable small catalogs retain the existing bounded SQL-literal path. Catalog NUL strings use scanner fallback. Domain rows retain their existing validation, including rejecting NUL where the model prohibits it.

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

Phase timers are non-overlapping: `QueryWait` covers admission, filesystem generation/eligibility checks, key selection and capacity-pressure child retirement; `QuerySpawn` covers OS child/I/O construction; `QueryBuild` covers staging/setup construction; `QueryRun` covers execution and result decoding; `QueryReset` covers rollback acknowledgement and input deletion. The first execution also runs locked configuration/version setup. These are attempted-phase timers, not success counters or hard timing guarantees.

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
