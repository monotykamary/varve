# Bounded native runtime reuse

Implemented and qualified against the pinned Linux v2 library on Railway; `query_native_reuse = false` remains the default. The full default/fault suites, real callback-detachment and reuse gates, and both real SDK services passed. See [source-bound qualification and limitations](evidence/performance-safety-20260919/native/README.md). This is not a sustained performance result or production certification.

## Selection and compatibility

`Config.query_native_reuse` explicitly opts the `duckdb_library` backend into reuse. It is inert without a native library. CLI selection and `query_retained_inputs` remain independent and unchanged. No dependency, bundled C++ build, quota increase, result cache, rollup substitution or durable format change is introduced.

An eligible request reuses the actual environment, database and connection. It still captures current engine snapshots, binds every selected raw/rollup/catalog input anew, executes SQL and decodes through the existing native JSON adapter. No row, derived value, SQL result or original-file pin is stored in the idle pool.

The existing conservative SQL AST reuse classifier is required. Stateful, unknown, dynamically evaluated and direct-scanner SQL uses a new private session that is never acquired from or returned to idle, but still consumes the same admission slot. A request without a namespace-bearing snapshot, or whose key exceeds the existing SQL-size bound, also uses the fresh path. Invalid inputs still fail existing validation. There is no automatic retry of a failed query or fallback to the CLI.

## EXPLAIN compatibility and qualification

SQL data results require exact native/CLI JSON equality. `EXPLAIN` instead returns the documented single nonempty `plan` string containing backend-rendered, opaque plan text. The CLI stages inputs in `__varve_input` and can show a sequential scan and mark join; native execution uses a custom `__VARVE_RAW_SCAN_0` scanner. Their physical plans are not expected to be byte-identical. No renderer or public output contract changes are needed for native reuse.

The reuse regression compares fresh-native and reused-native EXPLAIN output exactly for identical current inputs, repeats real idle acquisitions, and checks native scan/projection markers. It independently checks the CLI's single-plan shape and staged-input scan. Plan outputs must release callback owners and scanner credit before idle return. A same-key reused-session plan overflow must discard the session without a successful reset, release all credit, and allow a new session to return the original native plan. Namespace/table-set, data-result, security, quota and 130-refresh/3-construction/127-reuse assertions remain intact.

Qualification preserves the original cross-backend EXPLAIN equality failure as evidence of an invalid new test oracle, not a reuse-induced result change. Main explicitly authorized this contract-correct oracle replacement; native rendering is unchanged. Exact execution results and manifests belong to the qualification handoff, not a production or performance guarantee.

## Authority and admission

Each pool belongs to one `NativeRuntime`, its immutable pinned `Api` (path, actual hash, header hash, version) and its database owner. No process-global environment, connection, mutable callback registry or authority union is introduced. Within that pool the key contains the snapshot namespace, memory and thread limits, output and timeout limits, and the exact sorted/deduplicated absolute original-file allowlist. Changing any of those values requires a different session. Configuration is installed once and remains locked; no allowlist is widened or reset in place. Table/column/catalog names and values are request inputs, not retained identity proofs.

The existing `query_workers` capacity includes preparing/active leases and idle sessions. Acquisition is exclusive. Full active capacity fails rather than queues; an idle session is retired under the admission lock before opening a replacement. Raw scanner and snapshot reservations and derived/file pins remain under existing engine budgets. DuckDB memory remains bounded per configured worker, including while idle; this is not an aggregate-process RSS guarantee. A session is retired after 64 successful leases to bound internal catalog/version history even when explicit callback cleanup succeeds.

## Lifetime/reset proof

The pinned header says table-function registration belongs to the database, and destroying a builder does not unregister its function. A fresh connection or SQL rollback alone is therefore not accepted as a lifetime proof.

For reusable execution:

1. Reserve/prepare current Rust input owners and scratch using the existing budgets.
2. Acquire a private, exclusively owned session; apply locked settings if newly opened.
3. Begin a request transaction, register current lifetime-borrowed scanners and create current temporary relations.
4. Execute and fully destroy output/result/chunk/statement handles. The cancellation watcher repeatedly interrupts only this connection and is joined before cleanup.
5. Execute adapter-owned `ROLLBACK`. Reuse requires success and the query's production scanner ledger observing zero live callback owners, owner bytes and active callbacks. These counts cover UserData, BindState, GlobalState and LocalState, and owner credit is released only after deallocation.
6. If that proof is missing, close connection/database/environment before prepared sources and scratch expire. Query/bind/output/reset failure, cancellation and unwind also close rather than reuse. No failed SQL is retried. Only a positively detached session may enter idle.

Dropping the final runtime owner drops all idle handles in connection/database/environment order. An executing synchronous borrow (or engine Arc owner) prevents final runtime destruction until completion. Existing cancellation/deadline semantics remain cooperative, not a hard teardown deadline. Runtime destruction is not a force-abort API.

Actual v2 transaction and catalog cleanup behavior is a mandatory gate: tests require 130 current-input executions to construct exactly three sessions and reuse 127 times. If every rollback leaves registrations alive, the safe close fallback is not an optimization and must not be reported as reuse. Stop for Main's ABI/design decision instead of weakening that assertion or hiding fallback as a hit. Local vendor excerpts show registration through `RunFunctionInTransaction`, but do not provide the actual statement `QueryRunner` implementation; full runtime verification is still outstanding.

## Diagnostics and source tests

`Database::native_query_worker_stats() -> Option<query::QueryWorkerStats>` exposes native-only session counters. Status `native_query` adds `reuse_enabled` and `workers`. Here `spawned` means successful native session opens, `reused` means idle acquisitions (not successes), `resets` means proved detachments, and `discarded` means retirements/errors/fresh completion. Active/idle counts include no retained-input data; all `resident_*` fields are zero. Final drop is not observable through a subsequent stats call. Existing `query_worker_stats` CLI process counters and metrics retain their old meaning, with native active count still reported as before.

Registered module `query::native_reuse_tests` contains source regressions for repeated current raw/catalog/rollup refresh and bounded retirement; actual engine write visibility and reopen; active cold-file pins, retention and idle GC; namespace/table-set changes; exact allowlist/settings replacement; CLI output parity; phase cancellation; failed callback handoffs, overflow and unwind; fresh-only SQL; concurrent capacity/raw quota saturation and cancellation isolation. Tests use channels/phase hooks for scheduling, explicit storage clocks and scoped TempDirs. No new sleeps or probabilistic result assertions. Existing tests are unchanged except the separately authorized inherited fence tuple reflow.

No performance win is claimed. Main owns remote execution, strict lint/format, independent review and before/after source gates. Required evidence includes nonzero test selection and actual reuse counts, not merely a successful build or successful fresh fallback query.
