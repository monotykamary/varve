# Persistent client protocol — implementation contract

Varve uses JSON-RPC 2.0 over WebSocket at `/v1/ws`, subprotocol `varve.v1`. HTTP/1.1 already supports connection reuse; WebSocket removes repeated HTTP envelopes and supports multiple outstanding calls. It does not eliminate round trips or make local acknowledgment remotely durable.

First message: `{ "jsonrpc": "2.0", "id": "auth", "method": "auth", "params": { "token": "<operator token>" } }`. Unconfigured loopback accepts an empty token. Success: `{ "jsonrpc": "2.0", "id": "auth", "result": { "protocol": 1 } }`. Tokens never belong in URLs, subprotocol values or logs. Authentication has a bounded deadline. Browser origins require an explicit allowlist; unauthenticated loopback must not become a cross-origin write bypass.

Subsequent request IDs are nonempty ASCII strings up to 128 characters, unique among outstanding calls. No notifications or JSON-RPC batch arrays; a write's `rows` is the explicit atomic data batch. Responses can arrive out of order: same ID and either `result` or `{ "error": { "code": <number>, "message": <sanitized string> } }`.

| Method | Parameters | Result |
| --- | --- | --- |
| `ping` | `{}` | `{ "pong": true }` |
| `status` | `{}` | database status |
| `tables`, `policies`, `aggregates`, `jobs` | `{}` | corresponding HTTP operator endpoint result |
| `create` | `{ "name": "metrics", "config": {} }` | creation result |
| `write` | `{ "table": "metrics", "request_id": "stable-retry-id", "rows": [<Row>] }` | WriteReceipt after local WAL fsync |
| `query` | `{ "sql": "SELECT ..." }` | DuckDB JSON result |
| `checkpoint`, `maintain` | `{}` | operation result |

Rows: `{ timestamp_us: signed-i64, tenant: string, series: string, value: finite-f64, tags: { string: string } }`. Timestamp/sequence integers travel as JSON numeric literals without rounding. TypeScript must preserve integers outside the safe-number range, support bigint timestamps, and reject unsafe number inputs. DuckDB may encode unsigned result columns as strings; preserve that behavior.

Errors: -32700 parse error; -32600 invalid request; -32601 unsupported method; -32602 invalid parameters; -32001 unauthorized; -32003 admission/busy; -32000 operation failure. An operation failure or dropped/timed-out accepted write may have committed. Never automatically replay mutations: retry with the same request ID and identical payload. Explicitly rejected queue admission did not commit.

The WebSocket `query` method can also execute management `CALL` operations. Both SDKs conservatively preserve mutation ambiguity for interrupted SQL, including SELECT calls; management operations without an idempotency key require state inspection before retrying. Cancellation removes a local correlation, not a frame already queued for transmission: retained outbound bytes remain charged until flushed or discarded.

Both clients expose connect, insert(table, row, requestId), insertBatch(table, rows, requestId), query, createTable, status, ping, close. All writes share one ingestion coordinator. Bound in-flight calls, frames and deadlines; clean up on disconnect. No silent mutation reconnect/replay. PostgreSQL-wire is separate, with a documented subset: not PostgreSQL storage or SQL-dialect compatibility.
