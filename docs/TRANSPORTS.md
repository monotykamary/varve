# Persistent transports

The authoritative JSON wire contract is [CLIENT_PROTOCOL.md](CLIENT_PROTOCOL.md).
These are experimental single-node operator transports, not a hostile multi-tenant
SQL sandbox or a distributed durability guarantee.

## HTTP and WebSocket

`varve --data ./data serve --port 8080` serves HTTP and `/v1/ws` on the same port.
HTTP/1.1 supports connection reuse; this implementation enables keep-alive (within
its connection deadline). WebSocket removes repeated HTTP envelopes and allows
multiple outstanding RPCs. It does **not** eliminate a TCP handshake per HTTP
request—reused HTTP connections do not need one—or eliminate round trips.

Use HTTPS/WSS via the deployment TLS proxy for remote clients. Plain HTTP/WS does
not encrypt the operator token. Existing `--allow-remote` plus `VARVE_API_TOKEN`
requirements remain unchanged. The Railway deployment exposes only this HTTP port;
there is no PostgreSQL listener unless separately enabled.

Request exactly subprotocol `varve.v1`. Send the documented `auth` request as the
first text message, within 5 seconds. `VARVE_API_TOKEN` is the shared operator
credential, 32–4096 visible ASCII bytes. It must never appear in URLs, subprotocol
values, logs or command-line examples. Authentication failure closes the socket.
Unauthenticated loopback clients may use an empty token only with no Origin header.
Browser requests require **both** a configured token and an explicit exact Origin:

```sh
varve --data ./data serve --port 8080 --ws-origin https://app.example
```

Repeat `--ws-origin`, or set comma-separated `VARVE_WS_ORIGINS`. No wildcards,
`null` origins, trailing slashes, paths or implicit localhost exceptions. HTTP's
existing rejection of browser-origin requests remains unchanged. HTTP authenticates
before reading request bodies. WebSocket upgrades do not authenticate from HTTP
headers: an Authorization header or any URL query is rejected.

Supported methods are exactly `ping`, `status`, `tables`, `policies`, `aggregates`,
`jobs`, `create`, `write`, `query`, `checkpoint`, and `maintain`, following first-frame
`auth`. No batches or notifications. IDs are nonempty ASCII strings, at most 128
bytes, unique while outstanding. Responses can be out of order. A duplicate pending
ID gets a null-ID invalid-request error and disconnect, avoiding two indistinguishable
responses for the same ID. Binary messages disconnect. RFC WebSocket ping/pong is
supported in addition to the JSON `ping` method.

Both HTTP and WebSocket writes submit to **one shared Ingestor**. Neither path
occupies a blocking query worker while awaiting the group-commit receipt. Successful
receipts mean local WAL fsync, not queue admission or remote/S3 durability. There is
no automatic mutation replay. Reconnect and explicitly retry with the same stable
`request_id` and identical rows after an ambiguous outcome.

`-32003` is used only when a request was certainly rejected before admission:
transport slots/bytes/count or `Ingestor::submit` rejection. Accepted operation
failure, receipt loss, result-too-large and deadlines return `-32000`: a mutation
**may have committed**. Disconnect does not roll back accepted work.

### Bounds and lifecycle

| Environment | Default | Effect |
| --- | --- | --- |
| `VARVE_HTTP_MAX_CONNECTIONS` | 64 | HTTP and upgraded WS connections share permits |
| `VARVE_HTTP_REQUEST_WORKERS` | database `query_workers` (2) | Shared blocking operation permits across HTTP/WS/PG |
| `VARVE_HTTP_REQUEST_QUEUE` | 2 × workers | Additional shared in-flight slots |
| `VARVE_HTTP_MAX_BODY_BYTES` | 4 MiB | HTTP body, WS frame/message/response, PG query packet; clamped to database batch bytes and 4 MiB |
| `VARVE_HTTP_BODY_TIMEOUT_MS` | 10000 | HTTP body, WS sends, PG packet completion after its first byte |
| `VARVE_HTTP_REQUEST_TIMEOUT_MS` | 60000 | Per-operation accepted deadline |
| `VARVE_HTTP_CONNECTION_TIMEOUT_MS` | 65000 | Retire HTTP keepalive; drain an active request for at most request timeout + header timeout; WS uses heartbeat, authenticated PG may remain idle |
| `VARVE_HTTP_SHUTDOWN_TIMEOUT_MS` | 10000 | Network-task drain budget |
| `VARVE_WS_AUTH_TIMEOUT_MS` | 5000 | WS first frame and PG startup/SCRAM deadline |
| `VARVE_WS_MAX_PENDING` | 32 | Per-WS outstanding requests (hard maximum 1024) |
| `VARVE_WS_MAX_PENDING_BYTES` | 8 MiB | Per-WS sum of outstanding encoded request bytes (hard maximum 64 MiB) |
| `VARVE_WS_HEARTBEAT_MS` | 30000 | Server ping interval; disconnect on missing matching pong next tick |
| `VARVE_INGEST_QUEUE_CAPACITY` | 1024 | Shared bounded ingestion queue |
| `VARVE_INGEST_MAX_PENDING_BYTES` | 16 MiB | Shared ingestion admission accounting |
| `VARVE_INGEST_MAX_GROUP_REQUESTS` | 128 | Group request cap |
| `VARVE_INGEST_MAX_GROUP_ROWS` | 10000 | Group row cap; engine caps still apply |
| `VARVE_INGEST_MAX_GROUP_BYTES` | 4 MiB | Group byte cap; engine caps still apply |
| `VARVE_INGEST_MAX_DELAY_MS` | 2 | Maximum grouping delay (0 disables intentional waiting) |

Lifetime expiry disables HTTP keepalive rather than cancelling an active response immediately. A stalled drain is forcibly closed after the additional bounded grace period. `varve_http_connection_timeouts_total` counts retirement triggers, not necessarily failed requests. WebSocket upgrade and shutdown handling keep their separate bounds.

The existing HTTP CLI options override their corresponding environment values.
Global in-flight slots also cap outstanding encoded requests across connections;
per-connection count/byte caps do not replace global admission. Query subprocess
memory, execution time and output retain the database query limits. Outbound WS
messages are bounded and writes have deadlines. If even a sanitized error cannot
fit the configured outbound cap, the socket closes instead of exceeding the cap.
Logical limits are not hard RSS quotas. `/metrics` includes `varve_ingest_*` counters/gauges from the shared ingestor.

SIGINT/SIGTERM stops admission, closes persistent sockets, drains network tasks, then
calls `Ingestor::shutdown` to drain and join accepted writes. The ingestion join is
not forcibly cancelled at the network drain deadline: fsync stalls can extend
shutdown. Blocking operations already started may outlive a disconnected caller;
their worker permits remain held until they finish.

## Optional PostgreSQL v3 simple-query endpoint

The maintained `pgwire` crate implements startup, SCRAM-SHA-256 authentication and
result framing. It is **off by default**, separate-port and loopback-only:

```sh
# Supply VARVE_API_TOKEN through your secret manager/environment first.
varve --data ./data serve --port 8080 --pg-port 5433
psql 'host=127.0.0.1 port=5433 user=varve dbname=varve sslmode=disable' -W -c 'SELECT 1 AS answer'
```

`VARVE_PG_PORT` is the environment alternative. `--pg-bind` defaults to `127.0.0.1`.
Any non-loopback address fails startup—even with `--allow-remote`. TLS for this
listener is not implemented, so it must not be publicly bound/proxied as if it were
a TLS-protected database. SCRAM avoids sending cleartext credentials, but does not
encrypt query contents/results. A strong `VARVE_API_TOKEN` is required even on
loopback; the username is `varve`. The startup database name is not a separate
namespace/tenant and is ignored. Authentication state is separate per connection.

### Exact supported subset

- Standard protocol-v3 **simple Query** messages and repeated queries on a connection;
  `psql -c`, node-postgres `client.query('SELECT ...')` without parameters, and
  tokio-postgres `simple_query` are the intended interfaces.
- A conservative `sqlparser` DuckDB AST gate accepts one SELECT/WITH or EXPLAIN
  query before invoking the existing database adapter. This rejects management
  CALLs before the engine can intercept them; the engine's lexical read-only
  guard still applies. Unsupported parser syntax fails closed. No SQL interpolation
  or PostgreSQL-to-DuckDB rewriting is performed.
- Nonempty tabular results expose every field as PostgreSQL **TEXT (OID 25)**. NULL
  remains SQL NULL, integers preserve exact decimal strings, strings remain strings,
  other JSON values become their JSON text. The JSON adapter loses column order;
  object-key order is returned. Empty results have no column metadata, because the
  current DuckDB JSON adapter does not supply a schema for zero rows.
- No management writes, storage/dialect compatibility, PostgreSQL catalogs,
  transactions, multiple statements, COPY, extended/prepared/parameterized queries,
  portals or cancellation protocol. Unsupported simple statements return SQLSTATE
  `0A000`; unsupported frontend message types return an explicit error then close.
  Normal query errors leave the simple-query session usable. A psql shell's query
  input works, but psql `\d`/other PostgreSQL catalog meta-commands are not promised.

Limits: independent connection permits equal to `VARVE_HTTP_MAX_CONNECTIONS`, shared
operation workers/slots, at most 10000 bytes per startup/auth packet, bounded startup
deadline, configured query frame bytes, and 1024 output fields. Length is checked
**before allocation and pgwire decode**, rather than accepting pgwire's general
large-packet default. Startup, partially received packets, query execution and
output writes have deadlines. Authenticated sessions may remain idle until disconnect/shutdown while
holding their bounded connection permits; packet completion is timed only after
the first byte arrives. A too-large frame or stalled partial packet is dropped. TLS,
real result schema/type propagation and extended parameter binding are explicit
blockers to broader PostgreSQL compatibility, not advertised functionality.

## Transport acceptance ledger

Owned executable evidence is `tests/transports.rs`, plus existing
`tests/service.rs` and `tests/security.rs` for HTTP regressions.

| Check | Evidence | Status |
| --- | --- | --- |
| Exact auth/subprotocol/origins; no loopback browser bypass | Real WS handshake and first-frame tests | pass locally |
| Exact methods, i64, fsync receipt, HTTP/WS retry identity | Real HTTP + WS + DuckDB test | pass locally |
| Out-of-order IDs; writes do not occupy query workers | File-barrier query fixture, both write transports | pass locally |
| Frames, pending bytes/count, invalid IDs/envelopes, disconnect | Real WS tests including oversized response/error cap | pass locally |
| Accepted deadlines ambiguous, ping/pong, shutdown | Real process/socket tests, accepted-write drain/reopen | pass locally |
| PG management CALL cannot mutate; authenticated idle vs partial packet | Valid CALL/no-sequence-change and standard-client proxy regressions | pass locally |
| Standard PG client + SCRAM failure + explicit subset + frame bounds | `tokio-postgres::simple_query`, raw oversized startup, public-bind refusal | pass locally |
| Existing HTTP auth-before-body and lifecycle | Existing HTTP/security suites | pass locally (5 service + 4 security tests) |

Verified locally: 17 transport tests across the full suite and final focused
outbound-cap run; 5 service and 4 security regression tests; strict
`cargo clippy --bin varve --test transports -- -D warnings`; owned-path rustfmt.
Main owns full-workspace verification and publication; no production-readiness
claim follows from local tests.
