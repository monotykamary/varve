# Varve CLI and local service

The `varve` binary opens one local database directory at a time. Every command takes the common options before its subcommand:

```text
varve --data <PATH> [--config <JSON>] [--remote-dir <PATH> | --s3] <COMMAND>
```

`--config` reads a `Config` JSON object; omitted fields use their defaults. `--remote-dir` selects the filesystem remote store. `--s3` selects `S3Store::from_env()` and its `VARVE_S3_*`/AWS environment configuration. The two remote options are mutually exclusive. Configuration and table JSON inputs are capped at 64 KiB; batch CLI input is capped relative to `max_batch_bytes` before parsing. JSON input paths may be `-` to read stdin; do not use stdin for two inputs in the same invocation.

## Commands

```text
varve --data <PATH> init
varve --data <PATH> create <NAME> [--table-config <JSON>]
varve --data <PATH> write <JSON> [--now-us <MICROSECONDS>]
varve --data <PATH> query <SQL>
varve --data <PATH> scan <TABLE> [--start-us <N>] [--end-us <N>] [--tenant <S>] [--series <S>]
varve --data <PATH> status
varve --data <PATH> checkpoint
varve --data <PATH> maintain [--now-us <MICROSECONDS>]
varve --data <PATH> [REMOTE] ship
varve --data <PATH> compact
varve --data <PATH> rollups <TABLE>
varve --data <PATH> [REMOTE] vacuum-remote
varve --data <PATH> [REMOTE] remote-head
varve --data <PATH> [REMOTE] recover-remote-lock --owner <UUID> --confirm-owner-stopped
varve --data <PATH> [REMOTE] restore
varve --data <PATH> serve [--port <PORT>] [--bind 127.0.0.1] [--allow-remote]
```

`remote-head` inspects the remote head without opening the database. `recover-remote-lock` requires stopping the previous owner and supplying its UUID and confirmation flag; restore to a new directory afterwards. `vacuum-remote` removes objects unreachable from the protected current head.

`init` and `restore` print status. `create` prints its creation sequence. `write` prints a `WriteReceipt`. `query`, `scan`, `status`, `maintain`, `ship`, and `compact` print their serializable API results as JSON. `checkpoint` prints `{"ok":true}`. Errors go to stderr and return a nonzero exit status.

The write input is a single JSON object, keeping rows out of process arguments:

```json
{
  "table": "metrics",
  "request_id": "batch-0001",
  "rows": [
    {
      "timestamp_us": 1730000000000000,
      "tenant": "example",
      "series": "cpu",
      "value": 0.42,
      "tags": {"host": "local"}
    }
  ]
}
```

`--now-us` is accepted only by explicit `write` and `maintain` commands. It makes lateness and policy boundaries deterministic. Without it, and for service requests or scheduler ticks, Varve uses system microseconds since the Unix epoch.

`query` executes one trusted, read-only analytical SQL statement through the configured DuckDB v2 executable. It is not an untrusted SQL sandbox.

## HTTP service

`serve` remains loopback-only by default. A non-loopback address is accepted only when all of the following are true:

1. `--bind` names the non-loopback address.
2. `--allow-remote` is present.
3. `VARVE_API_TOKEN` contains 32 to 4096 visible ASCII bytes.

The token is read only from the environment. It is never accepted as a CLI argument, persisted in database configuration, returned by status, or logged. When `VARVE_API_TOKEN` is set, including on loopback, every endpoint except `GET /health` and `GET /ready` requires exactly one `Authorization: Bearer <token>` header. Comparison is performed against a BLAKE3 token hash in constant time. This is a single trusted-operator credential, not user identity, tenant isolation, or an untrusted SQL sandbox. Rotate it by restarting the process with a new environment value. Terminate TLS in an isolated trusted proxy/platform network; the service itself speaks HTTP/1.1.

The listening port comes from `--port`; if omitted, `PORT` is used for Railway compatibility. A public Railway-style invocation is:

```text
VARVE_API_TOKEN='<at-least-32-byte-secret>' \
  varve --data /var/lib/varve serve --bind 0.0.0.0 --allow-remote
```

Use one process/replica per persistent data directory. Public deployment does not change local-fsync semantics, create distributed failover, or make trusted SQL safe for mutually untrusted tenants.

### Routes

| Method | Route | Authentication | Body/response |
| --- | --- | --- | --- |
| `GET` | `/health` | none | Minimal process liveness JSON |
| `GET` | `/ready` | none | Nonblocking readiness; 503 when state is busy/fenced, no disk walk or operator worker/queue use |
| `GET` | `/v1/status` | bearer when configured | Database status JSON |
| `GET` | `/v1/tables` | bearer when configured | `SELECT * FROM varve_tables()` |
| `GET` | `/v1/policies` | bearer when configured | `SELECT * FROM varve_policies()` |
| `GET` | `/v1/aggregates` | bearer when configured | `SELECT * FROM varve_continuous_aggregates()` |
| `GET` | `/v1/jobs` | bearer when configured | `SELECT * FROM varve_jobs()` |
| `GET` | `/metrics` | bearer when configured | Numeric Prometheus text metrics |
| `GET` | `/v1/diagnostics/ingest` | bearer when configured | Bounded opt-in ingestion group traces; `VARVE_INGEST_TRACE_CAPACITY`, default 0 |
| `POST` | `/v1/tables` | bearer when configured | `{"name":"metrics","config":{...}}` (`config` may be omitted) |
| `POST` | `/v1/write` | bearer when configured | The write object shown above |
| `POST` | `/v1/query` | bearer when configured | `{"sql":"SELECT ..."}` or a core-whitelisted management `CALL` |
| `POST` | `/v1/maintain` | bearer when configured | `{}`; one manual maintenance pass |

Authentication and header checks happen before a request body is read. Every POST requires exactly one `Content-Type: application/json`. Requests carrying `Origin` are rejected; the service emits no permissive CORS policy. Ambiguous duplicate authorization, content, transfer, or host headers are rejected. Chunked request bodies are accepted only under the same byte cap and body deadline as fixed-length bodies; trailers are rejected.

### Service controls

Service controls are CLI flags with environment fallbacks. They are intentionally separate from the persisted `Config` in `model.rs`.

| CLI flag | Environment | Default |
| --- | --- | --- |
| `--port` | `PORT` | required from one source |
| `--max-connections` | `VARVE_HTTP_MAX_CONNECTIONS` | `64` |
| `--request-workers` | `VARVE_HTTP_REQUEST_WORKERS` | `Config.query_workers` |
| `--request-queue` | `VARVE_HTTP_REQUEST_QUEUE` | twice the request workers |
| `--max-body-bytes` | `VARVE_HTTP_MAX_BODY_BYTES` | `4 MiB` |
| `--header-timeout-ms` | `VARVE_HTTP_HEADER_TIMEOUT_MS` | `5000` |
| `--body-timeout-ms` | `VARVE_HTTP_BODY_TIMEOUT_MS` | `10000` |
| `--request-timeout-ms` | `VARVE_HTTP_REQUEST_TIMEOUT_MS` | `60000` |
| `--connection-timeout-ms` | `VARVE_HTTP_CONNECTION_TIMEOUT_MS` | `65000` |
| `--shutdown-timeout-ms` | `VARVE_HTTP_SHUTDOWN_TIMEOUT_MS` | `10000` |

The effective body cap is the smallest of `--max-body-bytes`, 4 MiB, and `Config.max_batch_bytes`, and is enforced while streaming independently of `Content-Length`. Header, body and whole-request deadlines limit slow clients.

At response completion, an ordinary HTTP response is marked `Connection: close` during the final `min(header timeout, connection lifetime / 2)` of the absolute connection lifetime (at age 60 seconds with the unchanged 65-second lifetime and 5-second header defaults). Hyper finishes its body/framing before EOF and does not admit a later request on that connection; the triggering write is neither rejected nor replayed. Before that window, HTTP reuse remains enabled. Valid WebSocket upgrades retain `Connection: upgrade` and their separate session bounds. The original hard lifetime still retires unused/idle connections and bounds stalled responses; an active request/response has at most the request timeout plus header timeout to drain after expiry before forced closure. No post-expiry admission window is added. Idle-close races, stalled drains and network/receipt loss can still leave a write outcome unknown: reconnect and explicitly retry the same stable request ID with identical rows, never assume EOF means rollback.

HTTP connections, accepted request slots, and blocking workers are independently bounded. DuckDB queries, other database calls and scheduler ticks run on blocking workers rather than Tokio reactor threads. Writes now wait for the shared ingestion coordinator's dedicated batching thread instead of occupying a query worker. Its conservative pending/group byte limits also apply; see [ingestion](INGESTION.md). Queue saturation returns `503`; body deadline and size failures return `408` and `413`; authentication failures return `401`; origin rejection returns `403`; malformed JSON returns `400`; and unsupported media types return `415`.

The protected `/metrics` endpoint exposes request/error/auth/timeout/resource counters and numeric database gauges without labels derived from user input. It does not expose the API token or error strings.

The background scheduler calls `Database::tick` every `Config.maintenance_interval_ms`; manual `POST /v1/maintain` remains a direct maintenance pass. Scheduler failures are logged and do not stop ingestion.

SIGINT and SIGTERM stop acceptance, notify live network connections to drain and stop the scheduler. Network tasks have the configured shutdown timeout; accepted ingestion is then drained and joined, so a stalled native fsync can extend total shutdown beyond that network deadline. Disconnecting a client does not cancel an admitted write. A platform hard-kill can leave an unacknowledged outcome unknown; an already acknowledged local write retains its WAL/fsync recovery guarantees. The service does not turn local acknowledgement into remote durability, and loss of the local disk can still lose an unshipped tail.

For persistent connection setup, heartbeat/origin controls, ingestion environment variables and the opt-in PostgreSQL-wire subset, see [TRANSPORTS.md](TRANSPORTS.md). For atomic JSON batches, see [BATCHING.md](BATCHING.md).
