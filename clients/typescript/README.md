# `@monotykamary/varve`

TypeScript ESM client for Varve's persistent JSON-RPC 2.0 WebSocket protocol. It targets modern browsers and Node.js 22+, or any runtime where a compatible `WebSocket` constructor is injected.

## Install

```sh
bun add @monotykamary/varve
```

## API

```ts
import { connect } from "@monotykamary/varve";

const client = await connect("wss://db.example.com/v1/ws", {
  token: process.env.VARVE_API_TOKEN,
  maxPendingRequests: 32,
  maxPendingBytes: 8 * 1024 * 1024,
  connectTimeoutMs: 5_000,
  authTimeoutMs: 5_000,
  requestTimeoutMs: 30_000,
});

await client.createTable("metrics", {});
await client.insert("metrics", {
  timestamp_us: 1_735_689_600_000_000n,
  tenant: "acme",
  series: "cpu",
  value: 0.72,
  tags: { host: "api-1" },
}, "stable-request-id");

await client.insertBatch("metrics", rows, "stable-batch-id");
const result = await client.query("SELECT * FROM metrics LIMIT 10");
const status = await client.status();
await client.ping();
await client.close();
```

The exact surface is:

- `connect(url, options?) -> Promise<VarveClient>`
- `insert(table, row, requestId, requestOptions?)`
- `insertBatch(table, rows, requestId, requestOptions?)`
- `query(sql, requestOptions?)`
- `createTable(name, config?, requestOptions?)`
- `status(requestOptions?)`
- `ping(requestOptions?)`
- `close(closeOptions?)`

Every request option accepts `signal` and `timeoutMs`. `connect` accepts the global browser/Node 22+ `WebSocket` by default and an injectable `WebSocket` constructor for other runtimes and tests.

## Integer and failure semantics

`timestamp_us` accepts a safe integer `number` or a signed-i64 `bigint`. Unsafe numeric inputs, non-finite values, out-of-range timestamps, and invalid tag maps are rejected before transmission. Bigints are encoded as JSON numeric literals, not strings. Responses use `lossless-json`'s `parseNumberAndBigInt`, so JSON integer literals are returned as `bigint`; decimal/exponential values remain `number`. DuckDB unsigned values that the server returns as strings remain strings.

The client never reconnects, retries, or replays a mutation. `VarveAdmissionError` (`-32003`) means the server explicitly rejected the request before admission. A dropped, aborted, locally timed-out, malformed-response, or `-32000` accepted mutation raises `VarveAmbiguousOutcomeError`: it may have committed. The underlying transport, abort, timeout, protocol, or RPC error is preserved as `cause`. Retry writes only with the same Varve request ID and byte-for-byte equivalent logical rows.

All `query(sql)` calls are conservatively classified as potentially mutating, matching the Rust client: the server accepts management SQL such as `CALL varve_create_table(...)`. No lexical read-only guess is made, even for comments or apparent `SELECT` statements. Consequently an accepted `SELECT` timeout raises `VarveAmbiguousOutcomeError` with a `VarveRequestTimeoutError` cause, and an accepted SQL abort has a `VarveAbortError` cause. Inspect server state before retrying SQL or table creation; these calls have no write request ID. A pre-admission abort remains an ordinary `VarveAbortError` (`admitted: false`); `ping` and `status` interruptions retain their ordinary error types. A client-side pending/frame/byte rejection raises `VarveClientLimitError` and was not sent.

Local `local_fsync` acknowledgment is not remote-object-storage durability. Varve v0.1 is single-node; this client does not add distributed or failover guarantees.

## Browser origins and credentials

The bearer token is sent only in the first JSON-RPC frame. The client fixes the subprotocol to `varve.v1` and rejects URLs containing credentials, query parameters, or fragments. Never put a token in a URL or subprotocol.

Browsers send an `Origin` header which scripts cannot override. The Varve server rejects browser origins unless the exact origin is allowlisted with a repeatable `--ws-origin https://app.example.com` argument or comma-separated `VARVE_WS_ORIGINS`. Wildcards, paths, trailing slashes, credentials, and non-HTTP(S) origins are rejected. Any connection carrying a browser `Origin` also requires a configured server API token and valid first-frame authentication, even on loopback with an allowlisted origin. Tokenless loopback access is only for clients without an `Origin` header, not a browser exception. Never allow an untrusted origin.

## Batching and concurrency

Use `insertBatch` when rows are one atomic idempotent batch. For independent single-row writes, bounded `Promise.all` concurrency lets the persistent connection keep several calls outstanding and gives the server ingestion coordinator an opportunity to group commits. An `await` inside a simple loop permits only one outstanding write and inherently prevents that batching opportunity. Keep concurrency at or below `maxPendingRequests` and server admission limits. The concurrent example uses **4** workers: the server defaults to 6 shared HTTP/WebSocket request slots (2 query workers + queue capacity 4), so 16 workers can hit admission errors. For higher concurrency, raise `VARVE_HTTP_REQUEST_QUEUE` (or `--request-queue`) and account for other clients sharing those slots. Also size per-connection `VARVE_WS_MAX_PENDING` (default 32) and `VARVE_WS_MAX_PENDING_BYTES` (default 8 MiB), client pending/frame limits, and ingestion admission budgets; increasing only the client limit does not raise server capacity. See [`examples/manual-batch.ts`](examples/manual-batch.ts) and [`examples/concurrent-inserts.ts`](examples/concurrent-inserts.ts).

Before each send, admission requires the new frame's UTF-8 size to fit within `maxPendingBytes` minus outstanding correlation payload bytes and native WebSocket `bufferedAmount`. This conservatively double-counts existing requests that are both pending and still buffered, and independently bounds the native outbound queue. Abort, timeout, and response cleanup free correlation capacity, but **not** retained outbound credit: it becomes available only as the transport transmits/discards frames. Limits count UTF-8 payload bytes, not JavaScript characters or total process memory. An injected `WebSocket` must expose an accurate nonnegative safe-integer `bufferedAmount` with native semantics; missing, wrongly typed, fractional, negative, unsafe, or throwing counters fail closed. A custom transport that lies about retained bytes cannot provide this bound.

## Development and publication checks

From `clients/typescript`:

```sh
npm ci
npm run typecheck
npm run build
npm test
VARVE_TEST_BINARY=/absolute/path/to/varve npm run test:integration
npm run pack:check
npm audit
```

`test:integration` fails when `VARVE_TEST_BINARY` is absent; unit tests do not claim live-server coverage. Publication is not performed by these checks; a pack dry-run does not verify registry availability, ownership, or installation. See [the local verification ledger](https://github.com/monotykamary/varve/blob/main/clients/typescript/VERIFICATION.md) for local evidence and scope.
