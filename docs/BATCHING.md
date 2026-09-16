# Inserting one row or an explicit batch

Varve already accepts a **row array as one atomic application batch**. You do not need a SQL INSERT statement, CSV importer, or background scheduler to submit a batch. All rows in that request validate and commit together. Request IDs identify the entire payload, not individual rows.

## JSON CLI

Create a table once, then use the included two-row example:

```sh
varve --data ./varve-data create metrics
varve --data ./varve-data write examples/write.json
```

A batch file has this shape:

```json
{
  "table": "metrics",
  "request_id": "import-2026-09-16-part-0001",
  "rows": [
    {"timestamp_us": 1000000, "tenant": "demo", "series": "cpu", "value": 12.5, "tags": {"host": "one"}},
    {"timestamp_us": 2000000, "tenant": "demo", "series": "cpu", "value": 17.5, "tags": {"host": "one"}}
  ]
}
```

These are historical synthetic timestamps. Tables with lateness or retention policies may reject or expire them; use current event timestamps for live data. Durations and timestamps are **microseconds**, not JavaScript milliseconds.

## HTTP

With the loopback service running and no API token configured:

```sh
curl --fail-with-body -sS http://127.0.0.1:8080/v1/write \
  -H 'Content-Type: application/json' \
  --data-binary @examples/write.json
```

Public endpoints require HTTPS termination and the configured bearer token. See [CLI and HTTP security](CLI.md). Do not put credentials in a URL. HTTP/1.1 can reuse connections; repeatedly starting a new curl process does not demonstrate the throughput of a pooled client.

## Batch boundaries and retries

- One row: supply a one-element `rows` array, or use the SDK's single-insert method.
- Manual batch: supply all intended rows in one `rows` array. A malformed or inadmissible row rejects the request, not just that row.
- Large import: split into bounded chunks with a distinct, deterministic request ID per chunk. Do not send the entire input file as an unbounded request.
- Retry: reuse the **same request ID and identical row payload**. A changed payload with the same ID is a conflict.
- Timed idempotency: tables opting into `idempotency_window_us` require timed IDs and reject expired retries; see [configuration](CONFIGURATION.md).
- Ordering: call/query after the write receipt if you need read-after-write. Merely sending two concurrent requests on the same socket does not order their completion.

Both core `max_batch_rows`/`max_batch_bytes` and service body/message limits apply. The supplied Railway configuration admits at most 1,000 rows and 1 MiB of encoded row data per application batch. JSON envelope/escaping and transport limits can make the effective byte limit smaller. Check the deployed configuration rather than assuming defaults are permanent.

## Single-insert throughput

The shared ingestion coordinator can combine independent pending single inserts into one physical WAL group. That is **group commit**, not acknowledgment on enqueue. A success receipt still follows local WAL fsync, and does not promise S3 has caught up.

Bound concurrent insert calls and await their receipts together. The default shared service admission window is small (`request_workers + request_queue`, normally 2 + 4); the SDK pending limit does not override it. For a 32-call pipeline, explicitly provision a larger shared queue, for example `VARVE_HTTP_REQUEST_QUEUE=64`, and account for the extra bounded request-buffer memory. The Railway template uses that 64-slot queue while keeping SQL workers at two. Awaiting each insert before submitting the next leaves only one outstanding request, so a server cannot manufacture cross-request batching without changing the acknowledgment contract. Prefer explicit application batches when you already have many rows in hand.

A group does not turn independently submitted requests into a user-selected multi-request transaction. Each request retains its own validation, idempotency identity and result. A disconnect or timeout after admission can leave the commit outcome unknown; neither SDK may silently reconnect and replay a mutation with a new ID.

See [persistent protocol](CLIENT_PROTOCOL.md) and [ingestion implementation](INGESTION.md) for the queue, limits and measured group-commit behavior.
