# varve-client

Asynchronous Rust client for Varve's persistent JSON-RPC 2.0 WebSocket protocol.
It is an independent crate and does not link the Varve storage engine.

## Protocol subset

The client supports `/v1/ws` with subprotocol `varve.v1` and exposes:

- `Client::connect` / `connect_with_config`
- `insert` and atomic `insert_batch`
- `query`, `create_table`, `status`, and `ping`
- bounded graceful `close`

The operator-only `tables`, `policies`, `aggregates`, `jobs`, `checkpoint`, and
`maintain` protocol methods are not currently wrapped. HTTP and PostgreSQL-wire
fallbacks are not implemented.

Rows use signed `i64` microsecond timestamps and serialize them as exact JSON
integer literals. Sequence fields are `u64`. SQL results remain
`serde_json::Value` because DuckDB result types vary and unsigned columns may be
encoded as strings by the server.

## Usage

```toml
[dependencies]
varve-client = "0.1"
```

```rust,no_run
use std::collections::BTreeMap;
use varve_client::{Client, RequestId, Row};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let client = Client::connect("wss://varve.example/v1/ws", "operator-token").await?;
let receipt = client.insert(
    "metrics",
    Row {
        timestamp_us: 1_735_689_600_000_000_i64,
        tenant: "acme".into(),
        series: "cpu".into(),
        value: 42.0,
        tags: BTreeMap::new(),
    },
    RequestId::new("measurement-2025-01-01-1")?,
).await?;
println!("sequence={}", receipt.sequence);
client.close().await?;
# Ok(())
# }
```

Run the included example against a local service:

```sh
VARVE_URL=ws://127.0.0.1:8080/v1/ws \
VARVE_TOKEN="$VARVE_API_TOKEN" \
cargo run --manifest-path clients/rust/Cargo.toml --example basic -- metrics stable-request-id
```

## Delivery and retry semantics

Calls are multiplexed over one persistent connection and responses may arrive
out of order. `ClientConfig` bounds pending calls, aggregate pending bytes, and
message size, and sets connect, authentication, request, and close deadlines.
Cancellation removes the pending correlation entry and releases its call slot.
Serialized request bytes remain charged until both correlation cleanup and
outbound flush/discard have completed; cancellation cannot free byte credit for
frames still queued or buffered by the writer.

Mutation calls are never automatically retried or reconnected. `WriteError`
separates these outcomes:

- `NotSent`: rejected locally before acceptance by the connection task.
- `AdmissionRejected`: server code `-32003`; the write did not commit.
- `Rejected`: a definitive server rejection such as invalid parameters.
- `OutcomeUnknown`: an accepted call timed out/disconnected, returned server
  operation failure `-32000`, or had an unusable result. It may have committed.

Retry an unknown write only with the same `RequestId` and byte-for-byte equivalent
rows. Varve deduplicates the entire write batch by that stable ID.

TLS uses rustls with WebPKI roots and certificate validation enabled. There is no
insecure TLS option. Credentials are accepted only as the authentication argument;
URLs containing user information are rejected, and credentials are never logged.

## Testing

Deterministic protocol fixtures need no server:

```sh
cargo test --manifest-path clients/rust/Cargo.toml --tests
```

The real-service suite is explicit and fails if `VARVE_TEST_BINARY` is missing:

```sh
cargo build --bin varve
VARVE_TEST_BINARY="$(pwd)/target/debug/varve" \
  cargo test --manifest-path clients/rust/Cargo.toml \
  --features service-tests --test real_service -- --nocapture
```

No container or downloaded fixture is required. The SQL assertion requires the
same DuckDB v2 executable expected by the Varve server.
