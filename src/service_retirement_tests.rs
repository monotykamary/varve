//! Deterministic HTTP retirement probes against the production Hyper connection.
use super::*;
use serde_json::Value;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::task::JoinHandle;
use tokio::time::advance;

// Keep Tokio from auto-advancing while the real ingestion thread does local fsync.
// Only explicit advance calls move the paused clock. Drop also cleans up on panic.
struct ManualClock(JoinHandle<()>);
impl ManualClock {
    fn new() -> Self {
        Self(tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        }))
    }
}
impl Drop for ManualClock {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn state() -> (TempDir, Arc<State>) {
    let dir = TempDir::new().unwrap();
    let db = Database::open(dir.path(), Config::default()).unwrap();
    db.create_table("metrics", TableConfig::default()).unwrap();
    let ingestor = Ingestor::new(
        db.clone(),
        IngestConfig {
            max_delay: Duration::ZERO,
            ..Default::default()
        },
    )
    .unwrap();
    let state = Arc::new(State {
        db,
        ingestor,
        ws: transport::Settings::resolve(Vec::new()).unwrap(),
        auth: Auth {
            token_hash: Some(*blake3::hash(b"retirement-test-token").as_bytes()),
        },
        workers: Arc::new(Semaphore::new(2)),
        slots: Arc::new(Semaphore::new(4)),
        max_body_bytes: HARD_MAX_BODY_BYTES,
        body_timeout: Duration::from_millis(DEFAULT_BODY_TIMEOUT_MS),
        request_timeout: Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS),
        metrics: Arc::new(Metrics::default()),
    });
    (dir, state)
}

async fn connect(
    state: &Arc<State>,
    header_ms: u64,
    lifetime_ms: u64,
) -> (DuplexStream, watch::Sender<bool>, JoinHandle<Result<()>>) {
    connect_sized(state, header_ms, lifetime_ms, 8192).await
}

async fn connect_sized(
    state: &Arc<State>,
    header_ms: u64,
    lifetime_ms: u64,
    capacity: usize,
) -> (DuplexStream, watch::Sender<bool>, JoinHandle<Result<()>>) {
    let (client, server) = tokio::io::duplex(capacity);
    let (shutdown, rx) = watch::channel(false);
    let state = state.clone();
    let (started, ready) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let connection = serve_connection(
            server,
            state,
            rx,
            Duration::from_millis(header_ms),
            Duration::from_millis(lifetime_ms),
        );
        tokio::pin!(connection);
        assert!(futures_util::poll!(&mut connection).is_pending());
        started.send(()).unwrap();
        connection.await
    });
    // Establish the production timer before the test can advance time.
    ready.await.unwrap();
    (client, shutdown, task)
}

async fn headers(client: &mut DuplexStream) -> String {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        assert!(bytes.len() < 8192, "unbounded response headers");
        bytes.push(client.read_u8().await.expect("EOF before complete headers"));
    }
    String::from_utf8(bytes).unwrap().to_ascii_lowercase()
}

async fn response(client: &mut DuplexStream) -> (String, Value) {
    let head = headers(client).await;
    let lengths: Vec<_> = head
        .lines()
        .filter_map(|line| line.strip_prefix("content-length: "))
        .collect();
    assert_eq!(lengths.len(), 1, "exact Content-Length: {head}");
    assert!(!head.contains("transfer-encoding:"));
    let mut body = vec![0; lengths[0].parse::<usize>().unwrap()];
    client.read_exact(&mut body).await.unwrap();
    (head, serde_json::from_slice(&body).unwrap())
}

async fn health(client: &mut DuplexStream) -> String {
    client
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let (head, body) = response(client).await;
    assert!(head.starts_with("http/1.1 200"));
    assert_eq!(body, json!({"ok": true}));
    head
}

fn batch(id: &str) -> String {
    json!({"table":"metrics", "request_id":id, "rows":[
        {"timestamp_us":1,"tenant":"t","series":"s","value":42.0,"tags":{}},
        {"timestamp_us":2,"tenant":"t","series":"s","value":43.0,"tags":{}}
    ]})
    .to_string()
}

async fn admit_write(client: &mut DuplexStream, body: &str) {
    client.write_all(format!(
        "POST /v1/write HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer retirement-test-token\r\nContent-Type: application/json\r\nContent-Length: {}\r\nExpect: 100-continue\r\n\r\n", body.len()
    ).as_bytes()).await.unwrap();
    // Hyper only sends this when the authenticated handler polls Incoming.
    assert_eq!(headers(client).await, "http/1.1 100 continue\r\n\r\n");
}

async fn eof(client: &mut DuplexStream) {
    assert_eq!(client.read(&mut [0]).await.unwrap(), 0, "expected EOF");
}

#[tokio::test(start_paused = true)]
async fn completion_window_write_has_close_receipt_and_no_next_admission() {
    let _clock = ManualClock::new();
    let (dir, state) = state();
    let (mut client, _shutdown, task) = connect(&state, 5_000, 65_000).await;
    assert!(!health(&mut client).await.contains("connection: close"));
    advance(Duration::from_millis(59_999)).await;
    assert!(!health(&mut client).await.contains("connection: close"));
    let body = batch("terminal");
    admit_write(&mut client, &body).await;
    assert_eq!(state.metrics.inflight_requests.load(Ordering::Relaxed), 1);
    advance(Duration::from_millis(1)).await;
    // This write was admitted BEFORE the window, but completes AT its boundary.
    // A pipelined write must not be admitted after the terminal response.
    let next = batch("must-not-admit");
    client.write_all(format!(
        "{body}POST /v1/write HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer retirement-test-token\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{next}", next.len()
    ).as_bytes()).await.unwrap();
    let (head, receipt) = response(&mut client).await;
    assert!(head.starts_with("http/1.1 200"), "{head}{receipt}");
    assert_eq!(receipt["durability"], "local_fsync");
    assert_eq!(receipt["duplicate"], false);
    assert!(
        head.contains("connection: close\r\n"),
        "missing proactive close handshake: {head}"
    );
    eof(&mut client).await;
    task.await.unwrap().unwrap();
    assert_eq!(state.metrics.requests_total.load(Ordering::Relaxed), 3);
    assert_eq!(
        state
            .metrics
            .connection_timeouts_total
            .load(Ordering::Relaxed),
        0
    );

    // Only the caller explicitly retries the identical stable ID on a new stream.
    let (mut retry, shutdown, task) = connect(&state, 5_000, 65_000).await;
    admit_write(&mut retry, &body).await;
    retry.write_all(body.as_bytes()).await.unwrap();
    let (head, duplicate) = response(&mut retry).await;
    assert!(!head.contains("connection: close"));
    assert_eq!(duplicate["duplicate"], true);
    assert_eq!(duplicate["sequence"], receipt["sequence"]);
    shutdown.send(true).unwrap();
    eof(&mut retry).await;
    task.await.unwrap().unwrap();
    state.ingestor.shutdown().unwrap();
    drop(state);
    let reopened = Database::open(dir.path(), Config::default()).unwrap();
    let rows = reopened.scan("metrics", None, None, None, None).unwrap();
    let expected: WriteInput = serde_json::from_str(&body).unwrap();
    assert_eq!(rows.len(), expected.rows.len());
    for (stored, expected) in rows.iter().zip(expected.rows) {
        assert_eq!(stored.row, expected);
    }
}

#[tokio::test(start_paused = true)]
async fn idle_and_unused_connections_keep_the_original_hard_expiry() {
    let _clock = ManualClock::new();
    let (_dir, state) = state();
    for idle in [false, true] {
        // A header budget larger than lifetime also exercises the unused case.
        let (mut client, _shutdown, task) = connect(&state, 70_000, 65_000).await;
        if idle {
            assert!(!health(&mut client).await.contains("connection: close"));
        }
        advance(Duration::from_millis(64_999)).await;
        let mut byte = [0];
        {
            let read = client.read(&mut byte);
            tokio::pin!(read);
            assert!(futures_util::poll!(&mut read).is_pending());
        }
        assert!(!task.is_finished());
        advance(Duration::from_millis(1)).await;
        eof(&mut client).await;
        task.await.unwrap().unwrap();
    }
    assert_eq!(
        state
            .metrics
            .connection_timeouts_total
            .load(Ordering::Relaxed),
        2
    );
    state.ingestor.shutdown().unwrap();
}

#[tokio::test(start_paused = true)]
async fn short_lifetime_keeps_initial_reuse_and_drains_active_write_or_shutdown() {
    let _clock = ManualClock::new();
    let (_dir, mut state) = state();
    let settings = Arc::get_mut(&mut state).unwrap();
    settings.request_timeout = Duration::from_millis(2_000);
    settings.body_timeout = Duration::from_millis(1_800);
    for shutdown_first in [false, true] {
        let (mut client, shutdown, task) = connect(&state, 3_500, 2_500).await;
        assert!(!health(&mut client).await.contains("connection: close"));
        advance(Duration::from_millis(1_800)).await;
        let body = batch(if shutdown_first { "shutdown" } else { "expiry" });
        admit_write(&mut client, &body).await;
        if shutdown_first {
            shutdown.send(true).unwrap();
        }
        advance(Duration::from_millis(1_100)).await;
        client.write_all(body.as_bytes()).await.unwrap();
        let (head, receipt) = response(&mut client).await;
        assert!(head.starts_with("http/1.1 200"));
        assert!(head.contains("connection: close"));
        assert_eq!(receipt["durability"], "local_fsync");
        eof(&mut client).await;
        task.await.unwrap().unwrap();
    }
    state.ingestor.shutdown().unwrap();
    assert_eq!(
        state
            .db
            .scan("metrics", None, None, None, None)
            .unwrap()
            .len(),
        4
    );
}

#[tokio::test(start_paused = true)]
async fn stalled_response_does_not_extend_the_hard_drain_budget() {
    let _clock = ManualClock::new();
    let (_dir, mut state) = state();
    Arc::get_mut(&mut state).unwrap().request_timeout = Duration::from_millis(2_000);
    let (mut client, _shutdown, task) = connect_sized(&state, 3_500, 2_500, 1).await;
    client
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap(), b'H');
    // Keep the output blocked, including throughout the original expiry drain.
    advance(Duration::from_millis(2_500)).await;
    while state
        .metrics
        .connection_timeouts_total
        .load(Ordering::Relaxed)
        == 0
    {
        tokio::task::yield_now().await;
    }
    advance(Duration::from_millis(5_499)).await;
    assert!(!task.is_finished());
    advance(Duration::from_millis(1)).await;
    task.await.unwrap().unwrap();
    let mut remainder = Vec::new();
    client.read_to_end(&mut remainder).await.unwrap();
    assert_eq!(
        remainder.len(),
        1,
        "only the bounded duplex byte remains before EOF"
    );
    state.ingestor.shutdown().unwrap();
}

#[tokio::test(start_paused = true)]
async fn retirement_arithmetic_and_upgrade_exclusion() {
    for (lifetime, header, expected) in [
        (
            Duration::from_millis(65_000),
            Duration::from_millis(5_000),
            Duration::from_millis(60_000),
        ),
        (
            Duration::from_millis(1),
            Duration::MAX,
            Duration::from_micros(500),
        ),
        (
            Duration::MAX,
            Duration::MAX,
            Duration::MAX - Duration::MAX / 2,
        ),
        (
            Duration::from_millis(u64::MAX),
            Duration::from_millis(5_000),
            Duration::from_millis(u64::MAX - 5_000),
        ),
    ] {
        let policy = ResponseRetirement::new(lifetime, header);
        assert_eq!(policy.close_after, expected);
        let mut ordinary = json_response(StatusCode::OK, &json!({}));
        policy.mark(&mut ordinary);
        assert!(!ordinary.headers().contains_key(hyper::header::CONNECTION));
    }
    let policy = ResponseRetirement::new(Duration::from_millis(1), Duration::MAX);
    advance(Duration::from_micros(500)).await;
    let mut ordinary = json_response(StatusCode::BAD_REQUEST, &json!({}));
    policy.mark(&mut ordinary);
    assert_eq!(ordinary.headers()[hyper::header::CONNECTION], "close");
    let mut upgrade = Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header("connection", "upgrade")
        .header("upgrade", "websocket")
        .body(Full::new(Bytes::new()))
        .unwrap();
    policy.mark(&mut upgrade);
    assert_eq!(upgrade.headers()[hyper::header::CONNECTION], "upgrade");
}

#[tokio::test(start_paused = true)]
async fn in_window_auth_and_upgrade_errors_keep_their_framed_responses() {
    let _clock = ManualClock::new();
    let (_dir, state) = state();
    for (path, status) in [("/v1/status", 401), ("/v1/ws", 400)] {
        let (mut client, _shutdown, task) = connect(&state, 5_000, 65_000).await;
        health(&mut client).await;
        advance(Duration::from_millis(60_001)).await;
        client
            .write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let (head, body) = response(&mut client).await;
        assert!(head.starts_with(&format!("http/1.1 {status}")), "{head}");
        assert!(head.contains("connection: close"));
        assert!(body["error"].is_string());
        if status == 401 {
            assert!(head.contains("www-authenticate: bearer"));
        }
        eof(&mut client).await;
        task.await.unwrap().unwrap();
    }
    state.ingestor.shutdown().unwrap();
}

#[tokio::test(start_paused = true)]
async fn valid_in_window_ws_upgrade_keeps_authenticated_session() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::{
        WebSocketStream,
        tungstenite::{Message, protocol::Role},
    };
    let _clock = ManualClock::new();
    let (_dir, state) = state();
    let (mut client, shutdown, task) = connect(&state, 5_000, 65_000).await;
    health(&mut client).await;
    advance(Duration::from_millis(60_000)).await;
    client.write_all(b"GET /v1/ws HTTP/1.1\r\nHost: localhost\r\nConnection: upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Protocol: varve.v1\r\n\r\n").await.unwrap();
    let head = headers(&mut client).await;
    assert!(head.starts_with("http/1.1 101"));
    assert!(head.contains("connection: upgrade"));
    assert!(head.contains("sec-websocket-protocol: varve.v1"));
    assert!(!head.contains("connection: close"));
    let mut ws = WebSocketStream::from_raw_socket(client, Role::Client, None).await;
    ws.send(Message::text(json!({"jsonrpc":"2.0", "id":"auth", "method":"auth", "params":{"token":"retirement-test-token"}}).to_string())).await.unwrap();
    let auth: Value =
        serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
    assert_eq!(auth["result"]["protocol"], 1);
    advance(Duration::from_millis(5_001)).await;
    ws.send(Message::text(
        json!({"jsonrpc":"2.0", "id":"p", "method":"ping", "params":{}}).to_string(),
    ))
    .await
    .unwrap();
    let pong: Value =
        serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
    assert_eq!(pong["id"], "p");
    assert!(pong.get("result").is_some());
    assert_eq!(
        state
            .metrics
            .connection_timeouts_total
            .load(Ordering::Relaxed),
        0
    );
    shutdown.send(true).unwrap();
    task.await.unwrap().unwrap();
    state.ingestor.shutdown().unwrap();
}

#[tokio::test(start_paused = true)]
async fn lost_receipt_is_ambiguous_and_explicit_reconnect_deduplicates() {
    let _clock = ManualClock::new();
    let (dir, state) = state();
    // One byte of server output is enough to witness response emission after
    // commit, but cannot be mistaken for a complete HTTP receipt. No engine hook.
    let (mut client, _shutdown, task) = connect_sized(&state, 5_000, 65_000, 1).await;
    let body = batch("lost-receipt");
    client.write_all(format!("POST /v1/write HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer retirement-test-token\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
    assert_eq!(client.read_u8().await.unwrap(), b'H');
    assert_eq!(
        state
            .db
            .scan("metrics", None, None, None, None)
            .unwrap()
            .len(),
        2
    );
    drop(client); // Outcome unknown to the caller: no complete status/body/receipt.
    assert!(task.await.unwrap().is_err());
    let (mut retry, shutdown, task) = connect(&state, 5_000, 65_000).await;
    admit_write(&mut retry, &body).await;
    retry.write_all(body.as_bytes()).await.unwrap();
    let (head, receipt) = response(&mut retry).await;
    assert!(head.starts_with("http/1.1 200"));
    assert_eq!(receipt["duplicate"], true);
    assert_eq!(receipt["durability"], "local_fsync");
    shutdown.send(true).unwrap();
    eof(&mut retry).await;
    task.await.unwrap().unwrap();
    state.ingestor.shutdown().unwrap();
    drop(state);
    let reopened = Database::open(dir.path(), Config::default()).unwrap();
    let expected: WriteInput = serde_json::from_str(&body).unwrap();
    let rows = reopened.scan("metrics", None, None, None, None).unwrap();
    assert_eq!(rows.len(), expected.rows.len());
    for (stored, expected) in rows.iter().zip(expected.rows) {
        assert_eq!(stored.row, expected);
    }
}
