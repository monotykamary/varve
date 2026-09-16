use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::time::timeout;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};

const TOKEN: &str = "transport-test-token-at-least-32-bytes";
type Ws = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

struct Server {
    child: Child,
    dir: TempDir,
    port: u16,
    pg: u16,
}
impl Server {
    fn start(token: bool, pg: bool, env: &[(&str, &str)], config: Value) -> Self {
        let dir = TempDir::new().unwrap();
        Self::in_dir(dir, token, pg, env, config)
    }
    fn in_dir(dir: TempDir, token: bool, pg: bool, env: &[(&str, &str)], config: Value) -> Self {
        let port = free_port();
        let pg_port = free_port();
        let config_path = dir.path().join("config.json");
        std::fs::write(&config_path, config.to_string()).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_varve"));
        command
            .args(["--data"])
            .arg(dir.path().join("data"))
            .arg("--config")
            .arg(config_path)
            .args([
                "serve",
                "--port",
                &port.to_string(),
                "--request-workers",
                "1",
                "--request-queue",
                "4",
                "--connection-timeout-ms",
                "15000",
            ])
            .env_remove("VARVE_API_TOKEN")
            .env_remove("VARVE_PG_PORT")
            .env_remove("VARVE_WS_ORIGINS")
            .env_remove("VARVE_WS_MAX_PENDING")
            .env_remove("VARVE_WS_MAX_PENDING_BYTES")
            .env_remove("VARVE_WS_AUTH_TIMEOUT_MS")
            .env_remove("VARVE_WS_HEARTBEAT_MS")
            .env_remove("VARVE_HTTP_MAX_BODY_BYTES")
            .env_remove("VARVE_INGEST_MAX_DELAY_MS")
            .env_remove("VARVE_INGEST_QUEUE_CAPACITY")
            .env_remove("VARVE_INGEST_MAX_PENDING_BYTES")
            .env_remove("VARVE_INGEST_MAX_GROUP_REQUESTS")
            .env_remove("VARVE_INGEST_MAX_GROUP_ROWS")
            .env_remove("VARVE_INGEST_MAX_GROUP_BYTES")
            .env("VARVE_HTTP_REQUEST_TIMEOUT_MS", "10000")
            .env("VARVE_HTTP_BODY_TIMEOUT_MS", "2000")
            .env("VARVE_HTTP_SHUTDOWN_TIMEOUT_MS", "2000")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        if token {
            command.env("VARVE_API_TOKEN", TOKEN);
        }
        if pg {
            command.args(["--pg-port", &pg_port.to_string()]);
        }
        for (key, value) in env {
            command.env(key, value);
        }
        let child = command.spawn().unwrap();
        let mut server = Self {
            child,
            dir,
            port,
            pg: pg_port,
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = server.child.try_wait().unwrap() {
                let mut err = String::new();
                server
                    .child
                    .stderr
                    .take()
                    .unwrap()
                    .read_to_string(&mut err)
                    .unwrap();
                panic!("startup {status}: {err}");
            }
            if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                break;
            }
            assert!(Instant::now() < deadline, "startup deadline");
            std::thread::sleep(Duration::from_millis(10));
        }
        server
    }
    async fn ws(&self, token: &str) -> Ws {
        let (mut ws, response) = connect_async(ws_request(self.port, None)).await.unwrap();
        assert_eq!(response.headers()["sec-websocket-protocol"], "varve.v1");
        send(&mut ws, "auth", "auth", json!({"token":token})).await;
        assert_eq!(
            recv(&mut ws).await,
            json!({"jsonrpc":"2.0", "id":"auth", "result":{"protocol":1}})
        );
        ws
    }
    #[cfg(unix)]
    fn interrupt(&mut self) {
        assert!(
            Command::new("kill")
                .args(["-INT", &self.child.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            assert!(Instant::now() < deadline, "shutdown deadline");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
fn ws_request(
    port: u16,
    origin: Option<&str>,
) -> tokio_tungstenite::tungstenite::http::Request<()> {
    let mut request = format!("ws://127.0.0.1:{port}/v1/ws")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("sec-websocket-protocol", "varve.v1".parse().unwrap());
    if let Some(origin) = origin {
        request
            .headers_mut()
            .insert("origin", origin.parse().unwrap());
    }
    request
}
async fn send(ws: &mut Ws, id: &str, method: &str, params: Value) {
    ws.send(Message::Text(
        json!({"jsonrpc":"2.0", "id":id, "method":method, "params":params})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
}
async fn recv(ws: &mut Ws) -> Value {
    timeout(Duration::from_secs(10), async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) => return serde_json::from_str(&text).unwrap(),
                Message::Ping(_) => ws.flush().await.unwrap(),
                other => panic!("unexpected {other:?}"),
            }
        }
    })
    .await
    .expect("response deadline")
}
async fn call(ws: &mut Ws, id: &str, method: &str, params: Value) -> Value {
    send(ws, id, method, params).await;
    let value = recv(ws).await;
    assert_eq!(value["id"], id);
    assert!(value.get("error").is_none(), "{value}");
    value["result"].clone()
}
fn batch(id: &str) -> Value {
    json!({"table":"metrics", "request_id":id, "rows":[{"timestamp_us":9007199254740993i64,"tenant":"a","series":"cpu","value":1.25,"tags":{}}]})
}
fn http(port: u16, path: &str, body: Value) -> (u16, Value) {
    let body = body.to_string();
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    write!(stream, "POST {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nAuthorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}", body.len()).unwrap();
    let mut text = String::new();
    stream.read_to_string(&mut text).unwrap();
    let (head, body) = text.split_once("\r\n\r\n").unwrap();
    (
        head.split_whitespace().nth(1).unwrap().parse().unwrap(),
        serde_json::from_str(body).unwrap(),
    )
}

#[tokio::test]
async fn ws_exact_wire_methods_durability_and_http_shared_ingestion() {
    let server = Server::start(true, false, &[], json!({}));
    let mut ws = server.ws(TOKEN).await;
    call(
        &mut ws,
        "c",
        "create",
        json!({"name":"metrics", "config":{}}),
    )
    .await;
    let receipt = call(&mut ws, "w", "write", batch("shared")).await;
    assert_eq!(receipt["durability"], "local_fsync");
    let (status, retry) = http(server.port, "/v1/write", batch("shared"));
    assert_eq!(status, 200);
    assert_eq!(receipt["sequence"], retry["sequence"]);
    let rows = call(
        &mut ws,
        "q",
        "query",
        json!({"sql":"SELECT timestamp_us, value FROM metrics"}),
    )
    .await;
    assert_eq!(rows[0]["timestamp_us"], 9007199254740993i64);
    for method in [
        "ping",
        "status",
        "tables",
        "policies",
        "aggregates",
        "jobs",
        "checkpoint",
        "maintain",
    ] {
        call(&mut ws, method, method, json!({})).await;
    }
    let mut conflict = batch("shared");
    conflict["rows"][0]["value"] = json!(2.0);
    send(&mut ws, "conflict", "write", conflict).await;
    assert_eq!(recv(&mut ws).await["error"]["code"], -32000);
    send(&mut ws, "unsupported", "ship", json!({})).await;
    assert_eq!(recv(&mut ws).await["error"]["code"], -32601);
    send(&mut ws, "invalid", "ping", json!({"extra":1})).await;
    assert_eq!(recv(&mut ws).await["error"]["code"], -32602);
    ws.close(None).await.unwrap();
}

#[tokio::test]
async fn ws_auth_origin_subprotocol_and_url_credentials_are_rejected() {
    let server = Server::start(
        true,
        false,
        &[("VARVE_WS_ORIGINS", "https://allowed.example")],
        json!({}),
    );
    assert!(
        connect_async(ws_request(server.port, Some("https://evil.example")))
            .await
            .is_err()
    );
    let mut request = ws_request(server.port, None);
    request.headers_mut().remove("sec-websocket-protocol");
    assert!(connect_async(request).await.is_err());
    let mut request = ws_request(server.port, None);
    *request.uri_mut() = format!("ws://127.0.0.1:{}/v1/ws?token=secret", server.port)
        .parse()
        .unwrap();
    assert!(connect_async(request).await.is_err());
    let (mut ws, _) = connect_async(ws_request(server.port, Some("https://allowed.example")))
        .await
        .unwrap();
    send(&mut ws, "auth", "auth", json!({"token":"incorrect"})).await;
    assert_eq!(recv(&mut ws).await["error"]["code"], -32001);
    let (mut allowed, _) = connect_async(ws_request(server.port, Some("https://allowed.example")))
        .await
        .unwrap();
    send(&mut allowed, "auth", "auth", json!({"token":TOKEN})).await;
    assert_eq!(recv(&mut allowed).await["result"]["protocol"], 1);
    let (mut ws, _) = connect_async(ws_request(server.port, None)).await.unwrap();
    send(&mut ws, "s", "status", json!({})).await;
    assert_eq!(recv(&mut ws).await["error"]["code"], -32001);
    let local = Server::start(
        false,
        false,
        &[("VARVE_WS_ORIGINS", "http://localhost")],
        json!({}),
    );
    assert!(
        connect_async(ws_request(local.port, Some("http://localhost")))
            .await
            .is_err()
    );
    let mut ws = local.ws("").await;
    assert_eq!(
        call(&mut ws, "p", "ping", json!({})).await,
        json!({"pong":true})
    );
}

#[tokio::test]
async fn ws_auth_deadline_frame_and_pending_byte_bounds() {
    let server = Server::start(
        true,
        false,
        &[
            ("VARVE_WS_AUTH_TIMEOUT_MS", "100"),
            ("VARVE_HTTP_MAX_BODY_BYTES", "512"),
            ("VARVE_WS_MAX_PENDING_BYTES", "100"),
        ],
        json!({}),
    );
    let (mut ws, _) = connect_async(ws_request(server.port, None)).await.unwrap();
    assert_eq!(recv(&mut ws).await["error"]["code"], -32001);
    let mut ws = server.ws(TOKEN).await;
    send(&mut ws, "bytes", "write", batch("never-admitted")).await;
    assert_eq!(recv(&mut ws).await["error"]["code"], -32003);
    ws.send(Message::Text("x".repeat(513).into()))
        .await
        .unwrap();
    let next = timeout(Duration::from_secs(2), ws.next()).await.unwrap();
    assert!(!matches!(next, Some(Ok(Message::Text(_)))));
    let mut ws = server.ws(TOKEN).await;
    call(&mut ws, "p", "ping", json!({})).await;
}

#[cfg(unix)]
fn gated_server(extra: &[(&str, &str)]) -> Server {
    use std::os::unix::fs::PermissionsExt;
    let dir = TempDir::new().unwrap();
    let script = dir.path().join("query.sh");
    // A file barrier makes worker occupancy deterministic; no sleep is used as proof of admission.
    std::fs::write(&script, format!("#!/bin/sh\ncat >/dev/null\ntouch '{}'\nwhile [ ! -f '{}' ]; do sleep 0.01; done\nprintf '[{{\"answer\":42}}]'\n", dir.path().join("entered").display(), dir.path().join("release").display())).unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    Server::in_dir(
        dir,
        true,
        false,
        extra,
        json!({"query_executable":script,"query_timeout_ms":10000}),
    )
}
#[cfg(unix)]
async fn entered(server: &Server) {
    timeout(Duration::from_secs(4), async {
        while !server.dir.path().join("entered").exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
#[cfg(unix)]
fn release(server: &Server) {
    std::fs::write(server.dir.path().join("release"), "go").unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn ws_out_of_order_and_both_write_transports_bypass_busy_query_worker() {
    let server = gated_server(&[]);
    let mut ws = server.ws(TOKEN).await;
    call(&mut ws, "c", "create", json!({"name":"metrics"})).await;
    send(
        &mut ws,
        "slow",
        "query",
        json!({"sql":"SELECT 42 AS answer"}),
    )
    .await;
    entered(&server).await;
    send(&mut ws, "fast", "ping", json!({})).await;
    let fast = recv(&mut ws).await;
    assert_eq!(fast["id"], "fast");
    let (status, _) = http(server.port, "/v1/write", batch("http-while-query"));
    assert_eq!(status, 200);
    send(&mut ws, "write", "write", batch("ws-while-query")).await;
    let write = recv(&mut ws).await;
    assert_eq!(write["id"], "write");
    assert!(write.get("error").is_none(), "{write}");
    release(&server);
    let slow = recv(&mut ws).await;
    assert_eq!(slow["id"], "slow");
    assert_eq!(slow["result"][0]["answer"], 42);
}

#[cfg(unix)]
#[tokio::test]
async fn ws_pending_limit_duplicate_ids_and_disconnect_cleanup() {
    let server = gated_server(&[("VARVE_WS_MAX_PENDING", "1")]);
    let mut ws = server.ws(TOKEN).await;
    send(&mut ws, "slow", "query", json!({"sql":"SELECT 42"})).await;
    entered(&server).await;
    send(&mut ws, "reject", "write", batch("not-enqueued")).await;
    assert_eq!(recv(&mut ws).await["error"]["code"], -32003);
    send(&mut ws, "slow", "ping", json!({})).await;
    let duplicate = recv(&mut ws).await;
    assert!(duplicate["id"].is_null());
    assert_eq!(duplicate["error"]["code"], -32600);
    drop(ws);
    release(&server);
    let mut ws = server.ws(TOKEN).await;
    call(&mut ws, "recovered", "status", json!({})).await;
}

#[cfg(unix)]
#[tokio::test]
async fn ws_accepted_deadline_is_ambiguous_not_busy() {
    let server = gated_server(&[
        ("VARVE_HTTP_REQUEST_TIMEOUT_MS", "500"),
        ("VARVE_HTTP_BODY_TIMEOUT_MS", "100"),
    ]);
    let mut ws = server.ws(TOKEN).await;
    send(&mut ws, "slow", "query", json!({"sql":"SELECT 42"})).await;
    entered(&server).await;
    let response = recv(&mut ws).await;
    assert_eq!(response["id"], "slow");
    assert_eq!(response["error"]["code"], -32000);
    release(&server);
    call(&mut ws, "ping", "ping", json!({})).await;
}

#[cfg(unix)]
#[tokio::test]
async fn ws_ping_pong_and_shutdown_close_persistent_connections() {
    let mut server = Server::start(true, false, &[("VARVE_WS_HEARTBEAT_MS", "100")], json!({}));
    let mut ws = server.ws(TOKEN).await;
    ws.send(Message::Ping(vec![1, 2, 3].into())).await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(2), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        Message::Pong(vec![1, 2, 3].into())
    );
    assert!(matches!(
        timeout(Duration::from_secs(2), ws.next()).await.unwrap(),
        Some(Ok(Message::Ping(_)))
    ));
    ws.flush().await.unwrap();
    call(&mut ws, "p", "ping", json!({})).await;
    server.interrupt();
    timeout(Duration::from_secs(2), async {
        loop {
            match ws.next().await {
                // Heartbeats already in the TCP receive buffer precede the close.
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                other => panic!("unexpected shutdown frame: {other:?}"),
            }
        }
    })
    .await
    .expect("persistent socket closed after shutdown");
}

#[cfg(unix)]
#[tokio::test]
async fn accepted_write_timeout_and_disconnect_still_drain_durably() {
    let mut server = Server::start(
        true,
        false,
        &[
            ("VARVE_INGEST_MAX_DELAY_MS", "1000"),
            ("VARVE_HTTP_REQUEST_TIMEOUT_MS", "200"),
            ("VARVE_HTTP_BODY_TIMEOUT_MS", "100"),
        ],
        json!({}),
    );
    let mut ws = server.ws(TOKEN).await;
    call(&mut ws, "c", "create", json!({"name":"metrics"})).await;
    send(&mut ws, "timed", "write", batch("accepted-timeout")).await;
    let response = recv(&mut ws).await;
    assert_eq!(response["error"]["code"], -32000, "{response}");
    assert_eq!(metric(server.port, "varve_ingest_submitted_total"), 1);
    send(
        &mut ws,
        "disconnected",
        "write",
        batch("accepted-disconnect"),
    )
    .await;
    timeout(Duration::from_secs(2), async {
        while metric(server.port, "varve_ingest_submitted_total") != 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    drop(ws);
    server.interrupt();
    let output = Command::new(env!("CARGO_BIN_EXE_varve"))
        .arg("--data")
        .arg(server.dir.path().join("data"))
        .args(["query", "SELECT COUNT(*) AS n FROM metrics"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let rows: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        rows[0]["n"], 2,
        "accepted writes survive timeout, disconnect and shutdown"
    );
}

#[cfg(unix)]
fn metric(port: u16, name: &str) -> u64 {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    write!(stream, "GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nAuthorization: Bearer {TOKEN}\r\n\r\n").unwrap();
    let mut text = String::new();
    stream.read_to_string(&mut text).unwrap();
    text.lines()
        .find_map(|line| line.strip_prefix(&format!("{name} ")))
        .unwrap()
        .parse()
        .unwrap()
}

#[tokio::test]
async fn websocket_missing_pong_closes_within_heartbeat_deadline() {
    let server = Server::start(true, false, &[("VARVE_WS_HEARTBEAT_MS", "100")], json!({}));
    let mut ws = server.ws(TOKEN).await;
    // No read/flush means tungstenite cannot automatically reply to the ping.
    tokio::time::sleep(Duration::from_millis(350)).await;
    timeout(Duration::from_secs(2), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Ping(_))) => continue,
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                other => panic!("unexpected frame: {other:?}"),
            }
        }
    })
    .await
    .expect("missing heartbeat must close the socket");
}

#[test]
fn http_connection_reuse_and_auth_before_body_remain_supported() {
    let server = Server::start(true, false, &[], json!({}));
    let mut stream = TcpStream::connect(("127.0.0.1", server.port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    for path in ["/health", "/v1/status"] {
        write!(
            stream,
            "GET {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {TOKEN}\r\n\r\n"
        )
        .unwrap();
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            stream.read_exact(&mut byte).unwrap();
            head.push(byte[0]);
        }
        let head = String::from_utf8(head).unwrap().to_lowercase();
        assert!(head.starts_with("http/1.1 200"), "{head}");
        assert!(!head.contains("connection: close"));
        let length: usize = head
            .lines()
            .find_map(|line| line.strip_prefix("content-length: "))
            .unwrap()
            .parse()
            .unwrap();
        let mut body = vec![0; length];
        stream.read_exact(&mut body).unwrap();
        let _: Value = serde_json::from_slice(&body).unwrap();
    }
    let mut stream = TcpStream::connect(("127.0.0.1", server.port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    write!(stream, "POST /v1/write HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 1000\r\nConnection: close\r\n\r\n").unwrap();
    let mut text = String::new();
    stream.read_to_string(&mut text).unwrap();
    assert!(text.starts_with("HTTP/1.1 401"), "{text}");
}

async fn pg_connect(
    server: &Server,
    password: &str,
) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>), tokio_postgres::Error> {
    let mut config = tokio_postgres::Config::new();
    config
        .host("127.0.0.1")
        .port(server.pg)
        .user("varve")
        .password(password)
        .dbname("varve")
        .connect_timeout(Duration::from_secs(2));
    let (client, connection) = config.connect(tokio_postgres::NoTls).await?;
    let task = tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok((client, task))
}

#[tokio::test]
async fn standard_postgres_client_scram_simple_query_and_explicit_subset() {
    let server = Server::start(true, true, &[], json!({}));
    assert!(pg_connect(&server, "wrong").await.is_err());
    let (client, task) = pg_connect(&server, TOKEN).await.unwrap();
    let messages = client
        .simple_query(
            "SELECT 9007199254740993::BIGINT AS exact, NULL AS absent, 'hello' AS greeting",
        )
        .await
        .unwrap();
    let row = messages
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .unwrap();
    assert_eq!(row.get("exact"), Some("9007199254740993"));
    assert_eq!(row.get("absent"), None);
    assert_eq!(row.get("greeting"), Some("hello"));
    for sql in [
        "BEGIN",
        "COMMIT",
        "COPY (SELECT 1) TO STDOUT",
        "CREATE TABLE unsafe(x INT)",
        "SELECT 1; SELECT 2",
    ] {
        let error = client.simple_query(sql).await.unwrap_err();
        assert_eq!(
            error.as_db_error().unwrap().code().code(),
            "0A000",
            "{sql}: {error}"
        );
    }
    assert!(client.simple_query("SELECT 1 AS still_alive").await.is_ok());
    assert!(client.prepare("SELECT $1").await.is_err());
    drop(client);
    task.abort();
}

#[tokio::test]
async fn pg_rejects_valid_management_calls_without_mutation() {
    let server = Server::start(true, true, &[], json!({}));
    let (client, task) = pg_connect(&server, TOKEN).await.unwrap();
    let mut ws = server.ws(TOKEN).await;
    let before = call(&mut ws, "before", "status", json!({})).await;
    let management = "CALL varve_create_table('pg_forbidden', '{}')";
    for sql in [
        management,
        "/* comment */ cAlL varve_create_table('pg_forbidden', '{}')",
        "EXPLAIN CALL varve_create_table('pg_forbidden', '{}')",
    ] {
        let error = client.simple_query(sql).await.unwrap_err();
        assert_eq!(error.as_db_error().unwrap().code().code(), "0A000");
    }
    let after = call(&mut ws, "after", "status", json!({})).await;
    assert_eq!(
        before["sequence"], after["sequence"],
        "rejected management SQL must not mutate state"
    );
    // Prove this was an otherwise-valid management operation, not a bogus CALL.
    let (status, _) = http(server.port, "/v1/query", json!({"sql":management}));
    assert_eq!(status, 200);
    let after_http = call(&mut ws, "after-http", "status", json!({})).await;
    assert!(after_http["sequence"].as_u64().unwrap() > after["sequence"].as_u64().unwrap());
    drop(client);
    task.abort();
}

#[tokio::test]
async fn pg_idle_sessions_persist_but_partial_packets_have_deadlines() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let server = Server::start(
        true,
        true,
        &[("VARVE_HTTP_BODY_TIMEOUT_MS", "100")],
        json!({}),
    );
    for partial in [vec![b'Q'], vec![b'Q', 0, 0, 0, 20, b'S', b'E']] {
        // The proxy forwards a real standard-client SCRAM handshake and two queries,
        // then injects either an incomplete header or body at a known idle boundary.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let upstream_port = server.pg;
        let (inject, injected) = tokio::sync::oneshot::channel::<()>();
        let proxy = tokio::spawn(async move {
            let (mut downstream, _) = listener.accept().await.unwrap();
            let mut upstream = tokio::net::TcpStream::connect(("127.0.0.1", upstream_port))
                .await
                .unwrap();
            tokio::select! {
                result = tokio::io::copy_bidirectional(&mut downstream, &mut upstream) => panic!("idle connection terminated: {result:?}"),
                _ = injected => {},
            }
            upstream.write_all(&partial).await.unwrap();
            let mut byte = [0];
            let closed = timeout(Duration::from_secs(2), upstream.read(&mut byte))
                .await
                .expect("partial packet deadline");
            assert!(
                matches!(closed, Ok(0) | Err(_)),
                "partial packet must close connection: {closed:?}"
            );
        });
        let mut config = tokio_postgres::Config::new();
        config
            .host("127.0.0.1")
            .port(port)
            .user("varve")
            .password(TOKEN)
            .dbname("varve");
        let (client, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
        let task = tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1 AS first").await.unwrap();
        tokio::time::sleep(Duration::from_millis(350)).await;
        client
            .simple_query("SELECT 2 AS second")
            .await
            .expect("authenticated idle wait is not a packet timeout");
        inject.send(()).unwrap();
        timeout(Duration::from_secs(3), proxy)
            .await
            .unwrap()
            .unwrap();
        drop(client);
        task.abort();
    }
}

#[tokio::test]
async fn pg_frame_bounds_and_public_bind_refusal() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let server = Server::start(true, true, &[], json!({}));
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", server.pg))
        .await
        .unwrap();
    stream.write_all(&1_000_000u32.to_be_bytes()).await.unwrap();
    let mut byte = [0];
    assert!(matches!(
        timeout(Duration::from_secs(2), stream.read(&mut byte))
            .await
            .unwrap(),
        Ok(0) | Err(_)
    ));
    let (client, task) = pg_connect(&server, TOKEN).await.unwrap();
    let too_large = format!("SELECT '{}'", "x".repeat(4 * 1024 * 1024));
    assert!(client.simple_query(&too_large).await.is_err());
    task.abort();
    let dir = TempDir::new().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_varve"))
        .arg("--data")
        .arg(dir.path())
        .args([
            "serve",
            "--port",
            &free_port().to_string(),
            "--pg-port",
            &free_port().to_string(),
            "--pg-bind",
            "0.0.0.0",
        ])
        .env("VARVE_API_TOKEN", TOKEN)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("TLS"));
}

#[tokio::test]
async fn ws_oversized_responses_never_exceed_frame_cap() {
    for cap in ["512", "120"] {
        let server = Server::start(
            true,
            false,
            &[("VARVE_HTTP_MAX_BODY_BYTES", cap)],
            json!({}),
        );
        let mut ws = server.ws(TOKEN).await;
        send(
            &mut ws,
            "q",
            "query",
            json!({"sql":"SELECT repeat('x', 1024) AS payload"}),
        )
        .await;
        let frame = timeout(Duration::from_secs(5), ws.next()).await.unwrap();
        if cap == "512" {
            let Message::Text(text) = frame.unwrap().unwrap() else {
                panic!("expected bounded error");
            };
            assert!(text.len() <= 512);
            let response: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(response["error"]["code"], -32000);
            call(&mut ws, "alive", "ping", json!({})).await;
        } else {
            assert!(
                matches!(frame, Some(Ok(Message::Close(_))) | Some(Err(_)) | None),
                "cannot send error larger than cap: {frame:?}"
            );
        }
    }
}

#[tokio::test]
async fn ws_ingestion_overload_is_proven_pre_admission() {
    let server = Server::start(
        true,
        false,
        &[("VARVE_INGEST_MAX_PENDING_BYTES", "1")],
        json!({}),
    );
    let mut ws = server.ws(TOKEN).await;
    call(&mut ws, "c", "create", json!({"name":"metrics"})).await;
    send(&mut ws, "overload", "write", batch("rejected")).await;
    assert_eq!(recv(&mut ws).await["error"]["code"], -32003);
    let rows = call(
        &mut ws,
        "q",
        "query",
        json!({"sql":"SELECT COUNT(*) AS n FROM metrics"}),
    )
    .await;
    assert_eq!(rows[0]["n"], 0);
    #[cfg(unix)]
    assert_eq!(metric(server.port, "varve_ingest_submitted_total"), 0);
}

#[tokio::test]
async fn ws_invalid_envelopes_and_independent_pipelined_ids() {
    let server = Server::start(true, false, &[], json!({}));
    let mut ws = server.ws(TOKEN).await;
    for (text, code) in [
        ("{", -32700),
        ("[]", -32600),
        (
            "{\"jsonrpc\":\"2.0\",\"method\":\"ping\",\"params\":{}}",
            -32600,
        ),
    ] {
        ws.send(Message::Text(text.into())).await.unwrap();
        assert_eq!(recv(&mut ws).await["error"]["code"], code);
    }
    for id in ["a", "b", "c"] {
        send(&mut ws, id, "ping", json!({})).await;
    }
    let mut responses = BTreeMap::new();
    for _ in 0..3 {
        let value = recv(&mut ws).await;
        responses.insert(value["id"].as_str().unwrap().to_owned(), value);
    }
    assert_eq!(responses.len(), 3);
    for value in responses.values() {
        assert_eq!(value["result"]["pong"], true);
    }
}
