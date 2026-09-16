use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tempfile::TempDir;

static SERVICE_TEST_LOCK: Mutex<()> = Mutex::new(());

fn service_test_guard() -> MutexGuard<'static, ()> {
    SERVICE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct Service {
    child: Child,
}

impl Service {
    fn start(data: &Path, config: &Path, port: u16) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_varve"))
            .arg("--data")
            .arg(data)
            .arg("--config")
            .arg(config)
            .arg("serve")
            .arg("--port")
            .arg(port.to_string())
            .env_remove("VARVE_API_TOKEN")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start varve service");
        let mut service = Self { child };
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = service.child.try_wait().expect("poll service") {
                let mut stderr = String::new();
                service
                    .child
                    .stderr
                    .take()
                    .expect("stderr pipe")
                    .read_to_string(&mut stderr)
                    .expect("read stderr");
                panic!("service exited during startup ({status}): {stderr}");
            }
            if request(port, "GET", "/health", None, &[]).is_ok() {
                return service;
            }
            assert!(Instant::now() < deadline, "service did not become healthy");
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn kill(&mut self) {
        self.child.kill().expect("kill service");
        self.child.wait().expect("reap service");
    }

    #[cfg(unix)]
    fn interrupt(&mut self) {
        let status = Command::new("kill")
            .arg("-INT")
            .arg(self.child.id().to_string())
            .status()
            .expect("send SIGINT");
        assert!(status.success(), "kill -INT failed");
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = self.child.try_wait().expect("poll graceful stop") {
                assert!(status.success(), "service exited unsuccessfully: {status}");
                return;
            }
            assert!(
                Instant::now() < deadline,
                "service did not stop after SIGINT"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn free_port() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind temporary port");
    listener.local_addr().expect("temporary address").port()
}

fn request(
    port: u16,
    method: &str,
    path: &str,
    body: Option<&Value>,
    extra_headers: &[(&str, &str)],
) -> Result<(u16, Value), String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).map_err(|error| error.to_string())?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|error| error.to_string())?;
    let body = body.map(Value::to_string).unwrap_or_default();
    let mut headers = String::new();
    if method == "POST" {
        headers.push_str("Content-Type: application/json\r\n");
    }
    for (name, value) in extra_headers {
        headers.push_str(&format!("{name}: {value}\r\n"));
    }
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Length: {}\r\n{headers}\r\n{body}",
        body.len()
    )
    .map_err(|error| error.to_string())?;
    stream.flush().map_err(|error| error.to_string())?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|error| error.to_string())?;
    let (head, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| format!("malformed response: {response:?}"))?;
    let status = head
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| format!("missing status: {head:?}"))?
        .parse::<u16>()
        .map_err(|error| error.to_string())?;
    let body = serde_json::from_str(body).map_err(|error| format!("{error}: {body:?}"))?;
    Ok((status, body))
}

fn post(port: u16, path: &str, body: Value) -> Value {
    let (status, body) = request(port, "POST", path, Some(&body), &[]).expect("HTTP request");
    assert_eq!(status, 200, "HTTP error: {body}");
    body
}

fn write_config(path: &Path, maintenance_interval_ms: u64, flush_interval_us: i64) {
    let config = json!({
        "maintenance_interval_ms": maintenance_interval_ms,
        "flush_interval_us": flush_interval_us,
        "query_timeout_ms": 5_000
    });
    fs::write(path, serde_json::to_vec(&config).unwrap()).expect("write config");
}

fn table() -> Value {
    json!({"name": "metrics", "config": {}})
}

fn batch(request_id: &str, first_value: f64) -> Value {
    json!({
        "table": "metrics",
        "request_id": request_id,
        "rows": [
            {"timestamp_us": 10, "tenant": "a", "series": "cpu", "value": first_value},
            {"timestamp_us": 20, "tenant": "a", "series": "cpu", "value": first_value + 1.0}
        ]
    })
}

fn wait_for_checkpoint(port: u16, sequence: u64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (_, status) = request(port, "GET", "/v1/status", None, &[]).expect("status request");
        if status["checkpoint_sequence"].as_u64().unwrap_or_default() >= sequence {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "scheduler did not checkpoint: {status}"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn http_ingest_is_idempotent_queries_expected_rows_and_scheduler_ticks() {
    let _guard = service_test_guard();
    let temporary = TempDir::new().unwrap();
    let data = temporary.path().join("data");
    let config = temporary.path().join("config.json");
    write_config(&config, 20, 1);
    let port = free_port();
    let mut service = Service::start(&data, &config, port);

    let table_sequence = post(port, "/v1/tables", table()).as_u64().unwrap();
    let first = post(port, "/v1/write", batch("batch-1", 2.0));
    assert!(first["sequence"].as_u64().unwrap() > table_sequence);
    assert_eq!(first["duplicate"], false);
    let duplicate = post(port, "/v1/write", batch("batch-1", 2.0));
    assert_eq!(duplicate["sequence"], first["sequence"]);
    assert_eq!(duplicate["duplicate"], true);

    let result = post(
        port,
        "/v1/query",
        json!({"sql": "SELECT count(*) AS count, sum(value) AS total FROM metrics"}),
    );
    assert_eq!(result, json!([{"count": 2, "total": 5.0}]));
    wait_for_checkpoint(port, first["sequence"].as_u64().unwrap());

    let (status, body) = request(
        port,
        "POST",
        "/v1/query",
        Some(&json!({"sql": "SELECT 1"})),
        &[("Origin", "https://example.invalid")],
    )
    .unwrap();
    assert_eq!(status, 403, "origin was not rejected: {body}");

    #[cfg(unix)]
    service.interrupt();
}

#[test]
fn abrupt_process_death_replays_committed_wal_on_restart() {
    let _guard = service_test_guard();
    let temporary = TempDir::new().unwrap();
    let data = temporary.path().join("data");
    let config = temporary.path().join("config.json");
    write_config(&config, 60_000, 60_000_000);
    let port = free_port();
    let mut service = Service::start(&data, &config, port);

    post(port, "/v1/tables", table());
    let receipt = post(port, "/v1/write", batch("wal-batch", 7.0));
    assert_eq!(receipt["durability"], "local_fsync");
    service.kill();

    let restart_port = free_port();
    let _restarted = Service::start(&data, &config, restart_port);
    let result = post(
        restart_port,
        "/v1/query",
        json!({"sql": "SELECT count(*) AS count, sum(value) AS total FROM metrics"}),
    );
    assert_eq!(result, json!([{"count": 2, "total": 15.0}]));
    let duplicate = post(restart_port, "/v1/write", batch("wal-batch", 7.0));
    assert_eq!(duplicate["sequence"], receipt["sequence"]);
    assert_eq!(duplicate["duplicate"], true);
}

fn invalid_json_post(port: u16, body: &str) -> u16 {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    write!(stream,"POST /v1/write HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",body.len(),body).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response.split_whitespace().nth(1).unwrap().parse().unwrap()
}

#[test]
fn http_exact_timestamps_and_invalid_numbers_survive_checkpoint_and_restore() {
    let _guard = service_test_guard();
    use std::sync::Arc;
    use varve::remote::FileStore;
    use varve::{Config, Database};
    let temporary = TempDir::new().unwrap();
    let data = temporary.path().join("data");
    let cfg = temporary.path().join("config.json");
    write_config(&cfg, 50, 1);
    let port = free_port();
    let mut service = Service::start(&data, &cfg, port);
    post(
        port,
        "/v1/tables",
        json!({"name":"metrics","config":{"window_us":1,"rollup_widths_us":[1]}}),
    );
    let receipt = post(
        port,
        "/v1/write",
        json!({"table":"metrics","request_id":"extremes","rows":[
            {"timestamp_us":i64::MIN,"tenant":"α","series":"cpu","value":0.10000000000000002_f64,"tags":{"城市":"東京'\""}},
            {"timestamp_us":i64::MAX,"tenant":"α","series":"cpu","value":1.5,"tags":{"城市":"東京'\""}}
        ]}),
    );
    for fields in [
        "\"timestamp_us\":9223372036854775808,\"value\":1",
        "\"timestamp_us\":-9223372036854775809,\"value\":1",
        "\"timestamp_us\":0.5,\"value\":1",
        "\"timestamp_us\":1,\"value\":1e999",
        "\"timestamp_us\":1,\"value\":NaN",
        "\"timestamp_us\":1,\"value\":null",
    ] {
        let body = format!(
            "{{\"table\":\"metrics\",\"request_id\":\"invalid\",\"rows\":[{{\"tenant\":\"α\",\"series\":\"cpu\",{fields}}}]}}"
        );
        assert_eq!(invalid_json_post(port, &body), 400, "{fields}");
    }
    let (_, status) = request(port, "GET", "/v1/status", None, &[]).unwrap();
    assert!(status["sequence"].as_u64().unwrap() >= receipt["sequence"].as_u64().unwrap());
    wait_for_checkpoint(port, receipt["sequence"].as_u64().unwrap());
    service.kill();
    let remote = Arc::new(FileStore::new(temporary.path().join("objects")).unwrap());
    let db = Database::open_with_remote(&data, Config::default(), Some(remote.clone())).unwrap();
    db.ship().unwrap();
    drop(db);
    let restored =
        Database::restore(temporary.path().join("restored"), Config::default(), remote).unwrap();
    let rows = restored.scan("metrics", None, None, None, None).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].row.timestamp_us, i64::MIN);
    assert_eq!(rows[1].row.timestamp_us, i64::MAX);
    assert_eq!(rows[0].row.value, 0.10000000000000002_f64);
    assert_eq!(rows[0].row.tags["城市"], "東京'\"");
}

#[test]
fn public_admin_commands_and_config_input_limit_are_registered() {
    let output = Command::new(env!("CARGO_BIN_EXE_varve"))
        .arg("--help")
        .output()
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    for command in [
        "rollups",
        "vacuum-remote",
        "remote-head",
        "recover-remote-lock",
        "serve",
        "restore",
    ] {
        assert!(help.contains(command), "missing {command}");
    }
    let tmp = TempDir::new().unwrap();
    let config = tmp.path().join("huge.json");
    fs::write(&config, vec![b' '; 65537]).unwrap();
    let data = tmp.path().join("db");
    let output = Command::new(env!("CARGO_BIN_EXE_varve"))
        .arg("--data")
        .arg(&data)
        .arg("--config")
        .arg(config)
        .arg("init")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(!data.exists());
}

#[test]
fn service_refuses_public_bind() {
    let temporary = TempDir::new().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_varve"))
        .arg("--data")
        .arg(temporary.path().join("data"))
        .arg("serve")
        .arg("--bind")
        .arg("0.0.0.0")
        .arg("--port")
        .arg("8080")
        .env_remove("VARVE_API_TOKEN")
        .output()
        .expect("run varve");
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("non-loopback"),
        "unexpected stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
