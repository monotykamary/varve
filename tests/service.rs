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
        Self::start_with_env(data, config, port, &[])
    }

    fn start_with_env(data: &Path, config: &Path, port: u16, env: &[(&str, &str)]) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_varve"));
        command
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
            .stderr(Stdio::piped());
        for (name, value) in env {
            command.env(name, value);
        }
        let child = command.spawn().expect("start varve service");
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

fn metrics_text(port: u16) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut response = String::new();
    stream
        .take(256 * 1024)
        .read_to_string(&mut response)
        .unwrap();
    let (headers, body) = response.split_once("\r\n\r\n").unwrap();
    assert!(headers.starts_with("HTTP/1.1 200"), "{headers}");
    body.to_owned()
}

fn metric_count(text: &str, name: &str) -> u64 {
    let prefix = format!("{name} ");
    text.lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .unwrap()
        .parse()
        .unwrap()
}

fn phase_count(text: &str, phase: &str) -> u64 {
    let prefix = format!("varve_phase_duration_seconds_count{{phase=\"{phase}\"}} ");
    text.lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .unwrap()
        .parse()
        .unwrap()
}

fn post(port: u16, path: &str, body: Value) -> Value {
    let (status, body) = request(port, "POST", path, Some(&body), &[]).expect("HTTP request");
    assert_eq!(status, 200, "HTTP error: {body}");
    body
}

fn read_persistent_response(stream: &mut TcpStream) -> Result<(u16, Value), String> {
    let mut response = Vec::new();
    let mut chunk = [0_u8; 4096];
    let header_end = loop {
        if let Some(index) = response.windows(4).position(|window| window == b"\r\n\r\n") {
            break index;
        }
        let read = stream.read(&mut chunk).map_err(|error| error.to_string())?;
        if read == 0 {
            return Err("connection closed before response headers".to_owned());
        }
        response.extend_from_slice(&chunk[..read]);
    };
    let head = std::str::from_utf8(&response[..header_end]).map_err(|error| error.to_string())?;
    let status = head
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| format!("missing status: {head:?}"))?
        .parse::<u16>()
        .map_err(|error| error.to_string())?;
    let content_length = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>())
        })
        .transpose()
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("missing content-length: {head:?}"))?;
    let body_start = header_end + 4;
    let response_length = body_start + content_length;
    while response.len() < response_length {
        let read = stream.read(&mut chunk).map_err(|error| error.to_string())?;
        if read == 0 {
            return Err("connection closed before response body".to_owned());
        }
        response.extend_from_slice(&chunk[..read]);
    }
    let body = serde_json::from_slice(&response[body_start..response_length])
        .map_err(|error| error.to_string())?;
    Ok((status, body))
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
fn ingestion_diagnostics_are_authenticated_and_opt_in() {
    let _guard = service_test_guard();
    let temporary = TempDir::new().unwrap();
    let config = temporary.path().join("config.json");
    write_config(&config, 1000, 5_000_000);
    let port = free_port();
    let _service = Service::start_with_env(
        &temporary.path().join("data"),
        &config,
        port,
        &[
            ("VARVE_API_TOKEN", "trace-fixture-token-0123456789abcdef"),
            ("VARVE_INGEST_TRACE_CAPACITY", "8"),
        ],
    );
    assert_eq!(
        request(port, "GET", "/v1/diagnostics/ingest", None, &[])
            .unwrap()
            .0,
        401
    );
    let auth = [(
        "Authorization",
        "Bearer trace-fixture-token-0123456789abcdef",
    )];
    assert_eq!(
        request(port, "POST", "/v1/tables", Some(&table()), &auth)
            .unwrap()
            .0,
        200
    );
    let (status, receipt) = request(
        port,
        "POST",
        "/v1/write",
        Some(&batch("trace-a", 1.0)),
        &auth,
    )
    .unwrap();
    assert_eq!(status, 200);
    let (status, traces) = request(port, "GET", "/v1/diagnostics/ingest", None, &auth).unwrap();
    assert_eq!(status, 200);
    assert_eq!(traces["capacity"], 8);
    assert_eq!(traces["groups"][0]["sequences"][0], receipt["sequence"]);
    assert_eq!(traces["groups"][0]["failed"], 0);
    assert_eq!(
        request(
            port,
            "POST",
            "/v1/diagnostics/ingest",
            Some(&json!({})),
            &auth
        )
        .unwrap()
        .0,
        405
    );
}

#[test]
fn retained_query_metrics_witness_reuse_and_only_new_raw_rows() {
    let _guard = service_test_guard();
    let temporary = TempDir::new().unwrap();
    let data = temporary.path().join("data");
    let config = temporary.path().join("config.json");
    fs::write(
        &config,
        serde_json::to_vec(&json!({
            "query_retained_inputs": true,
            "flush_policy": "pressure_only",
            "maintenance_interval_ms": 3_600_000,
            "query_timeout_ms": 5_000
        }))
        .unwrap(),
    )
    .unwrap();
    let port = free_port();
    let _service = Service::start(&data, &config, port);
    post(port, "/v1/tables", table());
    let rows: Vec<_> = (0..130)
        .map(|timestamp| {
            json!({
                "timestamp_us": timestamp, "tenant": "a", "series": "cpu", "value": 1.0
            })
        })
        .collect();
    post(
        port,
        "/v1/write",
        json!({
            "table": "metrics", "request_id": "initial", "rows": rows
        }),
    );
    let query = json!({"sql": "SELECT count(*) AS count, sum(value) AS total FROM metrics"});
    assert_eq!(
        post(port, "/v1/query", query.clone()),
        json!([{"count":130,"total":130.0}])
    );
    let first = metrics_text(port);
    assert_eq!(
        metric_count(&first, "varve_query_resident_full_loads_total"),
        1
    );
    assert_eq!(
        metric_count(&first, "varve_query_resident_raw_staged_rows_total"),
        130
    );
    assert!(metric_count(&first, "varve_query_resident_raw_staged_bytes_total") > 0);
    assert_eq!(
        metric_count(&first, "varve_query_resident_dynamic_loads_total"),
        0
    );
    assert_eq!(
        metric_count(&first, "varve_query_resident_dynamic_staged_bytes_total"),
        0
    );
    assert_eq!(
        post(port, "/v1/query", query.clone()),
        json!([{"count":130,"total":130.0}])
    );
    let repeated = metrics_text(port);
    assert_eq!(
        metric_count(&repeated, "varve_query_resident_raw_staged_rows_total"),
        130
    );
    assert_eq!(
        metric_count(&repeated, "varve_query_resident_raw_staged_bytes_total"),
        metric_count(&first, "varve_query_resident_raw_staged_bytes_total")
    );
    assert!(metric_count(&repeated, "varve_query_resident_hits_total") >= 1);
    let delta = post(port, "/v1/write", batch("delta", 2.0));
    assert_eq!(
        post(port, "/v1/query", query),
        json!([{"count":132,"total":135.0}])
    );
    let after = metrics_text(port);
    assert_eq!(
        metric_count(&after, "varve_query_resident_raw_staged_rows_total"),
        132
    );
    assert!(metric_count(&after, "varve_query_resident_delta_loads_total") >= 1);
    assert_eq!(metric_count(&after, "varve_query_resident_idle_rows"), 132);
    assert!(metric_count(&after, "varve_query_resident_idle_bytes") > 0);
    assert!(metric_count(&after, "varve_query_resident_idle_materialized_bytes") > 0);
    assert_eq!(
        after
            .lines()
            .filter(|line| line.starts_with("varve_query_resident_idle_materialized_bytes "))
            .count(),
        1
    );
    assert_eq!(metric_count(&after, "varve_query_workers_active"), 0);
    assert_eq!(
        metric_count(&after, "varve_query_resident_dynamic_loads_total"),
        0
    );
    assert_eq!(
        post(
            port,
            "/v1/query",
            json!({"sql": "SELECT CAST(sequence AS BIGINT) AS sequence FROM varve_status()"})
        ),
        json!([{"sequence": delta["sequence"]}])
    );
    let metadata = metrics_text(port);
    assert!(metric_count(&metadata, "varve_query_resident_dynamic_loads_total") > 0);
    assert!(metric_count(&metadata, "varve_query_resident_dynamic_staged_bytes_total") > 0);
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

    let before = metrics_text(port);
    assert!(before.contains("# TYPE varve_phase_duration_seconds histogram"));
    assert!(phase_count(&before, "wal_encode") >= 2);
    assert!(phase_count(&before, "wal_sync") >= 4);
    assert!(phase_count(&before, "state_lock_wait") > 0);
    for phase in [
        "disk_lock_wait",
        "disk_lock_hold",
        "wal_disk_lock_wait",
        "group_prepare",
    ] {
        assert!(phase_count(&before, phase) > 0, "{phase}");
    }
    for phase in [
        "derived_verify",
        "derived_publish",
        "raw_verify",
        "raw_publish",
    ] {
        assert!(before.contains(&format!(
            "varve_phase_duration_seconds_count{{phase=\"{phase}\"}} "
        )));
    }
    for _ in 0..2 {
        assert_eq!(
            post(port, "/v1/query", json!({"sql":"SELECT 1 AS value"})),
            json!([{"value":1}])
        );
    }
    let after = metrics_text(port);
    assert_eq!(
        phase_count(&after, "query_run") - phase_count(&before, "query_run"),
        2
    );
    assert!(phase_count(&after, "query_spawn") - phase_count(&before, "query_spawn") <= 1);
    assert_eq!(metric_count(&after, "varve_query_workers_active"), 0);
    assert!(metric_count(&after, "varve_query_workers_idle") <= 2);
    assert!(
        metric_count(&after, "varve_query_workers_reused_total")
            - metric_count(&before, "varve_query_workers_reused_total")
            >= 1
    );
    assert_eq!(
        metric_count(&after, "varve_query_workers_resets_total")
            - metric_count(&before, "varve_query_workers_resets_total"),
        2
    );
    assert!(metric_count(&after, "varve_control_root_bytes") > 0);
    assert!(metric_count(&after, "varve_derived_resident_bytes") > 0);
    assert_eq!(metric_count(&after, "varve_derived_working_bytes"), 0);
    for name in [
        "full_loads_total",
        "delta_loads_total",
        "hits_total",
        "invalidations_total",
        "raw_staged_rows_total",
        "raw_staged_bytes_total",
        "idle_rows",
        "idle_bytes",
    ] {
        assert_eq!(
            metric_count(&after, &format!("varve_query_resident_{name}")),
            0
        );
    }

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
fn connection_lifetime_drains_an_active_write() {
    let _guard = service_test_guard();
    let temporary = TempDir::new().unwrap();
    let data = temporary.path().join("data");
    let config = temporary.path().join("config.json");
    write_config(&config, 60_000, 60_000_000);
    let port = free_port();
    let mut service = Service::start_with_env(
        &data,
        &config,
        port,
        &[
            ("VARVE_HTTP_CONNECTION_TIMEOUT_MS", "2500"),
            ("VARVE_HTTP_REQUEST_TIMEOUT_MS", "2000"),
            ("VARVE_HTTP_BODY_TIMEOUT_MS", "1800"),
            ("VARVE_HTTP_HEADER_TIMEOUT_MS", "3500"),
        ],
    );
    post(port, "/v1/tables", table());

    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .write_all(b"GET /health HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
        .unwrap();
    stream.flush().unwrap();
    let (status, body) = read_persistent_response(&mut stream).unwrap();
    assert_eq!(status, 200);
    assert_eq!(body, json!({"ok": true}));

    thread::sleep(Duration::from_millis(1800));
    let body = batch("lifetime-drain", 42.0).to_string();
    write!(
        stream,
        "POST /v1/write HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .unwrap();
    stream.flush().unwrap();
    thread::sleep(Duration::from_millis(1100));
    stream.write_all(body.as_bytes()).unwrap();
    stream.flush().unwrap();

    let (status, receipt) = read_persistent_response(&mut stream).unwrap();
    assert_eq!(status, 200, "HTTP error: {receipt}");
    assert_eq!(receipt["durability"], "local_fsync");
    let mut byte = [0_u8; 1];
    assert_eq!(stream.read(&mut byte).unwrap(), 0, "connection stayed open");

    service.kill();
    let db = varve::Database::open(&data, varve::Config::default()).unwrap();
    let rows = db.scan("metrics", None, None, None, None).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].row.value, 42.0);
    assert_eq!(rows[1].row.value, 43.0);
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

#[test]
fn rebuilt_http_pipeline_recovers_receipts_and_cancels_timed_out_native_queries() {
    let _guard = service_test_guard();
    let temporary = TempDir::new().unwrap();
    let data = temporary.path().join("data");
    let config = temporary.path().join("config.json");
    let library = std::env::var("VARVE_DUCKDB_V2_LIBRARY").expect("pinned native library required");
    fs::write(
        &config,
        serde_json::to_vec(&json!({
            "segmented_journal": true,
            "checkpoint_frozen_prefix": true,
            "derived_pages": true,
            "duckdb_library": library,
            "query_executable": "/varve-test-no-cli-fallback",
            "query_timeout_ms": 5000,
            "maintenance_interval_ms": 60000
        }))
        .unwrap(),
    )
    .unwrap();
    let port = free_port();
    let env = [
        ("VARVE_HTTP_HEADER_TIMEOUT_MS", "50"),
        ("VARVE_HTTP_BODY_TIMEOUT_MS", "50"),
        ("VARVE_HTTP_REQUEST_TIMEOUT_MS", "250"),
        ("VARVE_HTTP_CONNECTION_TIMEOUT_MS", "5000"),
    ];
    let mut service = Service::start_with_env(&data, &config, port, &env);
    post(port, "/v1/tables", table());
    let receipt = post(port, "/v1/write", batch("durable-native", 2.0));
    assert_eq!(receipt["durability"], "local_fsync");
    assert_eq!(
        post(
            port,
            "/v1/query",
            json!({"sql":"SELECT count(*) AS count, sum(value) AS total FROM metrics"})
        ),
        json!([{"count":2,"total":5.0}])
    );
    let (_, status) = request(port, "GET", "/v1/status", None, &[]).unwrap();
    assert_eq!(status["segmented_journal"], true);
    assert_eq!(status["native_query"]["version"], "v2.0.0-alpha41533");
    let (code, body) = request(port, "POST", "/v1/query", Some(&json!({"sql":"SELECT sum(m.value + r.i) FROM metrics m CROSS JOIN range(1000000000) r(i)"})), &[]).unwrap();
    assert_eq!(code, 504, "{body}");
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let (_, status) = request(port, "GET", "/v1/status", None, &[]).unwrap();
        if status["active_snapshots"] == 0 && status["active_queries"] == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "native cancellation retained pins: {status}"
        );
        thread::sleep(Duration::from_millis(10));
    }
    let metrics = metrics_text(port);
    assert_eq!(
        metric_count(&metrics, "varve_query_workers_spawned_total"),
        0
    );
    assert!(
        phase_count(&metrics, "query_run") >= 2,
        "native query timing must be recorded"
    );
    // SIGKILL deliberately bypasses clean shutdown: the acknowledged journal
    // frame must replay and deduplicate without a checkpoint or CLI fallback.
    service.kill();
    let _restarted = Service::start_with_env(&data, &config, port, &env);
    let duplicate = post(port, "/v1/write", batch("durable-native", 2.0));
    assert_eq!(duplicate["duplicate"], true);
    assert_eq!(duplicate["sequence"], receipt["sequence"]);
    assert_eq!(
        post(
            port,
            "/v1/query",
            json!({"sql":"SELECT count(*) AS count, sum(value) AS total FROM metrics"})
        ),
        json!([{"count":2,"total":5.0}])
    );
}
