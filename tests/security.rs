#![cfg(unix)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

use tempfile::TempDir;

const TOKEN: &str = "varve-test-operator-token-32-bytes-minimum";
static SECURITY_TEST_LOCK: Mutex<()> = Mutex::new(());

fn security_test_guard() -> MutexGuard<'static, ()> {
    SECURITY_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct Service {
    child: Child,
    port: u16,
}

impl Service {
    fn start(data: &Path, bind: &str, extra: &[&str]) -> Self {
        let port = free_port();
        let mut command = Command::new(env!("CARGO_BIN_EXE_varve"));
        command
            .arg("--data")
            .arg(data)
            .arg("serve")
            .arg("--bind")
            .arg(bind)
            .args(extra)
            .env("PORT", port.to_string())
            .env("VARVE_API_TOKEN", TOKEN)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        let child = command.spawn().expect("start service");
        let mut service = Self { child, port };
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = service.child.try_wait().expect("poll service") {
                let mut stderr = String::new();
                service
                    .child
                    .stderr
                    .take()
                    .expect("stderr")
                    .read_to_string(&mut stderr)
                    .expect("read stderr");
                panic!("service exited during startup ({status}): {stderr}");
            }
            if raw_request(
                port,
                "GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            )
            .is_ok_and(|response| status(&response) == 200)
            {
                return service;
            }
            assert!(Instant::now() < deadline, "service did not become live");
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn terminate(&mut self) {
        let sent = Command::new("kill")
            .arg("-TERM")
            .arg(self.child.id().to_string())
            .status()
            .expect("send SIGTERM");
        assert!(sent.success());
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            if let Some(status) = self.child.try_wait().expect("poll service") {
                assert!(status.success(), "service exit status: {status}");
                return;
            }
            assert!(Instant::now() < deadline, "SIGTERM drain exceeded bound");
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
    TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn raw_request(port: u16, request: &str) -> Result<String, String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).map_err(|error| error.to_string())?;
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .map_err(|error| error.to_string())?;
    stream
        .write_all(request.as_bytes())
        .map_err(|error| error.to_string())?;
    stream.flush().map_err(|error| error.to_string())?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|error| error.to_string())?;
    Ok(response)
}

fn request(port: u16, method: &str, path: &str, body: &str, token: Option<&str>) -> String {
    let authorization = token
        .map(|token| format!("Authorization: Bearer {token}\r\n"))
        .unwrap_or_default();
    let content_type = if method == "POST" {
        "Content-Type: application/json\r\n"
    } else {
        ""
    };
    raw_request(
        port,
        &format!(
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n{authorization}{content_type}Content-Length: {}\r\n\r\n{body}",
            body.len()
        ),
    )
    .expect("HTTP response")
}

fn status(response: &str) -> u16 {
    response
        .split_whitespace()
        .nth(1)
        .expect("HTTP status")
        .parse()
        .expect("numeric HTTP status")
}

fn body(response: &str) -> &str {
    response
        .split_once("\r\n\r\n")
        .expect("HTTP response body")
        .1
}

#[test]
fn configured_token_protects_every_api_but_minimal_probes() {
    let _guard = security_test_guard();
    let temporary = TempDir::new().unwrap();
    let mut service = Service::start(temporary.path(), "127.0.0.1", &[]);

    assert_eq!(
        status(&request(service.port, "GET", "/health", "", None)),
        200
    );
    assert_eq!(
        status(&request(service.port, "GET", "/ready", "", None)),
        200
    );
    assert_eq!(
        status(&request(service.port, "GET", "/v1/status", "", None)),
        401
    );
    let unauthorized_body = raw_request(
        service.port,
        "POST /v1/query HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 1024\r\nConnection: close\r\n\r\n",
    )
    .expect("unauthorized response before body");
    assert_eq!(status(&unauthorized_body), 401, "{unauthorized_body}");
    assert_eq!(
        status(&request(
            service.port,
            "GET",
            "/v1/status",
            "",
            Some("wrong-token")
        )),
        401
    );
    assert_eq!(
        status(&request(service.port, "GET", "/v1/status", "", Some(TOKEN))),
        200
    );

    let created = request(
        service.port,
        "POST",
        "/v1/tables",
        r#"{"name":"metrics","config":{}}"#,
        Some(TOKEN),
    );
    assert_eq!(status(&created), 200, "{created}");
    for path in ["/v1/tables", "/v1/policies", "/v1/aggregates", "/v1/jobs"] {
        let metadata = request(service.port, "GET", path, "", Some(TOKEN));
        assert_eq!(status(&metadata), 200, "{path}: {metadata}");
    }

    let duplicate = raw_request(
        service.port,
        &format!(
            "GET /v1/status HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {TOKEN}\r\nAuthorization: Bearer {TOKEN}\r\nConnection: close\r\n\r\n"
        ),
    )
    .unwrap();
    assert_eq!(status(&duplicate), 400, "{duplicate}");

    let metrics = request(service.port, "GET", "/metrics", "", Some(TOKEN));
    assert_eq!(status(&metrics), 200, "{metrics}");
    for name in [
        "varve_http_auth_failures_total",
        "varve_metadata_bytes",
        "varve_unshipped_batches",
        "varve_idempotency_keys",
        "varve_rollup_groups",
        "varve_maintenance_failed",
    ] {
        assert!(body(&metrics).contains(name), "missing metric {name}");
    }
    for line in body(&metrics)
        .lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
    {
        let mut fields = line.split_whitespace();
        assert!(fields.next().unwrap().starts_with("varve_"));
        assert!(fields.next().unwrap().parse::<f64>().unwrap().is_finite());
        assert!(
            fields.next().is_none(),
            "metric must not contain user labels"
        );
    }
    assert!(!body(&metrics).contains(TOKEN));
    service.terminate();
}

#[test]
fn public_bind_requires_opt_in_and_a_strong_environment_token() {
    let _guard = security_test_guard();
    let temporary = TempDir::new().unwrap();
    let port = free_port();
    let run = |extra: &[&str], token: Option<&str>| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_varve"));
        command
            .arg("--data")
            .arg(temporary.path().join(format!("db-{}", extra.len())))
            .arg("serve")
            .arg("--bind")
            .arg("0.0.0.0")
            .arg("--port")
            .arg(port.to_string())
            .args(extra)
            .env_remove("VARVE_API_TOKEN");
        if let Some(token) = token {
            command.env("VARVE_API_TOKEN", token);
        }
        command.output().expect("run public bind gate")
    };

    let no_opt_in = run(&[], None);
    assert!(!no_opt_in.status.success());
    assert!(String::from_utf8_lossy(&no_opt_in.stderr).contains("--allow-remote"));

    let no_token = run(&["--allow-remote"], None);
    assert!(!no_token.status.success());
    assert!(String::from_utf8_lossy(&no_token.stderr).contains("VARVE_API_TOKEN"));

    let weak = "too-short";
    let weak_token = run(&["--allow-remote"], Some(weak));
    assert!(!weak_token.status.success());
    let stderr = String::from_utf8_lossy(&weak_token.stderr);
    assert!(stderr.contains("at least 32 bytes"));
    assert!(!stderr.contains(weak));

    let mut service = Service::start(
        &temporary.path().join("public"),
        "0.0.0.0",
        &["--allow-remote"],
    );
    assert_eq!(
        status(&request(service.port, "GET", "/v1/status", "", Some(TOKEN))),
        200
    );
    service.terminate();
}

#[test]
fn slow_chunked_and_queued_requests_are_bounded() {
    let _guard = security_test_guard();
    let temporary = TempDir::new().unwrap();
    let mut service = Service::start(
        temporary.path(),
        "127.0.0.1",
        &[
            "--request-workers",
            "1",
            "--request-queue",
            "1",
            "--max-connections",
            "4",
            "--max-body-bytes",
            "64",
            "--body-timeout-ms",
            "150",
            "--request-timeout-ms",
            "500",
            "--connection-timeout-ms",
            "1000",
        ],
    );

    let mut slow = TcpStream::connect(("127.0.0.1", service.port)).unwrap();
    slow.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    write!(
        slow,
        "POST /v1/query HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\nContent-Length: 16\r\nConnection: close\r\n\r\n{{"
    )
    .unwrap();
    slow.flush().unwrap();
    thread::sleep(Duration::from_millis(250));
    let mut slow_response = String::new();
    slow.read_to_string(&mut slow_response).unwrap();
    assert_eq!(status(&slow_response), 408, "{slow_response}");

    let oversized_chunked = raw_request(
        service.port,
        &format!(
            "POST /v1/query HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n40\r\n{}\r\n10\r\n{}\r\n0\r\n\r\n",
            "a".repeat(64),
            "b".repeat(16)
        ),
    )
    .unwrap();
    assert_eq!(status(&oversized_chunked), 413, "{oversized_chunked}");

    let mut blocked = Vec::new();
    for _ in 0..2 {
        let mut stream = TcpStream::connect(("127.0.0.1", service.port)).unwrap();
        write!(
            stream,
            "POST /v1/query HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\nContent-Length: 16\r\nConnection: close\r\n\r\n"
        )
        .unwrap();
        stream.flush().unwrap();
        blocked.push(stream);
    }
    thread::sleep(Duration::from_millis(40));
    let saturated = request(service.port, "GET", "/v1/status", "", Some(TOKEN));
    assert_eq!(status(&saturated), 503, "{saturated}");
    let ready = request(service.port, "GET", "/ready", "", None);
    assert_eq!(
        status(&ready),
        200,
        "readiness must bypass the full queue: {ready}"
    );
    let still_saturated = request(service.port, "GET", "/v1/status", "", Some(TOKEN));
    assert_eq!(status(&still_saturated), 503, "{still_saturated}");
    drop(blocked);
    service.terminate();
}

#[test]
fn sigterm_drains_within_the_configured_bound() {
    let _guard = security_test_guard();
    let temporary = TempDir::new().unwrap();
    let mut service = Service::start(
        temporary.path(),
        "127.0.0.1",
        &["--shutdown-timeout-ms", "500"],
    );
    let started = Instant::now();
    service.terminate();
    assert!(started.elapsed() < Duration::from_secs(3));
}
