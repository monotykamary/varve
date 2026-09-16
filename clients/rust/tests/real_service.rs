use std::collections::BTreeMap;
use std::env;
use std::fs::File;
use std::io::Read;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};
use varve_client::{
    Client, ClientConfig, ConnectError, Error, RequestId, Row, TableConfig, WriteError,
};

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

struct Service {
    child: Child,
    _data: TempDir,
    url: String,
}

impl Service {
    async fn start() -> Self {
        let binary = env::var_os("VARVE_TEST_BINARY")
            .expect("VARVE_TEST_BINARY must name the real Varve server binary");
        assert!(
            Path::new(&binary).is_file(),
            "VARVE_TEST_BINARY is not a file"
        );
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let data = tempfile::tempdir().unwrap();
        let stderr_path = data.path().join("server.stderr");
        let stderr = File::create(&stderr_path).unwrap();
        let child = Command::new(binary)
            .arg("--data")
            .arg(data.path().join("db"))
            .arg("serve")
            .arg("--port")
            .arg(port.to_string())
            .env("VARVE_API_TOKEN", TOKEN)
            .stdout(Stdio::null())
            .stderr(stderr)
            .spawn()
            .expect("start Varve test service");
        let mut service = Self {
            child,
            _data: data,
            url: format!("ws://127.0.0.1:{port}/v1/ws"),
        };
        // Allow cold binary loading on busy CI hosts, but fail fast on exit.
        for _ in 0..500 {
            if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                return service;
            }
            if let Some(status) = service.child.try_wait().unwrap() {
                let mut diagnostic = String::new();
                File::open(&stderr_path)
                    .unwrap()
                    .take(4096)
                    .read_to_string(&mut diagnostic)
                    .unwrap();
                panic!("Varve exited before listening ({status}): {diagnostic}");
            }
            sleep(Duration::from_millis(20)).await;
        }
        let mut diagnostic = String::new();
        File::open(&stderr_path)
            .unwrap()
            .take(4096)
            .read_to_string(&mut diagnostic)
            .unwrap();
        panic!("Varve test service did not listen before the deadline: {diagnostic}");
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn row(timestamp_us: i64, value: f64) -> Row {
    Row {
        timestamp_us,
        tenant: "integration".into(),
        series: "cpu".into(),
        value,
        tags: BTreeMap::from([("source".into(), "rust-client".into())]),
    }
}

#[tokio::test]
async fn real_service_auth_writes_query_errors_deadline_disconnect_and_close() {
    let mut service = Service::start().await;

    let auth_error = match Client::connect(&service.url, "incorrect-token").await {
        Ok(_) => panic!("invalid token unexpectedly authenticated"),
        Err(error) => error,
    };
    assert!(matches!(auth_error, ConnectError::Authentication(_)));

    let client = Client::connect(&service.url, TOKEN).await.unwrap();
    client.ping().await.unwrap();
    let sequence = client
        .create_table(
            "metrics",
            TableConfig {
                window_us: 1,
                rollup_widths_us: vec![],
                ..TableConfig::default()
            },
        )
        .await
        .unwrap();
    assert!(sequence > 0);

    let one = client.insert(
        "metrics",
        row(i64::MIN, 0.10000000000000002),
        RequestId::new("real-single").unwrap(),
    );
    let batch = client.insert_batch(
        "metrics",
        vec![row(0, 1.5), row(i64::MAX, 2.5)],
        RequestId::new("real-batch").unwrap(),
    );
    let (one, batch) = tokio::join!(one, batch);
    let one = one.unwrap();
    let batch = batch.unwrap();
    assert_eq!(one.rows, 1);
    assert_eq!(batch.rows, 2);
    let committed_sequence = one.sequence.max(batch.sequence);

    let duplicate = client
        .insert(
            "metrics",
            row(i64::MIN, 0.10000000000000002),
            RequestId::new("real-single").unwrap(),
        )
        .await
        .unwrap();
    assert!(duplicate.duplicate);
    let conflict = client
        .insert(
            "metrics",
            row(i64::MIN, 9.0),
            RequestId::new("real-single").unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(conflict, WriteError::OutcomeUnknown { .. }));

    let queried = client
        .query("SELECT CAST(timestamp_us AS VARCHAR) AS timestamp_us, value FROM metrics ORDER BY timestamp_us")
        .await
        .unwrap();
    let rows = queried.as_array().unwrap();
    assert_eq!(rows.len(), 3);
    assert!(
        rows.iter()
            .any(|value| value["timestamp_us"].as_str() == Some("-9223372036854775808"))
    );
    assert!(
        rows.iter()
            .any(|value| value["timestamp_us"].as_str() == Some("9223372036854775807"))
    );
    let status = client.status().await.unwrap();
    assert!(status.sequence >= committed_sequence);
    assert_eq!(status.tables, 1);
    client.close().await.unwrap();
    assert!(matches!(client.ping().await, Err(Error::Closed)));

    let deadline_client = Client::connect_with_config(
        &service.url,
        TOKEN,
        ClientConfig {
            request_timeout: Duration::from_millis(1),
            ..ClientConfig::default()
        },
    )
    .await
    .unwrap();
    let deadline = deadline_client
        .query("SELECT sum(i) FROM range(100000000) AS values(i)")
        .await
        .unwrap_err();
    assert!(matches!(deadline, Error::RequestTimeout(_)));

    service.child.kill().unwrap();
    service.child.wait().unwrap();
    timeout(Duration::from_secs(2), async {
        loop {
            match deadline_client.ping().await {
                Err(Error::Disconnected(_) | Error::Closed) => break,
                Err(Error::RequestTimeout(_)) => tokio::task::yield_now().await,
                other => panic!("unexpected result while observing disconnect: {other:?}"),
            }
        }
    })
    .await
    .expect("disconnect surfaced before deadline");
}
