use std::collections::BTreeMap;
use std::future::Future;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL;
use tokio_tungstenite::tungstenite::protocol::Message;
use varve_client::{
    AmbiguousWrite, Client, ClientConfig, ConnectError, Error, RequestId, Row, ServerError,
    WriteError,
};

type FixtureSocket = WebSocketStream<TcpStream>;

#[allow(clippy::result_large_err)]
async fn fixture_server<F, Fut>(handler: F) -> (String, JoinHandle<()>)
where
    F: FnOnce(FixtureSocket) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let socket = accept_hdr_async(stream, |request: &Request, mut response: Response| {
            assert_eq!(request.uri().path(), "/v1/ws");
            assert_eq!(
                request
                    .headers()
                    .get(SEC_WEBSOCKET_PROTOCOL)
                    .and_then(|value| value.to_str().ok()),
                Some("varve.v1")
            );
            response
                .headers_mut()
                .insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static("varve.v1"));
            Ok(response)
        })
        .await
        .unwrap();
        handler(socket).await;
    });
    (format!("ws://{address}/v1/ws"), task)
}

async fn read_json(socket: &mut FixtureSocket) -> Value {
    let message = timeout(Duration::from_secs(2), socket.next())
        .await
        .expect("fixture receive deadline")
        .expect("fixture peer closed")
        .expect("fixture receive error");
    serde_json::from_str(message.to_text().expect("text frame")).unwrap()
}

async fn send_json(socket: &mut FixtureSocket, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}

async fn accept_auth(socket: &mut FixtureSocket, expected_token: &str) {
    let auth = read_json(socket).await;
    assert_eq!(auth["jsonrpc"], "2.0");
    assert_eq!(auth["id"], "auth");
    assert_eq!(auth["method"], "auth");
    assert_eq!(auth["params"]["token"], expected_token);
    send_json(
        socket,
        json!({"jsonrpc":"2.0","id":"auth","result":{"protocol":1}}),
    )
    .await;
}

fn row(timestamp_us: i64, value: f64) -> Row {
    Row {
        timestamp_us,
        tenant: "tenant".into(),
        series: "cpu".into(),
        value,
        tags: BTreeMap::new(),
    }
}

fn response_for(request: &Value, result: Value) -> Value {
    json!({"jsonrpc":"2.0","id":request["id"],"result":result})
}

#[tokio::test]
async fn correlates_concurrent_single_and_batch_writes_out_of_order() {
    let (url, server) = fixture_server(|mut socket| async move {
        accept_auth(&mut socket, "secret").await;
        let first = read_json(&mut socket).await;
        let second = read_json(&mut socket).await;
        let requests = [&first, &second];
        let single = requests
            .iter()
            .find(|request| request["params"]["request_id"] == "single")
            .unwrap();
        let batch = requests
            .iter()
            .find(|request| request["params"]["request_id"] == "batch")
            .unwrap();
        assert_eq!(
            single["params"]["rows"][0]["timestamp_us"].as_i64(),
            Some(i64::MIN)
        );
        assert_eq!(
            batch["params"]["rows"][1]["timestamp_us"].as_i64(),
            Some(i64::MAX)
        );
        send_json(
            &mut socket,
            response_for(
                batch,
                json!({"sequence":12,"rows":2,"duplicate":false,"durability":"local_fsync"}),
            ),
        )
        .await;
        send_json(
            &mut socket,
            response_for(
                single,
                json!({"sequence":11,"rows":1,"duplicate":false,"durability":"local_fsync"}),
            ),
        )
        .await;
        let _ = socket.next().await;
    })
    .await;
    let client = Client::connect(&url, "secret").await.unwrap();
    let single = client.insert(
        "metrics",
        row(i64::MIN, 1.0),
        RequestId::new("single").unwrap(),
    );
    let batch = client.insert_batch(
        "metrics",
        vec![row(0, 2.0), row(i64::MAX, 3.0)],
        RequestId::new("batch").unwrap(),
    );
    let (single, batch) = tokio::join!(single, batch);
    assert_eq!(single.unwrap().sequence, 11);
    assert_eq!(batch.unwrap().sequence, 12);
    client.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn reports_authentication_rejection() {
    let (url, server) = fixture_server(|mut socket| async move {
        let auth = read_json(&mut socket).await;
        assert_eq!(auth["params"]["token"], "wrong");
        send_json(
            &mut socket,
            json!({"jsonrpc":"2.0","id":"auth","error":{"code":-32001,"message":"unauthorized"}}),
        )
        .await;
    })
    .await;
    let error = match Client::connect(&url, "wrong").await {
        Ok(_) => panic!("authentication unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        ConnectError::Authentication(ServerError { code: -32001, .. })
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn distinguishes_admission_rejection_from_ambiguous_operation_failure() {
    let (url, server) = fixture_server(|mut socket| async move {
        accept_auth(&mut socket, "").await;
        let admission = read_json(&mut socket).await;
        send_json(
            &mut socket,
            json!({"jsonrpc":"2.0","id":admission["id"],"error":{"code":-32003,"message":"busy"}}),
        )
        .await;
        let operation = read_json(&mut socket).await;
        send_json(
            &mut socket,
            json!({"jsonrpc":"2.0","id":operation["id"],"error":{"code":-32000,"message":"publication uncertain"}}),
        )
        .await;
        let _ = socket.next().await;
    })
    .await;
    let client = Client::connect(&url, "").await.unwrap();
    let rejected = client
        .insert("metrics", row(1, 1.0), RequestId::new("reject").unwrap())
        .await
        .unwrap_err();
    assert!(matches!(rejected, WriteError::AdmissionRejected(_)));
    assert!(!rejected.may_have_committed());
    let unknown = client
        .insert("metrics", row(2, 2.0), RequestId::new("unknown").unwrap())
        .await
        .unwrap_err();
    assert!(matches!(
        unknown,
        WriteError::OutcomeUnknown {
            cause: AmbiguousWrite::ServerOperation(_),
            ..
        }
    ));
    assert!(unknown.may_have_committed());
    client.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn accepted_write_disconnect_has_unknown_outcome() {
    let (url, server) = fixture_server(|mut socket| async move {
        accept_auth(&mut socket, "").await;
        let request = read_json(&mut socket).await;
        assert_eq!(request["method"], "write");
        socket.close(None).await.unwrap();
    })
    .await;
    let client = Client::connect(&url, "").await.unwrap();
    let error = client
        .insert(
            "metrics",
            row(1, 1.0),
            RequestId::new("disconnect").unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        WriteError::OutcomeUnknown {
            cause: AmbiguousWrite::Disconnected(_),
            ..
        }
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn enforces_outbound_size_and_request_deadline() {
    let (url, server) = fixture_server(|mut socket| async move {
        accept_auth(&mut socket, "").await;
        let request = read_json(&mut socket).await;
        assert_eq!(request["method"], "query");
        sleep(Duration::from_millis(100)).await;
    })
    .await;
    let config = ClientConfig {
        max_message_bytes: 512,
        request_timeout: Duration::from_millis(30),
        ..ClientConfig::default()
    };
    let client = Client::connect_with_config(&url, "", config).await.unwrap();
    let oversized = client.query(&"x".repeat(1_000)).await.unwrap_err();
    assert!(matches!(oversized, Error::MessageTooLarge { .. }));
    let deadline = client.query("SELECT 1").await.unwrap_err();
    assert!(matches!(deadline, Error::RequestTimeout(_)));
    server.await.unwrap();
}

#[tokio::test]
async fn cancellation_removes_pending_call_and_releases_capacity() {
    let (received_first, first_arrived) = tokio::sync::oneshot::channel();
    let (url, server) = fixture_server(|mut socket| async move {
        accept_auth(&mut socket, "").await;
        let _cancelled = read_json(&mut socket).await;
        let _ = received_first.send(());
        let ping = read_json(&mut socket).await;
        send_json(&mut socket, response_for(&ping, json!({"pong":true}))).await;
        let _ = socket.next().await;
    })
    .await;
    let config = ClientConfig {
        max_pending_calls: 1,
        ..ClientConfig::default()
    };
    let client = Client::connect_with_config(&url, "", config).await.unwrap();
    let cancelled_client = client.clone();
    let call = tokio::spawn(async move { cancelled_client.query("SELECT slow").await });
    first_arrived.await.unwrap();
    call.abort();
    let _ = call.await;
    tokio::task::yield_now().await;
    client.ping().await.unwrap();
    client.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn aggregate_pending_bytes_are_bounded_and_released_on_cancellation() {
    let (received_first, first_arrived) = tokio::sync::oneshot::channel();
    let (url, server) = fixture_server(|mut socket| async move {
        accept_auth(&mut socket, "").await;
        let _held = read_json(&mut socket).await;
        let _ = received_first.send(());
        let ping = read_json(&mut socket).await;
        send_json(&mut socket, response_for(&ping, json!({"pong":true}))).await;
        let _ = socket.next().await;
    })
    .await;
    let config = ClientConfig {
        max_pending_calls: 2,
        max_pending_bytes: 180,
        max_message_bytes: 512,
        ..ClientConfig::default()
    };
    let client = Client::connect_with_config(&url, "", config).await.unwrap();
    let held_client = client.clone();
    let held = tokio::spawn(async move { held_client.query(&"x".repeat(80)).await });
    first_arrived.await.unwrap();
    let overflow = client.query(&"y".repeat(80)).await.unwrap_err();
    assert!(matches!(overflow, Error::PendingBytesExceeded));
    held.abort();
    let _ = held.await;
    tokio::task::yield_now().await;
    client.ping().await.unwrap();
    client.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn close_sends_a_websocket_close_and_rejects_later_calls() {
    let (closed, saw_close) = tokio::sync::oneshot::channel();
    let (url, server) = fixture_server(|mut socket| async move {
        accept_auth(&mut socket, "").await;
        let message = socket.next().await.unwrap().unwrap();
        assert!(message.is_close());
        let _ = closed.send(());
    })
    .await;
    let client = Client::connect(&url, "").await.unwrap();
    client.close().await.unwrap();
    saw_close.await.unwrap();
    assert!(matches!(client.ping().await, Err(Error::Closed)));
    server.await.unwrap();
}

#[tokio::test]
async fn credentials_in_urls_are_rejected_without_echoing_them() {
    let error = match Client::connect("ws://operator:secret@example.invalid/v1/ws", "ignored").await
    {
        Ok(_) => panic!("credential-bearing URL unexpectedly accepted"),
        Err(error) => error,
    };
    assert!(matches!(error, ConnectError::InvalidUrl(_)));
    assert!(!error.to_string().contains("secret"));

    let query_error =
        match Client::connect("ws://example.invalid/v1/ws?token=secret", "ignored").await {
            Ok(_) => panic!("query-bearing URL unexpectedly accepted"),
            Err(error) => error,
        };
    assert!(matches!(query_error, ConnectError::InvalidUrl(_)));
    assert!(!query_error.to_string().contains("secret"));
}
