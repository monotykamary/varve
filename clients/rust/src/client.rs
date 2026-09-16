use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use futures_util::{Sink, SinkExt, Stream, StreamExt};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL;
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};
use url::Url;

use crate::{
    AmbiguousWrite, ConnectError, Error, RequestId, Row, ServerError, Status, TableConfig,
    WriteError, WriteReceipt,
};

#[cfg(test)]
mod tests;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type PendingMap = Arc<Mutex<HashMap<String, Pending>>>;

#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub max_pending_calls: usize,
    pub max_pending_bytes: usize,
    pub max_message_bytes: usize,
    pub connect_timeout: Duration,
    pub auth_timeout: Duration,
    pub request_timeout: Duration,
    pub close_timeout: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            max_pending_calls: 128,
            max_pending_bytes: 8 * 1024 * 1024,
            max_message_bytes: 4 * 1024 * 1024,
            connect_timeout: Duration::from_secs(10),
            auth_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            close_timeout: Duration::from_secs(5),
        }
    }
}

impl ClientConfig {
    fn validate(&self) -> Result<(), ConnectError> {
        if self.max_pending_calls == 0 {
            return Err(ConnectError::InvalidConfig(
                "max_pending_calls must be greater than zero",
            ));
        }
        if self.max_pending_bytes == 0 || self.max_pending_bytes > u32::MAX as usize {
            return Err(ConnectError::InvalidConfig(
                "max_pending_bytes must be between 1 and u32::MAX",
            ));
        }
        if self.max_message_bytes == 0 {
            return Err(ConnectError::InvalidConfig(
                "max_message_bytes must be greater than zero",
            ));
        }
        for (duration, name) in [
            (self.connect_timeout, "connect_timeout"),
            (self.auth_timeout, "auth_timeout"),
            (self.request_timeout, "request_timeout"),
            (self.close_timeout, "close_timeout"),
        ] {
            if duration.is_zero() {
                return Err(ConnectError::InvalidConfig(match name {
                    "connect_timeout" => "connect_timeout must be greater than zero",
                    "auth_timeout" => "auth_timeout must be greater than zero",
                    "request_timeout" => "request_timeout must be greater than zero",
                    _ => "close_timeout must be greater than zero",
                }));
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

struct Inner {
    commands: mpsc::Sender<Command>,
    pending: PendingMap,
    permits: Arc<Semaphore>,
    byte_permits: Arc<Semaphore>,
    next_id: AtomicU64,
    closed: Arc<AtomicBool>,
    task: AsyncMutex<Option<JoinHandle<()>>>,
    config: ClientConfig,
}

struct Pending {
    response: oneshot::Sender<PendingOutcome>,
    _permit: OwnedSemaphorePermit,
    _byte_permit: Arc<OwnedSemaphorePermit>,
}

enum PendingOutcome {
    Result(Value),
    Server(ServerError),
    Failed(Error),
}

enum Command {
    Send {
        text: String,
        byte_permit: Arc<OwnedSemaphorePermit>,
    },
    Close {
        acknowledged: oneshot::Sender<()>,
    },
}

// Field drop order matters: a failed/cancelled send can leave bytes in the sink.
// Discard the sink before returning its reservation, including on task abort.
struct ChargedSocket<S> {
    socket: S,
    writing: Option<Arc<OwnedSemaphorePermit>>,
}

struct ConnectionGuard {
    pending: PendingMap,
    closed: Arc<AtomicBool>,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        fail_pending(&self.pending, Error::Closed);
    }
}

struct PendingGuard {
    id: String,
    pending: PendingMap,
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        lock_pending(&self.pending).remove(&self.id);
    }
}

struct CallFailure {
    accepted: bool,
    kind: CallFailureKind,
}

enum CallFailureKind {
    Client(Error),
    Server(ServerError),
}

impl Client {
    pub async fn connect(
        url: impl AsRef<str>,
        token: impl AsRef<str>,
    ) -> Result<Self, ConnectError> {
        Self::connect_with_config(url, token, ClientConfig::default()).await
    }

    pub async fn connect_with_config(
        url: impl AsRef<str>,
        token: impl AsRef<str>,
        config: ClientConfig,
    ) -> Result<Self, ConnectError> {
        config.validate()?;
        let url = validate_url(url.as_ref())?;
        let mut request = url
            .as_str()
            .into_client_request()
            .map_err(|error| ConnectError::Handshake(error.to_string()))?;
        request
            .headers_mut()
            .insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static("varve.v1"));
        let websocket_config = WebSocketConfig::default()
            .max_message_size(Some(config.max_message_bytes))
            .max_frame_size(Some(config.max_message_bytes));
        let connect = connect_async_with_config(request, Some(websocket_config), false);
        let (mut socket, response) = timeout(config.connect_timeout, connect)
            .await
            .map_err(|_| ConnectError::Timeout(config.connect_timeout))?
            .map_err(|error| ConnectError::Handshake(error.to_string()))?;
        let negotiated = response
            .headers()
            .get(SEC_WEBSOCKET_PROTOCOL)
            .and_then(|value| value.to_str().ok());
        if negotiated != Some("varve.v1") {
            let _ = timeout(config.close_timeout, socket.close(None)).await;
            return Err(ConnectError::Subprotocol);
        }
        authenticate(&mut socket, token.as_ref(), &config).await?;

        let pending = Arc::new(Mutex::new(HashMap::new()));
        let closed = Arc::new(AtomicBool::new(false));
        let (commands, receiver) = mpsc::channel(config.max_pending_calls);
        let task = tokio::spawn(connection_task(
            socket,
            receiver,
            Arc::clone(&pending),
            Arc::clone(&closed),
            config.request_timeout,
            config.close_timeout,
        ));
        Ok(Self {
            inner: Arc::new(Inner {
                commands,
                pending,
                permits: Arc::new(Semaphore::new(config.max_pending_calls)),
                byte_permits: Arc::new(Semaphore::new(config.max_pending_bytes)),
                next_id: AtomicU64::new(1),
                closed,
                task: AsyncMutex::new(Some(task)),
                config,
            }),
        })
    }

    pub async fn insert(
        &self,
        table: &str,
        row: Row,
        request_id: RequestId,
    ) -> Result<WriteReceipt, WriteError> {
        self.insert_batch(table, vec![row], request_id).await
    }

    pub async fn insert_batch(
        &self,
        table: &str,
        rows: Vec<Row>,
        request_id: RequestId,
    ) -> Result<WriteReceipt, WriteError> {
        if rows.is_empty() {
            return Err(WriteError::NotSent(Error::InvalidRequest(
                "write batch must contain at least one row".into(),
            )));
        }
        for row in &rows {
            if let Err(error) = row.validate() {
                return Err(WriteError::NotSent(Error::InvalidRequest(error.into())));
            }
        }
        let params = json!({
            "table": table,
            "request_id": request_id.as_str(),
            "rows": rows,
        });
        match self.call_value("write", params).await {
            Ok(value) => {
                serde_json::from_value(value).map_err(|error| WriteError::OutcomeUnknown {
                    request_id,
                    cause: AmbiguousWrite::Protocol(format!("decode write receipt: {error}")),
                })
            }
            Err(failure) => Err(map_write_failure(request_id, failure)),
        }
    }

    pub async fn query(&self, sql: &str) -> Result<Value, Error> {
        self.call("query", json!({ "sql": sql })).await
    }

    pub async fn create_table(&self, name: &str, config: TableConfig) -> Result<u64, Error> {
        self.call("create", json!({ "name": name, "config": config }))
            .await
    }

    pub async fn status(&self) -> Result<Status, Error> {
        self.call("status", json!({})).await
    }

    pub async fn ping(&self) -> Result<(), Error> {
        #[derive(serde::Deserialize)]
        struct Pong {
            pong: bool,
        }
        let pong: Pong = self.call("ping", json!({})).await?;
        if pong.pong {
            Ok(())
        } else {
            Err(Error::Protocol(
                "ping response did not contain pong=true".into(),
            ))
        }
    }

    pub async fn close(&self) -> Result<(), Error> {
        let closing = async {
            if !self.inner.closed.swap(true, Ordering::AcqRel) {
                let (acknowledged, response) = oneshot::channel();
                timeout(
                    self.inner.config.close_timeout,
                    self.inner.commands.send(Command::Close { acknowledged }),
                )
                .await
                .map_err(|_| Error::RequestTimeout(self.inner.config.close_timeout))?
                .map_err(|_| Error::Closed)?;
                timeout(self.inner.config.close_timeout, response)
                    .await
                    .map_err(|_| Error::RequestTimeout(self.inner.config.close_timeout))?
                    .map_err(|_| {
                        Error::Disconnected("connection task stopped during close".into())
                    })?;
            }
            Ok(())
        }
        .await;
        // Even a failed close enqueue/ack must join or abort the writer so that
        // queued frames and any bytes retained by the sink are discarded.
        if let Some(mut task) = self.inner.task.lock().await.take() {
            match timeout(self.inner.config.close_timeout, &mut task).await {
                Ok(joined) => joined.map_err(|error| {
                    Error::Transport(format!("connection task failed: {error}"))
                })?,
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    fail_pending(&self.inner.pending, Error::Closed);
                    return Err(Error::RequestTimeout(self.inner.config.close_timeout));
                }
            }
        }
        closing
    }

    async fn call<T: DeserializeOwned>(&self, method: &str, params: Value) -> Result<T, Error> {
        let value =
            self.call_value(method, params)
                .await
                .map_err(|failure| match failure.kind {
                    CallFailureKind::Client(error) => error,
                    CallFailureKind::Server(error) => Error::Server(error),
                })?;
        serde_json::from_value(value)
            .map_err(|error| Error::Protocol(format!("decode {method} result: {error}")))
    }

    async fn call_value(&self, method: &str, params: Value) -> Result<Value, CallFailure> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(not_accepted(Error::Closed));
        }
        let permit = Arc::clone(&self.inner.permits)
            .try_acquire_owned()
            .map_err(|_| not_accepted(Error::TooManyPending))?;
        let id = self.next_wire_id();
        let request = RpcRequest {
            jsonrpc: "2.0",
            id: &id,
            method,
            params,
        };
        let text = serde_json::to_string(&request)
            .map_err(|error| not_accepted(Error::Protocol(format!("encode request: {error}"))))?;
        if text.len() > self.inner.config.max_message_bytes {
            return Err(not_accepted(Error::MessageTooLarge {
                actual: text.len(),
                maximum: self.inner.config.max_message_bytes,
            }));
        }
        if text.len() > self.inner.config.max_pending_bytes {
            return Err(not_accepted(Error::PendingBytesExceeded));
        }
        let byte_permit = Arc::new(
            Arc::clone(&self.inner.byte_permits)
                .try_acquire_many_owned(text.len() as u32)
                .map_err(|_| not_accepted(Error::PendingBytesExceeded))?,
        );
        let (response, receiver) = oneshot::channel();
        lock_pending(&self.inner.pending).insert(
            id.clone(),
            Pending {
                response,
                _permit: permit,
                _byte_permit: Arc::clone(&byte_permit),
            },
        );
        let guard = PendingGuard {
            id,
            pending: Arc::clone(&self.inner.pending),
        };
        match self
            .inner
            .commands
            .try_send(Command::Send { text, byte_permit })
        {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                return Err(not_accepted(Error::TooManyPending));
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Err(not_accepted(Error::Closed));
            }
        }
        let outcome = timeout(self.inner.config.request_timeout, receiver).await;
        drop(guard);
        match outcome {
            Err(_) => Err(CallFailure {
                accepted: true,
                kind: CallFailureKind::Client(Error::RequestTimeout(
                    self.inner.config.request_timeout,
                )),
            }),
            Ok(Err(_)) => Err(CallFailure {
                accepted: true,
                kind: CallFailureKind::Client(Error::Disconnected(
                    "connection ended before a response".into(),
                )),
            }),
            Ok(Ok(PendingOutcome::Result(value))) => Ok(value),
            Ok(Ok(PendingOutcome::Server(error))) => Err(CallFailure {
                accepted: true,
                kind: CallFailureKind::Server(error),
            }),
            Ok(Ok(PendingOutcome::Failed(error))) => Err(CallFailure {
                accepted: true,
                kind: CallFailureKind::Client(error),
            }),
        }
    }

    fn next_wire_id(&self) -> String {
        loop {
            let number = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
            let id = format!("rust-{number}");
            if !lock_pending(&self.inner.pending).contains_key(&id) {
                return id;
            }
        }
    }
}

#[derive(Serialize)]
struct RpcRequest<'a> {
    jsonrpc: &'static str,
    id: &'a str,
    method: &'a str,
    params: Value,
}

fn validate_url(input: &str) -> Result<Url, ConnectError> {
    let url = Url::parse(input).map_err(|_| ConnectError::InvalidUrl("URL is not valid"))?;
    if !matches!(url.scheme(), "ws" | "wss") {
        return Err(ConnectError::InvalidUrl("scheme must be ws or wss"));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ConnectError::InvalidUrl(
            "credentials must not be placed in the URL",
        ));
    }
    if url.host_str().is_none() {
        return Err(ConnectError::InvalidUrl("URL must include a host"));
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(ConnectError::InvalidUrl(
            "query strings and fragments are not accepted",
        ));
    }
    Ok(url)
}

async fn authenticate(
    socket: &mut Socket,
    token: &str,
    config: &ClientConfig,
) -> Result<(), ConnectError> {
    let text = serde_json::to_string(&json!({
        "jsonrpc": "2.0",
        "id": "auth",
        "method": "auth",
        "params": { "token": token },
    }))
    .map_err(|error| ConnectError::AuthenticationProtocol(error.to_string()))?;
    if text.len() > config.max_message_bytes {
        return Err(ConnectError::AuthenticationProtocol(
            "authentication message exceeds max_message_bytes".into(),
        ));
    }
    let deadline = Instant::now() + config.auth_timeout;
    timeout_at(
        deadline,
        socket.send(Message::Text(text.into())),
        config.auth_timeout,
    )
    .await?
    .map_err(|error| ConnectError::AuthenticationProtocol(error.to_string()))?;
    loop {
        let message = timeout_at(deadline, socket.next(), config.auth_timeout)
            .await?
            .ok_or_else(|| {
                ConnectError::AuthenticationProtocol(
                    "connection closed before authentication response".into(),
                )
            })?
            .map_err(|error| ConnectError::AuthenticationProtocol(error.to_string()))?;
        match message {
            Message::Text(text) => return parse_auth_response(text.as_ref()),
            Message::Ping(bytes) => {
                timeout_at(
                    deadline,
                    socket.send(Message::Pong(bytes)),
                    config.auth_timeout,
                )
                .await?
                .map_err(|error| ConnectError::AuthenticationProtocol(error.to_string()))?;
            }
            Message::Close(_) => {
                return Err(ConnectError::AuthenticationProtocol(
                    "connection closed before authentication completed".into(),
                ));
            }
            _ => {
                return Err(ConnectError::AuthenticationProtocol(
                    "unexpected frame during authentication".into(),
                ));
            }
        }
    }
}

async fn timeout_at<F, T>(
    deadline: Instant,
    future: F,
    configured: Duration,
) -> Result<T, ConnectError>
where
    F: Future<Output = T>,
{
    tokio::time::timeout_at(deadline, future)
        .await
        .map_err(|_| ConnectError::Timeout(configured))
}

fn parse_auth_response(text: &str) -> Result<(), ConnectError> {
    let value: Value = serde_json::from_str(text)
        .map_err(|error| ConnectError::AuthenticationProtocol(error.to_string()))?;
    if value.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || value.get("id").and_then(Value::as_str) != Some("auth")
    {
        return Err(ConnectError::AuthenticationProtocol(
            "authentication response has an invalid envelope".into(),
        ));
    }
    if let Some(error) = value.get("error") {
        return Err(ConnectError::Authentication(
            parse_server_error(error).map_err(ConnectError::AuthenticationProtocol)?,
        ));
    }
    if value.pointer("/result/protocol").and_then(Value::as_u64) != Some(1) {
        return Err(ConnectError::AuthenticationProtocol(
            "server did not confirm protocol version 1".into(),
        ));
    }
    Ok(())
}

async fn connection_task<S>(
    socket: S,
    mut commands: mpsc::Receiver<Command>,
    pending: PendingMap,
    closed: Arc<AtomicBool>,
    io_timeout: Duration,
    close_timeout: Duration,
) where
    S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error>
        + Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
        + Unpin,
{
    let mut transport = ChargedSocket {
        socket,
        writing: None,
    };
    let _guard = ConnectionGuard {
        pending: Arc::clone(&pending),
        closed: Arc::clone(&closed),
    };
    let reason = loop {
        tokio::select! {
            command = commands.recv() => {
                match command {
                    Some(Command::Send { text, byte_permit }) => {
                        transport.writing = Some(byte_permit);
                        match timeout(io_timeout, transport.socket.send(Message::Text(text.into()))).await {
                            Ok(Ok(())) => {
                                // SinkExt::send includes flush; only now can writer credit go.
                                transport.writing = None;
                            }
                            Ok(Err(error)) => break format!("send failed: {error}"),
                            Err(_) => break "send deadline exceeded".to_owned(),
                        }
                    }
                    Some(Command::Close { acknowledged }) => {
                        let _ = timeout(close_timeout, transport.socket.close()).await;
                        let _ = acknowledged.send(());
                        break "client closed".to_owned();
                    }
                    None => {
                        let _ = timeout(close_timeout, transport.socket.close()).await;
                        break "all client handles were dropped".to_owned();
                    }
                }
            }
            incoming = transport.socket.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        if let Err(error) = dispatch_response(text.as_ref(), &pending) {
                            break error;
                        }
                    }
                    Some(Ok(Message::Ping(bytes))) => {
                        match timeout(io_timeout, transport.socket.send(Message::Pong(bytes))).await {
                            Ok(Ok(())) => {}
                            Ok(Err(error)) => break format!("send pong failed: {error}"),
                            Err(_) => break "pong deadline exceeded".to_owned(),
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) | None => {
                        break "server closed the connection".to_owned();
                    }
                    Some(Ok(_)) => {
                        break "server sent a non-text protocol frame".to_owned();
                    }
                    Some(Err(error)) => {
                        break format!("receive failed: {error}");
                    }
                }
            }
        }
    };
    closed.store(true, Ordering::Release);
    fail_pending(&pending, Error::Disconnected(reason));
}

fn dispatch_response(text: &str, pending: &PendingMap) -> Result<(), String> {
    let value: Value =
        serde_json::from_str(text).map_err(|error| format!("invalid JSON: {error}"))?;
    let object = value
        .as_object()
        .ok_or_else(|| "response must be a JSON object".to_owned())?;
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err("response has an invalid jsonrpc version".into());
    }
    let id = object
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| "response ID must be a string".to_owned())?;
    let result = object.get("result");
    let error = object.get("error");
    let outcome = match (result, error) {
        (Some(value), None) => PendingOutcome::Result(value.clone()),
        (None, Some(value)) => PendingOutcome::Server(parse_server_error(value)?),
        _ => return Err("response must contain exactly one of result or error".into()),
    };
    if let Some(call) = lock_pending(pending).remove(id) {
        let _ = call.response.send(outcome);
    }
    Ok(())
}

fn parse_server_error(value: &Value) -> Result<ServerError, String> {
    let code = value
        .get("code")
        .and_then(Value::as_i64)
        .ok_or_else(|| "server error code must be an integer".to_owned())?;
    let message = value
        .get("message")
        .and_then(Value::as_str)
        .ok_or_else(|| "server error message must be a string".to_owned())?;
    Ok(ServerError {
        code,
        message: message.to_owned(),
    })
}

fn fail_pending(pending: &PendingMap, error: Error) {
    let calls = {
        let mut pending = lock_pending(pending);
        pending.drain().map(|(_, call)| call).collect::<Vec<_>>()
    };
    for call in calls {
        let _ = call.response.send(PendingOutcome::Failed(error.clone()));
    }
}

fn lock_pending(pending: &PendingMap) -> MutexGuard<'_, HashMap<String, Pending>> {
    pending
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn not_accepted(error: Error) -> CallFailure {
    CallFailure {
        accepted: false,
        kind: CallFailureKind::Client(error),
    }
}

fn map_write_failure(request_id: RequestId, failure: CallFailure) -> WriteError {
    match failure.kind {
        CallFailureKind::Server(error) if error.code == ServerError::ADMISSION_REJECTED => {
            WriteError::AdmissionRejected(error)
        }
        CallFailureKind::Server(error) if error.code == ServerError::OPERATION_FAILURE => {
            WriteError::OutcomeUnknown {
                request_id,
                cause: AmbiguousWrite::ServerOperation(error),
            }
        }
        CallFailureKind::Server(error) => WriteError::Rejected(error),
        CallFailureKind::Client(error) if !failure.accepted => WriteError::NotSent(error),
        CallFailureKind::Client(Error::RequestTimeout(_)) => WriteError::OutcomeUnknown {
            request_id,
            cause: AmbiguousWrite::Timeout,
        },
        CallFailureKind::Client(Error::Disconnected(message)) => WriteError::OutcomeUnknown {
            request_id,
            cause: AmbiguousWrite::Disconnected(message),
        },
        CallFailureKind::Client(error) => WriteError::OutcomeUnknown {
            request_id,
            cause: AmbiguousWrite::Protocol(error.to_string()),
        },
    }
}
