//! Bounded JSON-RPC 2.0 over WebSocket. No mutation is ever replayed here.
use std::collections::HashSet;

use futures_util::{SinkExt, StreamExt};
use hyper::header::{HeaderValue, SEC_WEBSOCKET_PROTOCOL};
use hyper::upgrade::OnUpgrade;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Message,
        handshake::server::create_response,
        protocol::{Role, WebSocketConfig},
    },
};

use super::*;

type Socket = WebSocketStream<TokioIo<hyper::upgrade::Upgraded>>;

pub(super) struct Settings {
    origins: Vec<String>,
    max_pending: usize,
    max_pending_bytes: usize,
    pub(super) auth_timeout: Duration,
    heartbeat: Duration,
}

impl Settings {
    pub(super) fn resolve(mut origins: Vec<String>) -> Result<Self> {
        if let Ok(value) = env::var("VARVE_WS_ORIGINS") {
            origins.extend(
                value
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned),
            );
        }
        for origin in &origins {
            let uri: hyper::Uri = origin.parse().context("invalid WS origin")?;
            ensure!(
                matches!(uri.scheme_str(), Some("http" | "https"))
                    && uri.authority().is_some()
                    && uri.path() == "/"
                    && uri.query().is_none()
                    && !origin.ends_with('/')
                    && !origin.contains('@'),
                "WS origins must be exact http(s) origins without paths or wildcards"
            );
            ensure!(
                !origin.contains('*'),
                "wildcard WS origins are not accepted"
            );
        }
        let max_pending = resolve_usize("VARVE_WS_MAX_PENDING", None, 32)?;
        let max_pending_bytes = resolve_usize("VARVE_WS_MAX_PENDING_BYTES", None, 8 * 1024 * 1024)?;
        let auth_ms = resolve_u64("VARVE_WS_AUTH_TIMEOUT_MS", None, 5_000)?;
        let heartbeat_ms = resolve_u64("VARVE_WS_HEARTBEAT_MS", None, 30_000)?;
        ensure!(
            max_pending > 0
                && max_pending <= 1024
                && max_pending_bytes > 0
                && max_pending_bytes <= 64 * 1024 * 1024,
            "invalid WS pending bounds"
        );
        ensure!(
            auth_ms > 0 && heartbeat_ms > 0,
            "WS deadlines must be positive"
        );
        Ok(Self {
            origins,
            max_pending,
            max_pending_bytes,
            auth_timeout: Duration::from_millis(auth_ms),
            heartbeat: Duration::from_millis(heartbeat_ms),
        })
    }
}

pub(super) fn upgrade(
    mut request: Request<Incoming>,
    state: &State,
    upgrades: &mpsc::Sender<OnUpgrade>,
) -> HttpResponse {
    let check = || -> Result<(), ApiError> {
        reject_ambiguous_headers(&request)?;
        validate_bodyless_request(&request)?;
        if request.uri().query().is_some() || request.headers().contains_key(AUTHORIZATION) {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "WebSocket credentials belong only in the first frame",
            ));
        }
        for name in [
            ORIGIN,
            SEC_WEBSOCKET_PROTOCOL,
            hyper::header::SEC_WEBSOCKET_KEY,
            hyper::header::SEC_WEBSOCKET_VERSION,
        ] {
            if request.headers().get_all(name).iter().count() > 1 {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "duplicate WebSocket header",
                ));
            }
        }
        if let Some(origin) = request.headers().get(ORIGIN) {
            if state.auth.token_hash.is_none() {
                return Err(ApiError::new(
                    StatusCode::FORBIDDEN,
                    "browser WebSockets require configured authentication",
                ));
            }
            if !origin
                .to_str()
                .ok()
                .is_some_and(|o| state.ws.origins.iter().any(|allowed| allowed == o))
            {
                return Err(ApiError::new(
                    StatusCode::FORBIDDEN,
                    "browser origin is not allowed",
                ));
            }
        }
        if request
            .headers()
            .get(SEC_WEBSOCKET_PROTOCOL)
            .and_then(|h| h.to_str().ok())
            != Some("varve.v1")
        {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "required subprotocol: varve.v1",
            ));
        }
        Ok(())
    };
    if let Err(error) = check() {
        return error_response(state, error);
    }
    // Tungstenite validates method, version, Upgrade, Connection and key headers.
    let upgrade = hyper::upgrade::on(&mut request);
    let handshake = request.map(|_| ());
    let response = create_response(&handshake);
    match response {
        Ok(response) => {
            if upgrades.try_send(upgrade).is_err() {
                return error_response(
                    state,
                    ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "upgrade unavailable"),
                );
            }
            let mut response = response.map(|_| Full::new(Bytes::new()));
            response
                .headers_mut()
                .insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static("varve.v1"));
            response
        }
        Err(_) => error_response(
            state,
            ApiError::new(StatusCode::BAD_REQUEST, "invalid WebSocket upgrade"),
        ),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Rpc {
    jsonrpc: String,
    id: String,
    method: String,
    params: Value,
}

fn parse(text: &str) -> std::result::Result<Rpc, Value> {
    let value: Value =
        serde_json::from_str(text).map_err(|_| error(Value::Null, -32700, "invalid JSON"))?;
    let rpc: Rpc =
        serde_json::from_value(value).map_err(|_| error(Value::Null, -32600, "invalid request"))?;
    if rpc.jsonrpc != "2.0" || rpc.id.is_empty() || rpc.id.len() > 128 || !rpc.id.is_ascii() {
        return Err(error(Value::Null, -32600, "invalid request"));
    }
    Ok(rpc)
}

fn error(id: Value, code: i32, message: &str) -> Value {
    json!({"jsonrpc":"2.0", "id":id, "error":{"code":code,"message":message}})
}
fn success(id: &str, result: Value) -> Value {
    json!({"jsonrpc":"2.0", "id":id, "result":result})
}

async fn send(socket: &mut Socket, value: Value, state: &State) -> Result<()> {
    let mut text = value.to_string();
    if text.len() > state.max_body_bytes {
        text = error(
            value["id"].clone(),
            -32000,
            "response exceeds byte limit; operation outcome may be committed",
        )
        .to_string();
    }
    // A very small configured cap may not even fit the sanitized error envelope.
    // Disconnect rather than violate the outbound bound. Accepted outcomes stay ambiguous.
    ensure!(
        text.len() <= state.max_body_bytes,
        "response error exceeds byte limit"
    );
    timeout(state.body_timeout, socket.send(Message::Text(text.into()))).await??;
    Ok(())
}

pub(super) async fn serve(
    upgrade: OnUpgrade,
    state: Arc<State>,
    mut shutdown: watch::Receiver<bool>,
) {
    let Ok(Ok(io)) = timeout(state.ws.auth_timeout, upgrade).await else {
        return;
    };
    let config = WebSocketConfig::default()
        .max_message_size(Some(state.max_body_bytes))
        .max_frame_size(Some(state.max_body_bytes))
        .write_buffer_size(0)
        .max_write_buffer_size(state.max_body_bytes.saturating_add(1024));
    let mut socket =
        WebSocketStream::from_raw_socket(TokioIo::new(io), Role::Server, Some(config)).await;
    let first = tokio::select! {
        _ = shutdown.changed() => return,
        frame = timeout(state.ws.auth_timeout, socket.next()) => frame,
    };
    let authenticated = if let Ok(Some(Ok(Message::Text(text)))) = first {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Credentials {
            token: String,
        }
        parse(&text)
            .ok()
            .filter(|rpc| rpc.id == "auth" && rpc.method == "auth")
            .and_then(|rpc| serde_json::from_value::<Credentials>(rpc.params).ok())
            .is_some_and(|credentials| match state.auth.token_hash {
                Some(hash) => {
                    hash.ct_eq(blake3::hash(credentials.token.as_bytes()).as_bytes())
                        .unwrap_u8()
                        == 1
                }
                None => credentials.token.is_empty(),
            })
    } else {
        false
    };
    if !authenticated {
        state
            .metrics
            .auth_failures_total
            .fetch_add(1, Ordering::Relaxed);
        let _ = send(
            &mut socket,
            error(json!("auth"), -32001, "unauthorized"),
            &state,
        )
        .await;
        let _ = timeout(state.body_timeout, socket.close(None)).await;
        return;
    }
    if send(&mut socket, success("auth", json!({"protocol":1})), &state)
        .await
        .is_err()
    {
        return;
    }
    let mut calls = JoinSet::new();
    let mut ids = HashSet::new();
    let mut pending_bytes = 0usize;
    let mut heartbeat = tokio::time::interval(state.ws.heartbeat);
    heartbeat.tick().await;
    let mut awaiting_pong = false;
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            _ = heartbeat.tick() => {
                if awaiting_pong { break; }
                awaiting_pong = true;
                if !matches!(timeout(state.body_timeout, socket.send(Message::Ping(Bytes::from_static(b"varve")))).await, Ok(Ok(()))) { break; }
            }
            Some(joined) = calls.join_next(), if !calls.is_empty() => {
                let Ok((id, bytes, response, _slot)) = joined else { break; };
                ids.remove(&id);
                pending_bytes -= bytes;
                if send(&mut socket, response, &state).await.is_err() { break; }
            }
            frame = socket.next() => {
                match frame {
                    Some(Ok(Message::Text(text))) => {
                        let rpc = match parse(&text) {
                            Ok(rpc) => rpc,
                            Err(response) => { if send(&mut socket, response, &state).await.is_err() { break; } continue; }
                        };
                        // Duplicate IDs cannot receive two correlatable responses: close after a null-ID error.
                        if ids.contains(&rpc.id) {
                            let _ = send(&mut socket, error(Value::Null, -32600, "duplicate outstanding ID"), &state).await;
                            break;
                        }
                        let bytes = text.len();
                        let slot = state.slots.clone().try_acquire_owned();
                        if ids.len() >= state.ws.max_pending || bytes > state.ws.max_pending_bytes.saturating_sub(pending_bytes) || slot.is_err() {
                            if send(&mut socket, error(json!(rpc.id), -32003, "request rejected before admission"), &state).await.is_err() { break; }
                            continue;
                        }
                        let slot = slot.expect("checked admission");
                        ids.insert(rpc.id.clone());
                        pending_bytes += bytes;
                        let state = state.clone();
                        calls.spawn(async move {
                            let id = rpc.id.clone();
                            let response = match timeout(state.request_timeout, dispatch(&state, rpc)).await {
                                Ok(Ok(result)) => success(&id, result),
                                Ok(Err((code, message))) => error(json!(id), code, message),
                                Err(_) => error(json!(id), -32000, "accepted request timed out; outcome may be committed"),
                            };
                            (id, bytes, response, slot)
                        });
                    }
                    Some(Ok(Message::Ping(_))) => {
                        if !matches!(timeout(state.body_timeout, socket.flush()).await, Ok(Ok(()))) { break; }
                    }
                    Some(Ok(Message::Pong(payload))) if payload.as_ref() == b"varve" => awaiting_pong = false,
                    Some(Ok(Message::Pong(_))) => {},
                    _ => break,
                }
            }
        }
    }
    // Aborting receipt waiters does not roll back accepted ingestion. The coordinator drains independently.
    calls.abort_all();
    while calls.join_next().await.is_some() {}
    let _ = timeout(state.body_timeout, socket.close(None)).await;
}

pub(super) fn ingest_metrics(stats: &varve::IngestStats) -> String {
    format!(
        "varve_ingest_submitted_total {}\nvarve_ingest_rejected_total {}\nvarve_ingest_completed_total {}\nvarve_ingest_pending_requests {}\nvarve_ingest_pending_bytes {}\nvarve_ingest_groups_total {}\nvarve_ingest_dropped_receivers_total {}\n",
        stats.submitted,
        stats.rejected,
        stats.completed,
        stats.pending_requests,
        stats.pending_bytes,
        stats.groups,
        stats.dropped_receivers,
    )
}

type RpcResult = std::result::Result<Value, (i32, &'static str)>;
fn params<T: for<'de> Deserialize<'de>>(
    value: Value,
) -> std::result::Result<T, (i32, &'static str)> {
    serde_json::from_value(value).map_err(|_| (-32602, "invalid parameters"))
}

async fn dispatch(state: &Arc<State>, rpc: Rpc) -> RpcResult {
    if rpc.method == "write" {
        let input: WriteInput = params(rpc.params)?;
        let now_us = system_now_us().map_err(|_| (-32000, "clock unavailable"))?;
        // submit() errors guarantee no admission; receipt errors never make that guarantee.
        let receipt = state
            .ingestor
            .submit(WriteRequest {
                table: input.table,
                request_id: input.request_id,
                rows: input.rows,
                now_us,
            })
            .map_err(|_| (-32003, "write rejected before admission"))?;
        let value = receipt
            .await
            .map_err(|_| (-32000, "receipt unavailable; outcome may be committed"))?
            .map_err(|_| (-32000, "operation failed; outcome may be committed"))?;
        return serde_json::to_value(value).map_err(|_| (-32000, "response encoding failed"));
    }
    enum Operation {
        Status,
        Create(CreateInput),
        Query(String),
        Checkpoint,
        Maintain,
    }
    let operation = match rpc.method.as_str() {
        "create" => Operation::Create(params(rpc.params)?),
        "query" => Operation::Query(params::<QueryInput>(rpc.params)?.sql),
        "ping" | "status" | "tables" | "policies" | "aggregates" | "jobs" | "checkpoint"
        | "maintain" => {
            let _: EmptyInput = params(rpc.params)?;
            match rpc.method.as_str() {
                "ping" => return Ok(json!({"pong":true})),
                "status" => Operation::Status,
                "tables" => Operation::Query("SELECT * FROM varve_tables()".into()),
                "policies" => Operation::Query("SELECT * FROM varve_policies()".into()),
                "aggregates" => {
                    Operation::Query("SELECT * FROM varve_continuous_aggregates()".into())
                }
                "jobs" => Operation::Query("SELECT * FROM varve_jobs()".into()),
                "checkpoint" => Operation::Checkpoint,
                _ => Operation::Maintain,
            }
        }
        _ => return Err((-32601, "unsupported method")),
    };
    let permit = state
        .workers
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| (-32000, "service shutting down"))?;
    let db = state.db.clone();
    tokio::task::spawn_blocking(move || -> Result<Value> {
        let _permit = permit;
        Ok(match operation {
            Operation::Status => serde_json::to_value(db.status()?)?,
            Operation::Create(input) => {
                serde_json::to_value(db.create_table(&input.name, input.config)?)?
            }
            Operation::Query(sql) => db.query(&sql)?,
            Operation::Checkpoint => {
                db.checkpoint()?;
                json!({"ok":true})
            }
            Operation::Maintain => serde_json::to_value(db.maintain(system_now_us()?)?)?,
        })
    })
    .await
    .map_err(|_| (-32000, "worker failed; outcome may be committed"))?
    .map_err(|_| (-32000, "operation failed; outcome may be committed"))
}
