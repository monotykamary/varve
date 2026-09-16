use std::convert::Infallible;
use std::env;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, ensure};
use bytes::{Bytes, BytesMut};
use http_body_util::{BodyExt, Full};
use hyper::body::{Body, Incoming};
use hyper::header::{
    AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, HOST, ORIGIN, TRANSFER_ENCODING, WWW_AUTHENTICATE,
};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use serde::Deserialize;
use serde_json::json;
use subtle::ConstantTimeEq;
use tokio::net::TcpListener;
use tokio::runtime::Builder;
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};
use varve::{Config, Database, IngestConfig, Ingestor, Row, TableConfig, WriteRequest};

#[path = "pg_transport.rs"]
mod pg_transport;
#[path = "transport.rs"]
mod transport;

use crate::system_now_us;

const HARD_MAX_BODY_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_MAX_CONNECTIONS: usize = 64;
const DEFAULT_HEADER_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_BODY_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 60_000;
const DEFAULT_CONNECTION_TIMEOUT_MS: u64 = 65_000;
const DEFAULT_SHUTDOWN_TIMEOUT_MS: u64 = 10_000;

type HttpBody = Full<Bytes>;
type HttpResponse = Response<HttpBody>;

pub struct Overrides {
    pub port: Option<u16>,
    pub pg_port: Option<u16>,
    pub pg_bind: IpAddr,
    pub ws_origins: Vec<String>,
    pub max_connections: Option<usize>,
    pub request_workers: Option<usize>,
    pub request_queue: Option<usize>,
    pub max_body_bytes: Option<usize>,
    pub header_timeout_ms: Option<u64>,
    pub body_timeout_ms: Option<u64>,
    pub request_timeout_ms: Option<u64>,
    pub connection_timeout_ms: Option<u64>,
    pub shutdown_timeout_ms: Option<u64>,
}

struct Settings {
    address: SocketAddr,
    max_connections: usize,
    request_workers: usize,
    request_queue: usize,
    max_body_bytes: usize,
    header_timeout: Duration,
    body_timeout: Duration,
    request_timeout: Duration,
    connection_timeout: Duration,
    shutdown_timeout: Duration,
    auth: Auth,
    ingest: IngestConfig,
    ws: transport::Settings,
    pg: Option<pg_transport::Settings>,
}

struct Auth {
    token_hash: Option<[u8; 32]>,
}

#[derive(Default)]
struct Metrics {
    started: Option<Instant>,
    requests_total: AtomicU64,
    errors_total: AtomicU64,
    auth_failures_total: AtomicU64,
    request_timeouts_total: AtomicU64,
    connection_timeouts_total: AtomicU64,
    rejected_connections_total: AtomicU64,
    body_bytes_total: AtomicU64,
    active_connections: AtomicUsize,
    inflight_requests: AtomicUsize,
}

struct State {
    db: Database,
    ingestor: Ingestor,
    ws: transport::Settings,
    auth: Auth,
    workers: Arc<Semaphore>,
    slots: Arc<Semaphore>,
    max_body_bytes: usize,
    body_timeout: Duration,
    request_timeout: Duration,
    metrics: Arc<Metrics>,
}

#[derive(Clone, Copy)]
enum Route {
    Health,
    Ready,
    Status,
    CreateTable,
    Write,
    Query,
    Maintain,
    Tables,
    Policies,
    Aggregates,
    Jobs,
    Metrics,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateInput {
    name: String,
    #[serde(default)]
    config: TableConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteInput {
    table: String,
    request_id: String,
    rows: Vec<Row>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryInput {
    sql: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EmptyInput {}

struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}

pub fn serve(
    db: Database,
    config: Config,
    bind: IpAddr,
    allow_remote: bool,
    overrides: Overrides,
) -> Result<()> {
    let settings = Settings::resolve(&config, bind, allow_remote, overrides)?;
    let shutdown_timeout = settings.shutdown_timeout;
    let async_workers = settings.request_workers.clamp(2, 8);
    let runtime = Builder::new_multi_thread()
        .worker_threads(async_workers)
        .enable_io()
        .enable_time()
        .build()
        .context("build HTTP runtime")?;
    let result = runtime.block_on(run_server(db, config, settings));
    runtime.shutdown_timeout(shutdown_timeout);
    result
}

impl Settings {
    fn resolve(
        config: &Config,
        bind: IpAddr,
        allow_remote: bool,
        overrides: Overrides,
    ) -> Result<Self> {
        ensure!(
            bind.is_loopback() || allow_remote,
            "serve refuses non-loopback bind addresses without --allow-remote"
        );
        let token_hash = match env::var("VARVE_API_TOKEN") {
            Ok(token) => {
                let bytes = token.as_bytes();
                ensure!(
                    bytes.len() >= 32,
                    "VARVE_API_TOKEN must be at least 32 bytes"
                );
                ensure!(
                    bytes.len() <= 4096 && bytes.iter().all(u8::is_ascii_graphic),
                    "VARVE_API_TOKEN must contain 32 to 4096 visible ASCII bytes"
                );
                Some(*blake3::hash(bytes).as_bytes())
            }
            Err(env::VarError::NotPresent) => None,
            Err(env::VarError::NotUnicode(_)) => {
                return Err(anyhow!("VARVE_API_TOKEN must be valid UTF-8"));
            }
        };
        ensure!(
            bind.is_loopback() || token_hash.is_some(),
            "non-loopback service requires VARVE_API_TOKEN with at least 32 bytes"
        );

        let port = resolve_required("PORT", overrides.port)?;
        let request_workers = resolve_usize(
            "VARVE_HTTP_REQUEST_WORKERS",
            overrides.request_workers,
            config.query_workers,
        )?;
        let request_queue = resolve_usize(
            "VARVE_HTTP_REQUEST_QUEUE",
            overrides.request_queue,
            request_workers.saturating_mul(2),
        )?;
        let max_connections = resolve_usize(
            "VARVE_HTTP_MAX_CONNECTIONS",
            overrides.max_connections,
            DEFAULT_MAX_CONNECTIONS,
        )?;
        let configured_body_limit = resolve_usize(
            "VARVE_HTTP_MAX_BODY_BYTES",
            overrides.max_body_bytes,
            HARD_MAX_BODY_BYTES,
        )?;
        let header_timeout_ms = resolve_u64(
            "VARVE_HTTP_HEADER_TIMEOUT_MS",
            overrides.header_timeout_ms,
            DEFAULT_HEADER_TIMEOUT_MS,
        )?;
        let body_timeout_ms = resolve_u64(
            "VARVE_HTTP_BODY_TIMEOUT_MS",
            overrides.body_timeout_ms,
            DEFAULT_BODY_TIMEOUT_MS,
        )?;
        let request_timeout_ms = resolve_u64(
            "VARVE_HTTP_REQUEST_TIMEOUT_MS",
            overrides.request_timeout_ms,
            DEFAULT_REQUEST_TIMEOUT_MS,
        )?;
        let connection_timeout_ms = resolve_u64(
            "VARVE_HTTP_CONNECTION_TIMEOUT_MS",
            overrides.connection_timeout_ms,
            DEFAULT_CONNECTION_TIMEOUT_MS,
        )?;
        let shutdown_timeout_ms = resolve_u64(
            "VARVE_HTTP_SHUTDOWN_TIMEOUT_MS",
            overrides.shutdown_timeout_ms,
            DEFAULT_SHUTDOWN_TIMEOUT_MS,
        )?;

        ensure!(request_workers > 0, "request workers must be positive");
        ensure!(request_queue > 0, "request queue must be positive");
        ensure!(max_connections > 0, "max connections must be positive");
        ensure!(
            max_connections >= request_workers,
            "max connections must be at least request workers"
        );
        ensure!(configured_body_limit > 0, "max body bytes must be positive");
        ensure!(header_timeout_ms > 0, "header timeout must be positive");
        ensure!(body_timeout_ms > 0, "body timeout must be positive");
        ensure!(
            request_timeout_ms >= body_timeout_ms,
            "request timeout must be at least body timeout"
        );
        ensure!(
            connection_timeout_ms >= request_timeout_ms,
            "connection timeout must be at least request timeout"
        );
        ensure!(shutdown_timeout_ms > 0, "shutdown timeout must be positive");

        Ok(Self {
            address: SocketAddr::new(bind, port),
            max_connections,
            request_workers,
            request_queue,
            max_body_bytes: configured_body_limit
                .min(config.max_batch_bytes)
                .min(HARD_MAX_BODY_BYTES),
            header_timeout: Duration::from_millis(header_timeout_ms),
            body_timeout: Duration::from_millis(body_timeout_ms),
            request_timeout: Duration::from_millis(request_timeout_ms),
            connection_timeout: Duration::from_millis(connection_timeout_ms),
            shutdown_timeout: Duration::from_millis(shutdown_timeout_ms),
            ingest: {
                let defaults = IngestConfig::default();
                IngestConfig {
                    queue_capacity: resolve_usize(
                        "VARVE_INGEST_QUEUE_CAPACITY",
                        None,
                        defaults.queue_capacity,
                    )?,
                    max_pending_bytes: resolve_usize(
                        "VARVE_INGEST_MAX_PENDING_BYTES",
                        None,
                        defaults.max_pending_bytes,
                    )?,
                    max_group_requests: resolve_usize(
                        "VARVE_INGEST_MAX_GROUP_REQUESTS",
                        None,
                        defaults.max_group_requests,
                    )?,
                    max_group_rows: resolve_usize(
                        "VARVE_INGEST_MAX_GROUP_ROWS",
                        None,
                        defaults.max_group_rows,
                    )?,
                    max_group_bytes: resolve_usize(
                        "VARVE_INGEST_MAX_GROUP_BYTES",
                        None,
                        defaults.max_group_bytes,
                    )?,
                    max_delay: Duration::from_millis(resolve_u64(
                        "VARVE_INGEST_MAX_DELAY_MS",
                        None,
                        2,
                    )?),
                }
            },
            ws: transport::Settings::resolve(overrides.ws_origins)?,
            pg: pg_transport::Settings::resolve(overrides.pg_port, overrides.pg_bind)?,
            auth: Auth { token_hash },
        })
    }
}

fn resolve_required(name: &str, cli: Option<u16>) -> Result<u16> {
    match cli {
        Some(value) => Ok(value),
        None => env::var(name)
            .with_context(|| format!("serve requires --port or {name}"))?
            .parse()
            .with_context(|| format!("parse {name}")),
    }
}

fn resolve_usize(name: &str, cli: Option<usize>, default: usize) -> Result<usize> {
    match cli {
        Some(value) => Ok(value),
        None => match env::var(name) {
            Ok(value) => value.parse().with_context(|| format!("parse {name}")),
            Err(env::VarError::NotPresent) => Ok(default),
            Err(error) => Err(error).with_context(|| format!("read {name}")),
        },
    }
}

fn resolve_u64(name: &str, cli: Option<u64>, default: u64) -> Result<u64> {
    match cli {
        Some(value) => Ok(value),
        None => match env::var(name) {
            Ok(value) => value.parse().with_context(|| format!("parse {name}")),
            Err(env::VarError::NotPresent) => Ok(default),
            Err(error) => Err(error).with_context(|| format!("read {name}")),
        },
    }
}

async fn run_server(db: Database, config: Config, settings: Settings) -> Result<()> {
    let listener = TcpListener::bind(settings.address)
        .await
        .with_context(|| format!("bind HTTP service at {}", settings.address))?;
    let connections = Arc::new(Semaphore::new(settings.max_connections));
    let metrics = Arc::new(Metrics {
        started: Some(Instant::now()),
        ..Metrics::default()
    });
    let ingestor = Ingestor::new(db.clone(), settings.ingest)?;
    let state = Arc::new(State {
        db: db.clone(),
        ingestor: ingestor.clone(),
        ws: settings.ws,
        auth: settings.auth,
        workers: Arc::new(Semaphore::new(settings.request_workers)),
        slots: Arc::new(Semaphore::new(
            settings
                .request_workers
                .saturating_add(settings.request_queue),
        )),
        max_body_bytes: settings.max_body_bytes,
        body_timeout: settings.body_timeout,
        request_timeout: settings.request_timeout,
        metrics: Arc::clone(&metrics),
    });
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut tasks = JoinSet::new();
    if let Some(pg) = settings.pg {
        let listener = TcpListener::bind(pg.address)
            .await
            .context("bind PostgreSQL service")?;
        tasks.spawn(pg_transport::serve(
            listener,
            pg,
            state.clone(),
            shutdown_rx.clone(),
            settings.max_connections,
        ));
    }
    tasks.spawn(run_scheduler(
        db,
        Duration::from_millis(config.maintenance_interval_ms),
        shutdown_rx.clone(),
    ));
    let signal = shutdown_signal();
    tokio::pin!(signal);

    loop {
        tokio::select! {
            signal = &mut signal => {
                signal?;
                break;
            }
            Some(joined) = tasks.join_next(), if !tasks.is_empty() => {
                // Malformed/disconnected clients must not take down the listener.
                if joined.is_err() {
                    metrics.errors_total.fetch_add(1, Ordering::Relaxed);
                }
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted.context("accept HTTP connection")?;
                let permit = match Arc::clone(&connections).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        metrics.rejected_connections_total.fetch_add(1, Ordering::Relaxed);
                        drop(stream);
                        continue;
                    }
                };
                let _ = stream.set_nodelay(true);
                let state = Arc::clone(&state);
                let shutdown = shutdown_rx.clone();
                let header_timeout = settings.header_timeout;
                let connection_timeout = settings.connection_timeout;
                metrics.active_connections.fetch_add(1, Ordering::Relaxed);
                tasks.spawn(async move {
                    let _permit = permit;
                    let _active = ActiveConnection(Arc::clone(&state.metrics));
                    serve_connection(
                        stream,
                        state,
                        shutdown,
                        header_timeout,
                        connection_timeout,
                    )
                    .await
                });
            }
        }
    }

    drop(listener);
    let _ = shutdown_tx.send(true);
    let drain = async {
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(Ok(())) => {}
                Ok(Err(_)) => eprintln!("service connection ended with an error"),
                Err(error) if error.is_cancelled() => {}
                Err(error) => eprintln!("service task panicked: {error}"),
            }
        }
    };
    if timeout(settings.shutdown_timeout, drain).await.is_err() {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
    tokio::task::spawn_blocking(move || ingestor.shutdown())
        .await
        .context("join ingestion shutdown")??;
    Ok(())
}

struct ActiveConnection(Arc<Metrics>);

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        self.0.active_connections.fetch_sub(1, Ordering::Relaxed);
    }
}

async fn serve_connection(
    stream: tokio::net::TcpStream,
    state: Arc<State>,
    mut shutdown: watch::Receiver<bool>,
    header_timeout: Duration,
    connection_timeout: Duration,
) -> Result<()> {
    let metrics = Arc::clone(&state.metrics);
    let (upgrade_tx, mut upgrade_rx) = tokio::sync::mpsc::channel(1);
    let request_state = state.clone();
    let service = service_fn(move |request| {
        let state = Arc::clone(&request_state);
        let upgrade_tx = upgrade_tx.clone();
        async move {
            let response = if request.uri().path() == "/v1/ws" {
                transport::upgrade(request, &state, &upgrade_tx)
            } else {
                handle_request(request, state).await
            };
            Ok::<_, Infallible>(response)
        }
    });
    let mut builder = http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .header_read_timeout(header_timeout)
        .keep_alive(true)
        .max_headers(64);
    let connection = builder
        .serve_connection(TokioIo::new(stream), service)
        .with_upgrades();
    tokio::pin!(connection);
    tokio::select! {
        result = &mut connection => {
            result.context("serve HTTP connection")?;
        },
        _ = sleep(connection_timeout) => {
            metrics.connection_timeouts_total.fetch_add(1, Ordering::Relaxed);
            connection.as_mut().graceful_shutdown();
            let drain_timeout = state.request_timeout.saturating_add(header_timeout);
            if let Ok(result) = timeout(drain_timeout, &mut connection).await {
                result.context("drain retired HTTP connection")?;
            }
        },
        changed = shutdown.changed() => {
            let _ = changed;
            connection.as_mut().graceful_shutdown();
            connection.await.context("drain HTTP connection")?;
        }
    }
    if !*shutdown.borrow()
        && let Ok(upgrade) = upgrade_rx.try_recv()
    {
        transport::serve(upgrade, state, shutdown).await;
    }
    Ok(())
}

fn readiness_response(db: &Database) -> HttpResponse {
    let ready = db.is_ready();
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    json_response(status, &json!({"ready": ready}))
}

async fn handle_request(request: Request<Incoming>, state: Arc<State>) -> HttpResponse {
    state.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
    let route = match preflight(&request, &state) {
        Ok(route) => route,
        Err(error) => return error_response(&state, error),
    };

    if matches!(route, Route::Health) {
        return json_response(StatusCode::OK, &json!({"ok": true}));
    }
    if matches!(route, Route::Ready) {
        return readiness_response(&state.db);
    }

    let _slot = match Arc::clone(&state.slots).try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return error_response(
                &state,
                ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "request queue is full"),
            );
        }
    };
    state
        .metrics
        .inflight_requests
        .fetch_add(1, Ordering::Relaxed);
    let _inflight = Inflight(Arc::clone(&state.metrics));
    match timeout(
        state.request_timeout,
        process_request(request, route, Arc::clone(&state)),
    )
    .await
    {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => error_response(&state, error),
        Err(_) => {
            state
                .metrics
                .request_timeouts_total
                .fetch_add(1, Ordering::Relaxed);
            error_response(
                &state,
                ApiError::new(StatusCode::GATEWAY_TIMEOUT, "request deadline exceeded"),
            )
        }
    }
}

struct Inflight(Arc<Metrics>);

impl Drop for Inflight {
    fn drop(&mut self) {
        self.0.inflight_requests.fetch_sub(1, Ordering::Relaxed);
    }
}

fn preflight(request: &Request<Incoming>, state: &State) -> Result<Route, ApiError> {
    reject_ambiguous_headers(request)?;
    let method = request.method();
    let path = request.uri().path();
    let public_probe = method == Method::GET && matches!(path, "/health" | "/ready");

    if request.headers().contains_key(ORIGIN) {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "browser-origin requests are not accepted",
        ));
    }
    if !public_probe {
        authenticate(request, state)?;
    }
    if method == Method::POST {
        validate_content_type(request)?;
    } else {
        validate_bodyless_request(request)?;
    }

    match (method, path) {
        (&Method::GET, "/health") => Ok(Route::Health),
        (&Method::GET, "/ready") => Ok(Route::Ready),
        (&Method::GET, "/v1/status") => Ok(Route::Status),
        (&Method::GET, "/v1/tables") => Ok(Route::Tables),
        (&Method::GET, "/v1/policies") => Ok(Route::Policies),
        (&Method::GET, "/v1/aggregates") => Ok(Route::Aggregates),
        (&Method::GET, "/v1/jobs") => Ok(Route::Jobs),
        (&Method::GET, "/metrics") => Ok(Route::Metrics),
        (&Method::POST, "/v1/tables") => Ok(Route::CreateTable),
        (&Method::POST, "/v1/write") => Ok(Route::Write),
        (&Method::POST, "/v1/query") => Ok(Route::Query),
        (&Method::POST, "/v1/maintain") => Ok(Route::Maintain),
        (
            _,
            "/health" | "/ready" | "/v1/status" | "/v1/tables" | "/v1/policies" | "/v1/aggregates"
            | "/v1/jobs" | "/metrics" | "/v1/write" | "/v1/query" | "/v1/maintain",
        ) => Err(ApiError::new(
            StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed",
        )),
        _ => Err(ApiError::new(StatusCode::NOT_FOUND, "route not found")),
    }
}

fn reject_ambiguous_headers(request: &Request<Incoming>) -> Result<(), ApiError> {
    for name in [
        AUTHORIZATION,
        CONTENT_TYPE,
        CONTENT_LENGTH,
        TRANSFER_ENCODING,
        HOST,
    ] {
        if request.headers().get_all(name).iter().count() > 1 {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "ambiguous duplicate request header",
            ));
        }
    }
    if request.headers().contains_key(CONTENT_LENGTH)
        && request.headers().contains_key(TRANSFER_ENCODING)
    {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "content-length and transfer-encoding cannot be combined",
        ));
    }
    Ok(())
}

fn authenticate(request: &Request<Incoming>, state: &State) -> Result<(), ApiError> {
    let Some(expected) = state.auth.token_hash else {
        return Ok(());
    };
    let header = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    let token = header.and_then(|value| {
        let (scheme, token) = value.split_once(' ')?;
        (scheme.eq_ignore_ascii_case("Bearer") && !token.is_empty() && !token.contains(' '))
            .then_some(token)
    });
    let candidate = blake3::hash(token.unwrap_or_default().as_bytes());
    let equal = expected.ct_eq(candidate.as_bytes()).unwrap_u8() == 1;
    if token.is_none() || !equal {
        state
            .metrics
            .auth_failures_total
            .fetch_add(1, Ordering::Relaxed);
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "missing or invalid bearer token",
        ));
    }
    Ok(())
}

fn validate_bodyless_request(request: &Request<Incoming>) -> Result<(), ApiError> {
    let nonempty_content_length = request
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|length| length > 0);
    if nonempty_content_length || request.headers().contains_key(TRANSFER_ENCODING) {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "request body is not accepted for this method",
        ));
    }
    Ok(())
}

fn validate_content_type(request: &Request<Incoming>) -> Result<(), ApiError> {
    let valid = request
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("application/json"));
    if valid {
        Ok(())
    } else {
        Err(ApiError::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "POST requires Content-Type: application/json",
        ))
    }
}

async fn process_request(
    request: Request<Incoming>,
    route: Route,
    state: Arc<State>,
) -> Result<HttpResponse, ApiError> {
    match route {
        Route::Ready => Ok(readiness_response(&state.db)),
        Route::Status => {
            run_blocking(&state, |db| {
                let value = db.status().map_err(database_error)?;
                Ok(json_response(StatusCode::OK, &value))
            })
            .await
        }
        Route::Tables => query_route(&state, "SELECT * FROM varve_tables()").await,
        Route::Policies => query_route(&state, "SELECT * FROM varve_policies()").await,
        Route::Aggregates => {
            query_route(&state, "SELECT * FROM varve_continuous_aggregates()").await
        }
        Route::Jobs => query_route(&state, "SELECT * FROM varve_jobs()").await,
        Route::Metrics => {
            run_blocking(&state, {
                let metrics = Arc::clone(&state.metrics);
                let ingestor = state.ingestor.clone();
                move |db| {
                    let status = db.status().map_err(database_error)?;
                    Ok(text_response(
                        StatusCode::OK,
                        format!(
                            "{}{}{}",
                            render_metrics(&metrics, &status, &db.query_worker_stats()),
                            transport::ingest_metrics(&ingestor.stats()),
                            db.performance().prometheus()
                        ),
                    ))
                }
            })
            .await
        }
        Route::CreateTable => {
            let input: CreateInput = read_json_body(request, &state).await?;
            run_blocking(&state, move |db| {
                let value = db
                    .create_table(&input.name, input.config)
                    .map_err(database_error)?;
                Ok(json_response(StatusCode::OK, &value))
            })
            .await
        }
        Route::Write => {
            let input: WriteInput = read_json_body(request, &state).await?;
            let value = ingest(&state, input).await.map_err(database_error)?;
            Ok(json_response(StatusCode::OK, &value))
        }
        Route::Query => {
            let input: QueryInput = read_json_body(request, &state).await?;
            run_blocking(&state, move |db| {
                let value = db.query(&input.sql).map_err(database_error)?;
                Ok(json_response(StatusCode::OK, &value))
            })
            .await
        }
        Route::Maintain => {
            let _: EmptyInput = read_json_body(request, &state).await?;
            run_blocking(&state, move |db| {
                let now = system_now_us().map_err(database_error)?;
                let value = db.maintain(now).map_err(database_error)?;
                Ok(json_response(StatusCode::OK, &value))
            })
            .await
        }
        Route::Health => unreachable!("liveness returns before request processing"),
    }
}

async fn ingest(state: &State, input: WriteInput) -> Result<varve::WriteReceipt> {
    let receipt = state.ingestor.submit(WriteRequest {
        table: input.table,
        request_id: input.request_id,
        rows: input.rows,
        now_us: system_now_us()?,
    })?;
    receipt
        .await
        .context("ingestion receipt unavailable; outcome may have committed")?
}

async fn query_route(state: &Arc<State>, sql: &'static str) -> Result<HttpResponse, ApiError> {
    run_blocking(state, move |db| {
        let value = db.query(sql).map_err(database_error)?;
        Ok(json_response(StatusCode::OK, &value))
    })
    .await
}

async fn read_json_body<T: for<'de> Deserialize<'de>>(
    request: Request<Incoming>,
    state: &State,
) -> Result<T, ApiError> {
    if let Some(length) = request.headers().get(CONTENT_LENGTH) {
        let length = length
            .to_str()
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| ApiError::new(StatusCode::BAD_REQUEST, "invalid content-length"))?;
        if length > state.max_body_bytes as u64 {
            return Err(ApiError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "request body exceeds configured limit",
            ));
        }
    }
    let bytes = timeout(
        state.body_timeout,
        collect_body(request.into_body(), state.max_body_bytes),
    )
    .await
    .map_err(|_| {
        ApiError::new(
            StatusCode::REQUEST_TIMEOUT,
            "request body deadline exceeded",
        )
    })??;
    state
        .metrics
        .body_bytes_total
        .fetch_add(bytes.len() as u64, Ordering::Relaxed);
    serde_json::from_slice(&bytes).map_err(|error| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            format!("parse request JSON: {error}"),
        )
    })
}

async fn collect_body(mut body: Incoming, limit: usize) -> Result<Bytes, ApiError> {
    let mut bytes = BytesMut::with_capacity(body.size_hint().lower().min(limit as u64) as usize);
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|error| {
            ApiError::new(
                StatusCode::BAD_REQUEST,
                format!("read request body: {error}"),
            )
        })?;
        if let Some(data) = frame.data_ref() {
            if bytes.len().saturating_add(data.len()) > limit {
                return Err(ApiError::new(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "request body exceeds configured limit",
                ));
            }
            bytes.extend_from_slice(data);
        } else if frame.is_trailers() {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "request trailers are not accepted",
            ));
        }
    }
    Ok(bytes.freeze())
}

async fn run_blocking<F>(state: &Arc<State>, operation: F) -> Result<HttpResponse, ApiError>
where
    F: FnOnce(Database) -> Result<HttpResponse, ApiError> + Send + 'static,
{
    let permit = Arc::clone(&state.workers)
        .acquire_owned()
        .await
        .map_err(|_| ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "service is shutting down"))?;
    let db = state.db.clone();
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        operation(db)
    })
    .await
    .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "request worker failed"))?
}

fn database_error(error: anyhow::Error) -> ApiError {
    let message = format!("{error:#}");
    let status = if message.contains("query concurrency limit") || message.contains("fenced") {
        StatusCode::SERVICE_UNAVAILABLE
    } else if message.contains("request_id conflicts") {
        StatusCode::CONFLICT
    } else if message.contains("timed out") || message.contains("timeout") {
        StatusCode::GATEWAY_TIMEOUT
    } else {
        StatusCode::BAD_REQUEST
    };
    ApiError::new(status, message)
}

async fn run_scheduler(
    db: Database,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                let _ = changed;
                return Ok(());
            }
            _ = sleep(interval) => {
                let db = db.clone();
                let result = tokio::task::spawn_blocking(move || {
                    let now = system_now_us()?;
                    db.tick(now)
                }).await;
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => eprintln!("background scheduler failed: {error:#}"),
                    Err(error) => eprintln!("background scheduler worker failed: {error}"),
                }
            }
        }
    }
}

#[cfg(unix)]
async fn shutdown_signal() -> Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result.context("install SIGINT handler")?,
        _ = terminate.recv() => {}
    }
    Ok(())
}

#[cfg(not(unix))]
async fn shutdown_signal() -> Result<()> {
    tokio::signal::ctrl_c()
        .await
        .context("install interrupt handler")
}

fn render_metrics(
    metrics: &Metrics,
    status: &varve::Status,
    workers: &varve::query::QueryWorkerStats,
) -> String {
    let uptime = metrics
        .started
        .map(|started| started.elapsed().as_secs_f64())
        .unwrap_or_default();
    format!(
        concat!(
            "# TYPE varve_uptime_seconds gauge\nvarve_uptime_seconds {uptime}\n",
            "# TYPE varve_http_requests_total counter\nvarve_http_requests_total {requests}\n",
            "# TYPE varve_http_errors_total counter\nvarve_http_errors_total {errors}\n",
            "# TYPE varve_http_auth_failures_total counter\nvarve_http_auth_failures_total {auth_failures}\n",
            "# TYPE varve_http_request_timeouts_total counter\nvarve_http_request_timeouts_total {request_timeouts}\n",
            "# TYPE varve_http_connection_timeouts_total counter\nvarve_http_connection_timeouts_total {connection_timeouts}\n",
            "# TYPE varve_http_rejected_connections_total counter\nvarve_http_rejected_connections_total {rejected_connections}\n",
            "# TYPE varve_http_body_bytes_total counter\nvarve_http_body_bytes_total {body_bytes}\n",
            "# TYPE varve_http_active_connections gauge\nvarve_http_active_connections {active_connections}\n",
            "# TYPE varve_http_inflight_requests gauge\nvarve_http_inflight_requests {inflight_requests}\n",
            "# TYPE varve_sequence gauge\nvarve_sequence {sequence}\n",
            "# TYPE varve_checkpoint_sequence gauge\nvarve_checkpoint_sequence {checkpoint_sequence}\n",
            "# TYPE varve_remote_sequence gauge\nvarve_remote_sequence {remote_sequence}\n",
            "# TYPE varve_hot_rows gauge\nvarve_hot_rows {hot_rows}\n",
            "# TYPE varve_wal_bytes gauge\nvarve_wal_bytes {wal_bytes}\n",
            "# TYPE varve_disk_bytes gauge\nvarve_disk_bytes {disk_bytes}\n",
            "# TYPE varve_tables gauge\nvarve_tables {tables}\n",
            "# TYPE varve_segments gauge\nvarve_segments {segments}\n",
            "# TYPE varve_active_queries gauge\nvarve_active_queries {active_queries}\n",
            "# TYPE varve_fenced gauge\nvarve_fenced {fenced}\n",
            "# TYPE varve_unshipped_batches gauge\nvarve_unshipped_batches {unshipped_batches}\n",
            "# TYPE varve_hot_bytes gauge\nvarve_hot_bytes {hot_bytes}\n",
            "# TYPE varve_metadata_bytes gauge\nvarve_metadata_bytes {metadata_bytes}\n",
            "# TYPE varve_control_root_bytes gauge\nvarve_control_root_bytes {control_root_bytes}\n",
            "# TYPE varve_derived_encoded_bytes gauge\nvarve_derived_encoded_bytes {derived_encoded_bytes}\n",
            "# TYPE varve_derived_resident_bytes gauge\nvarve_derived_resident_bytes {derived_resident_bytes}\n",
            "# TYPE varve_derived_working_bytes gauge\nvarve_derived_working_bytes {derived_working_bytes}\n",
            "# TYPE varve_query_workers_active gauge\nvarve_query_workers_active {workers_active}\n",
            "# TYPE varve_query_workers_idle gauge\nvarve_query_workers_idle {workers_idle}\n",
            "# TYPE varve_query_workers_spawned_total counter\nvarve_query_workers_spawned_total {workers_spawned}\n",
            "# TYPE varve_query_workers_reused_total counter\nvarve_query_workers_reused_total {workers_reused}\n",
            "# TYPE varve_query_workers_resets_total counter\nvarve_query_workers_resets_total {workers_resets}\n",
            "# TYPE varve_query_workers_discarded_total counter\nvarve_query_workers_discarded_total {workers_discarded}\n",
            "# TYPE varve_idempotency_keys gauge\nvarve_idempotency_keys {idempotency_keys}\n",
            "# TYPE varve_rollup_groups gauge\nvarve_rollup_groups {rollup_groups}\n",
            "# TYPE varve_decoded_cache_bytes gauge\nvarve_decoded_cache_bytes {decoded_cache_bytes}\n",
            "# TYPE varve_disk_cache_bytes gauge\nvarve_disk_cache_bytes {disk_cache_bytes}\n",
            "# TYPE varve_active_snapshots gauge\nvarve_active_snapshots {active_snapshots}\n",
            "# TYPE varve_maintenance_failed gauge\nvarve_maintenance_failed {maintenance_failed}\n"
        ),
        uptime = uptime,
        requests = metrics.requests_total.load(Ordering::Relaxed),
        errors = metrics.errors_total.load(Ordering::Relaxed),
        auth_failures = metrics.auth_failures_total.load(Ordering::Relaxed),
        request_timeouts = metrics.request_timeouts_total.load(Ordering::Relaxed),
        connection_timeouts = metrics.connection_timeouts_total.load(Ordering::Relaxed),
        rejected_connections = metrics.rejected_connections_total.load(Ordering::Relaxed),
        body_bytes = metrics.body_bytes_total.load(Ordering::Relaxed),
        active_connections = metrics.active_connections.load(Ordering::Relaxed),
        inflight_requests = metrics.inflight_requests.load(Ordering::Relaxed),
        sequence = status.sequence,
        checkpoint_sequence = status.checkpoint_sequence,
        remote_sequence = status.remote_sequence,
        hot_rows = status.hot_rows,
        wal_bytes = status.wal_bytes,
        disk_bytes = status.disk_bytes,
        tables = status.tables,
        segments = status.segments,
        active_queries = status.active_queries,
        fenced = usize::from(status.fenced.is_some()),
        unshipped_batches = status.unshipped_batches,
        hot_bytes = status.hot_bytes,
        metadata_bytes = status.metadata_bytes,
        control_root_bytes = status.control_root_bytes,
        derived_encoded_bytes = status.derived_encoded_bytes,
        derived_resident_bytes = status.derived_resident_bytes,
        derived_working_bytes = status.derived_working_bytes,
        workers_active = workers.active,
        workers_idle = workers.idle,
        workers_spawned = workers.spawned,
        workers_reused = workers.reused,
        workers_resets = workers.resets,
        workers_discarded = workers.discarded,
        idempotency_keys = status.idempotency_keys,
        rollup_groups = status.rollup_groups,
        decoded_cache_bytes = status.decoded_cache_bytes,
        disk_cache_bytes = status.disk_cache_bytes,
        active_snapshots = status.active_snapshots,
        maintenance_failed = usize::from(status.last_maintenance_error.is_some()),
    )
}

fn json_response(status: StatusCode, value: &impl serde::Serialize) -> HttpResponse {
    match serde_json::to_vec(value) {
        Ok(bytes) => response(status, "application/json", Bytes::from(bytes)),
        Err(_) => response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "application/json",
            Bytes::from_static(b"{\"error\":\"serialize response failed\"}"),
        ),
    }
}

fn text_response(status: StatusCode, body: String) -> HttpResponse {
    response(
        status,
        "text/plain; version=0.0.4; charset=utf-8",
        Bytes::from(body),
    )
}

fn response(status: StatusCode, content_type: &'static str, body: Bytes) -> HttpResponse {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, content_type)
        .header("Cache-Control", "no-store")
        .header("X-Content-Type-Options", "nosniff")
        .body(Full::new(body))
        .expect("static response headers are valid")
}

fn error_response(state: &State, error: ApiError) -> HttpResponse {
    state.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
    let mut response = json_response(error.status, &json!({"error": error.message}));
    if error.status == StatusCode::UNAUTHORIZED {
        response.headers_mut().insert(
            WWW_AUTHENTICATE,
            hyper::header::HeaderValue::from_static("Bearer"),
        );
    }
    response
}
