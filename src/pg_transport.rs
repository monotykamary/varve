//! Opt-in, loopback-only PostgreSQL v3 simple-query transport with SCRAM.
//! SQL is DuckDB read-only SQL, not a PostgreSQL compatibility layer.
use std::fmt::Debug;

use async_trait::async_trait;
use futures_util::{Sink, stream};
use pgwire::api::auth::sasl::{
    SASLAuthStartupHandler,
    scram::{SCRAM_ITERATIONS, ScramAuth, gen_salted_password},
};
use pgwire::api::auth::{AuthSource, DefaultServerParameterProvider, LoginInfo, Password};
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{
    DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response as PgResponse,
};
use pgwire::api::{
    ClientInfo, ClientPortalStore, DefaultClient, NoopHandler, PgWireConnectionState, Type,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::server::{PgWireMessageServerCodec, process_error, process_message};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::codec::{Decoder, Framed};

use super::*;

pub(super) struct Settings {
    pub(super) address: SocketAddr,
    password: Password,
}

impl Settings {
    pub(super) fn resolve(port: Option<u16>, bind: IpAddr) -> Result<Option<Self>> {
        let port = match port {
            Some(port) => Some(port),
            None => env::var("VARVE_PG_PORT")
                .ok()
                .map(|p| p.parse::<u16>())
                .transpose()
                .context("invalid VARVE_PG_PORT")?,
        };
        let Some(port) = port else {
            return Ok(None);
        };
        ensure!(
            bind.is_loopback(),
            "PostgreSQL non-loopback binding requires TLS; TLS is not implemented"
        );
        let token = env::var("VARVE_API_TOKEN")
            .context("PostgreSQL requires VARVE_API_TOKEN even on loopback")?;
        ensure!(
            token.len() >= 32,
            "PostgreSQL requires a strong operator token"
        );
        let salt = uuid::Uuid::new_v4().as_bytes().to_vec();
        let salted = gen_salted_password(&token, &salt, SCRAM_ITERATIONS);
        Ok(Some(Self {
            address: SocketAddr::new(bind, port),
            password: Password::new(Some(salt), salted),
        }))
    }
}

struct Credentials(Password);
impl Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Credentials([REDACTED])")
    }
}
#[async_trait]
impl AuthSource for Credentials {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        if login.user() != Some("varve") {
            return Err(pg_error("28P01", "invalid credentials"));
        }
        Ok(self.0.clone())
    }
}

fn pg_error(code: &str, message: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        code.into(),
        message.into(),
    )))
}

struct Queries(Arc<State>);
#[async_trait]
impl SimpleQueryHandler for Queries {
    async fn do_query<C>(&self, _client: &mut C, sql: &str) -> PgWireResult<Vec<PgResponse>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // Database::query intercepts management CALLs before its lexical read-only
        // check. Validate structurally before invoking it; never interpolate SQL.
        if sql.len() > self.0.max_body_bytes {
            return Err(pg_error("54000", "query exceeds byte limit"));
        }
        let _slot = self
            .0
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| pg_error("53300", "request rejected before admission"))?;
        let work = async {
            let permit = self
                .0
                .workers
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| pg_error("57014", "service stopping"))?;
            let db = self.0.db.clone();
            let sql = sql.to_owned();
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                validate_read_only(&sql)?;
                db.query(&sql)
            })
            .await
            .map_err(|_| pg_error("XX000", "query worker failed"))?
            .map_err(|_| {
                pg_error(
                    "0A000",
                    "read-only DuckDB query failed or statement is unsupported",
                )
            })
        };
        let value = timeout(self.0.request_timeout, work)
            .await
            .map_err(|_| pg_error("57014", "query deadline exceeded"))??;
        encode_result(value)
    }
}

fn validate_read_only(sql: &str) -> Result<()> {
    use sqlparser::{ast::Statement, dialect::DuckDbDialect, parser::Parser};
    ensure!(sql.len() <= 64 * 1024, "SQL exceeds 64KiB limit");
    let statements = Parser::parse_sql(&DuckDbDialect {}, sql)?;
    ensure!(
        statements.len() == 1,
        "exactly one read-only query is required"
    );
    let read_only = match &statements[0] {
        Statement::Query(_) => true,
        Statement::Explain { statement, .. } => matches!(statement.as_ref(), Statement::Query(_)),
        _ => false,
    };
    ensure!(
        read_only,
        "only SELECT/WITH and EXPLAIN queries are supported"
    );
    Ok(())
}

fn encode_result(value: Value) -> PgWireResult<Vec<PgResponse>> {
    let Value::Array(rows) = value else {
        return Err(pg_error("0A000", "query result is not tabular"));
    };
    // The DuckDB JSON adapter has no schema for empty results. All nonempty fields
    // are explicitly TEXT (OID 25), preserving exact integers and nulls.
    let fields = Arc::new(
        rows.first()
            .and_then(Value::as_object)
            .map(|row| {
                row.keys()
                    .map(|name| {
                        FieldInfo::new(name.clone(), None, None, Type::TEXT, FieldFormat::Text)
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
    );
    if fields.len() > 1024 {
        return Err(pg_error("54000", "too many result columns"));
    }
    let schema = fields.clone();
    let data = stream::iter(rows.into_iter().map(move |row| {
        let mut encoder = DataRowEncoder::new(schema.clone());
        for field in schema.iter() {
            let value = match &row[field.name()] {
                Value::Null => None,
                Value::String(s) => Some(s.clone()),
                other => Some(other.to_string()),
            };
            encoder.encode_field(&value)?;
        }
        Ok(encoder.take_row())
    }));
    Ok(vec![PgResponse::Query(QueryResponse::new(fields, data))])
}

pub(super) async fn serve(
    listener: TcpListener,
    settings: Settings,
    state: Arc<State>,
    mut shutdown: watch::Receiver<bool>,
    max_connections: usize,
) -> Result<()> {
    let permits = Arc::new(Semaphore::new(max_connections));
    let credentials = Arc::new(Credentials(settings.password));
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => {},
            accepted = listener.accept() => {
                let (stream, _) = accepted.context("accept PostgreSQL connection")?;
                let Ok(permit) = permits.clone().try_acquire_owned() else { continue; };
                let state = state.clone();
                let credentials = credentials.clone();
                let mut shutdown = shutdown.clone();
                connections.spawn(async move {
                    let _permit = permit;
                    tokio::select! {
                        _ = shutdown.changed() => {},
                        _ = connection(stream, credentials, state) => {},
                    }
                });
            }
        }
    }
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    Ok(())
}

async fn connection(
    mut stream: tokio::net::TcpStream,
    credentials: Arc<Credentials>,
    state: Arc<State>,
) -> Result<()> {
    stream.set_nodelay(true)?;
    let address = stream.peer_addr()?;
    // Bound startup INCLUDING SSL negotiation and SCRAM, not each individual read.
    let deadline = tokio::time::Instant::now() + state.ws.auth_timeout;
    let mut first =
        tokio::time::timeout_at(deadline, packet(&mut stream, true, 10_000, None)).await??;
    if first.len() == 8 && first[4..8] == [4, 210, 22, 47] {
        tokio::time::timeout_at(deadline, stream.write_all(b"N")).await??;
        first =
            tokio::time::timeout_at(deadline, packet(&mut stream, true, 10_000, None)).await??;
    }
    let mut socket = Framed::new(
        stream,
        PgWireMessageServerCodec::<String>::new(DefaultClient::new(address, false)),
    );
    socket.set_state(PgWireConnectionState::AwaitingStartup);
    // SASL handler contains connection-specific state: never share it between sockets.
    let auth = Arc::new(
        SASLAuthStartupHandler::new(Arc::new(DefaultServerParameterProvider::default()))
            .with_scram(ScramAuth::new(credentials)),
    );
    let queries = Arc::new(Queries(state.clone()));
    let noop = Arc::new(NoopHandler);
    let mut bytes = first;
    loop {
        let startup = matches!(
            socket.state(),
            PgWireConnectionState::AwaitingStartup
                | PgWireConnectionState::AuthenticationInProgress
        );
        if !startup {
            let tag = bytes[0];
            if tag == b'X' {
                break;
            }
            if tag != b'Q' {
                // Reject before pgwire can allocate prepared statement/portal state.
                // Close after the explicit error; extended Sync recovery is not advertised.
                let _ = timeout(
                    state.body_timeout,
                    process_error(
                        &mut socket,
                        pg_error(
                            "0A000",
                            "only simple-query protocol is supported; no extended queries or COPY",
                        ),
                        false,
                    ),
                )
                .await;
                break;
            }
        }
        let message = socket
            .codec_mut()
            .decode(&mut bytes)?
            .context("incomplete PostgreSQL frame")?;
        if matches!(message, PgWireFrontendMessage::Terminate(_)) {
            break;
        }
        let process = process_message(
            message,
            &mut socket,
            auth.clone(),
            queries.clone(),
            noop.clone(),
            noop.clone(),
            noop.clone(),
        );
        let result = if startup {
            tokio::time::timeout_at(deadline, process).await?
        } else {
            timeout(state.request_timeout + state.body_timeout, process).await?
        };
        if let Err(error) = result {
            // Crate authentication errors contain no raw password. Avoid logging inputs.
            timeout(state.body_timeout, process_error(&mut socket, error, false)).await??;
            if startup {
                break;
            }
        }
        let authenticating = matches!(
            socket.state(),
            PgWireConnectionState::AwaitingStartup
                | PgWireConnectionState::AuthenticationInProgress
        );
        bytes = if authenticating {
            tokio::time::timeout_at(deadline, packet(socket.get_mut(), false, 10_000, None))
                .await??
        } else {
            // Authenticated idle sessions retain their bounded connection permit.
            // The packet-completion deadline starts only when its first byte arrives.
            let tag = socket.get_mut().read_u8().await?;
            timeout(
                state.body_timeout,
                packet(socket.get_mut(), false, state.max_body_bytes, Some(tag)),
            )
            .await??
        };
    }
    Ok(())
}

async fn packet(
    stream: &mut tokio::net::TcpStream,
    startup: bool,
    limit: usize,
    first: Option<u8>,
) -> Result<BytesMut> {
    // pgwire's general codec permits ~1 GiB frontend packets. Check the length
    // BEFORE allocating or decoding, then hand precisely one complete packet to it.
    let mut header = [0u8; 5];
    let prefix = if startup { 0 } else { 1 };
    let start = if let Some(first) = first {
        header[0] = first;
        1
    } else {
        0
    };
    stream.read_exact(&mut header[start..4 + prefix]).await?;
    let length =
        u32::from_be_bytes(header[prefix..prefix + 4].try_into().expect("four bytes")) as usize;
    ensure!(
        length >= 4 && length <= limit,
        "PostgreSQL packet exceeds configured bound"
    );
    let mut bytes = BytesMut::zeroed(length + prefix);
    bytes[..4 + prefix].copy_from_slice(&header[..4 + prefix]);
    stream.read_exact(&mut bytes[4 + prefix..]).await?;
    Ok(bytes)
}
