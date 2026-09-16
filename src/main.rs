use std::fs;
use std::io::{self, Read};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, ensure};
use clap::{Parser, Subcommand};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use varve::remote::{FileStore, RemoteStore, S3Store};
use varve::{Config, Database, Row, TableConfig};

mod service;

#[derive(Debug, Parser)]
#[command(name = "varve", version, about = "Local-first time-series storage")]
struct Cli {
    #[arg(long, value_name = "PATH")]
    data: PathBuf,
    #[arg(long, value_name = "JSON")]
    config: Option<PathBuf>,
    #[arg(long, value_name = "PATH", conflicts_with = "s3")]
    remote_dir: Option<PathBuf>,
    #[arg(long, conflicts_with = "remote_dir")]
    s3: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Initialize a database, or verify that an existing database opens.
    Init,
    /// Create a table with an immutable configuration.
    Create {
        name: String,
        #[arg(long, value_name = "JSON")]
        table_config: Option<PathBuf>,
    },
    /// Append one JSON batch from a file or stdin (`-`).
    Write {
        #[arg(value_name = "JSON")]
        input: PathBuf,
        #[arg(long, allow_hyphen_values = true)]
        now_us: Option<i64>,
    },
    /// Execute one trusted, read-only SQL statement.
    Query { sql: String },
    /// Scan stored rows through the typed API.
    Scan {
        table: String,
        #[arg(long, allow_hyphen_values = true)]
        start_us: Option<i64>,
        #[arg(long, allow_hyphen_values = true)]
        end_us: Option<i64>,
        #[arg(long)]
        tenant: Option<String>,
        #[arg(long)]
        series: Option<String>,
    },
    /// Print database status.
    Status,
    /// Publish hot rows into a durable local checkpoint.
    Checkpoint,
    /// Run one policy maintenance tick.
    Maintain {
        #[arg(long, allow_hyphen_values = true)]
        now_us: Option<i64>,
    },
    /// Publish durable state to the configured remote store.
    Ship,
    /// Compact eligible local segments.
    Compact,
    /// Restore a database from the configured remote store.
    Restore,
    /// Read independently retained continuous aggregates.
    Rollups { table: String },
    /// Remove remote objects unreachable from the current checkpoint under a remote GC lock.
    VacuumRemote,
    /// Inspect remote publication and lock state without opening a database.
    RemoteHead,
    /// Break an abandoned restore/GC lock ONLY after stopping its owner.
    RecoverRemoteLock {
        #[arg(long)]
        owner: String,
        #[arg(long)]
        confirm_owner_stopped: bool,
    },
    /// Run the HTTP service and maintenance scheduler.
    Serve {
        #[arg(long, default_value = "127.0.0.1")]
        bind: IpAddr,
        /// Permit a non-loopback bind. Requires VARVE_API_TOKEN.
        #[arg(long)]
        allow_remote: bool,
        /// Listening port. If omitted, the Railway-compatible PORT environment variable is used.
        #[arg(long)]
        port: Option<u16>,
        #[arg(long)]
        max_connections: Option<usize>,
        #[arg(long)]
        request_workers: Option<usize>,
        #[arg(long)]
        request_queue: Option<usize>,
        #[arg(long)]
        max_body_bytes: Option<usize>,
        #[arg(long)]
        header_timeout_ms: Option<u64>,
        #[arg(long)]
        body_timeout_ms: Option<u64>,
        #[arg(long)]
        request_timeout_ms: Option<u64>,
        #[arg(long)]
        connection_timeout_ms: Option<u64>,
        #[arg(long)]
        shutdown_timeout_ms: Option<u64>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteInput {
    table: String,
    request_id: String,
    rows: Vec<Row>,
}

#[derive(Debug, Serialize)]
struct OkOutput {
    ok: bool,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    let config = match &cli.config {
        Some(path) => read_json(path).context("read database config")?,
        None => Config::default(),
    };
    config.validate()?;
    let remote = build_remote(cli.remote_dir.as_deref(), cli.s3)?;

    if matches!(cli.command, Command::RemoteHead) {
        let head = remote
            .context("remote-head requires a remote store")?
            .head()?
            .context("no remote head")?;
        return print_json(&serde_json::from_slice::<serde_json::Value>(&head.bytes)?);
    }
    if let Command::RecoverRemoteLock {
        owner,
        confirm_owner_stopped,
    } = &cli.command
    {
        ensure!(
            *confirm_owner_stopped,
            "confirm the lock owner is stopped with --confirm-owner-stopped; never break a live lock"
        );
        Database::recover_remote_lock(
            remote.context("lock recovery requires a remote store")?,
            owner,
        )?;
        return print_json(&OkOutput { ok: true });
    }
    if matches!(cli.command, Command::Restore) {
        let remote = remote.context("restore requires --remote-dir or --s3")?;
        let db = Database::restore(&cli.data, config, remote)?;
        return print_json(&db.status()?);
    }

    let db = Database::open_with_remote(&cli.data, config.clone(), remote)?;
    match cli.command {
        Command::Init | Command::Status => print_json(&db.status()?),
        Command::Create { name, table_config } => {
            let table_config = match table_config {
                Some(path) => read_json(&path).context("read table config")?,
                None => TableConfig::default(),
            };
            print_json(&db.create_table(&name, table_config)?)
        }
        Command::Write { input, now_us } => {
            let input: WriteInput = read_json_limited(
                &input,
                config
                    .max_batch_bytes
                    .saturating_mul(2)
                    .saturating_add(1024),
            )
            .context("read write input")?;
            let receipt = db.write(
                &input.table,
                &input.request_id,
                input.rows,
                explicit_or_system_now(now_us)?,
            )?;
            print_json(&receipt)
        }
        Command::Query { sql } => print_json(&db.query(&sql)?),
        Command::Scan {
            table,
            start_us,
            end_us,
            tenant,
            series,
        } => print_json(&db.scan(
            &table,
            start_us,
            end_us,
            tenant.as_deref(),
            series.as_deref(),
        )?),
        Command::Checkpoint => {
            db.checkpoint()?;
            print_json(&OkOutput { ok: true })
        }
        Command::Maintain { now_us } => print_json(&db.maintain(explicit_or_system_now(now_us)?)?),
        Command::Ship => print_json(&db.ship()?),
        Command::Compact => print_json(&db.compact()?),
        Command::Rollups { table } => print_json(&db.rollups(&table)?),
        Command::VacuumRemote => print_json(&db.vacuum_remote()?),
        Command::RemoteHead | Command::RecoverRemoteLock { .. } => {
            unreachable!("remote administration handled before opening database")
        }
        Command::Serve {
            bind,
            allow_remote,
            port,
            max_connections,
            request_workers,
            request_queue,
            max_body_bytes,
            header_timeout_ms,
            body_timeout_ms,
            request_timeout_ms,
            connection_timeout_ms,
            shutdown_timeout_ms,
        } => service::serve(
            db,
            config,
            bind,
            allow_remote,
            service::Overrides {
                port,
                max_connections,
                request_workers,
                request_queue,
                max_body_bytes,
                header_timeout_ms,
                body_timeout_ms,
                request_timeout_ms,
                connection_timeout_ms,
                shutdown_timeout_ms,
            },
        ),
        Command::Restore => unreachable!("restore handled before opening the local database"),
    }
}

fn build_remote(root: Option<&Path>, s3: bool) -> Result<Option<Arc<dyn RemoteStore>>> {
    match (root, s3) {
        (Some(root), false) => Ok(Some(Arc::new(FileStore::new(root)?))),
        (None, true) => Ok(Some(Arc::new(S3Store::from_env()?))),
        (None, false) => Ok(None),
        (Some(_), true) => unreachable!("clap rejects conflicting remote options"),
    }
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    read_json_limited(path, 64 * 1024)
}

fn read_json_limited<T: DeserializeOwned>(path: &Path, limit: usize) -> Result<T> {
    let mut bytes = Vec::new();
    if path == Path::new("-") {
        io::stdin()
            .lock()
            .take(limit as u64 + 1)
            .read_to_end(&mut bytes)?;
    } else {
        fs::File::open(path)
            .with_context(|| format!("read {}", path.display()))?
            .take(limit as u64 + 1)
            .read_to_end(&mut bytes)?;
    }
    ensure!(bytes.len() <= limit, "JSON input exceeds {limit} bytes");
    serde_json::from_slice(&bytes).context("parse JSON")
}

fn print_json(value: &impl Serialize) -> Result<()> {
    serde_json::to_writer(io::stdout().lock(), value)?;
    println!();
    Ok(())
}

fn explicit_or_system_now(now_us: Option<i64>) -> Result<i64> {
    match now_us {
        Some(now_us) => Ok(now_us),
        None => system_now_us(),
    }
}

fn system_now_us() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?;
    i64::try_from(duration.as_micros()).context("system epoch microseconds exceed i64")
}
