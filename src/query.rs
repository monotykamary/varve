use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::env;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crate::model::{RollupRow, StoredRow, validate_name};

const STDERR_LIMIT: usize = 256 * 1024;
const MAX_QUERY_SCRIPT_BYTES: usize = 8 * 1024 * 1024;
const POLL_INTERVAL: Duration = Duration::from_millis(2);

#[derive(Clone, Debug)]
pub struct QueryTable {
    pub name: String,
    pub hot: Vec<StoredRow>,
    pub files: Vec<PathBuf>,
    pub rollups: Vec<RollupRow>,
    pub cutoff_us: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct QueryOptions {
    pub executable: PathBuf,
    pub memory_mb: usize,
    pub threads: usize,
    pub timeout_ms: u64,
    pub max_output_bytes: usize,
}

#[derive(Clone, Debug, Default)]
pub struct CatalogRelation {
    pub name: String,
    pub columns: Vec<(String, String)>,
    pub rows: Vec<Value>,
}

#[derive(Clone, Debug, Default)]
pub struct AggregateAlias {
    pub name: String,
    pub source: String,
    pub width_us: i64,
}

#[derive(Clone, Debug, Default)]
pub struct QueryCatalog {
    pub relations: Vec<CatalogRelation>,
    pub aggregates: Vec<AggregateAlias>,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            executable: PathBuf::from("duckdb"),
            memory_mb: 128,
            threads: 2,
            timeout_ms: 30_000,
            max_output_bytes: 8 * 1024 * 1024,
        }
    }
}

pub fn execute(tables: &[QueryTable], sql: &str, options: &QueryOptions) -> Result<Value> {
    execute_with_catalog(tables, sql, options, &QueryCatalog::default())
}

pub fn execute_with_catalog(
    tables: &[QueryTable],
    sql: &str,
    options: &QueryOptions,
    catalog: &QueryCatalog,
) -> Result<Value> {
    validate_options(options)?;
    validate_read_only_statement(sql)?;

    let worker = tempfile::Builder::new()
        .prefix("varve-query-")
        .tempdir()
        .context("create private DuckDB worker directory")?;
    let (setup, input) = build_query(tables, sql, options, catalog, worker.path())?;
    ensure!(
        setup.len() <= MAX_QUERY_SCRIPT_BYTES,
        "DuckDB SQL script exceeds {} bytes",
        MAX_QUERY_SCRIPT_BYTES
    );
    let mut script = tempfile::Builder::new()
        .prefix("query-")
        .suffix(".sql")
        .tempfile_in(worker.path())
        .context("create private DuckDB SQL script")?;
    script
        .write_all(setup.as_bytes())
        .context("write private DuckDB SQL script")?;
    script.flush().context("flush private DuckDB SQL script")?;

    let mut command = base_command(options, worker.path());
    command
        .arg("-json")
        .arg(":memory:")
        .arg("-f")
        .arg(script.path());
    let output = run_command(
        command,
        Some(input),
        options.timeout_ms,
        options.max_output_bytes,
    )?;
    if lex_sql(sql)?
        .first()
        .is_some_and(|token| token == "EXPLAIN")
    {
        // DuckDB v2's CLI renders EXPLAIN as a plan tree even with -json.
        let plan = std::str::from_utf8(&output.stdout).context("DuckDB plan is not UTF-8")?;
        return Ok(json!([{"plan": plan.trim_end()}]));
    }
    if output.stdout.iter().all(u8::is_ascii_whitespace) {
        return Ok(Value::Array(Vec::new()));
    }
    serde_json::from_slice(&output.stdout).context("decode DuckDB JSON output")
}

pub fn version(options: &QueryOptions) -> Result<String> {
    validate_options(options)?;
    let worker = tempfile::Builder::new()
        .prefix("varve-version-")
        .tempdir()
        .context("create private DuckDB version directory")?;
    let mut command = Command::new(&options.executable);
    command
        .arg("-no-init")
        .arg("-batch")
        .arg("-bail")
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    isolate_environment(&mut command, worker.path());
    let output = run_command(command, None, options.timeout_ms, 16 * 1024)
        .context("run DuckDB version check")?;
    let version = std::str::from_utf8(&output.stdout)
        .context("DuckDB version output is not UTF-8")?
        .trim()
        .to_owned();
    ensure!(is_v2(&version), "DuckDB v2 is required, found {version:?}");
    Ok(version)
}

fn validate_options(options: &QueryOptions) -> Result<()> {
    ensure!(
        options.memory_mb >= 16,
        "query memory_mb must be at least 16"
    );
    ensure!(options.threads > 0, "query threads must be positive");
    ensure!(options.timeout_ms > 0, "query timeout_ms must be positive");
    ensure!(
        options.max_output_bytes > 0,
        "query max_output_bytes must be positive"
    );
    Ok(())
}

fn is_v2(version: &str) -> bool {
    let version = version.strip_prefix('v').unwrap_or(version);
    version.starts_with("2.")
}

fn base_command(options: &QueryOptions, worker: &Path) -> Command {
    let mut command = Command::new(&options.executable);
    command
        .arg("-no-init")
        .arg("-batch")
        .arg("-bail")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    isolate_environment(&mut command, worker);
    command
}

fn isolate_environment(command: &mut Command, worker: &Path) {
    let path = env::var_os("PATH");
    let system_root = env::var_os("SystemRoot");
    let windir = env::var_os("WINDIR");
    command.env_clear();
    if let Some(path) = path {
        command.env("PATH", path);
    }
    if let Some(system_root) = system_root {
        command.env("SystemRoot", system_root);
    }
    if let Some(windir) = windir {
        command.env("WINDIR", windir);
    }
    command
        .env("HOME", worker)
        .env("TMPDIR", worker)
        .env("TMP", worker)
        .env("TEMP", worker);
}

fn build_query(
    tables: &[QueryTable],
    sql: &str,
    options: &QueryOptions,
    catalog: &QueryCatalog,
    worker: &Path,
) -> Result<(String, Vec<u8>)> {
    let mut table_names = HashSet::new();
    let mut exposed_relations = HashSet::new();
    for table in tables {
        validate_name(&table.name).context("validate query table name")?;
        ensure!(
            table_names.insert(table.name.to_ascii_lowercase()),
            "duplicate query table {}",
            table.name
        );
        exposed_relations.insert(table.name.to_ascii_lowercase());
        exposed_relations.insert(format!("{}__rollup", table.name).to_ascii_lowercase());
    }
    validate_catalog(catalog, tables, &mut exposed_relations)?;

    let mut allowed_paths = Vec::new();
    allowed_paths.push(quote_literal("/dev/stdin"));
    for table in tables {
        for path in &table.files {
            allowed_paths.push(quote_path(path)?);
        }
    }

    let worker_path = quote_path(worker)?;
    let mut setup = String::new();
    setup.push_str(&format!(
        "SET memory_limit = '{}MB'; SET threads = {}; SET max_temp_directory_size = '0B'; SET preserve_insertion_order = false; ",
        options.memory_mb, options.threads
    ));
    setup.push_str("SET autoinstall_known_extensions = false; SET autoload_known_extensions = false; SET allow_community_extensions = false; SET allow_unsigned_extensions = false; ");
    setup.push_str("SET allowed_directories = []; SET allowed_paths = [");
    setup.push_str(&allowed_paths.join(","));
    setup.push_str("]; ");
    setup.push_str("SET home_directory = ");
    setup.push_str(&worker_path);
    setup.push_str("; SET temp_directory = ");
    setup.push_str(&worker_path);
    setup.push_str("; SET enable_external_access = false; SET lock_configuration = true; ");
    setup.push_str(
        "CREATE TEMP TABLE __varve_version_gate AS SELECT CASE WHEN starts_with(version(), 'v2.') THEN 1 ELSE error('DuckDB v2 is required') END AS ok; ",
    );
    setup.push_str(
        "CREATE TEMP TABLE __varve_input AS SELECT * FROM read_json('/dev/stdin', format = 'newline_delimited', auto_detect = false, columns = {kind: 'VARCHAR', table_name: 'VARCHAR', timestamp_us: 'BIGINT', tenant: 'VARCHAR', series: 'VARCHAR', value: 'DOUBLE', tags: 'VARCHAR', sequence: 'UBIGINT', ordinal: 'UINTEGER', width_us: 'BIGINT', bucket_us: 'BIGINT', count: 'UBIGINT', sum: 'DOUBLE', min: 'DOUBLE', max: 'DOUBLE', first: 'DOUBLE', last: 'DOUBLE', first_timestamp_us: 'BIGINT', last_timestamp_us: 'BIGINT', first_sequence: 'UBIGINT', first_ordinal: 'UINTEGER', last_sequence: 'UBIGINT', last_ordinal: 'UINTEGER'}); ",
    );

    let mut input = Vec::new();
    append_json_line(&mut input, &json!({"kind": "sentinel"}))?;
    for table in tables {
        append_table_views(&mut setup, table)?;
        append_table_input(&mut input, table)?;
    }
    for relation in &catalog.relations {
        append_catalog_macro(&mut setup, relation)?;
        append_catalog_input(&mut input, relation)?;
    }
    for alias in &catalog.aggregates {
        append_aggregate_alias(&mut setup, alias, tables)?;
    }
    setup.push_str(sql);
    Ok((setup, input))
}

fn validate_catalog(
    catalog: &QueryCatalog,
    tables: &[QueryTable],
    exposed_relations: &mut HashSet<String>,
) -> Result<()> {
    let mut macros = HashSet::new();
    for relation in &catalog.relations {
        validate_catalog_identifier(&relation.name, "catalog relation")?;
        ensure!(
            macros.insert(relation.name.to_ascii_lowercase()),
            "duplicate catalog relation {}",
            relation.name
        );
        ensure!(
            !relation.columns.is_empty(),
            "catalog relation {} must have at least one column",
            relation.name
        );
        let mut columns = HashSet::new();
        for (name, column_type) in &relation.columns {
            validate_catalog_identifier(name, "catalog column")?;
            ensure!(
                columns.insert(name.to_ascii_lowercase()),
                "duplicate catalog column {name} in {}",
                relation.name
            );
            catalog_type(column_type)?;
        }
        for row in &relation.rows {
            catalog_row_values(relation, row)?;
        }
    }

    for alias in &catalog.aggregates {
        validate_catalog_identifier(&alias.name, "aggregate alias")?;
        validate_catalog_identifier(&alias.source, "aggregate source")?;
        ensure!(
            exposed_relations.insert(alias.name.to_ascii_lowercase()),
            "duplicate exposed relation {}",
            alias.name
        );
        let matching_sources = tables
            .iter()
            .filter(|table| table.name.eq_ignore_ascii_case(&alias.source))
            .count();
        ensure!(
            matching_sources == 1,
            "aggregate alias {} has unknown or ambiguous source {}",
            alias.name,
            alias.source
        );
    }
    Ok(())
}

fn validate_catalog_identifier(identifier: &str, kind: &str) -> Result<()> {
    ensure!(
        !identifier.is_empty() && identifier.len() <= 255,
        "{kind} identifier must be 1..255 bytes"
    );
    ensure!(!identifier.contains('\0'), "{kind} identifier contains NUL");
    Ok(())
}

fn catalog_type(column_type: &str) -> Result<&'static str> {
    match column_type {
        "VARCHAR" => Ok("VARCHAR"),
        "BIGINT" => Ok("BIGINT"),
        "UBIGINT" => Ok("UBIGINT"),
        "DOUBLE" => Ok("DOUBLE"),
        "BOOLEAN" => Ok("BOOLEAN"),
        _ => bail!("unsupported catalog column type {column_type:?}"),
    }
}

fn catalog_row_values(relation: &CatalogRelation, row: &Value) -> Result<Vec<Value>> {
    let values = match row {
        Value::Array(values) => {
            ensure!(
                values.len() == relation.columns.len(),
                "catalog relation {} row has {} values for {} columns",
                relation.name,
                values.len(),
                relation.columns.len()
            );
            values.clone()
        }
        Value::Object(values) => {
            ensure!(
                values.len() == relation.columns.len(),
                "catalog relation {} row has {} fields for {} columns",
                relation.name,
                values.len(),
                relation.columns.len()
            );
            relation
                .columns
                .iter()
                .map(|(name, _)| {
                    values.get(name).cloned().with_context(|| {
                        format!("catalog relation {} row is missing {name}", relation.name)
                    })
                })
                .collect::<Result<Vec<_>>>()?
        }
        _ => bail!(
            "catalog relation {} row must be an array or object",
            relation.name
        ),
    };

    for (value, (name, column_type)) in values.iter().zip(&relation.columns) {
        let valid = value.is_null()
            || match column_type.as_str() {
                "VARCHAR" => value.is_string(),
                "BIGINT" => value.as_i64().is_some(),
                "UBIGINT" => value.as_u64().is_some(),
                "DOUBLE" => value.as_f64().is_some_and(f64::is_finite),
                "BOOLEAN" => value.is_boolean(),
                _ => false,
            };
        ensure!(
            valid,
            "catalog relation {} column {name} is not a nullable {column_type}",
            relation.name
        );
    }
    Ok(values)
}

fn append_json_line(output: &mut Vec<u8>, value: &Value) -> Result<()> {
    serde_json::to_writer(&mut *output, value)?;
    output.push(b'\n');
    Ok(())
}

fn append_table_input(input: &mut Vec<u8>, table: &QueryTable) -> Result<()> {
    for row in &table.hot {
        row.row.validate().context("validate hot query row")?;
        append_json_line(
            input,
            &json!({
                "kind": "hot",
                "table_name": table.name,
                "timestamp_us": row.row.timestamp_us,
                "tenant": row.row.tenant,
                "series": row.row.series,
                "value": row.row.value,
                "tags": serde_json::to_string(&row.row.tags)?,
                "sequence": row.sequence,
                "ordinal": row.ordinal,
            }),
        )?;
    }
    for rollup in &table.rollups {
        ensure!(
            rollup.sum.is_finite()
                && rollup.min.is_finite()
                && rollup.max.is_finite()
                && rollup.first.is_finite()
                && rollup.last.is_finite(),
            "rollup values must be finite"
        );
        append_json_line(
            input,
            &json!({
                "kind": "rollup",
                "table_name": table.name,
                "width_us": rollup.width_us,
                "bucket_us": rollup.bucket_us,
                "tenant": rollup.tenant,
                "series": rollup.series,
                "tags": serde_json::to_string(&rollup.tags)?,
                "count": rollup.count,
                "sum": rollup.sum,
                "min": rollup.min,
                "max": rollup.max,
                "first": rollup.first,
                "last": rollup.last,
                "first_timestamp_us": rollup.first_timestamp_us,
                "last_timestamp_us": rollup.last_timestamp_us,
                "first_sequence": rollup.first_sequence,
                "first_ordinal": rollup.first_ordinal,
                "last_sequence": rollup.last_sequence,
                "last_ordinal": rollup.last_ordinal,
            }),
        )?;
    }
    Ok(())
}

fn append_catalog_input(input: &mut Vec<u8>, relation: &CatalogRelation) -> Result<()> {
    for row in &relation.rows {
        let values = catalog_row_values(relation, row)?;
        append_json_line(
            input,
            &json!({
                "kind": "catalog",
                "table_name": relation.name,
                "tags": serde_json::to_string(&values)?,
            }),
        )?;
    }
    Ok(())
}

fn append_table_views(sql: &mut String, table: &QueryTable) -> Result<()> {
    let identifier = quote_identifier(&table.name);
    let table_literal = quote_literal(&table.name);
    sql.push_str("CREATE TEMP VIEW ");
    sql.push_str(&identifier);
    sql.push_str(" AS SELECT timestamp_us, tenant, series, value, tags, sequence, ordinal FROM (");
    sql.push_str("SELECT timestamp_us, tenant, series, value, tags, sequence, ordinal FROM __varve_input WHERE kind = 'hot' AND table_name = ");
    sql.push_str(&table_literal);
    if let Some(cutoff) = table.cutoff_us {
        sql.push_str(&format!(" AND timestamp_us >= {cutoff}"));
    }
    if !table.files.is_empty() {
        sql.push_str(" UNION ALL SELECT timestamp_us, tenant, series, value, tags, sequence, ordinal FROM read_parquet([");
        for (index, path) in table.files.iter().enumerate() {
            if index > 0 {
                sql.push(',');
            }
            sql.push_str(&quote_path(path)?);
        }
        sql.push_str("])");
        if let Some(cutoff) = table.cutoff_us {
            sql.push_str(&format!(" WHERE timestamp_us >= {cutoff}"));
        }
    }
    sql.push_str("); ");

    sql.push_str("CREATE TEMP VIEW ");
    sql.push_str(&quote_identifier(&format!("{}__rollup", table.name)));
    sql.push_str(" AS SELECT width_us, bucket_us, tenant, series, tags, count, sum, sum / nullif(count, 0) AS average, min, max, first, last, first AS open, max AS high, min AS low, last AS close, first_timestamp_us, last_timestamp_us, first_sequence, first_ordinal, last_sequence, last_ordinal FROM __varve_input WHERE kind = 'rollup' AND table_name = ");
    sql.push_str(&table_literal);
    sql.push_str("; ");
    Ok(())
}

fn append_catalog_macro(sql: &mut String, relation: &CatalogRelation) -> Result<()> {
    sql.push_str("CREATE TEMP MACRO ");
    sql.push_str(&quote_identifier(&relation.name));
    sql.push_str("() AS TABLE SELECT ");
    for (index, (name, column_type)) in relation.columns.iter().enumerate() {
        if index > 0 {
            sql.push(',');
        }
        let column_type = catalog_type(column_type)?;
        if column_type == "VARCHAR" {
            sql.push_str(&format!("json_extract_string(tags, '$[{index}]')"));
        } else {
            sql.push_str("CAST(json_extract(tags, '");
            sql.push_str(&format!("$[{index}]"));
            sql.push_str("') AS ");
            sql.push_str(column_type);
            sql.push(')');
        }
        sql.push_str(" AS ");
        sql.push_str(&quote_identifier(name));
    }
    sql.push_str(" FROM __varve_input WHERE kind = 'catalog' AND table_name = ");
    sql.push_str(&quote_literal(&relation.name));
    sql.push_str("; ");
    Ok(())
}

fn append_aggregate_alias(
    sql: &mut String,
    alias: &AggregateAlias,
    tables: &[QueryTable],
) -> Result<()> {
    let source = tables
        .iter()
        .find(|table| table.name.eq_ignore_ascii_case(&alias.source))
        .context("aggregate source disappeared after validation")?;
    sql.push_str("CREATE TEMP VIEW ");
    sql.push_str(&quote_identifier(&alias.name));
    sql.push_str(" AS SELECT * REPLACE (CAST(count AS BIGINT) AS count) FROM ");
    sql.push_str(&quote_identifier(&format!("{}__rollup", source.name)));
    sql.push_str(" WHERE width_us = ");
    sql.push_str(&alias.width_us.to_string());
    sql.push_str("; ");
    Ok(())
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn quote_literal(literal: &str) -> String {
    format!("'{}'", literal.replace('\'', "''"))
}

fn quote_path(path: &Path) -> Result<String> {
    let path = path
        .to_str()
        .with_context(|| format!("Parquet path is not UTF-8: {}", path.display()))?;
    ensure!(!path.contains('\0'), "Parquet path contains NUL");
    Ok(quote_literal(path))
}

struct ProcessOutput {
    stdout: Vec<u8>,
}

fn run_command(
    mut command: Command,
    input: Option<Vec<u8>>,
    timeout_ms: u64,
    max_output: usize,
) -> Result<ProcessOutput> {
    let mut child = command.spawn().context("spawn DuckDB executable")?;
    let stdout = child.stdout.take().context("capture DuckDB stdout")?;
    let stderr = child.stderr.take().context("capture DuckDB stderr")?;
    let stdout_overflow = Arc::new(AtomicBool::new(false));
    let stderr_overflow = Arc::new(AtomicBool::new(false));
    let stdout_thread = read_bounded(stdout, max_output, Arc::clone(&stdout_overflow));
    let stderr_thread = read_bounded(stderr, STDERR_LIMIT, Arc::clone(&stderr_overflow));
    let stdin_thread = child.stdin.take().map(|mut stdin| {
        thread::spawn(move || -> std::io::Result<()> {
            if let Some(input) = input {
                stdin.write_all(&input)?;
            }
            Ok(())
        })
    });

    let deadline = Instant::now()
        .checked_add(Duration::from_millis(timeout_ms))
        .context("query timeout is too large")?;
    let (status, stop_reason) = wait_bounded(&mut child, deadline, &stdout_overflow)?;
    let stdout = stdout_thread
        .join()
        .map_err(|_| anyhow::anyhow!("DuckDB stdout reader panicked"))??;
    let stderr = stderr_thread
        .join()
        .map_err(|_| anyhow::anyhow!("DuckDB stderr reader panicked"))??;
    let stdin_result = stdin_thread
        .map(|writer| {
            writer
                .join()
                .map_err(|_| anyhow::anyhow!("DuckDB stdin writer panicked"))
        })
        .transpose()?
        .transpose();

    if stop_reason == StopReason::OutputLimit || stdout_overflow.load(Ordering::Relaxed) {
        bail!("DuckDB output exceeded {} bytes", max_output);
    }
    if stop_reason == StopReason::Timeout {
        bail!("DuckDB query timed out");
    }
    if !status.success() {
        let message = String::from_utf8_lossy(&stderr);
        if stderr_overflow.load(Ordering::Relaxed) {
            bail!("DuckDB failed ({status}): {message} [stderr truncated]");
        }
        bail!("DuckDB failed ({status}): {message}");
    }
    if let Err(error) = stdin_result {
        return Err(error).context("write DuckDB stdin");
    }
    Ok(ProcessOutput { stdout })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StopReason {
    Completed,
    OutputLimit,
    Timeout,
}

fn wait_bounded(
    child: &mut Child,
    deadline: Instant,
    output_overflow: &AtomicBool,
) -> Result<(ExitStatus, StopReason)> {
    loop {
        if let Some(status) = child.try_wait().context("poll DuckDB process")? {
            return Ok((status, StopReason::Completed));
        }
        if output_overflow.load(Ordering::Relaxed) {
            child.kill().context("kill DuckDB after output limit")?;
            let status = child.wait().context("reap DuckDB after output limit")?;
            return Ok((status, StopReason::OutputLimit));
        }
        if Instant::now() >= deadline {
            child.kill().context("kill timed out DuckDB query")?;
            let status = child.wait().context("reap timed out DuckDB query")?;
            return Ok((status, StopReason::Timeout));
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn read_bounded<R: Read + Send + 'static>(
    mut reader: R,
    limit: usize,
    overflow: Arc<AtomicBool>,
) -> thread::JoinHandle<std::io::Result<Vec<u8>>> {
    thread::spawn(move || {
        let mut output = Vec::with_capacity(limit.min(64 * 1024));
        let mut buffer = [0_u8; 16 * 1024];
        loop {
            let read = reader.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            let remaining = limit.saturating_sub(output.len());
            output.extend_from_slice(&buffer[..read.min(remaining)]);
            if read > remaining {
                overflow.store(true, Ordering::Relaxed);
            }
        }
        Ok(output)
    })
}

fn validate_read_only_statement(sql: &str) -> Result<()> {
    let tokens = lex_sql(sql)?;
    let first = tokens.first().context("SQL statement is empty")?;
    ensure!(
        matches!(first.as_str(), "SELECT" | "WITH" | "EXPLAIN"),
        "SQL must begin with SELECT, WITH, or EXPLAIN"
    );

    const FORBIDDEN: &[&str] = &[
        "ALTER",
        "ATTACH",
        "CALL",
        "CHECKPOINT",
        "COMMENT",
        "COPY",
        "CREATE",
        "DELETE",
        "DETACH",
        "DROP",
        "EXPORT",
        "FORCE",
        "GRANT",
        "IMPORT",
        "INSERT",
        "INSTALL",
        "LOAD",
        "MERGE",
        "PRAGMA",
        "REPLACE",
        "RESET",
        "REVOKE",
        "SET",
        "TRUNCATE",
        "UPDATE",
        "USE",
        "VACUUM",
    ];
    if let Some(token) = tokens
        .iter()
        .find(|token| FORBIDDEN.contains(&token.as_str()))
    {
        bail!("SQL contains forbidden keyword {token}");
    }
    Ok(())
}

fn lex_sql(sql: &str) -> Result<Vec<String>> {
    let bytes = sql.as_bytes();
    let mut index = 0;
    let mut tokens = Vec::new();
    let mut ended = false;
    let mut at_line_start = true;
    while index < bytes.len() {
        match bytes[index] {
            byte if byte.is_ascii_whitespace() => {
                if byte == b'\n' || byte == b'\r' {
                    at_line_start = true;
                }
                index += 1;
            }
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                let next = skip_block_comment(bytes, index + 2)?;
                if bytes[index..next]
                    .iter()
                    .any(|byte| *byte == b'\n' || *byte == b'\r')
                {
                    at_line_start = true;
                }
                index = next;
            }
            b'\'' => {
                ensure!(
                    !ended,
                    "only whitespace or comments may follow the SQL statement"
                );
                at_line_start = false;
                index = skip_quoted(bytes, index + 1, b'\'')?;
            }
            b'"' => {
                ensure!(
                    !ended,
                    "only whitespace or comments may follow the SQL statement"
                );
                at_line_start = false;
                index = skip_quoted(bytes, index + 1, b'"')?;
            }
            b'$' => {
                ensure!(
                    !ended,
                    "only whitespace or comments may follow the SQL statement"
                );
                at_line_start = false;
                if let Some(next) = skip_dollar_quote(bytes, index)? {
                    index = next;
                } else {
                    index += 1;
                }
            }
            b';' => {
                ensure!(!ended, "SQL must contain exactly one statement");
                ended = true;
                at_line_start = false;
                index += 1;
            }
            b'.' if at_line_start
                && bytes
                    .get(index + 1)
                    .is_some_and(|byte| byte.is_ascii_alphabetic()) =>
            {
                bail!("DuckDB CLI dot commands are not allowed")
            }
            byte if byte.is_ascii_alphabetic() || byte == b'_' => {
                ensure!(!ended, "SQL must contain exactly one statement");
                at_line_start = false;
                let start = index;
                index += 1;
                while index < bytes.len()
                    && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
                {
                    index += 1;
                }
                tokens.push(sql[start..index].to_ascii_uppercase());
            }
            _ => {
                ensure!(
                    !ended,
                    "only whitespace or comments may follow the SQL statement"
                );
                at_line_start = false;
                index += 1;
            }
        }
    }
    Ok(tokens)
}

fn skip_quoted(bytes: &[u8], mut index: usize, quote: u8) -> Result<usize> {
    while index < bytes.len() {
        if bytes[index] == quote {
            if bytes.get(index + 1) == Some(&quote) {
                index += 2;
            } else {
                return Ok(index + 1);
            }
        } else {
            index += 1;
        }
    }
    bail!("unterminated SQL quote")
}

fn skip_block_comment(bytes: &[u8], mut index: usize) -> Result<usize> {
    let mut depth = 1_usize;
    while index < bytes.len() {
        if bytes.get(index..index + 2) == Some(b"/*") {
            depth += 1;
            index += 2;
        } else if bytes.get(index..index + 2) == Some(b"*/") {
            depth -= 1;
            index += 2;
            if depth == 0 {
                return Ok(index);
            }
        } else {
            index += 1;
        }
    }
    bail!("unterminated SQL block comment")
}

fn skip_dollar_quote(bytes: &[u8], index: usize) -> Result<Option<usize>> {
    let mut delimiter_end = index + 1;
    while delimiter_end < bytes.len()
        && (bytes[delimiter_end].is_ascii_alphanumeric() || bytes[delimiter_end] == b'_')
    {
        delimiter_end += 1;
    }
    if bytes.get(delimiter_end) != Some(&b'$') {
        return Ok(None);
    }
    let delimiter = &bytes[index..=delimiter_end];
    let content_start = delimiter_end + 1;
    let Some(relative_end) = bytes[content_start..]
        .windows(delimiter.len())
        .position(|window| window == delimiter)
    else {
        bail!("unterminated SQL dollar quote");
    };
    Ok(Some(content_start + relative_end + delimiter.len()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_one_analytical_statement() {
        validate_read_only_statement(
            " /* lead */ WITH x AS (SELECT ';' AS value) SELECT * FROM x;",
        )
        .unwrap();
        validate_read_only_statement("EXPLAIN ANALYZE SELECT 1").unwrap();
        validate_read_only_statement("SELECT $$semi; DELETE$$").unwrap();
    }

    #[test]
    fn rejects_mutations_and_injected_statements() {
        for sql in [
            "DELETE FROM metrics",
            "WITH gone AS (DELETE FROM metrics RETURNING *) SELECT * FROM gone",
            "EXPLAIN UPDATE metrics SET value = 1",
            "SELECT 1; DROP TABLE metrics",
            "SELECT 1; .shell whoami",
            "SELECT 1\n.shell whoami",
            "SELECT 1\n.read /tmp/query.sql",
            ".read /tmp/query.sql",
            "/* unterminated",
            "SELECT 'unterminated",
        ] {
            assert!(
                validate_read_only_statement(sql).is_err(),
                "accepted {sql:?}"
            );
        }
    }
}
