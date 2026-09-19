use anyhow::{Context, Result, bail, ensure};
use serde::Serialize;
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

mod native;
mod resident_types;
mod workers;
pub(crate) use native::NativeRuntime;
#[cfg(test)]
mod native_race_tests;
#[cfg(test)]
mod native_reuse_tests;
#[cfg(test)]
mod native_tests;
pub(crate) use resident_types::{
    ResidentBatch, ResidentFile, ResidentLineage, ResidentSnapshot, ResidentTable,
};
pub use workers::{QueryRuntime, QueryWorkerStats};

const STDERR_LIMIT: usize = 256 * 1024;
const MAX_QUERY_SCRIPT_BYTES: usize = 8 * 1024 * 1024;
const MAX_QUERY_INPUT_BYTES: usize = 128 * 1024 * 1024;
const MAX_CATALOG_SQL_BYTES: usize = 32 * 1024;
const MAX_TYPED_INPUT_ROWS: usize = 128;
const MAX_TYPED_SQL_BYTES: usize = 32 * 1024;
const POLL_INTERVAL: Duration = Duration::from_millis(2);

// One schema drives both ingestion paths, including unsigned tie-breakers.
const INPUT_COLUMNS: &[(&str, &str)] = &[
    ("kind", "VARCHAR"),
    ("table_name", "VARCHAR"),
    ("batch_id", "VARCHAR"),
    ("timestamp_us", "BIGINT"),
    ("tenant", "VARCHAR"),
    ("series", "VARCHAR"),
    ("value", "DOUBLE"),
    ("tags", "VARCHAR"),
    ("sequence", "UBIGINT"),
    ("ordinal", "UINTEGER"),
    ("width_us", "BIGINT"),
    ("bucket_us", "BIGINT"),
    ("count", "UBIGINT"),
    ("sum", "DOUBLE"),
    ("min", "DOUBLE"),
    ("max", "DOUBLE"),
    ("first", "DOUBLE"),
    ("last", "DOUBLE"),
    ("first_timestamp_us", "BIGINT"),
    ("last_timestamp_us", "BIGINT"),
    ("first_sequence", "UBIGINT"),
    ("first_ordinal", "UINTEGER"),
    ("last_sequence", "UBIGINT"),
    ("last_ordinal", "UINTEGER"),
];

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
        (!input.is_empty()).then_some(input),
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
    build_query_mode(tables, sql, options, catalog, worker, true)
}

fn build_query_mode(
    tables: &[QueryTable],
    sql: &str,
    options: &QueryOptions,
    catalog: &QueryCatalog,
    worker: &Path,
    inline_relations: bool,
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
    let literals = if inline_relations {
        typed_relations(tables, catalog)?
    } else {
        None
    };
    let has_input = literals.is_none()
        && (tables
            .iter()
            .any(|table| !table.hot.is_empty() || !table.rollups.is_empty())
            || catalog
                .relations
                .iter()
                .any(|relation| !relation.rows.is_empty()));
    if has_input {
        allowed_paths.push(quote_literal("/dev/stdin"));
    }
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
    if has_input {
        setup.push_str("CREATE TEMP TABLE __varve_input AS SELECT * FROM read_json('/dev/stdin', format = 'newline_delimited', auto_detect = false, columns = {");
        for (index, (name, kind)) in INPUT_COLUMNS.iter().enumerate() {
            if index > 0 {
                setup.push(',');
            }
            setup.push_str(&format!(
                "{}: {}",
                quote_identifier(name),
                quote_literal(kind)
            ));
        }
        setup.push_str("}); ");
    } else {
        setup.push_str("CREATE TEMP TABLE __varve_input (");
        for (index, (name, kind)) in INPUT_COLUMNS.iter().enumerate() {
            if index > 0 {
                setup.push(',');
            }
            setup.push_str(&format!("{} {kind}", quote_identifier(name)));
        }
        setup.push_str("); ");
    }

    let mut input = Vec::new();
    for table in tables {
        append_table_views(&mut setup, table)?;
        if literals.is_none() {
            append_table_input(&mut input, table)?;
        }
    }
    if let Some(literals) = &literals {
        setup.push_str(literals);
    } else {
        for relation in &catalog.relations {
            append_catalog_macro(&mut setup, relation)?;
            append_catalog_input(&mut input, relation)?;
        }
    }
    for alias in &catalog.aggregates {
        append_aggregate_alias(&mut setup, alias, tables)?;
    }
    setup.push_str(sql);
    // Literals must not consume headroom available to the original scanner script.
    if literals.is_some() && setup.len() > MAX_QUERY_SCRIPT_BYTES {
        return build_query_mode(tables, sql, options, catalog, worker, false);
    }
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

fn catalog_row_values<'a>(relation: &CatalogRelation, row: &'a Value) -> Result<Vec<&'a Value>> {
    let values = match row {
        Value::Array(values) => {
            ensure!(
                values.len() == relation.columns.len(),
                "catalog relation {} row has {} values for {} columns",
                relation.name,
                values.len(),
                relation.columns.len()
            );
            values.iter().collect()
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
                    values.get(name).with_context(|| {
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

fn append_json_line(output: &mut Vec<u8>, value: &impl Serialize) -> Result<()> {
    let mut writer = BoundedInput(output, MAX_QUERY_INPUT_BYTES);
    serde_json::to_writer(&mut writer, value)?;
    writer.write_all(b"\n")?;
    Ok(())
}

struct BoundedInput<'a>(&'a mut Vec<u8>, usize);

impl Write for BoundedInput<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.1.saturating_sub(self.0.len()) {
            return Err(std::io::Error::other("DuckDB query input exceeds 128 MiB"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

// Borrow strings; only tags need an owned JSON string for the VARCHAR contract.
#[derive(Serialize)]
struct HotInput<'a> {
    kind: &'a str,
    table_name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    batch_id: Option<&'a str>,
    timestamp_us: i64,
    tenant: &'a str,
    series: &'a str,
    value: f64,
    tags: String,
    sequence: u64,
    ordinal: u32,
}

#[derive(Serialize)]
struct RollupInput<'a> {
    kind: &'a str,
    table_name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    batch_id: Option<&'a str>,
    width_us: i64,
    bucket_us: i64,
    tenant: &'a str,
    series: &'a str,
    tags: String,
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
    first: f64,
    last: f64,
    first_timestamp_us: i64,
    last_timestamp_us: i64,
    first_sequence: u64,
    first_ordinal: u32,
    last_sequence: u64,
    last_ordinal: u32,
}

fn append_table_input(input: &mut Vec<u8>, table: &QueryTable) -> Result<()> {
    for row in &table.hot {
        row.row.validate().context("validate hot query row")?;
        append_json_line(
            input,
            &HotInput {
                kind: "hot",
                table_name: &table.name,
                batch_id: None,
                timestamp_us: row.row.timestamp_us,
                tenant: &row.row.tenant,
                series: &row.row.series,
                value: row.row.value,
                tags: serde_json::to_string(&row.row.tags)?,
                sequence: row.sequence,
                ordinal: row.ordinal,
            },
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
            &RollupInput {
                kind: "rollup",
                table_name: &table.name,
                batch_id: None,
                width_us: rollup.width_us,
                bucket_us: rollup.bucket_us,
                tenant: &rollup.tenant,
                series: &rollup.series,
                tags: serde_json::to_string(&rollup.tags)?,
                count: rollup.count,
                sum: rollup.sum,
                min: rollup.min,
                max: rollup.max,
                first: rollup.first,
                last: rollup.last,
                first_timestamp_us: rollup.first_timestamp_us,
                last_timestamp_us: rollup.last_timestamp_us,
                first_sequence: rollup.first_sequence,
                first_ordinal: rollup.first_ordinal,
                last_sequence: rollup.last_sequence,
                last_ordinal: rollup.last_ordinal,
            },
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
    sql.push_str("SELECT timestamp_us, tenant, series, value, tags, sequence, ordinal FROM __varve_input AS raw WHERE kind = 'hot' AND table_name = ");
    sql.push_str(&table_literal);
    sql.push_str(" AND (batch_id IS NULL OR EXISTS (SELECT 1 FROM __varve_input AS selected WHERE selected.kind = 'selected' AND selected.table_name = raw.table_name AND selected.batch_id = raw.batch_id))");
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

// This is copied, bounded SQL construction, not native or zero-copy ingestion.
// Selection depends only on the supplied snapshot, never on the user's SQL.
fn typed_relations(tables: &[QueryTable], catalog: &QueryCatalog) -> Result<Option<String>> {
    let rows = tables
        .iter()
        .flat_map(|table| [table.hot.len(), table.rollups.len()])
        .chain(catalog.relations.iter().map(|relation| relation.rows.len()))
        .try_fold(0usize, |total, count| total.checked_add(count));
    if rows.is_none_or(|rows| rows > MAX_TYPED_INPUT_ROWS) {
        return Ok(None);
    }
    let mut sql = TypedSql(String::new());
    let mut first = true;
    for table in tables {
        for stored in &table.hot {
            let row = &stored.row;
            row.validate().context("validate hot query row")?;
            let Some(tags) = typed_tags(&row.tags) else {
                return Ok(None);
            };
            use TypedValue::*;
            if sql
                .row(
                    &mut first,
                    &[
                        Text("hot"),
                        Text(&table.name),
                        Null,
                        Signed(row.timestamp_us),
                        Text(&row.tenant),
                        Text(&row.series),
                        Double(row.value),
                        Text(&tags),
                        Unsigned(stored.sequence),
                        Unsigned(stored.ordinal.into()),
                        Null,
                        Null,
                        Null,
                        Null,
                        Null,
                        Null,
                        Null,
                        Null,
                        Null,
                        Null,
                        Null,
                        Null,
                        Null,
                        Null,
                    ],
                )
                .is_err()
            {
                return Ok(None);
            }
        }
        for row in &table.rollups {
            ensure!(
                [row.sum, row.min, row.max, row.first, row.last]
                    .iter()
                    .all(|v| v.is_finite()),
                "rollup values must be finite"
            );
            let Some(tags) = typed_tags(&row.tags) else {
                return Ok(None);
            };
            use TypedValue::*;
            if sql
                .row(
                    &mut first,
                    &[
                        Text("rollup"),
                        Text(&table.name),
                        Null,
                        Null,
                        Text(&row.tenant),
                        Text(&row.series),
                        Null,
                        Text(&tags),
                        Null,
                        Null,
                        Signed(row.width_us),
                        Signed(row.bucket_us),
                        Unsigned(row.count),
                        Double(row.sum),
                        Double(row.min),
                        Double(row.max),
                        Double(row.first),
                        Double(row.last),
                        Signed(row.first_timestamp_us),
                        Signed(row.last_timestamp_us),
                        Unsigned(row.first_sequence),
                        Unsigned(row.first_ordinal.into()),
                        Unsigned(row.last_sequence),
                        Unsigned(row.last_ordinal.into()),
                    ],
                )
                .is_err()
            {
                return Ok(None);
            }
        }
    }
    if !first && sql.push("; ").is_err() {
        return Ok(None);
    }
    let Some(catalog) = catalog_literals(catalog)? else {
        return Ok(None);
    };
    if sql.push(&catalog).is_err() {
        return Ok(None);
    }
    Ok(Some(sql.0))
}

fn typed_tags(tags: &std::collections::BTreeMap<String, String>) -> Option<String> {
    let mut bytes = Vec::new();
    serde_json::to_writer(&mut BoundedInput(&mut bytes, MAX_TYPED_SQL_BYTES), tags).ok()?;
    String::from_utf8(bytes).ok()
}

enum TypedValue<'a> {
    Null,
    Text(&'a str),
    Signed(i64),
    Unsigned(u64),
    Double(f64),
}

struct TypedSql(String);

impl TypedSql {
    fn push(&mut self, text: &str) -> std::fmt::Result {
        if text.len() > MAX_TYPED_SQL_BYTES.saturating_sub(self.0.len()) {
            return Err(std::fmt::Error);
        }
        self.0.push_str(text);
        Ok(())
    }

    fn literal(&mut self, text: &str, kind: &str) -> std::fmt::Result {
        // NUL cannot occur in a CLI SQL script. Keep scanner semantics for it.
        if text.len() > MAX_TYPED_SQL_BYTES || text.contains('\0') {
            return Err(std::fmt::Error);
        }
        self.push("CAST('")?;
        for (index, part) in text.split('\'').enumerate() {
            if index > 0 {
                self.push("''")?;
            }
            self.push(part)?;
        }
        self.push("' AS ")?;
        self.push(kind)?;
        self.push(")")
    }

    fn row(
        &mut self,
        first: &mut bool,
        values: &[TypedValue<'_>; INPUT_COLUMNS.len()],
    ) -> std::fmt::Result {
        self.push(if *first {
            "INSERT INTO __varve_input VALUES ("
        } else {
            ",("
        })?;
        *first = false;
        for (index, (value, (_, kind))) in values.iter().zip(INPUT_COLUMNS).enumerate() {
            if index > 0 {
                self.push(",")?;
            }
            match value {
                TypedValue::Null => self.push("NULL")?,
                TypedValue::Text(text) => self.literal(text, kind)?,
                TypedValue::Signed(value) => self.literal(&value.to_string(), kind)?,
                TypedValue::Unsigned(value) => self.literal(&value.to_string(), kind)?,
                // Cast the round-trip decimal string directly to DOUBLE, without
                // SQL decimal inference (which would also erase negative zero).
                TypedValue::Double(value) => self.literal(&value.to_string(), kind)?,
            }
        }
        self.push(")")
    }
}

// Bound the complete encoded macros, not just the source payload. Any miss uses
// the existing JSON scanner; no relation is pruned based on the user's SQL.
fn catalog_literals(catalog: &QueryCatalog) -> Result<Option<String>> {
    let mut sql = String::new();
    macro_rules! push {
        ($text:expr) => {{
            let text = $text;
            if text.len() > MAX_CATALOG_SQL_BYTES - sql.len() {
                return Ok(None);
            }
            sql.push_str(&text);
        }};
    }
    for relation in &catalog.relations {
        // Even empty relations must not allocate an unbounded NULL-value vector.
        if relation.columns.len() > MAX_CATALOG_SQL_BYTES {
            return Ok(None);
        }
        push!(format!(
            "CREATE TEMP MACRO {}() AS TABLE SELECT * FROM (VALUES ",
            quote_identifier(&relation.name)
        ));
        // A typed NULL row plus WHERE false also supplies empty relation types.
        for index in 0..relation.rows.len().max(1) {
            if index > 0 {
                push!(",");
            }
            push!("(");
            let values = if relation.rows.is_empty() {
                vec![&Value::Null; relation.columns.len()]
            } else {
                catalog_row_values(relation, &relation.rows[index])?
            };
            for (i, (value, (_, kind))) in values.iter().zip(&relation.columns).enumerate() {
                if i > 0 {
                    push!(",");
                }
                let literal = match value {
                    Value::Null => "NULL".into(),
                    Value::String(text) => {
                        if text.contains('\0') || text.len() > MAX_CATALOG_SQL_BYTES {
                            return Ok(None);
                        }
                        quote_literal(text)
                    }
                    // Quoting numbers avoids inference through signed or decimal
                    // intermediate types, in particular for u64::MAX.
                    _ => quote_literal(&value.to_string()),
                };
                push!(format!("CAST({literal} AS {kind})"));
            }
            push!(")");
        }
        push!(") AS catalog_values(");
        for (i, (name, _)) in relation.columns.iter().enumerate() {
            if i > 0 {
                push!(",");
            }
            push!(quote_identifier(name));
        }
        push!(")");
        if relation.rows.is_empty() {
            push!(" WHERE false");
        }
        push!("; ");
    }
    Ok(Some(sql))
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
    if input.is_none() {
        // No pipe or stdin writer thread for payload-free queries.
        command.stdin(Stdio::null());
    }
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

// Pooling is an optimization, not a SQL permission boundary. ROLLBACK does not
// reset all connection state (notably setseed/random). Parse misses and anything
// outside this deliberately small non-mutating subset use a disposable worker.
fn pooling_eligible(sql: &str, catalog: &QueryCatalog) -> bool {
    use sqlparser::ast::{
        BinaryOperator, Expr, FunctionArguments, ObjectName, ObjectNamePart, Query, SelectFlavor,
        SetExpr, Statement, TableFactor, UnaryOperator, Visit, Visitor,
    };
    use sqlparser::dialect::DuckDbDialect;
    use sqlparser::parser::Parser;
    use std::ops::ControlFlow;

    fn simple_name(name: &ObjectName) -> Option<&str> {
        match name.0.as_slice() {
            [ObjectNamePart::Identifier(name)] if matches!(name.quote_style, None | Some('"')) => {
                Some(&name.value)
            }
            _ => None,
        }
    }

    fn scalar(name: &ObjectName) -> bool {
        simple_name(name).is_some_and(|name| {
            matches!(
                name.to_ascii_lowercase().as_str(),
                "abs"
                    | "avg"
                    | "count"
                    | "sum"
                    | "min"
                    | "max"
                    | "first"
                    | "last"
                    | "coalesce"
                    | "nullif"
                    | "sin"
                    | "repeat"
                    | "error"
                    | "lower"
                    | "upper"
                    | "length"
                    | "current_setting"
                    | "floor"
                    | "ceil"
                    | "round"
                    | "date_trunc"
                    | "to_timestamp"
                    | "row_number"
                    | "rank"
                    | "dense_rank"
                    | "lag"
                    | "lead"
                    | "first_value"
                    | "last_value"
            )
        })
    }

    struct Eligibility<'a>(&'a QueryCatalog);
    impl Visitor for Eligibility<'_> {
        type Break = ();

        fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<()> {
            match statement {
                Statement::Query(_) | Statement::Explain { .. } => ControlFlow::Continue(()),
                _ => ControlFlow::Break(()),
            }
        }

        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
            // Keep unfamiliar grammar fresh, even when sqlparser can parse it.
            let SetExpr::Select(select) = query.body.as_ref() else {
                return ControlFlow::Break(());
            };
            if !query.locks.is_empty()
                || query.for_clause.is_some()
                || query.settings.is_some()
                || query.format_clause.is_some()
                || !query.pipe_operators.is_empty()
                || select.optimizer_hint.is_some()
                || select.select_modifiers.is_some()
                || select.top.is_some()
                || select.top_before_distinct
                || select.exclude.is_some()
                || select.into.is_some()
                || !select.lateral_views.is_empty()
                || select.prewhere.is_some()
                || !select.connect_by.is_empty()
                || !select.cluster_by.is_empty()
                || !select.distribute_by.is_empty()
                || !select.sort_by.is_empty()
                || select.value_table_mode.is_some()
                || select.flavor != SelectFlavor::Standard
            {
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }

        fn pre_visit_table_factor(&mut self, table: &TableFactor) -> ControlFlow<()> {
            match table {
                TableFactor::Table {
                    name,
                    args,
                    with_hints,
                    version,
                    with_ordinality,
                    partitions,
                    json_path,
                    sample,
                    index_hints,
                    ..
                } if with_hints.is_empty()
                    && version.is_none()
                    && !with_ordinality
                    && partitions.is_empty()
                    && json_path.is_none()
                    && sample.is_none()
                    && index_hints.is_empty() =>
                {
                    let Some(name) = simple_name(name) else {
                        return ControlFlow::Break(());
                    };
                    if let Some(args) = args {
                        if args.settings.is_some() {
                            return ControlFlow::Break(());
                        }
                        let builtin = matches!(
                            name.to_ascii_lowercase().as_str(),
                            "range" | "duckdb_tables" | "duckdb_functions"
                        );
                        // Only our generated zero-argument table macros qualify.
                        // prepare/build_query validates the complete catalog before
                        // any SQL executes; callers cannot supply macro SQL bodies.
                        let generated = args.args.is_empty()
                            && self
                                .0
                                .relations
                                .iter()
                                .any(|relation| relation.name.eq_ignore_ascii_case(name));
                        if !builtin && !generated {
                            return ControlFlow::Break(());
                        }
                    }
                }
                TableFactor::Derived { sample: None, .. } => {}
                _ => return ControlFlow::Break(()),
            }
            ControlFlow::Continue(())
        }

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            match expr {
                Expr::Function(function)
                    if scalar(&function.name)
                        && !function.uses_odbc_syntax
                        && matches!(function.parameters, FunctionArguments::None)
                        && matches!(&function.args, FunctionArguments::List(args) if args.clauses.is_empty()) =>
                    {}
                Expr::BinaryOp {
                    op:
                        BinaryOperator::Plus
                        | BinaryOperator::Minus
                        | BinaryOperator::Multiply
                        | BinaryOperator::Divide
                        | BinaryOperator::Modulo
                        | BinaryOperator::StringConcat
                        | BinaryOperator::Eq
                        | BinaryOperator::NotEq
                        | BinaryOperator::Gt
                        | BinaryOperator::Lt
                        | BinaryOperator::GtEq
                        | BinaryOperator::LtEq
                        | BinaryOperator::And
                        | BinaryOperator::Or,
                    ..
                } => {}
                Expr::UnaryOp {
                    op: UnaryOperator::Plus | UnaryOperator::Minus | UnaryOperator::Not,
                    ..
                } => {}
                Expr::Identifier(_)
                | Expr::CompoundIdentifier(_)
                | Expr::Value(_)
                | Expr::Nested(_)
                | Expr::Cast { .. }
                | Expr::Case { .. }
                | Expr::IsNull(_)
                | Expr::IsNotNull(_)
                | Expr::IsTrue(_)
                | Expr::IsFalse(_)
                | Expr::IsNotTrue(_)
                | Expr::IsNotFalse(_)
                | Expr::IsDistinctFrom(..)
                | Expr::IsNotDistinctFrom(..)
                | Expr::InList { .. }
                | Expr::InSubquery { .. }
                | Expr::Between { .. }
                | Expr::Exists { .. }
                | Expr::Subquery(_) => {}
                _ => return ControlFlow::Break(()),
            }
            // The visitor traverses arguments, filters, windows and subqueries too.
            ControlFlow::Continue(())
        }
    }

    let Ok(statements) = Parser::parse_sql(&DuckDbDialect {}, sql) else {
        return false;
    };
    statements.len() == 1 && statements.visit(&mut Eligibility(catalog)).is_continue()
}

fn validate_read_only_statement(sql: &str) -> Result<()> {
    ensure!(
        sql.len() <= MAX_QUERY_SCRIPT_BYTES,
        "DuckDB SQL script exceeds {} bytes",
        MAX_QUERY_SCRIPT_BYTES
    );
    ensure!(!sql.contains('\0'), "SQL contains NUL");
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
    fn pooling_requires_known_parsed_functions_and_grammar() {
        let catalog = QueryCatalog {
            relations: vec![CatalogRelation {
                name: "quoted\"catalog".into(),
                columns: vec![("value".into(), "DOUBLE".into())],
                rows: vec![],
            }],
            aggregates: vec![],
        };
        for sql in [
            "SELECT 1",
            "SELECT \"SuM\"(value) FROM measurements",
            "WITH x AS (SELECT 1 AS n) SELECT count(*) FROM x",
            "EXPLAIN ANALYZE SELECT abs(-1)",
            "SELECT * FROM \"quoted\"\"catalog\"()",
            "SELECT 'setseed(0.25)' AS inert_text",
            "SELECT date_trunc('minute', to_timestamp(timestamp_us/1000000.0)) FROM measurements",
            "SELECT row_number() OVER (ORDER BY value), lag(value) OVER (ORDER BY value) FROM measurements",
        ] {
            assert!(pooling_eligible(sql, &catalog), "not pooled: {sql}");
        }
        for sql in [
            "SELECT setseed(0.25)",
            "SELECT \"setseed\"(0.25)",
            "SELECT \"SeTsEeD\" /* comment */ (0.25)",
            "SELECT * FROM query('SELECT setseed(0.25)')",
            "SELECT * FROM \"query\"($$SELECT setseed(0.25)$$)",
            "SELECT random()",
            "SELECT mystery_function(1)",
            "SELECT main.abs(1)",
            "SELECT * FROM unknown_macro()",
            "SELECT abs((SELECT setseed(0.25)))",
            "SELECT count(*) FILTER (WHERE setseed(0.25) IS NULL)",
            "SELECT sum(1) OVER (ORDER BY setseed(0.25))",
            "SELECT row_number() OVER (ORDER BY setseed(0.25))",
            "SELECT lag(setseed(0.25)) OVER (ORDER BY 1)",
            "SELECT * FROM range(CAST(setseed(0.25) AS BIGINT))",
            "WITH x AS (SELECT setseed(0.25)) SELECT * FROM x",
            "SELECT 1 UNION ALL SELECT 2",
            "SELECT unparsed !!! syntax",
            "SELECT * FROM measurements USING SAMPLE 1 ROWS",
            "SELECT * FROM \"quoted\"\"catalog\"(setseed(0.25))",
        ] {
            assert!(
                !pooling_eligible(sql, &catalog),
                "incorrectly pooled: {sql}"
            );
        }
    }

    #[test]
    fn query_input_writer_checks_bound_before_appending() {
        let mut bytes = Vec::new();
        let mut writer = BoundedInput(&mut bytes, 4);
        writer.write_all(b"1234").unwrap();
        assert!(writer.write_all(b"5").is_err());
        assert_eq!(bytes, b"1234");
        assert!(validate_read_only_statement("SELECT '\0'").is_err());
    }

    #[test]
    fn payload_free_setup_has_no_scanner_or_stdin_allowance() {
        let worker = tempfile::tempdir().unwrap();
        let mut tables = vec![QueryTable {
            name: "metrics".into(),
            hot: vec![],
            files: vec![],
            rollups: vec![],
            cutoff_us: None,
        }];
        let catalog = QueryCatalog {
            relations: vec![CatalogRelation {
                name: "metadata".into(),
                columns: vec![("name".into(), "VARCHAR".into())],
                rows: vec![],
            }],
            aggregates: vec![AggregateAlias {
                name: "minute".into(),
                source: "metrics".into(),
                width_us: 60,
            }],
        };
        for disk in [false, true] {
            if disk {
                tables[0].files.push(worker.path().join("selected.parquet"));
            }
            let (sql, input) = build_query(
                &tables,
                "SELECT 42",
                &QueryOptions::default(),
                &catalog,
                worker.path(),
            )
            .unwrap();
            assert!(input.is_empty());
            assert!(!sql.contains("read_json("));
            assert!(!sql.contains("/dev/stdin"));
            assert!(!sql.contains("sentinel"));
            assert!(sql.contains("SET lock_configuration = true"));
            for (name, kind) in INPUT_COLUMNS {
                assert!(sql.contains(&format!("{} {kind}", quote_identifier(name))));
            }
            assert_eq!(sql.contains("read_parquet("), disk);
        }
    }

    #[test]
    fn catalog_literal_budget_and_original_script_headroom() {
        let worker = tempfile::tempdir().unwrap();
        let options = QueryOptions::default();
        let mut catalog = QueryCatalog {
            relations: vec![CatalogRelation {
                name: "metadata".into(),
                columns: vec![("text".into(), "VARCHAR".into())],
                rows: vec![json!(["x".repeat(16 * 1024)])],
            }],
            aggregates: vec![],
        };
        assert!(catalog_literals(&catalog).unwrap().unwrap().len() <= MAX_CATALOG_SQL_BYTES);
        let (legacy, _) =
            build_query_mode(&[], "", &options, &catalog, worker.path(), false).unwrap();
        let query = format!(
            "SELECT 42 AS answer /*{}*/",
            "x".repeat(MAX_QUERY_SCRIPT_BYTES - legacy.len() - 128)
        );
        let (expected, expected_input) =
            build_query_mode(&[], &query, &options, &catalog, worker.path(), false).unwrap();
        assert!(expected.len() <= MAX_QUERY_SCRIPT_BYTES);
        let (actual, input) = build_query(&[], &query, &options, &catalog, worker.path()).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(input, expected_input);
        assert_eq!(
            execute_with_catalog(&[], &query, &options, &catalog).unwrap(),
            json!([{"answer": 42}])
        );
        // Escaping expands this source under 32KiB past the encoded budget.
        catalog.relations[0].rows = vec![json!(["'".repeat(17 * 1024)])];
        assert!(catalog_literals(&catalog).unwrap().is_none());
        catalog.relations[0].rows = vec![json!(["a\0b"])];
        assert!(catalog_literals(&catalog).unwrap().is_none());
    }

    #[test]
    fn typed_records_preserve_legacy_fields_without_sentinel() {
        use crate::model::Row;
        let stored = StoredRow {
            row: Row {
                timestamp_us: i64::MIN,
                tenant: "tenant雪".into(),
                series: "cpu\"\\\n".into(),
                value: f64::MIN_POSITIVE,
                tags: std::collections::BTreeMap::from([("\"\\\n雪".into(), "'\t".into())]),
            },
            sequence: u64::MAX,
            ordinal: u32::MAX,
        };
        let rollup = RollupRow::from_row(1, &stored).unwrap();
        let mut table = QueryTable {
            name: "metrics".into(),
            hot: vec![stored.clone()],
            files: vec![],
            rollups: vec![rollup.clone()],
            cutoff_us: None,
        };
        let worker = tempfile::tempdir().unwrap();
        let (sql, input) = build_query_mode(
            std::slice::from_ref(&table),
            "SELECT 1",
            &QueryOptions::default(),
            &QueryCatalog::default(),
            worker.path(),
            false,
        )
        .unwrap();
        assert!(sql.contains("read_json('/dev/stdin'"));
        let lines: Vec<Value> = input
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect();
        let mut hot = serde_json::to_value(&stored.row).unwrap();
        hot["kind"] = json!("hot");
        hot["table_name"] = json!("metrics");
        hot["tags"] = json!(serde_json::to_string(&stored.row.tags).unwrap());
        hot["sequence"] = json!(stored.sequence);
        hot["ordinal"] = json!(stored.ordinal);
        let mut expected_rollup = serde_json::to_value(&rollup).unwrap();
        expected_rollup["kind"] = json!("rollup");
        expected_rollup["table_name"] = json!("metrics");
        expected_rollup["tags"] = json!(serde_json::to_string(&rollup.tags).unwrap());
        assert_eq!(lines, vec![hot, expected_rollup]);
        table.hot[0].row.value = f64::NAN;
        assert!(append_table_input(&mut vec![], &table).is_err());
        table.hot.clear();
        table.rollups[0].sum = f64::INFINITY;
        assert!(append_table_input(&mut vec![], &table).is_err());
    }

    #[test]
    fn small_named_aggregate_setup_is_typed_and_bounds_are_snapshot_wide() {
        let worker = tempfile::tempdir().unwrap();
        let stored = StoredRow {
            row: crate::model::Row {
                timestamp_us: 0,
                tenant: "t".into(),
                series: "s".into(),
                value: -0.0,
                tags: Default::default(),
            },
            sequence: u64::MAX,
            ordinal: u32::MAX,
        };
        let mut tables = vec![QueryTable {
            name: "metrics".into(),
            hot: vec![stored.clone()],
            files: vec![],
            rollups: vec![RollupRow::from_row(1, &stored).unwrap()],
            cutoff_us: Some(0),
        }];
        let mut catalog = QueryCatalog {
            relations: vec![CatalogRelation {
                name: "metadata".into(),
                columns: vec![("text".into(), "VARCHAR".into())],
                rows: vec![json!(["雪'"])],
            }],
            aggregates: vec![AggregateAlias {
                name: "minute".into(),
                source: "metrics".into(),
                width_us: 1,
            }],
        };
        let options = QueryOptions::default();
        for query in [
            "SELECT * FROM minute",
            "SELECT count(*) FROM metrics",
            "WITH a AS (SELECT * FROM minute) SELECT * FROM a",
        ] {
            let (script, input) =
                build_query(&tables, query, &options, &catalog, worker.path()).unwrap();
            assert!(input.is_empty());
            assert!(!script.contains("read_json("));
            assert!(!script.contains("/dev/stdin"));
            assert!(script.contains("INSERT INTO __varve_input VALUES"));
            assert!(script.contains("CREATE TEMP VIEW \"minute\""));
            assert!(script.contains("CAST('18446744073709551615' AS UBIGINT)"));
            assert!(script.contains("CAST('-0' AS DOUBLE)"));
            assert!(script.contains("SET allowed_paths = [];"));
            assert!(script.contains("SET lock_configuration = true"));
        }
        // Selected aggregate-only snapshots also need no raw rows or scanner.
        tables[0].hot.clear();
        let (script, input) = build_query(
            &tables,
            "SELECT * FROM minute",
            &options,
            &catalog,
            worker.path(),
        )
        .unwrap();
        assert!(input.is_empty());
        assert!(!script.contains("read_json("));
        tables[0].rollups.clear();
        catalog.relations[0].rows = vec![json!([""]); MAX_TYPED_INPUT_ROWS];
        assert!(typed_relations(&tables, &catalog).unwrap().is_some());
        tables[0].hot.push(stored);
        assert!(typed_relations(&tables, &catalog).unwrap().is_none());
        let (script, input) =
            build_query(&tables, "SELECT 1", &options, &catalog, worker.path()).unwrap();
        assert!(script.contains("read_json('/dev/stdin'"));
        assert_eq!(
            input.iter().filter(|&&b| b == b'\n').count(),
            MAX_TYPED_INPUT_ROWS + 1
        );
        for text in ["'".repeat(17 * 1024), "nul\0value".into()] {
            catalog.relations[0].rows = vec![json!([text])];
            assert!(typed_relations(&tables, &catalog).unwrap().is_none());
        }
        catalog.relations[0].rows = vec![json!(["x".repeat(16 * 1024)])];
        tables[0].hot[0].row.tags = (0..16)
            .map(|i| (format!("tag{i}"), "x".repeat(1024)))
            .collect();
        assert!(catalog_literals(&catalog).unwrap().is_some());
        assert!(
            typed_relations(&tables, &QueryCatalog::default())
                .unwrap()
                .is_some()
        );
        assert!(typed_relations(&tables, &catalog).unwrap().is_none());
        catalog.relations.clear();
        tables[0].hot[0].row.tags = (0..32)
            .map(|i| (format!("tag{i}"), "'".repeat(1024)))
            .collect();
        assert!(typed_relations(&tables, &catalog).unwrap().is_none());
        let (script, input) =
            build_query(&tables, "SELECT 1", &options, &catalog, worker.path()).unwrap();
        assert!(script.contains("read_json("));
        assert!(!input.is_empty());
    }

    #[test]
    fn typed_sql_checks_encoded_bound_before_each_append() {
        let mut sql = TypedSql(String::new());
        sql.push(&"x".repeat(MAX_TYPED_SQL_BYTES)).unwrap();
        assert!(sql.push("雪").is_err());
        assert_eq!(sql.0.len(), MAX_TYPED_SQL_BYTES);
        let mut sql = TypedSql(String::new());
        assert!(
            sql.literal(&"'".repeat(MAX_TYPED_SQL_BYTES / 2), "VARCHAR")
                .is_err()
        );
        assert!(sql.0.len() <= MAX_TYPED_SQL_BYTES);
        assert!(
            typed_tags(&std::collections::BTreeMap::from([(
                "key".into(),
                "x".repeat(MAX_TYPED_SQL_BYTES)
            )]))
            .is_none()
        );
    }

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
