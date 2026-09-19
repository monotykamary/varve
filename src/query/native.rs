mod ffi;
mod output;
mod scan;
#[cfg(test)]
mod startup;

use super::{QueryCatalog, QueryOptions, QueryTable, ResidentSnapshot};
use crate::model::validate_name;
use crate::raw_memory::RawMemoryBudget;
use anyhow::{Context, Result, bail, ensure};
use ffi::{Api, DuckStr, Handle, OwnedHandle};
use scan::{CatalogData, Column, RawBatch, ScannerScratch, ScannerSpec, ScratchPlan};
use serde_json::Value;
use std::collections::{BTreeSet, HashSet};
use std::ffi::CString;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const POLL_INTERVAL: Duration = Duration::from_millis(2);
const STOP_NONE: u8 = 0;
const STOP_CANCELLED: u8 = 1;
const STOP_TIMEOUT: u8 = 2;

/// In-process DuckDB v2 runtime. Every execution gets a fresh private database.
/// The caller must retain selected-file pins until `execute` returns.
pub(crate) struct NativeRuntime {
    api: Arc<Api>,
    capacity: usize,
    active: AtomicUsize,
    #[cfg(test)]
    before_execute: Mutex<Option<BeforeExecuteHook>>,
    #[cfg(test)]
    last_scratch_usage: Mutex<Option<scan::ScratchUsage>>,
}

#[cfg(test)]
type BeforeExecuteHook = Box<dyn FnOnce() + Send>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NativeIdentity {
    pub library_path: PathBuf,
    pub library_sha256: String,
    pub header_sha256: &'static str,
    pub version: String,
}

struct NativeSession {
    connection: OwnedHandle,
    _database: OwnedHandle,
    _environment: OwnedHandle,
}

impl NativeRuntime {
    pub(crate) fn new(library_path: &Path, capacity: usize) -> Result<Self> {
        ensure!(capacity > 0, "native query capacity must be positive");
        let api = ffi::load(library_path)?;
        Ok(Self {
            api,
            capacity,
            active: AtomicUsize::new(0),
            #[cfg(test)]
            before_execute: Mutex::new(None),
            #[cfg(test)]
            last_scratch_usage: Mutex::new(None),
        })
    }

    pub(crate) fn active_queries(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }

    pub(crate) fn identity(&self) -> NativeIdentity {
        NativeIdentity {
            library_path: self.api.path.clone(),
            library_sha256: self.api.digest.clone(),
            header_sha256: ffi::HEADER_SHA256,
            version: self.api.version.clone(),
        }
    }

    fn create_open_option(&self, name: &str, setting: &str) -> Result<OwnedHandle> {
        let mut option = OwnedHandle::new(
            Arc::clone(&self.api),
            ptr::null_mut(),
            self.api.option_destroy,
        );
        let mut error = ptr::null_mut();
        let code = unsafe {
            (self.api.option_create)(
                DuckStr::from_bytes(name.as_bytes()),
                DuckStr::from_bytes(setting.as_bytes()),
                option.slot(),
                &mut error,
            )
        };
        unsafe {
            self.api
                .check(code, &mut error, &format!("create DuckDB option {name}"))?
        };
        ensure!(!option.is_null(), "DuckDB v2 returned a null {name} option");
        Ok(option)
    }

    fn open_private_session(&self, options: &QueryOptions) -> Result<NativeSession> {
        let mut error = ptr::null_mut();
        let mut environment = OwnedHandle::new(
            Arc::clone(&self.api),
            ptr::null_mut(),
            self.api.destroy_environment,
        );
        let code = unsafe { (self.api.create_environment)(environment.slot(), &mut error) };
        unsafe {
            self.api
                .check(code, &mut error, "create private DuckDB v2 environment")?
        };
        ensure!(
            !environment.is_null(),
            "DuckDB v2 returned a null environment"
        );

        let open_options = [
            self.create_open_option("threads", &options.threads.to_string())?,
            self.create_open_option("memory_limit", &format!("{}MB", options.memory_mb))?,
        ];
        let mut option_handles = open_options
            .iter()
            .map(OwnedHandle::get)
            .collect::<Vec<_>>();
        let mut database = OwnedHandle::new(Arc::clone(&self.api), ptr::null_mut(), self.api.close);
        let memory = DuckStr::from_bytes(b":memory:");
        let code = unsafe {
            (self.api.open)(
                environment.get(),
                memory,
                option_handles.as_mut_ptr(),
                option_handles.len(),
                database.slot(),
                &mut error,
            )
        };
        unsafe {
            self.api
                .check(code, &mut error, "open private native DuckDB database")?
        };
        ensure!(!database.is_null(), "DuckDB v2 returned a null database");

        let mut connection =
            OwnedHandle::new(Arc::clone(&self.api), ptr::null_mut(), self.api.disconnect);
        let code = unsafe { (self.api.connect)(database.get(), connection.slot(), &mut error) };
        unsafe {
            self.api
                .check(code, &mut error, "connect private native DuckDB database")?
        };
        ensure!(
            !connection.is_null(),
            "DuckDB v2 returned a null connection"
        );
        Ok(NativeSession {
            connection,
            _database: database,
            _environment: environment,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn execute(
        &self,
        tables: &[QueryTable],
        snapshot: Option<&ResidentSnapshot>,
        sql: &str,
        options: &QueryOptions,
        catalog: &QueryCatalog,
        raw_memory: &RawMemoryBudget,
        cancelled: &AtomicBool,
    ) -> Result<Value> {
        let _admission = self.admit()?;
        super::validate_options(options)?;
        super::validate_read_only_statement(sql)?;
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(options.timeout_ms))
            .context("query timeout is too large")?;
        check_deadline(deadline, cancelled)?;
        let scratch_lease = ScannerScratch::reserve(
            ScratchPlan::for_query(tables, snapshot, catalog, options.threads)?,
            raw_memory,
        )?;
        // This stack owner predates prepared/session and survives the FINAL
        // scratch Arc deallocation, including every early return and unwind.
        let scratch = scratch_lease.as_ref().map(|lease| lease.scratch());
        let prepared = PreparedQuery::new(
            tables,
            snapshot,
            catalog,
            options.threads,
            scratch,
            deadline,
            cancelled,
        )?;
        check_deadline(deadline, cancelled)?;

        // Keep opaque environment/database handles query-private. The pinned
        // API explicitly permits cross-thread interrupt, not shared opens.
        let session = self.open_private_session(options)?;
        let connection = session.connection.get();

        let abort = Arc::new(AtomicBool::new(false));
        let reason = Arc::new(AtomicU8::new(STOP_NONE));
        let done = Arc::new((Mutex::new(false), Condvar::new()));
        let result = thread::scope(|scope| {
            let watcher = {
                let api = Arc::clone(&self.api);
                let abort = Arc::clone(&abort);
                let reason = Arc::clone(&reason);
                let done = Arc::clone(&done);
                let connection = connection as usize;
                scope.spawn(move || {
                    cancellation_watch(
                        &api,
                        connection as Handle,
                        deadline,
                        cancelled,
                        &abort,
                        &reason,
                        &done,
                    )
                })
            };
            let completion = WatchCompletion(&done);
            // SAFETY: prepared borrows this execute call's inputs. Connection,
            // database and environment guards are destroyed before prepared,
            // after the watcher and every callback have finished.
            let execution = unsafe {
                self.configure_and_execute(
                    connection,
                    &prepared,
                    sql,
                    options,
                    deadline,
                    cancelled,
                    Arc::clone(&abort),
                )
            };
            drop(completion);
            let watcher_result = watcher.join();
            ensure!(
                watcher_result.is_ok(),
                "native DuckDB cancellation watcher panicked"
            );
            match reason.load(Ordering::Acquire) {
                STOP_CANCELLED => bail!("DuckDB query cancelled"),
                STOP_TIMEOUT => bail!("DuckDB query timed out"),
                _ => execution,
            }
        });
        // The watcher is joined and every result/chunk is gone before teardown.
        // Closing the private session destroys every dynamically leased callback
        // owner before PreparedQuery and the fixed scanner lease are released.
        drop(session);
        #[cfg(test)]
        let scratch_usage = scratch.as_ref().map(|scratch| scratch.usage());
        drop(prepared);
        drop(scratch_lease);
        #[cfg(test)]
        {
            *self
                .last_scratch_usage
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = scratch_usage;
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    /// SAFETY: close the connection/database before any prepared source expires.
    unsafe fn configure_and_execute(
        &self,
        connection: Handle,
        prepared: &PreparedQuery<'_>,
        sql: &str,
        options: &QueryOptions,
        deadline: Instant,
        cancelled: &AtomicBool,
        abort: Arc<AtomicBool>,
    ) -> Result<Value> {
        let statements = settings_sql(prepared)?;
        let mut generated_bytes = sql.len();
        for statement in &statements {
            generated_bytes = generated_bytes
                .checked_add(statement.len())
                .context("native query SQL length overflow")?;
            ensure!(
                generated_bytes <= super::MAX_QUERY_SCRIPT_BYTES,
                "DuckDB SQL script exceeds {} bytes",
                super::MAX_QUERY_SCRIPT_BYTES
            );
            execute_no_rows(&self.api, connection, statement)?;
            check_deadline(deadline, cancelled)?;
        }

        // SAFETY: inherited from this function's scoped-teardown contract.
        unsafe {
            for table in &prepared.tables {
                scan::register(
                    &self.api,
                    connection,
                    &table.raw_function,
                    Arc::clone(&table.raw),
                    Arc::clone(&abort),
                )?;
                scan::register(
                    &self.api,
                    connection,
                    &table.rollup_function,
                    Arc::clone(&table.rollup),
                    Arc::clone(&abort),
                )?;
            }
            for relation in &prepared.catalogs {
                scan::register(
                    &self.api,
                    connection,
                    &relation.function,
                    Arc::clone(&relation.scanner),
                    Arc::clone(&abort),
                )?;
            }
        }

        for statement in prepared.relation_sql()? {
            generated_bytes = generated_bytes
                .checked_add(statement.len())
                .context("native relation SQL length overflow")?;
            ensure!(
                generated_bytes <= super::MAX_QUERY_SCRIPT_BYTES,
                "DuckDB SQL script exceeds {} bytes",
                super::MAX_QUERY_SCRIPT_BYTES
            );
            execute_no_rows(&self.api, connection, &statement)?;
            check_deadline(deadline, cancelled)?;
        }
        execute_no_rows(&self.api, connection, "SET lock_configuration = true")?;
        check_deadline(deadline, cancelled)?;

        let explain = super::lex_sql(sql)?
            .first()
            .is_some_and(|token| token == "EXPLAIN");
        #[cfg(test)]
        {
            let hook = self
                .before_execute
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take();
            if let Some(hook) = hook {
                hook();
            }
        }
        let mut result = execute_statement(&self.api, connection, sql)?;
        output::consume(
            &self.api,
            connection,
            &mut result,
            explain,
            options.max_output_bytes,
            &abort,
        )
    }

    fn admit(&self) -> Result<Admission<'_>> {
        let mut active = self.active.load(Ordering::Acquire);
        loop {
            ensure!(active < self.capacity, "query worker capacity exhausted");
            match self.active.compare_exchange_weak(
                active,
                active + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(Admission { runtime: self }),
                Err(observed) => active = observed,
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn last_scratch_usage(&self) -> Option<scan::ScratchUsage> {
        *self
            .last_scratch_usage
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    #[cfg(test)]
    pub(crate) fn set_before_execute_hook(&self, hook: impl FnOnce() + Send + 'static) {
        *self
            .before_execute
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(Box::new(hook));
    }

    #[cfg(test)]
    pub(crate) fn inject_callback_failure(&self, stage: u8) -> impl Drop {
        scan::inject_callback_failure(stage)
    }

    #[cfg(test)]
    pub(crate) fn live_callback_owners(&self) -> usize {
        scan::live_owners()
    }
}

struct Admission<'a> {
    runtime: &'a NativeRuntime,
}

impl Drop for Admission<'_> {
    fn drop(&mut self) {
        self.runtime.active.fetch_sub(1, Ordering::AcqRel);
    }
}

struct PreparedQuery<'a> {
    tables: Vec<PreparedTable<'a>>,
    catalogs: Vec<PreparedCatalog<'a>>,
    aggregates: Vec<super::AggregateAlias>,
    _scratch: Option<Arc<ScannerScratch>>,
}

struct PreparedTable<'a> {
    name: String,
    files: Vec<PathBuf>,
    cutoff_us: Option<i64>,
    raw_function: String,
    rollup_function: String,
    raw: Arc<ScannerSpec<'a>>,
    rollup: Arc<ScannerSpec<'a>>,
}

struct PreparedCatalog<'a> {
    name: String,
    function: String,
    scanner: Arc<ScannerSpec<'a>>,
}

impl<'a> PreparedQuery<'a> {
    fn new(
        tables: &'a [QueryTable],
        snapshot: Option<&'a ResidentSnapshot>,
        catalog: &'a QueryCatalog,
        threads: usize,
        scratch: Option<&Arc<ScannerScratch>>,
        deadline: Instant,
        cancelled: &AtomicBool,
    ) -> Result<Self> {
        let mut table_names = HashSet::new();
        let mut exposed = HashSet::new();
        for table in tables {
            validate_name(&table.name).context("validate query table name")?;
            ensure!(
                table_names.insert(table.name.to_ascii_lowercase()),
                "duplicate query table {}",
                table.name
            );
            exposed.insert(table.name.to_ascii_lowercase());
            exposed.insert(format!("{}__rollup", table.name).to_ascii_lowercase());
        }
        super::validate_catalog(catalog, tables, &mut exposed)?;
        if let Some(snapshot) = snapshot {
            validate_snapshot(tables, snapshot, deadline, cancelled)?;
        }

        let mut prepared_tables = Vec::with_capacity(tables.len());
        for (index, table) in tables.iter().enumerate() {
            check_deadline(deadline, cancelled)?;
            let batches: Box<[RawBatch<'_>]> = if let Some(snapshot) = snapshot {
                let resident = snapshot
                    .tables
                    .iter()
                    .find(|resident| resident.name == table.name)
                    .context("resident table disappeared after validation")?;
                scan::exact_slice(resident.batches.len(), |index| {
                    let batch = &resident.batches[index];
                    validate_native_rows(batch.rows.as_ref(), deadline, cancelled)?;
                    Ok(RawBatch::shared(batch.rows.clone()))
                })?
            } else if table.hot.is_empty() {
                Box::new([])
            } else {
                validate_native_rows(&table.hot, deadline, cancelled)?;
                // This borrow is pinned by the synchronous execute frame and is
                // released only after connection/database callback teardown.
                Box::new([RawBatch::borrowed(&table.hot)])
            };
            for rollup in &table.rollups {
                ensure!(
                    [
                        rollup.sum,
                        rollup.min,
                        rollup.max,
                        rollup.first,
                        rollup.last
                    ]
                    .iter()
                    .all(|value| value.is_finite()),
                    "rollup values must be finite"
                );
            }
            for path in &table.files {
                ensure!(path.is_absolute(), "native Parquet path must be absolute");
                ensure!(
                    path.to_str().is_some_and(|path| !path.contains('\0')),
                    "Parquet path is not valid UTF-8 or contains NUL"
                );
            }
            prepared_tables.push(PreparedTable {
                name: table.name.clone(),
                files: table.files.clone(),
                cutoff_us: table.cutoff_us,
                raw_function: format!("__varve_raw_scan_{index}"),
                rollup_function: format!("__varve_rollup_scan_{index}"),
                raw: ScannerSpec::raw(
                    batches,
                    threads,
                    Arc::clone(scratch.context("native scanner scratch is missing")?),
                )?,
                rollup: ScannerSpec::rollup(
                    &table.rollups,
                    threads,
                    Arc::clone(scratch.context("native scanner scratch is missing")?),
                )?,
            });
        }

        let mut catalogs = Vec::with_capacity(catalog.relations.len());
        for (index, relation) in catalog.relations.iter().enumerate() {
            check_deadline(deadline, cancelled)?;
            let columns = scan::exact_slice(relation.columns.len(), |index| {
                let (name, kind) = &relation.columns[index];
                Ok(Column {
                    name: name.as_str().into(),
                    kind: scan::catalog_type(kind)?,
                })
            })?;
            for row in &relation.rows {
                check_deadline(deadline, cancelled)?;
                super::catalog_row_values(relation, row)?;
            }
            let data = CatalogData::borrowed(relation, columns);
            catalogs.push(PreparedCatalog {
                name: relation.name.clone(),
                function: format!("__varve_catalog_scan_{index}"),
                scanner: ScannerSpec::catalog(
                    data,
                    threads,
                    Arc::clone(scratch.context("native scanner scratch is missing")?),
                )?,
            });
        }
        Ok(Self {
            tables: prepared_tables,
            catalogs,
            aggregates: catalog.aggregates.clone(),
            _scratch: scratch.cloned(),
        })
    }

    fn relation_sql(&self) -> Result<Vec<String>> {
        let mut statements =
            Vec::with_capacity(self.tables.len() * 2 + self.catalogs.len() + self.aggregates.len());
        for table in &self.tables {
            let name = super::quote_identifier(&table.name);
            let raw = super::quote_identifier(&table.raw_function);
            let mut sql = format!(
                "CREATE TEMP VIEW {name} AS SELECT timestamp_us, tenant, series, value, tags, sequence, ordinal FROM (SELECT timestamp_us, tenant, series, value, tags, sequence, ordinal FROM {raw}()"
            );
            if let Some(cutoff) = table.cutoff_us {
                sql.push_str(&format!(" WHERE timestamp_us >= {cutoff}"));
            }
            if !table.files.is_empty() {
                sql.push_str(" UNION ALL SELECT timestamp_us, tenant, series, value, tags, sequence, ordinal FROM read_parquet([");
                for (index, path) in table.files.iter().enumerate() {
                    if index > 0 {
                        sql.push(',');
                    }
                    sql.push_str(&super::quote_path(path)?);
                }
                sql.push_str("])");
                if let Some(cutoff) = table.cutoff_us {
                    sql.push_str(&format!(" WHERE timestamp_us >= {cutoff}"));
                }
            }
            sql.push(')');
            statements.push(sql);

            let rollup_name = super::quote_identifier(&format!("{}__rollup", table.name));
            let rollup = super::quote_identifier(&table.rollup_function);
            statements.push(format!(
                "CREATE TEMP VIEW {rollup_name} AS SELECT width_us, bucket_us, tenant, series, tags, count, sum, sum / nullif(count, 0) AS average, min, max, first, last, first AS open, max AS high, min AS low, last AS close, first_timestamp_us, last_timestamp_us, first_sequence, first_ordinal, last_sequence, last_ordinal FROM {rollup}()"
            ));
        }
        for relation in &self.catalogs {
            statements.push(format!(
                "CREATE TEMP MACRO {}() AS TABLE SELECT * FROM {}()",
                super::quote_identifier(&relation.name),
                super::quote_identifier(&relation.function),
            ));
        }
        for alias in &self.aggregates {
            let source = self
                .tables
                .iter()
                .find(|table| table.name.eq_ignore_ascii_case(&alias.source))
                .context("aggregate source disappeared after validation")?;
            statements.push(format!(
                "CREATE TEMP VIEW {} AS SELECT * REPLACE (CAST(count AS BIGINT) AS count) FROM {} WHERE width_us = {}",
                super::quote_identifier(&alias.name),
                super::quote_identifier(&format!("{}__rollup", source.name)),
                alias.width_us,
            ));
        }
        Ok(statements)
    }
}

fn validate_native_rows(
    rows: &[crate::model::StoredRow],
    deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<()> {
    for row in rows {
        check_deadline(deadline, cancelled)?;
        row.row
            .validate()
            .context("validate native hot query row")?;
    }
    Ok(())
}

fn validate_snapshot(
    tables: &[QueryTable],
    snapshot: &ResidentSnapshot,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<()> {
    ensure!(
        tables.iter().all(|table| table.hot.is_empty()),
        "resident query requires empty QueryTable.hot"
    );
    ensure!(
        tables.len() == snapshot.tables.len(),
        "resident table scope mismatch"
    );
    let selected = tables
        .iter()
        .map(|table| table.name.as_str())
        .collect::<BTreeSet<_>>();
    ensure!(
        selected.len() == tables.len(),
        "resident table scope mismatch"
    );

    let mut lineage_names = BTreeSet::new();
    let mut lineage = Vec::with_capacity(snapshot.lineage.len());
    for table in &snapshot.lineage {
        check_deadline(deadline, cancelled)?;
        ensure!(
            lineage_names.insert(table.name.as_str()),
            "duplicate resident lineage table"
        );
        let ids = table
            .ids
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        ensure!(
            ids.len() == table.ids.len(),
            "duplicate resident lineage identity"
        );
        lineage.push((table.name.as_str(), ids));
    }

    let mut resident_names = BTreeSet::new();
    for resident in &snapshot.tables {
        check_deadline(deadline, cancelled)?;
        ensure!(
            selected.contains(resident.name.as_str())
                && resident_names.insert(resident.name.as_str()),
            "resident table scope mismatch"
        );
        let query = tables
            .iter()
            .find(|table| table.name == resident.name)
            .context("resident selected table is missing")?;
        let selected_paths = resident
            .files
            .iter()
            .map(|file| &file.path)
            .collect::<BTreeSet<_>>();
        let query_paths = query.files.iter().collect::<BTreeSet<_>>();
        ensure!(
            selected_paths == query_paths
                && selected_paths.len() == resident.files.len()
                && query_paths.len() == query.files.len(),
            "resident selected file scope mismatch"
        );
        let live = lineage
            .iter()
            .find(|(name, _)| *name == resident.name)
            .map(|(_, ids)| ids)
            .context("resident selected table is missing lineage")?;
        let mut ids = BTreeSet::new();
        for batch in &resident.batches {
            ensure!(
                ids.insert(batch.id.as_str()),
                "duplicate resident batch identity"
            );
            ensure!(
                live.contains(batch.id.as_str()),
                "invalid resident batch identity"
            );
        }
        for file in &resident.files {
            ensure!(
                file.path.is_absolute(),
                "resident file path must be absolute"
            );
            ensure!(file.rows > 0, "resident file rows must be positive");
            ensure!(
                file.min_timestamp_us <= file.max_timestamp_us,
                "resident file timestamp bounds are invalid"
            );
            ensure!(
                ids.insert(file.id.as_str()),
                "duplicate resident batch identity"
            );
            ensure!(
                live.contains(file.id.as_str()),
                "invalid resident file identity"
            );
        }
    }
    Ok(())
}

fn settings_sql(prepared: &PreparedQuery) -> Result<Vec<String>> {
    let mut paths = Vec::new();
    for table in &prepared.tables {
        for path in &table.files {
            paths.push(super::quote_path(path)?);
        }
    }
    paths.sort();
    paths.dedup();
    Ok(vec![
        "SET max_temp_directory_size = '0B'".to_owned(),
        "SET preserve_insertion_order = false".to_owned(),
        "SET autoinstall_known_extensions = false".to_owned(),
        "SET autoload_known_extensions = false".to_owned(),
        "SET allow_community_extensions = false".to_owned(),
        "SET allow_unsigned_extensions = false".to_owned(),
        "SET allowed_directories = []".to_owned(),
        format!("SET allowed_paths = [{}]", paths.join(",")),
        "SET enable_external_access = false".to_owned(),
    ])
}

fn execute_no_rows(api: &Arc<Api>, connection: Handle, sql: &str) -> Result<()> {
    let result = execute_statement(api, connection, sql)?;
    let mut changed = 0;
    let mut error = ptr::null_mut();
    let code = unsafe { (api.result_drain)(result.get(), &mut changed, &mut error) };
    unsafe { api.check(code, &mut error, "execute native DuckDB setup statement")? };
    Ok(())
}

fn execute_statement(api: &Arc<Api>, connection: Handle, sql: &str) -> Result<OwnedHandle> {
    let sql = CString::new(sql).context("DuckDB SQL contains NUL")?;
    let mut error = ptr::null_mut();
    let mut iterator = ptr::null_mut();
    let code = unsafe { (api.parse_sql)(connection, sql.as_ptr(), &mut iterator, &mut error) };
    unsafe { api.check(code, &mut error, "parse native DuckDB SQL")? };
    let iterator = OwnedHandle::new(Arc::clone(api), iterator, api.statement_iterator_destroy);
    let mut statement = ptr::null_mut();
    let code = unsafe { (api.statement_iterator_next)(iterator.get(), &mut statement, &mut error) };
    unsafe { api.check(code, &mut error, "read native DuckDB SQL statement")? };
    ensure!(!statement.is_null(), "DuckDB SQL statement is empty");
    let statement = OwnedHandle::new(Arc::clone(api), statement, api.sql_statement_destroy);
    let mut extra = ptr::null_mut();
    let code = unsafe { (api.statement_iterator_next)(iterator.get(), &mut extra, &mut error) };
    unsafe { api.check(code, &mut error, "check native DuckDB SQL statement count")? };
    if !extra.is_null() {
        let _extra = OwnedHandle::new(Arc::clone(api), extra, api.sql_statement_destroy);
        bail!("native DuckDB SQL must contain exactly one statement");
    }
    let mut result = ptr::null_mut();
    let code = unsafe {
        (api.statement_execute)(
            connection,
            statement.get(),
            ptr::null(),
            ptr::null(),
            0,
            &mut result,
            &mut error,
        )
    };
    unsafe { api.check(code, &mut error, "execute native DuckDB SQL")? };
    ensure!(!result.is_null(), "DuckDB v2 returned a null result");
    Ok(OwnedHandle::new(
        Arc::clone(api),
        result,
        api.result_destroy,
    ))
}

struct WatchCompletion<'a>(&'a (Mutex<bool>, Condvar));

impl Drop for WatchCompletion<'_> {
    fn drop(&mut self) {
        let (lock, wake) = self.0;
        *lock.lock().unwrap_or_else(|error| error.into_inner()) = true;
        wake.notify_all();
    }
}

fn cancellation_watch(
    api: &Arc<Api>,
    connection: Handle,
    deadline: Instant,
    cancelled: &AtomicBool,
    abort: &AtomicBool,
    reason: &AtomicU8,
    done: &(Mutex<bool>, Condvar),
) {
    loop {
        let stop = if cancelled.load(Ordering::Acquire) {
            STOP_CANCELLED
        } else if Instant::now() >= deadline {
            STOP_TIMEOUT
        } else {
            STOP_NONE
        };
        if stop != STOP_NONE {
            let _ = reason.compare_exchange(STOP_NONE, stop, Ordering::AcqRel, Ordering::Acquire);
            abort.store(true, Ordering::Release);
            let mut error = ptr::null_mut();
            let _ = unsafe { (api.connection_interrupt)(connection, &mut error) };
            if !error.is_null() {
                let _ = unsafe { (api.error_info_destroy)(&mut error) };
            }
            // Interrupt is a no-op before activation. Keep retrying until the
            // execution owner confirms completion, including activation gaps.
        }
        let (lock, wake) = done;
        let finished = lock.lock().unwrap_or_else(|error| error.into_inner());
        if *finished {
            return;
        }
        let (finished, _) = wake
            .wait_timeout(finished, POLL_INTERVAL)
            .unwrap_or_else(|error| error.into_inner());
        if *finished {
            return;
        }
    }
}

fn check_deadline(deadline: Instant, cancelled: &AtomicBool) -> Result<()> {
    ensure!(!cancelled.load(Ordering::Acquire), "DuckDB query cancelled");
    ensure!(Instant::now() < deadline, "DuckDB query timed out");
    Ok(())
}
