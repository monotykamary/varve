use super::*;
use crate::metrics::{Metrics, Phase};
use std::process::ChildStdin;
use std::sync::{Mutex, mpsc};

#[path = "resident.rs"]
mod resident;
use resident::{ResidentCapacityError, ResidentRequest, ResidentState};

fn reusable_input_limit(options: &QueryOptions, request_limit: usize) -> usize {
    // Logical input bytes are not DuckDB allocations. Leave headroom for typed
    // storage, mutation versions and query operators; the DuckDB cap stays final.
    request_limit.min(options.memory_mb.saturating_mul(1024 * 1024) / 4)
}

/// A bounded pool of isolated DuckDB CLI processes. Admission never queues.
/// Selected files must remain immutable and pinned until execution returns.
pub struct QueryRuntime {
    capacity: usize,
    pool: Mutex<Pool>,
    metrics: Arc<Metrics>,
}

/// Lifecycle counters; reuse counts matching idle acquisitions, not query successes.
/// Resident counters record acknowledged adapter installs after staging cleanup,
/// even if subsequent SQL fails. A resident hit means no missing raw batches;
/// current rollups/catalog may still be replaced. Dynamic loads count changed
/// non-raw payloads, including an empty replacement with zero staged bytes.
/// Idle bytes include logical inputs and batch/schema metadata, not active workers
/// or a hard RSS bound.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct QueryWorkerStats {
    pub spawned: u64,
    pub reused: u64,
    pub resets: u64,
    pub discarded: u64,
    pub active: usize,
    pub idle: usize,
    pub resident_full_loads: u64,
    pub resident_delta_loads: u64,
    pub resident_hits: u64,
    pub resident_invalidations: u64,
    pub resident_raw_staged_rows: u64,
    pub resident_raw_staged_bytes: u64,
    pub resident_dynamic_loads: u64,
    pub resident_dynamic_staged_bytes: u64,
    pub resident_idle_rows: usize,
    pub resident_idle_bytes: usize,
    /// Lifetime materialization charges, including replaced inputs; not allocator RSS.
    pub resident_idle_materialized_bytes: usize,
}

#[derive(Default)]
struct Pool {
    active: usize,
    idle: Vec<Worker>,
    spawned: u64,
    reused: u64,
    resets: u64,
    discarded: u64,
    resident: QueryWorkerStats,
}

#[derive(PartialEq, Eq)]
struct Key {
    executable: PathBuf,
    generation: ExecutableGeneration,
    memory_mb: usize,
    threads: usize,
    files: Vec<PathBuf>,
}

// Metadata is a cheap generation discriminator, not a content digest. Executables
// must not be mutated in place; replacements must be atomic and coordinated with
// admission. Wrapper targets/libraries remain part of the trusted installation.
#[derive(PartialEq, Eq)]
struct ExecutableGeneration {
    #[cfg(unix)]
    identity: (u64, u64, u64, i64, i64, i64, i64),
}

impl ExecutableGeneration {
    fn read(path: &Path) -> Result<Self> {
        let metadata = std::fs::metadata(path).context("stat DuckDB executable")?;
        ensure!(metadata.is_file(), "DuckDB executable is not a file");
        Ok(Self {
            #[cfg(unix)]
            identity: {
                use std::os::unix::fs::MetadataExt;
                (
                    metadata.dev(),
                    metadata.ino(),
                    metadata.size(),
                    metadata.mtime(),
                    metadata.mtime_nsec(),
                    metadata.ctime(),
                    metadata.ctime_nsec(),
                )
            },
        })
    }
}

impl Key {
    fn new(tables: &[QueryTable], options: &QueryOptions, include_files: bool) -> Result<Self> {
        let executable =
            if options.executable.components().count() > 1 || options.executable.is_absolute() {
                options.executable.canonicalize()
            } else {
                env::split_paths(&env::var_os("PATH").unwrap_or_default())
                    .map(|path| path.join(&options.executable))
                    .find(|path| path.is_file())
                    .with_context(|| {
                        format!("find DuckDB executable {}", options.executable.display())
                    })?
                    .canonicalize()
            }
            .context("resolve DuckDB executable")?;
        let mut files = Vec::new();
        for table in tables {
            for file in &table.files {
                ensure!(
                    file.is_absolute(),
                    "query worker requires absolute immutable file paths"
                );
                if include_files {
                    files.push(file.clone());
                }
            }
        }
        files.sort();
        files.dedup();
        let generation = ExecutableGeneration::read(&executable)?;
        Ok(Self {
            executable,
            generation,
            memory_mb: options.memory_mb,
            threads: options.threads,
            files,
        })
    }
}

impl QueryRuntime {
    pub fn new(capacity: usize) -> Self {
        Self::with_metrics(capacity, Arc::new(Metrics::default()))
    }

    pub fn with_metrics(capacity: usize, metrics: Arc<Metrics>) -> Self {
        Self {
            capacity,
            pool: Mutex::new(Pool::default()),
            metrics,
        }
    }

    pub fn stats(&self) -> QueryWorkerStats {
        let pool = self.pool.lock().unwrap_or_else(|error| error.into_inner());
        QueryWorkerStats {
            spawned: pool.spawned,
            reused: pool.reused,
            resets: pool.resets,
            discarded: pool.discarded,
            active: pool.active,
            idle: pool.idle.len(),
            resident_idle_rows: pool
                .idle
                .iter()
                .filter_map(|worker| worker.resident.as_ref())
                .map(|state| state.rows)
                .sum(),
            resident_idle_bytes: pool
                .idle
                .iter()
                .filter_map(|worker| worker.resident.as_ref())
                .map(|state| state.bytes)
                .sum(),
            resident_idle_materialized_bytes: pool
                .idle
                .iter()
                .filter_map(|worker| worker.resident.as_ref())
                .map(|state| state.materialized_bytes)
                .fold(0, usize::saturating_add),
            ..pool.resident
        }
    }

    pub fn execute_with_catalog(
        &self,
        tables: &[QueryTable],
        sql: &str,
        options: &QueryOptions,
        catalog: &QueryCatalog,
    ) -> Result<Value> {
        self.execute_with_catalog_cancellable(
            tables,
            sql,
            options,
            catalog,
            &AtomicBool::new(false),
        )
    }

    pub fn execute_with_catalog_cancellable(
        &self,
        tables: &[QueryTable],
        sql: &str,
        options: &QueryOptions,
        catalog: &QueryCatalog,
        cancelled: &AtomicBool,
    ) -> Result<Value> {
        self.execute_inner(
            tables,
            None,
            sql,
            options,
            catalog,
            cancelled,
            MAX_QUERY_INPUT_BYTES,
        )
    }

    #[cfg(test)]
    pub(crate) fn execute_resident_with_catalog(
        &self,
        tables: &[QueryTable],
        snapshot: &ResidentSnapshot,
        sql: &str,
        options: &QueryOptions,
        catalog: &QueryCatalog,
    ) -> Result<Value> {
        self.execute_resident_with_catalog_cancellable(
            tables,
            snapshot,
            sql,
            options,
            catalog,
            &AtomicBool::new(false),
        )
    }

    pub(crate) fn execute_resident_with_catalog_cancellable(
        &self,
        tables: &[QueryTable],
        snapshot: &ResidentSnapshot,
        sql: &str,
        options: &QueryOptions,
        catalog: &QueryCatalog,
        cancelled: &AtomicBool,
    ) -> Result<Value> {
        self.execute_inner(
            tables,
            Some(snapshot),
            sql,
            options,
            catalog,
            cancelled,
            MAX_QUERY_INPUT_BYTES,
        )
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    fn execute_resident_with_limit(
        &self,
        tables: &[QueryTable],
        snapshot: &ResidentSnapshot,
        sql: &str,
        options: &QueryOptions,
        catalog: &QueryCatalog,
        cancelled: &AtomicBool,
        resident_limit: usize,
    ) -> Result<Value> {
        self.execute_inner(
            tables,
            Some(snapshot),
            sql,
            options,
            catalog,
            cancelled,
            resident_limit,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_inner(
        &self,
        tables: &[QueryTable],
        snapshot: Option<&ResidentSnapshot>,
        sql: &str,
        options: &QueryOptions,
        catalog: &QueryCatalog,
        cancelled: &AtomicBool,
        resident_limit: usize,
    ) -> Result<Value> {
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(options.timeout_ms))
            .context("query timeout is too large")?;
        validate_options(options)?;
        validate_read_only_statement(sql)?;
        check_deadline(deadline, cancelled)?;
        // Select/reserve atomically, before staging. Retirement stays under this
        // lock: another admission must not spawn while an evicted child still lives.
        let wait_timer = self.metrics.timer(Phase::QueryWait);
        let mut pool = self
            .pool
            .lock()
            .map_err(|_| anyhow::anyhow!("query pool poisoned"))?;
        ensure!(
            pool.active < self.capacity,
            "query worker capacity exhausted"
        );
        // Reusable workers never gain original-file authority. Unknown cold charges
        // and typed retained-capacity failures instead use a disposable selected-only
        // request; selected files remain native and exactly allowlisted there.
        let resident = snapshot
            .map(|snapshot| {
                let candidates = pool
                    .idle
                    .iter()
                    .filter_map(|worker| worker.resident.as_ref())
                    .collect::<Vec<_>>();
                let build = |native_files| {
                    ResidentRequest::new_with_limit(
                        tables,
                        snapshot,
                        options,
                        catalog,
                        &candidates,
                        native_files,
                        if native_files {
                            resident_limit
                        } else {
                            reusable_input_limit(options, resident_limit)
                        },
                        deadline,
                        cancelled,
                    )
                };
                let unknown_cold_charge = snapshot
                    .tables
                    .iter()
                    .flat_map(|table| &table.files)
                    .any(|file| file.charged_bytes == 0);
                if unknown_cold_charge {
                    build(true)
                } else {
                    match build(false) {
                        Ok(request) => Ok(request),
                        Err(error) if error.is::<ResidentCapacityError>() => build(true),
                        Err(error) => Err(error),
                    }
                }
            })
            .transpose()?;
        let native_files = resident.as_ref().is_some_and(ResidentRequest::native_files);
        let eligible = cfg!(unix) && pooling_eligible(sql, catalog) && !native_files;
        let key = Key::new(tables, options, snapshot.is_none() || !eligible)?;
        // Other platforms lack this Unix generation contract; retain bounded
        // execution there but never acquire or return a reusable process.
        check_deadline(deadline, cancelled)?;
        let index = if eligible {
            let mut best = None;
            for (index, worker) in pool.idle.iter().enumerate() {
                if worker.key != key {
                    continue;
                }
                let score = match (&worker.resident, &resident) {
                    (None, None) => 0,
                    (Some(state), Some(request))
                        if state.compatible(request) && request.installable_on(state)? =>
                    {
                        state.reuse_score(request)
                    }
                    _ => continue,
                };
                if best.is_none_or(|(_, best_score)| score > best_score) {
                    best = Some((index, score));
                }
            }
            best.map(|(index, _)| index)
        } else {
            None
        };
        let worker = index.map(|index| pool.idle.swap_remove(index));
        if worker.is_some() {
            pool.reused = pool.reused.saturating_add(1);
        }
        pool.active += 1;
        if pool.active + pool.idle.len() > self.capacity {
            // Fresh-only work leaves a matching reusable worker alone whenever
            // spare capacity (or an incompatible eviction candidate) permits it.
            let index = pool
                .idle
                .iter()
                .position(|worker| worker.key != key)
                .unwrap_or(pool.idle.len() - 1);
            let retired = pool.idle.swap_remove(index);
            if retired.resident.is_some() {
                pool.resident.resident_invalidations =
                    pool.resident.resident_invalidations.saturating_add(1);
            }
            drop(retired);
            pool.discarded = pool.discarded.saturating_add(1);
        }
        drop(pool);
        let mut lease = Lease {
            runtime: self,
            worker,
            reusable: false,
            reset_complete: false,
        };
        drop(wait_timer);
        check_deadline(deadline, cancelled)?;
        if lease.worker.is_none() {
            let _spawn_timer = self.metrics.timer(Phase::QuerySpawn);
            lease.worker = Some(Worker::spawn(key, options)?);
            let mut pool = self.pool.lock().unwrap_or_else(|error| error.into_inner());
            pool.spawned = pool.spawned.saturating_add(1);
        }
        let worker = lease.worker.as_mut().expect("leased worker");
        let build_timer = self.metrics.timer(Phase::QueryBuild);
        let (script, input) = if let Some(request) = resident {
            let accepted = worker.install_resident(request, options, deadline, cancelled)?;
            let mut pool = self.pool.lock().unwrap_or_else(|error| error.into_inner());
            pool.resident.resident_full_loads = pool
                .resident
                .resident_full_loads
                .saturating_add(accepted.resident_full_loads);
            pool.resident.resident_delta_loads = pool
                .resident
                .resident_delta_loads
                .saturating_add(accepted.resident_delta_loads);
            pool.resident.resident_hits = pool
                .resident
                .resident_hits
                .saturating_add(accepted.resident_hits);
            pool.resident.resident_raw_staged_rows = pool
                .resident
                .resident_raw_staged_rows
                .saturating_add(accepted.resident_raw_staged_rows);
            pool.resident.resident_raw_staged_bytes = pool
                .resident
                .resident_raw_staged_bytes
                .saturating_add(accepted.resident_raw_staged_bytes);
            pool.resident.resident_dynamic_loads = pool
                .resident
                .resident_dynamic_loads
                .saturating_add(accepted.resident_dynamic_loads);
            pool.resident.resident_dynamic_staged_bytes = pool
                .resident
                .resident_dynamic_staged_bytes
                .saturating_add(accepted.resident_dynamic_staged_bytes);
            let script = format!("BEGIN TRANSACTION;\n{sql}\n;\n");
            ensure!(
                script.len() <= MAX_QUERY_SCRIPT_BYTES,
                "DuckDB SQL script exceeds {} bytes",
                MAX_QUERY_SCRIPT_BYTES
            );
            (script, None)
        } else {
            worker.prepare(tables, sql, options, catalog)?
        };
        check_deadline(deadline, cancelled)?;
        drop(build_timer);
        let run_timer = self.metrics.timer(Phase::QueryRun);
        let output = worker.run(script, deadline, cancelled, options.max_output_bytes)?;
        let result = if lex_sql(sql)?
            .first()
            .is_some_and(|token| token == "EXPLAIN")
        {
            let plan = std::str::from_utf8(&output).context("DuckDB plan is not UTF-8")?;
            json!([{"plan": plan.trim_end()}])
        } else if output.iter().all(u8::is_ascii_whitespace) {
            Value::Array(Vec::new())
        } else {
            serde_json::from_slice(&output).context("decode DuckDB JSON output")?
        };
        check_deadline(deadline, cancelled)?;
        drop(run_timer);
        let _reset_timer = self.metrics.timer(Phase::QueryReset);
        let reset = worker.run("ROLLBACK;\n".into(), deadline, cancelled, 0)?;
        ensure!(reset.is_empty(), "unexpected DuckDB reset output");
        // Deleting staging before marking the worker reusable is part of success.
        if let Some(input) = input {
            input.close().context("remove private query input")?;
        }
        check_deadline(deadline, cancelled)?;
        lease.reset_complete = true;
        lease.reusable = eligible;
        Ok(result)
    }
}

struct Lease<'a> {
    runtime: &'a QueryRuntime,
    worker: Option<Worker>,
    reusable: bool,
    reset_complete: bool,
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        let discarded = !self.reusable && self.worker.is_some();
        let invalidated = discarded
            && self
                .worker
                .as_ref()
                .is_some_and(|worker| worker.resident_attempted);
        if !self.reusable {
            // Kill and join all I/O before releasing the admission slot.
            drop(self.worker.take());
        }
        let mut pool = self
            .runtime
            .pool
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(worker) = self.worker.take() {
            pool.idle.push(worker);
        }
        if self.reset_complete {
            pool.resets = pool.resets.saturating_add(1);
        }
        if invalidated {
            pool.resident.resident_invalidations =
                pool.resident.resident_invalidations.saturating_add(1);
        }
        if discarded {
            pool.discarded = pool.discarded.saturating_add(1);
        }
        pool.active -= 1;
    }
}

fn check_deadline(deadline: Instant, cancelled: &AtomicBool) -> Result<()> {
    ensure!(!cancelled.load(Ordering::Acquire), "DuckDB query cancelled");
    ensure!(Instant::now() < deadline, "DuckDB query timed out");
    Ok(())
}

enum Event {
    Output(Vec<u8>),
    Error(Vec<u8>),
    Eof,
    IoError(std::io::Error),
}

struct Worker {
    key: Key,
    child: Child,
    stdin: Option<ChildStdin>,
    receiver: Option<mpsc::Receiver<Event>>,
    readers: Vec<thread::JoinHandle<()>>,
    writer: Option<thread::JoinHandle<(ChildStdin, std::io::Result<()>)>>,
    directory: tempfile::TempDir,
    inputs: PathBuf,
    initialized: bool,
    resident: Option<ResidentState>,
    resident_attempted: bool,
    #[cfg(test)]
    run_calls: usize,
}

impl Worker {
    fn spawn(key: Key, options: &QueryOptions) -> Result<Self> {
        let directory = tempfile::Builder::new()
            .prefix("varve-query-worker-")
            .tempdir()
            .context("create private DuckDB worker directory")?;
        let inputs = directory.path().join("inputs");
        std::fs::create_dir(&inputs).context("create private query input directory")?;
        let mut resolved = options.clone();
        resolved.executable = key.executable.clone();
        let mut command = base_command(&resolved, directory.path());
        command
            .current_dir(directory.path())
            .arg("-json")
            .arg(":memory:");
        let mut child = command.spawn().context("spawn DuckDB executable")?;
        let stdin = child.stdin.take();
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        // A bounded queue applies backpressure; no per-worker unbounded I/O buffer.
        let (sender, receiver) = mpsc::sync_channel(4);
        let mut worker = Self {
            key,
            child,
            stdin,
            receiver: Some(receiver),
            readers: Vec::new(),
            writer: None,
            directory,
            inputs,
            initialized: false,
            resident: None,
            resident_attempted: false,
            #[cfg(test)]
            run_calls: 0,
        };
        worker
            .readers
            .push(pipe_reader(stdout, sender.clone(), false)?);
        worker.readers.push(pipe_reader(stderr, sender, true)?);
        Ok(worker)
    }

    fn prepare(
        &mut self,
        tables: &[QueryTable],
        sql: &str,
        options: &QueryOptions,
        catalog: &QueryCatalog,
    ) -> Result<(String, Option<tempfile::NamedTempFile>)> {
        self.prepare_mode(tables, sql, options, catalog, true)
    }

    fn prepare_mode(
        &mut self,
        tables: &[QueryTable],
        sql: &str,
        options: &QueryOptions,
        catalog: &QueryCatalog,
        inline_relations: bool,
    ) -> Result<(String, Option<tempfile::NamedTempFile>)> {
        // Build generated SQL separately from user SQL: never rewrite user strings.
        let (setup, input) = build_query_mode(
            tables,
            "",
            options,
            catalog,
            self.directory.path(),
            inline_relations,
        )?;
        let typed_input = input.is_empty();
        let (_, request) = setup
            .split_once("CREATE TEMP TABLE __varve_version_gate")
            .context("query setup missing version gate")?;
        let mut request = format!("CREATE TEMP TABLE __varve_version_gate{request}");
        let input = if input.is_empty() {
            None
        } else {
            let mut file = tempfile::Builder::new()
                .prefix("request-")
                .suffix(".json")
                .tempfile_in(&self.inputs)
                .context("create private query input")?;
            file.write_all(&input)
                .context("stage copied NDJSON query input")?;
            file.flush().context("flush private query input")?;
            let scanner = format!("FROM read_json({}, format = ", quote_path(file.path())?);
            request = request.replacen("FROM read_json('/dev/stdin', format = ", &scanner, 1);
            Some(file)
        };
        let mut script = self.configuration(options)?;
        script.push_str("BEGIN TRANSACTION;\n");
        script.push_str(&request);
        script.push('\n');
        script.push_str(sql);
        // Newline prevents a trailing SQL line comment from consuming framing.
        script.push_str("\n;\n");
        if inline_relations && typed_input && script.len() > MAX_QUERY_SCRIPT_BYTES {
            // Retry once without literals, including user SQL and worker framing
            // in the headroom check. Never reject a scanner-compatible request.
            drop(input);
            drop(script);
            drop(request);
            drop(setup);
            return self.prepare_mode(tables, sql, options, catalog, false);
        }
        ensure!(
            script.len() <= MAX_QUERY_SCRIPT_BYTES,
            "DuckDB SQL script exceeds {} bytes",
            MAX_QUERY_SCRIPT_BYTES
        );
        Ok((script, input))
    }

    fn run(
        &mut self,
        mut script: String,
        deadline: Instant,
        cancelled: &AtomicBool,
        limit: usize,
    ) -> Result<Vec<u8>> {
        #[cfg(test)]
        {
            self.run_calls += 1;
        }
        // CLI-only token: never exposed through current_query(), files or SQL catalogs.
        let marker = format!("varve_ack_{}\n", uuid::Uuid::new_v4().simple()).into_bytes();
        script.push_str(".print ");
        script.push_str(std::str::from_utf8(&marker).expect("ASCII acknowledgement"));
        let mut stdin = self.stdin.take().context("DuckDB stdin unavailable")?;
        self.writer = Some(
            thread::Builder::new()
                .name("varve-query-writer".into())
                .spawn(move || {
                    let result = stdin
                        .write_all(script.as_bytes())
                        .and_then(|()| stdin.flush());
                    (stdin, result)
                })
                .context("spawn DuckDB stdin writer")?,
        );
        let mut output = Vec::new();
        let mut stderr = Vec::new();
        let mut stdout_closed = false;
        let receiver = self.receiver.as_ref().expect("worker receiver");
        loop {
            check_deadline(deadline, cancelled)?;
            if let Some(status) = self.child.try_wait().context("poll DuckDB worker")? {
                // Drain bounded queued diagnostics after an exited child's readers finish.
                while let Ok(event) = receiver.recv_timeout(POLL_INTERVAL) {
                    check_deadline(deadline, cancelled)?;
                    if let Event::Error(bytes) = event {
                        let remaining = STDERR_LIMIT.saturating_sub(stderr.len());
                        stderr.extend_from_slice(&bytes[..bytes.len().min(remaining)]);
                    }
                }
                bail!(
                    "DuckDB failed ({status}): {}",
                    String::from_utf8_lossy(&stderr)
                );
            }
            match receiver.recv_timeout(POLL_INTERVAL) {
                Ok(Event::Output(bytes)) => {
                    ensure!(
                        output.len().saturating_add(bytes.len())
                            <= limit.saturating_add(marker.len()),
                        "DuckDB output exceeded {} bytes",
                        limit
                    );
                    output.extend_from_slice(&bytes);
                    if output.ends_with(&marker) {
                        let end = output.len() - marker.len();
                        ensure!(
                            end == 0 || output[end - 1] == b'\n',
                            "invalid DuckDB response framing"
                        );
                        output.truncate(end);
                        ensure!(
                            output.len() <= limit,
                            "DuckDB output exceeded {} bytes",
                            limit
                        );
                        ensure!(
                            stderr.is_empty(),
                            "DuckDB stderr: {}",
                            String::from_utf8_lossy(&stderr)
                        );
                        let (stdin, result) = self
                            .writer
                            .take()
                            .expect("request writer")
                            .join()
                            .map_err(|_| anyhow::anyhow!("DuckDB stdin writer panicked"))?;
                        self.stdin = Some(stdin);
                        result.context("write DuckDB stdin")?;
                        self.initialized = true;
                        return Ok(output);
                    }
                }
                Ok(Event::Error(bytes)) => {
                    ensure!(
                        stderr.len().saturating_add(bytes.len()) <= STDERR_LIMIT,
                        "DuckDB stderr limit exceeded"
                    );
                    stderr.extend_from_slice(&bytes);
                }
                Ok(Event::Eof) => stdout_closed = true,
                Ok(Event::IoError(error)) => return Err(error).context("read DuckDB worker pipe"),
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    bail!("DuckDB worker EOF: {}", String::from_utf8_lossy(&stderr))
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    ensure!(
                        !stdout_closed,
                        "DuckDB worker EOF: {}",
                        String::from_utf8_lossy(&stderr)
                    );
                }
            }
        }
    }
}

fn pipe_reader<R: Read + Send + 'static>(
    mut pipe: R,
    sender: mpsc::SyncSender<Event>,
    stderr: bool,
) -> Result<thread::JoinHandle<()>> {
    thread::Builder::new()
        .name("varve-query-reader".into())
        .spawn(move || {
            let mut buffer = [0_u8; 8192];
            loop {
                let event = match pipe.read(&mut buffer) {
                    Ok(0) => {
                        if !stderr {
                            let _ = sender.send(Event::Eof);
                        }
                        break;
                    }
                    Ok(n) if stderr => Event::Error(buffer[..n].to_vec()),
                    Ok(n) => Event::Output(buffer[..n].to_vec()),
                    Err(error) => {
                        let _ = sender.send(Event::IoError(error));
                        break;
                    }
                };
                if sender.send(event).is_err() {
                    break;
                }
            }
        })
        .context("spawn DuckDB pipe reader")
}

impl Drop for Worker {
    fn drop(&mut self) {
        // Drop receiver first so blocked reader sends cannot deadlock cleanup.
        drop(self.receiver.take());
        let _ = self.child.kill();
        let _ = self.child.wait();
        drop(self.stdin.take());
        if let Some(writer) = self.writer.take() {
            let _ = writer.join();
        }
        for reader in self.readers.drain(..) {
            let _ = reader.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_prepare_has_no_staging_and_scanner_headroom_is_preserved() {
        let options = QueryOptions {
            executable: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".tools/duckdb"),
            ..QueryOptions::default()
        };
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
        let tables = [QueryTable {
            name: "metrics".into(),
            hot: vec![],
            files: vec![],
            rollups: vec![RollupRow::from_row(1, &stored).unwrap()],
            cutoff_us: None,
        }];
        let mut catalog = QueryCatalog {
            relations: vec![CatalogRelation {
                name: "metadata".into(),
                columns: vec![("text".into(), "VARCHAR".into())],
                rows: vec![json!(["x".repeat(16 * 1024)])],
            }],
            aggregates: vec![AggregateAlias {
                name: "minute".into(),
                source: "metrics".into(),
                width_us: 1,
            }],
        };
        let mut worker =
            Worker::spawn(Key::new(&tables, &options, true).unwrap(), &options).unwrap();
        let (script, input) = worker
            .prepare(&tables, "SELECT * FROM minute", &options, &catalog)
            .unwrap();
        assert!(input.is_none());
        assert!(!script.contains("read_json("));
        assert!(!script.contains("/dev/stdin"));
        assert!(script.contains("INSERT INTO __varve_input VALUES"));
        assert!(script.contains("BEGIN TRANSACTION"));
        assert!(script.contains("SET allowed_paths = [];"));
        assert_eq!(std::fs::read_dir(&worker.inputs).unwrap().count(), 0);
        let (baseline, staged) = worker
            .prepare_mode(&tables, "", &options, &catalog, false)
            .unwrap();
        staged.unwrap().close().unwrap();
        let query = format!(
            "SELECT 42 AS answer /*{}*/",
            "x".repeat(MAX_QUERY_SCRIPT_BYTES - baseline.len() - 128)
        );
        let (script, staged) = worker.prepare(&tables, &query, &options, &catalog).unwrap();
        assert!(script.len() <= MAX_QUERY_SCRIPT_BYTES);
        assert!(script.contains("read_json("));
        let staged = staged.unwrap();
        assert_eq!(std::fs::read_dir(&worker.inputs).unwrap().count(), 1);
        let deadline = Instant::now() + Duration::from_secs(10);
        let cancelled = AtomicBool::new(false);
        let result = worker.run(script, deadline, &cancelled, 1024).unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&result).unwrap(),
            json!([{"answer": 42}])
        );
        assert!(
            worker
                .run("ROLLBACK;\n".into(), deadline, &cancelled, 0)
                .unwrap()
                .is_empty()
        );
        staged.close().unwrap();
        catalog.relations.clear();
        let (script, input) = worker
            .prepare(&tables, "SELECT count FROM minute", &options, &catalog)
            .unwrap();
        assert!(input.is_none());
        assert!(!script.contains("read_json("));
        assert!(!script.contains("SET allowed_paths"));
        let result = worker.run(script, deadline, &cancelled, 1024).unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&result).unwrap(),
            json!([{"count": 1}])
        );
        assert!(
            worker
                .run("ROLLBACK;\n".into(), deadline, &cancelled, 0)
                .unwrap()
                .is_empty()
        );
        assert_eq!(std::fs::read_dir(&worker.inputs).unwrap().count(), 0);
    }
}
