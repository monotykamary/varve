use super::*;
use crate::metrics::{Metrics, Phase};
use std::process::ChildStdin;
use std::sync::{Mutex, mpsc};

/// A bounded pool of isolated DuckDB CLI processes. Admission never queues.
/// Selected files must remain immutable and pinned until execution returns.
pub struct QueryRuntime {
    capacity: usize,
    pool: Mutex<Pool>,
    metrics: Arc<Metrics>,
}

/// Lifecycle counters; reuse counts matching idle acquisitions, not query successes.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct QueryWorkerStats {
    pub spawned: u64,
    pub reused: u64,
    pub resets: u64,
    pub discarded: u64,
    pub active: usize,
    pub idle: usize,
}

#[derive(Default)]
struct Pool {
    active: usize,
    idle: Vec<Worker>,
    spawned: u64,
    reused: u64,
    resets: u64,
    discarded: u64,
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
    fn new(tables: &[QueryTable], options: &QueryOptions) -> Result<Self> {
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
                files.push(file.clone());
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
        let key = Key::new(tables, options)?;
        // Other platforms lack this Unix generation contract; retain bounded
        // execution there but never acquire or return a reusable process.
        let eligible = cfg!(unix) && pooling_eligible(sql, catalog);
        check_deadline(deadline, cancelled)?;
        let index = if eligible {
            pool.idle.iter().position(|worker| worker.key == key)
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
            drop(pool.idle.swap_remove(index));
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
        let (script, input) = worker.prepare(tables, sql, options, catalog)?;
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
        // Build generated SQL separately from user SQL: never rewrite user strings.
        let (setup, input) = build_query(tables, "", options, catalog, self.directory.path())?;
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
            file.write_all(&input).context("stage typed query input")?;
            file.flush().context("flush private query input")?;
            let scanner = format!("FROM read_json({}, format = ", quote_path(file.path())?);
            request = request.replacen("FROM read_json('/dev/stdin', format = ", &scanner, 1);
            Some(file)
        };
        let mut script = String::new();
        if !self.initialized {
            let mut paths = Vec::new();
            for file in &self.key.files {
                paths.push(quote_path(file)?);
            }
            script.push_str(&format!(
                "SET memory_limit = '{}MB'; SET threads = {}; SET max_temp_directory_size = '0B'; SET preserve_insertion_order = false; ",
                options.memory_mb, options.threads));
            script.push_str("SET autoinstall_known_extensions = false; SET autoload_known_extensions = false; SET allow_community_extensions = false; SET allow_unsigned_extensions = false; ");
            script.push_str(&format!("SET allowed_directories = [{}]; SET allowed_paths = [{}]; SET home_directory = {}; SET temp_directory = {}; SET enable_external_access = false; SET lock_configuration = true;\n",
                quote_path(&self.inputs)?, paths.join(","), quote_path(self.directory.path())?, quote_path(self.directory.path())?));
        }
        script.push_str("BEGIN TRANSACTION;\n");
        script.push_str(&request);
        script.push('\n');
        script.push_str(sql);
        // Newline prevents a trailing SQL line comment from consuming framing.
        script.push_str("\n;\n");
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
