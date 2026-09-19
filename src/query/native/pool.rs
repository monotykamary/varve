use super::*;

// Retirement also bounds engine-internal catalog/version history. It is not a
// refund of raw credit: no source or scanner credit may enter an idle session.
const MAX_LEASES: usize = 64;

#[derive(PartialEq, Eq)]
pub(super) struct Key {
    namespace: String,
    memory_mb: usize,
    threads: usize,
    timeout_ms: u64,
    max_output_bytes: usize,
    files: Vec<PathBuf>,
}

impl Key {
    pub(super) fn new(
        tables: &[QueryTable],
        snapshot: &ResidentSnapshot,
        options: &QueryOptions,
    ) -> Result<Self> {
        // The pool belongs to exactly one NativeRuntime and immutable pinned
        // Api. Library path/hash/header/version and database ownership cannot
        // cross that boundary. Never pool a request without namespace authority.
        ensure!(
            !snapshot.namespace.is_empty(),
            "native reuse namespace is empty"
        );
        let mut bytes = snapshot.namespace.len();
        for path in tables.iter().flat_map(|table| &table.files) {
            ensure!(path.is_absolute(), "native Parquet path must be absolute");
            bytes = bytes
                .checked_add(path.as_os_str().len())
                .and_then(|n| n.checked_add(std::mem::size_of::<PathBuf>()))
                .context("native reuse key length overflow")?;
        }
        ensure!(
            bytes <= super::super::MAX_QUERY_SCRIPT_BYTES,
            "native reuse key exceeds SQL bound"
        );
        let mut files = tables
            .iter()
            .flat_map(|table| table.files.iter().cloned())
            .collect::<Vec<_>>();
        files.sort();
        files.dedup();
        Ok(Self {
            namespace: snapshot.namespace.clone(),
            memory_mb: options.memory_mb,
            threads: options.threads,
            timeout_ms: options.timeout_ms,
            max_output_bytes: options.max_output_bytes,
            files,
        })
    }
}

#[derive(Default)]
pub(super) struct Pool {
    active: usize,
    idle: Vec<(Key, NativeSession)>,
    stats: super::super::QueryWorkerStats,
}

impl Pool {
    pub(super) fn stats(&self) -> super::super::QueryWorkerStats {
        super::super::QueryWorkerStats {
            active: self.active,
            idle: self.idle.len(),
            ..self.stats
        }
    }
}

pub(super) struct Admission<'a> {
    runtime: &'a NativeRuntime,
    key: Option<Key>,
    pub(super) session: Option<NativeSession>,
    constructed: bool,
}

impl NativeRuntime {
    pub(super) fn admit(
        &self,
        prepare_key: impl FnOnce() -> Result<Option<Key>>,
    ) -> Result<Admission<'_>> {
        let mut pool = self.pool.lock().unwrap_or_else(|error| error.into_inner());
        ensure!(
            pool.active < self.capacity,
            "query worker capacity exhausted"
        );
        // Bound eligibility parsing and key allocation too. This critical
        // section can delay admission; it is not a wait-free queue guarantee.
        let key = prepare_key()?;
        let index = key
            .as_ref()
            .and_then(|key| pool.idle.iter().position(|(old, _)| old == key));
        let session = index.map(|index| {
            pool.stats.reused = pool.stats.reused.saturating_add(1);
            pool.idle.swap_remove(index).1
        });
        // Admission includes preparing executions, active handles and idle
        // handles. Retire before permitting another open, under the same lock.
        if session.is_none() && pool.active + pool.idle.len() == self.capacity {
            let retired = pool.idle.pop().expect("capacity contains an idle session");
            drop(retired);
            pool.stats.discarded = pool.stats.discarded.saturating_add(1);
        }
        pool.active += 1;
        let constructed = session.is_some();
        Ok(Admission {
            runtime: self,
            key,
            session,
            constructed,
        })
    }
}

impl Admission<'_> {
    pub(super) fn reusable(&self) -> bool {
        self.key.is_some()
    }

    pub(super) fn opened(&mut self) {
        self.constructed = true;
        let mut pool = self
            .runtime
            .pool
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        pool.stats.spawned = pool.stats.spawned.saturating_add(1);
    }

    pub(super) fn reset(&mut self, mut session: NativeSession) {
        session.leases += 1;
        let mut pool = self
            .runtime
            .pool
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        pool.stats.resets = pool.stats.resets.saturating_add(1);
        if session.leases < MAX_LEASES {
            self.session = Some(session);
        } else {
            drop(session);
        }
    }
}

impl Drop for Admission<'_> {
    fn drop(&mut self) {
        let mut pool = self
            .runtime
            .pool
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(session) = self.session.take() {
            // Only untouched clean acquisitions or positively reset sessions
            // reach this branch. Executing sessions live later on the stack.
            pool.idle.push((
                self.key.take().expect("reusable session has authority"),
                session,
            ));
        } else if self.constructed {
            pool.stats.discarded = pool.stats.discarded.saturating_add(1);
        }
        pool.active -= 1;
    }
}
