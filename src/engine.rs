use crate::derived::{self, DerivedRefs, RollupIndex, RollupSelection};
use crate::derived_root::{self, CheckpointRoot};
use crate::job_runtime::{self, JobRuntime};
use crate::metrics::{MeasuredDiskGuard, Metrics, PerformanceSnapshot, Phase, PhaseTimer};
use crate::model::*;
use crate::query::{
    self, QueryOptions, QueryTable, ResidentBatch, ResidentFile, ResidentLineage, ResidentSnapshot,
    ResidentTable,
};
use crate::raw_memory::{self, RawMemoryBudget, RawMemoryStatus, SharedRawRows};
use crate::remote::RemoteStore;
use crate::tier::RemoteHead;
use crate::{segment, wal};
use anyhow::{Context, Result, bail, ensure};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

#[path = "write_input.rs"]
mod write_input;
use write_input::ValidatedAppend;
pub(crate) use write_input::{AdmittedWrite, PreparedWrite};

#[path = "commit_boundary.rs"]
mod commit_boundary;
pub(crate) use commit_boundary::PreparedEpoch;
use commit_boundary::{PendingAppend, PreparedPublication};
#[path = "publication_gate.rs"]
mod publication_gate;
use publication_gate::{CommitLease, PublicationGate};
#[path = "append_overlay.rs"]
mod append_overlay;
use append_overlay::{AppendDelta, AppendOverlay, AppendView};

#[path = "commit_log.rs"]
mod commit_log;

pub(crate) fn journal_config(config: &Config) -> Result<crate::journal::JournalConfig> {
    commit_log::config(config)
}

#[derive(Clone, Copy)]
enum WriteMode {
    Single,
    Group,
}

impl WriteMode {
    fn grouped(self) -> bool {
        matches!(self, Self::Group)
    }
    fn admit_private(
        self,
        accumulated: usize,
        prepared: &PreparedAppend,
        config: &Config,
    ) -> Result<usize> {
        let bytes = prepared.working_bytes();
        ensure!(
            bytes <= prepared.derived.working.bytes,
            "rollback exceeds reserved derived working bytes"
        );
        let total = accumulated
            .checked_add(bytes)
            .context("rollback metadata accounting overflow")?;
        if self.grouped() {
            ensure!(
                total <= config.metadata_max_bytes,
                "group rollback metadata byte budget exceeded"
            );
        } else {
            // Retain the legacy group-only ceiling for private working state.
            // Direct preparation is covered by the derived reservation alone.
            ensure!(accumulated == 0, "direct write stages only one input");
        }
        Ok(total)
    }
    fn encode(self, sequence: u64, items: &[&PreparedWrite]) -> Result<wal::EncodedRecord> {
        match self {
            Self::Single => {
                ensure!(items.len() == 1, "direct write requires one input");
                wal::EncodedRecord::append(sequence, items[0])
            }
            Self::Group => wal::EncodedRecord::append_group(
                sequence,
                &items.iter().map(|item| &***item).collect::<Vec<_>>(),
            ),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteRequest {
    pub table: String,
    pub request_id: String,
    pub rows: Vec<Row>,
    pub now_us: i64,
}

impl WriteRequest {
    /// Conservative retained-allocation/escaped-JSON charge, not an RSS quota.
    pub(crate) fn admission_bytes(&self) -> Result<usize> {
        #[cfg(test)]
        write_input::ADMISSION_PASSES.with(|count| count.set(count.get() + 1));
        validate_name(&self.table)?;
        validate_request_id(&self.request_id)?;
        ensure!(!self.rows.is_empty(), "batch row admission limit");
        let mut bytes = 512usize
            .saturating_add(self.table.capacity().saturating_mul(6))
            .saturating_add(self.request_id.capacity().saturating_mul(6))
            .saturating_add(
                self.rows
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Row>()),
            );
        for row in &self.rows {
            row.validate()?;
            bytes = bytes
                .saturating_add(row.estimated_bytes().saturating_mul(6))
                .saturating_add(row.tenant.capacity())
                .saturating_add(row.series.capacity());
            for (key, value) in &row.tags {
                bytes = bytes
                    .saturating_add(key.capacity())
                    .saturating_add(value.capacity());
            }
        }
        ensure!(bytes < usize::MAX, "request byte accounting overflow");
        Ok(bytes)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WriteReceipt {
    pub sequence: u64,
    pub rows: usize,
    pub duplicate: bool,
    pub durability: String,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptEntry {
    pub sequence: u64,
    pub rows: usize,
    pub digest: String,
    #[serde(default)]
    pub issued_us: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_fingerprint: Option<String>,
}
// Exactly the encoded size of the final BLAKE3 hex proof, including JSON escaping.
const GROUP_PROOF_RESERVATION: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Segment {
    pub id: String,
    pub shard: u32,
    pub window_us: i64,
    pub rows: u64,
    pub min_timestamp_us: i64,
    pub max_timestamp_us: i64,
    pub bytes: u64,
    pub decoded_bytes: u64,
}
impl Segment {
    pub fn key(&self) -> String {
        format!("segments/{}.parquet", self.id)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Table {
    pub config: TableConfig,
    #[serde(default)]
    pub creation_config: Option<TableConfig>,
    pub created_sequence: u64,
    pub segments: Vec<Segment>,
    pub receipts: BTreeMap<String, ReceiptEntry>,
    pub rollups: BTreeMap<String, RollupRow>,
    pub cutoff_us: Option<i64>,
    pub rollup_cutoff_us: Option<i64>,
    #[serde(default)]
    pub idempotency_floor_us: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ControlStamp {
    pub sequence: u64,
    pub digest: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Manifest {
    pub format_version: u32,
    pub database_id: String,
    pub checkpoint_sequence: u64,
    #[serde(default, skip_serializing_if = "journal_disabled")]
    pub segmented_journal: bool,
    pub tables: BTreeMap<String, Table>,
    #[serde(default)]
    pub continuous_aggregates: BTreeMap<String, ContinuousAggregate>,
    #[serde(default)]
    pub jobs: BTreeMap<String, JobDefinition>,
    #[serde(default)]
    pub control_history: Vec<ControlStamp>,
}
pub(crate) fn journal_disabled(enabled: &bool) -> bool {
    !*enabled
}

#[derive(Clone, Debug, Serialize, Default)]
pub struct MaintenanceReport {
    pub flushed: bool,
    pub compacted: usize,
    pub expired_rows: u64,
    pub evicted_files: usize,
    pub reclaimed_files: usize,
    pub shipped_sequence: Option<u64>,
}
#[derive(Clone, Debug, Serialize)]
pub struct Status {
    pub database_id: String,
    pub sequence: u64,
    pub checkpoint_sequence: u64,
    pub remote_sequence: u64,
    pub segmented_journal: bool,
    pub native_query: Option<serde_json::Value>,
    pub unshipped_batches: u64,
    pub hot_rows: usize,
    pub hot_bytes: usize,
    pub raw_memory: RawMemoryStatus,
    pub wal_bytes: u64,
    pub disk_bytes: u64,
    pub metadata_bytes: usize,
    pub control_root_bytes: usize,
    pub derived_encoded_bytes: usize,
    pub derived_resident_bytes: usize,
    pub derived_working_bytes: usize,
    pub decoded_cache_bytes: usize,
    pub disk_cache_bytes: u64,
    pub tables: usize,
    pub segments: usize,
    pub rollup_groups: usize,
    pub idempotency_keys: usize,
    pub active_queries: usize,
    pub active_snapshots: usize,
    pub fenced: Option<String>,
    pub last_maintenance_error: Option<String>,
}

pub(crate) struct CacheEntry {
    pub rows: SharedRawRows,
    pub bytes: usize,
    pub touched: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HotEpoch {
    sequence: u64,
    first_now_us: i64,
}

pub(crate) struct State {
    pub catalog: Manifest,
    pub derived_refs: Option<BTreeMap<String, DerivedRefs>>,
    pub rollup_indexes: BTreeMap<String, RollupIndex>,
    pub derived_working: Arc<AtomicUsize>,
    pub control_root_bytes: usize,
    pub derived_resident_bytes: usize,
    derived_accounting: DerivedAccounting,
    pub metadata_bytes: usize,
    pub sequence: u64,
    pub raw_memory: RawMemoryBudget,
    replaying: bool,
    pub generation: u64,
    root_epoch: u64,
    control_epoch: u64,
    // Disposable query identity only; never a root/control publication guard.
    raw_stamps: BTreeMap<String, u64>,
    pub hot: BTreeMap<String, Vec<ResidentBatch>>,
    hot_epochs: Vec<HotEpoch>,
    pub hot_bytes: usize,
    pub wal_bytes: u64,
    pub fenced: Option<String>,
    pub decoded: BTreeMap<String, CacheEntry>,
    pub cache_clock: u64,
    pub first_hot_us: Option<i64>,
    pub last_ship_us: Option<i64>,
    pub remote_token: Option<String>,
    pub remote_owner: String,
    pub remote_head: Option<RemoteHead>,
    pub remote_segment_ids: BTreeSet<String>,
    pub remote_vacuum_pending: bool,
    pub last_maintenance_error: Option<String>,
    pub idempotency_floors: BTreeMap<String, i64>,
    pub job_runtime: BTreeMap<String, JobRuntime>,
}
// These tokens are process-local and never serialized. A fresh worker pool is
// owned by each opened Inner; allocating fresh tokens also separates recovery
// and recreation in this process. Exhaustion must never wrap into stale reuse.
fn fresh_raw_stamp() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        value.checked_add(1)
    })
    .expect("raw lineage identity exhausted")
}

fn capture_resident_snapshot(s: &State, tables: Vec<ResidentTable>) -> ResidentSnapshot {
    ResidentSnapshot {
        namespace: s.catalog.database_id.clone(),
        sequence: s.sequence,
        tables,
        lineage: s
            .catalog
            .tables
            .iter()
            .map(|(name, table)| ResidentLineage {
                name: name.clone(),
                raw_stamp: s.raw_stamps[name],
                // Cache residency and planner selection cannot change complete-table
                // coverage. Enumerate identities, never hash or scan raw rows here.
                ids: s
                    .hot
                    .get(name)
                    .into_iter()
                    .flatten()
                    .map(|batch| batch.id.clone())
                    .chain(table.segments.iter().map(|segment| segment.id.clone()))
                    .collect(),
            })
            .collect(),
    }
}

fn reconcile_raw_stamps(s: &mut State, previous: &Manifest, raw_rows_preserved: bool) {
    s.raw_stamps
        .retain(|name, _| s.catalog.tables.contains_key(name));
    for (name, table) in &s.catalog.tables {
        let changed = previous.database_id != s.catalog.database_id
            || previous.tables.get(name).is_none_or(|old| {
                old.created_sequence != table.created_sequence
                    || old.cutoff_us != table.cutoff_us
                    || (!raw_rows_preserved && old.segments != table.segments)
            });
        if changed || !s.raw_stamps.contains_key(name) {
            s.raw_stamps.insert(name.clone(), fresh_raw_stamp());
        }
    }
}
pub(crate) struct Inner {
    pub root: PathBuf,
    pub config: Config,
    pub raw_memory: RawMemoryBudget,
    pub state: Mutex<State>,
    // Lock order: maintenance/remote operation, commit, state, disk admission.
    // Readers never acquire commit; a publisher owns it through durable install.
    pub commit: Arc<PublicationGate>,
    pub journal: Option<Mutex<crate::journal::Journal>>,
    pub metrics: Arc<Metrics>,
    pub disk_admission: Mutex<()>,
    pub maintenance_preparation: Mutex<()>,
    #[cfg(feature = "fault-injection")]
    maintenance_test_hook: Mutex<Option<MaintenanceTestHook>>,
    pub remote_operation: Mutex<()>,
    pub remote: Option<Arc<dyn RemoteStore>>,
    pub readers: Arc<AtomicUsize>,
    pub segment_pins: Arc<Mutex<BTreeMap<String, usize>>>,
    pub query_active: AtomicUsize,
    pub query_runtime: query::QueryRuntime,
    pub native_runtime: Option<query::NativeRuntime>,
    pub jobs_running: Mutex<BTreeSet<String>>,
    _file_lock: File,
}
impl Drop for Inner {
    fn drop(&mut self) {
        // Release at the final owner, not merely when the last duplicated or
        // fork-inherited file description eventually closes in another process.
        let _ = fs2::FileExt::unlock(&self._file_lock);
    }
}
#[derive(Clone)]
pub struct Database {
    pub(crate) inner: Arc<Inner>,
}

#[cfg(feature = "fault-injection")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenanceHookPhase {
    CheckpointPrepare,
    /// Locked fallback: use a pre-released error hook, not a reader barrier.
    CheckpointLockedPrepare,
    CheckpointBeforePublish,
    CheckpointReclaim,
    CompactionPrepare,
    RootPrepare,
    DerivedPagePrepared,
    GroupCheckpointComplete,
    GroupBeforePublish,
    WalBeforeSync,
    WalBeforeDirectorySync,
    EpochBeforeInstall,
    SqlSnapshotCaptured,
}

#[cfg(feature = "fault-injection")]
#[derive(Clone)]
pub struct MaintenanceTestHook {
    phase: MaintenanceHookPhase,
    shared: Arc<(Mutex<MaintenanceHookState>, std::sync::Condvar)>,
}

#[cfg(feature = "fault-injection")]
#[derive(Default)]
struct MaintenanceHookState {
    entered: bool,
    released: bool,
    fail: bool,
}

#[cfg(feature = "fault-injection")]
impl MaintenanceTestHook {
    pub fn new(phase: MaintenanceHookPhase) -> Self {
        Self {
            phase,
            shared: Arc::new((
                Mutex::new(MaintenanceHookState::default()),
                std::sync::Condvar::new(),
            )),
        }
    }

    pub fn wait_until_blocked(&self, timeout: std::time::Duration) -> bool {
        let (state, changed) = &*self.shared;
        let state = state.lock().expect("maintenance hook poisoned");
        changed
            .wait_timeout_while(state, timeout, |state| !state.entered)
            .expect("maintenance hook poisoned")
            .0
            .entered
    }

    pub fn release(&self) {
        let (state, changed) = &*self.shared;
        let mut state = state.lock().expect("maintenance hook poisoned");
        state.released = true;
        changed.notify_all();
    }

    pub fn release_with_error(&self) {
        let (state, changed) = &*self.shared;
        let mut state = state.lock().expect("maintenance hook poisoned");
        state.fail = true;
        state.released = true;
        changed.notify_all();
    }

    fn block(&self, phase: MaintenanceHookPhase) -> Result<()> {
        if self.phase != phase {
            return Ok(());
        }
        let (state, changed) = &*self.shared;
        let mut state = state.lock().expect("maintenance hook poisoned");
        state.entered = true;
        changed.notify_all();
        while !state.released {
            state = changed.wait(state).expect("maintenance hook poisoned");
        }
        ensure!(!state.fail, "injected maintenance preparation failure");
        Ok(())
    }
}

pub(crate) struct StateGuard<'a> {
    guard: MutexGuard<'a, State>,
    _hold: PhaseTimer<'a>,
}

impl Deref for StateGuard<'_> {
    type Target = State;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl DerefMut for StateGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

pub(crate) struct Pin {
    counter: Arc<AtomicUsize>,
    pins: Arc<Mutex<BTreeMap<String, usize>>>,
    ids: BTreeSet<String>,
}
impl Pin {
    pub(crate) fn new(inner: &Inner, ids: BTreeSet<String>) -> Result<Self> {
        let mut pins = inner
            .segment_pins
            .lock()
            .map_err(|_| anyhow::anyhow!("segment pins poisoned"))?;
        for id in &ids {
            *pins.entry(id.clone()).or_default() += 1;
        }
        inner.readers.fetch_add(1, Ordering::SeqCst);
        Ok(Self {
            counter: inner.readers.clone(),
            pins: inner.segment_pins.clone(),
            ids,
        })
    }

    pub(crate) fn add(&mut self, id: String) -> Result<()> {
        if self.ids.contains(&id) {
            return Ok(());
        }
        let mut pins = self
            .pins
            .lock()
            .map_err(|_| anyhow::anyhow!("segment pins poisoned"))?;
        *pins.entry(id.clone()).or_default() += 1;
        self.ids.insert(id);
        Ok(())
    }
}
impl Drop for Pin {
    fn drop(&mut self) {
        if let Ok(mut pins) = self.pins.lock() {
            for id in &self.ids {
                if let Some(count) = pins.get_mut(id) {
                    *count -= 1;
                    if *count == 0 {
                        pins.remove(id);
                    }
                }
            }
        }
        self.counter.fetch_sub(1, Ordering::SeqCst);
    }
}
struct QueryPermit<'a>(&'a AtomicUsize);
impl Drop for QueryPermit<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Database {
    pub fn open(path: impl AsRef<Path>, config: Config) -> Result<Self> {
        Self::open_with_remote(path, config, None)
    }
    pub fn open_with_remote(
        path: impl AsRef<Path>,
        config: Config,
        remote: Option<Arc<dyn RemoteStore>>,
    ) -> Result<Self> {
        Self::open_locked(path, config, remote, None)
    }
    pub(crate) fn open_locked(
        path: impl AsRef<Path>,
        mut config: Config,
        remote: Option<Arc<dyn RemoteStore>>,
        held_lock: Option<File>,
    ) -> Result<Self> {
        config.validate()?;
        fs::create_dir_all(path.as_ref())?;
        let root = fs::canonicalize(path)?;
        crate::tier::validate_remote_layout(&root, remote.as_deref())?;
        let lock = if let Some(lock) = held_lock {
            lock
        } else {
            let lock = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(root.join("LOCK"))?;
            lock.try_lock_exclusive()
                .context("database is already open by another process")?;
            ensure!(
                !root.join("RESTORING").exists(),
                "incomplete restore; do not initialize or open this directory"
            );
            lock
        };
        for dir in ["wal", "segments", "cache", "staging", "derived"] {
            fs::create_dir_all(root.join(dir))?;
        }
        wal::sync_dir(&root)?;
        cleanup_temporaries(&root)?;
        let manifest_path = root.join("manifest.bin");
        let mut checkpoint = if manifest_path.exists() {
            derived_root::decode(
                &wal::read_bounded(&manifest_path, config.metadata_max_bytes)?,
                &config,
            )?
        } else {
            // Journal artifacts require their original manifest authority, even
            // when this process did not opt in. Do not parse or repair them here.
            let journal_path = root.join("journal");
            let journal_is_empty = match fs::symlink_metadata(&journal_path) {
                Ok(metadata) => metadata.is_dir() && fs::read_dir(&journal_path)?.next().is_none(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
                Err(error) => return Err(error.into()),
            };
            ensure!(
                journal_is_empty
                    && fs::read_dir(root.join("wal"))?.next().is_none()
                    && fs::read_dir(root.join("segments"))?.next().is_none()
                    && fs::read_dir(root.join("derived"))?.next().is_none(),
                "missing manifest in nonempty database; refusing to initialize over data"
            );
            let catalog = Manifest {
                format_version: FORMAT_VERSION,
                database_id: uuid::Uuid::new_v4().to_string(),
                checkpoint_sequence: 0,
                segmented_journal: config.segmented_journal,
                tables: BTreeMap::new(),
                continuous_aggregates: BTreeMap::new(),
                jobs: BTreeMap::from([(
                    "varve_maintenance".to_owned(),
                    crate::control::builtin_job(&config)?,
                )]),
                control_history: Vec::new(),
            };
            wal::atomic_write(&manifest_path, &encode_manifest(&catalog)?)?;
            CheckpointRoot {
                catalog,
                derived: None,
            }
        };
        if checkpoint.catalog.segmented_journal {
            config.segmented_journal = true;
        } else if config.segmented_journal {
            ensure!(
                fs::read_dir(root.join("wal"))?.next().is_none(),
                "segmented journal migration requires an explicit completed checkpoint and empty legacy WAL; reopen legacy, flush and GC first"
            );
            if root.join("journal").exists() {
                ensure!(
                    directory_bytes(&root.join("journal"))? == 0,
                    "unbound journal data exists; refusing implicit migration"
                );
            }
            checkpoint.catalog.segmented_journal = true;
            wal::atomic_write(&manifest_path, &checkpoint.encode(&config)?)?;
        }
        config.validate()?;
        if checkpoint.catalog.segmented_journal {
            ensure!(
                fs::read_dir(root.join("wal"))?.next().is_none(),
                "segmented root cannot contain legacy WAL records"
            );
        } else if root.join("journal").exists() {
            ensure!(
                directory_bytes(&root.join("journal"))? == 0,
                "journal data exists without durable format authority"
            );
        }
        let control_root_bytes = fs::metadata(&manifest_path)?.len() as usize;
        checkpoint.hydrate(&config, |page| {
            wal::read_bounded(&root.join(page.key()), page.bytes as usize)
        })?;
        let derived_refs = checkpoint.derived;
        let catalog = checkpoint.catalog;
        validate_manifest(&catalog)?;
        let rollup_indexes = build_rollup_indexes(&catalog, &config)?;
        let derived_resident_bytes = derived_root::resident_bytes(&catalog, true);
        let metadata_bytes = logical_metadata_bytes(&catalog)?;
        let derived_accounting = DerivedAccounting::build(&catalog, &config)?;
        let journal = if catalog.segmented_journal {
            Some(crate::journal::Journal::open_with_checkpoint(
                root.join("journal"),
                journal_config(&config)?,
                catalog.checkpoint_sequence,
            )?)
        } else {
            None
        };
        let wal_bytes = journal.as_ref().map_or_else(
            || directory_bytes(&root.join("wal")),
            |journal| Ok(journal.stats().disk_bytes),
        )?;
        ensure!(
            wal_bytes <= config.max_disk_bytes,
            "WAL recovery exceeds disk budget"
        );
        let raw_memory =
            RawMemoryBudget::new(config.raw_memory_max_bytes, config.raw_working_max_bytes)?;
        let mut state = State {
            raw_memory: raw_memory.clone(),
            sequence: catalog.checkpoint_sequence,
            replaying: true,
            generation: 0,
            root_epoch: 0,
            control_epoch: 0,
            raw_stamps: catalog
                .tables
                .keys()
                .map(|name| (name.clone(), fresh_raw_stamp()))
                .collect(),
            catalog,
            derived_refs,
            rollup_indexes,
            derived_working: Arc::new(AtomicUsize::new(0)),
            control_root_bytes,
            derived_resident_bytes,
            derived_accounting,
            metadata_bytes,
            hot: BTreeMap::new(),
            hot_epochs: Vec::new(),
            hot_bytes: 0,
            wal_bytes,
            fenced: None,
            decoded: BTreeMap::new(),
            cache_clock: 0,
            first_hot_us: None,
            last_ship_us: None,
            remote_token: None,
            remote_owner: uuid::Uuid::new_v4().to_string(),
            remote_head: None,
            remote_segment_ids: BTreeSet::new(),
            remote_vacuum_pending: false,
            last_maintenance_error: None,
            idempotency_floors: BTreeMap::new(),
            job_runtime: BTreeMap::new(),
        };
        let metrics = Arc::new(Metrics::default());
        let recovery_working = (wal_bytes > 0)
            .then(|| {
                raw_memory.reserve_working(raw_memory::codec_charge(
                    config
                        .max_batch_bytes
                        .saturating_mul(2)
                        .min(wal_bytes as usize),
                    (wal_bytes as usize).min(wal::MAX_FRAME_BYTES),
                ))
            })
            .transpose()?;
        if let Some(journal) = &journal {
            let checkpoint = state.sequence;
            journal.scan(|group| {
                for (sequence, bytes) in group.records() {
                    let record = wal::decode(bytes)?;
                    ensure!(record.sequence == sequence, "journal/WAL sequence mismatch");
                    if sequence > checkpoint {
                        replay_with_metrics(&mut state, record, &config, Some(&metrics))?;
                        check_recovery_budget(&config, &state)?;
                    }
                }
                Ok(())
            })?;
        } else {
            for record in wal::records(&root, state.sequence, config.wal_max_bytes)? {
                replay_with_metrics(&mut state, record?, &config, Some(&metrics))?;
                check_recovery_budget(&config, &state)?;
            }
        }
        drop(recovery_working);
        check_recovery_budget(&config, &state)?;
        state.replaying = false;
        state.idempotency_floors = state
            .catalog
            .tables
            .iter()
            .filter_map(|(name, table)| {
                table
                    .idempotency_floor_us
                    .map(|floor| (name.clone(), floor))
            })
            .collect();
        let mut runtime = job_runtime::load(&root)?;
        runtime.retain(|name, entry| {
            state
                .catalog
                .jobs
                .get(name)
                .is_some_and(|job| job.updated_sequence == entry.generation)
        });
        for (name, job) in &state.catalog.jobs {
            runtime
                .entry(name.clone())
                .or_insert_with(|| JobRuntime::from_definition(job));
        }
        state.job_runtime = runtime;
        crate::tier::load_binding(&root, &mut state, remote.as_deref())?;
        ensure!(
            state.hot_bytes <= config.hot_max_bytes && hot_count(&state) <= config.hot_max_rows,
            "recovery hot set exceeds configured budget; reopen with larger limits"
        );
        let query_runtime =
            query::QueryRuntime::with_metrics(config.query_workers, Arc::clone(&metrics));
        let native_runtime = config
            .duckdb_library
            .as_deref()
            .map(|path| {
                query::NativeRuntime::new(path, config.query_workers)
                    .map(|runtime| runtime.with_reuse(config.query_native_reuse))
            })
            .transpose()?;
        let db = Self {
            inner: Arc::new(Inner {
                root,
                config,
                raw_memory,
                state: Mutex::new(state),
                metrics,
                disk_admission: Mutex::new(()),
                maintenance_preparation: Mutex::new(()),
                commit: Arc::new(PublicationGate::default()),
                journal: journal.map(Mutex::new),
                #[cfg(feature = "fault-injection")]
                maintenance_test_hook: Mutex::new(None),
                remote_operation: Mutex::new(()),
                remote,
                readers: Arc::new(AtomicUsize::new(0)),
                segment_pins: Arc::new(Mutex::new(BTreeMap::new())),
                query_active: AtomicUsize::new(0),
                query_runtime,
                native_runtime,
                jobs_running: Mutex::new(BTreeSet::new()),
                _file_lock: lock,
            }),
        };
        {
            let mut s = db.lock()?;
            for table in s.catalog.tables.values() {
                for seg in &table.segments {
                    let path = db.inner.root.join(seg.key());
                    if path.exists() {
                        let _raw_verify = db.inner.raw_memory.reserve_working(
                            (seg.bytes as usize).saturating_mul(2).saturating_add(256),
                        )?;
                        verify_segment(&path, seg)?;
                    } else {
                        ensure!(
                            db.inner.remote.is_some(),
                            "segment {} is archived; configure its remote store",
                            seg.id
                        );
                    }
                }
            }
            if db.inner.config.derived_pages && s.derived_refs.is_none() {
                checkpoint_locked(&db.inner, &mut s)?;
            }
            gc_locked(&db.inner, &mut s)?;
        }
        db.ensure_builtin_job()?;
        Ok(db)
    }
    /// Returns readiness without waiting for the database mutex or touching catalog/disk state.
    pub fn is_ready(&self) -> bool {
        !self.inner.commit.is_poisoned()
            && self
                .inner
                .state
                .try_lock()
                .is_ok_and(|state| state.fenced.is_none())
    }

    pub(crate) fn committed_sequence(&self) -> Result<u64> {
        Ok(self.lock()?.sequence)
    }

    pub(crate) fn lock(&self) -> Result<StateGuard<'_>> {
        let wait = self.inner.metrics.timer(Phase::StateLockWait);
        let guard =
            self.inner.state.lock().map_err(|_| {
                anyhow::anyhow!("database state mutex poisoned; reopen for recovery")
            })?;
        drop(wait);
        Ok(StateGuard {
            guard,
            _hold: self.inner.metrics.timer(Phase::StateLockHold),
        })
    }

    pub(crate) fn lock_commit(&self) -> Result<CommitLease> {
        let _wait = self.inner.metrics.timer(Phase::CommitLockWait);
        self.inner.commit.lock()
    }

    pub(crate) fn lock_remote_operation(&self) -> Result<MutexGuard<'_, ()>> {
        self.inner
            .remote_operation
            .lock()
            .map_err(|_| anyhow::anyhow!("remote operation mutex poisoned"))
    }

    pub(crate) fn lock_maintenance_preparation(&self) -> Result<MutexGuard<'_, ()>> {
        self.inner
            .maintenance_preparation
            .lock()
            .map_err(|_| anyhow::anyhow!("maintenance preparation mutex poisoned"))
    }

    #[cfg(feature = "fault-injection")]
    pub fn set_maintenance_test_hook(&self, hook: Option<MaintenanceTestHook>) -> Result<()> {
        *self
            .inner
            .maintenance_test_hook
            .lock()
            .map_err(|_| anyhow::anyhow!("maintenance test hook poisoned"))? = hook;
        Ok(())
    }

    #[cfg(feature = "fault-injection")]
    pub(crate) fn block_maintenance_test_hook(&self, phase: MaintenanceHookPhase) -> Result<()> {
        let hook = self
            .inner
            .maintenance_test_hook
            .lock()
            .map_err(|_| anyhow::anyhow!("maintenance test hook poisoned"))?
            .clone();
        if let Some(hook) = hook {
            hook.block(phase)?;
        }
        Ok(())
    }

    pub fn create_table(&self, name: &str, config: TableConfig) -> Result<u64> {
        validate_name(name)?;
        config.validate()?;
        let _commit = self.lock_commit()?;
        let mut s = self.lock()?;
        healthy(&s)?;
        ensure!(
            !s.catalog.continuous_aggregates.contains_key(name)
                && !s.catalog.jobs.contains_key(name),
            "table name collides with control metadata"
        );
        if let Some(table) = s.catalog.tables.get(name) {
            ensure!(
                table.creation_config.as_ref().unwrap_or(&table.config) == &config,
                "table exists with a different immutable creation configuration"
            );
            return Ok(table.created_sequence);
        }
        ensure!(
            s.catalog.tables.len() < self.inner.config.max_tables,
            "table admission limit"
        );
        let sequence = next_sequence(&s)?;
        let _derived_working = reserve_catalog_clone(&s, &self.inner.config)?;
        let mut projected = s.catalog.clone();
        projected
            .tables
            .insert(name.to_owned(), empty_table(config.clone(), sequence));
        projected.checkpoint_sequence = sequence;
        check_state_catalog_budget(&self.inner, &s, &projected, hot_count(&s))?;
        let record = wal::Record::new(
            sequence,
            wal::Operation::CreateTable {
                name: name.to_string(),
                config,
            },
        );
        commit_record(&self.inner, &mut s, &record)?;
        replay(&mut s, record, &self.inner.config)?;
        Ok(sequence)
    }

    pub fn write(
        &self,
        table: &str,
        request_id: &str,
        rows: Vec<Row>,
        now_us: i64,
    ) -> Result<WriteReceipt> {
        let request = AdmittedWrite::new(WriteRequest {
            table: table.to_owned(),
            request_id: request_id.to_owned(),
            rows,
            now_us,
        })?;
        self.write_single_admitted(request)
    }

    fn write_single_admitted(&self, mut request: AdmittedWrite) -> Result<WriteReceipt> {
        request.reserve(&self.inner.raw_memory)?;
        self.write_group_attempts(vec![request.prepare(&self.inner.config)], WriteMode::Single)
            .pop()
            .expect("single input has one outcome")
    }

    /// Commits independent requests in bounded physical WAL groups, in input order.
    /// Admission proofs move with input ownership; state-dependent checks still run
    /// at every commit attempt and after every checkpoint boundary.
    pub fn write_group(&self, requests: Vec<WriteRequest>) -> Vec<Result<WriteReceipt>> {
        self.write_admitted_group(requests.into_iter().map(AdmittedWrite::new).collect())
    }

    pub(crate) fn write_admitted_group(
        &self,
        requests: Vec<Result<AdmittedWrite>>,
    ) -> Vec<Result<WriteReceipt>> {
        self.write_prepared_group(
            requests
                .into_iter()
                .map(|request| {
                    request.and_then(|mut request| {
                        request.reserve(&self.inner.raw_memory)?;
                        request.prepare(&self.inner.config)
                    })
                })
                .collect(),
        )
    }

    /// Consumes stage-owned preparation proofs without repeating row validation or hashing.
    pub(crate) fn write_prepared_group(
        &self,
        requests: Vec<Result<PreparedWrite>>,
    ) -> Vec<Result<WriteReceipt>> {
        let mut results: Vec<_> = (0..requests.len()).map(|_| None).collect();
        let mut group = Vec::new();
        let mut indices = Vec::new();
        let mut rows = 0usize;
        let mut bytes = 128usize;
        let config = &self.inner.config;
        let row_limit = config.max_batch_rows.min(config.hot_max_rows);
        let byte_limit = config
            .max_batch_bytes
            .min(config.hot_max_bytes)
            .min(config.wal_max_bytes as usize)
            .min(wal::MAX_FRAME_BYTES);
        let flush = |group: &mut Vec<PreparedWrite>,
                     indices: &mut Vec<usize>,
                     results: &mut Vec<Option<Result<WriteReceipt>>>| {
            let prepared = std::mem::take(group).into_iter().map(Ok).collect();
            for (index, result) in std::mem::take(indices)
                .into_iter()
                .zip(self.write_group_attempts(prepared, WriteMode::Group))
            {
                results[index] = Some(result);
            }
        };
        for (index, request) in requests.into_iter().enumerate() {
            let request = match request {
                Ok(request) => request,
                Err(error) => {
                    results[index] = Some(Err(error));
                    continue;
                }
            };
            let size = request.admission_bytes();
            if !group.is_empty()
                && (group.len() == wal::MAX_GROUP_REQUESTS
                    || rows.saturating_add(request.rows.len()) > row_limit
                    || bytes.saturating_add(size) > byte_limit)
            {
                flush(&mut group, &mut indices, &mut results);
                rows = 0;
                bytes = 128;
            }
            if request.rows.len() > row_limit || size.saturating_add(128) > byte_limit {
                results[index] = Some(
                    self.write_group_attempts(vec![Ok(request)], WriteMode::Single)
                        .pop()
                        .expect("single request has one outcome"),
                );
            } else {
                rows += request.rows.len();
                bytes += size;
                group.push(request);
                indices.push(index);
            }
        }
        if !group.is_empty() {
            flush(&mut group, &mut indices, &mut results);
        }
        results
            .into_iter()
            .map(|result| result.expect("every input has a result"))
            .collect()
    }

    fn write_group_attempts(
        &self,
        requests: Vec<OwnedGroupRequest>,
        mode: WriteMode,
    ) -> Vec<Result<WriteReceipt>> {
        self.prepare_group_attempts(requests, mode).publish()
    }

    /// One bounded physical epoch, including no-WAL terminal outcomes.
    pub(crate) fn prepare_epoch(&self, requests: Vec<Result<PreparedWrite>>) -> PreparedEpoch {
        self.prepare_group_attempts(requests, WriteMode::Group)
    }

    fn prepare_group_attempts(
        &self,
        mut requests: Vec<OwnedGroupRequest>,
        mode: WriteMode,
    ) -> PreparedEpoch {
        let count = requests.len();
        // Only outcomes checked against already durable state survive an outer
        // admission error, including a later ambiguous publication fence. They
        // acknowledge no part of the new (possibly unpublished) group.
        let mut durable: Vec<Option<Result<WriteReceipt>>> = (0..count).map(|_| None).collect();
        let mut validation_errors: Vec<Option<String>> = (0..count).map(|_| None).collect();
        let mut initial_checkpoint_pending = false;
        let mut terminal_floors = BTreeMap::new();
        let mut run = || -> Result<(Vec<Result<WriteReceipt>>, Option<PreparedPublication>)> {
            let mut commit = self.lock_commit()?;
            let mut s = self.lock()?;
            let mut group_prepare = mode
                .grouped()
                .then(|| self.inner.metrics.timer(Phase::GroupPrepare));
            healthy(&s)?;
            let config = &self.inner.config;
            resolve_group_durable(&s, &requests, &mut durable);
            // Durable retries/conflicts and unknown tables cannot consume new capacity.
            let mut rows = 0usize;
            let mut bytes = 0usize;
            let mut new_requests = 0usize;
            for (index, request) in requests.iter().enumerate() {
                if durable[index].is_some() {
                    continue;
                }
                let request = request.as_ref().expect("validated input");
                if s.catalog
                    .tables
                    .get(&request.table)
                    .is_some_and(|table| !table.receipts.contains_key(&request.request_id))
                {
                    rows += request.rows.len();
                    bytes += if mode.grouped() {
                        request.admission_bytes()
                    } else {
                        request.validated().resident_bytes()
                    };
                    new_requests += 1;
                }
            }
            let receipt_count: usize = s
                .catalog
                .tables
                .values()
                .map(|table| table.receipts.len())
                .sum();
            let initial_pressure = new_requests > 0
                && (mode.grouped()
                    || (rows <= config.hot_max_rows && bytes <= config.hot_max_bytes))
                && (hot_count(&s).saturating_add(rows) > config.hot_max_rows
                    || s.hot_bytes.saturating_add(bytes) > config.hot_max_bytes
                    || (mode.grouped()
                        && s.wal_bytes.saturating_add(bytes as u64).saturating_add(128)
                            > config.wal_max_bytes)
                    || (receipt_count.saturating_add(new_requests) > config.max_idempotency_keys
                        && (idempotency_checkpoint_due(&s)
                            || requests.iter().zip(&durable).any(|(request, outcome)| {
                                outcome.as_ref().is_some_and(Result::is_ok)
                                    && request
                                        .as_ref()
                                        .ok()
                                        .and_then(|item| {
                                            duplicate_clock_floor(&s, item).map(|floor| {
                                                floor
                                                    > s.idempotency_floors
                                                        .get(&item.table)
                                                        .copied()
                                                        .unwrap_or(i64::MIN)
                                            })
                                        })
                                        .unwrap_or(false)
                            }))));
            let mut offlock_checkpoint_used = false;
            if initial_pressure {
                initial_checkpoint_pending = true;
                drop(group_prepare.take());
                if !mode.grouped() {
                    let item = requests[0].as_ref().expect("validated direct input");
                    // A direct pressure checkpoint may prune using this input's
                    // validated clock, just as legacy direct admission did.
                    group_retry(&mut s, item, &item.digest)?;
                }
                let floors = group_checkpoint_floors(&s, &requests, &durable);
                restore_group_floors(&mut s, &floors);
                if config.checkpoint_frozen_prefix {
                    let observed = s.root_epoch;
                    drop(s);
                    drop(commit);
                    {
                        let _pressure = self.inner.metrics.timer(Phase::AdmissionCheckpoint);
                        let _ = checkpoint_prepared(self)?;
                    }
                    #[cfg(feature = "fault-injection")]
                    self.block_maintenance_test_hook(
                        MaintenanceHookPhase::GroupCheckpointComplete,
                    )?;
                    commit = self.lock_commit()?;
                    s = self.lock()?;
                    healthy(&s)?;
                    ensure!(
                        s.root_epoch > observed,
                        "pressure checkpoint did not publish a new durable root"
                    );
                    offlock_checkpoint_used = true;
                } else {
                    {
                        let _pressure = self.inner.metrics.timer(Phase::AdmissionCheckpoint);
                        checkpoint_locked(&self.inner, &mut s)?;
                    }
                }
                group_prepare = mode
                    .grouped()
                    .then(|| self.inner.metrics.timer(Phase::GroupPrepare));
            }
            initial_checkpoint_pending = false;
            let mut checkpoint_retry = !offlock_checkpoint_used
                && (s.sequence != s.catalog.checkpoint_sequence
                    || hot_count(&s) > 0
                    || idempotency_checkpoint_due(&s));
            loop {
                if group_prepare.is_none() {
                    group_prepare = mode
                        .grouped()
                        .then(|| self.inner.metrics.timer(Phase::GroupPrepare));
                }
                // Another caller may have committed one of our pending IDs while
                // unlocked. Its independently validated duplicate/floor is not staging.
                resolve_group_durable(&s, &requests, &mut durable);
                let sequence = next_sequence(&s)?;
                let mut overlay = AppendOverlay::new(&s);
                let mut needs_headroom = false;
                let mut needs_direct_checkpoint = false;
                let mut direct_encoded = None;
                for (index, request) in requests.iter().enumerate() {
                    if let Some(result) = &durable[index] {
                        let mut result = match result {
                            Ok(receipt) => Ok(receipt.clone()),
                            Err(error) => Err(anyhow::anyhow!("{error:#}")),
                        };
                        // The cheap prepass cannot see same-epoch receipt conflicts.
                        // Its conservative clock simulation can reject a later durable
                        // retry that the actual overlay permits. Recheck errors here;
                        // already verified durable successes remain unconditional.
                        if result.is_err()
                            && let Ok(item) = request
                            && s.catalog
                                .tables
                                .get(&item.table)
                                .is_some_and(|t| t.receipts.contains_key(&item.request_id))
                        {
                            result = overlay.retry(item).map(|r| r.expect("durable receipt"));
                        }
                        if let Ok(receipt) = &result {
                            let item = request.as_ref().expect("durable input");
                            overlay.durable_duplicate(item);
                            durable[index] = Some(Ok(receipt.clone()));
                        }
                        overlay.outcomes.push(result);
                        continue;
                    }
                    let result = (|| -> Result<WriteReceipt> {
                        let item = request
                            .as_ref()
                            .map_err(|error| anyhow::anyhow!("{error:#}"))?;
                        if mode.grouped() {
                            return overlay.prepare_group_item(
                                item,
                                sequence,
                                config,
                                Some(&self.inner.metrics),
                            );
                        }
                        if let Some(receipt) = overlay.retry(item)? {
                            return Ok(receipt);
                        }
                        if !mode.grouped()
                            && !offlock_checkpoint_used
                            && overlay.idempotency_checkpoint_due()
                            && s.catalog
                                .tables
                                .values()
                                .map(|table| table.receipts.len())
                                .sum::<usize>()
                                >= config.max_idempotency_keys
                        {
                            return Err(OffLockCheckpoint.into());
                        }
                        overlay.check_lateness(item)?;
                        let prepared = {
                            let preflight = preflight_append(
                                &overlay.view(),
                                item.validated(),
                                sequence,
                                overlay.accepted_rows,
                                None,
                                config,
                                Some(&self.inner.metrics),
                            )?;
                            // Preserve direct admission order without a second projection:
                            // metadata headroom, exact legacy WAL, then derived limits.
                            let encode_timer = self.inner.metrics.timer(Phase::WalEncode);
                            let encoded = mode.encode(sequence, &[item])?;
                            drop(encode_timer);
                            ensure!(
                                encoded.len() as u64 <= config.wal_max_bytes,
                                "batch exceeds WAL capacity"
                            );
                            if s.wal_bytes
                                .saturating_add(commit_log::required_bytes(&self.inner, &encoded)?)
                                > config.wal_max_bytes
                            {
                                ensure!(
                                    !offlock_checkpoint_used,
                                    "batch exceeds remaining WAL capacity after bounded checkpoint retry"
                                );
                                return Err(OffLockCheckpoint.into());
                            }
                            direct_encoded = Some(encoded);
                            preflight.finish(&overlay.view(), config)?
                        };
                        overlay.accept(item, prepared, mode, config)
                    })();
                    needs_direct_checkpoint |= result
                        .as_ref()
                        .err()
                        .is_some_and(|error| error.is::<OffLockCheckpoint>());
                    needs_headroom |= result
                        .as_ref()
                        .err()
                        .is_some_and(|error| error.is::<CheckpointHeadroom>());
                    validation_errors[index] = result.as_ref().err().map(|e| format!("{e:#}"));
                    overlay.outcomes.push(result);
                }
                // Baseline is still immutable; include duplicates validated against
                // the actual ordered overlay, not only conservative clock simulation.
                let floors_before = group_checkpoint_floors(&s, &requests, &durable);
                if (needs_headroom
                    && (checkpoint_retry
                        || (!mode.grouped()
                            && !offlock_checkpoint_used
                            && overlay.idempotency_checkpoint_due())))
                    || (needs_direct_checkpoint && !offlock_checkpoint_used)
                {
                    drop(group_prepare.take());
                    // Discard private allocations before a pressure checkpoint.
                    let direct_floors = if mode.grouped() {
                        BTreeMap::new()
                    } else {
                        std::mem::take(&mut overlay.delta.floors)
                    };
                    drop(overlay);
                    checkpoint_retry = false;
                    if mode.grouped() {
                        restore_group_floors(&mut s, &floors_before);
                    } else {
                        s.idempotency_floors.extend(direct_floors);
                    }
                    // A direct input's validated clock was historically retained
                    // before prepare_record; it can reclaim receipts at this frontier.
                    if config.checkpoint_frozen_prefix {
                        let observed = s.root_epoch;
                        drop(s);
                        drop(commit);
                        {
                            let _pressure = self.inner.metrics.timer(Phase::AdmissionCheckpoint);
                            let _ = checkpoint_prepared(self)?;
                        }
                        #[cfg(feature = "fault-injection")]
                        self.block_maintenance_test_hook(
                            MaintenanceHookPhase::GroupCheckpointComplete,
                        )?;
                        commit = self.lock_commit()?;
                        s = self.lock()?;
                        healthy(&s)?;
                        ensure!(
                            s.root_epoch > observed,
                            "pressure checkpoint did not publish a new durable root"
                        );
                        offlock_checkpoint_used = true;
                    } else {
                        {
                            let _pressure = self.inner.metrics.timer(Phase::AdmissionCheckpoint);
                            checkpoint_locked(&self.inner, &mut s)?;
                        }
                        if needs_direct_checkpoint {
                            // Bound even a defensive no-progress direct-pressure retry.
                            offlock_checkpoint_used = true;
                        }
                    }
                    continue;
                }
                let mut results = std::mem::take(&mut overlay.outcomes);
                if !overlay.items.is_empty() {
                    // Validate completely before constructing the private descriptor.
                    let publication = (|| -> Result<_> {
                        let encoded = if let Some(encoded) = direct_encoded.take() {
                            encoded
                        } else {
                            let encode_timer = self.inner.metrics.timer(Phase::WalEncode);
                            let encoded = mode.encode(sequence, &overlay.items)?;
                            drop(encode_timer);
                            encoded
                        };
                        ensure!(
                            (!mode.grouped() || encoded.len() <= config.max_batch_bytes)
                                && encoded.len() as u64 <= config.wal_max_bytes,
                            "group exceeds WAL/recovery byte capacity"
                        );
                        let fingerprint = encoded.group_fingerprint();
                        ensure!(
                            fingerprint.is_some() == mode.grouped(),
                            "group proof mode mismatch"
                        );
                        ensure!(
                            fingerprint
                                .as_ref()
                                .is_none_or(|proof| proof.len() == GROUP_PROOF_RESERVATION.len()),
                            "group proof reservation mismatch"
                        );
                        let published_now = overlay
                            .items
                            .first()
                            .and_then(|item| item.now_us)
                            .context("live group request missing clock")?;
                        if s.wal_bytes
                            .saturating_add(commit_log::required_bytes(&self.inner, &encoded)?)
                            > config.wal_max_bytes
                        {
                            ensure!(
                                checkpoint_retry && !offlock_checkpoint_used,
                                "group exceeds remaining WAL capacity after bounded checkpoint retry"
                            );
                            return Err(OffLockCheckpoint.into());
                        }
                        overlay.fingerprint(sequence, fingerprint.as_deref())?;
                        ensure!(sequence == next_sequence(&s)?, "noncontiguous live group");
                        let generation = s
                            .generation
                            .checked_add(1)
                            .context("state generation exhausted")?;
                        check_recovery_budget_view(config, &overlay.view())?;
                        ensure!(
                            s.wal_bytes
                                .saturating_add(commit_log::required_bytes(&self.inner, &encoded)?)
                                <= config.wal_max_bytes,
                            "group exceeds remaining WAL capacity"
                        );
                        drop(group_prepare.take());
                        // Unlike prepare_record, this cannot checkpoint provisional state.
                        #[cfg(feature = "fault-injection")]
                        self.block_maintenance_test_hook(MaintenanceHookPhase::GroupBeforePublish)?;
                        Ok((encoded, generation, published_now))
                    })();
                    let publication: Result<()> = match publication {
                        Ok((encoded, generation, published_now)) => {
                            let detach = self.inner.metrics.timer(Phase::CommitDetach);
                            let ordinals = std::mem::take(&mut overlay.ordinals);
                            let pending = overlay.into_pending();
                            drop(detach);
                            // Preserve only independently durable clocks across the handoff.
                            // Inputs cease to exist; no empty-row PreparedWrite escapes.
                            for (request, outcome) in requests.iter().zip(&durable) {
                                if outcome.as_ref().is_some_and(Result::is_ok) {
                                    let item = request.as_ref().expect("durable input");
                                    if let Some(floor) = duplicate_clock_floor(&s, item) {
                                        let entry = terminal_floors
                                            .entry(item.table.clone())
                                            .or_insert(i64::MIN);
                                        *entry = (*entry).max(floor);
                                    }
                                }
                            }
                            let mut materializing = commit_boundary::MaterializingEpoch {
                                encoded: Some(encoded),
                                inputs: std::mem::take(&mut requests),
                                pending: Some(pending),
                                envelopes: Vec::new(),
                            };
                            let materialized = (|| -> Result<()> {
                                // Detach ALL accepted frame credits while the encoded-first
                                // owner still owns every input. A partial failure cannot
                                // drop an input's frame credit ahead of encoded bytes.
                                for (input, result) in materializing.inputs.iter_mut().zip(&results)
                                {
                                    if result
                                        .as_ref()
                                        .is_ok_and(|r| r.sequence == sequence && !r.duplicate)
                                    {
                                        let envelope = input
                                            .as_mut()
                                            .expect("accepted input")
                                            .detach_frame_credit(&self.inner.raw_memory)?;
                                        materializing.envelopes.push(envelope);
                                    }
                                }
                                let mut ordinals = ordinals.into_iter();
                                for (input, result) in std::mem::take(&mut materializing.inputs)
                                    .into_iter()
                                    .zip(&results)
                                {
                                    if result
                                        .as_ref()
                                        .is_ok_and(|r| r.sequence == sequence && !r.duplicate)
                                    {
                                        let input = input.expect("accepted input");
                                        let table = input.table.clone();
                                        let bytes = input.validated().resident_bytes();
                                        let (rows, remainder) = input.materialize(
                                            &self.inner.raw_memory,
                                            sequence,
                                            ordinals.next().expect("accepted ordinal"),
                                        )?;
                                        materializing
                                            .pending
                                            .as_mut()
                                            .expect("pending append")
                                            .delta
                                            .batches
                                            .push((table, resident_batch(rows, bytes)));
                                        // The frame portion was detached before this call.
                                        debug_assert_eq!(remainder.bytes(), 0);
                                    }
                                }
                                Ok(())
                            })();
                            if let Err(error) = materialized {
                                // This is terminal, never a stale-state/checkpoint retry.
                                let message = format!("{error:#}");
                                for result in &mut results {
                                    if result.as_ref().is_ok_and(|r| r.sequence == sequence) {
                                        *result = Err(anyhow::anyhow!(message.clone()));
                                    }
                                }
                                restore_group_floors(&mut s, &floors_before);
                                commit_boundary::install_duplicate_floors(&mut s, &terminal_floors);
                                return Ok((results, None));
                            }
                            // The owned lease retains the exact exclusion domain,
                            // but neither State nor a borrowed guard crosses the handoff.
                            drop(s);
                            return Ok((
                                results,
                                Some(PreparedPublication {
                                    pending: materializing.pending.take().expect("pending append"),
                                    encoded: materializing.encoded.take().expect("encoded append"),
                                    _raw_envelopes: std::mem::take(&mut materializing.envelopes),
                                    sequence,
                                    generation,
                                    published_now,
                                    floors_before,
                                    commit,
                                }),
                            ));
                        }
                        Err(error) => {
                            drop(overlay);
                            Err(error)
                        }
                    };
                    drop(group_prepare.take());
                    if let Err(error) = publication {
                        restore_group_floors(&mut s, &floors_before);
                        if error.is::<OffLockCheckpoint>() && !offlock_checkpoint_used {
                            // Rebuild the private candidate against the new baseline;
                            // static inputs and independently durable outcomes survive.
                            if config.checkpoint_frozen_prefix {
                                let observed = s.root_epoch;
                                drop(s);
                                drop(commit);
                                {
                                    let _pressure =
                                        self.inner.metrics.timer(Phase::AdmissionCheckpoint);
                                    let _ = checkpoint_prepared(self)?;
                                }
                                #[cfg(feature = "fault-injection")]
                                self.block_maintenance_test_hook(
                                    MaintenanceHookPhase::GroupCheckpointComplete,
                                )?;
                                commit = self.lock_commit()?;
                                s = self.lock()?;
                                healthy(&s)?;
                                ensure!(
                                    s.root_epoch > observed,
                                    "pressure checkpoint did not publish a new durable root"
                                );
                            } else {
                                let _pressure =
                                    self.inner.metrics.timer(Phase::AdmissionCheckpoint);
                                checkpoint_locked(&self.inner, &mut s)?;
                            }
                            offlock_checkpoint_used = true;
                            checkpoint_retry = false;
                            continue;
                        }
                        let message = format!("{error:#}");
                        for result in &mut results {
                            if result.as_ref().is_ok_and(|r| r.sequence == sequence) {
                                *result = Err(anyhow::anyhow!(message.clone()));
                            }
                        }
                    }
                } else {
                    // Deliberate no-WAL policy: retain valid ordered clocks even
                    // when every new input was rejected after clock validation.
                    let floors = overlay.delta.floors;
                    s.idempotency_floors.extend(floors);
                }
                drop(group_prepare.take());
                // Retry floors are capped only while earlier inputs are pending.
                // Even failed publication must retain every durable acknowledgment clock.
                commit_group_duplicate_floors(&mut s, &requests, &durable);
                return Ok((results, None));
            }
        };
        let outcome = run();
        if outcome.is_err() {
            // No new group receipt is acknowledged here. Preserve only clock
            // advances belonging to independently validated durable duplicates.
            if let Ok(_commit) = self.lock_commit()
                && let Ok(mut s) = self.lock()
            {
                if initial_checkpoint_pending {
                    resolve_terminal_durable(&s, &requests, &mut durable);
                }
                commit_group_duplicate_floors(&mut s, &requests, &durable);
            }
        }
        let (results, publication) = match outcome {
            Ok(prepared) => prepared,
            Err(error) => (
                durable
                    .iter_mut()
                    .zip(validation_errors)
                    .map(|(durable, validation)| {
                        durable.take().unwrap_or_else(|| {
                            Err(anyhow::anyhow!(
                                validation.unwrap_or_else(|| format!("{error:#}"))
                            ))
                        })
                    })
                    .collect(),
                None,
            ),
        };
        PreparedEpoch {
            db: self.clone(),
            results,
            publication,
            duplicate_floors: terminal_floors,
        }
    }

    pub fn checkpoint(&self) -> Result<()> {
        checkpoint_prepared(self).map(|_| ())
    }

    pub fn rollups(&self, table: &str) -> Result<Vec<RollupRow>> {
        self.select_rollups(table, RollupSelection::default())
    }

    pub fn select_rollups(
        &self,
        table: &str,
        selection: RollupSelection<'_>,
    ) -> Result<Vec<RollupRow>> {
        let s = self.lock()?;
        healthy(&s)?;
        let (rows, _working) = select_rollups_locked(&s, &self.inner.config, table, selection)?;
        Ok(rows)
    }

    pub fn scan(
        &self,
        table: &str,
        start_us: Option<i64>,
        end_us: Option<i64>,
        tenant: Option<&str>,
        series: Option<&str>,
    ) -> Result<Vec<StoredRow>> {
        if let (Some(start), Some(end)) = (start_us, end_us) {
            ensure!(start <= end, "invalid half-open time range");
        }
        // The returned vector leaves accounting only at the caller handoff.
        let mut output_memory = self.inner.raw_memory.reserve(raw_memory::row_charge(0))?;
        let mut bytes = 0usize;
        let snapshot_timer = self.inner.metrics.timer(Phase::Snapshot);
        let (segments, cutoff_us, mut output, pin) = {
            let s = self.lock()?;
            healthy(&s)?;
            let table_state = s.catalog.tables.get(table).context("unknown table")?;
            let start = match (start_us, table_state.cutoff_us) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (a, b) => a.or(b),
            };
            let matches = |row: &StoredRow| {
                start.is_none_or(|value| row.row.timestamp_us >= value)
                    && end_us.is_none_or(|value| row.row.timestamp_us < value)
                    && tenant.is_none_or(|value| row.row.tenant == value)
                    && series.is_none_or(|value| row.row.series == value)
            };
            let selected = s
                .hot
                .get(table)
                .into_iter()
                .flatten()
                .flat_map(|batch| batch.rows.iter())
                .filter(|row| matches(row));
            let mut output = Vec::new();
            for row in selected {
                push_scan_row(
                    &mut output,
                    &mut bytes,
                    &mut output_memory,
                    row,
                    self.inner.config.query_max_output_bytes,
                )?;
            }
            let shard = tenant
                .zip(series)
                .map(|(tenant, series)| shard_for(tenant, series, table_state.config.shards));
            let segments: Vec<_> = table_state
                .segments
                .iter()
                .filter(|segment| {
                    !start.is_some_and(|value| segment.max_timestamp_us < value)
                        && !end_us.is_some_and(|value| segment.min_timestamp_us >= value)
                        && shard.is_none_or(|shard| segment.shard == shard)
                })
                .cloned()
                .collect();
            let ids = segments.iter().map(|segment| segment.id.clone()).collect();
            let pin = Pin::new(&self.inner, ids)?;
            (segments, table_state.cutoff_us, output, pin)
        };
        drop(snapshot_timer);
        let _pin = pin;
        let start = match (start_us, cutoff_us) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
        let matches = |row: &StoredRow| {
            start.is_none_or(|value| row.row.timestamp_us >= value)
                && end_us.is_none_or(|value| row.row.timestamp_us < value)
                && tenant.is_none_or(|value| row.row.tenant == value)
                && series.is_none_or(|value| row.row.series == value)
        };

        for descriptor in &segments {
            ensure!(
                descriptor.decoded_bytes <= self.inner.config.hot_max_bytes as u64,
                "segment decoded working set exceeds hot memory budget"
            );
            let cached = {
                let mut s = self.lock()?;
                healthy(&s)?;
                touch_decoded(&mut s, &descriptor.id)
            };
            let rows = if let Some(rows) = cached {
                rows
            } else {
                let rows = read_raw_segment(&self.inner, descriptor, false)?;
                ensure!(
                    rows.len() as u64 == descriptor.rows,
                    "segment row-count mismatch"
                );
                let decoded_bytes = rows
                    .iter()
                    .map(|row| row.row.estimated_bytes())
                    .sum::<usize>();
                ensure!(
                    decoded_bytes as u64 == descriptor.decoded_bytes,
                    "segment decoded-size metadata mismatch"
                );
                let mut s = self.lock()?;
                healthy(&s)?;
                offer_decoded(
                    &mut s,
                    self.inner.config.decoded_cache_bytes,
                    descriptor.id.clone(),
                    rows.clone(),
                    decoded_bytes,
                );
                rows
            };
            for row in rows.iter().filter(|row| matches(row)) {
                push_scan_row(
                    &mut output,
                    &mut bytes,
                    &mut output_memory,
                    row,
                    self.inner.config.query_max_output_bytes,
                )?;
            }
        }
        // sequence/ordinal uniquely identify public rows, so this is a total order.
        // Unstable sort uses no O(output) scratch allocation.
        output.sort_unstable_by(|a, b| {
            (
                &a.row.tenant,
                &a.row.series,
                a.row.timestamp_us,
                a.sequence,
                a.ordinal,
            )
                .cmp(&(
                    &b.row.tenant,
                    &b.row.series,
                    b.row.timestamp_us,
                    b.sequence,
                    b.ordinal,
                ))
        });
        Ok(output)
    }

    pub fn query(&self, sql: &str) -> Result<serde_json::Value> {
        self.query_cancellable(sql, &std::sync::atomic::AtomicBool::new(false))
    }

    /// Cancellation retains snapshot/file pins until native or CLI teardown completes.
    pub fn query_cancellable(
        &self,
        sql: &str,
        cancelled: &std::sync::atomic::AtomicBool,
    ) -> Result<serde_json::Value> {
        ensure!(!cancelled.load(Ordering::Acquire), "query cancelled");
        ensure!(sql.len() <= 64 * 1024, "SQL exceeds 64KiB limit");
        if let Some(result) = self.try_control_call(sql)? {
            return Ok(result);
        }
        self.inner
            .query_active
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                (n < self.inner.config.query_workers).then_some(n + 1)
            })
            .map_err(|_| anyhow::anyhow!("query concurrency limit"))?;
        let _permit = QueryPermit(&self.inner.query_active);
        let snapshot_timer = self.inner.metrics.timer(Phase::Snapshot);
        let retained =
            self.inner.config.query_retained_inputs || self.inner.native_runtime.is_some();
        let (_raw_working, snapshots, mut resident_snapshot, pin, catalog, _derived_working) = {
            let mut s = self.lock()?;
            healthy(&s)?;
            let names = s.catalog.tables.keys().cloned().collect::<Vec<_>>();
            let planning_catalog = query_catalog(
                &s,
                names.iter().map(String::as_str).collect(),
                CatalogRows::SchemaOnly,
            )?;
            let scan_plan = crate::plan::plan_with_catalog(sql, &names, &planning_catalog);
            let mut raw_working = Vec::new();
            let mut snapshots = Vec::new();
            let mut resident_tables = Vec::new();
            let mut touched_decoded = Vec::new();
            let mut derived_working = Vec::new();
            for (name, table) in &s.catalog.tables {
                if scan_plan.as_ref().is_some_and(|plan| plan.table != *name) {
                    continue;
                }
                let raw_selected = scan_plan
                    .as_ref()
                    .is_none_or(|plan| !plan.rollup && !plan.empty);
                let shard = scan_plan.as_ref().and_then(|plan| {
                    plan.tenant
                        .as_deref()
                        .zip(plan.series.as_deref())
                        .map(|(tenant, series)| shard_for(tenant, series, table.config.shards))
                });
                let descriptors = table
                    .segments
                    .iter()
                    .filter(|segment| {
                        table
                            .cutoff_us
                            .is_none_or(|cutoff| segment.max_timestamp_us >= cutoff)
                            && scan_plan.as_ref().is_none_or(|plan| {
                                !plan.rollup
                                    && !plan.empty
                                    && plan
                                        .start_us
                                        .is_none_or(|start| segment.max_timestamp_us >= start)
                                    && plan.end_us.is_none_or(|end| segment.min_timestamp_us < end)
                                    && shard.is_none_or(|shard| segment.shard == shard)
                            })
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                // Snapshot handle/pin storage is independent of shared payload size.
                // Native scanner buffers/owners are admitted separately before DB open.
                if self.inner.native_runtime.is_some() {
                    let pin_bytes = SharedRawRows::pin_metadata_bytes();
                    let mut count = 0usize;
                    let mut metadata = 0usize;
                    if raw_selected {
                        for batch in s
                            .hot
                            .get(name)
                            .into_iter()
                            .flatten()
                            .filter(|b| b.overlaps(scan_plan.as_ref(), table.cutoff_us))
                        {
                            count = count
                                .checked_add(1)
                                .context("native snapshot count overflow")?;
                            metadata = metadata
                                .checked_add(pin_bytes)
                                .and_then(|n| n.checked_add(batch.id.len()))
                                .context("native snapshot metadata overflow")?;
                        }
                        for descriptor in
                            descriptors.iter().filter(|d| s.decoded.contains_key(&d.id))
                        {
                            count = count
                                .checked_add(1)
                                .context("native snapshot count overflow")?;
                            metadata = metadata
                                .checked_add(pin_bytes)
                                .and_then(|n| n.checked_add(descriptor.id.len()))
                                .context("native snapshot metadata overflow")?;
                        }
                    }
                    if count != 0 {
                        // Include old+new Vec backing during geometric growth, and
                        // the four-slot minimum allocation for a nonempty iterator.
                        let slots = count
                            .checked_mul(3)
                            .context("native snapshot capacity overflow")?
                            .max(4);
                        metadata = metadata
                            .checked_add(
                                slots
                                    .checked_mul(std::mem::size_of::<ResidentBatch>())
                                    .context("native snapshot layout overflow")?,
                            )
                            .context("native snapshot metadata overflow")?;
                    }
                    raw_working.push(self.inner.raw_memory.reserve(metadata)?);
                } else {
                    let raw_bytes = s
                        .hot
                        .get(name)
                        .into_iter()
                        .flatten()
                        .map(|b| b.charged_bytes)
                        .sum::<usize>()
                        .saturating_add(
                            descriptors
                                .iter()
                                .filter_map(|d| s.decoded.get(&d.id))
                                .map(|e| e.bytes)
                                .sum::<usize>(),
                        );
                    if raw_selected && raw_bytes > 0 {
                        raw_working.push(self.inner.raw_memory.reserve(
                            raw_memory::codec_charge(
                                raw_bytes,
                                raw_bytes.saturating_mul(if retained { 2 } else { 4 }),
                            ),
                        )?);
                    }
                }
                let hot = if retained {
                    Vec::new()
                } else {
                    s.hot
                        .get(name)
                        .into_iter()
                        .flatten()
                        .flat_map(|batch| batch.rows.iter())
                        .filter(|row| {
                            table
                                .cutoff_us
                                .is_none_or(|cutoff| row.row.timestamp_us >= cutoff)
                                && scan_plan.as_ref().is_none_or(|plan| {
                                    !plan.rollup
                                        && plan.matches_series(&row.row.tenant, &row.row.series)
                                        && plan
                                            .start_us
                                            .is_none_or(|start| row.row.timestamp_us >= start)
                                        && plan.end_us.is_none_or(|end| row.row.timestamp_us < end)
                                })
                        })
                        .cloned()
                        .collect()
                };
                let mut fallback = Vec::new();
                let mut resident_batches = if retained && raw_selected {
                    s.hot
                        .get(name)
                        .into_iter()
                        .flatten()
                        .filter(|batch| batch.overlaps(scan_plan.as_ref(), table.cutoff_us))
                        .map(ResidentBatch::pinned)
                        .collect()
                } else {
                    Vec::new()
                };
                for descriptor in descriptors {
                    if retained {
                        if let Some(entry) = s.decoded.get(&descriptor.id) {
                            // The immutable descriptor already proves time overlap; do not
                            // rescan decoded rows to reconstruct its bounds per query.
                            resident_batches.push(ResidentBatch {
                                id: descriptor.id.clone(),
                                rows: entry.rows.pin(),
                                charged_bytes: entry.bytes,
                                verified_segment: true,
                                min_timestamp_us: descriptor.min_timestamp_us,
                                max_timestamp_us: descriptor.max_timestamp_us,
                            });
                            touched_decoded.push(descriptor.id);
                        } else {
                            fallback.push(descriptor);
                        }
                    } else {
                        fallback.push(descriptor);
                    }
                }
                let rollups = if scan_plan
                    .as_ref()
                    .is_some_and(|plan| !plan.rollup || plan.empty)
                {
                    Vec::new()
                } else {
                    let selection = RollupSelection {
                        tenant: scan_plan.as_ref().and_then(|plan| plan.tenant.as_deref()),
                        series: scan_plan.as_ref().and_then(|plan| plan.series.as_deref()),
                        width_us: scan_plan.as_ref().and_then(|plan| plan.rollup_width_us),
                        ..RollupSelection::default()
                    };
                    let (rows, working) =
                        select_rollups_locked(&s, &self.inner.config, name, selection)?;
                    derived_working.push(working);
                    rows
                };
                snapshots.push((
                    QueryTable {
                        name: name.clone(),
                        hot,
                        files: Vec::new(),
                        rollups,
                        cutoff_us: table.cutoff_us,
                    },
                    fallback,
                ));
                if retained {
                    resident_tables.push(ResidentTable {
                        name: name.clone(),
                        batches: resident_batches,
                        files: Vec::new(),
                    });
                }
            }
            for id in touched_decoded {
                let _ = touch_decoded(&mut s, &id);
            }
            let pinned_ids = snapshots
                .iter()
                .flat_map(|(_, segments)| segments.iter().map(|segment| segment.id.clone()))
                .collect();
            let pin = Pin::new(&self.inner, pinned_ids)?;
            let selected_names = snapshots
                .iter()
                .map(|(table, _)| table.name.as_str())
                .collect();
            let catalog_rows = if retained
                && scan_plan
                    .as_ref()
                    .is_some_and(crate::plan::ScanPlan::proves_single_storage_source)
            {
                CatalogRows::SchemaOnly
            } else {
                CatalogRows::Full
            };
            let catalog = query_catalog(&s, selected_names, catalog_rows)?;
            // Capture complete logical lineage under the same state lock as the
            // selected inputs, never after file resolution or planner filtering.
            let resident_snapshot =
                retained.then(|| capture_resident_snapshot(&s, resident_tables));
            (
                raw_working,
                snapshots,
                resident_snapshot,
                pin,
                catalog,
                derived_working,
            )
        };
        drop(snapshot_timer);
        #[cfg(feature = "fault-injection")]
        self.block_maintenance_test_hook(MaintenanceHookPhase::SqlSnapshotCaptured)?;
        let mut tables = Vec::with_capacity(snapshots.len());
        for (mut table, descriptors) in snapshots {
            for segment in &descriptors {
                let path = resolve_segment(&self.inner, segment)?;
                if let Some(snapshot) = &mut resident_snapshot {
                    let resident = snapshot
                        .tables
                        .iter_mut()
                        .find(|resident| resident.name == table.name)
                        .expect("captured resident table");
                    resident.files.push(ResidentFile {
                        id: segment.id.clone(),
                        path: path.clone(),
                        rows: usize::try_from(segment.rows)
                            .context("segment row count overflow")?,
                        charged_bytes: usize::try_from(segment.decoded_bytes)
                            .context("segment decoded size overflow")?,
                        min_timestamp_us: segment.min_timestamp_us,
                        max_timestamp_us: segment.max_timestamp_us,
                    });
                }
                table.files.push(path);
            }
            tables.push(table);
        }
        let _pin = pin;
        let config = &self.inner.config;
        let options = QueryOptions {
            executable: config.query_executable.clone(),
            memory_mb: config.query_memory_mb,
            threads: config.query_threads,
            timeout_ms: config.query_timeout_ms,
            max_output_bytes: config.query_max_output_bytes,
        };
        if let Some(runtime) = &self.inner.native_runtime {
            let _run_timer = self.inner.metrics.timer(Phase::QueryRun);
            runtime.execute(
                &tables,
                resident_snapshot.as_ref(),
                sql,
                &options,
                &catalog,
                &self.inner.raw_memory,
                cancelled,
            )
        } else if let Some(snapshot) = resident_snapshot.as_ref() {
            self.inner
                .query_runtime
                .execute_resident_with_catalog_cancellable(
                    &tables, snapshot, sql, &options, &catalog, cancelled,
                )
        } else {
            self.inner
                .query_runtime
                .execute_with_catalog_cancellable(&tables, sql, &options, &catalog, cancelled)
        }
    }

    pub fn query_worker_stats(&self) -> query::QueryWorkerStats {
        let mut stats = self.inner.query_runtime.stats();
        if let Some(runtime) = &self.inner.native_runtime {
            stats.active = runtime.active_queries();
        }
        stats
    }

    /// Native session counts, distinct from compatibility CLI process counters.
    pub fn native_query_worker_stats(&self) -> Option<query::QueryWorkerStats> {
        self.inner
            .native_runtime
            .as_ref()
            .map(|runtime| runtime.stats())
    }

    /// Persistence diagnostics after the current writer operation. This may wait
    /// for journal I/O; normal status and query snapshots do not take this lock.
    pub fn journal_stats(&self) -> Result<Option<crate::journal::JournalStats>> {
        self.inner
            .journal
            .as_ref()
            .map(|journal| {
                journal
                    .lock()
                    .map(|journal| journal.stats())
                    .map_err(|_| anyhow::anyhow!("journal owner poisoned"))
            })
            .transpose()
    }

    pub fn performance(&self) -> PerformanceSnapshot {
        self.inner.metrics.snapshot()
    }

    pub fn status(&self) -> Result<Status> {
        let mut status = {
            let s = self.lock()?;
            let remote_sequence = s
                .remote_head
                .as_ref()
                .map(|head| head.sequence)
                .unwrap_or(0);
            Status {
                database_id: s.catalog.database_id.clone(),
                sequence: s.sequence,
                checkpoint_sequence: s.catalog.checkpoint_sequence,
                remote_sequence,
                segmented_journal: s.catalog.segmented_journal,
                native_query: self.inner.native_runtime.as_ref().map(|runtime| {
                    let identity = runtime.identity();
                    json!({"library_path":identity.library_path,"library_sha256":identity.library_sha256,"header_sha256":identity.header_sha256,"version":identity.version,"reuse_enabled":runtime.reuse_enabled(),"workers":runtime.stats()})
                }),
                unshipped_batches: s.sequence.saturating_sub(remote_sequence),
                hot_rows: hot_count(&s),
                hot_bytes: s.hot_bytes,
                raw_memory: self.inner.raw_memory.status(),
                wal_bytes: s.wal_bytes,
                disk_bytes: 0,
                metadata_bytes: s.metadata_bytes,
                control_root_bytes: s.control_root_bytes,
                derived_encoded_bytes: persisted_derived_bytes(&s),
                derived_resident_bytes: s.derived_resident_bytes,
                derived_working_bytes: s.derived_working.load(Ordering::SeqCst),
                decoded_cache_bytes: s.decoded.values().map(|entry| entry.bytes).sum(),
                disk_cache_bytes: 0,
                tables: s.catalog.tables.len(),
                segments: s
                    .catalog
                    .tables
                    .values()
                    .map(|table| table.segments.len())
                    .sum(),
                rollup_groups: s
                    .catalog
                    .tables
                    .values()
                    .map(|table| table.rollups.len())
                    .sum(),
                idempotency_keys: s
                    .catalog
                    .tables
                    .values()
                    .map(|table| table.receipts.len())
                    .sum(),
                active_queries: self.inner.query_active.load(Ordering::SeqCst),
                active_snapshots: self.inner.readers.load(Ordering::SeqCst),
                fenced: s.fenced.clone().or_else(|| {
                    self.inner.commit.is_poisoned().then(|| {
                        "publication gate poisoned; reopen for recovery before further writes".to_owned()
                    })
                }),
                last_maintenance_error: s.last_maintenance_error.clone(),
            }
        };
        status.disk_bytes = directory_bytes(&self.inner.root)?;
        status.disk_cache_bytes = directory_bytes(&self.inner.root.join("cache"))?;
        Ok(status)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CatalogRows {
    Full,
    SchemaOnly,
}

impl CatalogRows {
    fn collect<T>(self, values: impl Iterator<Item = T>) -> Vec<T> {
        match self {
            Self::Full => values.collect(),
            Self::SchemaOnly => Vec::new(),
        }
    }
}

fn query_catalog(
    s: &State,
    selected_tables: Vec<&str>,
    rows: CatalogRows,
) -> Result<query::QueryCatalog> {
    use query::{AggregateAlias, CatalogRelation, QueryCatalog};
    let tables = rows.collect(s.catalog.tables.iter().map(|(name, table)| {
        json!([
            name,
            u64::from(table.config.shards),
            table.config.window_us,
            table.created_sequence
        ])
    }));
    let policies = rows.collect(s.catalog.tables.iter().map(|(name, table)| {
        json!([
            name,
            table.config.late_after_us,
            table.config.retention_us,
            table.config.archive_after_us,
            table.config.rollup_retention_us,
            table.config.idempotency_window_us,
            table.idempotency_floor_us
        ])
    }));
    let aggregates = rows.collect(s.catalog.continuous_aggregates.values().map(|aggregate| {
        json!([
            aggregate.name,
            aggregate.source,
            aggregate.width_us,
            aggregate.created_sequence
        ])
    }));
    let jobs = rows.collect(s.catalog.jobs.values().map(|definition| {
        let mut job = definition.clone();
        if let Some(runtime) = s
            .job_runtime
            .get(&job.name)
            .filter(|runtime| runtime.generation == job.updated_sequence)
        {
            runtime.apply_to(&mut job);
        }
        let kind = match job.kind {
            JobKind::Checkpoint => "checkpoint",
            JobKind::Compact => "compact",
            JobKind::Ship => "ship",
            JobKind::Maintain => "maintain",
            JobKind::VacuumRemote => "vacuum_remote",
        };
        json!([
            job.name,
            kind,
            job.interval_us,
            job.paused,
            job.next_run_us,
            job.running,
            u64::from(job.attempts),
            job.latest_run.as_ref().map(|run| run.run_id),
            job.latest_run.as_ref().and_then(|run| run.success),
            job.latest_run.as_ref().and_then(|run| run.error.clone())
        ])
    }));
    let remote_sequence = s
        .remote_head
        .as_ref()
        .map(|head| head.sequence)
        .unwrap_or(0);
    let status = rows.collect(std::iter::once_with(|| {
        json!([
            s.catalog.database_id,
            s.sequence,
            s.catalog.checkpoint_sequence,
            remote_sequence,
            s.sequence.saturating_sub(remote_sequence),
            hot_count(s) as u64,
            s.hot_bytes as u64,
            s.wal_bytes,
            s.metadata_bytes as u64,
            s.catalog.tables.len() as u64,
            s.catalog.continuous_aggregates.len() as u64,
            s.catalog.jobs.len() as u64,
            s.fenced.is_none()
        ])
    }));
    let relation = |name: &str, columns: Vec<(&str, &str)>, rows| CatalogRelation {
        name: name.into(),
        columns: columns
            .into_iter()
            .map(|(name, kind)| (name.into(), kind.into()))
            .collect(),
        rows,
    };
    let selected: BTreeSet<_> = selected_tables.into_iter().collect();
    let aliases = s
        .catalog
        .continuous_aggregates
        .values()
        .filter(|aggregate| selected.contains(aggregate.source.as_str()))
        .map(|aggregate| AggregateAlias {
            name: aggregate.name.clone(),
            source: aggregate.source.clone(),
            width_us: aggregate.width_us,
        })
        .collect();
    Ok(QueryCatalog {
        relations: vec![
            relation(
                "varve_tables",
                vec![
                    ("name", "VARCHAR"),
                    ("shards", "UBIGINT"),
                    ("window_us", "BIGINT"),
                    ("created_sequence", "UBIGINT"),
                ],
                tables,
            ),
            relation(
                "varve_policies",
                vec![
                    ("table_name", "VARCHAR"),
                    ("late_after_us", "BIGINT"),
                    ("retention_us", "BIGINT"),
                    ("archive_after_us", "BIGINT"),
                    ("rollup_retention_us", "BIGINT"),
                    ("idempotency_window_us", "BIGINT"),
                    ("idempotency_floor_us", "BIGINT"),
                ],
                policies,
            ),
            relation(
                "varve_continuous_aggregates",
                vec![
                    ("name", "VARCHAR"),
                    ("source", "VARCHAR"),
                    ("width_us", "BIGINT"),
                    ("created_sequence", "UBIGINT"),
                ],
                aggregates,
            ),
            relation(
                "varve_jobs",
                vec![
                    ("name", "VARCHAR"),
                    ("kind", "VARCHAR"),
                    ("interval_us", "BIGINT"),
                    ("paused", "BOOLEAN"),
                    ("next_run_us", "BIGINT"),
                    ("running", "BOOLEAN"),
                    ("attempts", "UBIGINT"),
                    ("latest_run_id", "UBIGINT"),
                    ("latest_success", "BOOLEAN"),
                    ("latest_error", "VARCHAR"),
                ],
                jobs,
            ),
            relation(
                "varve_status",
                vec![
                    ("database_id", "VARCHAR"),
                    ("sequence", "UBIGINT"),
                    ("checkpoint_sequence", "UBIGINT"),
                    ("remote_sequence", "UBIGINT"),
                    ("unshipped_batches", "UBIGINT"),
                    ("hot_rows", "UBIGINT"),
                    ("hot_bytes", "UBIGINT"),
                    ("wal_bytes", "UBIGINT"),
                    ("metadata_bytes", "UBIGINT"),
                    ("tables", "UBIGINT"),
                    ("continuous_aggregates", "UBIGINT"),
                    ("jobs", "UBIGINT"),
                    ("healthy", "BOOLEAN"),
                ],
                status,
            ),
        ],
        aggregates: aliases,
    })
}

fn select_rollups_locked(
    s: &State,
    config: &Config,
    name: &str,
    selection: RollupSelection<'_>,
) -> Result<(Vec<RollupRow>, DerivedWorking)> {
    let table = s.catalog.tables.get(name).context("unknown table")?;
    let index = s
        .rollup_indexes
        .get(name)
        .context("missing resident rollup index")?;
    let available = config
        .derived_max_bytes
        .saturating_sub(s.derived_resident_bytes)
        .saturating_sub(s.derived_working.load(Ordering::SeqCst));
    let selected = index.select(&table.rollups, selection, available)?;
    let bytes = selected.iter().fold(0usize, |bytes, row| {
        bytes
            .saturating_add(derived::rollup_resident_bytes("", row))
            .saturating_add(64)
    });
    let guard = reserve_derived(s, config, bytes)?;
    let rows = selected.into_iter().cloned().collect();
    Ok((rows, guard))
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct MapAccounting {
    json_bytes: usize,
    entries: usize,
    max_entry_bytes: usize,
}
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct TableAccounting {
    rollups: MapAccounting,
    receipts: MapAccounting,
    root_bound: usize,
    encoded_bound: usize,
    oversized_sets: usize,
}
#[derive(Default)]
struct DerivedAccounting {
    tables: BTreeMap<String, TableAccounting>,
    control_base: usize,
    root_bound: usize,
    encoded_bound: usize,
    oversized_sets: usize,
}

#[cfg(test)]
thread_local! {
    static JSON_COUNT_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn json_bytes<T: Serialize>(value: &T) -> Result<usize> {
    #[cfg(test)]
    JSON_COUNT_CALLS.with(|calls| calls.set(calls.get() + 1));
    struct Count(usize);
    impl std::io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| std::io::Error::other("JSON count overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    serde_json::to_writer(&mut count, value)?;
    Ok(count.0)
}

fn map_accounting<T: Serialize>(map: &BTreeMap<String, T>) -> Result<MapAccounting> {
    let mut max_entry_bytes = 0;
    for (key, value) in map {
        max_entry_bytes = max_entry_bytes.max(json_bytes(&(key, value))?);
    }
    Ok(MapAccounting {
        json_bytes: json_bytes(map)?,
        entries: map.len(),
        max_entry_bytes,
    })
}

impl TableAccounting {
    fn bound(&mut self, name: &str, config: &Config) -> Result<()> {
        self.root_bound = 0;
        self.encoded_bound = 0;
        self.oversized_sets = 0;
        for (kind, map) in [
            (derived::PageKind::Rollup, &self.rollups),
            (derived::PageKind::Receipt, &self.receipts),
        ] {
            if map.entries == 0 {
                continue;
            }
            let overhead = derived::page_overhead(name, kind)?;
            let capacity = config
                .derived_page_bytes
                .checked_sub(overhead)
                .context("derived page target too small")?;
            self.oversized_sets += usize::from(map.max_entry_bytes > capacity);
            // Adjacent greedy pages together contain at least one payload-capacity
            // of entry bytes. Thus pairs prove this bound without re-packing maps.
            let body = map
                .json_bytes
                .saturating_sub(2)
                .saturating_add(map.entries.saturating_mul(2));
            let pages = map.entries.min(body.saturating_mul(2) / capacity + 1);
            self.encoded_bound = self
                .encoded_bound
                .saturating_add(body)
                .saturating_add(pages.saturating_mul(overhead));
            // Digest/byte/count references are fixed-size; reserve decimal growth
            // in the two PageSet totals independently of the inline map size.
            self.root_bound = self
                .root_bound
                .saturating_add(pages.saturating_mul(192))
                .saturating_add(38);
        }
        Ok(())
    }
    fn entries_fit(&self, name: &str, config: &Config) -> Result<()> {
        for (kind, map) in [
            (derived::PageKind::Rollup, &self.rollups),
            (derived::PageKind::Receipt, &self.receipts),
        ] {
            ensure!(
                map.max_entry_bytes
                    .saturating_add(derived::page_overhead(name, kind)?)
                    <= config.derived_page_bytes,
                "derived entry exceeds configured page target"
            );
        }
        Ok(())
    }
}

impl DerivedAccounting {
    fn build(catalog: &Manifest, config: &Config) -> Result<Self> {
        let mut accounting = Self::default();
        let mut empty = BTreeMap::new();
        for (name, table) in &catalog.tables {
            let mut entry = TableAccounting {
                rollups: map_accounting(&table.rollups)?,
                receipts: map_accounting(&table.receipts)?,
                ..Default::default()
            };
            entry.bound(name, config)?;
            accounting.root_bound = accounting.root_bound.saturating_add(entry.root_bound);
            accounting.encoded_bound = accounting.encoded_bound.saturating_add(entry.encoded_bound);
            accounting.oversized_sets += entry.oversized_sets;
            accounting.tables.insert(name.clone(), entry);
            empty.insert(name.clone(), DerivedRefs::default());
        }
        let mut sizing = config.clone();
        sizing.metadata_max_bytes = wal::MAX_FRAME_BYTES;
        accounting.control_base =
            derived_root::encode_control(catalog, &empty, catalog.checkpoint_sequence, &sizing)?
                .len();
        Ok(accounting)
    }
    fn replace(&mut self, name: String, next: TableAccounting) -> Option<TableAccounting> {
        let old = self.tables.insert(name, next.clone());
        self.root_bound = self
            .root_bound
            .saturating_sub(old.as_ref().map_or(0, |a| a.root_bound))
            .saturating_add(next.root_bound);
        self.encoded_bound = self
            .encoded_bound
            .saturating_sub(old.as_ref().map_or(0, |a| a.encoded_bound))
            .saturating_add(next.encoded_bound);
        self.oversized_sets = self.oversized_sets - old.as_ref().map_or(0, |a| a.oversized_sets)
            + next.oversized_sets;
        old
    }
}

struct DerivedAppend {
    working: DerivedWorking,
    resident: usize,
    accounting: TableAccounting,
}

pub(crate) struct DerivedWorking {
    counter: Arc<AtomicUsize>,
    bytes: usize,
}
impl Drop for DerivedWorking {
    fn drop(&mut self) {
        self.counter.fetch_sub(self.bytes, Ordering::SeqCst);
    }
}

pub(crate) fn reserve_derived(s: &State, config: &Config, bytes: usize) -> Result<DerivedWorking> {
    let resident = s.derived_resident_bytes;
    let working = s.derived_working.load(Ordering::SeqCst);
    ensure!(
        resident.saturating_add(working).saturating_add(bytes) <= config.derived_max_bytes,
        "derived resident/working byte budget exceeded"
    );
    s.derived_working.fetch_add(bytes, Ordering::SeqCst);
    Ok(DerivedWorking {
        counter: Arc::clone(&s.derived_working),
        bytes,
    })
}

pub(crate) fn reserve_catalog_clone(s: &State, config: &Config) -> Result<DerivedWorking> {
    reserve_derived(s, config, derived_root::resident_bytes(&s.catalog, false))
}

fn build_rollup_indexes(
    catalog: &Manifest,
    config: &Config,
) -> Result<BTreeMap<String, RollupIndex>> {
    let mut remaining = config
        .derived_max_bytes
        .checked_sub(derived_root::resident_bytes(catalog, false))
        .context("derived resident map budget exceeded")?;
    let mut indexes = BTreeMap::new();
    for (name, table) in &catalog.tables {
        let index = RollupIndex::rebuild(&table.rollups, remaining)?;
        remaining -= index.resident_bytes();
        indexes.insert(name.clone(), index);
    }
    Ok(indexes)
}

fn persisted_derived_bytes(s: &State) -> usize {
    s.derived_refs
        .iter()
        .flat_map(|tables| tables.values())
        .fold(0usize, |total, refs| {
            total
                .saturating_add(refs.rollups.encoded_bytes as usize)
                .saturating_add(refs.receipts.encoded_bytes as usize)
        })
}

fn uses_derived_pages(s: &State, config: &Config) -> bool {
    s.derived_refs.is_some() || config.derived_pages
}

fn check_derived_checkpoint_headroom(s: &State, config: &Config, resident: usize) -> Result<()> {
    if !s.replaying {
        let page_working = if uses_derived_pages(s, config) {
            config.derived_page_bytes.saturating_mul(4)
        } else {
            0
        };
        // Both checkpoint paths retain cloned maps while persist_manifest builds
        // replacement indexes: resident + maps + indexes + page buffers. Leave
        // this space for a future checkpoint after other working users drain.
        ensure!(
            resident.saturating_mul(2).saturating_add(page_working) <= config.derived_max_bytes,
            "projected derived checkpoint working budget exceeded"
        );
    }
    Ok(())
}

fn logical_metadata_bytes(catalog: &Manifest) -> Result<usize> {
    struct Count(usize);
    impl std::io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| std::io::Error::other("metadata count overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(40);
    serde_json::to_writer(&mut count, catalog)?;
    Ok(count.0)
}

pub(crate) fn check_state_catalog_budget(
    inner: &Inner,
    s: &State,
    catalog: &Manifest,
    hot_rows: usize,
) -> Result<()> {
    let resident = derived_root::resident_bytes(catalog, true);
    ensure!(
        resident.saturating_add(s.derived_working.load(Ordering::SeqCst))
            <= inner.config.derived_max_bytes,
        "derived catalog resident/working budget exceeded"
    );
    // Control preflight (notably aggregate backfill) can grow the derived maps
    // without append admission. Do not impose new headroom on historical state
    // merely to checkpoint it or perform a non-growing control operation.
    if resident > s.derived_resident_bytes {
        check_derived_checkpoint_headroom(s, &inner.config, resident)?;
    }
    if uses_derived_pages(s, &inner.config) {
        let _working = reserve_derived(
            s,
            &inner.config,
            inner.config.derived_page_bytes.saturating_mul(4),
        )?;
        let (_, bytes) =
            derived_root::project(catalog, &inner.config, catalog.checkpoint_sequence, None)?;
        ensure!(
            bytes.saturating_add(hot_rows.saturating_mul(512)) <= inner.config.metadata_max_bytes,
            "projected checkpoint exceeds control metadata byte budget"
        );
        Ok(())
    } else {
        check_metadata_budget(&inner.config, catalog, hot_rows)
    }
}

struct AppendAccounting {
    metadata_delta: i128,
    // Derived failures must not overtake metadata admission or WAL preparation.
    derived: Result<DerivedProjection>,
}

struct DerivedProjection {
    accounting: TableAccounting,
    resident: usize,
    working: usize,
}

struct EncodedUpsert {
    key_bytes: usize,
    value_bytes: usize,
    previous_bytes: Option<usize>,
    entry_bytes: usize,
}

impl EncodedUpsert {
    fn delta(&self, nonempty: bool) -> i128 {
        match self.previous_bytes {
            Some(previous) => self.value_bytes as i128 - previous as i128,
            None => self.key_bytes as i128 + self.value_bytes as i128 + 1 + i128::from(nonempty),
        }
    }

    fn apply(&self, map: &mut MapAccounting) -> Result<()> {
        map.json_bytes = apply_encoded_delta(map.json_bytes, self.delta(map.entries > 0))?;
        map.max_entry_bytes = map.max_entry_bytes.max(self.entry_bytes);
        map.entries += usize::from(self.previous_bytes.is_none());
        Ok(())
    }
}

fn encoded_upsert<K: Serialize, V: Serialize>(
    key: &K,
    value: &V,
    previous: Option<&V>,
) -> Result<EncodedUpsert> {
    let key_bytes = json_bytes(key)?;
    let value_bytes = json_bytes(value)?;
    let previous_bytes = previous.map(json_bytes).transpose()?;
    let entry_bytes = key_bytes
        .checked_add(value_bytes)
        .and_then(|bytes| bytes.checked_add(3))
        .context("JSON count overflow")?;
    Ok(EncodedUpsert {
        key_bytes,
        value_bytes,
        previous_bytes,
        entry_bytes,
    })
}

// This one-use scalar projection belongs to this exact locked state, not merely
// its sequence: even a same-sequence checkpoint can prune receipts/change floors.
#[cfg(test)]
fn project_append_accounting(
    s: &State,
    table: &str,
    id: &str,
    receipt: &ReceiptEntry,
    updates: &BTreeMap<String, RollupRow>,
    metrics: Option<&Metrics>,
) -> Result<AppendAccounting> {
    project_append_accounting_view(
        &AppendView::committed(s),
        table,
        id,
        receipt,
        updates,
        metrics,
    )
}
#[cfg(test)]
impl AppendAccounting {
    fn metadata_bytes(&self, s: &State, sequence: u64) -> Result<usize> {
        self.metadata_bytes_view(&AppendView::committed(s), sequence)
    }
}
#[cfg(test)]
fn check_derived_append(
    s: &State,
    config: &Config,
    table: &str,
    projection: Result<DerivedProjection>,
    hot_rows: usize,
) -> Result<DerivedAppend> {
    check_derived_append_view(
        &AppendView::committed(s),
        config,
        table,
        projection,
        hot_rows,
    )
}

fn project_append_accounting_view(
    s: &AppendView<'_>,
    table_name: &str,
    id: &str,
    receipt: &ReceiptEntry,
    updates: &BTreeMap<String, RollupRow>,
    metrics: Option<&Metrics>,
) -> Result<AppendAccounting> {
    let _timer = metrics.map(|metrics| metrics.timer(Phase::AppendAccounting));
    ensure!(s.catalog.tables.contains_key(table_name), "unknown table");
    let encoded = encoded_upsert(&id, receipt, s.receipt(table_name, id))?;
    let mut metadata_delta = encoded.delta(s.receipts_nonempty(table_name));
    let mut derived = (|| -> Result<DerivedProjection> {
        let mut accounting = s.accounting(table_name)?.clone();
        encoded.apply(&mut accounting.receipts)?;
        let working = derived::receipt_resident_bytes(id, receipt);
        let resident = if encoded.previous_bytes.is_none() {
            s.resident().saturating_add(working)
        } else {
            s.resident()
        };
        Ok(DerivedProjection {
            accounting,
            resident,
            working,
        })
    })();
    let mut nonempty = s.rollups_nonempty(table_name);
    for (key, row) in updates {
        let previous = s.rollup(table_name, key);
        let encoded = encoded_upsert(key, row, previous)?;
        metadata_delta += encoded.delta(nonempty);
        nonempty = true;
        derived = derived.and_then(|mut projected| {
            encoded.apply(&mut projected.accounting.rollups)?;
            let bytes = derived::rollup_resident_bytes(key, row);
            projected.working = projected.working.saturating_add(bytes.saturating_mul(2));
            if let Some(old) = previous {
                projected.resident = projected
                    .resident
                    .saturating_sub(derived::rollup_resident_bytes(key, old));
            } else {
                projected.resident = projected
                    .resident
                    .saturating_add(RollupIndex::entry_bytes(key, row));
            }
            projected.resident = projected.resident.saturating_add(bytes);
            Ok(projected)
        });
    }
    Ok(AppendAccounting {
        metadata_delta,
        derived,
    })
}

impl AppendAccounting {
    fn metadata_bytes_view(&self, s: &AppendView<'_>, checkpoint_sequence: u64) -> Result<usize> {
        apply_encoded_delta(
            s.metadata_bytes(),
            self.metadata_delta
                + checkpoint_sequence_delta(s.catalog.checkpoint_sequence, checkpoint_sequence),
        )
    }
}

fn check_derived_append_view(
    s: &AppendView<'_>,
    config: &Config,
    table_name: &str,
    projection: Result<DerivedProjection>,
    hot_rows: usize,
) -> Result<DerivedAppend> {
    let DerivedProjection {
        mut accounting,
        resident: projected,
        working,
    } = projection?;
    let old = s.accounting(table_name)?;
    accounting.bound(table_name, config)?;
    ensure!(
        projected
            .saturating_add(working)
            .saturating_add(s.derived_working.load(Ordering::SeqCst))
            <= config.derived_max_bytes,
        "projected derived resident/working budget exceeded"
    );
    check_derived_checkpoint_headroom(s, config, projected)?;
    if uses_derived_pages(s, config) && !s.replaying {
        accounting.entries_fit(table_name, config)?;
        ensure!(
            s.oversized_sets() - old.oversized_sets + accounting.oversized_sets == 0,
            "existing derived entry exceeds configured writer target"
        );
        let encoded = s
            .encoded_bound()
            .saturating_sub(old.encoded_bound)
            .saturating_add(accounting.encoded_bound);
        ensure!(
            encoded <= config.derived_max_bytes,
            "projected derived encoded byte budget exceeded"
        );
        let bytes = s
            .derived_accounting
            .control_base
            .saturating_add(s.root_bound())
            .saturating_sub(old.root_bound)
            .saturating_add(accounting.root_bound)
            .saturating_add(20);
        ensure!(
            projected.saturating_add(config.derived_page_bytes.saturating_mul(64))
                <= config.derived_max_bytes,
            "projected derived recovery working budget exceeded"
        );
        if bytes.saturating_add(hot_rows.saturating_mul(512)) > config.metadata_max_bytes {
            return Err(CheckpointHeadroom.into());
        }
    }
    Ok(DerivedAppend {
        working: reserve_derived(s, config, working)?,
        resident: projected,
        accounting,
    })
}

fn empty_table(config: TableConfig, sequence: u64) -> Table {
    Table {
        creation_config: Some(config.clone()),
        config,
        created_sequence: sequence,
        segments: Vec::new(),
        receipts: BTreeMap::new(),
        rollups: BTreeMap::new(),
        cutoff_us: None,
        rollup_cutoff_us: None,
        idempotency_floor_us: None,
    }
}
#[cfg(test)]
fn serialized_map_upsert_delta<V: Serialize>(
    map: &BTreeMap<String, V>,
    key: &str,
    value: &V,
    projected_nonempty: bool,
) -> Result<i128> {
    let encoded_value = serde_json::to_vec(value)?.len() as i128;
    if let Some(previous) = map.get(key) {
        return Ok(encoded_value - serde_json::to_vec(previous)?.len() as i128);
    }
    let separator = usize::from(projected_nonempty);
    Ok((serde_json::to_vec(key)?.len() + 1 + separator) as i128 + encoded_value)
}

fn apply_encoded_delta(base: usize, delta: i128) -> Result<usize> {
    let value = (base as i128)
        .checked_add(delta)
        .context("manifest encoded-size accounting overflow")?;
    ensure!(
        value >= 0 && value <= usize::MAX as i128,
        "invalid manifest encoded-size delta"
    );
    Ok(value as usize)
}

fn checkpoint_sequence_delta(previous: u64, next: u64) -> i128 {
    i128::from(next.checked_ilog10().unwrap_or(0))
        - i128::from(previous.checked_ilog10().unwrap_or(0))
}

#[cfg(test)]
fn append_metadata_bytes(
    s: &State,
    table_name: &str,
    request_id: &str,
    receipt: &ReceiptEntry,
    updates: &BTreeMap<String, RollupRow>,
    checkpoint_sequence: u64,
) -> Result<usize> {
    let table = s.catalog.tables.get(table_name).context("unknown table")?;
    let mut delta = serialized_map_upsert_delta(
        &table.receipts,
        request_id,
        receipt,
        !table.receipts.is_empty(),
    )?;
    let mut rollups_nonempty = !table.rollups.is_empty();
    for (key, value) in updates {
        delta += serialized_map_upsert_delta(&table.rollups, key, value, rollups_nonempty)?;
        rollups_nonempty = true;
    }
    delta += checkpoint_sequence_delta(s.catalog.checkpoint_sequence, checkpoint_sequence);
    apply_encoded_delta(s.metadata_bytes, delta)
}
fn check_recovery_budget(config: &Config, s: &State) -> Result<()> {
    check_recovery_budget_view(config, &AppendView::committed(s))
}

fn check_recovery_budget_view(config: &Config, s: &AppendView<'_>) -> Result<()> {
    let _working = reserve_derived(
        s,
        config,
        s.resident().saturating_sub(s.derived_resident_bytes),
    )?;
    if uses_derived_pages(s, config) && !s.replaying {
        let bytes = s
            .derived_accounting
            .control_base
            .saturating_add(s.root_bound())
            .saturating_add(20);
        ensure!(
            bytes.saturating_add(s.hot_rows().saturating_mul(512)) <= config.metadata_max_bytes,
            "control metadata recovery byte budget exceeded"
        );
    }
    ensure!(
        uses_derived_pages(s, config)
            || s.metadata_bytes()
                .saturating_add(s.hot_rows().saturating_mul(512))
                <= config.metadata_max_bytes,
        "metadata/recovery byte budget exceeded; increase metadata_max_bytes"
    );
    ensure!(
        s.hot_bytes() <= config.hot_max_bytes && s.hot_rows() <= config.hot_max_rows,
        "recovery hot set exceeds configured budget"
    );
    ensure!(
        s.receipts() <= config.max_idempotency_keys && s.rollups() <= config.max_rollup_groups,
        "recovery metadata group limits exceeded"
    );
    Ok(())
}

pub(crate) fn check_metadata_budget(
    config: &Config,
    catalog: &Manifest,
    hot_rows: usize,
) -> Result<()> {
    ensure!(
        encode_manifest(catalog)?
            .len()
            .saturating_add(hot_rows.saturating_mul(512))
            <= config.metadata_max_bytes,
        "metadata/recovery byte budget exceeded; increase metadata_max_bytes"
    );
    Ok(())
}
fn cleanup_temporaries(root: &Path) -> Result<usize> {
    let mut removed = 0;
    for directory in [root.join("wal"), root.join("staging"), root.to_path_buf()] {
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let wal_temp =
                directory == root.join("wal") && name.starts_with('.') && name.ends_with(".tmp");
            let segment_temp = directory == root.join("staging")
                && name.starts_with("segment-")
                && name.ends_with(".tmp");
            let manifest_temp = directory == root
                && name
                    .strip_prefix('.')
                    .and_then(|n| n.strip_suffix(".tmp"))
                    .is_some_and(|n| uuid::Uuid::parse_str(n).is_ok());
            if wal_temp || segment_temp || manifest_temp {
                ensure!(
                    entry.file_type()?.is_file(),
                    "unexpected non-file temporary"
                );
                fs::remove_file(entry.path())?;
                removed += 1;
            }
        }
        wal::sync_dir(&directory)?;
    }
    Ok(removed)
}

pub(crate) fn healthy(s: &State) -> Result<()> {
    if let Some(reason) = &s.fenced {
        bail!("database fenced; reopen for recovery: {reason}");
    }
    Ok(())
}
pub(crate) fn next_sequence(s: &State) -> Result<u64> {
    s.sequence.checked_add(1).context("sequence exhausted")
}
pub(crate) fn hot_count(s: &State) -> usize {
    s.hot.values().flatten().map(|batch| batch.rows.len()).sum()
}

fn push_hot_epoch(s: &mut State, sequence: u64, first_now_us: i64) {
    debug_assert!(
        s.hot_epochs
            .last()
            .is_none_or(|epoch| epoch.sequence < sequence)
    );
    // Every physical append has at least one row. The row charge already includes
    // a fixed allocation allowance, so this queue is bounded by hot row admission.
    s.hot_epochs.push(HotEpoch {
        sequence,
        first_now_us,
    });
    s.first_hot_us = s.hot_epochs.first().map(|epoch| epoch.first_now_us);
}

fn resident_batch(rows: SharedRawRows, charged_bytes: usize) -> ResidentBatch {
    // Row::estimated_bytes includes a fixed 128-byte allocation charge per row.
    // Every batch is nonempty, so that existing charge also bounds batch nodes
    // without double-accounting a second fixed overhead.
    debug_assert!(!rows.is_empty());
    ResidentBatch::new(uuid::Uuid::new_v4().to_string(), rows, charged_bytes)
}

pub(crate) fn lock_disk_admission(inner: &Inner) -> Result<MeasuredDiskGuard<'_>> {
    inner
        .metrics
        .lock_disk(&inner.disk_admission)
        .map_err(|_| anyhow::anyhow!("disk admission lock poisoned"))
}
fn parse_timed_request_id(request_id: &str) -> Result<i64> {
    let mut parts = request_id.splitn(3, ':');
    ensure!(
        parts.next() == Some("v1"),
        "timed request_id must use v1:<issued_us>:<nonce>"
    );
    let issued = parts
        .next()
        .context("timed request_id is missing issued_us")?
        .parse::<i64>()
        .context("timed request_id issued_us is not an i64")?;
    ensure!(
        parts.next().is_some_and(|nonce| !nonce.is_empty()),
        "timed request_id nonce must not be empty"
    );
    Ok(issued)
}

pub(crate) fn advance_idempotency_floors(s: &mut State, now_us: i64) {
    for (name, table) in &s.catalog.tables {
        if let Some(window_us) = table.config.idempotency_window_us {
            let candidate = checked_cutoff(now_us, window_us);
            let floor = s
                .idempotency_floors
                .entry(name.clone())
                .or_insert(table.idempotency_floor_us.unwrap_or(i64::MIN));
            *floor = (*floor).max(candidate);
        }
    }
}

pub(crate) fn idempotency_checkpoint_due(s: &State) -> bool {
    s.catalog.tables.iter().any(|(name, table)| {
        table.config.idempotency_window_us.is_some()
            && (table
                .receipts
                .values()
                .any(|receipt| receipt.issued_us.is_none())
                || s.idempotency_floors.get(name).is_some_and(|floor| {
                    Some(*floor) != table.idempotency_floor_us
                        || table
                            .receipts
                            .values()
                            .any(|receipt| receipt.issued_us.is_some_and(|issued| issued < *floor))
                }))
    })
}

fn apply_idempotency_checkpoint(s: &State, next: &mut Manifest) {
    for (name, table) in &mut next.tables {
        let Some(window_us) = table.config.idempotency_window_us else {
            continue;
        };
        table
            .receipts
            .retain(|_, receipt| receipt.issued_us.is_some());
        if let Some(floor) = s.idempotency_floors.get(name).copied() {
            let floor = floor.max(table.idempotency_floor_us.unwrap_or(i64::MIN));
            table.idempotency_floor_us = Some(floor);
            table
                .receipts
                .retain(|_, receipt| receipt.issued_us.is_some_and(|issued| issued >= floor));
        }
        debug_assert!(window_us > 0);
    }
}

#[derive(Debug)]
struct OffLockCheckpoint;

impl std::fmt::Display for OffLockCheckpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("group requires an off-lock checkpoint")
    }
}

impl std::error::Error for OffLockCheckpoint {}

#[derive(Debug)]
struct CheckpointHeadroom;

impl std::fmt::Display for CheckpointHeadroom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("projected checkpoint exceeds metadata byte budget")
    }
}

impl std::error::Error for CheckpointHeadroom {}

struct PreparedAppend<D = DerivedAppend> {
    derived: D,
    table: String,
    request_id: String,
    receipt: ReceiptEntry,
    updates: BTreeMap<String, RollupRow>,
    batch: Option<ResidentBatch>,
    bytes: usize,
    metadata_bytes: usize,
}
impl PreparedAppend {
    fn working_bytes(&self) -> usize {
        let proof_bytes = self
            .receipt
            .group_fingerprint
            .as_ref()
            .map_or(0, String::len);
        self.updates.iter().fold(proof_bytes, |bytes, (key, row)| {
            bytes
                .saturating_add(key.len().saturating_mul(2))
                .saturating_add(row.tenant.len().saturating_mul(2))
                .saturating_add(row.series.len().saturating_mul(2))
                .saturating_add(
                    row.tags
                        .iter()
                        .map(|(k, v)| (k.len() + v.len() + 128) * 2)
                        .sum::<usize>(),
                )
                .saturating_add(1024)
        })
    }
}
#[cfg(test)]
struct AppendUndo {
    _derived_working: DerivedWorking,
    derived_resident_bytes: usize,
    derived_accounting: TableAccounting,
    table: String,
    request_id: String,
    rollups: BTreeMap<String, Option<RollupRow>>,
    hot_len: Option<usize>,
    hot_bytes: usize,
    metadata_bytes: usize,
}

fn push_scan_row(
    output: &mut Vec<StoredRow>,
    bytes: &mut usize,
    credit: &mut crate::raw_memory::RawReservation,
    row: &StoredRow,
    limit: usize,
) -> Result<()> {
    let next = bytes
        .checked_add(row.row.estimated_bytes())
        .context("scan byte accounting overflow")?;
    ensure!(next <= limit, "scan output budget exceeded");
    credit.resize(raw_memory::row_charge(next))?;
    output.push(row.clone());
    *bytes = next;
    Ok(())
}

type OwnedGroupRequest = Result<PreparedWrite>;

#[cfg(all(test, feature = "fault-injection"))]
fn prepared_test_input(item: wal::AppendItem, hint: usize) -> Result<PreparedWrite> {
    let expected_digest = item.digest;
    let input = AdmittedWrite::new(WriteRequest {
        table: item.table,
        request_id: item.request_id,
        rows: item.rows,
        now_us: item.now_us.expect("live test clock"),
    })?
    .prepare(&Config::default())?;
    assert_eq!(input.digest, expected_digest);
    Ok(input.with_test_admission_hint(hint))
}

fn duplicate_clock_floor(s: &State, item: &wal::AppendItem) -> Option<i64> {
    s.catalog
        .tables
        .get(&item.table)?
        .config
        .idempotency_window_us
        .map(|window| checked_cutoff(item.now_us.expect("validated duplicate clock"), window))
}

// A retry root must remain compatible with the ordered new records it may later
// precede. Only independently validated duplicate clocks may enter that root,
// capped by clock-eligible pending IDs. Full duplicate clocks are applied at
// their staging positions and on terminal return, not used as rollback floors.
fn group_checkpoint_floors(
    s: &State,
    requests: &[OwnedGroupRequest],
    durable: &[Option<Result<WriteReceipt>>],
) -> BTreeMap<String, Option<i64>> {
    let mut baseline = BTreeMap::new();
    for item in requests.iter().flatten() {
        if s.catalog
            .tables
            .get(&item.table)
            .is_some_and(|table| table.config.idempotency_window_us.is_some())
        {
            baseline.insert(
                item.table.clone(),
                s.idempotency_floors.get(&item.table).copied(),
            );
        }
    }
    let mut simulated: BTreeMap<_, _> = baseline
        .iter()
        .filter_map(|(table, floor)| floor.map(|floor| (table.clone(), floor)))
        .collect();
    let mut ceilings: BTreeMap<String, i64> = BTreeMap::new();
    let mut duplicate_floors: BTreeMap<String, i64> = BTreeMap::new();
    for (request, outcome) in requests.iter().zip(durable) {
        let Ok(item) = request else {
            continue;
        };
        if let Some(outcome) = outcome {
            if outcome.is_ok()
                && let Some(floor) = duplicate_clock_floor(s, item)
            {
                let entry = simulated.entry(item.table.clone()).or_insert(i64::MIN);
                *entry = (*entry).max(floor);
                let entry = duplicate_floors
                    .entry(item.table.clone())
                    .or_insert(i64::MIN);
                *entry = (*entry).max(floor);
            } else if outcome.is_err()
                && baseline.contains_key(&item.table)
                && let Some(table) = s.catalog.tables.get(&item.table)
                && let Some(receipt) = table.receipts.get(&item.request_id)
                && retry_with_receipt(
                    table,
                    Some(receipt),
                    s.idempotency_floors.get(&item.table).copied(),
                    item,
                    &item.digest,
                )
                .is_ok()
            {
                // The clock-only prepass cannot see same-epoch conflicts. Keep
                // an independently valid durable receipt available for the real
                // ordered overlay's recheck after this root. This is NOT a
                // success proof: earlier accepted clocks may still reject it.
                // Invalid digest/own-clock/baseline-floor retries cannot pin GC.
                let issued = parse_timed_request_id(&item.request_id).expect("validated timed ID");
                let ceiling = ceilings.entry(item.table.clone()).or_insert(i64::MAX);
                *ceiling = (*ceiling).min(issued);
            }
        } else if group_retry_with_floors(&s.catalog, &mut simulated, item, &item.digest)
            .is_ok_and(|receipt| receipt.is_none())
            && baseline.contains_key(&item.table)
        {
            // The typed validation above checked syntax, the current floor and
            // future skew. An already invalid input must not constrain later work.
            let issued = parse_timed_request_id(&item.request_id).expect("validated timed ID");
            let ceiling = ceilings.entry(item.table.clone()).or_insert(i64::MAX);
            *ceiling = (*ceiling).min(issued);
        }
    }
    for (table, floor) in duplicate_floors {
        let safe = floor.min(ceilings.get(&table).copied().unwrap_or(i64::MAX));
        let entry = baseline.entry(table).or_default();
        // The baseline is fresh under State on every attempt. Never lower an
        // external/live or persisted floor, including after off-lock preparation.
        *entry = Some(entry.unwrap_or(i64::MIN).max(safe));
    }
    baseline
}

// Explicit retry/checkpoint policy, never a preparation rollback. Call only
// under State with floor scalars derived from its current committed baseline.
fn restore_group_floors(s: &mut State, floors: &BTreeMap<String, Option<i64>>) {
    for (table, floor) in floors {
        if let Some(floor) = floor {
            s.idempotency_floors.insert(table.clone(), *floor);
        } else {
            s.idempotency_floors.remove(table);
        }
    }
}

fn commit_group_duplicate_floors(
    s: &mut State,
    requests: &[OwnedGroupRequest],
    durable: &[Option<Result<WriteReceipt>>],
) {
    for (request, outcome) in requests.iter().zip(durable) {
        if outcome.as_ref().is_some_and(Result::is_ok) {
            let item = request.as_ref().expect("durable input");
            if let Some(floor) = duplicate_clock_floor(s, item) {
                let live = s
                    .idempotency_floors
                    .entry(item.table.clone())
                    .or_insert(i64::MIN);
                *live = (*live).max(floor);
            }
        }
    }
}

fn resolve_group_durable(
    s: &State,
    requests: &[OwnedGroupRequest],
    durable: &mut [Option<Result<WriteReceipt>>],
) {
    // Simulate clock validation in input order with only touched floor scalars.
    // Do not advance live floors ahead of earlier new inputs: a later clock can
    // reject a later old retry, but must not retroactively reject an earlier row.
    let mut floors: BTreeMap<_, _> = requests
        .iter()
        .flatten()
        .filter_map(|item| {
            s.idempotency_floors
                .get(&item.table)
                .map(|floor| (item.table.clone(), *floor))
        })
        .collect();
    for (request, outcome) in requests.iter().zip(durable) {
        let item = match request {
            Ok(item) => item,
            Err(error) => {
                *outcome = Some(Err(anyhow::anyhow!("{error:#}")));
                continue;
            }
        };
        if let Some(result) = outcome {
            if result.is_ok()
                && let Some(floor) = duplicate_clock_floor(s, item)
            {
                let entry = floors.entry(item.table.clone()).or_insert(i64::MIN);
                *entry = (*entry).max(floor);
            }
            continue;
        }
        let known = s
            .catalog
            .tables
            .get(&item.table)
            .is_none_or(|table| table.receipts.contains_key(&item.request_id));
        let result = group_retry_with_floors(&s.catalog, &mut floors, item, &item.digest);
        if known {
            *outcome = Some(result.map(|receipt| receipt.expect("existing durable receipt")));
        }
    }
}

// Initial pressure can fail before any actual overlay acceptance. In that case
// hypothetical clocks from new requests (including same-ID conflicts invisible
// to the cheap prepass) have no publication authority. Recheck ONLY existing
// durable receipts against committed floors and ordered durable retries. New
// IDs stay failed; digest, own-clock and persisted-floor checks remain exact.
fn resolve_terminal_durable(
    s: &State,
    requests: &[OwnedGroupRequest],
    durable: &mut [Option<Result<WriteReceipt>>],
) {
    let mut floors = BTreeMap::new();
    for (request, outcome) in requests.iter().zip(durable) {
        let Ok(item) = request else { continue };
        let Some(table) = s.catalog.tables.get(&item.table) else {
            continue;
        };
        if !table.receipts.contains_key(&item.request_id) {
            continue;
        }
        if let Some(floor) = s.idempotency_floors.get(&item.table) {
            let live = floors.entry(item.table.clone()).or_insert(*floor);
            *live = (*live).max(*floor);
        }
        if outcome.as_ref().is_some_and(Result::is_ok) {
            if let Some(floor) = duplicate_clock_floor(s, item) {
                let live = floors.entry(item.table.clone()).or_insert(floor);
                *live = (*live).max(floor);
            }
        } else {
            *outcome = Some(
                group_retry_with_floors(&s.catalog, &mut floors, item, &item.digest)
                    .map(|receipt| receipt.expect("existing durable receipt")),
            );
        }
    }
}

fn group_retry(
    s: &mut State,
    request: &wal::AppendItem,
    digest: &str,
) -> Result<Option<WriteReceipt>> {
    group_retry_with_floors(&s.catalog, &mut s.idempotency_floors, request, digest)
}

fn group_retry_with_floors(
    catalog: &Manifest,
    floors: &mut BTreeMap<String, i64>,
    request: &wal::AppendItem,
    digest: &str,
) -> Result<Option<WriteReceipt>> {
    let table = catalog
        .tables
        .get(&request.table)
        .context("unknown table")?;
    let (receipt, floor) = retry_with_receipt(
        table,
        table.receipts.get(&request.request_id),
        floors.get(&request.table).copied(),
        request,
        digest,
    )?;
    if let Some(floor) = floor {
        floors.insert(request.table.clone(), floor);
    }
    Ok(receipt)
}

fn retry_with_receipt(
    table: &Table,
    receipt: Option<&ReceiptEntry>,
    previous_floor: Option<i64>,
    request: &wal::AppendItem,
    digest: &str,
) -> Result<(Option<WriteReceipt>, Option<i64>)> {
    let now_us = request.now_us.context("live group request missing clock")?;
    let floor = if let Some(window) = table.config.idempotency_window_us {
        let issued = parse_timed_request_id(&request.request_id)?;
        let floor = previous_floor
            .unwrap_or(i64::MIN)
            .max(checked_cutoff(now_us, window));
        ensure!(
            issued >= floor,
            "request_id is outside the idempotency window"
        );
        ensure!(
            receipt.is_some() || issued <= now_us.saturating_add(IDEMPOTENCY_MAX_FUTURE_SKEW_US),
            "request_id issue time exceeds the future-skew limit"
        );
        Some(floor)
    } else {
        None
    };
    let duplicate = if let Some(receipt) = receipt {
        ensure!(
            receipt.digest == digest,
            "request_id conflicts with different data"
        );
        Some(receipt_for(receipt, true))
    } else {
        None
    };
    Ok((duplicate, floor))
}

fn prepare_group_append(
    s: &State,
    item: &wal::AppendItem,
    sequence: u64,
    ordinal: usize,
    group_fingerprint: Option<&str>,
    config: &Config,
    metrics: Option<&Metrics>,
) -> Result<PreparedAppend> {
    prepare_validated_append(
        s,
        ValidatedAppend::recovered(item, config)?,
        sequence,
        ordinal,
        group_fingerprint,
        config,
        metrics,
    )
}

#[cfg(test)]
fn prepare_live_append(
    s: &State,
    item: &PreparedWrite,
    sequence: u64,
    ordinal: usize,
    group_fingerprint: Option<&str>,
    config: &Config,
    metrics: Option<&Metrics>,
) -> Result<PreparedAppend> {
    prepare_validated_append(
        s,
        item.validated(),
        sequence,
        ordinal,
        group_fingerprint,
        config,
        metrics,
    )
}

fn prepare_validated_append(
    s: &State,
    input: ValidatedAppend<'_>,
    sequence: u64,
    ordinal: usize,
    group_fingerprint: Option<&str>,
    config: &Config,
    metrics: Option<&Metrics>,
) -> Result<PreparedAppend> {
    preflight_append(
        &AppendView::committed(s),
        input,
        sequence,
        ordinal,
        group_fingerprint,
        config,
        metrics,
    )?
    .finish(&AppendView::committed(s), config)
    .and_then(|mut prepared| {
        let reservation = s
            .raw_memory
            .reserve(raw_memory::row_charge(prepared.bytes))?;
        let stored = SharedRawRows::build(reservation, || {
            Ok(input
                .item()
                .rows
                .iter()
                .cloned()
                .enumerate()
                .map(|(index, row)| StoredRow {
                    row,
                    sequence,
                    ordinal: (ordinal + index) as u32,
                })
                .collect())
        })?;
        prepared.batch = Some(resident_batch(stored, prepared.bytes));
        Ok(prepared)
    })
}

impl PreparedAppend<Result<DerivedProjection>> {
    fn finish(self, s: &AppendView<'_>, config: &Config) -> Result<PreparedAppend> {
        let derived = check_derived_append_view(
            s,
            config,
            &self.table,
            self.derived,
            s.hot_rows() + self.receipt.rows,
        )?;
        Ok(PreparedAppend {
            derived,
            table: self.table,
            request_id: self.request_id,
            receipt: self.receipt,
            updates: self.updates,
            batch: self.batch,
            bytes: self.bytes,
            metadata_bytes: self.metadata_bytes,
        })
    }
}

fn preflight_append(
    s: &AppendView<'_>,
    input: ValidatedAppend<'_>,
    sequence: u64,
    ordinal: usize,
    group_fingerprint: Option<&str>,
    config: &Config,
    metrics: Option<&Metrics>,
) -> Result<PreparedAppend<Result<DerivedProjection>>> {
    input.check_limits(config, ordinal)?;
    let item = input.item();
    let table = s
        .catalog
        .tables
        .get(&item.table)
        .context("WAL references unknown table")?;
    ensure!(
        s.receipt(&item.table, &item.request_id).is_none(),
        "duplicate request in committed WAL"
    );
    let issued_us = if table.config.idempotency_window_us.is_some() {
        let issued = parse_timed_request_id(&item.request_id)?;
        ensure!(
            table
                .idempotency_floor_us
                .is_none_or(|floor| issued >= floor),
            "WAL request_id precedes the durable idempotency floor"
        );
        Some(issued)
    } else {
        None
    };
    for row in &item.rows {
        window_start(row.timestamp_us, table.config.window_us)?;
        ensure!(
            table
                .cutoff_us
                .is_none_or(|cutoff| row.timestamp_us >= cutoff),
            "WAL violates retention cutoff"
        );
    }
    let bytes = input.resident_bytes();
    ensure!(
        s.hot_bytes().saturating_add(bytes) <= config.hot_max_bytes
            && s.hot_rows().saturating_add(item.rows.len()) <= config.hot_max_rows,
        "group exceeds hot-tier capacity"
    );
    ensure!(
        s.receipts() < config.max_idempotency_keys,
        "idempotency registry full; refusing to forget committed request IDs"
    );
    let update_budget = if uses_derived_pages(s, config) {
        config.derived_max_bytes
    } else {
        config.metadata_max_bytes
    };
    let updates = aggregate_updates(table, &item.rows, sequence, ordinal, update_budget, |key| {
        s.rollup(&item.table, key)
    })?;
    let new_groups = updates
        .keys()
        .filter(|key| s.rollup(&item.table, key).is_none())
        .count();
    ensure!(
        s.rollups().saturating_add(new_groups) <= config.max_rollup_groups,
        "rollup state admission limit"
    );
    let receipt = ReceiptEntry {
        sequence,
        rows: item.rows.len(),
        digest: item.digest.clone(),
        issued_us,
        group_fingerprint: group_fingerprint.map(str::to_owned),
    };
    let projection = project_append_accounting_view(
        s,
        &item.table,
        &item.request_id,
        &receipt,
        &updates,
        metrics,
    )?;
    let metadata_bytes = projection.metadata_bytes_view(s, s.catalog.checkpoint_sequence)?;
    // The checkpoint projection changes only the sequence token's encoded width.
    let projected = apply_encoded_delta(
        metadata_bytes,
        checkpoint_sequence_delta(s.catalog.checkpoint_sequence, sequence),
    )?;
    if !uses_derived_pages(s, config)
        && projected.saturating_add(
            s.hot_rows()
                .saturating_add(item.rows.len())
                .saturating_mul(512),
        ) > config.metadata_max_bytes
    {
        return Err(CheckpointHeadroom.into());
    }
    Ok(PreparedAppend {
        derived: projection.derived,
        table: item.table.clone(),
        request_id: item.request_id.clone(),
        receipt,
        updates,
        batch: None,
        bytes,
        metadata_bytes,
    })
}

#[cfg(test)]
thread_local! {
    static GROUP_APPEND_APPLICATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn install_recovered_append(s: &mut State, prepared: PreparedAppend) {
    let mut overlay = AppendOverlay::new(s);
    overlay.push(prepared);
    overlay.into_pending().install(s);
}

// Legacy mutation/undo remains only as an independent test oracle.
#[cfg(test)]
fn apply_group_append(s: &mut State, prepared: PreparedAppend) -> AppendUndo {
    #[cfg(test)]
    GROUP_APPEND_APPLICATIONS.with(|count| count.set(count.get() + 1));
    let mut undo = AppendUndo {
        _derived_working: prepared.derived.working,
        derived_resident_bytes: s.derived_resident_bytes,
        derived_accounting: s
            .derived_accounting
            .tables
            .get(&prepared.table)
            .expect("prepared accounting")
            .clone(),
        table: prepared.table.clone(),
        request_id: prepared.request_id.clone(),
        rollups: BTreeMap::new(),
        hot_len: s.hot.get(&prepared.table).map(Vec::len),
        hot_bytes: s.hot_bytes,
        metadata_bytes: s.metadata_bytes,
    };
    let index = s.rollup_indexes.entry(prepared.table.clone()).or_default();
    for (key, row) in &prepared.updates {
        index
            .insert(key, row, usize::MAX)
            .expect("prevalidated rollup index");
    }
    let table = s
        .catalog
        .tables
        .get_mut(&prepared.table)
        .expect("prepared table");
    for (key, value) in prepared.updates {
        let previous = table.rollups.insert(key.clone(), value);
        undo.rollups.insert(key, previous);
    }
    table.receipts.insert(prepared.request_id, prepared.receipt);
    s.hot
        .entry(prepared.table.clone())
        .or_default()
        .push(prepared.batch.expect("materialized recovery/test append"));
    s.hot_bytes += prepared.bytes;
    s.metadata_bytes = prepared.metadata_bytes;
    s.derived_resident_bytes = prepared.derived.resident;
    s.derived_accounting
        .replace(prepared.table, prepared.derived.accounting);
    undo
}

#[cfg(test)]
fn undo_group_append(s: &mut State, undo: AppendUndo) {
    let table = s.catalog.tables.get_mut(&undo.table).expect("staged table");
    table.receipts.remove(&undo.request_id);
    for (key, previous) in undo.rollups {
        if let Some(value) = previous {
            table.rollups.insert(key, value);
        } else {
            if let Some(row) = table.rollups.remove(&key) {
                s.rollup_indexes
                    .get_mut(&undo.table)
                    .expect("staged index")
                    .remove(&key, &row);
            }
        }
    }
    if let Some(len) = undo.hot_len {
        s.hot.get_mut(&undo.table).unwrap().truncate(len);
    } else {
        s.hot.remove(&undo.table);
    }
    s.hot_bytes = undo.hot_bytes;
    s.metadata_bytes = undo.metadata_bytes;
    s.derived_resident_bytes = undo.derived_resident_bytes;
    s.derived_accounting
        .replace(undo.table, undo.derived_accounting);
}

fn receipt_for(r: &ReceiptEntry, duplicate: bool) -> WriteReceipt {
    WriteReceipt {
        sequence: r.sequence,
        rows: r.rows,
        duplicate,
        durability: "local_fsync".into(),
    }
}

pub(crate) fn commit_record(inner: &Inner, s: &mut State, record: &wal::Record) -> Result<()> {
    if !matches!(
        &record.operation,
        wal::Operation::Append { .. } | wal::Operation::AppendGroup { .. }
    ) {
        s.control_epoch
            .checked_add(1)
            .context("control epoch exhausted")?;
    }
    let encoded = prepare_record(inner, s, record)?;
    publish_record(inner, s, &encoded)
}

fn prepare_record(
    inner: &Inner,
    s: &mut State,
    record: &wal::Record,
) -> Result<wal::EncodedRecord> {
    let encode_timer = inner.metrics.timer(Phase::WalEncode);
    let encoded = wal::EncodedRecord::new(record)?;
    drop(encode_timer);
    let bytes = commit_log::required_bytes(inner, &encoded)?;
    ensure!(
        bytes <= inner.config.wal_max_bytes,
        "batch exceeds WAL capacity"
    );
    if s.wal_bytes.saturating_add(bytes) > inner.config.wal_max_bytes {
        checkpoint_locked(inner, s)?;
    }
    Ok(encoded)
}

// Complete temporary bytes stay charged across both syncs. Namespace changes
// still need disk admission: a walk may already hold the temporary's DirEntry.
fn append_disk_barrier<'a>(
    inner: &'a Inner,
    disk: &mut Option<MeasuredDiskGuard<'a>>,
    barrier: wal::AppendBarrier,
) -> Result<()> {
    match barrier {
        wal::AppendBarrier::FileSync | wal::AppendBarrier::DirectorySync => drop(disk.take()),
        wal::AppendBarrier::Rename | wal::AppendBarrier::TempCleanup => {
            // Failures before FileSync or after acquiring for Rename still own
            // admission. Never recursively acquire that non-reentrant mutex.
            if disk.is_none() {
                let wait = inner.metrics.timer(Phase::WalDiskLockWait);
                *disk = Some(lock_disk_admission(inner)?);
                drop(wait);
            }
        }
    }
    Ok(())
}

fn publish_append(db: &Database, encoded: &wal::EncodedRecord) -> Result<Result<usize>> {
    let inner = &db.inner;
    if inner.journal.is_some() {
        return commit_log::append(inner, encoded, Some(db));
    }
    let wait = inner.metrics.timer(Phase::WalDiskLockWait);
    let mut disk = Some(lock_disk_admission(inner)?);
    drop(wait);
    ensure_budget(inner, encoded.len() as u64)?;
    Ok(wal::append_encoded_with_barriers(
        &inner.root,
        encoded,
        Some(&inner.metrics),
        |barrier| {
            append_disk_barrier(inner, &mut disk, barrier)?;
            #[cfg(feature = "fault-injection")]
            match barrier {
                wal::AppendBarrier::FileSync => {
                    db.block_maintenance_test_hook(MaintenanceHookPhase::WalBeforeSync)?;
                }
                wal::AppendBarrier::DirectorySync => {
                    db.block_maintenance_test_hook(MaintenanceHookPhase::WalBeforeDirectorySync)?;
                }
                wal::AppendBarrier::Rename | wal::AppendBarrier::TempCleanup => {}
            }
            Ok(())
        },
    ))
}

fn publish_record(inner: &Inner, s: &mut State, encoded: &wal::EncodedRecord) -> Result<()> {
    if inner.journal.is_some() {
        return match commit_log::append(inner, encoded, None)? {
            Ok(size) => {
                s.wal_bytes += size as u64;
                Ok(())
            }
            Err(error) => {
                s.fenced = Some(format!("ambiguous journal publication: {error:#}"));
                Err(error)
            }
        };
    }
    let wal_disk_wait = inner.metrics.timer(Phase::WalDiskLockWait);
    let _disk = lock_disk_admission(inner)?;
    drop(wal_disk_wait);
    ensure_budget(inner, encoded.len() as u64)?;
    match wal::append_encoded(&inner.root, encoded, Some(&inner.metrics)) {
        Ok(size) => {
            s.wal_bytes += size as u64;
            Ok(())
        }
        Err(e) => {
            s.fenced = Some(format!("ambiguous WAL publication: {e:#}"));
            Err(e)
        }
    }
}

pub(crate) fn replay(s: &mut State, record: wal::Record, config: &Config) -> Result<()> {
    replay_with_metrics(s, record, config, None)
}

fn replay_with_metrics(
    s: &mut State,
    record: wal::Record,
    config: &Config,
    metrics: Option<&Metrics>,
) -> Result<()> {
    let fingerprint = wal::group_fingerprint(&record)?;
    apply_record(s, record, config, fingerprint.as_deref(), metrics)
}

fn apply_record(
    s: &mut State,
    record: wal::Record,
    config: &Config,
    group_fingerprint: Option<&str>,
    metrics: Option<&Metrics>,
) -> Result<()> {
    ensure!(record.sequence == next_sequence(s)?, "noncontiguous replay");
    if matches!(&record.operation, wal::Operation::AppendGroup { .. }) {
        ensure!(
            wal::encode(&record)?.len() <= config.max_batch_bytes,
            "WAL group exceeds recovery byte budget"
        );
    }
    let mut append_applied = false;
    let mut append_now_us = None;
    match record.operation {
        wal::Operation::CreateTable {
            name,
            config: table_config,
        } => {
            validate_name(&name)?;
            table_config.validate()?;
            ensure!(
                !s.catalog.tables.contains_key(&name)
                    && !s.catalog.continuous_aggregates.contains_key(&name)
                    && !s.catalog.jobs.contains_key(&name),
                "duplicate or colliding table creation in WAL"
            );
            ensure!(
                s.catalog.tables.len() < config.max_tables,
                "table recovery budget exceeded"
            );
            s.raw_stamps.insert(name.clone(), fresh_raw_stamp());
            s.catalog
                .tables
                .insert(name, empty_table(table_config, record.sequence));
        }
        wal::Operation::Append {
            table,
            request_id,
            digest,
            rows,
        } => {
            let item = wal::AppendItem {
                table,
                request_id,
                digest,
                rows,
                now_us: None,
            };
            let prepared =
                prepare_group_append(s, &item, record.sequence, 0, None, config, metrics)?;
            install_recovered_append(s, prepared);
            append_applied = true;
            append_now_us = Some(i64::MIN);
        }
        wal::Operation::AppendGroup { items } => {
            ensure!(
                group_fingerprint.is_some(),
                "group proof required for replay"
            );
            ensure!(
                !items.is_empty() && items.len() <= wal::MAX_GROUP_REQUESTS,
                "invalid WAL group request count"
            );
            append_now_us = Some(
                items
                    .first()
                    .and_then(|item| item.now_us)
                    .unwrap_or(i64::MIN),
            );
            let mut ordinal = 0usize;
            for item in items {
                let count = item.rows.len();
                ensure!(
                    ordinal.saturating_add(count) <= config.max_batch_rows,
                    "WAL group exceeds recovery row budget"
                );
                let prepared = prepare_group_append(
                    s,
                    &item,
                    record.sequence,
                    ordinal,
                    group_fingerprint,
                    config,
                    metrics,
                )?;
                install_recovered_append(s, prepared);
                ordinal += count;
                check_recovery_budget(config, s)?;
            }
            append_applied = true;
        }
        operation @ (wal::Operation::SetPolicy { .. }
        | wal::Operation::CreateContinuousAggregate { .. }
        | wal::Operation::DropContinuousAggregate { .. }
        | wal::Operation::PutJob { .. }
        | wal::Operation::DropJob { .. }
        | wal::Operation::JobStarted { .. }
        | wal::Operation::JobFinished { .. }) => {
            crate::control::replay_control_operation(s, record.sequence, operation)?;
        }
    }
    s.sequence = record.sequence;
    s.generation = s
        .generation
        .checked_add(1)
        .context("state generation exhausted")?;
    if append_applied {
        push_hot_epoch(s, record.sequence, append_now_us.unwrap_or(i64::MIN));
    } else {
        s.control_epoch = s
            .control_epoch
            .checked_add(1)
            .context("control epoch exhausted")?;
        let index_bytes = derived_root::resident_bytes(&s.catalog, true)
            .saturating_sub(derived_root::resident_bytes(&s.catalog, false));
        let _working = reserve_derived(s, config, index_bytes)?;
        s.rollup_indexes = build_rollup_indexes(&s.catalog, config)?;
        s.metadata_bytes = logical_metadata_bytes(&s.catalog)?;
        s.derived_resident_bytes = derived_root::resident_bytes(&s.catalog, true);
        s.derived_accounting = DerivedAccounting::build(&s.catalog, config)?;
    }
    Ok(())
}

fn aggregate_updates<'a>(
    t: &Table,
    rows: &[Row],
    sequence: u64,
    ordinal: usize,
    byte_budget: usize,
    lookup: impl Fn(&str) -> Option<&'a RollupRow>,
) -> Result<BTreeMap<String, RollupRow>> {
    let mut updates = BTreeMap::new();
    let mut update_bytes = 0usize;
    for (index, row) in rows.iter().enumerate() {
        let ordinal = (ordinal + index) as u32;
        for width in &t.config.rollup_widths_us {
            let bucket = window_start(row.timestamp_us, *width)?;
            if t.rollup_cutoff_us
                .is_some_and(|cutoff| bucket.saturating_add(*width) <= cutoff)
            {
                continue;
            }
            let key =
                serde_json::to_string(&(*width, bucket, &row.tenant, &row.series, &row.tags))?;
            if !updates.contains_key(&key) {
                update_bytes = update_bytes
                    .saturating_add(key.len())
                    .saturating_add(row.estimated_bytes().saturating_mul(2))
                    .saturating_add(256);
                ensure!(
                    update_bytes <= byte_budget,
                    "rollup update metadata byte budget exceeded"
                );
            }
            if let Some(agg) = updates.get_mut(&key) {
                add_borrowed_rollup(agg, row, sequence, ordinal)?;
            } else if let Some(existing) = lookup(&key) {
                let mut agg = existing.clone();
                add_borrowed_rollup(&mut agg, row, sequence, ordinal)?;
                updates.insert(key, agg);
            } else {
                updates.insert(
                    key,
                    RollupRow {
                        width_us: *width,
                        bucket_us: bucket,
                        tenant: row.tenant.clone(),
                        series: row.series.clone(),
                        tags: row.tags.clone(),
                        count: 1,
                        sum: row.value,
                        min: row.value,
                        max: row.value,
                        first: row.value,
                        last: row.value,
                        first_timestamp_us: row.timestamp_us,
                        last_timestamp_us: row.timestamp_us,
                        first_sequence: sequence,
                        last_sequence: sequence,
                        first_ordinal: ordinal,
                        last_ordinal: ordinal,
                    },
                );
            }
        }
    }
    Ok(updates)
}

// Mirrors RollupRow::add without an owning StoredRow just to borrow its payload.
fn add_borrowed_rollup(agg: &mut RollupRow, row: &Row, sequence: u64, ordinal: u32) -> Result<()> {
    let sum = agg.sum + row.value;
    ensure!(sum.is_finite(), "rollup sum overflow");
    agg.count = agg.count.checked_add(1).context("rollup count overflow")?;
    agg.sum = sum;
    agg.min = agg.min.min(row.value);
    agg.max = agg.max.max(row.value);
    let key = (row.timestamp_us, sequence, ordinal);
    if key
        < (
            agg.first_timestamp_us,
            agg.first_sequence,
            agg.first_ordinal,
        )
    {
        agg.first = row.value;
        agg.first_timestamp_us = row.timestamp_us;
        agg.first_sequence = sequence;
        agg.first_ordinal = ordinal;
    }
    if key > (agg.last_timestamp_us, agg.last_sequence, agg.last_ordinal) {
        agg.last = row.value;
        agg.last_timestamp_us = row.timestamp_us;
        agg.last_sequence = sequence;
        agg.last_ordinal = ordinal;
    }
    Ok(())
}

pub(crate) fn encode_manifest(catalog: &Manifest) -> Result<Vec<u8>> {
    let mut bytes = b"VARVEM01".to_vec();
    bytes.extend(serde_json::to_vec(catalog)?);
    ensure!(
        bytes.len() + 32 <= wal::MAX_FRAME_BYTES,
        "manifest exceeds format size limit"
    );
    let hash = blake3::hash(&bytes);
    bytes.extend(hash.as_bytes());
    Ok(bytes)
}
pub(crate) fn decode_manifest(bytes: &[u8]) -> Result<Manifest> {
    ensure!(
        bytes.len() >= 40 && bytes.len() <= wal::MAX_FRAME_BYTES && &bytes[..8] == b"VARVEM01",
        "invalid manifest magic/size/version"
    );
    let (payload, digest) = bytes.split_at(bytes.len() - 32);
    ensure!(
        blake3::hash(payload).as_bytes() == digest,
        "manifest checksum mismatch"
    );
    let catalog: Manifest = serde_json::from_slice(&bytes[8..bytes.len() - 32])?;
    validate_manifest(&catalog)?;
    Ok(catalog)
}
pub(crate) fn validate_manifest(c: &Manifest) -> Result<()> {
    ensure!(
        c.format_version == FORMAT_VERSION,
        "unsupported manifest version"
    );
    uuid::Uuid::parse_str(&c.database_id)?;
    crate::control::validate_control_manifest(c)?;
    for (name, t) in &c.tables {
        validate_name(name)?;
        t.config.validate()?;
        if let Some(created) = &t.creation_config {
            created.validate()?;
            ensure!(
                created.shards == t.config.shards && created.window_us == t.config.window_us,
                "immutable table partitioning changed"
            );
        }
        ensure!(
            t.created_sequence > 0 && t.created_sequence <= c.checkpoint_sequence,
            "invalid table creation sequence"
        );
        for (key, row) in &t.rollups {
            derived::validate_rollup(key, row)?;
        }
        let mut identities = BTreeSet::new();
        for seg in &t.segments {
            ensure!(
                seg.id.len() == 64
                    && seg
                        .id
                        .bytes()
                        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
                "invalid segment digest"
            );
            ensure!(identities.insert(&seg.id), "duplicate manifest segment");
            ensure!(
                seg.rows > 0
                    && seg.bytes > 0
                    && seg.bytes <= wal::MAX_FRAME_BYTES as u64
                    && seg.decoded_bytes > 0
                    && seg.shard < t.config.shards
                    && seg.min_timestamp_us <= seg.max_timestamp_us,
                "invalid segment metadata"
            );
            ensure!(
                window_start(seg.min_timestamp_us, t.config.window_us)? == seg.window_us
                    && window_start(seg.max_timestamp_us, t.config.window_us)? == seg.window_us,
                "segment crosses time window"
            );
        }
        ensure!(
            t.idempotency_floor_us.is_none() || t.config.idempotency_window_us.is_some(),
            "idempotency floor persisted for a table without a window"
        );
        for (id, receipt) in &t.receipts {
            validate_request_id(id)?;
            ensure!(
                receipt.group_fingerprint.as_ref().is_none_or(|proof| {
                    proof.len() == 64
                        && proof
                            .bytes()
                            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                }),
                "invalid group receipt fingerprint"
            );
            ensure!(
                receipt.sequence <= c.checkpoint_sequence
                    && receipt.sequence > 0
                    && receipt.rows > 0,
                "invalid receipt sequence"
            );
            match receipt.issued_us {
                Some(issued_us) => {
                    ensure!(
                        t.config.idempotency_window_us.is_some()
                            && parse_timed_request_id(id)? == issued_us
                            && t.idempotency_floor_us
                                .is_none_or(|floor| issued_us >= floor),
                        "invalid timed idempotency receipt"
                    );
                }
                None => ensure!(
                    t.idempotency_floor_us.is_none(),
                    "legacy receipt retained below a timed idempotency floor"
                ),
            }
        }
    }
    Ok(())
}

#[derive(Clone)]
struct RootStamp {
    generation: u64,
    sequence: u64,
    idempotency_floors: BTreeMap<String, i64>,
}

impl RootStamp {
    fn capture(s: &State) -> Self {
        Self {
            generation: s.generation,
            sequence: s.sequence,
            idempotency_floors: s.idempotency_floors.clone(),
        }
    }

    fn matches(&self, s: &State) -> bool {
        self.generation == s.generation
            && self.sequence == s.sequence
            && self.idempotency_floors == s.idempotency_floors
    }
}

pub(crate) struct RootPreparation {
    pub next: Manifest,
    stamp: RootStamp,
    pages: bool,
    resident_limit: usize,
    working: DerivedWorking,
}

pub(crate) fn capture_root(s: &State, config: &Config) -> Result<RootPreparation> {
    let pages = uses_derived_pages(s, config);
    let workspace = if pages {
        config.derived_page_bytes.saturating_mul(4)
    } else {
        0
    };
    // Reserve maps AND replacement indexes before cloning, using cached admission
    // accounting. The guard remains charged through validation and retirement.
    let working = reserve_derived(
        s,
        config,
        s.derived_resident_bytes.saturating_add(workspace),
    )?;
    Ok(RootPreparation {
        next: s.catalog.clone(),
        stamp: RootStamp::capture(s),
        pages,
        resident_limit: s.derived_resident_bytes,
        working,
    })
}

pub(crate) struct PreparedRoot {
    // Only checkpoint/compaction may assert equivalence of changed segment IDs.
    raw_rows_preserved: bool,
    root: CheckpointRoot,
    bytes: Vec<u8>,
    indexes: BTreeMap<String, RollupIndex>,
    accounting: DerivedAccounting,
    logical_bytes: usize,
    resident_bytes: usize,
    stamp: RootStamp,
    _dependencies: wal::DurableDependencies<Pin>,
    _working: DerivedWorking,
}

impl PreparedRoot {
    pub(crate) fn preserving_raw_rows(mut self) -> Self {
        self.raw_rows_preserved = true;
        self
    }

    pub(crate) fn is_current(&self, s: &State) -> bool {
        self.stamp.matches(s)
    }
}

pub(crate) fn prepare_root(inner: &Inner, candidate: RootPreparation) -> Result<PreparedRoot> {
    build_prepared_root(inner, candidate, true)
}

fn build_prepared_root(
    inner: &Inner,
    candidate: RootPreparation,
    off_lock: bool,
) -> Result<PreparedRoot> {
    let _root_prepare_timer = inner.metrics.timer(Phase::RootPrepare);
    let RootPreparation {
        next,
        stamp,
        pages,
        resident_limit,
        working,
    } = candidate;
    #[cfg(feature = "fault-injection")]
    if off_lock {
        block_root_test_hook(inner, MaintenanceHookPhase::RootPrepare)?;
    }
    #[cfg(not(feature = "fault-injection"))]
    let _ = off_lock;
    validate_manifest(&next)?;
    let resident_bytes = derived_root::resident_bytes(&next, true);
    ensure!(
        resident_bytes <= resident_limit,
        "prepared root exceeds reserved resident bytes"
    );
    let logical_bytes = logical_metadata_bytes(&next)?;
    let indexes = build_rollup_indexes(&next, &inner.config)?;
    let accounting = DerivedAccounting::build(&next, &inner.config)?;
    // Startup syncs the database root after creating this durable anchor.
    let mut dependencies = wal::DependencyBatch::new(
        &inner.root.join("derived"),
        Pin::new(inner, BTreeSet::new())?,
    );
    let mut new_pages = false;
    let root = if pages {
        derived_root::prepare(next, &inner.config, |page, bytes| {
            {
                // Lock order is disk -> pins; never acquire state here. Pin before
                // either verifying an existing object or publishing a new one.
                let _disk = lock_disk_admission(inner)?;
                dependencies.protection_mut().add(page.key())?;
                let path = inner.root.join(page.key());
                let _publish =
                    (!path.try_exists()?).then(|| inner.metrics.timer(Phase::DerivedPublish));
                new_pages |= dependencies.stage(
                    &path,
                    bytes,
                    |existing| {
                        let _verify = inner.metrics.timer(Phase::DerivedVerify);
                        page.verify(existing)
                    },
                    |additional| ensure_budget(inner, additional),
                )?;
            }
            #[cfg(feature = "fault-injection")]
            if off_lock {
                block_root_test_hook(inner, MaintenanceHookPhase::DerivedPagePrepared)?;
            }
            Ok(())
        })?
    } else {
        CheckpointRoot {
            catalog: next,
            derived: None,
        }
    };
    let dependencies = dependencies.finish()?;
    if new_pages {
        // This crash point still observes durable page(s), never mere renames.
        wal::failpoint("derived_page_published");
    }
    let bytes = root.encode(&inner.config)?;
    ensure!(
        bytes.len() <= inner.config.metadata_max_bytes,
        "metadata/recovery byte budget exceeded"
    );
    wal::failpoint("derived_pages_published");
    Ok(PreparedRoot {
        raw_rows_preserved: false,
        root,
        bytes,
        indexes,
        accounting,
        logical_bytes,
        resident_bytes,
        stamp,
        _dependencies: dependencies,
        _working: working,
    })
}

#[cfg(feature = "fault-injection")]
fn block_root_test_hook(inner: &Inner, phase: MaintenanceHookPhase) -> Result<()> {
    let hook = inner
        .maintenance_test_hook
        .lock()
        .map_err(|_| anyhow::anyhow!("maintenance test hook poisoned"))?
        .clone();
    if let Some(hook) = hook {
        hook.block(phase)?;
    }
    Ok(())
}

// Return the displaced maps/indexes with the reservation intact so prepared
// callers can free them after releasing state. Dependency I/O and derived map
// construction stay outside this validated publication step; raw identity
// reconciliation below visits table/segment metadata only.
pub(crate) fn publish_prepared_root(
    inner: &Inner,
    s: &mut State,
    mut prepared: PreparedRoot,
) -> Result<PreparedRoot> {
    healthy(s)?;
    ensure!(prepared.is_current(s), "stale prepared root");
    let generation = s
        .generation
        .checked_add(1)
        .context("state generation exhausted")?;
    let root_epoch = s
        .root_epoch
        .checked_add(1)
        .context("root epoch exhausted")?;
    let _disk = lock_disk_admission(inner)?;
    ensure_budget(inner, prepared.bytes.len() as u64)?;
    let publication = {
        let _manifest_timer = inner.metrics.timer(Phase::ManifestCommit);
        wal::atomic_write(&inner.root.join("manifest.bin"), &prepared.bytes)
    };
    if let Err(e) = publication {
        s.fenced = Some(format!("ambiguous manifest publication: {e:#}"));
        return Err(e);
    }
    wal::failpoint("manifest_published");
    s.metadata_bytes = prepared.logical_bytes;
    s.control_root_bytes = prepared.bytes.len();
    std::mem::swap(&mut s.derived_refs, &mut prepared.root.derived);
    std::mem::swap(&mut s.rollup_indexes, &mut prepared.indexes);
    std::mem::swap(&mut s.derived_accounting, &mut prepared.accounting);
    std::mem::swap(&mut s.catalog, &mut prepared.root.catalog);
    reconcile_raw_stamps(s, &prepared.root.catalog, prepared.raw_rows_preserved);
    s.derived_resident_bytes = prepared.resident_bytes;
    s.generation = generation;
    s.root_epoch = root_epoch;
    Ok(prepared)
}

pub(crate) fn persist_manifest(inner: &Inner, s: &mut State, next: Manifest) -> Result<()> {
    persist_manifest_with_raw(inner, s, next, false)
}

fn persist_manifest_with_raw(
    inner: &Inner,
    s: &mut State,
    next: Manifest,
    raw_rows_preserved: bool,
) -> Result<()> {
    // Control and admission callers already reserve their catalog clone. Keep
    // their synchronous compatibility path and its map-growing liveness guard.
    check_state_catalog_budget(inner, s, &next, 0)?;
    let resident_limit = derived_root::resident_bytes(&next, true);
    let index_bytes = resident_limit.saturating_sub(derived_root::resident_bytes(&next, false));
    let pages = uses_derived_pages(s, &inner.config);
    let workspace = if pages {
        inner.config.derived_page_bytes.saturating_mul(4)
    } else {
        0
    };
    let working = reserve_derived(s, &inner.config, index_bytes.saturating_add(workspace))?;
    let prepared = build_prepared_root(
        inner,
        RootPreparation {
            next,
            stamp: RootStamp::capture(s),
            pages,
            resident_limit,
            working,
        },
        false,
    )?;
    let prepared = if raw_rows_preserved {
        prepared.preserving_raw_rows()
    } else {
        prepared
    };
    drop(publish_prepared_root(inner, s, prepared)?);
    Ok(())
}

struct CheckpointPreparation {
    root: RootPreparation,
    hot: Vec<(String, TableConfig, Vec<ResidentBatch>)>,
}

#[derive(Clone)]
struct ReceiptRetirement {
    table: String,
    removed: Vec<(String, ReceiptEntry)>,
    floor: Option<i64>,
}

struct FrozenCheckpointPreparation {
    root: RootPreparation,
    hot: Vec<(String, TableConfig, Vec<ResidentBatch>)>,
    retirements: Vec<ReceiptRetirement>,
    checkpoint_sequence: u64,
    prior_checkpoint_sequence: u64,
    root_epoch: u64,
    control_epoch: u64,
    idempotency_floors: BTreeMap<String, i64>,
}

struct PreparedFrozenRoot {
    root: CheckpointRoot,
    bytes: Vec<u8>,
    logical_bytes: usize,
    checkpoint_sequence: u64,
    prior_checkpoint_sequence: u64,
    root_epoch: u64,
    control_epoch: u64,
    idempotency_floors: BTreeMap<String, i64>,
    _dependencies: wal::DurableDependencies<Pin>,
    _working: DerivedWorking,
}

struct FrozenInstallPlan {
    metadata_bytes: usize,
    control_base: usize,
    derived_resident_bytes: usize,
    accounting: Vec<(String, TableAccounting)>,
    hot_bytes: usize,
    generation: u64,
    root_epoch: u64,
}

fn capture_frozen_checkpoint(
    s: &State,
    config: &Config,
) -> Result<Option<FrozenCheckpointPreparation>> {
    if s.sequence == s.catalog.checkpoint_sequence
        && s.hot.values().all(Vec::is_empty)
        && !idempotency_checkpoint_due(s)
        && !(config.derived_pages && s.derived_refs.is_none())
    {
        return Ok(None);
    }
    let mut root = capture_root(s, config)?;
    apply_idempotency_checkpoint(s, &mut root.next);
    let mut retirements = Vec::new();
    for (name, current) in &s.catalog.tables {
        let next = root
            .next
            .tables
            .get(name)
            .context("captured checkpoint table missing")?;
        let removed = current
            .receipts
            .iter()
            .filter(|(id, _)| !next.receipts.contains_key(*id))
            .map(|(id, receipt)| (id.clone(), receipt.clone()))
            .collect::<Vec<_>>();
        if !removed.is_empty() || current.idempotency_floor_us != next.idempotency_floor_us {
            retirements.push(ReceiptRetirement {
                table: name.clone(),
                removed,
                floor: next.idempotency_floor_us,
            });
        }
    }
    let hot = s
        .hot
        .iter()
        .filter(|(_, batches)| !batches.is_empty())
        .map(|(name, batches)| {
            let table_config = root
                .next
                .tables
                .get(name)
                .context("hot rows without table")?
                .config
                .clone();
            Ok((
                name.clone(),
                table_config,
                batches.iter().map(ResidentBatch::pinned).collect(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(FrozenCheckpointPreparation {
        root,
        hot,
        retirements,
        checkpoint_sequence: s.sequence,
        prior_checkpoint_sequence: s.catalog.checkpoint_sequence,
        root_epoch: s.root_epoch,
        control_epoch: s.control_epoch,
        idempotency_floors: s.idempotency_floors.clone(),
    }))
}

fn prepare_frozen_root(
    inner: &Inner,
    candidate: RootPreparation,
    checkpoint_sequence: u64,
    prior_checkpoint_sequence: u64,
    root_epoch: u64,
    control_epoch: u64,
    idempotency_floors: BTreeMap<String, i64>,
) -> Result<PreparedFrozenRoot> {
    let _root_prepare_timer = inner.metrics.timer(Phase::RootPrepare);
    let RootPreparation {
        next,
        stamp: _,
        pages,
        resident_limit,
        working,
    } = candidate;
    #[cfg(feature = "fault-injection")]
    block_root_test_hook(inner, MaintenanceHookPhase::RootPrepare)?;
    validate_manifest(&next)?;
    let resident_bytes = derived_root::resident_bytes(&next, true);
    ensure!(
        resident_bytes <= resident_limit,
        "prepared frozen root exceeds reserved resident bytes"
    );
    let logical_bytes = logical_metadata_bytes(&next)?;
    // Startup syncs the database root after creating this durable anchor.
    let mut dependencies = wal::DependencyBatch::new(
        &inner.root.join("derived"),
        Pin::new(inner, BTreeSet::new())?,
    );
    let mut new_pages = false;
    let root = if pages {
        derived_root::prepare(next, &inner.config, |page, bytes| {
            {
                let _disk = lock_disk_admission(inner)?;
                dependencies.protection_mut().add(page.key())?;
                let path = inner.root.join(page.key());
                let _publish =
                    (!path.try_exists()?).then(|| inner.metrics.timer(Phase::DerivedPublish));
                new_pages |= dependencies.stage(
                    &path,
                    bytes,
                    |existing| {
                        let _verify = inner.metrics.timer(Phase::DerivedVerify);
                        page.verify(existing)
                    },
                    |additional| ensure_budget(inner, additional),
                )?;
            }
            #[cfg(feature = "fault-injection")]
            block_root_test_hook(inner, MaintenanceHookPhase::DerivedPagePrepared)?;
            Ok(())
        })?
    } else {
        CheckpointRoot {
            catalog: next,
            derived: None,
        }
    };
    let dependencies = dependencies.finish()?;
    if new_pages {
        // This crash point still observes durable page(s), never mere renames.
        wal::failpoint("derived_page_published");
    }
    let bytes = root.encode(&inner.config)?;
    ensure!(
        bytes.len() <= inner.config.metadata_max_bytes,
        "metadata/recovery byte budget exceeded"
    );
    wal::failpoint("derived_pages_published");
    Ok(PreparedFrozenRoot {
        root,
        bytes,
        logical_bytes,
        checkpoint_sequence,
        prior_checkpoint_sequence,
        root_epoch,
        control_epoch,
        idempotency_floors,
        _dependencies: dependencies,
        _working: working,
    })
}

fn frozen_prefix_is_current(
    s: &State,
    prepared: &PreparedFrozenRoot,
    hot: &[(String, TableConfig, Vec<ResidentBatch>)],
) -> bool {
    if s.sequence < prepared.checkpoint_sequence
        || s.catalog.checkpoint_sequence != prepared.prior_checkpoint_sequence
        || s.root_epoch != prepared.root_epoch
        || s.control_epoch != prepared.control_epoch
        || s.idempotency_floors != prepared.idempotency_floors
    {
        return false;
    }
    hot.iter().all(|(name, table_config, captured)| {
        let Some(table) = s.catalog.tables.get(name) else {
            return false;
        };
        if &table.config != table_config {
            return false;
        }
        let current = s.hot.get(name).map(Vec::as_slice).unwrap_or_default();
        current.len() >= captured.len()
            && current.iter().zip(captured).all(|(live, frozen)| {
                live.id == frozen.id
                    && live.charged_bytes == frozen.charged_bytes
                    && SharedRawRows::ptr_eq(&live.rows, &frozen.rows)
            })
    })
}

fn serialized_map_remove_delta(
    live_entries: usize,
    removed: &[(String, ReceiptEntry)],
) -> Result<i128> {
    ensure!(
        removed.len() <= live_entries,
        "receipt retirement exceeds live map"
    );
    let entry_bytes = removed.iter().try_fold(0i128, |total, (id, receipt)| {
        let bytes = serde_json::to_vec(id)?.len() + 1 + serde_json::to_vec(receipt)?.len();
        total
            .checked_add(bytes as i128)
            .context("receipt retirement accounting overflow")
    })?;
    let separators = removed.len().min(live_entries.saturating_sub(1));
    Ok(-entry_bytes - separators as i128)
}

fn segment_extension_delta(existing: usize, segments: &[Segment]) -> Result<i128> {
    segments
        .iter()
        .enumerate()
        .try_fold(0i128, |total, (index, segment)| {
            let separator = usize::from(existing.saturating_add(index) > 0);
            total
                .checked_add((separator + serde_json::to_vec(segment)?.len()) as i128)
                .context("segment descriptor accounting overflow")
        })
}

fn plan_frozen_install(
    inner: &Inner,
    s: &mut State,
    prepared: &PreparedFrozenRoot,
    hot: &[(String, TableConfig, Vec<ResidentBatch>)],
    written: &[(String, WrittenSegment)],
    retirements: &[ReceiptRetirement],
) -> Result<FrozenInstallPlan> {
    ensure!(
        frozen_prefix_is_current(s, prepared, hot),
        "stale frozen-prefix checkpoint"
    );
    let generation = s
        .generation
        .checked_add(1)
        .context("state generation exhausted")?;
    let root_epoch = s
        .root_epoch
        .checked_add(1)
        .context("root epoch exhausted")?;
    let mut metadata_delta =
        checkpoint_sequence_delta(s.catalog.checkpoint_sequence, prepared.checkpoint_sequence);
    let mut control_delta = metadata_delta;
    let mut output_counts = BTreeMap::<String, usize>::new();
    for (name, output) in written {
        let table = s
            .catalog
            .tables
            .get(name)
            .context("checkpoint output table missing")?;
        let index = output_counts.entry(name.clone()).or_default();
        let delta = segment_extension_delta(
            table.segments.len().saturating_add(*index),
            std::slice::from_ref(&output.descriptor),
        )?;
        metadata_delta = metadata_delta
            .checked_add(delta)
            .context("checkpoint metadata accounting overflow")?;
        control_delta = control_delta
            .checked_add(delta)
            .context("checkpoint control accounting overflow")?;
        *index += 1;
    }
    for (name, count) in &output_counts {
        s.catalog
            .tables
            .get_mut(name)
            .context("checkpoint output table missing")?
            .segments
            .try_reserve(*count)
            .context("checkpoint segment descriptor reservation failed")?;
    }

    let mut derived_resident_bytes = s.derived_resident_bytes;
    let mut accounting = Vec::new();
    for retirement in retirements {
        let table = s
            .catalog
            .tables
            .get(&retirement.table)
            .context("receipt retirement table missing")?;
        for (id, receipt) in &retirement.removed {
            ensure!(
                table.receipts.get(id) == Some(receipt),
                "captured receipt changed during prefix preparation"
            );
            derived_resident_bytes = derived_resident_bytes
                .checked_sub(derived::receipt_resident_bytes(id, receipt))
                .context("derived receipt resident accounting underflow")?;
        }
        let removal_delta = serialized_map_remove_delta(table.receipts.len(), &retirement.removed)?;
        metadata_delta = metadata_delta
            .checked_add(removal_delta)
            .context("checkpoint receipt metadata accounting overflow")?;
        let floor_delta = serde_json::to_vec(&retirement.floor)?.len() as i128
            - serde_json::to_vec(&table.idempotency_floor_us)?.len() as i128;
        metadata_delta = metadata_delta
            .checked_add(floor_delta)
            .context("checkpoint floor metadata accounting overflow")?;
        control_delta = control_delta
            .checked_add(floor_delta)
            .context("checkpoint floor control accounting overflow")?;

        let current = s
            .derived_accounting
            .tables
            .get(&retirement.table)
            .context("receipt retirement accounting missing")?;
        let mut next = current.clone();
        next.receipts.json_bytes = apply_encoded_delta(next.receipts.json_bytes, removal_delta)?;
        next.receipts.entries = next
            .receipts
            .entries
            .checked_sub(retirement.removed.len())
            .context("receipt accounting entry underflow")?;
        // Keep max_entry_bytes conservative. A surviving tail may be the maximum,
        // and rescanning the live map would violate the bounded install contract.
        next.bound(&retirement.table, &inner.config)?;
        accounting.push((retirement.table.clone(), next));
    }
    let metadata_bytes = apply_encoded_delta(s.metadata_bytes, metadata_delta)?;
    let control_base = apply_encoded_delta(s.derived_accounting.control_base, control_delta)?;
    let retired_hot_bytes = hot.iter().try_fold(0usize, |total, (_, _, batches)| {
        batches.iter().try_fold(total, |sum, batch| {
            sum.checked_add(batch.charged_bytes)
                .context("captured hot-byte accounting overflow")
        })
    })?;
    let hot_bytes = s
        .hot_bytes
        .checked_sub(retired_hot_bytes)
        .context("captured hot-byte accounting underflow")?;
    let retired_rows = hot.iter().try_fold(0usize, |total, (_, _, batches)| {
        batches.iter().try_fold(total, |sum, batch| {
            sum.checked_add(batch.rows.len())
                .context("captured hot-row accounting overflow")
        })
    })?;
    let tail_rows = hot_count(s)
        .checked_sub(retired_rows)
        .context("captured hot-row accounting underflow")?;

    if uses_derived_pages(s, &inner.config) {
        let mut root_bound = s.derived_accounting.root_bound;
        let mut encoded_bound = s.derived_accounting.encoded_bound;
        let mut oversized_sets = s.derived_accounting.oversized_sets;
        for (name, next) in &accounting {
            let old = s
                .derived_accounting
                .tables
                .get(name)
                .context("receipt retirement accounting missing")?;
            next.entries_fit(name, &inner.config)?;
            root_bound = root_bound
                .saturating_sub(old.root_bound)
                .saturating_add(next.root_bound);
            encoded_bound = encoded_bound
                .saturating_sub(old.encoded_bound)
                .saturating_add(next.encoded_bound);
            oversized_sets = oversized_sets - old.oversized_sets + next.oversized_sets;
        }
        ensure!(
            oversized_sets == 0,
            "existing derived entry exceeds configured writer target"
        );
        ensure!(
            encoded_bound <= inner.config.derived_max_bytes,
            "projected derived encoded byte budget exceeded"
        );
        ensure!(
            control_base
                .saturating_add(root_bound)
                .saturating_add(20)
                .saturating_add(tail_rows.saturating_mul(512))
                <= inner.config.metadata_max_bytes,
            "projected frozen-prefix control metadata budget exceeded"
        );
    } else {
        ensure!(
            metadata_bytes.saturating_add(tail_rows.saturating_mul(512))
                <= inner.config.metadata_max_bytes,
            "projected frozen-prefix metadata budget exceeded"
        );
    }
    ensure!(
        derived_resident_bytes.saturating_add(s.derived_working.load(Ordering::SeqCst))
            <= inner.config.derived_max_bytes,
        "projected frozen-prefix derived budget exceeded"
    );
    ensure!(
        prepared.logical_bytes <= inner.config.metadata_max_bytes
            || uses_derived_pages(s, &inner.config),
        "frozen root logical metadata budget exceeded"
    );
    Ok(FrozenInstallPlan {
        metadata_bytes,
        control_base,
        derived_resident_bytes,
        accounting,
        hot_bytes,
        generation,
        root_epoch,
    })
}

fn capture_checkpoint(s: &State, config: &Config) -> Result<Option<CheckpointPreparation>> {
    if s.sequence == s.catalog.checkpoint_sequence
        && s.hot.values().all(Vec::is_empty)
        && !idempotency_checkpoint_due(s)
        && !(config.derived_pages && s.derived_refs.is_none())
    {
        return Ok(None);
    }
    let mut root = capture_root(s, config)?;
    apply_idempotency_checkpoint(s, &mut root.next);
    let hot = s
        .hot
        .iter()
        .filter(|(_, rows)| !rows.is_empty())
        .map(|(name, rows)| {
            let config = root
                .next
                .tables
                .get(name)
                .context("hot rows without table")?
                .config
                .clone();
            Ok((
                name.clone(),
                config,
                rows.iter().map(ResidentBatch::pinned).collect(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(CheckpointPreparation { root, hot }))
}

#[derive(Clone, Copy)]
enum StaleCheckpoint {
    Defer,
    ExplicitFallback,
}

#[must_use = "obsolete WAL deletion still requires its directory barrier"]
struct RetiredWalPrefix {
    directory: Option<PathBuf>,
}

impl RetiredWalPrefix {
    fn finish(self) -> Result<()> {
        match self.directory {
            Some(directory) => wal::sync_dir(&directory),
            None => Ok(()),
        }
    }
}

fn retire_wal_prefix_locked(
    inner: &Inner,
    s: &mut State,
    frontier: u64,
) -> Result<RetiredWalPrefix> {
    if inner.journal.is_some() {
        commit_log::reclaim(inner, s, frontier)?;
        return Ok(RetiredWalPrefix { directory: None });
    }
    let _disk = lock_disk_admission(inner)?;
    let directory = inner.root.join("wal");
    for entry in fs::read_dir(&directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name
            .strip_suffix(".wal")
            .and_then(|sequence| sequence.parse::<u64>().ok())
            .is_some_and(|sequence| sequence <= frontier)
        {
            ensure!(
                entry.file_type()?.is_file(),
                "unexpected non-file WAL entry"
            );
            fs::remove_file(entry.path())?;
        }
    }
    s.wal_bytes = directory_bytes(&directory)?;
    Ok(RetiredWalPrefix {
        directory: Some(directory),
    })
}

fn checkpoint_frozen_prefix_with_policy(db: &Database, stale: StaleCheckpoint) -> Result<bool> {
    let _preparation = match stale {
        StaleCheckpoint::ExplicitFallback => Some(db.lock_maintenance_preparation()?),
        StaleCheckpoint::Defer => match db.inner.maintenance_preparation.try_lock() {
            Ok(gate) => Some(gate),
            Err(std::sync::TryLockError::WouldBlock) => return Ok(false),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                bail!("maintenance preparation mutex poisoned")
            }
        },
    };
    let mut captured = {
        let s = db.lock()?;
        healthy(&s)?;
        let capture_timer = db.inner.metrics.timer(Phase::CheckpointCapture);
        let captured = capture_frozen_checkpoint(&s, &db.inner.config)?;
        drop(capture_timer);
        let Some(captured) = captured else {
            return Ok(false);
        };
        captured
    };
    #[cfg(feature = "fault-injection")]
    db.block_maintenance_test_hook(MaintenanceHookPhase::CheckpointPrepare)?;
    let prepare_timer = db.inner.metrics.timer(Phase::CheckpointPrepare);
    let mut output_pin = Pin::new(&db.inner, BTreeSet::new())?;
    let preparation: Result<(PreparedFrozenRoot, Vec<(String, WrittenSegment)>)> = (|| {
        let mut written = Vec::new();
        for (name, table_config, batches) in &captured.hot {
            let table = captured
                .root
                .next
                .tables
                .get_mut(name)
                .context("checkpoint table disappeared")?;
            let outputs = write_resident_partitioned_with_pin(
                &db.inner,
                table_config,
                batches,
                Some(&mut output_pin),
            )?;
            table
                .segments
                .extend(outputs.iter().map(|output| output.descriptor.clone()));
            written.extend(outputs.into_iter().map(|output| (name.clone(), output)));
        }
        captured.root.next.checkpoint_sequence = captured.checkpoint_sequence;
        wal::failpoint("segments_published");
        let prepared = prepare_frozen_root(
            &db.inner,
            captured.root,
            captured.checkpoint_sequence,
            captured.prior_checkpoint_sequence,
            captured.root_epoch,
            captured.control_epoch,
            std::mem::take(&mut captured.idempotency_floors),
        )?;
        Ok((prepared, written))
    })();
    drop(prepare_timer);
    let (mut prepared, written) = match preparation {
        Ok(prepared) => prepared,
        Err(error) => {
            drop(output_pin);
            let _commit = db.lock_commit()?;
            let mut s = db.lock()?;
            if s.fenced.is_none()
                && let Err(cleanup) = cleanup_unpublished_segments(&db.inner, &s)
            {
                let reason = format!(
                    "frozen-prefix preparation failed and output cleanup failed: {cleanup:#}"
                );
                s.fenced = Some(reason.clone());
                return Err(error.context(reason));
            }
            return Err(error);
        }
    };
    #[cfg(feature = "fault-injection")]
    db.block_maintenance_test_hook(MaintenanceHookPhase::CheckpointBeforePublish)?;
    let _commit = db.lock_commit()?;
    let mut s = db.lock()?;
    healthy(&s)?;
    if !frozen_prefix_is_current(&s, &prepared, &captured.hot) {
        drop(s);
        drop(output_pin);
        drop(prepared);
        let s = db.lock()?;
        healthy(&s)?;
        cleanup_unpublished_segments(&db.inner, &s)?;
        return match stale {
            StaleCheckpoint::Defer => Ok(false),
            StaleCheckpoint::ExplicitFallback => {
                bail!("stale frozen-prefix checkpoint; retry the bounded operation")
            }
        };
    }
    let plan = plan_frozen_install(
        &db.inner,
        &mut s,
        &prepared,
        &captured.hot,
        &written,
        &captured.retirements,
    )?;
    let retired_batch_count = captured
        .hot
        .iter()
        .try_fold(0usize, |total, (_, _, batches)| {
            total
                .checked_add(batches.len())
                .context("captured batch count overflow")
        })?;
    let mut retired_hot = Vec::new();
    retired_hot
        .try_reserve_exact(retired_batch_count)
        .context("retired hot-prefix reservation failed")?;
    let _publish_timer = db.inner.metrics.timer(Phase::CheckpointPublish);
    let _disk = lock_disk_admission(&db.inner)?;
    ensure_budget(&db.inner, prepared.bytes.len() as u64)?;
    let publication = {
        let _manifest_timer = db.inner.metrics.timer(Phase::ManifestCommit);
        wal::atomic_write(&db.inner.root.join("manifest.bin"), &prepared.bytes)
    };
    if let Err(error) = publication {
        s.fenced = Some(format!("ambiguous manifest publication: {error:#}"));
        return Err(error);
    }
    wal::failpoint("manifest_published");

    for (name, _, batches) in &captured.hot {
        let live = s.hot.get_mut(name).expect("validated frozen hot table");
        retired_hot.extend(live.drain(..batches.len()));
    }
    for retirement in &captured.retirements {
        let table = s
            .catalog
            .tables
            .get_mut(&retirement.table)
            .expect("validated retirement table");
        for (id, _) in &retirement.removed {
            let removed = table.receipts.remove(id);
            debug_assert!(removed.is_some());
        }
        table.idempotency_floor_us = retirement.floor;
    }
    for (name, output) in written {
        let WrittenSegment { descriptor, rows } = output;
        if let Some(rows) = rows {
            offer_decoded(
                &mut s,
                db.inner.config.decoded_cache_bytes,
                descriptor.id.clone(),
                rows,
                descriptor.decoded_bytes as usize,
            );
        }
        s.catalog
            .tables
            .get_mut(&name)
            .expect("validated checkpoint output table")
            .segments
            .push(descriptor);
    }
    s.catalog.checkpoint_sequence = prepared.checkpoint_sequence;
    s.hot_epochs
        .retain(|epoch| epoch.sequence > prepared.checkpoint_sequence);
    s.first_hot_us = s.hot_epochs.first().map(|epoch| epoch.first_now_us);
    s.hot_bytes = plan.hot_bytes;
    s.metadata_bytes = plan.metadata_bytes;
    s.control_root_bytes = prepared.bytes.len();
    s.derived_resident_bytes = plan.derived_resident_bytes;
    s.derived_accounting.control_base = plan.control_base;
    for (name, accounting) in plan.accounting {
        s.derived_accounting.replace(name, accounting);
    }
    let retired_derived_refs = std::mem::replace(&mut s.derived_refs, prepared.root.derived.take());
    s.generation = plan.generation;
    s.root_epoch = plan.root_epoch;
    wal::failpoint("frozen_prefix_installed");
    drop(_disk);
    let retirement = retire_wal_prefix_locked(&db.inner, &mut s, prepared.checkpoint_sequence)?;
    drop(_publish_timer);
    drop(s);
    // The durable root and live accounting are installed; only obsolete names
    // and privately owned allocations remain. New appends keep their own WAL
    // file/directory barriers and cannot recreate this checkpointed prefix.
    drop(_commit);
    let _reclaim_timer = db.inner.metrics.timer(Phase::CheckpointReclaim);
    #[cfg(feature = "fault-injection")]
    db.block_maintenance_test_hook(MaintenanceHookPhase::CheckpointReclaim)?;
    retirement.finish()?;
    wal::failpoint("frozen_prefix_wal_retired");
    drop(retired_hot);
    drop(retired_derived_refs);
    drop(prepared);
    drop(output_pin);
    Ok(true)
}

pub(crate) fn checkpoint_prepared(db: &Database) -> Result<bool> {
    checkpoint_prepared_with_policy(db, StaleCheckpoint::ExplicitFallback)
}

pub(crate) fn checkpoint_prepared_scheduled(db: &Database) -> Result<bool> {
    checkpoint_prepared_with_policy(db, StaleCheckpoint::Defer)
}

fn checkpoint_locked_fallback(db: &Database) -> Result<bool> {
    let _commit = db.lock_commit()?;
    let mut s = db.lock()?;
    healthy(&s)?;
    let needed = s.sequence != s.catalog.checkpoint_sequence
        || s.hot.values().any(|rows| !rows.is_empty())
        || idempotency_checkpoint_due(&s);
    checkpoint_locked(&db.inner, &mut s)?;
    Ok(needed)
}

fn checkpoint_prepared_with_policy(db: &Database, stale: StaleCheckpoint) -> Result<bool> {
    if db.inner.config.checkpoint_frozen_prefix {
        return checkpoint_frozen_prefix_with_policy(db, stale);
    }
    let _preparation = match db.inner.maintenance_preparation.try_lock() {
        Ok(gate) => gate,
        Err(std::sync::TryLockError::WouldBlock) => {
            return match stale {
                StaleCheckpoint::Defer => Ok(false),
                StaleCheckpoint::ExplicitFallback => checkpoint_locked_fallback(db),
            };
        }
        Err(std::sync::TryLockError::Poisoned(_)) => {
            bail!("maintenance preparation mutex poisoned")
        }
    };
    let (mut prepared, mut output_pin) = {
        let s = db.lock()?;
        healthy(&s)?;
        let capture_timer = db.inner.metrics.timer(Phase::CheckpointCapture);
        let captured = capture_checkpoint(&s, &db.inner.config)?;
        drop(capture_timer);
        let Some(prepared) = captured else {
            return Ok(false);
        };
        let pin = Pin::new(&db.inner, BTreeSet::new())?;
        (prepared, pin)
    };
    #[cfg(feature = "fault-injection")]
    db.block_maintenance_test_hook(MaintenanceHookPhase::CheckpointPrepare)?;
    let prepare_timer = db.inner.metrics.timer(Phase::CheckpointPrepare);
    let preparation: Result<(PreparedRoot, Vec<WrittenSegment>)> = (|| {
        let mut written_segments = Vec::new();
        for (name, config, batches) in &prepared.hot {
            let table = prepared
                .root
                .next
                .tables
                .get_mut(name)
                .context("checkpoint table disappeared")?;
            let written = write_resident_partitioned_with_pin(
                &db.inner,
                config,
                batches,
                Some(&mut output_pin),
            )?;
            table
                .segments
                .extend(written.iter().map(|segment| segment.descriptor.clone()));
            written_segments.extend(written);
        }
        prepared.root.next.checkpoint_sequence = prepared.root.stamp.sequence;
        wal::failpoint("segments_published");
        drop(prepared.hot);
        Ok((
            prepare_root(&db.inner, prepared.root)?.preserving_raw_rows(),
            written_segments,
        ))
    })();
    drop(prepare_timer);
    let (prepared, written_segments) = match preparation {
        Ok(prepared) => prepared,
        Err(error) => {
            drop(output_pin);
            let _commit = db.lock_commit()?;
            let mut s = db.lock()?;
            if s.fenced.is_none()
                && let Err(cleanup) = cleanup_unpublished_segments(&db.inner, &s)
            {
                let reason =
                    format!("checkpoint preparation failed and output cleanup failed: {cleanup:#}");
                s.fenced = Some(reason.clone());
                return Err(error.context(reason));
            }
            return Err(error);
        }
    };
    #[cfg(feature = "fault-injection")]
    db.block_maintenance_test_hook(MaintenanceHookPhase::CheckpointBeforePublish)?;
    let _commit = db.lock_commit()?;
    let mut s = db.lock()?;
    healthy(&s)?;
    if !prepared.is_current(&s) {
        drop(s);
        drop(output_pin);
        drop(prepared);
        let s = db.lock()?;
        healthy(&s)?;
        cleanup_unpublished_segments(&db.inner, &s)?;
        drop(s);
        drop(_commit);
        return match stale {
            StaleCheckpoint::Defer => Ok(false),
            StaleCheckpoint::ExplicitFallback => checkpoint_locked_fallback(db),
        };
    }
    let publish_timer = db.inner.metrics.timer(Phase::CheckpointPublish);
    let retired = match publish_prepared_root(&db.inner, &mut s, prepared) {
        Ok(retired) => retired,
        Err(error) => {
            drop(output_pin);
            if s.fenced.is_none()
                && let Err(cleanup) = cleanup_unpublished_segments(&db.inner, &s)
            {
                let reason =
                    format!("checkpoint publication failed and output cleanup failed: {cleanup:#}");
                s.fenced = Some(reason.clone());
                return Err(error.context(reason));
            }
            return Err(error);
        }
    };
    drop(publish_timer);
    for written in written_segments {
        if let Some(rows) = written.rows {
            offer_decoded(
                &mut s,
                db.inner.config.decoded_cache_bytes,
                written.descriptor.id,
                rows,
                written.descriptor.decoded_bytes as usize,
            );
        }
    }
    let retired_hot = std::mem::take(&mut s.hot);
    s.hot_epochs.clear();
    s.hot_bytes = 0;
    s.first_hot_us = None;
    drop(s);
    drop(retired_hot);
    drop(retired);
    drop(output_pin);
    let mut s = db.lock()?;
    healthy(&s)?;
    gc_locked(&db.inner, &mut s)?;
    Ok(true)
}

pub(crate) fn checkpoint_locked(inner: &Inner, s: &mut State) -> Result<()> {
    if s.sequence == s.catalog.checkpoint_sequence
        && s.hot.values().all(Vec::is_empty)
        && !idempotency_checkpoint_due(s)
        && !(inner.config.derived_pages && s.derived_refs.is_none())
    {
        return Ok(());
    }
    let _locked_timer = inner.metrics.timer(Phase::CheckpointLocked);
    #[cfg(feature = "fault-injection")]
    block_root_test_hook(inner, MaintenanceHookPhase::CheckpointLockedPrepare)?;
    let publication: Result<Vec<WrittenSegment>> = (|| {
        let prepare_timer = inner.metrics.timer(Phase::CheckpointPrepare);
        let capture_timer = inner.metrics.timer(Phase::CheckpointCapture);
        let _derived_working = reserve_catalog_clone(s, &inner.config)?;
        let mut next = s.catalog.clone();
        apply_idempotency_checkpoint(s, &mut next);
        drop(capture_timer);
        let mut written_segments = Vec::new();
        for (name, batches) in &s.hot {
            let table = next
                .tables
                .get_mut(name)
                .context("hot rows without table")?;
            let written = write_resident_partitioned_with_pin(inner, &table.config, batches, None)?;
            table
                .segments
                .extend(written.iter().map(|segment| segment.descriptor.clone()));
            written_segments.extend(written);
        }
        next.checkpoint_sequence = s.sequence;
        wal::failpoint("segments_published");
        drop(prepare_timer);
        let _publish_timer = inner.metrics.timer(Phase::CheckpointPublish);
        persist_manifest_with_raw(inner, s, next, true)?;
        Ok(written_segments)
    })();
    let written_segments = match publication {
        Ok(written) => written,
        Err(error) => {
            // An ambiguous manifest may already reference the new files; preserve them for recovery.
            if s.fenced.is_none()
                && let Err(cleanup) = cleanup_unpublished_segments(inner, s)
            {
                let reason = format!("checkpoint aborted and output cleanup failed: {cleanup:#}");
                s.fenced = Some(reason.clone());
                return Err(error.context(reason));
            }
            return Err(error);
        }
    };
    for written in written_segments {
        if let Some(rows) = written.rows {
            offer_decoded(
                s,
                inner.config.decoded_cache_bytes,
                written.descriptor.id,
                rows,
                written.descriptor.decoded_bytes as usize,
            );
        }
    }
    s.hot.clear();
    s.hot_epochs.clear();
    s.hot_bytes = 0;
    s.first_hot_us = None;
    gc_locked(inner, s)?;
    Ok(())
}

pub(crate) fn cleanup_unpublished_segments(inner: &Inner, s: &State) -> Result<()> {
    let _disk = lock_disk_admission(inner)?;
    let pins = inner
        .segment_pins
        .lock()
        .map_err(|_| anyhow::anyhow!("segment pins poisoned during checkpoint cleanup"))?;
    let mut retained: BTreeSet<String> = s
        .catalog
        .tables
        .values()
        .flat_map(|table| table.segments.iter().map(|segment| segment.id.clone()))
        .collect();
    retained.extend(pins.keys().cloned());
    let directory = inner.root.join("segments");
    for entry in fs::read_dir(&directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(id) = name.to_str().and_then(|name| name.strip_suffix(".parquet")) else {
            continue;
        };
        if id.len() == 64
            && id.bytes().all(|byte| byte.is_ascii_hexdigit())
            && !retained.contains(id)
        {
            ensure!(
                entry.file_type()?.is_file(),
                "unexpected non-file segment output"
            );
            fs::remove_file(entry.path())?;
        }
    }
    wal::sync_dir(&directory)
}

struct WrittenSegment {
    descriptor: Segment,
    rows: Option<SharedRawRows>,
}

pub(crate) fn write_partitioned(
    inner: &Inner,
    config: &TableConfig,
    rows: &[StoredRow],
) -> Result<Vec<Segment>> {
    write_partitioned_with_pin(inner, config, rows, None)
}

pub(crate) fn write_partitioned_with_pin(
    inner: &Inner,
    config: &TableConfig,
    rows: &[StoredRow],
    output_pin: Option<&mut Pin>,
) -> Result<Vec<Segment>> {
    Ok(
        write_partitioned_rows(inner, config, rows.iter(), output_pin, false)?
            .into_iter()
            .map(|written| written.descriptor)
            .collect(),
    )
}

fn write_resident_partitioned_with_pin(
    inner: &Inner,
    config: &TableConfig,
    batches: &[ResidentBatch],
    output_pin: Option<&mut Pin>,
) -> Result<Vec<WrittenSegment>> {
    write_partitioned_rows(
        inner,
        config,
        batches.iter().flat_map(|batch| batch.rows.iter()),
        output_pin,
        (inner.config.query_retained_inputs || inner.native_runtime.is_some())
            && inner.config.decoded_cache_bytes > 0,
    )
}

fn write_partitioned_rows<'a>(
    inner: &Inner,
    config: &TableConfig,
    rows: impl Iterator<Item = &'a StoredRow> + Clone,
    mut output_pin: Option<&mut Pin>,
    retain_rows: bool,
) -> Result<Vec<WrittenSegment>> {
    // This local pin also protects outputs when the caller already holds state
    // and therefore does not supply a longer-lived output pin.
    let mut dependencies = wal::DependencyBatch::new(
        &inner.root.join("segments"),
        Pin::new(inner, BTreeSet::new())?,
    );
    if output_pin.is_some() {
        // Off-lock callers supply an output pin; None is the state-locked
        // compatibility path. Drain a GC that sampled readers == 0 before pin
        // registration: its raw-file sweep holds state, not disk admission.
        // No disk lock is held here and no encoding/I/O runs under state.
        let _state = inner
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("database state mutex poisoned; reopen for recovery"))?;
    }
    let logical = rows
        .clone()
        .fold(0usize, |n, r| n.saturating_add(r.row.estimated_bytes()));
    let _partition_memory = inner
        .raw_memory
        .reserve_working(raw_memory::row_charge(logical))?;
    let mut partitions: BTreeMap<(u32, i64), Vec<StoredRow>> = BTreeMap::new();
    for row in rows {
        let shard = shard_for(&row.row.tenant, &row.row.series, config.shards);
        let window = window_start(row.row.timestamp_us, config.window_us)?;
        partitions
            .entry((shard, window))
            .or_default()
            .push(row.clone());
    }
    let mut result = Vec::new();
    for ((shard, window), mut rows) in partitions {
        rows.sort_by(|a, b| {
            (
                &a.row.tenant,
                &a.row.series,
                a.row.timestamp_us,
                a.sequence,
                a.ordinal,
            )
                .cmp(&(
                    &b.row.tenant,
                    &b.row.series,
                    b.row.timestamp_us,
                    b.sequence,
                    b.ordinal,
                ))
        });
        for chunk in rows.chunks(inner.config.segment_rows) {
            let preparation_bytes = inner
                .config
                .hot_max_bytes
                .saturating_add(inner.config.metadata_max_bytes)
                .min(wal::MAX_FRAME_BYTES);
            let _codec_memory = inner.raw_memory.reserve_working(raw_memory::codec_charge(
                raw_memory::logical_bytes(chunk),
                preparation_bytes,
            ))?;
            let bytes = segment::encode_with_limit(chunk, preparation_bytes as u64)?;
            wal::failpoint("segment_written");
            let id = blake3::hash(&bytes).to_hex().to_string();
            let path = inner.root.join("segments").join(format!("{id}.parquet"));
            {
                let _disk = lock_disk_admission(inner)?;
                dependencies.protection_mut().add(id.clone())?;
                if let Some(pin) = output_pin.as_deref_mut() {
                    pin.add(id.clone())?;
                }
                let _publish =
                    (!path.try_exists()?).then(|| inner.metrics.timer(Phase::RawPublish));
                dependencies.stage(
                    &path,
                    &bytes,
                    |existing| {
                        let _verify = inner.metrics.timer(Phase::RawVerify);
                        ensure!(existing == bytes, "immutable local segment collision");
                        Ok(())
                    },
                    |additional| ensure_budget(inner, additional),
                )?;
            }
            let descriptor = Segment {
                id,
                shard,
                window_us: window,
                rows: chunk.len() as u64,
                bytes: bytes.len() as u64,
                decoded_bytes: chunk
                    .iter()
                    .map(|row| row.row.estimated_bytes() as u64)
                    .sum(),
                min_timestamp_us: chunk.iter().map(|row| row.row.timestamp_us).min().unwrap(),
                max_timestamp_us: chunk.iter().map(|row| row.row.timestamp_us).max().unwrap(),
            };
            // Optional serving copies cannot consume maintenance headroom or block
            // durable publication. Acquire regular credit BEFORE copying.
            let resident_rows = if retain_rows
                && descriptor.decoded_bytes <= inner.config.decoded_cache_bytes as u64
            {
                inner
                    .raw_memory
                    .reserve(raw_memory::row_charge(descriptor.decoded_bytes as usize))
                    .ok()
                    .map(|credit| SharedRawRows::build(credit, || Ok(chunk.to_vec())))
                    .transpose()?
            } else {
                None
            };
            result.push(WrittenSegment {
                descriptor,
                rows: resident_rows,
            });
        }
    }
    let _dependencies = dependencies.finish()?;
    Ok(result)
}

pub(crate) fn verify_segment(path: &Path, seg: &Segment) -> Result<()> {
    let bytes = wal::read_bounded(path, wal::MAX_FRAME_BYTES)?;
    ensure!(
        bytes.len() as u64 == seg.bytes && blake3::hash(&bytes).to_hex().as_str() == seg.id,
        "segment integrity failure: {}",
        path.display()
    );
    Ok(())
}

pub(crate) fn resolve_segment(inner: &Inner, seg: &Segment) -> Result<PathBuf> {
    let _raw_file = inner
        .raw_memory
        .reserve((seg.bytes as usize).saturating_mul(2).saturating_add(256))?;
    resolve_segment_reserved(inner, seg)
}

// Caller already owns codec or file-buffer credit.
fn resolve_segment_reserved(inner: &Inner, seg: &Segment) -> Result<PathBuf> {
    let local = inner.root.join(seg.key());
    if local.exists() {
        verify_segment(&local, seg)?;
        return Ok(local);
    }
    let cache = inner.root.join("cache").join(format!("{}.parquet", seg.id));
    if cache.exists() {
        verify_segment(&cache, seg)?;
        return Ok(cache);
    }
    ensure!(
        seg.bytes <= inner.config.disk_cache_bytes,
        "cold segment exceeds disk-cache budget"
    );
    let remote = inner
        .remote
        .as_ref()
        .context("cold segment requires remote store")?;
    let bytes = remote.get_bounded(&seg.key(), seg.bytes as usize)?;
    ensure!(
        bytes.len() as u64 == seg.bytes && blake3::hash(&bytes).to_hex().as_str() == seg.id,
        "remote segment integrity failure"
    );
    let _disk = lock_disk_admission(inner)?;
    if local.exists() {
        verify_segment(&local, seg)?;
        return Ok(local);
    }
    if cache.exists() {
        verify_segment(&cache, seg)?;
        return Ok(cache);
    }
    let mut used = directory_bytes(&inner.root.join("cache"))?;
    let mut removed = false;
    if used.saturating_add(seg.bytes) > inner.config.disk_cache_bytes {
        let mut files =
            fs::read_dir(inner.root.join("cache"))?.collect::<std::io::Result<Vec<_>>>()?;
        files.sort_by_key(|file| {
            file.metadata()
                .and_then(|metadata| metadata.modified())
                .ok()
        });
        for file in files {
            if used.saturating_add(seg.bytes) <= inner.config.disk_cache_bytes {
                break;
            }
            let id = file
                .path()
                .file_stem()
                .and_then(|value| value.to_str())
                .unwrap_or_default()
                .to_owned();
            if inner
                .segment_pins
                .lock()
                .map_err(|_| anyhow::anyhow!("segment pins poisoned"))?
                .contains_key(&id)
            {
                continue;
            }
            let file_bytes = file.metadata()?.len();
            fs::remove_file(file.path())?;
            used = used.saturating_sub(file_bytes);
            removed = true;
        }
    }
    if removed {
        wal::sync_dir(&inner.root.join("cache"))?;
    }
    ensure!(
        used.saturating_add(seg.bytes) <= inner.config.disk_cache_bytes,
        "cold snapshot exceeds disk-cache budget or cache is pinned; increase disk_cache_bytes"
    );
    ensure_budget(inner, seg.bytes)?;
    wal::atomic_write(&cache, &bytes)?;
    Ok(cache)
}

fn touch_decoded(s: &mut State, id: &str) -> Option<SharedRawRows> {
    s.cache_clock = s.cache_clock.wrapping_add(1);
    let touched = s.cache_clock;
    let entry = s.decoded.get_mut(id)?;
    entry.touched = touched;
    Some(entry.rows.pin())
}

fn offer_decoded(s: &mut State, budget: usize, id: String, rows: SharedRawRows, bytes: usize) {
    if bytes > budget {
        return;
    }
    if s.decoded.contains_key(&id) {
        let _ = touch_decoded(s, &id);
        return;
    }
    while s
        .decoded
        .values()
        .map(|entry| entry.bytes)
        .sum::<usize>()
        .saturating_add(bytes)
        > budget
    {
        let key = s
            .decoded
            .iter()
            .min_by_key(|(_, entry)| entry.touched)
            .map(|(key, _)| key.clone());
        if let Some(key) = key {
            s.decoded.remove(&key);
        } else {
            break;
        }
    }
    s.cache_clock = s.cache_clock.wrapping_add(1);
    s.decoded.insert(
        id,
        CacheEntry {
            rows,
            bytes,
            touched: s.cache_clock,
        },
    );
}

/// Decode admission covers both the owned result and transient codec buffers.
/// Maintenance results never enter the cache with a working-pool reservation.
pub(crate) fn read_raw_segment(
    inner: &Inner,
    seg: &Segment,
    working: bool,
) -> Result<SharedRawRows> {
    let logical = usize::try_from(seg.decoded_bytes).context("segment decoded size overflow")?;
    ensure!(logical > 0, "segment missing decoded-size bound");
    let reserve = |bytes| {
        if working {
            inner.raw_memory.reserve_working(bytes)
        } else {
            Ok(inner.raw_memory.reserve(bytes)?)
        }
    };
    let credit = reserve(raw_memory::row_charge(logical))?;
    let _codec = reserve(raw_memory::codec_charge(logical, seg.bytes as usize))?;
    let path = resolve_segment_reserved(inner, seg)?;
    SharedRawRows::build(credit, || {
        let rows = segment::read_with_limit(&path, logical)?;
        ensure!(rows.len() as u64 == seg.rows, "segment row-count mismatch");
        ensure!(
            raw_memory::logical_bytes(&rows) == logical,
            "segment decoded-size metadata mismatch"
        );
        Ok(rows)
    })
}

pub(crate) fn read_segment_locked(
    inner: &Inner,
    s: &mut State,
    seg: &Segment,
) -> Result<SharedRawRows> {
    ensure!(
        seg.decoded_bytes <= inner.config.hot_max_bytes as u64,
        "segment decoded working set exceeds hot memory budget; reopen with larger hot_max_bytes"
    );
    if let Some(rows) = touch_decoded(s, &seg.id) {
        return Ok(rows);
    }
    let rows = read_raw_segment(inner, seg, true)?;
    ensure!(rows.len() as u64 == seg.rows, "segment row-count mismatch");
    let bytes = rows.iter().map(|r| r.row.estimated_bytes()).sum::<usize>();
    ensure!(
        bytes as u64 == seg.decoded_bytes,
        "segment decoded-size metadata mismatch"
    );
    // Maintenance-owned decode is transient, never a cache owner of headroom.
    Ok(rows)
}

// Caller must own the commit gate (or be in unshared startup). In particular,
// WAL temporaries are fully disk-charged but private until append installation;
// the gate prevents cleanup from unlinking them while state is unlocked.
pub(crate) fn gc_locked(inner: &Inner, s: &mut State) -> Result<usize> {
    let mut removed = cleanup_temporaries(&inner.root)?;
    for entry in fs::read_dir(inner.root.join("wal"))? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if (name.ends_with(".wal")
            && name
                .trim_end_matches(".wal")
                .parse::<u64>()
                .is_ok_and(|seq| seq <= s.catalog.checkpoint_sequence))
            || (name.starts_with('.') && name.ends_with(".tmp"))
        {
            fs::remove_file(entry.path())?;
            removed += 1;
        }
    }
    wal::sync_dir(&inner.root.join("wal"))?;
    s.wal_bytes = directory_bytes(&inner.root.join("wal"))?;
    if inner.journal.is_some() {
        commit_log::reclaim(inner, s, s.catalog.checkpoint_sequence)?;
    }
    {
        let _disk = lock_disk_admission(inner)?;
        let mut retained: BTreeSet<String> = s
            .derived_refs
            .iter()
            .flat_map(|tables| tables.values())
            .flat_map(|refs| refs.rollups.pages.iter().chain(&refs.receipts.pages))
            .map(|p| p.key())
            .collect();
        let pins = inner
            .segment_pins
            .lock()
            .map_err(|_| anyhow::anyhow!("derived pins poisoned"))?;
        retained.extend(
            pins.keys()
                .filter(|key| key.starts_with("derived/"))
                .cloned(),
        );
        let directory = inner.root.join("derived");
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let orphan = name
                .strip_suffix(".page")
                .is_some_and(|id| id.len() == 64 && id.bytes().all(|b| b.is_ascii_hexdigit()));
            let temporary = name.starts_with('.') && name.ends_with(".tmp");
            if (orphan && !retained.contains(&format!("derived/{name}"))) || temporary {
                ensure!(
                    entry.file_type()?.is_file(),
                    "unexpected non-file derived object"
                );
                fs::remove_file(entry.path())?;
                removed += 1;
            }
        }
        wal::sync_dir(&directory)?;
    }
    if inner.readers.load(Ordering::SeqCst) == 0 {
        let referenced: BTreeSet<_> = s
            .catalog
            .tables
            .values()
            .flat_map(|t| t.segments.iter().map(|seg| format!("{}.parquet", seg.id)))
            .collect();
        for dir in ["segments", "cache"] {
            for entry in fs::read_dir(inner.root.join(dir))? {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().to_string();
                if !referenced.contains(&name) {
                    ensure!(
                        entry.file_type()?.is_file(),
                        "unexpected directory in segment storage"
                    );
                    fs::remove_file(entry.path())?;
                    removed += 1;
                }
            }
            wal::sync_dir(&inner.root.join(dir))?;
        }
        s.decoded
            .retain(|id, _| referenced.contains(&format!("{id}.parquet")));
    }
    Ok(removed)
}

pub(crate) fn directory_bytes(root: &Path) -> Result<u64> {
    let mut total = 0u64;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        ensure!(
            !kind.is_symlink(),
            "symlinks are not permitted inside a database directory"
        );
        total = total
            .checked_add(if kind.is_dir() {
                directory_bytes(&entry.path())?
            } else {
                entry.metadata()?.len()
            })
            .context("disk accounting overflow")?;
    }
    Ok(total)
}
pub(crate) fn ensure_budget(inner: &Inner, additional: u64) -> Result<()> {
    let _timer = inner.metrics.timer(Phase::DiskAccount);
    ensure!(
        directory_bytes(&inner.root)?
            .saturating_add(additional)
            .saturating_add(if inner.journal.is_some() {
                crate::journal::RESERVED_SEAL_BYTES
            } else {
                0
            })
            <= inner.config.max_disk_bytes,
        "local disk admission budget exhausted; compact, archive, or increase max_disk_bytes"
    );
    Ok(())
}

#[cfg(all(test, feature = "fault-injection"))]
mod prefix_review_tests {
    use super::*;
    use std::time::Duration;
    use tempfile::TempDir;

    fn row(value: f64) -> Row {
        Row {
            timestamp_us: 1,
            tenant: "tenant".into(),
            series: "series".into(),
            value,
            tags: BTreeMap::new(),
        }
    }

    fn item(id: &str, rows: Vec<Row>, now_us: i64) -> wal::AppendItem {
        wal::AppendItem {
            table: "metrics".into(),
            request_id: id.into(),
            digest: blake3::hash(&serde_json::to_vec(&rows).unwrap())
                .to_hex()
                .to_string(),
            rows,
            now_us: Some(now_us),
        }
    }

    #[test]
    fn exact_wal_retry_recovers_owned_slots_sequence_proof_and_floor_baseline() {
        for pages in [false, true] {
            for fail in [false, true] {
                let temp = TempDir::new().unwrap();
                let config = Config {
                    checkpoint_frozen_prefix: true,
                    derived_pages: pages,
                    ..Config::default()
                };
                let db = Database::open(temp.path(), config.clone()).unwrap();
                db.create_table(
                    "metrics",
                    TableConfig {
                        shards: 1,
                        rollup_widths_us: vec![10],
                        idempotency_window_us: Some(100),
                        ..Default::default()
                    },
                )
                .unwrap();
                db.write("metrics", "v1:90:seed", vec![row(1.0); 8], 90)
                    .unwrap();
                let wal_bytes = db.status().unwrap().wal_bytes;
                drop(db);
                let config = Config {
                    wal_max_bytes: wal_bytes + 128,
                    ..config
                };
                let db = Database::open(temp.path(), config.clone()).unwrap();
                let a = item("v1:150:a", vec![row(2.0)], 150);
                let b = item("v1:150:b", vec![row(3.0)], 150);
                let expected = wal::Record::new(
                    4,
                    wal::Operation::AppendGroup {
                        items: vec![a.clone(), b.clone()],
                    },
                );
                let expected_frame = wal::encode(&expected).unwrap();
                assert!(expected_frame.len() as u64 <= config.wal_max_bytes);
                assert!(wal_bytes + expected_frame.len() as u64 > config.wal_max_bytes);
                let inputs = vec![
                    item("v1:90:seed", vec![row(1.0); 8], 90),
                    item("v1:90:seed", vec![row(9.0)], 90),
                    a.clone(),
                    a,
                    b,
                ];
                // Public admission deliberately overestimates the frame. Supply
                // an underestimated hint at the private attempt boundary to
                // exercise the defensive *exact* encoded-WAL pressure branch.
                // Real canonical items, encoding, WAL capacity, checkpoint and
                // publication all remain unchanged; no synthetic WAL bytes.
                let inputs = inputs
                    .into_iter()
                    .map(|item| prepared_test_input(item, 0))
                    .collect();
                let hook = MaintenanceTestHook::new(MaintenanceHookPhase::GroupCheckpointComplete);
                db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
                let worker = db.clone();
                let group = std::thread::spawn(move || {
                    worker.write_group_attempts(inputs, WriteMode::Group)
                });
                assert!(hook.wait_until_blocked(Duration::from_secs(5)));
                let status = db.status().unwrap();
                assert_eq!(
                    (
                        status.sequence,
                        status.checkpoint_sequence,
                        status.hot_rows,
                        status.idempotency_keys
                    ),
                    (2, 2, 0, 1)
                );
                assert_eq!(status.wal_bytes, 0);
                assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(-10));
                assert_eq!(db.rollups("metrics").unwrap()[0].count, 8);
                let tail = db
                    .write("metrics", "v1:100:tail", vec![row(4.0)], 190)
                    .unwrap();
                assert_eq!(tail.sequence, 3);
                db.checkpoint().unwrap();
                assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(90));
                if fail {
                    let failure =
                        MaintenanceTestHook::new(MaintenanceHookPhase::GroupBeforePublish);
                    failure.release_with_error();
                    db.set_maintenance_test_hook(Some(failure)).unwrap();
                }
                hook.release();
                let results = group.join().unwrap();
                assert_eq!(results.len(), 5);
                assert!(results[0].as_ref().unwrap().duplicate);
                assert_eq!(results[0].as_ref().unwrap().sequence, 2);
                assert!(format!("{:#}", results[1].as_ref().unwrap_err()).contains("conflicts"));
                if fail {
                    assert!(results[2..].iter().all(Result::is_err));
                    assert!(!wal::path(temp.path(), 4).exists());
                } else {
                    assert!(
                        results[2..]
                            .iter()
                            .all(|r| r.as_ref().unwrap().sequence == 4)
                    );
                    assert!(!results[2].as_ref().unwrap().duplicate);
                    assert!(results[3].as_ref().unwrap().duplicate);
                    assert!(!results[4].as_ref().unwrap().duplicate);
                    assert_eq!(fs::read(wal::path(temp.path(), 4)).unwrap(), expected_frame);
                    let proof = wal::group_fingerprint(&expected).unwrap().unwrap();
                    let s = db.lock().unwrap();
                    assert_eq!(
                        s.catalog.tables["metrics"].receipts["v1:150:a"]
                            .group_fingerprint
                            .as_ref(),
                        Some(&proof)
                    );
                    assert_eq!(
                        s.catalog.tables["metrics"].receipts["v1:150:b"]
                            .group_fingerprint
                            .as_ref(),
                        Some(&proof)
                    );
                }
                let count = if fail { 9 } else { 11 };
                assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(90));
                assert_eq!(db.rollups("metrics").unwrap()[0].count, count);
                assert_eq!(db.performance().phases["checkpoint_locked"].count, 0);
                assert_eq!(db.performance().phases["checkpoint_capture"].count, 2);
                assert!(
                    db.write("metrics", "v1:89:old", vec![row(0.0)], 89)
                        .is_err()
                );
                drop(db);
                let db = Database::open(temp.path(), config).unwrap();
                assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(90));
                assert_eq!(db.rollups("metrics").unwrap()[0].count, count);
                assert_eq!(
                    db.scan("metrics", None, None, None, None).unwrap().len(),
                    count as usize
                );
                assert!(
                    db.write("metrics", "v1:89:old", vec![row(0.0)], 89)
                        .is_err()
                );
            }
        }
    }

    #[test]
    fn later_duplicate_floor_survives_exact_wal_retry_in_input_order() {
        for frozen in [false, true] {
            for pages in [false, true] {
                for final_root in [false, true] {
                    let temp = TempDir::new().unwrap();
                    let config = Config {
                        checkpoint_frozen_prefix: frozen,
                        derived_pages: pages,
                        ..Default::default()
                    };
                    let db = Database::open(temp.path(), config.clone()).unwrap();
                    db.create_table(
                        "metrics",
                        TableConfig {
                            idempotency_window_us: Some(100),
                            ..Default::default()
                        },
                    )
                    .unwrap();
                    db.write("metrics", "v1:190:seed", vec![row(1.0); 8], 190)
                        .unwrap();
                    let wal_bytes = db.status().unwrap().wal_bytes;
                    drop(db);
                    let config = Config {
                        wal_max_bytes: wal_bytes + 128,
                        ..config
                    };
                    let db = Database::open(temp.path(), config.clone()).unwrap();
                    assert!(
                        db.write("metrics", "v1:190:seed", vec![row(1.0); 8], 190)
                            .unwrap()
                            .duplicate
                    );
                    let new = item("v1:100:new", vec![row(2.0)], 100);
                    let expected = wal::Record::new(
                        3,
                        wal::Operation::AppendGroup {
                            items: vec![new.clone()],
                        },
                    );
                    let frame = wal::encode(&expected).unwrap();
                    assert!(frame.len() as u64 <= config.wal_max_bytes);
                    assert!(wal_bytes + frame.len() as u64 > config.wal_max_bytes);
                    // Deliberately underestimated PRIVATE hints reach defensive exact-WAL
                    // pressure; public admission normally overestimates these bytes.
                    // Canonical records, capacities, checkpoint and fsync are real.
                    let results = db.write_group_attempts(
                        vec![
                            prepared_test_input(new, 0),
                            prepared_test_input(item("v1:190:seed", vec![row(1.0); 8], 250), 0),
                        ],
                        WriteMode::Group,
                    );
                    assert!(
                        results[0].is_ok(),
                        "frozen={frozen} pages={pages}: {results:?}"
                    );
                    assert_eq!(results[0].as_ref().unwrap().sequence, 3);
                    assert!(!results[0].as_ref().unwrap().duplicate);
                    assert!(results[1].as_ref().unwrap().duplicate);
                    assert_eq!(results[1].as_ref().unwrap().sequence, 2);
                    assert_eq!(db.status().unwrap().checkpoint_sequence, 2);
                    assert_eq!(db.status().unwrap().hot_rows, 1);
                    assert_eq!(fs::read(wal::path(temp.path(), 3)).unwrap(), frame);
                    assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(150));
                    assert!(
                        db.lock().unwrap().catalog.tables["metrics"]
                            .idempotency_floor_us
                            .unwrap()
                            <= 100
                    );
                    assert_eq!(
                        db.performance().phases[if frozen {
                            "checkpoint_capture"
                        } else {
                            "checkpoint_locked"
                        }]
                        .count,
                        1
                    );
                    if final_root {
                        db.checkpoint().unwrap();
                    }
                    drop(db);
                    let db = Database::open(temp.path(), config).unwrap();
                    assert_eq!(db.status().unwrap().sequence, 3);
                    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 9);
                    if final_root {
                        assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(150));
                        assert!(
                            db.write("metrics", "v1:100:new", vec![row(2.0)], 100)
                                .is_err()
                        );
                        assert_eq!(db.status().unwrap().idempotency_keys, 1);
                    } else {
                        assert!(
                            db.write("metrics", "v1:100:new", vec![row(2.0)], 100)
                                .unwrap()
                                .duplicate
                        );
                        assert_eq!(db.status().unwrap().idempotency_keys, 2);
                    }
                }
            }
        }
    }

    fn timed_request(id: &str, value: i64, now_us: i64) -> WriteRequest {
        WriteRequest {
            table: "metrics".into(),
            request_id: id.into(),
            rows: vec![row(value as f64)],
            now_us,
        }
    }

    #[test]
    fn timed_manifest_fence_duplicate_child() {
        let Ok(root) = std::env::var("VARVE_ORDERING_FAULT_ROOT") else {
            return;
        };
        let config = Config {
            checkpoint_frozen_prefix: true,
            derived_pages: std::env::var("VARVE_PREFIX_PAGES").unwrap() == "true",
            hot_max_rows: 2,
            ..Default::default()
        };
        let db = Database::open(root, config).unwrap();
        let results = db.write_group(vec![
            timed_request("v1:100:new", 2, 100),
            timed_request("v1:190:seed", 1, 250),
        ]);
        assert!(results[0].is_err());
        assert!(results[1].as_ref().unwrap().duplicate);
        assert_eq!(results[1].as_ref().unwrap().sequence, 2);
        assert_eq!(
            db.lock()
                .unwrap()
                .idempotency_floors
                .get("metrics")
                .copied(),
            Some(150)
        );
        assert!(db.status().unwrap().fenced.is_some());
        // The acknowledgment still refers only to the old immutable receipt.
        assert!(db.write_group(vec![timed_request("v1:190:seed", 1, 250)])[0].is_err());
        assert!(db.write_group(vec![timed_request("v1:160:new", 2, 160)])[0].is_err());
    }

    #[test]
    fn timed_manifest_ambiguity_retains_full_duplicate_clock_without_unfencing() {
        for pages in [false, true] {
            for point in [
                "atomic_manifest.bin_before_write",
                "atomic_manifest.bin_before_rename",
                "atomic_manifest.bin_before_dir_sync",
            ] {
                let temp = TempDir::new().unwrap();
                let config = Config {
                    checkpoint_frozen_prefix: true,
                    derived_pages: pages,
                    hot_max_rows: 2,
                    ..Default::default()
                };
                let db = Database::open(temp.path(), config.clone()).unwrap();
                db.create_table(
                    "metrics",
                    TableConfig {
                        idempotency_window_us: Some(100),
                        ..Default::default()
                    },
                )
                .unwrap();
                db.write("metrics", "v1:190:seed", vec![row(1.0)], 190)
                    .unwrap();
                db.write("metrics", "v1:190:fill", vec![row(3.0)], 190)
                    .unwrap();
                drop(db);
                let output = std::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "engine::prefix_review_tests::timed_manifest_fence_duplicate_child",
                        "--nocapture",
                    ])
                    .env("VARVE_ORDERING_FAULT_ROOT", temp.path())
                    .env("VARVE_PREFIX_PAGES", pages.to_string())
                    .env_remove("VARVE_FAILPOINT")
                    .env("VARVE_IO_FAILPOINT", point)
                    .output()
                    .unwrap();
                assert!(
                    output.status.success(),
                    "pages={pages} point={point}: {} {}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
                let db = Database::open(temp.path(), config.clone()).unwrap();
                assert_eq!(db.status().unwrap().sequence, 3);
                assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 2);
                assert_eq!(db.status().unwrap().idempotency_keys, 2);
                assert!(
                    db.write_group(vec![timed_request("v1:190:seed", 1, 250)])[0]
                        .as_ref()
                        .unwrap()
                        .duplicate
                );
                db.checkpoint().unwrap();
                drop(db);
                let db = Database::open(temp.path(), config).unwrap();
                assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(150));
                assert!(db.write_group(vec![timed_request("v1:100:new", 2, 100)])[0].is_err());
                assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 2);
            }
        }
    }

    #[test]
    fn competing_same_sequence_root_stales_frozen_candidate_without_control_change() {
        for pages in [false, true] {
            let temp = TempDir::new().unwrap();
            let config = Config {
                checkpoint_frozen_prefix: true,
                derived_pages: pages,
                ..Default::default()
            };
            let db = Database::open(temp.path(), config.clone()).unwrap();
            db.create_table(
                "metrics",
                TableConfig {
                    idempotency_window_us: Some(100),
                    ..Default::default()
                },
            )
            .unwrap();
            db.write("metrics", "v1:10:seed", vec![row(1.0)], 10)
                .unwrap();
            db.checkpoint().unwrap();
            assert!(
                db.write("metrics", "v1:10:seed", vec![row(1.0)], 20)
                    .unwrap()
                    .duplicate
            );
            {
                let s = db.lock().unwrap();
                assert_eq!(s.sequence, s.catalog.checkpoint_sequence);
                assert!(idempotency_checkpoint_due(&s));
            }
            let hook = MaintenanceTestHook::new(MaintenanceHookPhase::RootPrepare);
            db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
            let worker = db.clone();
            let checkpoint = std::thread::spawn(move || worker.checkpoint());
            assert!(hook.wait_until_blocked(std::time::Duration::from_secs(5)));
            db.set_maintenance_test_hook(None).unwrap();
            // Persist only the already-live floor at the existing frontier.
            // Of the frozen candidate's scalar stamps, only root_epoch changes.
            let competing = {
                let mut s = db.lock().unwrap();
                let before = (
                    s.sequence,
                    s.catalog.checkpoint_sequence,
                    s.control_epoch,
                    s.idempotency_floors.clone(),
                );
                let root = s.root_epoch;
                let result = checkpoint_locked(&db.inner, &mut s);
                assert_eq!(
                    (
                        s.sequence,
                        s.catalog.checkpoint_sequence,
                        s.control_epoch,
                        s.idempotency_floors.clone()
                    ),
                    before
                );
                assert_eq!(s.root_epoch, root + 1);
                result
            };
            hook.release();
            let error = checkpoint.join().unwrap().unwrap_err();
            competing.unwrap();
            assert!(format!("{error:#}").contains("stale frozen-prefix"));
            assert_eq!(db.status().unwrap().checkpoint_sequence, 2);
            assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 1);
            drop(db);
            let db = Database::open(temp.path(), config).unwrap();
            assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(-80));
            assert!(
                db.write("metrics", "v1:10:seed", vec![row(1.0)], 20)
                    .unwrap()
                    .duplicate
            );
            assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 1);
        }
    }

    #[test]
    fn capped_no_progress_retry_still_applies_full_terminal_duplicate_clock() {
        for pages in [false, true] {
            let temp = TempDir::new().unwrap();
            let config = Config {
                checkpoint_frozen_prefix: true,
                derived_pages: pages,
                ..Default::default()
            };
            let db = Database::open(temp.path(), config.clone()).unwrap();
            db.create_table(
                "metrics",
                TableConfig {
                    idempotency_window_us: Some(100),
                    ..Default::default()
                },
            )
            .unwrap();
            db.write("metrics", "v1:190:seed", vec![row(1.0)], 190)
                .unwrap();
            db.write("metrics", "v1:190:seed", vec![row(1.0)], 200)
                .unwrap();
            db.checkpoint().unwrap();
            assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(100));
            // A private overestimated hint forces a defensive no-op checkpoint;
            // this is not a claim of natural public-path reachability.
            let results = db.write_group_attempts(
                vec![
                    prepared_test_input(
                        item("v1:100:new", vec![row(2.0)], 100),
                        config.wal_max_bytes as usize,
                    ),
                    prepared_test_input(item("v1:190:seed", vec![row(1.0)], 250), 0),
                ],
                WriteMode::Group,
            );
            assert!(
                format!("{:#}", results[0].as_ref().unwrap_err())
                    .contains("did not publish a new durable root")
            );
            assert!(results[1].as_ref().unwrap().duplicate);
            assert_eq!(results[1].as_ref().unwrap().sequence, 2);
            assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(150));
            assert_eq!(db.status().unwrap().sequence, 2);
            assert!(db.status().unwrap().fenced.is_none());
            db.checkpoint().unwrap();
            drop(db);
            let db = Database::open(temp.path(), config).unwrap();
            assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(150));
            assert!(
                db.write("metrics", "v1:100:new", vec![row(2.0)], 100)
                    .is_err()
            );
            assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 1);
        }
    }

    #[test]
    fn no_progress_error_preserves_only_verified_durable_outcomes() {
        let temp = TempDir::new().unwrap();
        let config = Config {
            checkpoint_frozen_prefix: true,
            ..Config::default()
        };
        let db = Database::open(temp.path(), config.clone()).unwrap();
        db.create_table("metrics", TableConfig::default()).unwrap();
        db.write("metrics", "seed", vec![row(1.0)], 0).unwrap();
        db.checkpoint().unwrap();
        let results = db.write_group_attempts(
            vec![
                prepared_test_input(item("seed", vec![row(1.0)], 0), 0),
                prepared_test_input(item("seed", vec![row(2.0)], 0), 0),
                // A conservative capacity hint can request a clean/no-op checkpoint.
                prepared_test_input(
                    item("new", vec![row(3.0)], 0),
                    config.wal_max_bytes as usize,
                ),
            ],
            WriteMode::Group,
        );
        assert!(results[0].as_ref().unwrap().duplicate);
        assert!(format!("{:#}", results[1].as_ref().unwrap_err()).contains("conflicts"));
        assert!(
            format!("{:#}", results[2].as_ref().unwrap_err())
                .contains("did not publish a new durable root")
        );
        assert_eq!(db.status().unwrap().sequence, 2);
        assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 1);
        assert_eq!(db.performance().phases["checkpoint_locked"].count, 0);
    }

    #[test]
    fn legacy_job_start_and_finish_each_invalidate_frozen_control_epoch() {
        for pages in [false, true] {
            let temp = TempDir::new().unwrap();
            let config = Config {
                checkpoint_frozen_prefix: true,
                derived_pages: pages,
                ..Config::default()
            };
            let db = Database::open(temp.path(), config.clone()).unwrap();
            db.create_table("metrics", TableConfig::default()).unwrap();
            db.create_job("legacy_job", JobKind::Compact, 10).unwrap();
            db.write("metrics", "seed", vec![row(1.0)], 1).unwrap();
            let mut run = JobRun {
                run_id: 1,
                scheduled_us: 1,
                started_us: 1,
                finished_us: None,
                attempt: 1,
                success: None,
                error: None,
            };
            for finished in [false, true] {
                let before = fs::read(temp.path().join("manifest.bin")).unwrap();
                let hook = MaintenanceTestHook::new(MaintenanceHookPhase::RootPrepare);
                db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
                let worker = db.clone();
                let checkpoint = std::thread::spawn(move || worker.checkpoint());
                assert!(hook.wait_until_blocked(Duration::from_secs(5)));
                {
                    let mut s = db.lock().unwrap();
                    let epoch = s.control_epoch;
                    let operation = if finished {
                        run.finished_us = Some(2);
                        run.success = Some(true);
                        wal::Operation::JobFinished {
                            name: "legacy_job".into(),
                            run: run.clone(),
                            next_run_us: 12,
                        }
                    } else {
                        wal::Operation::JobStarted {
                            name: "legacy_job".into(),
                            run: run.clone(),
                            manual: true,
                        }
                    };
                    let record = wal::Record::new(next_sequence(&s).unwrap(), operation);
                    // Legacy job WAL still uses the real commit/replay guard;
                    // current jobs use their independent private runtime journal.
                    commit_record(&db.inner, &mut s, &record).unwrap();
                    replay(&mut s, record, &config).unwrap();
                    assert_eq!(s.control_epoch, epoch + 1);
                    assert_eq!(s.catalog.jobs["legacy_job"].running, !finished);
                }
                hook.release();
                let error = checkpoint.join().unwrap().unwrap_err();
                assert!(format!("{error:#}").contains("stale frozen-prefix"));
                assert_eq!(fs::read(temp.path().join("manifest.bin")).unwrap(), before);
                assert_eq!(db.status().unwrap().derived_working_bytes, 0);
                assert!(db.status().unwrap().fenced.is_none());
                db.set_maintenance_test_hook(None).unwrap();
            }
            db.checkpoint().unwrap();
            drop(db);
            let db = Database::open(temp.path(), config).unwrap();
            assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 1);
        }
    }
}

#[cfg(all(test, feature = "fault-injection"))]
#[path = "commit_boundary_tests.rs"]
mod commit_boundary_tests;

#[cfg(all(test, feature = "fault-injection"))]
#[path = "commit_boundary_multitable_tests.rs"]
mod commit_boundary_multitable_tests;

#[cfg(all(test, feature = "fault-injection"))]
#[path = "owned_epoch_tests.rs"]
mod owned_epoch_tests;

#[cfg(test)]
#[path = "raw_budget_tests.rs"]
mod raw_budget_tests;

#[cfg(test)]
#[path = "raw_pressure_tests.rs"]
mod raw_pressure_tests;

#[cfg(test)]
#[path = "raw_lineage_tests.rs"]
mod raw_lineage_tests;

#[cfg(test)]
#[path = "engine_metrics_tests.rs"]
mod engine_metrics_tests;

#[cfg(test)]
#[path = "append_accounting_tests.rs"]
mod append_accounting_tests;

#[cfg(test)]
#[path = "hot_path_tests.rs"]
mod hot_path_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn checkpoint_output_rows_require_an_enabled_fitting_cache() -> Result<()> {
        for (enabled, budget, expected) in [
            (false, 8 * 1024 * 1024, false),
            (true, 0, false),
            (true, 1, false),
            (true, 8 * 1024 * 1024, true),
        ] {
            let dir = TempDir::new()?;
            let db = Database::open(
                dir.path(),
                Config {
                    query_retained_inputs: enabled,
                    decoded_cache_bytes: budget,
                    ..Config::default()
                },
            )?;
            let table = TableConfig::default();
            db.create_table("metrics", table.clone())?;
            db.write(
                "metrics",
                "one",
                vec![Row {
                    timestamp_us: 0,
                    tenant: "tenant".into(),
                    series: "series".into(),
                    value: 1.0,
                    tags: BTreeMap::new(),
                }],
                0,
            )?;
            let batches = db.lock()?.hot["metrics"].clone();
            let written = write_resident_partitioned_with_pin(&db.inner, &table, &batches, None)?;
            assert_eq!(written.len(), 1);
            assert_eq!(written[0].rows.is_some(), expected);
            if let Some(rows) = &written[0].rows {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].row.value, 1.0);
            }
        }
        Ok(())
    }

    #[test]
    fn derived_checkpoint_headroom_covers_clone_indexes_and_sticky_pages() -> Result<()> {
        for derived_pages in [false, true] {
            let dir = TempDir::new()?;
            let config = Config {
                derived_pages,
                derived_page_bytes: 4096,
                ..Config::default()
            };
            let db = Database::open(dir.path(), config.clone())?;
            db.create_table("metrics", TableConfig::default())?;
            db.write(
                "metrics",
                "first",
                vec![Row {
                    timestamp_us: 0,
                    tenant: "tenant".into(),
                    series: "series".into(),
                    value: 1.0,
                    tags: BTreeMap::new(),
                }],
                0,
            )?;
            let mut s = db.lock()?;
            let resident = s.derived_resident_bytes;
            let maps = derived_root::resident_bytes(&s.catalog, false);
            let pages = if derived_pages {
                4 * config.derived_page_bytes
            } else {
                0
            };
            let boundary = Config {
                // An authoritative v2 root remains paged with the flag disabled.
                derived_pages: false,
                derived_max_bytes: 2 * resident + pages,
                ..config.clone()
            };
            check_derived_checkpoint_headroom(&s, &boundary, resident)?;
            {
                let _clone = reserve_catalog_clone(&s, &boundary)?;
                let _indexes = reserve_derived(&s, &boundary, resident - maps + pages)?;
                assert_eq!(s.derived_working.load(Ordering::SeqCst), resident + pages);
            }
            let insufficient = Config {
                derived_max_bytes: boundary.derived_max_bytes - 1,
                ..boundary
            };
            assert!(check_derived_checkpoint_headroom(&s, &insufficient, resident).is_err());
            s.replaying = true;
            check_derived_checkpoint_headroom(&s, &insufficient, resident)?;
            s.replaying = false;
            checkpoint_locked(&db.inner, &mut s)?;
            assert_eq!(s.catalog.checkpoint_sequence, s.sequence);
            assert_eq!(s.derived_working.load(Ordering::SeqCst), 0);
        }
        Ok(())
    }

    #[test]
    fn derived_dimensions_are_validated_in_the_legacy_envelope_too() -> Result<()> {
        let dir = TempDir::new()?;
        let db = Database::open(dir.path(), Config::default())?;
        db.create_table("metrics", TableConfig::default())?;
        db.write(
            "metrics",
            "id",
            vec![Row {
                timestamp_us: 0,
                tenant: "tenant".into(),
                series: "series".into(),
                value: 1.0,
                tags: BTreeMap::new(),
            }],
            0,
        )?;
        db.checkpoint()?;
        let mut catalog = db.lock()?.catalog.clone();
        catalog
            .tables
            .get_mut("metrics")
            .unwrap()
            .rollups
            .values_mut()
            .next()
            .unwrap()
            .tenant
            .clear();
        let encoded = encode_manifest(&catalog)?;
        assert!(
            decode_manifest(&encoded)
                .unwrap_err()
                .to_string()
                .contains("tenant")
        );
        Ok(())
    }

    #[test]
    fn derived_greedy_bounds_cover_names_escaping_and_decimal_edges() -> Result<()> {
        for name in ["m".to_owned(), "m".repeat(63)] {
            for page_bytes in [4096, 16_384, 131_072] {
                for count in [0, 1, 9, 10, 99, 100] {
                    let config = Config {
                        derived_page_bytes: page_bytes,
                        ..Config::default()
                    };
                    let mut rollups = BTreeMap::new();
                    let mut receipts = BTreeMap::new();
                    for i in 0..count {
                        let size = [0, 17, 127, 511, 1024][i % 5];
                        let source = Row {
                            timestamp_us: i as i64 - 100,
                            tenant: "é".repeat(if i % 3 == 0 { 128 } else { 1 }),
                            series: format!("s{i}"),
                            value: if i % 2 == 0 { -0.0 } else { f64::MIN_POSITIVE },
                            tags: BTreeMap::from([("tag".into(), "\"".repeat(size))]),
                        };
                        source.validate()?;
                        let row = RollupRow::from_row(
                            10,
                            &StoredRow {
                                row: source,
                                sequence: [9, 10, 99, 100, u64::MAX][i % 5],
                                ordinal: 0,
                            },
                        )?;
                        rollups.insert(derived::canonical_key(&row)?, row);
                        receipts.insert(
                            format!("r{i}{}", "\\".repeat(i % 128)),
                            ReceiptEntry {
                                sequence: [9, 10, 99, 100, u64::MAX][i % 5],
                                rows: i + 1,
                                digest: "a".repeat(64),
                                issued_us: None,
                                group_fingerprint: Some("b".repeat(64)),
                            },
                        );
                    }
                    let mut accounting = TableAccounting {
                        rollups: map_accounting(&rollups)?,
                        receipts: map_accounting(&receipts)?,
                        ..Default::default()
                    };
                    accounting.bound(&name, &config)?;
                    let actual = derived::encode_rollups(
                        &name,
                        &rollups,
                        derived_root::limits(&config, false, config.derived_max_bytes),
                        |_, _| Ok(()),
                    );
                    if accounting.entries_fit(&name, &config).is_err() {
                        assert!(actual.is_err(), "oversized entry unexpectedly encoded");
                        continue;
                    }
                    let actual = actual?;
                    let receipts = derived::encode_receipts(
                        &name,
                        &receipts,
                        derived_root::limits(&config, true, config.derived_max_bytes),
                        |_, _| Ok(()),
                    )?;
                    assert!(
                        actual.encoded_bytes as usize + receipts.encoded_bytes as usize
                            <= accounting.encoded_bound
                    );
                    let refs = DerivedRefs {
                        rollups: actual,
                        receipts,
                    };
                    let root_extra = json_bytes(&refs)? - json_bytes(&DerivedRefs::default())?;
                    assert!(
                        root_extra <= accounting.root_bound,
                        "root bound: {name}/{page_bytes}/{count}"
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn derived_append_caches_match_recomputation_without_page_encoding() -> Result<()> {
        fn verify(s: &State, config: &Config) -> Result<()> {
            assert_eq!(
                s.derived_resident_bytes,
                derived_root::resident_bytes(&s.catalog, true)
            );
            let rebuilt = DerivedAccounting::build(&s.catalog, config)?;
            assert_eq!(s.derived_accounting.root_bound, rebuilt.root_bound);
            assert_eq!(s.derived_accounting.encoded_bound, rebuilt.encoded_bound);
            for (name, table) in &s.catalog.tables {
                let cached = &s.derived_accounting.tables[name];
                assert_eq!(cached.rollups.json_bytes, json_bytes(&table.rollups)?);
                assert_eq!(cached.receipts.json_bytes, json_bytes(&table.receipts)?);
                assert_eq!(cached.rollups.entries, table.rollups.len());
                assert_eq!(cached.receipts.entries, table.receipts.len());
                let selected = s.rollup_indexes[name].select(
                    &table.rollups,
                    RollupSelection::default(),
                    config.derived_max_bytes,
                )?;
                assert_eq!(selected, table.rollups.values().collect::<Vec<_>>());
                let actual = derived::encode_rollups(
                    name,
                    &table.rollups,
                    derived_root::limits(config, false, config.derived_max_bytes),
                    |_, _| Ok(()),
                )?;
                let receipts = derived::encode_receipts(
                    name,
                    &table.receipts,
                    derived_root::limits(config, true, config.derived_max_bytes),
                    |_, _| Ok(()),
                )?;
                assert!(
                    actual.encoded_bytes as usize + receipts.encoded_bytes as usize
                        <= cached.encoded_bound
                );
                let root_extra = serde_json::to_vec(&DerivedRefs {
                    rollups: actual,
                    receipts,
                })?
                .len()
                    - serde_json::to_vec(&DerivedRefs::default())?.len();
                assert!(root_extra <= cached.root_bound);
            }
            Ok(())
        }
        for derived_pages in [false, true] {
            let dir = TempDir::new()?;
            let config = Config {
                derived_pages,
                derived_page_bytes: 4096,
                ..Config::default()
            };
            let db = Database::open(dir.path(), config.clone())?;
            for table in ["metrics", "other"] {
                db.create_table(
                    table,
                    TableConfig {
                        rollup_widths_us: vec![10, 20],
                        ..TableConfig::default()
                    },
                )?;
            }
            for i in 0..12 {
                let row = Row {
                    timestamp_us: i - 6,
                    tenant: "é".into(),
                    series: "雪".into(),
                    value: i as f64,
                    tags: BTreeMap::from([("tag".into(), "x".repeat(40 + (i as usize % 3) * 60))]),
                };
                let before = derived::codec_calls();
                db.write("metrics", &format!("single{i}"), vec![row.clone()], 10)?;
                assert_eq!(
                    derived::codec_calls(),
                    before,
                    "append must not encode any page set"
                );
                let results = db.write_group(vec![WriteRequest {
                    table: "other".into(),
                    request_id: format!("group{i}"),
                    rows: vec![row],
                    now_us: 10,
                }]);
                results.into_iter().next().unwrap()?;
                assert_eq!(
                    derived::codec_calls(),
                    before,
                    "group stage/apply must not encode any page set"
                );
                let s = db.lock()?;
                verify(&s, &config)?;
            }
            {
                let mut s = db.lock()?;
                let before = s.derived_resident_bytes;
                let rows = vec![Row {
                    timestamp_us: -1,
                    tenant: "t".into(),
                    series: "s".into(),
                    value: 3.0,
                    tags: BTreeMap::new(),
                }];
                let item = wal::AppendItem {
                    table: "metrics".into(),
                    request_id: "provisional".into(),
                    digest: blake3::hash(&serde_json::to_vec(&rows)?)
                        .to_hex()
                        .to_string(),
                    rows,
                    now_us: Some(10),
                };
                let prepared =
                    prepare_group_append(&s, &item, next_sequence(&s)?, 0, None, &config, None)?;
                let undo = apply_group_append(&mut s, prepared);
                verify(&s, &config)?;
                undo_group_append(&mut s, undo);
                verify(&s, &config)?;
                assert_eq!(s.derived_resident_bytes, before);
                assert_eq!(s.derived_working.load(Ordering::SeqCst), 0);
            }
            db.checkpoint()?;
            drop(db);
            let db = Database::open(dir.path(), config.clone())?;
            let s = db.lock()?;
            verify(&s, &config)?;
        }
        Ok(())
    }

    #[test]
    #[cfg(unix)]
    fn final_owner_releases_lock_despite_a_surviving_duplicate_descriptor() {
        let temp = TempDir::new().unwrap();
        let db = Database::open(temp.path(), Config::default()).unwrap();
        let owner = db.clone();
        // dup and fork retain the same open-file description behind a flock.
        let inherited = db.inner._file_lock.try_clone().unwrap();
        drop(db);
        assert!(Database::open(temp.path(), Config::default()).is_err());
        drop(owner);
        let reopened = Database::open(temp.path(), Config::default())
            .expect("the final database owner must release its lock explicitly");
        drop(inherited);
        assert!(Database::open(temp.path(), Config::default()).is_err());
        drop(reopened);
        assert!(Database::open(temp.path(), Config::default()).is_ok());
    }

    #[test]
    fn live_group_applies_each_new_item_once_and_preserves_replay() -> Result<()> {
        for derived_pages in [false, true] {
            let dir = TempDir::new()?;
            let config = Config {
                derived_pages,
                ..Config::default()
            };
            let db = Database::open(dir.path(), config.clone())?;
            db.create_table(
                "metrics",
                TableConfig {
                    rollup_widths_us: vec![10, 20],
                    ..TableConfig::default()
                },
            )?;
            let request = |id: &str, value: f64| WriteRequest {
                table: "metrics".into(),
                request_id: id.into(),
                now_us: 10,
                rows: vec![Row {
                    timestamp_us: 2,
                    tenant: "tenant".into(),
                    series: "series".into(),
                    value,
                    tags: BTreeMap::new(),
                }],
            };
            let before = GROUP_APPEND_APPLICATIONS.with(std::cell::Cell::get);
            let results = db.write_group(vec![
                request("a", 2.0),
                request("a", 2.0),
                request("a", 9.0),
                request("b", 3.0),
            ]);
            assert!(results[0].is_ok());
            assert!(results[1].as_ref().unwrap().duplicate);
            assert!(results[2].is_err());
            assert!(results[3].is_ok());
            assert_eq!(
                GROUP_APPEND_APPLICATIONS.with(std::cell::Cell::get) - before,
                0,
                "live preparation must never mutate/undo committed maps"
            );
            let rollups = db.rollups("metrics")?;
            assert!(
                rollups
                    .iter()
                    .all(|r| r.count == 2 && r.sum == 5.0 && r.last == 3.0)
            );
            {
                let s = db.lock()?;
                assert_eq!(s.metadata_bytes, logical_metadata_bytes(&s.catalog)?);
                assert_eq!(
                    s.derived_resident_bytes,
                    derived_root::resident_bytes(&s.catalog, true)
                );
                assert_eq!(s.derived_working.load(Ordering::SeqCst), 0);
                let bytes =
                    wal::read_bounded(&wal::path(dir.path(), s.sequence), wal::MAX_FRAME_BYTES)?;
                let fingerprint = wal::group_fingerprint(&wal::decode(&bytes)?)?;
                for receipt in s.catalog.tables["metrics"].receipts.values() {
                    assert_eq!(receipt.group_fingerprint, fingerprint);
                }
            }
            drop(db);
            let db = Database::open(dir.path(), config)?;
            assert_eq!(db.rollups("metrics")?, rollups);
            assert_eq!(db.scan("metrics", None, None, None, None)?.len(), 2);
            assert!(
                db.write_group(vec![request("a", 2.0)])[0]
                    .as_ref()
                    .unwrap()
                    .duplicate
            );
            db.checkpoint()?;
        }
        Ok(())
    }

    #[test]
    fn group_undo_metadata_is_bounded_and_exact() {
        let temp = tempfile::TempDir::new().unwrap();
        let db = Database::open(
            temp.path(),
            Config {
                metadata_max_bytes: 10_000,
                ..Default::default()
            },
        )
        .unwrap();
        db.create_table(
            "metrics",
            TableConfig {
                rollup_widths_us: vec![10, 20, 30, 40, 50, 60],
                ..Default::default()
            },
        )
        .unwrap();
        let request = |id: &str| WriteRequest {
            table: "metrics".into(),
            request_id: id.into(),
            now_us: 0,
            rows: vec![Row {
                timestamp_us: 0,
                tenant: "t".into(),
                series: "s".into(),
                value: 1.0,
                tags: BTreeMap::new(),
            }],
        };
        let seed = request("seed");
        db.write(&seed.table, &seed.request_id, seed.rows, 0)
            .unwrap();
        let results = db.write_group(vec![request("a"), request("b"), request("a")]);
        assert!(results[0].is_ok());
        assert!(format!("{:#}", results[1].as_ref().unwrap_err()).contains("rollback metadata"));
        assert!(results[2].as_ref().unwrap().duplicate);
        let s = db.lock().unwrap();
        assert_eq!(s.metadata_bytes, encode_manifest(&s.catalog).unwrap().len());
        assert_eq!(hot_count(&s), 2);
        assert_eq!(s.catalog.tables["metrics"].receipts.len(), 2);
        assert!(
            s.catalog.tables["metrics"]
                .rollups
                .values()
                .all(|row| row.count == 2)
        );
    }

    #[test]
    fn group_proof_metadata_admission_is_exact_before_publication() {
        for short_by in [0, 1] {
            let temp = TempDir::new().unwrap();
            let mut config = Config::default();
            let db = Database::open(temp.path(), config.clone()).unwrap();
            db.create_table(
                "metrics",
                TableConfig {
                    rollup_widths_us: Vec::new(),
                    ..Default::default()
                },
            )
            .unwrap();
            db.checkpoint().unwrap();
            let request = |id: &str| WriteRequest {
                table: "metrics".into(),
                request_id: id.into(),
                now_us: 10,
                rows: vec![Row {
                    timestamp_us: 1,
                    tenant: "t".into(),
                    series: "s".into(),
                    value: 1.0,
                    tags: BTreeMap::new(),
                }],
            };
            let mut s = db.lock().unwrap();
            let before = encode_manifest(&s.catalog).unwrap();
            let sequence = next_sequence(&s).unwrap();
            let mut undo = Vec::new();
            let mut required = 0;
            for id in ["a", "b"] {
                let request = request(id);
                let item = wal::AppendItem {
                    table: request.table,
                    request_id: request.request_id,
                    digest: blake3::hash(&serde_json::to_vec(&request.rows).unwrap())
                        .to_hex()
                        .to_string(),
                    rows: request.rows,
                    now_us: Some(request.now_us),
                };
                let prepared = prepare_group_append(
                    &s,
                    &item,
                    sequence,
                    undo.len(),
                    Some(GROUP_PROOF_RESERVATION),
                    &config,
                    None,
                )
                .unwrap();
                let mut legacy = prepared.receipt.clone();
                legacy.group_fingerprint = None;
                assert_eq!(
                    serde_json::to_vec(&prepared.receipt).unwrap().len()
                        - serde_json::to_vec(&legacy).unwrap().len(),
                    87
                );
                // This fixture has no aggregates: the new proof still consumes working space.
                assert_eq!(prepared.working_bytes(), 64);
                required = append_metadata_bytes(
                    &s,
                    "metrics",
                    id,
                    &prepared.receipt,
                    &prepared.updates,
                    sequence,
                )
                .unwrap()
                    + (hot_count(&s) + 1) * 512;
                undo.push(apply_group_append(&mut s, prepared));
                assert_eq!(s.metadata_bytes, encode_manifest(&s.catalog).unwrap().len());
            }
            for entry in undo.into_iter().rev() {
                undo_group_append(&mut s, entry);
            }
            assert_eq!(encode_manifest(&s.catalog).unwrap(), before);
            assert_eq!(s.metadata_bytes, before.len());
            drop(s);
            drop(db);
            config.metadata_max_bytes = required - short_by;
            let db = Database::open(temp.path(), config.clone()).unwrap();
            let results = db.write_group(vec![request("a"), request("b"), request("a")]);
            assert!(results[0].is_ok());
            assert!(results[2].as_ref().unwrap().duplicate);
            if short_by == 0 {
                assert!(results[1].is_ok());
            } else {
                assert!(
                    format!("{:#}", results[1].as_ref().unwrap_err())
                        .contains("projected checkpoint exceeds metadata")
                );
            }
            let bytes =
                fs::read(temp.path().join("wal").join(format!("{sequence:020}.wal"))).unwrap();
            let record = wal::decode(&bytes).unwrap();
            let proof = wal::group_fingerprint(&record).unwrap().unwrap();
            assert_ne!(proof, GROUP_PROOF_RESERVATION);
            let check = |db: &Database| {
                let s = db.lock().unwrap();
                assert_eq!(s.metadata_bytes, encode_manifest(&s.catalog).unwrap().len());
                assert_eq!(s.catalog.tables["metrics"].receipts.len(), 2 - short_by);
                for receipt in s.catalog.tables["metrics"].receipts.values() {
                    assert_eq!(receipt.group_fingerprint.as_deref(), Some(proof.as_str()));
                }
            };
            check(&db);
            drop(db);
            let db = Database::open(temp.path(), config.clone()).unwrap();
            check(&db);
            db.checkpoint().unwrap();
            check(&db);
            drop(db);
            let db = Database::open(temp.path(), config).unwrap();
            check(&db);
        }
    }

    #[test]
    fn group_headroom_checkpoints_only_durable_state_before_replanning() {
        group_headroom_case(false, false);
    }

    #[test]
    fn group_headroom_replanning_restores_provisional_clock_floors() {
        group_headroom_case(true, false);
    }

    #[test]
    fn frozen_group_headroom_retries_once_after_complete_undo() {
        group_headroom_case(false, true);
        group_headroom_case(true, true);
    }

    fn group_headroom_case(timed: bool, frozen_prefix: bool) {
        let temp = TempDir::new().unwrap();
        let mut config = Config {
            checkpoint_frozen_prefix: frozen_prefix,
            ..Config::default()
        };
        let db = Database::open(temp.path(), config.clone()).unwrap();
        db.create_table(
            "metrics",
            TableConfig {
                rollup_widths_us: Vec::new(),
                idempotency_window_us: timed.then_some(100),
                ..Default::default()
            },
        )
        .unwrap();
        db.checkpoint().unwrap();
        let request = |id: &str, count: usize| {
            let now_us = match id {
                "seed" => 90,
                "b" => 201,
                _ => 100,
            };
            WriteRequest {
                table: "metrics".into(),
                request_id: if timed {
                    format!("v1:{now_us}:{id}")
                } else {
                    id.into()
                },
                now_us,
                rows: vec![
                    Row {
                        timestamp_us: 1,
                        tenant: "t".into(),
                        series: "s".into(),
                        value: 1.0,
                        tags: BTreeMap::new(),
                    };
                    count
                ],
            }
        };
        let seed = request("seed", 16);
        let seed_receipt = db
            .write(&seed.table, &seed.request_id, seed.rows, seed.now_us)
            .unwrap();
        let mut s = db.lock().unwrap();
        let sequence = next_sequence(&s).unwrap();
        let before = encode_manifest(&s.catalog).unwrap();
        let mut undo = Vec::new();
        let mut required = 0;
        for id in ["a", "b"] {
            let request = request(id, 2);
            let item = wal::AppendItem {
                table: request.table,
                request_id: request.request_id.clone(),
                digest: blake3::hash(&serde_json::to_vec(&request.rows).unwrap())
                    .to_hex()
                    .to_string(),
                rows: request.rows,
                now_us: Some(request.now_us),
            };
            let prepared = prepare_group_append(
                &s,
                &item,
                sequence,
                undo.len() * 2,
                Some(GROUP_PROOF_RESERVATION),
                &config,
                None,
            )
            .unwrap();
            required = append_metadata_bytes(
                &s,
                "metrics",
                &request.request_id,
                &prepared.receipt,
                &prepared.updates,
                sequence,
            )
            .unwrap()
                + (hot_count(&s) + 2) * 512;
            undo.push(apply_group_append(&mut s, prepared));
        }
        for entry in undo.into_iter().rev() {
            undo_group_append(&mut s, entry);
        }
        assert_eq!(encode_manifest(&s.catalog).unwrap(), before);
        assert!(s.metadata_bytes + hot_count(&s) * 512 < required - 1);
        drop(s);
        drop(db);
        config.metadata_max_bytes = required - 1;
        let db = Database::open(temp.path(), config.clone()).unwrap();
        let mut unknown = request("unknown", 1);
        unknown.table = "missing".into();
        let results = db.write_group(vec![
            request("a", 2),
            unknown,
            request("b", 2),
            request("a", 2),
            request("seed", 16),
        ]);
        assert!(results[0].is_ok());
        assert!(results[1].is_err());
        assert!(results[2].is_ok(), "{:#}", results[2].as_ref().unwrap_err());
        if timed {
            // The later request expires these IDs, but must not retroactively reject the earlier staged write.
            assert!(
                format!("{:#}", results[3].as_ref().unwrap_err())
                    .contains("outside the idempotency window")
            );
            assert!(
                format!("{:#}", results[4].as_ref().unwrap_err())
                    .contains("outside the idempotency window")
            );
        } else {
            assert!(results[3].as_ref().unwrap().duplicate);
            assert_eq!(results[4].as_ref().unwrap().sequence, seed_receipt.sequence);
            assert!(results[4].as_ref().unwrap().duplicate);
        }
        assert_eq!(results[0].as_ref().unwrap().sequence, sequence);
        assert_eq!(results[2].as_ref().unwrap().sequence, sequence);
        let check = |db: &Database| {
            let s = db.lock().unwrap();
            assert_eq!(s.catalog.checkpoint_sequence, seed_receipt.sequence);
            assert_eq!(hot_count(&s), 4);
            assert_eq!(
                s.catalog.tables["metrics"]
                    .segments
                    .iter()
                    .map(|segment| segment.rows)
                    .sum::<u64>(),
                16
            );
            assert_eq!(s.catalog.tables["metrics"].receipts.len(), 3);
            assert_eq!(s.metadata_bytes, encode_manifest(&s.catalog).unwrap().len());
            assert!(s.fenced.is_none());
        };
        check(&db);
        assert_eq!(
            db.scan("metrics", None, None, None, None).unwrap().len(),
            20
        );
        let durable = decode_manifest(
            &wal::read_bounded(&temp.path().join("manifest.bin"), wal::MAX_FRAME_BYTES).unwrap(),
        )
        .unwrap();
        assert_eq!(durable.tables["metrics"].receipts.len(), 1);
        assert!(
            durable.tables["metrics"]
                .receipts
                .contains_key(&request("seed", 16).request_id)
        );
        drop(db);
        let db = Database::open(temp.path(), config).unwrap();
        check(&db);
    }

    #[test]
    fn legacy_group_wal_replay_preserves_absent_clocks() {
        let temp = TempDir::new().unwrap();
        let db = Database::open(temp.path(), Config::default()).unwrap();
        db.create_table("metrics", TableConfig::default()).unwrap();
        let sequence = next_sequence(&db.lock().unwrap()).unwrap();
        drop(db);
        let rows = vec![Row {
            timestamp_us: 1,
            tenant: "t".into(),
            series: "s".into(),
            value: 1.0,
            tags: BTreeMap::new(),
        }];
        let record = wal::Record::new(
            sequence,
            wal::Operation::AppendGroup {
                items: vec![wal::AppendItem {
                    table: "metrics".into(),
                    request_id: "legacy-group".into(),
                    digest: blake3::hash(&serde_json::to_vec(&rows).unwrap())
                        .to_hex()
                        .to_string(),
                    rows,
                    now_us: None,
                }],
            },
        );
        assert!(!serde_json::to_string(&record).unwrap().contains("now_us"));
        wal::append(temp.path(), &record).unwrap();
        let proof = wal::group_fingerprint(&record).unwrap();
        let db = Database::open(temp.path(), Config::default()).unwrap();
        {
            let s = db.lock().unwrap();
            assert_eq!(
                s.catalog.tables["metrics"].receipts["legacy-group"].group_fingerprint,
                proof
            );
            assert_eq!(s.metadata_bytes, encode_manifest(&s.catalog).unwrap().len());
        }
        db.checkpoint().unwrap();
        drop(db);
        let db = Database::open(temp.path(), Config::default()).unwrap();
        let s = db.lock().unwrap();
        assert_eq!(
            s.catalog.tables["metrics"].receipts["legacy-group"].group_fingerprint,
            proof
        );
        assert_eq!(s.metadata_bytes, encode_manifest(&s.catalog).unwrap().len());
    }

    #[test]
    fn legacy_single_receipt_encoding_is_unchanged() {
        let json = r#"{"sequence":1,"rows":1,"digest":"legacy","issued_us":null}"#;
        let receipt: ReceiptEntry = serde_json::from_str(json).unwrap();
        assert!(receipt.group_fingerprint.is_none());
        assert_eq!(serde_json::to_string(&receipt).unwrap(), json);
    }

    #[test]
    fn readiness_is_nonblocking_when_state_is_busy() {
        let temporary = TempDir::new().unwrap();
        let database = Database::open(temporary.path(), Config::default()).unwrap();
        assert!(database.is_ready());
        let _state = database.inner.state.lock().unwrap();
        assert!(!database.is_ready());
    }

    fn sample_rollup(count: u64, sequence: u64, tenant: &str) -> RollupRow {
        RollupRow {
            width_us: 10,
            bucket_us: -10,
            tenant: tenant.into(),
            series: "cpu\"\\\n".into(),
            tags: BTreeMap::from([("城市".into(), "東京\t".into())]),
            count,
            sum: count as f64,
            min: 1.0,
            max: count as f64,
            first: 1.0,
            last: count as f64,
            first_timestamp_us: -9,
            last_timestamp_us: count as i64,
            first_sequence: 1,
            last_sequence: sequence,
            first_ordinal: 0,
            last_ordinal: 0,
        }
    }

    #[test]
    fn checkpoint_sequence_width_matches_json_at_every_decimal_boundary() {
        let mut values = vec![0, u64::MAX];
        let mut power = 1_u64;
        loop {
            values.extend([power - 1, power]);
            let Some(next) = power.checked_mul(10) else {
                break;
            };
            power = next;
        }
        for previous in &values {
            for next in &values {
                let expected = serde_json::to_vec(next).unwrap().len() as i128
                    - serde_json::to_vec(previous).unwrap().len() as i128;
                assert_eq!(checkpoint_sequence_delta(*previous, *next), expected);
            }
        }
    }

    #[test]
    fn append_metadata_delta_matches_exact_serializer() {
        let temporary = TempDir::new().unwrap();
        let database = Database::open(temporary.path(), Config::default()).unwrap();
        database
            .create_table(
                "metrics",
                TableConfig {
                    shards: 1,
                    rollup_widths_us: vec![10],
                    ..Default::default()
                },
            )
            .unwrap();
        database.checkpoint().unwrap();
        let mut state = database.inner.state.lock().unwrap();
        let empty_updates = BTreeMap::from([
            ("escaped-\n-城市-a".to_owned(), sample_rollup(9, 9, "a")),
            ("escaped-\t-城市-b".to_owned(), sample_rollup(10, 10, "b")),
        ]);
        let empty_receipt = ReceiptEntry {
            sequence: 10,
            rows: 10,
            digest: "0".repeat(64),
            issued_us: Some(1_000),
            group_fingerprint: None,
        };
        let projected = append_metadata_bytes(
            &state,
            "metrics",
            "v1:1000:multi",
            &empty_receipt,
            &empty_updates,
            10,
        )
        .unwrap();
        let mut expected = state.catalog.clone();
        let expected_table = expected.tables.get_mut("metrics").unwrap();
        expected_table
            .receipts
            .insert("v1:1000:multi".into(), empty_receipt);
        expected_table.rollups.extend(empty_updates);
        expected.checkpoint_sequence = 10;
        assert_eq!(projected, encode_manifest(&expected).unwrap().len());

        let existing_key = "[10,-10,\"租户\",\"cpu\\\"\",{\"k\":\"v\"}]".to_owned();
        state
            .catalog
            .tables
            .get_mut("metrics")
            .unwrap()
            .rollups
            .insert(existing_key.clone(), sample_rollup(9, 9, "租户"));
        state.metadata_bytes = encode_manifest(&state.catalog).unwrap().len();

        for index in 0..128u64 {
            let request_id = format!("v1:{}:nonce-\\\"-城市-{index}", 1_000 + index);
            let receipt = ReceiptEntry {
                sequence: 9 + index,
                rows: if index < 2 { 9 + index as usize } else { 1 },
                digest: format!("{index:064x}"),
                issued_us: Some(1_000 + index as i64),
                group_fingerprint: (index % 2 == 0).then(|| format!("{index:064x}")),
            };
            let key = if index % 3 == 0 {
                existing_key.clone()
            } else {
                format!("[10,{index},\"租户-{index}\",\"cpu\",{{\"line\":\"\\n\"}}]")
            };
            let updates = BTreeMap::from([(
                key,
                sample_rollup(10 + index, 10 + index, &format!("租户-{index}")),
            )]);
            let projected = append_metadata_bytes(
                &state,
                "metrics",
                &request_id,
                &receipt,
                &updates,
                receipt.sequence,
            )
            .unwrap();
            let mut expected = state.catalog.clone();
            let table = expected.tables.get_mut("metrics").unwrap();
            table.receipts.insert(request_id.clone(), receipt.clone());
            table.rollups.extend(updates.clone());
            expected.checkpoint_sequence = receipt.sequence;
            assert_eq!(projected, encode_manifest(&expected).unwrap().len());

            let actual = append_metadata_bytes(
                &state,
                "metrics",
                &request_id,
                &receipt,
                &updates,
                state.catalog.checkpoint_sequence,
            )
            .unwrap();
            let table = state.catalog.tables.get_mut("metrics").unwrap();
            table.receipts.insert(request_id, receipt);
            table.rollups.extend(updates);
            state.metadata_bytes = actual;
            assert_eq!(
                state.metadata_bytes,
                encode_manifest(&state.catalog).unwrap().len()
            );
        }
    }

    #[test]
    fn cached_metadata_tracks_control_checkpoint_and_retention_publications() {
        let temporary = TempDir::new().unwrap();
        let database = Database::open(temporary.path(), Config::default()).unwrap();
        database
            .create_table(
                "metrics",
                TableConfig {
                    shards: 1,
                    window_us: 10,
                    retention_us: Some(10),
                    rollup_widths_us: vec![5],
                    ..Default::default()
                },
            )
            .unwrap();
        database
            .write(
                "metrics",
                "legacy",
                vec![Row {
                    timestamp_us: 1,
                    tenant: "城市".into(),
                    series: "cpu".into(),
                    value: 1.0,
                    tags: BTreeMap::new(),
                }],
                1,
            )
            .unwrap();
        for check in [0, 1] {
            if check == 1 {
                database.checkpoint().unwrap();
            }
            let state = database.inner.state.lock().unwrap();
            assert_eq!(
                state.metadata_bytes,
                encode_manifest(&state.catalog).unwrap().len()
            );
        }
        database
            .set_policy(
                "metrics",
                LifecyclePolicy {
                    retention_us: Some(5),
                    rollup_retention_us: Some(20),
                    ..Default::default()
                },
            )
            .unwrap();
        database.checkpoint().unwrap();
        database.maintain(20).unwrap();
        let state = database.inner.state.lock().unwrap();
        assert_eq!(
            state.metadata_bytes,
            encode_manifest(&state.catalog).unwrap().len()
        );
    }

    #[test]
    fn control_epoch_exhaustion_rejects_before_wal_publication() {
        let temporary = TempDir::new().unwrap();
        let database = Database::open(temporary.path(), Config::default()).unwrap();
        let mut state = database.lock().unwrap();
        state.control_epoch = u64::MAX;
        let sequence = next_sequence(&state).unwrap();
        let record = wal::Record::new(
            sequence,
            wal::Operation::CreateTable {
                name: "metrics".into(),
                config: TableConfig::default(),
            },
        );
        let error = commit_record(&database.inner, &mut state, &record).unwrap_err();
        assert!(format!("{error:#}").contains("control epoch exhausted"));
        assert!(!wal::path(temporary.path(), sequence).exists());
    }
}
