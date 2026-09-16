use crate::job_runtime::{self, JobRuntime};
use crate::model::*;
use crate::query::{self, QueryOptions, QueryTable};
use crate::remote::RemoteStore;
use crate::tier::RemoteHead;
use crate::{segment, wal};
use anyhow::{Context, Result, bail, ensure};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WriteReceipt {
    pub sequence: u64,
    pub rows: usize,
    pub duplicate: bool,
    pub durability: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptEntry {
    pub sequence: u64,
    pub rows: usize,
    pub digest: String,
    #[serde(default)]
    pub issued_us: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
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
    pub tables: BTreeMap<String, Table>,
    #[serde(default)]
    pub continuous_aggregates: BTreeMap<String, ContinuousAggregate>,
    #[serde(default)]
    pub jobs: BTreeMap<String, JobDefinition>,
    #[serde(default)]
    pub control_history: Vec<ControlStamp>,
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
    pub unshipped_batches: u64,
    pub hot_rows: usize,
    pub hot_bytes: usize,
    pub wal_bytes: u64,
    pub disk_bytes: u64,
    pub metadata_bytes: usize,
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
    pub rows: Arc<Vec<StoredRow>>,
    pub bytes: usize,
    pub touched: u64,
}
pub(crate) struct State {
    pub catalog: Manifest,
    pub metadata_bytes: usize,
    pub sequence: u64,
    pub hot: BTreeMap<String, Vec<StoredRow>>,
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
pub(crate) struct Inner {
    pub root: PathBuf,
    pub config: Config,
    pub state: Mutex<State>,
    pub disk_admission: Mutex<()>,
    pub remote_operation: Mutex<()>,
    pub remote: Option<Arc<dyn RemoteStore>>,
    pub readers: Arc<AtomicUsize>,
    pub segment_pins: Arc<Mutex<BTreeMap<String, usize>>>,
    pub query_active: AtomicUsize,
    pub jobs_running: Mutex<BTreeSet<String>>,
    _file_lock: File,
}
#[derive(Clone)]
pub struct Database {
    pub(crate) inner: Arc<Inner>,
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
        config: Config,
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
        for dir in ["wal", "segments", "cache", "staging"] {
            fs::create_dir_all(root.join(dir))?;
        }
        wal::sync_dir(&root)?;
        cleanup_temporaries(&root)?;
        let manifest_path = root.join("manifest.bin");
        let catalog = if manifest_path.exists() {
            decode_manifest(&wal::read_bounded(
                &manifest_path,
                config.metadata_max_bytes,
            )?)?
        } else {
            ensure!(
                fs::read_dir(root.join("wal"))?.next().is_none()
                    && fs::read_dir(root.join("segments"))?.next().is_none(),
                "missing manifest in nonempty database; refusing to initialize over data"
            );
            let catalog = Manifest {
                format_version: FORMAT_VERSION,
                database_id: uuid::Uuid::new_v4().to_string(),
                checkpoint_sequence: 0,
                tables: BTreeMap::new(),
                continuous_aggregates: BTreeMap::new(),
                jobs: BTreeMap::from([(
                    "varve_maintenance".to_owned(),
                    crate::control::builtin_job(&config)?,
                )]),
                control_history: Vec::new(),
            };
            wal::atomic_write(&manifest_path, &encode_manifest(&catalog)?)?;
            catalog
        };
        validate_manifest(&catalog)?;
        let metadata_bytes = encode_manifest(&catalog)?.len();
        let wal_bytes = directory_bytes(&root.join("wal"))?;
        ensure!(
            wal_bytes <= config.max_disk_bytes,
            "WAL recovery exceeds disk budget"
        );
        let mut state = State {
            sequence: catalog.checkpoint_sequence,
            catalog,
            metadata_bytes,
            hot: BTreeMap::new(),
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
        for record in wal::records(&root, state.sequence, config.wal_max_bytes)? {
            replay(&mut state, record?, &config)?;
            check_recovery_budget(&config, &state)?;
        }
        check_recovery_budget(&config, &state)?;
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
        let db = Self {
            inner: Arc::new(Inner {
                root,
                config,
                state: Mutex::new(state),
                disk_admission: Mutex::new(()),
                remote_operation: Mutex::new(()),
                remote,
                readers: Arc::new(AtomicUsize::new(0)),
                segment_pins: Arc::new(Mutex::new(BTreeMap::new())),
                query_active: AtomicUsize::new(0),
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
            gc_locked(&db.inner, &mut s)?;
        }
        db.ensure_builtin_job()?;
        Ok(db)
    }
    /// Returns readiness without waiting for the database mutex or touching catalog/disk state.
    pub fn is_ready(&self) -> bool {
        self.inner
            .state
            .try_lock()
            .is_ok_and(|state| state.fenced.is_none())
    }

    pub(crate) fn lock(&self) -> Result<MutexGuard<'_, State>> {
        self.inner
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("database state mutex poisoned; reopen for recovery"))
    }

    pub(crate) fn lock_remote_operation(&self) -> Result<MutexGuard<'_, ()>> {
        self.inner
            .remote_operation
            .lock()
            .map_err(|_| anyhow::anyhow!("remote operation mutex poisoned"))
    }

    pub fn create_table(&self, name: &str, config: TableConfig) -> Result<u64> {
        validate_name(name)?;
        config.validate()?;
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
        let mut projected = s.catalog.clone();
        projected
            .tables
            .insert(name.to_owned(), empty_table(config.clone(), sequence));
        projected.checkpoint_sequence = sequence;
        check_metadata_budget(&self.inner.config, &projected, hot_count(&s))?;
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
        validate_request_id(request_id)?;
        ensure!(
            !rows.is_empty() && rows.len() <= self.inner.config.max_batch_rows,
            "batch row admission limit"
        );
        let encoded = serde_json::to_vec(&rows)?;
        ensure!(
            encoded.len() <= self.inner.config.max_batch_bytes,
            "batch byte admission limit"
        );
        let digest = blake3::hash(&encoded).to_hex().to_string();
        let mut s = self.lock()?;
        healthy(&s)?;
        let table_state = s.catalog.tables.get(table).context("unknown table")?;
        let window = table_state.config.idempotency_window_us;
        let has_receipt = table_state.receipts.contains_key(request_id);
        let (issued_us, effective_floor) = if let Some(window_us) = window {
            let issued_us = parse_timed_request_id(request_id)?;
            let candidate = checked_cutoff(now_us, window_us);
            let floor = s
                .idempotency_floors
                .get(table)
                .copied()
                .unwrap_or(i64::MIN)
                .max(candidate);
            ensure!(
                issued_us >= floor,
                "request_id is outside the idempotency window"
            );
            ensure!(
                has_receipt || issued_us <= now_us.saturating_add(IDEMPOTENCY_MAX_FUTURE_SKEW_US),
                "request_id issue time exceeds the future-skew limit"
            );
            (Some(issued_us), Some(floor))
        } else {
            (None, None)
        };
        if let Some(receipt) = s
            .catalog
            .tables
            .get(table)
            .and_then(|table| table.receipts.get(request_id))
        {
            ensure!(
                receipt.digest == digest,
                "request_id conflicts with different data"
            );
            let duplicate = receipt_for(receipt, true);
            if let Some(floor) = effective_floor {
                s.idempotency_floors.insert(table.to_owned(), floor);
            }
            return Ok(duplicate);
        }
        if let Some(floor) = effective_floor {
            s.idempotency_floors.insert(table.to_owned(), floor);
        }
        let receipt_count = s
            .catalog
            .tables
            .values()
            .map(|table| table.receipts.len())
            .sum::<usize>();
        if receipt_count >= self.inner.config.max_idempotency_keys && window.is_some() {
            checkpoint_locked(&self.inner, &mut s)?;
        }
        ensure!(
            s.catalog
                .tables
                .values()
                .map(|table| table.receipts.len())
                .sum::<usize>()
                < self.inner.config.max_idempotency_keys,
            "idempotency registry full; refusing to forget committed request IDs"
        );
        let t = s.catalog.tables.get(table).context("unknown table")?;
        for row in &rows {
            row.validate()?;
            window_start(row.timestamp_us, t.config.window_us)?;
            if let Some(cutoff) = t.cutoff_us {
                ensure!(
                    row.timestamp_us >= cutoff,
                    "row precedes retained raw-data cutoff"
                );
            }
            if let Some(age) = t.config.late_after_us {
                ensure!(
                    row.timestamp_us >= checked_cutoff(now_us, age),
                    "row exceeds allowed lateness"
                );
            }
        }
        let estimated = rows.iter().map(Row::estimated_bytes).sum::<usize>();
        ensure!(
            estimated <= self.inner.config.hot_max_bytes
                && rows.len() <= self.inner.config.hot_max_rows,
            "batch exceeds hot-tier capacity"
        );
        let sequence = next_sequence(&s)?;
        let stored: Vec<_> = rows
            .iter()
            .cloned()
            .enumerate()
            .map(|(ordinal, row)| StoredRow {
                row,
                sequence,
                ordinal: ordinal as u32,
            })
            .collect();
        let updates = aggregate_updates(t, &stored, self.inner.config.metadata_max_bytes)?;
        let total_groups: usize = s
            .catalog
            .tables
            .values()
            .map(|table| table.rollups.len())
            .sum();
        let new_groups = updates
            .keys()
            .filter(|key| !t.rollups.contains_key(*key))
            .count();
        ensure!(
            total_groups + new_groups <= self.inner.config.max_rollup_groups,
            "rollup state admission limit"
        );
        if s.hot_bytes + estimated > self.inner.config.hot_max_bytes
            || hot_count(&s) + rows.len() > self.inner.config.hot_max_rows
        {
            checkpoint_locked(&self.inner, &mut s)?;
        }
        let receipt = ReceiptEntry {
            sequence,
            rows: rows.len(),
            digest: digest.clone(),
            issued_us,
        };
        let mut projected =
            append_metadata_bytes(&s, table, request_id, &receipt, &updates, sequence)?;
        if projected.saturating_add((hot_count(&s) + rows.len()).saturating_mul(512))
            > self.inner.config.metadata_max_bytes
            && (hot_count(&s) > 0 || idempotency_checkpoint_due(&s))
        {
            checkpoint_locked(&self.inner, &mut s)?;
            projected = append_metadata_bytes(&s, table, request_id, &receipt, &updates, sequence)?;
        }
        ensure!(
            projected.saturating_add((hot_count(&s) + rows.len()).saturating_mul(512))
                <= self.inner.config.metadata_max_bytes,
            "projected checkpoint exceeds metadata byte budget"
        );
        let record = wal::Record::new(
            sequence,
            wal::Operation::Append {
                table: table.into(),
                request_id: request_id.into(),
                digest: digest.clone(),
                rows,
            },
        );
        commit_record(&self.inner, &mut s, &record)?;
        let actual_metadata_bytes = append_metadata_bytes(
            &s,
            table,
            request_id,
            &receipt,
            &updates,
            s.catalog.checkpoint_sequence,
        )?;
        let t = s
            .catalog
            .tables
            .get_mut(table)
            .expect("table validated before WAL");
        t.rollups.extend(updates);
        t.receipts.insert(request_id.into(), receipt.clone());
        s.hot.entry(table.into()).or_default().extend(stored);
        s.hot_bytes += estimated;
        s.sequence = sequence;
        s.metadata_bytes = actual_metadata_bytes;
        s.first_hot_us.get_or_insert(now_us);
        Ok(receipt_for(&receipt, false))
    }

    pub fn checkpoint(&self) -> Result<()> {
        let mut s = self.lock()?;
        healthy(&s)?;
        checkpoint_locked(&self.inner, &mut s)
    }

    pub fn rollups(&self, table: &str) -> Result<Vec<RollupRow>> {
        let s = self.lock()?;
        healthy(&s)?;
        Ok(s.catalog
            .tables
            .get(table)
            .context("unknown table")?
            .rollups
            .values()
            .cloned()
            .collect())
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
        let (table_state, mut output, pin) = {
            let s = self.lock()?;
            healthy(&s)?;
            let mut table_state = s
                .catalog
                .tables
                .get(table)
                .context("unknown table")?
                .clone();
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
            let output: Vec<StoredRow> = s
                .hot
                .get(table)
                .into_iter()
                .flatten()
                .filter(|row| matches(row))
                .cloned()
                .collect();
            table_state.segments.retain(|segment| {
                !start.is_some_and(|value| segment.max_timestamp_us < value)
                    && !end_us.is_some_and(|value| segment.min_timestamp_us >= value)
                    && (tenant.zip(series).is_none_or(|(tenant, series)| {
                        segment.shard == shard_for(tenant, series, table_state.config.shards)
                    }))
            });
            let ids = table_state
                .segments
                .iter()
                .map(|segment| segment.id.clone())
                .collect();
            let pin = Pin::new(&self.inner, ids)?;
            (table_state, output, pin)
        };
        let _pin = pin;
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
        let mut bytes: usize = output.iter().map(|row| row.row.estimated_bytes()).sum();
        ensure!(
            bytes <= self.inner.config.query_max_output_bytes,
            "scan output budget exceeded"
        );
        for descriptor in &table_state.segments {
            ensure!(
                descriptor.decoded_bytes <= self.inner.config.hot_max_bytes as u64,
                "segment decoded working set exceeds hot memory budget"
            );
            let path = resolve_segment(&self.inner, descriptor)?;
            let rows = Arc::new(segment::read(&path)?);
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
            if decoded_bytes <= self.inner.config.decoded_cache_bytes {
                let mut s = self.lock()?;
                healthy(&s)?;
                s.cache_clock = s.cache_clock.wrapping_add(1);
                let touched = s.cache_clock;
                while s.decoded.values().map(|entry| entry.bytes).sum::<usize>() + decoded_bytes
                    > self.inner.config.decoded_cache_bytes
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
                s.decoded.insert(
                    descriptor.id.clone(),
                    CacheEntry {
                        rows: rows.clone(),
                        bytes: decoded_bytes,
                        touched,
                    },
                );
            }
            for row in rows.iter().filter(|row| matches(row)) {
                bytes = bytes
                    .checked_add(row.row.estimated_bytes())
                    .context("scan byte accounting overflow")?;
                ensure!(
                    bytes <= self.inner.config.query_max_output_bytes,
                    "scan output budget exceeded"
                );
                output.push(row.clone());
            }
        }
        output.sort_by(|a, b| {
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
        let (snapshots, pin, catalog) = {
            let s = self.lock()?;
            healthy(&s)?;
            let names = s.catalog.tables.keys().cloned().collect::<Vec<_>>();
            let planning_catalog = query_catalog(&s, names.iter().map(String::as_str).collect())?;
            let scan_plan = crate::plan::plan_with_catalog(sql, &names, &planning_catalog);
            let mut selected: BTreeMap<_, _> = s
                .catalog
                .tables
                .iter()
                .filter(|(name, _)| scan_plan.as_ref().is_none_or(|plan| plan.table == **name))
                .map(|(name, table)| (name.clone(), table.clone()))
                .collect();
            if let Some(plan) = &scan_plan {
                for table in selected.values_mut() {
                    table.segments.retain(|segment| {
                        !plan.rollup
                            && !plan.empty
                            && plan
                                .start_us
                                .is_none_or(|start| segment.max_timestamp_us >= start)
                            && plan.end_us.is_none_or(|end| segment.min_timestamp_us < end)
                    });
                }
            }
            let pinned_ids = selected
                .values()
                .flat_map(|table| table.segments.iter().map(|segment| segment.id.clone()))
                .collect();
            let pin = Pin::new(&self.inner, pinned_ids)?;
            let mut snapshots = Vec::new();
            for (name, table) in selected {
                let descriptors = table.segments.clone();
                snapshots.push((
                    QueryTable {
                        hot: s
                            .hot
                            .get(&name)
                            .into_iter()
                            .flatten()
                            .filter(|row| {
                                scan_plan.as_ref().is_none_or(|plan| {
                                    !plan.rollup
                                        && !plan.empty
                                        && plan
                                            .start_us
                                            .is_none_or(|start| row.row.timestamp_us >= start)
                                        && plan.end_us.is_none_or(|end| row.row.timestamp_us < end)
                                })
                            })
                            .cloned()
                            .collect(),
                        name,
                        files: Vec::new(),
                        rollups: if scan_plan.as_ref().is_some_and(|plan| !plan.rollup) {
                            Vec::new()
                        } else {
                            table.rollups.values().cloned().collect()
                        },
                        cutoff_us: table.cutoff_us,
                    },
                    descriptors,
                ));
            }
            let selected_names = snapshots
                .iter()
                .map(|(table, _)| table.name.as_str())
                .collect();
            let catalog = query_catalog(&s, selected_names)?;
            (snapshots, pin, catalog)
        };
        let mut tables = Vec::with_capacity(snapshots.len());
        for (mut table, descriptors) in snapshots {
            for segment in &descriptors {
                table.files.push(resolve_segment(&self.inner, segment)?);
            }
            tables.push(table);
        }
        let _pin = pin;
        let c = &self.inner.config;
        let options = QueryOptions {
            executable: c.query_executable.clone(),
            memory_mb: c.query_memory_mb,
            threads: c.query_threads,
            timeout_ms: c.query_timeout_ms,
            max_output_bytes: c.query_max_output_bytes,
        };
        query::execute_with_catalog(&tables, sql, &options, &catalog)
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
                unshipped_batches: s.sequence.saturating_sub(remote_sequence),
                hot_rows: hot_count(&s),
                hot_bytes: s.hot_bytes,
                wal_bytes: s.wal_bytes,
                disk_bytes: 0,
                metadata_bytes: s.metadata_bytes,
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
                fenced: s.fenced.clone(),
                last_maintenance_error: s.last_maintenance_error.clone(),
            }
        };
        status.disk_bytes = directory_bytes(&self.inner.root)?;
        status.disk_cache_bytes = directory_bytes(&self.inner.root.join("cache"))?;
        Ok(status)
    }
}

fn query_catalog(s: &State, selected_tables: Vec<&str>) -> Result<query::QueryCatalog> {
    use query::{AggregateAlias, CatalogRelation, QueryCatalog};
    let tables = s
        .catalog
        .tables
        .iter()
        .map(|(name, table)| {
            json!([
                name,
                u64::from(table.config.shards),
                table.config.window_us,
                table.created_sequence
            ])
        })
        .collect();
    let policies = s
        .catalog
        .tables
        .iter()
        .map(|(name, table)| {
            json!([
                name,
                table.config.late_after_us,
                table.config.retention_us,
                table.config.archive_after_us,
                table.config.rollup_retention_us,
                table.config.idempotency_window_us,
                table.idempotency_floor_us
            ])
        })
        .collect();
    let aggregates = s
        .catalog
        .continuous_aggregates
        .values()
        .map(|aggregate| {
            json!([
                aggregate.name,
                aggregate.source,
                aggregate.width_us,
                aggregate.created_sequence
            ])
        })
        .collect();
    let jobs = s
        .catalog
        .jobs
        .values()
        .map(|definition| {
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
        })
        .collect();
    let remote_sequence = s
        .remote_head
        .as_ref()
        .map(|head| head.sequence)
        .unwrap_or(0);
    let status = vec![json!([
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
    ])];
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
    delta += serde_json::to_vec(&checkpoint_sequence)?.len() as i128
        - serde_json::to_vec(&s.catalog.checkpoint_sequence)?.len() as i128;
    apply_encoded_delta(s.metadata_bytes, delta)
}
fn check_recovery_budget(config: &Config, s: &State) -> Result<()> {
    ensure!(
        s.metadata_bytes
            .saturating_add(hot_count(s).saturating_mul(512))
            <= config.metadata_max_bytes,
        "metadata/recovery byte budget exceeded; increase metadata_max_bytes"
    );
    ensure!(
        s.hot_bytes <= config.hot_max_bytes && hot_count(s) <= config.hot_max_rows,
        "recovery hot set exceeds configured budget"
    );
    ensure!(
        s.catalog
            .tables
            .values()
            .map(|table| table.receipts.len())
            .sum::<usize>()
            <= config.max_idempotency_keys
            && s.catalog
                .tables
                .values()
                .map(|table| table.rollups.len())
                .sum::<usize>()
                <= config.max_rollup_groups,
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
    s.hot.values().map(Vec::len).sum()
}

pub(crate) fn lock_disk_admission(inner: &Inner) -> Result<MutexGuard<'_, ()>> {
    inner
        .disk_admission
        .lock()
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

fn idempotency_checkpoint_due(s: &State) -> bool {
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

fn receipt_for(r: &ReceiptEntry, duplicate: bool) -> WriteReceipt {
    WriteReceipt {
        sequence: r.sequence,
        rows: r.rows,
        duplicate,
        durability: "local_fsync".into(),
    }
}

pub(crate) fn commit_record(inner: &Inner, s: &mut State, record: &wal::Record) -> Result<()> {
    let bytes = wal::encode(record)?.len() as u64;
    ensure!(
        bytes <= inner.config.wal_max_bytes,
        "batch exceeds WAL capacity"
    );
    if s.wal_bytes.saturating_add(bytes) > inner.config.wal_max_bytes {
        checkpoint_locked(inner, s)?;
    }
    let _disk = lock_disk_admission(inner)?;
    ensure_budget(inner, bytes)?;
    match wal::append(&inner.root, record) {
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
    ensure!(record.sequence == next_sequence(s)?, "noncontiguous replay");
    let mut append_applied = false;
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
            validate_request_id(&request_id)?;
            ensure!(
                !rows.is_empty() && rows.len() <= config.max_batch_rows,
                "invalid/recovery oversized WAL batch"
            );
            ensure!(
                blake3::hash(&serde_json::to_vec(&rows)?).to_hex().as_str() == digest,
                "WAL batch digest mismatch"
            );
            let t = s
                .catalog
                .tables
                .get(&table)
                .context("WAL references unknown table")?;
            ensure!(
                !t.receipts.contains_key(&request_id),
                "duplicate request in committed WAL"
            );
            let issued_us = if t.config.idempotency_window_us.is_some() {
                let issued = parse_timed_request_id(&request_id)?;
                ensure!(
                    t.idempotency_floor_us.is_none_or(|floor| issued >= floor),
                    "WAL request_id precedes the durable idempotency floor"
                );
                Some(issued)
            } else {
                None
            };
            for row in &rows {
                row.validate()?;
                window_start(row.timestamp_us, t.config.window_us)?;
                ensure!(
                    t.cutoff_us.is_none_or(|cutoff| row.timestamp_us >= cutoff),
                    "WAL violates retention cutoff"
                );
            }
            let bytes: usize = rows.iter().map(Row::estimated_bytes).sum();
            let count = rows.len();
            let stored: Vec<_> = rows
                .into_iter()
                .enumerate()
                .map(|(ordinal, row)| StoredRow {
                    row,
                    sequence: record.sequence,
                    ordinal: ordinal as u32,
                })
                .collect();
            let updates = aggregate_updates(t, &stored, config.metadata_max_bytes)?;
            let receipt = ReceiptEntry {
                sequence: record.sequence,
                rows: count,
                digest,
                issued_us,
            };
            let metadata_bytes = append_metadata_bytes(
                s,
                &table,
                &request_id,
                &receipt,
                &updates,
                s.catalog.checkpoint_sequence,
            )?;
            let t = s
                .catalog
                .tables
                .get_mut(&table)
                .expect("table validated during replay");
            t.rollups.extend(updates);
            t.receipts.insert(request_id, receipt);
            s.hot.entry(table).or_default().extend(stored);
            s.hot_bytes += bytes;
            s.metadata_bytes = metadata_bytes;
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
    if !append_applied {
        s.metadata_bytes = encode_manifest(&s.catalog)?.len();
    }
    Ok(())
}

pub(crate) fn aggregate_updates(
    t: &Table,
    rows: &[StoredRow],
    byte_budget: usize,
) -> Result<BTreeMap<String, RollupRow>> {
    let mut updates = BTreeMap::new();
    let mut update_bytes = 0usize;
    for stored in rows {
        for width in &t.config.rollup_widths_us {
            let bucket = window_start(stored.row.timestamp_us, *width)?;
            if t.rollup_cutoff_us
                .is_some_and(|cutoff| bucket.saturating_add(*width) <= cutoff)
            {
                continue;
            }
            let key = serde_json::to_string(&(
                *width,
                bucket,
                &stored.row.tenant,
                &stored.row.series,
                &stored.row.tags,
            ))?;
            if !updates.contains_key(&key) {
                update_bytes = update_bytes
                    .saturating_add(key.len())
                    .saturating_add(stored.row.estimated_bytes().saturating_mul(2))
                    .saturating_add(256);
                ensure!(
                    update_bytes <= byte_budget,
                    "rollup update metadata byte budget exceeded"
                );
            }
            if let Some(agg) = updates.get_mut(&key) {
                RollupRow::add(agg, stored)?;
            } else if let Some(existing) = t.rollups.get(&key) {
                let mut agg = existing.clone();
                agg.add(stored)?;
                updates.insert(key, agg);
            } else {
                updates.insert(key, RollupRow::from_row(*width, stored)?);
            }
        }
    }
    Ok(updates)
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

pub(crate) fn persist_manifest(inner: &Inner, s: &mut State, next: Manifest) -> Result<()> {
    validate_manifest(&next)?;
    check_metadata_budget(&inner.config, &next, 0)?;
    let bytes = encode_manifest(&next)?;
    let _disk = lock_disk_admission(inner)?;
    ensure_budget(inner, bytes.len() as u64)?;
    if let Err(e) = wal::atomic_write(&inner.root.join("manifest.bin"), &bytes) {
        s.fenced = Some(format!("ambiguous manifest publication: {e:#}"));
        return Err(e);
    }
    wal::failpoint("manifest_published");
    s.metadata_bytes = bytes.len();
    s.catalog = next;
    Ok(())
}

pub(crate) fn checkpoint_locked(inner: &Inner, s: &mut State) -> Result<()> {
    if s.sequence == s.catalog.checkpoint_sequence
        && s.hot.values().all(Vec::is_empty)
        && !idempotency_checkpoint_due(s)
    {
        return Ok(());
    }
    let publication = (|| {
        let mut next = s.catalog.clone();
        apply_idempotency_checkpoint(s, &mut next);
        for (name, rows) in &s.hot {
            let table = next
                .tables
                .get_mut(name)
                .context("hot rows without table")?;
            table
                .segments
                .extend(write_partitioned(inner, &table.config, rows)?);
        }
        next.checkpoint_sequence = s.sequence;
        wal::failpoint("segments_published");
        persist_manifest(inner, s, next)
    })();
    if let Err(error) = publication {
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
    s.hot.clear();
    s.hot_bytes = 0;
    s.first_hot_us = None;
    gc_locked(inner, s)?;
    Ok(())
}

fn cleanup_unpublished_segments(inner: &Inner, s: &State) -> Result<()> {
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

pub(crate) fn write_partitioned(
    inner: &Inner,
    config: &TableConfig,
    rows: &[StoredRow],
) -> Result<Vec<Segment>> {
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
            let _disk = lock_disk_admission(inner)?;
            let temp = tempfile::Builder::new()
                .prefix("segment-")
                .suffix(".tmp")
                .tempfile_in(inner.root.join("staging"))?;
            let used = directory_bytes(&inner.root)?;
            let remaining = inner.config.max_disk_bytes.saturating_sub(used);
            let limit = remaining.min(wal::MAX_FRAME_BYTES as u64);
            ensure!(
                limit > 0,
                "local disk admission budget exhausted; compact, archive, or increase max_disk_bytes"
            );
            segment::write_with_limit(temp.path(), chunk, limit)?;
            wal::failpoint("segment_written");
            File::open(temp.path())?.sync_all()?;
            let bytes = wal::read_bounded(temp.path(), wal::MAX_FRAME_BYTES)?;
            let id = blake3::hash(&bytes).to_hex().to_string();
            let path = inner.root.join("segments").join(format!("{id}.parquet"));
            if path.exists() {
                ensure!(
                    wal::read_bounded(&path, wal::MAX_FRAME_BYTES)? == bytes,
                    "immutable local segment collision"
                );
            } else {
                fs::rename(temp.path(), &path)?;
                wal::sync_dir(&inner.root.join("segments"))?;
            }
            result.push(Segment {
                id,
                shard,
                window_us: window,
                rows: chunk.len() as u64,
                bytes: bytes.len() as u64,
                decoded_bytes: chunk.iter().map(|r| r.row.estimated_bytes() as u64).sum(),
                min_timestamp_us: chunk.iter().map(|r| r.row.timestamp_us).min().unwrap(),
                max_timestamp_us: chunk.iter().map(|r| r.row.timestamp_us).max().unwrap(),
            });
        }
    }
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

pub(crate) fn read_segment_locked(
    inner: &Inner,
    s: &mut State,
    seg: &Segment,
) -> Result<Arc<Vec<StoredRow>>> {
    ensure!(
        seg.decoded_bytes <= inner.config.hot_max_bytes as u64,
        "segment decoded working set exceeds hot memory budget; reopen with larger hot_max_bytes"
    );
    s.cache_clock = s.cache_clock.wrapping_add(1);
    if let Some(entry) = s.decoded.get_mut(&seg.id) {
        entry.touched = s.cache_clock;
        return Ok(entry.rows.clone());
    }
    let path = resolve_segment(inner, seg)?;
    let rows = Arc::new(segment::read(&path)?);
    ensure!(rows.len() as u64 == seg.rows, "segment row-count mismatch");
    let bytes = rows.iter().map(|r| r.row.estimated_bytes()).sum::<usize>();
    ensure!(
        bytes as u64 == seg.decoded_bytes,
        "segment decoded-size metadata mismatch"
    );
    if bytes <= inner.config.decoded_cache_bytes {
        while s.decoded.values().map(|e| e.bytes).sum::<usize>() + bytes
            > inner.config.decoded_cache_bytes
        {
            let key = s
                .decoded
                .iter()
                .min_by_key(|(_, e)| e.touched)
                .map(|(k, _)| k.clone());
            if let Some(key) = key {
                s.decoded.remove(&key);
            } else {
                break;
            }
        }
        s.decoded.insert(
            seg.id.clone(),
            CacheEntry {
                rows: rows.clone(),
                bytes,
                touched: s.cache_clock,
            },
        );
    }
    Ok(rows)
}

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
    ensure!(
        directory_bytes(&inner.root)?.saturating_add(additional) <= inner.config.max_disk_bytes,
        "local disk admission budget exhausted; compact, archive, or increase max_disk_bytes"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

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
}
