use anyhow::{Result, bail, ensure};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

pub const FORMAT_VERSION: u32 = 1;
pub const IDEMPOTENCY_MAX_FUTURE_SKEW_US: i64 = 300_000_000;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Row {
    pub timestamp_us: i64,
    pub tenant: String,
    pub series: String,
    pub value: f64,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
}

impl Row {
    pub fn validate(&self) -> Result<()> {
        ensure!(self.value.is_finite(), "value must be finite");
        ensure!(
            !self.tenant.is_empty() && self.tenant.len() <= 256,
            "tenant must be 1..256 bytes"
        );
        ensure!(
            !self.series.is_empty() && self.series.len() <= 1024,
            "series must be 1..1024 bytes"
        );
        ensure!(
            !self.tenant.contains('\0') && !self.series.contains('\0'),
            "NUL is not permitted"
        );
        ensure!(self.tags.len() <= 32, "at most 32 tags");
        for (k, v) in &self.tags {
            ensure!(
                !k.is_empty() && k.len() <= 128 && v.len() <= 1024,
                "invalid tag size"
            );
            ensure!(
                !k.contains('\0') && !v.contains('\0'),
                "NUL is not permitted in tags"
            );
        }
        Ok(())
    }
    pub fn estimated_bytes(&self) -> usize {
        128 + self.tenant.len()
            + self.series.len()
            + self
                .tags
                .iter()
                .map(|(k, v)| k.len() + v.len() + 64)
                .sum::<usize>()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StoredRow {
    #[serde(flatten)]
    pub row: Row,
    pub sequence: u64,
    pub ordinal: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct TableConfig {
    pub shards: u32,
    pub window_us: i64,
    pub late_after_us: Option<i64>,
    pub retention_us: Option<i64>,
    pub archive_after_us: Option<i64>,
    pub rollup_widths_us: Vec<i64>,
    pub rollup_retention_us: Option<i64>,
    pub idempotency_window_us: Option<i64>,
}

impl Default for TableConfig {
    fn default() -> Self {
        Self {
            shards: 8,
            window_us: 3_600_000_000,
            late_after_us: None,
            retention_us: None,
            archive_after_us: None,
            rollup_widths_us: vec![60_000_000],
            rollup_retention_us: None,
            idempotency_window_us: None,
        }
    }
}
impl TableConfig {
    pub fn validate(&self) -> Result<()> {
        ensure!((1..=1024).contains(&self.shards), "shards must be 1..1024");
        ensure!(self.window_us > 0, "window_us must be positive");
        for duration in [
            self.late_after_us,
            self.retention_us,
            self.archive_after_us,
            self.rollup_retention_us,
            self.idempotency_window_us,
        ]
        .into_iter()
        .flatten()
        {
            ensure!(duration > 0, "policy durations must be positive");
        }
        ensure!(
            self.rollup_widths_us.len() <= 16,
            "at most 16 rollup widths"
        );
        let mut widths = self.rollup_widths_us.clone();
        widths.sort_unstable();
        widths.dedup();
        ensure!(
            widths.len() == self.rollup_widths_us.len(),
            "duplicate rollup widths"
        );
        ensure!(
            widths.iter().all(|w| *w > 0),
            "rollup widths must be positive"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct LifecyclePolicy {
    pub late_after_us: Option<i64>,
    pub retention_us: Option<i64>,
    pub archive_after_us: Option<i64>,
    pub rollup_retention_us: Option<i64>,
    pub idempotency_window_us: Option<i64>,
}

impl LifecyclePolicy {
    pub fn validate(&self) -> Result<()> {
        for duration in [
            self.late_after_us,
            self.retention_us,
            self.archive_after_us,
            self.rollup_retention_us,
            self.idempotency_window_us,
        ]
        .into_iter()
        .flatten()
        {
            ensure!(duration > 0, "policy durations must be positive");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ContinuousAggregate {
    pub name: String,
    pub source: String,
    pub width_us: i64,
    pub created_sequence: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    Checkpoint,
    Compact,
    Ship,
    Maintain,
    VacuumRemote,
}

impl JobKind {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "checkpoint" => Ok(Self::Checkpoint),
            "compact" => Ok(Self::Compact),
            "ship" => Ok(Self::Ship),
            "maintain" => Ok(Self::Maintain),
            "vacuum_remote" => Ok(Self::VacuumRemote),
            _ => bail!("unknown job kind"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct JobRun {
    pub run_id: u64,
    pub scheduled_us: i64,
    pub started_us: i64,
    pub finished_us: Option<i64>,
    pub attempt: u32,
    pub success: Option<bool>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct JobDefinition {
    pub name: String,
    pub kind: JobKind,
    pub interval_us: i64,
    pub paused: bool,
    pub next_run_us: Option<i64>,
    pub running: bool,
    pub attempts: u32,
    pub created_sequence: u64,
    pub updated_sequence: u64,
    pub latest_run: Option<JobRun>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct JobAlter {
    pub interval_us: Option<i64>,
    pub paused: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub hot_max_bytes: usize,
    pub metadata_max_bytes: usize,
    pub hot_max_rows: usize,
    pub wal_max_bytes: u64,
    pub max_disk_bytes: u64,
    pub decoded_cache_bytes: usize,
    pub disk_cache_bytes: u64,
    pub max_batch_rows: usize,
    pub max_batch_bytes: usize,
    pub max_idempotency_keys: usize,
    pub max_rollup_groups: usize,
    pub max_tables: usize,
    pub segment_rows: usize,
    pub compact_min_segments: usize,
    pub flush_interval_us: i64,
    pub ship_interval_us: i64,
    pub maintenance_interval_ms: u64,
    pub query_executable: PathBuf,
    pub query_memory_mb: usize,
    pub query_threads: usize,
    pub query_timeout_ms: u64,
    pub query_max_output_bytes: usize,
    pub query_workers: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            hot_max_bytes: 16 * 1024 * 1024,
            metadata_max_bytes: 32 * 1024 * 1024,
            hot_max_rows: 100_000,
            wal_max_bytes: 64 * 1024 * 1024,
            max_disk_bytes: 512 * 1024 * 1024,
            decoded_cache_bytes: 8 * 1024 * 1024,
            disk_cache_bytes: 32 * 1024 * 1024,
            max_batch_rows: 10_000,
            max_batch_bytes: 4 * 1024 * 1024,
            max_idempotency_keys: 100_000,
            max_rollup_groups: 100_000,
            max_tables: 128,
            segment_rows: 16_384,
            compact_min_segments: 4,
            flush_interval_us: 5_000_000,
            ship_interval_us: 1_000_000,
            maintenance_interval_ms: 1000,
            query_executable: PathBuf::from("duckdb"),
            query_memory_mb: 128,
            query_threads: 2,
            query_timeout_ms: 30_000,
            query_max_output_bytes: 8 * 1024 * 1024,
            query_workers: 2,
        }
    }
}
impl Config {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.hot_max_bytes > 0
                && self.hot_max_rows > 0
                && self.max_batch_rows > 0
                && self.max_batch_rows <= u32::MAX as usize,
            "invalid row/memory limits"
        );
        ensure!(
            (1024..=64 * 1024 * 1024).contains(&self.metadata_max_bytes),
            "metadata_max_bytes must be 1KiB..64MiB"
        );
        ensure!(
            self.max_batch_bytes > 0 && self.max_batch_bytes <= 64 * 1024 * 1024,
            "max_batch_bytes must be 1..64MiB"
        );
        ensure!(
            self.wal_max_bytes > 0 && self.max_disk_bytes > self.wal_max_bytes,
            "invalid WAL/disk budgets"
        );
        ensure!(
            self.segment_rows > 0 && self.compact_min_segments >= 2,
            "invalid segment/compaction limits"
        );
        ensure!(
            self.max_idempotency_keys > 0 && self.max_rollup_groups > 0 && self.max_tables > 0,
            "metadata limits must be positive"
        );
        ensure!(
            self.flush_interval_us > 0
                && self.ship_interval_us > 0
                && self.maintenance_interval_ms > 0,
            "scheduler intervals must be positive"
        );
        ensure!(
            self.query_memory_mb >= 16
                && self.query_threads > 0
                && self.query_workers > 0
                && self.query_timeout_ms > 0
                && self.query_max_output_bytes > 0,
            "invalid query limits"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RollupRow {
    pub width_us: i64,
    pub bucket_us: i64,
    pub tenant: String,
    pub series: String,
    pub tags: BTreeMap<String, String>,
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub first: f64,
    pub last: f64,
    pub first_timestamp_us: i64,
    pub last_timestamp_us: i64,
    pub first_sequence: u64,
    pub first_ordinal: u32,
    pub last_sequence: u64,
    pub last_ordinal: u32,
}
impl RollupRow {
    pub fn from_row(width_us: i64, stored: &StoredRow) -> Result<Self> {
        let r = &stored.row;
        Ok(Self {
            width_us,
            bucket_us: window_start(r.timestamp_us, width_us)?,
            tenant: r.tenant.clone(),
            series: r.series.clone(),
            tags: r.tags.clone(),
            count: 1,
            sum: r.value,
            min: r.value,
            max: r.value,
            first: r.value,
            last: r.value,
            first_timestamp_us: r.timestamp_us,
            last_timestamp_us: r.timestamp_us,
            first_sequence: stored.sequence,
            last_sequence: stored.sequence,
            first_ordinal: stored.ordinal,
            last_ordinal: stored.ordinal,
        })
    }
    pub fn add(&mut self, stored: &StoredRow) -> Result<()> {
        let r = &stored.row;
        let sum = self.sum + r.value;
        ensure!(sum.is_finite(), "rollup sum overflow");
        self.count = self
            .count
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("rollup count overflow"))?;
        self.sum = sum;
        self.min = self.min.min(r.value);
        self.max = self.max.max(r.value);
        let key = (r.timestamp_us, stored.sequence, stored.ordinal);
        if key
            < (
                self.first_timestamp_us,
                self.first_sequence,
                self.first_ordinal,
            )
        {
            self.first = r.value;
            self.first_timestamp_us = r.timestamp_us;
            self.first_sequence = stored.sequence;
            self.first_ordinal = stored.ordinal;
        }
        if key
            > (
                self.last_timestamp_us,
                self.last_sequence,
                self.last_ordinal,
            )
        {
            self.last = r.value;
            self.last_timestamp_us = r.timestamp_us;
            self.last_sequence = stored.sequence;
            self.last_ordinal = stored.ordinal;
        }
        Ok(())
    }
}

pub fn validate_name(name: &str) -> Result<()> {
    ensure!(
        !name.is_empty() && name.len() <= 63 && name.as_bytes()[0].is_ascii_lowercase(),
        "table name must begin with a-z and be 1..63 bytes"
    );
    ensure!(
        name.bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
            && !name.contains("__"),
        "table names allow a-z, 0-9 and single underscores"
    );
    Ok(())
}

pub fn window_start(timestamp_us: i64, width_us: i64) -> Result<i64> {
    ensure!(width_us > 0, "window must be positive");
    timestamp_us
        .div_euclid(width_us)
        .checked_mul(width_us)
        .ok_or_else(|| anyhow::anyhow!("timestamp window underflow"))
}

pub fn shard_for(tenant: &str, series: &str, shards: u32) -> u32 {
    // Length-prefix components so different tenant/series boundaries cannot alias.
    let mut hash = 0xcbf29ce484222325u64;
    for bytes in [
        (tenant.len() as u64).to_le_bytes().to_vec(),
        tenant.as_bytes().to_vec(),
        (series.len() as u64).to_le_bytes().to_vec(),
        series.as_bytes().to_vec(),
    ] {
        for byte in bytes {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    (hash % u64::from(shards.max(1))) as u32
}

pub fn checked_cutoff(now_us: i64, age_us: i64) -> i64 {
    now_us.saturating_sub(age_us)
}

pub fn validate_request_id(id: &str) -> Result<()> {
    if id.is_empty() || id.len() > 256 || id.contains('\0') {
        bail!("request_id must be 1..256 non-NUL bytes");
    }
    Ok(())
}
