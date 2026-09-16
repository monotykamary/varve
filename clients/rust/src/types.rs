use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

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
    pub fn validate(&self) -> Result<(), &'static str> {
        if !self.value.is_finite() {
            return Err("value must be finite");
        }
        if self.tenant.is_empty() || self.tenant.len() > 256 {
            return Err("tenant must be 1..256 bytes");
        }
        if self.series.is_empty() || self.series.len() > 1024 {
            return Err("series must be 1..1024 bytes");
        }
        if self.tenant.contains('\0') || self.series.contains('\0') {
            return Err("NUL is not permitted in tenant or series");
        }
        if self.tags.len() > 32 {
            return Err("at most 32 tags are permitted");
        }
        for (key, value) in &self.tags {
            if key.is_empty() || key.len() > 128 || value.len() > 1024 {
                return Err("invalid tag size");
            }
            if key.contains('\0') || value.contains('\0') {
                return Err("NUL is not permitted in tags");
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
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

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct WriteReceipt {
    pub sequence: u64,
    pub rows: usize,
    pub duplicate: bool,
    pub durability: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RequestId(String);

impl RequestId {
    pub fn new(value: impl Into<String>) -> Result<Self, RequestIdError> {
        let value = value.into();
        if value.is_empty() {
            return Err(RequestIdError::Empty);
        }
        if value.len() > 128 {
            return Err(RequestIdError::TooLong);
        }
        if !value.is_ascii() {
            return Err(RequestIdError::NonAscii);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for RequestId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl TryFrom<String> for RequestId {
    type Error = RequestIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for RequestId {
    type Error = RequestIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum RequestIdError {
    #[error("request ID must not be empty")]
    Empty,
    #[error("request ID must be at most 128 bytes")]
    TooLong,
    #[error("request ID must contain only ASCII characters")]
    NonAscii,
}
