//! Varve: local-first time-series storage with asynchronous object-store protection.
mod control;
mod derived;
mod derived_root;
pub mod engine;
pub mod ingest;
mod job_runtime;
pub mod metrics;
pub mod model;
mod plan;
mod policy;
pub mod query;
pub mod remote;
pub mod segment;
mod tier;
mod wal;

pub use derived::RollupSelection;
pub use engine::{Database, MaintenanceReport, Status, WriteReceipt, WriteRequest};
pub use ingest::{IngestConfig, IngestStats, Ingestor};
pub use model::{
    Config, ContinuousAggregate, IDEMPOTENCY_MAX_FUTURE_SKEW_US, JobAlter, JobDefinition, JobKind,
    JobRun, LifecyclePolicy, RollupRow, Row, StoredRow, TableConfig,
};
