//! Varve: local-first time-series storage with asynchronous object-store protection.
mod control;
mod derived;
mod derived_root;
pub mod engine;
pub mod flow;
pub mod ingest;
mod job_runtime;
pub mod journal;
pub mod metrics;
pub mod model;
mod plan;
mod policy;
pub mod query;
pub mod raw_memory;
pub mod remote;
pub mod segment;
mod tier;
mod wal;

pub use derived::RollupSelection;
pub use engine::{Database, MaintenanceReport, Status, WriteReceipt, WriteRequest};
pub use ingest::{
    IngestConfig, IngestFlush, IngestStats, IngestTrace, IngestTraceSnapshot, Ingestor,
};
pub use model::{
    Config, ContinuousAggregate, FlushPolicy, IDEMPOTENCY_MAX_FUTURE_SKEW_US, JobAlter,
    JobDefinition, JobKind, JobRun, LifecyclePolicy, RollupRow, Row, StoredRow, TableConfig,
};
