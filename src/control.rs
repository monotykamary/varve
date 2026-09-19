use crate::engine::*;
use crate::job_runtime::{self, JobRuntime};
use crate::model::*;
use crate::wal;
use anyhow::{Context, Result, bail, ensure};
use serde::Serialize;
use serde_json::{Value, json};
use sqlparser::ast::Statement;
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;
use std::collections::BTreeMap;

const CONTROL_HISTORY_LIMIT: usize = 4096;
const MAX_JOB_ATTEMPTS: u32 = 5;
const BUILTIN_MAINTENANCE_JOB: &str = "varve_maintenance";

#[derive(Serialize)]
struct DigestJob<'a> {
    name: &'a str,
    kind: &'a JobKind,
    interval_us: i64,
    paused: bool,
    created_sequence: u64,
    updated_sequence: u64,
}

fn control_digest(catalog: &Manifest) -> Result<String> {
    let tables: BTreeMap<_, _> = catalog
        .tables
        .iter()
        .map(|(name, table)| {
            (
                name,
                (
                    table.creation_config.as_ref().unwrap_or(&table.config),
                    &table.config,
                    table.created_sequence,
                ),
            )
        })
        .collect();
    let jobs: Vec<_> = catalog
        .jobs
        .values()
        .map(|job| DigestJob {
            name: &job.name,
            kind: &job.kind,
            interval_us: job.interval_us,
            paused: job.paused,
            created_sequence: job.created_sequence,
            updated_sequence: job.updated_sequence,
        })
        .collect();
    let bytes = serde_json::to_vec(&(tables, &catalog.continuous_aggregates, jobs))?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

fn stamp_projected(catalog: &mut Manifest, sequence: u64) -> Result<String> {
    let digest = control_digest(catalog)?;
    catalog.control_history.push(ControlStamp {
        sequence,
        digest: digest.clone(),
    });
    if catalog.control_history.len() > CONTROL_HISTORY_LIMIT {
        catalog.control_history.remove(0);
    }
    Ok(digest)
}

fn verify_and_stamp(catalog: &mut Manifest, sequence: u64, expected: &str) -> Result<()> {
    ensure!(
        expected.len() == 64
            && expected
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()),
        "invalid control digest"
    );
    let actual = stamp_projected(catalog, sequence)?;
    ensure!(actual == expected, "control WAL state digest mismatch");
    Ok(())
}

fn validate_interval(interval_us: i64) -> Result<()> {
    ensure!(interval_us > 0, "job interval_us must be positive");
    Ok(())
}

fn validate_job(job: &JobDefinition, checkpoint_sequence: u64) -> Result<()> {
    validate_name(&job.name)?;
    validate_interval(job.interval_us)?;
    let builtin_identity = job.name == BUILTIN_MAINTENANCE_JOB && job.created_sequence == 0;
    ensure!(
        (builtin_identity || job.created_sequence > 0)
            && job.created_sequence <= job.updated_sequence
            && job.updated_sequence <= checkpoint_sequence,
        "invalid job definition sequence"
    );
    ensure!(
        job.attempts <= MAX_JOB_ATTEMPTS,
        "invalid job attempt count"
    );
    if let Some(run) = &job.latest_run {
        ensure!(
            run.run_id > 0
                && run.attempt > 0
                && run.attempt <= MAX_JOB_ATTEMPTS
                && run.error.as_ref().is_none_or(|error| error.len() <= 2048),
            "invalid job run"
        );
        ensure!(
            job.running == run.finished_us.is_none(),
            "invalid job running state"
        );
    } else {
        ensure!(!job.running, "job is running without a run record");
    }
    Ok(())
}

pub(crate) fn validate_control_manifest(catalog: &Manifest) -> Result<()> {
    ensure!(
        catalog.control_history.len() <= CONTROL_HISTORY_LIMIT,
        "control history limit exceeded"
    );
    let mut prior = 0;
    for stamp in &catalog.control_history {
        ensure!(
            stamp.sequence > prior
                && stamp.sequence <= catalog.checkpoint_sequence
                && stamp.digest.len() == 64
                && stamp
                    .digest
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()),
            "invalid control history"
        );
        prior = stamp.sequence;
    }
    for (name, aggregate) in &catalog.continuous_aggregates {
        validate_name(name)?;
        ensure!(aggregate.name == *name, "aggregate name/key mismatch");
        ensure!(aggregate.width_us > 0, "aggregate width must be positive");
        ensure!(
            aggregate.created_sequence > 0
                && aggregate.created_sequence <= catalog.checkpoint_sequence,
            "invalid aggregate creation sequence"
        );
        let table = catalog
            .tables
            .get(&aggregate.source)
            .context("aggregate source table missing")?;
        ensure!(
            table.config.rollup_widths_us.contains(&aggregate.width_us),
            "aggregate width is not active on source table"
        );
    }
    for job in catalog.jobs.values() {
        validate_job(job, catalog.checkpoint_sequence)?;
    }
    Ok(())
}

pub(crate) fn replay_control_operation(
    s: &mut State,
    sequence: u64,
    operation: wal::Operation,
) -> Result<()> {
    match operation {
        wal::Operation::SetPolicy {
            table,
            policy,
            stamp,
        } => {
            policy.validate()?;
            let target = s
                .catalog
                .tables
                .get_mut(&table)
                .context("policy references unknown table")?;
            target
                .creation_config
                .get_or_insert_with(|| target.config.clone());
            target.config.late_after_us = policy.late_after_us;
            target.config.retention_us = policy.retention_us;
            ensure!(
                target.config.idempotency_window_us.is_none()
                    || policy.idempotency_window_us.is_some(),
                "timed idempotency cannot be disabled after cutover"
            );
            target.config.archive_after_us = policy.archive_after_us;
            target.config.rollup_retention_us = policy.rollup_retention_us;
            target.config.idempotency_window_us = policy.idempotency_window_us;
            target.config.validate()?;
            verify_and_stamp(&mut s.catalog, sequence, &stamp)?;
        }
        wal::Operation::CreateContinuousAggregate {
            aggregate,
            backfill,
            stamp,
        } => {
            validate_name(&aggregate.name)?;
            ensure!(
                aggregate.width_us > 0 && aggregate.created_sequence == sequence,
                "invalid aggregate WAL metadata"
            );
            ensure!(
                !s.catalog.tables.contains_key(&aggregate.name),
                "aggregate name collides with a table"
            );
            ensure!(
                !s.catalog
                    .continuous_aggregates
                    .contains_key(&aggregate.name)
                    && !s.catalog.jobs.contains_key(&aggregate.name),
                "duplicate or colliding aggregate creation"
            );
            let table = s
                .catalog
                .tables
                .get_mut(&aggregate.source)
                .context("aggregate source table missing")?;
            table
                .creation_config
                .get_or_insert_with(|| table.config.clone());
            if !table.config.rollup_widths_us.contains(&aggregate.width_us) {
                ensure!(
                    table.config.rollup_widths_us.len() < 16,
                    "at most 16 active rollup widths per table"
                );
                table.config.rollup_widths_us.push(aggregate.width_us);
                table.config.rollup_widths_us.sort_unstable();
            }
            ensure!(
                backfill
                    .values()
                    .all(|row| row.width_us == aggregate.width_us),
                "aggregate backfill has the wrong width"
            );
            table.rollups.extend(backfill);
            s.catalog
                .continuous_aggregates
                .insert(aggregate.name.clone(), aggregate);
            verify_and_stamp(&mut s.catalog, sequence, &stamp)?;
        }
        wal::Operation::DropContinuousAggregate { name, stamp } => {
            let aggregate = s
                .catalog
                .continuous_aggregates
                .remove(&name)
                .context("aggregate does not exist")?;
            let shared = s.catalog.continuous_aggregates.values().any(|other| {
                other.source == aggregate.source && other.width_us == aggregate.width_us
            });
            let table = s
                .catalog
                .tables
                .get_mut(&aggregate.source)
                .context("aggregate source table missing")?;
            let original = table.creation_config.as_ref().unwrap_or(&table.config);
            if !shared && !original.rollup_widths_us.contains(&aggregate.width_us) {
                table
                    .config
                    .rollup_widths_us
                    .retain(|width| *width != aggregate.width_us);
                table
                    .rollups
                    .retain(|_, row| row.width_us != aggregate.width_us);
            }
            verify_and_stamp(&mut s.catalog, sequence, &stamp)?;
        }
        wal::Operation::PutJob { job, stamp } => {
            validate_name(&job.name)?;
            validate_interval(job.interval_us)?;
            ensure!(
                job.updated_sequence == sequence,
                "job update sequence mismatch"
            );
            ensure!(
                !s.catalog.tables.contains_key(&job.name)
                    && !s.catalog.continuous_aggregates.contains_key(&job.name),
                "job name collides with a table or aggregate"
            );
            if let Some(existing) = s.catalog.jobs.get(&job.name) {
                ensure!(
                    existing.created_sequence == job.created_sequence,
                    "job creation identity changed"
                );
            } else {
                ensure!(
                    job.created_sequence == sequence,
                    "new job creation sequence mismatch"
                );
            }
            s.catalog.jobs.insert(job.name.clone(), job);
            verify_and_stamp(&mut s.catalog, sequence, &stamp)?;
        }
        wal::Operation::DropJob { name, stamp } => {
            ensure!(
                name != BUILTIN_MAINTENANCE_JOB,
                "the built-in maintenance job cannot be dropped"
            );
            s.catalog.jobs.remove(&name).context("job does not exist")?;
            verify_and_stamp(&mut s.catalog, sequence, &stamp)?;
        }
        wal::Operation::JobStarted { name, run, manual } => {
            let job = s
                .catalog
                .jobs
                .get_mut(&name)
                .context("job start references unknown job")?;
            ensure!(
                !job.paused || manual,
                "paused job cannot start on a scheduled tick"
            );
            ensure!(
                run.finished_us.is_none() && run.success.is_none() && run.error.is_none(),
                "invalid job start record"
            );
            ensure!(
                run.attempt > 0 && run.attempt <= MAX_JOB_ATTEMPTS,
                "job retry bound exceeded"
            );
            if job.running {
                let prior = job.latest_run.as_ref().context("running job has no run")?;
                let expected_attempt = prior
                    .attempt
                    .checked_add(1)
                    .context("job attempt overflow")?
                    .min(MAX_JOB_ATTEMPTS);
                ensure!(
                    prior.run_id == run.run_id && run.attempt == expected_attempt,
                    "invalid job retry"
                );
            }
            job.running = true;
            job.attempts = run.attempt;
            job.latest_run = Some(run);
        }
        wal::Operation::JobFinished {
            name,
            run,
            next_run_us,
        } => {
            let job = s
                .catalog
                .jobs
                .get_mut(&name)
                .context("job finish references unknown job")?;
            let active = job.latest_run.as_ref().context("job has no active run")?;
            ensure!(
                job.running && active.run_id == run.run_id && active.attempt == run.attempt,
                "job completion does not match active run"
            );
            ensure!(
                run.finished_us.is_some() && run.success.is_some(),
                "invalid job completion record"
            );
            ensure!(
                run.error.as_ref().is_none_or(|error| error.len() <= 2048),
                "job error exceeds 2KiB"
            );
            job.running = false;
            job.attempts = run.attempt;
            job.latest_run = Some(run);
            job.next_run_us = Some(next_run_us);
        }
        _ => bail!("non-control operation passed to control replay"),
    }
    Ok(())
}

fn preflight(inner: &Inner, s: &State, projected: &mut Manifest, sequence: u64) -> Result<()> {
    projected.checkpoint_sequence = sequence;
    check_state_catalog_budget(inner, s, projected, hot_count(s))
}

fn commit_and_replay(db: &Database, s: &mut State, operation: wal::Operation) -> Result<u64> {
    let sequence = next_sequence(s)?;
    let record = wal::Record::new(sequence, operation);
    commit_record(&db.inner, s, &record)?;
    if let Err(error) = replay(s, record, &db.inner.config) {
        s.fenced = Some(format!(
            "committed control WAL could not be applied: {error:#}"
        ));
        return Err(error.context("apply committed control WAL; reopen for recovery"));
    }
    Ok(sequence)
}

fn runtime_for(s: &State, job: &JobDefinition) -> JobRuntime {
    s.job_runtime
        .get(&job.name)
        .filter(|runtime| runtime.generation == job.updated_sequence)
        .cloned()
        .unwrap_or_else(|| JobRuntime::from_definition(job))
}

fn validate_runtime(runtime: &JobRuntime) -> Result<()> {
    ensure!(
        runtime.attempts <= MAX_JOB_ATTEMPTS,
        "invalid job runtime attempt count"
    );
    if let Some(run) = &runtime.latest_run {
        ensure!(
            run.run_id > 0
                && run.attempt > 0
                && run.attempt <= MAX_JOB_ATTEMPTS
                && run.error.as_ref().is_none_or(|error| error.len() <= 2048),
            "invalid private job run"
        );
        ensure!(
            runtime.running == run.finished_us.is_none(),
            "private job running state does not match latest run"
        );
    } else {
        ensure!(
            !runtime.running,
            "private job is running without a run record"
        );
    }
    Ok(())
}

fn persist_runtime_map(
    inner: &Inner,
    s: &mut State,
    next: BTreeMap<String, JobRuntime>,
) -> Result<()> {
    for (name, runtime) in &next {
        let job = s
            .catalog
            .jobs
            .get(name)
            .context("job runtime has no definition")?;
        ensure!(
            runtime.generation == job.updated_sequence,
            "stale job runtime generation"
        );
        validate_runtime(runtime)?;
    }
    let new_len = job_runtime::encoded_len(&next)? as u64;
    let _disk = lock_disk_admission(inner)?;
    ensure_budget(inner, new_len)?;
    job_runtime::persist(&inner.root, &next)?;
    s.job_runtime = next;
    Ok(())
}

fn persist_committed_runtime(
    inner: &Inner,
    s: &mut State,
    next: BTreeMap<String, JobRuntime>,
    committed: u64,
) -> Result<()> {
    if let Err(error) = persist_runtime_map(inner, s, next) {
        let reason = format!(
            "job mutation committed at sequence {committed}, but runtime journal publication failed; database fenced, reopen and inspect the durable definition: {error:#}"
        );
        s.fenced = Some(reason.clone());
        bail!(reason);
    }
    Ok(())
}

fn aggregate_width(
    rows: &[StoredRow],
    width_us: i64,
    cutoff_us: Option<i64>,
    output: &mut BTreeMap<String, RollupRow>,
    byte_budget: usize,
) -> Result<()> {
    let mut resident = output.iter().fold(0usize, |sum, (key, row)| {
        sum.saturating_add(crate::derived::rollup_resident_bytes(key, row))
    });
    for stored in rows {
        if cutoff_us.is_some_and(|cutoff| stored.row.timestamp_us < cutoff) {
            continue;
        }
        let bucket = window_start(stored.row.timestamp_us, width_us)?;
        let key = serde_json::to_string(&(
            width_us,
            bucket,
            &stored.row.tenant,
            &stored.row.series,
            &stored.row.tags,
        ))?;
        if let Some(row) = output.get_mut(&key) {
            row.add(stored)?;
        } else {
            let row = RollupRow::from_row(width_us, stored)?;
            resident = resident.saturating_add(crate::derived::rollup_resident_bytes(&key, &row));
            ensure!(
                resident <= byte_budget,
                "derived backfill resident budget exceeded"
            );
            output.insert(key, row);
        }
        ensure!(
            serde_json::to_vec(output)?.len() <= byte_budget,
            "aggregate backfill exceeds metadata budget"
        );
    }
    Ok(())
}

pub(crate) fn builtin_job(config: &Config) -> Result<JobDefinition> {
    let interval_us = i64::try_from(config.maintenance_interval_ms)
        .context("maintenance interval exceeds i64")?
        .checked_mul(1_000)
        .context("maintenance interval overflow")?;
    validate_interval(interval_us)?;
    Ok(JobDefinition {
        name: BUILTIN_MAINTENANCE_JOB.into(),
        kind: JobKind::Maintain,
        interval_us,
        paused: false,
        next_run_us: Some(interval_us),
        running: false,
        attempts: 0,
        created_sequence: 0,
        updated_sequence: 0,
        latest_run: None,
    })
}

impl Database {
    pub(crate) fn ensure_builtin_job(&self) -> Result<()> {
        let interval_us = builtin_job(&self.inner.config)?.interval_us;
        if self
            .lock()?
            .catalog
            .jobs
            .contains_key(BUILTIN_MAINTENANCE_JOB)
        {
            return Ok(());
        }
        self.create_job(BUILTIN_MAINTENANCE_JOB, JobKind::Maintain, interval_us)?;
        Ok(())
    }

    pub fn set_policy(&self, table: &str, policy: LifecyclePolicy) -> Result<u64> {
        validate_name(table)?;
        policy.validate()?;
        let _commit = self.lock_commit()?;
        let mut s = self.lock()?;
        healthy(&s)?;
        let sequence = next_sequence(&s)?;
        let _derived_working = reserve_catalog_clone(&s, &self.inner.config)?;
        let mut projected = s.catalog.clone();
        let target = projected.tables.get_mut(table).context("unknown table")?;
        target
            .creation_config
            .get_or_insert_with(|| target.config.clone());
        target.config.late_after_us = policy.late_after_us;
        target.config.retention_us = policy.retention_us;
        ensure!(
            target.config.idempotency_window_us.is_none() || policy.idempotency_window_us.is_some(),
            "timed idempotency cannot be disabled after cutover"
        );
        target.config.archive_after_us = policy.archive_after_us;
        target.config.rollup_retention_us = policy.rollup_retention_us;
        target.config.idempotency_window_us = policy.idempotency_window_us;
        target.config.validate()?;
        let stamp = stamp_projected(&mut projected, sequence)?;
        preflight(&self.inner, &s, &mut projected, sequence)?;
        commit_and_replay(
            self,
            &mut s,
            wal::Operation::SetPolicy {
                table: table.into(),
                policy,
                stamp,
            },
        )
    }

    pub fn policy(&self, table: &str) -> Result<LifecyclePolicy> {
        let s = self.lock()?;
        healthy(&s)?;
        let config = &s.catalog.tables.get(table).context("unknown table")?.config;
        Ok(LifecyclePolicy {
            late_after_us: config.late_after_us,
            retention_us: config.retention_us,
            archive_after_us: config.archive_after_us,
            rollup_retention_us: config.rollup_retention_us,
            idempotency_window_us: config.idempotency_window_us,
        })
    }

    pub fn idempotency_floor_us(&self, table: &str) -> Result<Option<i64>> {
        let s = self.lock()?;
        healthy(&s)?;
        let durable = s
            .catalog
            .tables
            .get(table)
            .context("unknown table")?
            .idempotency_floor_us;
        Ok(s.idempotency_floors.get(table).copied().or(durable))
    }

    pub fn continuous_aggregates(&self) -> Result<Vec<ContinuousAggregate>> {
        let s = self.lock()?;
        healthy(&s)?;
        Ok(s.catalog.continuous_aggregates.values().cloned().collect())
    }

    pub fn create_continuous_aggregate(
        &self,
        name: &str,
        source: &str,
        width_us: i64,
    ) -> Result<u64> {
        validate_name(name)?;
        validate_name(source)?;
        ensure!(width_us > 0, "aggregate width_us must be positive");
        let (barrier, cutoff, segments, mut backfill, pin, _backfill_working, backfill_budget) = {
            let _commit = self.lock_commit()?;
            let mut s = self.lock()?;
            healthy(&s)?;
            ensure!(
                !s.catalog.tables.contains_key(name),
                "aggregate name collides with a table"
            );
            ensure!(
                !s.catalog.continuous_aggregates.contains_key(name)
                    && !s.catalog.jobs.contains_key(name),
                "aggregate already exists or collides with a job"
            );
            let table = s
                .catalog
                .tables
                .get(source)
                .context("unknown source table")?;
            let unique = !table.config.rollup_widths_us.contains(&width_us);
            ensure!(
                !unique || table.config.rollup_widths_us.len() < 16,
                "at most 16 active rollup widths per table"
            );
            checkpoint_locked(&self.inner, &mut s)?;
            let table = s
                .catalog
                .tables
                .get(source)
                .context("unknown source table")?;
            let ids = table
                .segments
                .iter()
                .map(|segment| segment.id.clone())
                .collect();
            let pin = Pin::new(&self.inner, ids)?;
            let available = self
                .inner
                .config
                .derived_max_bytes
                .saturating_sub(s.derived_resident_bytes)
                .saturating_sub(s.derived_working.load(std::sync::atomic::Ordering::SeqCst));
            let backfill_budget = (available / 8).min(self.inner.config.metadata_max_bytes);
            ensure!(
                backfill_budget > 0,
                "derived backfill working budget exhausted"
            );
            let working =
                reserve_derived(&s, &self.inner.config, backfill_budget.saturating_mul(4))?;
            let existing_bytes = table
                .rollups
                .iter()
                .filter(|(_, r)| r.width_us == width_us)
                .fold(0usize, |sum, (key, row)| {
                    sum.saturating_add(crate::derived::rollup_resident_bytes(key, row))
                });
            ensure!(
                unique || existing_bytes <= backfill_budget,
                "derived backfill working budget exceeded"
            );
            let existing = if unique {
                BTreeMap::new()
            } else {
                table
                    .rollups
                    .iter()
                    .filter(|(_, row)| row.width_us == width_us)
                    .map(|(key, row)| (key.clone(), row.clone()))
                    .collect()
            };
            (
                s.sequence,
                table.cutoff_us,
                table.segments.clone(),
                existing,
                pin,
                working,
                backfill_budget,
            )
        };
        if backfill.is_empty() {
            for descriptor in &segments {
                ensure!(
                    descriptor.decoded_bytes <= self.inner.config.hot_max_bytes as u64,
                    "backfill segment exceeds bounded working set"
                );
                let rows = read_raw_segment(&self.inner, descriptor, true)?;
                ensure!(
                    rows.len() as u64 == descriptor.rows,
                    "backfill segment row-count mismatch"
                );
                aggregate_width(&rows, width_us, cutoff, &mut backfill, backfill_budget)?;
            }
        }
        drop(pin);
        let _commit = self.lock_commit()?;
        let mut s = self.lock()?;
        healthy(&s)?;
        ensure!(
            s.sequence == barrier,
            "writes changed during aggregate backfill; retry the operation"
        );
        let sequence = next_sequence(&s)?;
        let aggregate = ContinuousAggregate {
            name: name.into(),
            source: source.into(),
            width_us,
            created_sequence: sequence,
        };
        let _derived_working = reserve_catalog_clone(&s, &self.inner.config)?;
        let mut projected = s.catalog.clone();
        let table = projected
            .tables
            .get_mut(source)
            .context("unknown source table")?;
        table
            .creation_config
            .get_or_insert_with(|| table.config.clone());
        if !table.config.rollup_widths_us.contains(&width_us) {
            table.config.rollup_widths_us.push(width_us);
            table.config.rollup_widths_us.sort_unstable();
        }
        table.rollups.extend(backfill.clone());
        projected
            .continuous_aggregates
            .insert(name.into(), aggregate.clone());
        let stamp = stamp_projected(&mut projected, sequence)?;
        preflight(&self.inner, &s, &mut projected, sequence)?;
        commit_and_replay(
            self,
            &mut s,
            wal::Operation::CreateContinuousAggregate {
                aggregate,
                backfill,
                stamp,
            },
        )
    }

    pub fn drop_continuous_aggregate(&self, name: &str) -> Result<u64> {
        validate_name(name)?;
        let _commit = self.lock_commit()?;
        let mut s = self.lock()?;
        healthy(&s)?;
        let sequence = next_sequence(&s)?;
        let _derived_working = reserve_catalog_clone(&s, &self.inner.config)?;
        let mut projected = s.catalog.clone();
        let aggregate = projected
            .continuous_aggregates
            .remove(name)
            .context("aggregate does not exist")?;
        let shared = projected
            .continuous_aggregates
            .values()
            .any(|other| other.source == aggregate.source && other.width_us == aggregate.width_us);
        let table = projected
            .tables
            .get_mut(&aggregate.source)
            .context("aggregate source table missing")?;
        let original = table.creation_config.as_ref().unwrap_or(&table.config);
        if !shared && !original.rollup_widths_us.contains(&aggregate.width_us) {
            table
                .config
                .rollup_widths_us
                .retain(|width| *width != aggregate.width_us);
            table
                .rollups
                .retain(|_, row| row.width_us != aggregate.width_us);
        }
        let stamp = stamp_projected(&mut projected, sequence)?;
        preflight(&self.inner, &s, &mut projected, sequence)?;
        commit_and_replay(
            self,
            &mut s,
            wal::Operation::DropContinuousAggregate {
                name: name.into(),
                stamp,
            },
        )
    }

    pub fn create_job(&self, name: &str, kind: JobKind, interval_us: i64) -> Result<u64> {
        validate_name(name)?;
        validate_interval(interval_us)?;
        let _commit = self.lock_commit()?;
        let mut s = self.lock()?;
        healthy(&s)?;
        ensure!(
            !s.catalog.jobs.contains_key(name)
                && !s.catalog.tables.contains_key(name)
                && !s.catalog.continuous_aggregates.contains_key(name),
            "job name already exists or collides with a relation"
        );
        let sequence = next_sequence(&s)?;
        let job = JobDefinition {
            name: name.into(),
            kind,
            interval_us,
            paused: false,
            next_run_us: Some(interval_us),
            running: false,
            attempts: 0,
            created_sequence: sequence,
            updated_sequence: sequence,
            latest_run: None,
        };
        let _derived_working = reserve_catalog_clone(&s, &self.inner.config)?;
        let mut projected = s.catalog.clone();
        projected.jobs.insert(name.into(), job.clone());
        let stamp = stamp_projected(&mut projected, sequence)?;
        preflight(&self.inner, &s, &mut projected, sequence)?;
        commit_and_replay(self, &mut s, wal::Operation::PutJob { job, stamp })
    }

    pub fn alter_job(&self, name: &str, alter: JobAlter) -> Result<u64> {
        validate_name(name)?;
        if let Some(interval) = alter.interval_us {
            validate_interval(interval)?;
        }
        ensure!(
            alter.interval_us.is_some() || alter.paused.is_some(),
            "empty job alteration"
        );
        let _commit = self.lock_commit()?;
        let mut s = self.lock()?;
        healthy(&s)?;
        let sequence = next_sequence(&s)?;
        let definition = s.catalog.jobs.get(name).context("job does not exist")?;
        let mut runtime = runtime_for(&s, definition);
        ensure!(!runtime.running, "cannot alter a running job");
        let _derived_working = reserve_catalog_clone(&s, &self.inner.config)?;
        let mut projected = s.catalog.clone();
        let job = projected
            .jobs
            .get_mut(name)
            .expect("job definition validated");
        if let Some(interval) = alter.interval_us {
            job.interval_us = interval;
            runtime.next_run_us = runtime.next_run_us.map(|next| next.min(interval));
        }
        if let Some(paused) = alter.paused {
            job.paused = paused;
        }
        job.updated_sequence = sequence;
        runtime.generation = sequence;
        let job = job.clone();
        let stamp = stamp_projected(&mut projected, sequence)?;
        preflight(&self.inner, &s, &mut projected, sequence)?;
        let committed = commit_and_replay(self, &mut s, wal::Operation::PutJob { job, stamp })?;
        let mut next_runtime = s.job_runtime.clone();
        next_runtime.insert(name.to_owned(), runtime);
        persist_committed_runtime(&self.inner, &mut s, next_runtime, committed)?;
        Ok(committed)
    }

    pub fn pause_job(&self, name: &str) -> Result<u64> {
        self.alter_job(
            name,
            JobAlter {
                interval_us: None,
                paused: Some(true),
            },
        )
    }

    pub fn resume_job(&self, name: &str) -> Result<u64> {
        self.alter_job(
            name,
            JobAlter {
                interval_us: None,
                paused: Some(false),
            },
        )
    }

    pub fn drop_job(&self, name: &str) -> Result<u64> {
        validate_name(name)?;
        ensure!(
            name != BUILTIN_MAINTENANCE_JOB,
            "the built-in maintenance job cannot be dropped"
        );
        let _commit = self.lock_commit()?;
        let mut s = self.lock()?;
        healthy(&s)?;
        let definition = s.catalog.jobs.get(name).context("job does not exist")?;
        ensure!(
            !runtime_for(&s, definition).running,
            "cannot drop a running job"
        );
        let sequence = next_sequence(&s)?;
        let _derived_working = reserve_catalog_clone(&s, &self.inner.config)?;
        let mut projected = s.catalog.clone();
        projected.jobs.remove(name);
        let stamp = stamp_projected(&mut projected, sequence)?;
        preflight(&self.inner, &s, &mut projected, sequence)?;
        let committed = commit_and_replay(
            self,
            &mut s,
            wal::Operation::DropJob {
                name: name.into(),
                stamp,
            },
        )?;
        let mut next_runtime = s.job_runtime.clone();
        next_runtime.remove(name);
        persist_committed_runtime(&self.inner, &mut s, next_runtime, committed)?;
        Ok(committed)
    }

    pub fn jobs(&self) -> Result<Vec<JobDefinition>> {
        let s = self.lock()?;
        healthy(&s)?;
        Ok(s.catalog
            .jobs
            .values()
            .map(|definition| {
                let mut job = definition.clone();
                runtime_for(&s, definition).apply_to(&mut job);
                job
            })
            .collect())
    }

    fn execute_job(&self, name: &str, now_us: i64, force: bool) -> Result<()> {
        {
            let mut running = self
                .inner
                .jobs_running
                .lock()
                .map_err(|_| anyhow::anyhow!("job execution mutex poisoned"))?;
            if !running.insert(name.to_owned()) {
                return Ok(());
            }
        }
        let started = (|| {
            let _commit = self.lock_commit()?;
            let mut s = self.lock()?;
            healthy(&s)?;
            let definition = s
                .catalog
                .jobs
                .get(name)
                .context("job does not exist")?
                .clone();
            let mut runtime = runtime_for(&s, &definition);
            if (definition.paused && !force)
                || (!force
                    && !runtime.running
                    && runtime.next_run_us.is_none_or(|next| next > now_us))
            {
                return Ok(None);
            }
            let retry = runtime.running
                || runtime.latest_run.as_ref().is_some_and(|run| {
                    run.success == Some(false) && run.attempt < MAX_JOB_ATTEMPTS
                });
            let (run_id, scheduled_us, attempt) = if retry {
                let prior = runtime
                    .latest_run
                    .as_ref()
                    .context("retrying job has no prior run")?;
                (
                    prior.run_id,
                    prior.scheduled_us,
                    prior
                        .attempt
                        .checked_add(1)
                        .context("job attempt overflow")?
                        .min(MAX_JOB_ATTEMPTS),
                )
            } else {
                let run_id = runtime.latest_run.as_ref().map_or(Ok(1), |run| {
                    run.run_id.checked_add(1).context("job run id overflow")
                })?;
                (run_id, runtime.next_run_us.unwrap_or(now_us), 1)
            };
            let run = JobRun {
                run_id,
                scheduled_us,
                started_us: now_us,
                finished_us: None,
                attempt,
                success: None,
                error: None,
            };
            runtime.running = true;
            runtime.attempts = attempt;
            runtime.latest_run = Some(run.clone());
            let mut next_runtime = s.job_runtime.clone();
            next_runtime.insert(name.to_owned(), runtime);
            persist_runtime_map(&self.inner, &mut s, next_runtime)?;
            Ok(Some((definition.kind, definition.interval_us, run)))
        })();
        let outcome = (|| match started {
            Ok(Some((kind, interval_us, mut run))) => {
                let action = match kind {
                    JobKind::Checkpoint => self.checkpoint().map(|_| ()),
                    JobKind::Compact => self.compact().map(|_| ()),
                    JobKind::Ship => self.ship().map(|_| ()),
                    JobKind::Maintain => self.maintain(now_us).map(|_| ()),
                    JobKind::VacuumRemote => self.vacuum_remote().map(|_| ()),
                };
                run.finished_us = Some(now_us);
                run.success = Some(action.is_ok());
                run.error = action.as_ref().err().map(|error| {
                    let text = format!("{error:#}");
                    text.chars().take(2048).collect()
                });
                let delay = if action.is_ok() || run.attempt >= MAX_JOB_ATTEMPTS {
                    interval_us
                } else {
                    let shift = run.attempt.saturating_sub(1).min(20);
                    interval_us.min(1_000_000i64.saturating_mul(1i64 << shift))
                };
                let next_run_us = now_us.checked_add(delay).context("job next-run overflow")?;
                let persisted = (|| {
                    let _commit = self.lock_commit()?;
                    let mut s = self.lock()?;
                    healthy(&s)?;
                    let definition = s
                        .catalog
                        .jobs
                        .get(name)
                        .context("job disappeared during run")?;
                    let mut runtime = runtime_for(&s, definition);
                    let active = runtime
                        .latest_run
                        .as_ref()
                        .context("job has no active run")?;
                    ensure!(
                        runtime.running
                            && active.run_id == run.run_id
                            && active.attempt == run.attempt,
                        "job completion does not match private active run"
                    );
                    runtime.running = false;
                    runtime.attempts = run.attempt;
                    runtime.latest_run = Some(run);
                    runtime.next_run_us = Some(next_run_us);
                    let mut next_runtime = s.job_runtime.clone();
                    next_runtime.insert(name.to_owned(), runtime);
                    persist_runtime_map(&self.inner, &mut s, next_runtime)
                })();
                persisted?;
                action
            }
            Ok(None) => Ok(()),
            Err(error) => Err(error),
        })();
        self.inner
            .jobs_running
            .lock()
            .map_err(|_| anyhow::anyhow!("job execution mutex poisoned"))?
            .remove(name);
        outcome
    }

    pub fn run_job_now(&self, name: &str, now_us: i64) -> Result<()> {
        self.execute_job(name, now_us, true)
    }

    pub fn tick(&self, now_us: i64) -> Result<()> {
        let names: Vec<String> = {
            let s = self.lock()?;
            healthy(&s)?;
            s.catalog
                .jobs
                .values()
                .filter(|definition| {
                    let runtime = runtime_for(&s, definition);
                    !definition.paused
                        && (runtime.running
                            || runtime.next_run_us.is_some_and(|next| next <= now_us))
                })
                .map(|job| job.name.clone())
                .collect()
        };
        let mut errors = Vec::new();
        for name in names {
            if let Err(error) = self.execute_job(&name, now_us, false) {
                errors.push(format!("{name}: {error:#}"));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            bail!("scheduled job failures: {}", errors.join("; "))
        }
    }

    pub(crate) fn try_control_call(&self, sql: &str) -> Result<Option<Value>> {
        let dialect = DuckDbDialect {};
        let statements = Parser::parse_sql(&dialect, sql).context("parse SQL")?;
        ensure!(
            statements.len() == 1,
            "exactly one SQL statement is required"
        );
        if !matches!(&statements[0], Statement::Call(_)) {
            return Ok(None);
        }
        let normalized = statements[0].to_string();
        let (function, args) = parse_call_literals(&normalized)?;
        let result = match function.as_str() {
            "varve_create_table" => {
                expect_len(&args, 2)?;
                let config: TableConfig = serde_json::from_str(args[1].as_string()?)?;
                json!({"sequence": self.create_table(args[0].as_string()?, config)?})
            }
            "varve_set_policy" => {
                expect_len(&args, 2)?;
                let policy: LifecyclePolicy = serde_json::from_str(args[1].as_string()?)?;
                json!({"sequence": self.set_policy(args[0].as_string()?, policy)?})
            }
            "varve_create_continuous_aggregate" => {
                expect_len(&args, 3)?;
                json!({"sequence": self.create_continuous_aggregate(args[0].as_string()?, args[1].as_string()?, args[2].as_i64()?)?})
            }
            "varve_drop_continuous_aggregate" => {
                expect_len(&args, 1)?;
                json!({"sequence": self.drop_continuous_aggregate(args[0].as_string()?)?})
            }
            "varve_create_job" => {
                expect_len(&args, 3)?;
                json!({"sequence": self.create_job(args[0].as_string()?, JobKind::parse(args[1].as_string()?)?, args[2].as_i64()?)?})
            }
            "varve_alter_job" => {
                expect_len(&args, 2)?;
                let alter: JobAlter = serde_json::from_str(args[1].as_string()?)?;
                json!({"sequence": self.alter_job(args[0].as_string()?, alter)?})
            }
            "varve_drop_job" => {
                expect_len(&args, 1)?;
                json!({"sequence": self.drop_job(args[0].as_string()?)?})
            }
            "varve_run_job" => {
                ensure!(
                    args.len() == 1 || args.len() == 2,
                    "varve_run_job expects one or two arguments"
                );
                let now_us = if args.len() == 2 {
                    args[1].as_i64()?
                } else {
                    0
                };
                self.run_job_now(args[0].as_string()?, now_us)?;
                json!({"job": args[0].as_string()?, "run_at_us": now_us})
            }
            _ => bail!("CALL function is not whitelisted"),
        };
        Ok(Some(result))
    }
}

#[derive(Debug)]
enum Literal {
    String(String),
    Integer(i64),
}

impl Literal {
    fn as_string(&self) -> Result<&str> {
        match self {
            Self::String(value) => Ok(value),
            _ => bail!("expected a string literal"),
        }
    }
    fn as_i64(&self) -> Result<i64> {
        match self {
            Self::Integer(value) => Ok(*value),
            _ => bail!("expected an integer literal"),
        }
    }
}

fn expect_len(args: &[Literal], expected: usize) -> Result<()> {
    ensure!(
        args.len() == expected,
        "control function expects {expected} arguments"
    );
    Ok(())
}

fn parse_call_literals(sql: &str) -> Result<(String, Vec<Literal>)> {
    let body = sql
        .trim()
        .strip_prefix("CALL ")
        .context("invalid CALL statement")?;
    let open = body.find('(').context("CALL requires parentheses")?;
    ensure!(body.ends_with(')'), "invalid CALL statement");
    let function = body[..open].trim().to_ascii_lowercase();
    ensure!(
        function
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte == b'_'),
        "invalid control function name"
    );
    let content = &body[open + 1..body.len() - 1];
    let mut args = Vec::new();
    let mut index = 0;
    while index < content.len() {
        while index < content.len() && content.as_bytes()[index].is_ascii_whitespace() {
            index += 1;
        }
        if index == content.len() {
            break;
        }
        if content.as_bytes()[index] == b'\'' {
            index += 1;
            let mut value = String::new();
            loop {
                ensure!(index < content.len(), "unterminated string literal");
                if content.as_bytes()[index] == b'\'' {
                    if index + 1 < content.len() && content.as_bytes()[index + 1] == b'\'' {
                        value.push('\'');
                        index += 2;
                    } else {
                        index += 1;
                        break;
                    }
                } else {
                    let ch = content[index..]
                        .chars()
                        .next()
                        .context("invalid UTF-8 boundary")?;
                    value.push(ch);
                    index += ch.len_utf8();
                }
            }
            args.push(Literal::String(value));
        } else {
            let start = index;
            if content.as_bytes()[index] == b'-' {
                index += 1;
            }
            while index < content.len() && content.as_bytes()[index].is_ascii_digit() {
                index += 1;
            }
            ensure!(
                index > start && &content[start..index] != "-",
                "only string and integer literals are allowed"
            );
            args.push(Literal::Integer(content[start..index].parse()?));
        }
        while index < content.len() && content.as_bytes()[index].is_ascii_whitespace() {
            index += 1;
        }
        if index < content.len() {
            ensure!(
                content.as_bytes()[index] == b',',
                "only literal arguments are allowed"
            );
            index += 1;
            ensure!(index < content.len(), "trailing comma in CALL");
        }
    }
    Ok((function, args))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn legacy_job_runtime_wal_events_replay_into_private_overlay_baseline() {
        let temporary = TempDir::new().unwrap();
        let config = Config::default();
        let database = Database::open(temporary.path(), config.clone()).unwrap();
        let created = database
            .create_job("legacy_job", JobKind::Checkpoint, 10)
            .unwrap();
        let run = JobRun {
            run_id: 44,
            scheduled_us: 10,
            started_us: 11,
            finished_us: None,
            attempt: 1,
            success: None,
            error: None,
        };
        wal::append(
            temporary.path(),
            &wal::Record::new(
                created + 1,
                wal::Operation::JobStarted {
                    name: "legacy_job".into(),
                    run: run.clone(),
                    manual: false,
                },
            ),
        )
        .unwrap();
        let mut finished = run;
        finished.finished_us = Some(12);
        finished.success = Some(true);
        wal::append(
            temporary.path(),
            &wal::Record::new(
                created + 2,
                wal::Operation::JobFinished {
                    name: "legacy_job".into(),
                    run: finished,
                    next_run_us: 22,
                },
            ),
        )
        .unwrap();
        drop(database);

        let reopened = Database::open(temporary.path(), config).unwrap();
        let job = reopened
            .jobs()
            .unwrap()
            .into_iter()
            .find(|job| job.name == "legacy_job")
            .unwrap();
        assert_eq!(job.latest_run.unwrap().run_id, 44);
        assert_eq!(job.next_run_us, Some(22));
    }
}
