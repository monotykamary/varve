use crate::model::{JobDefinition, JobRun};
use crate::wal;
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

const MAGIC: &[u8; 8] = b"VARVEJ01";
pub(crate) const MAX_JOURNAL_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct JobRuntime {
    pub generation: u64,
    pub next_run_us: Option<i64>,
    pub running: bool,
    pub attempts: u32,
    pub latest_run: Option<JobRun>,
}

impl JobRuntime {
    pub(crate) fn from_definition(job: &JobDefinition) -> Self {
        Self {
            generation: job.updated_sequence,
            next_run_us: job.next_run_us,
            running: job.running,
            attempts: job.attempts,
            latest_run: job.latest_run.clone(),
        }
    }

    pub(crate) fn apply_to(&self, job: &mut JobDefinition) {
        job.next_run_us = self.next_run_us;
        job.running = self.running;
        job.attempts = self.attempts;
        job.latest_run = self.latest_run.clone();
    }

    fn validate(&self) -> Result<()> {
        ensure!(self.attempts <= 5, "invalid job runtime attempt count");
        if let Some(run) = &self.latest_run {
            ensure!(
                run.run_id > 0
                    && (1..=5).contains(&run.attempt)
                    && run.error.as_ref().is_none_or(|error| error.len() <= 2048),
                "invalid job runtime run"
            );
            ensure!(
                self.running == run.finished_us.is_none(),
                "job runtime running state mismatch"
            );
        } else {
            ensure!(!self.running, "job runtime is running without a run");
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    format_version: u32,
    jobs: BTreeMap<String, JobRuntime>,
}

fn encode(jobs: &BTreeMap<String, JobRuntime>) -> Result<Vec<u8>> {
    let journal = Journal {
        format_version: 1,
        jobs: jobs.clone(),
    };
    let mut bytes = MAGIC.to_vec();
    bytes.extend(serde_json::to_vec(&journal)?);
    ensure!(
        bytes.len().saturating_add(32) <= MAX_JOURNAL_BYTES,
        "job runtime journal exceeds 4MiB"
    );
    let checksum = blake3::hash(&bytes);
    bytes.extend_from_slice(checksum.as_bytes());
    Ok(bytes)
}

pub(crate) fn encoded_len(jobs: &BTreeMap<String, JobRuntime>) -> Result<usize> {
    Ok(encode(jobs)?.len())
}

pub(crate) fn load(root: &Path) -> Result<BTreeMap<String, JobRuntime>> {
    let path = root.join("job-runtime.bin");
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let bytes = wal::read_bounded(&path, MAX_JOURNAL_BYTES)?;
    ensure!(
        bytes.len() >= MAGIC.len() + 32 && &bytes[..MAGIC.len()] == MAGIC,
        "invalid job runtime journal magic or size"
    );
    let (payload, checksum) = bytes.split_at(bytes.len() - 32);
    ensure!(
        blake3::hash(payload).as_bytes() == checksum,
        "job runtime journal checksum mismatch"
    );
    let journal: Journal = serde_json::from_slice(&payload[MAGIC.len()..])?;
    ensure!(
        journal.format_version == 1,
        "unsupported job runtime journal version"
    );
    for runtime in journal.jobs.values() {
        runtime.validate()?;
    }
    Ok(journal.jobs)
}

pub(crate) fn persist(root: &Path, jobs: &BTreeMap<String, JobRuntime>) -> Result<usize> {
    let bytes = encode(jobs)?;
    wal::atomic_write(&root.join("job-runtime.bin"), &bytes)?;
    Ok(bytes.len())
}
