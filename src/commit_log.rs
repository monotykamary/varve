//! Engine-owned segmented persistence. All mutation is serialized by the commit
//! owner; reader state is released during append sync and immutable publication
//! happens only after a durable journal receipt.
use super::*;
use crate::journal::{JournalConfig, JournalIoPhase};

pub(super) fn config(config: &Config) -> Result<JournalConfig> {
    let frame = usize::try_from(config.wal_max_bytes.saturating_sub(128))?.min(16 * 1024 * 1024);
    ensure!(frame >= 137, "WAL budget cannot contain a journal record");
    let segment = frame as u64 + 128;
    let segments = config
        .wal_max_bytes
        .div_ceil(segment)
        .saturating_add(64)
        .min(65_536) as usize;
    let limits = JournalConfig {
        max_total_bytes: config.wal_max_bytes,
        segment_bytes: segment,
        max_segments: segments,
        max_frame_bytes: frame,
        max_record_bytes: frame
            .saturating_sub(136)
            .min(config.max_batch_bytes.saturating_add(64 * 1024)),
        max_records_per_group: 1,
    };
    limits.validate()?;
    Ok(limits)
}

pub(super) fn required_bytes(inner: &Inner, encoded: &wal::EncodedRecord) -> Result<u64> {
    match &inner.journal {
        Some(journal) => journal
            .lock()
            .map_err(|_| anyhow::anyhow!("journal owner poisoned"))?
            .required_append_bytes(&[encoded.as_bytes()])
            .and_then(|bytes| {
                bytes
                    .checked_add(crate::journal::RESERVED_SEAL_BYTES)
                    .context("journal admission overflow")
            }),
        None => Ok(encoded.len() as u64),
    }
}

fn observe<'a>(
    inner: &'a Inner,
    disk: &mut Option<MeasuredDiskGuard<'a>>,
    phase: JournalIoPhase,
    db: Option<&Database>,
) -> Result<()> {
    match phase {
        JournalIoPhase::BeforeWrite | JournalIoPhase::BeforeNamespaceChange => {
            if disk.is_none() {
                *disk = Some(lock_disk_admission(inner)?);
            }
        }
        JournalIoPhase::FileSync | JournalIoPhase::DirectorySync => {
            drop(disk.take());
            #[cfg(feature = "fault-injection")]
            if let Some(db) = db {
                db.block_maintenance_test_hook(match phase {
                    JournalIoPhase::FileSync => MaintenanceHookPhase::WalBeforeSync,
                    JournalIoPhase::DirectorySync => MaintenanceHookPhase::WalBeforeDirectorySync,
                    _ => unreachable!(),
                })?;
            }
            #[cfg(not(feature = "fault-injection"))]
            let _ = db;
        }
    }
    Ok(())
}

/// Outer errors are pre-I/O admission failures; inner errors fence publication.
pub(super) fn append(
    inner: &Inner,
    encoded: &wal::EncodedRecord,
    db: Option<&Database>,
) -> Result<Result<usize>> {
    let mut journal = inner
        .journal
        .as_ref()
        .expect("segmented journal selected")
        .lock()
        .map_err(|_| anyhow::anyhow!("journal owner poisoned"))?;
    let growth = journal.required_append_bytes(&[encoded.as_bytes()])?;
    let before = journal.stats();
    ensure!(
        encoded.sequence()
            == before
                .durable_sequence
                .checked_add(1)
                .context("journal sequence exhausted")?,
        "journal/WAL publication sequence mismatch"
    );
    let mut disk = Some(lock_disk_admission(inner)?);
    ensure_budget(inner, growth)?;
    let _publication = inner.metrics.timer(Phase::WalWrite);
    let result = journal.append_group_with_observer(&[encoded.as_bytes()], |phase| {
        observe(inner, &mut disk, phase, db)
    });
    Ok(result.and_then(|receipt| {
        ensure!(
            receipt.first_sequence() == encoded.sequence()
                && receipt.last_sequence() == encoded.sequence(),
            "journal receipt sequence mismatch"
        );
        let after = journal.stats();
        let growth = after
            .disk_bytes
            .checked_sub(before.disk_bytes)
            .context("journal append reduced disk extent")?;
        usize::try_from(growth).context("journal growth does not fit host accounting")
    }))
}

/// A frozen uploader pins sealed files on disk, never hot-ring entries.
pub(super) fn reclaim(inner: &Inner, state: &mut State, frontier: u64) -> Result<usize> {
    let mut journal = inner
        .journal
        .as_ref()
        .expect("segmented journal selected")
        .lock()
        .map_err(|_| anyhow::anyhow!("journal owner poisoned"))?;
    let pinned = inner
        .segment_pins
        .lock()
        .map_err(|_| anyhow::anyhow!("journal pins poisoned"))?
        .keys()
        .any(|key| key.starts_with("journal/"));
    let before = journal.stats();
    if !pinned {
        let mut disk = Some(lock_disk_admission(inner)?);
        if let Err(error) = journal
            .reclaim_through_with_observer(frontier, |phase| observe(inner, &mut disk, phase, None))
        {
            state.fenced = Some(format!("ambiguous journal reclamation: {error:#}"));
            return Err(error);
        }
    }
    let after = journal.stats();
    state.wal_bytes = after.disk_bytes;
    Ok(before.segments.saturating_sub(after.segments))
}
