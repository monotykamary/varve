//! Sealed journal transport. Ordinary uploads retain only disk pins and one
//! bounded object buffer; recovery/proof snapshots are private and short-lived.
use super::*;
use crate::journal::{Journal, JournalIoPhase, SealedSegment};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct JournalRef {
    pub object: ObjectRef,
    pub file_name: String,
    pub first_sequence: u64,
    pub last_sequence: u64,
}

impl JournalRef {
    fn descriptor(&self) -> SealedSegment {
        SealedSegment {
            relative_path: PathBuf::from(&self.file_name),
            first_sequence: self.first_sequence,
            last_sequence: self.last_sequence,
            bytes: self.object.bytes,
        }
    }
}

fn file_name(path: &Path) -> Result<String> {
    let name = path.to_str().context("non-UTF8 journal file name")?;
    let id: u64 = name
        .strip_prefix("segment-")
        .and_then(|name| name.strip_suffix(".jrn"))
        .context("invalid journal file name")?
        .parse()?;
    ensure!(
        id > 0 && name == format!("segment-{id:020}.jrn"),
        "unsafe journal file name"
    );
    Ok(name.to_owned())
}

pub(super) fn validate_refs(head: &RemoteHead) -> Result<()> {
    ensure!(
        head.wal.is_empty() || head.journal.is_empty(),
        "mixed remote WAL/journal transport"
    );
    let mut names = BTreeSet::new();
    let mut keys = BTreeSet::new();
    let mut previous: Option<u64> = None;
    for reference in &head.journal {
        reference.object.validate("journal/", ".jrn")?;
        ensure!(
            reference.object.key == format!("journal/{}.jrn", reference.object.digest),
            "journal key/digest mismatch"
        );
        file_name(Path::new(&reference.file_name))?;
        ensure!(
            names.insert(&reference.file_name) && keys.insert(&reference.object.key),
            "duplicate remote journal reference"
        );
        ensure!(
            reference.first_sequence > 0 && reference.first_sequence <= reference.last_sequence,
            "invalid journal record range"
        );
        if let Some(last) = previous {
            ensure!(
                last.checked_add(1) == Some(reference.first_sequence),
                "noncontiguous remote journal ranges"
            );
        }
        previous = Some(reference.last_sequence);
    }
    Ok(())
}

pub(super) fn validate_transport(head: &RemoteHead, checkpoint: &Manifest) -> Result<()> {
    validate_refs(head)?;
    ensure!(
        checkpoint.checkpoint_sequence <= head.sequence,
        "remote checkpoint is ahead of head"
    );
    if checkpoint.segmented_journal {
        ensure!(
            head.wal.is_empty(),
            "segmented root has legacy WAL transport"
        );
        if checkpoint.checkpoint_sequence == head.sequence {
            ensure!(
                head.journal.is_empty(),
                "checkpoint-only head contains journal tail"
            );
        } else {
            let first = head
                .journal
                .first()
                .context("incomplete remote journal tail")?;
            let next = checkpoint
                .checkpoint_sequence
                .checked_add(1)
                .context("journal sequence overflow")?;
            ensure!(
                first.first_sequence <= next && first.last_sequence >= next,
                "journal tail is unaligned with checkpoint"
            );
            ensure!(
                head.journal.last().map(|r| r.last_sequence) == Some(head.sequence),
                "incomplete remote journal tail"
            );
        }
    } else {
        ensure!(head.journal.is_empty(), "legacy root has journal transport");
        ensure!(
            u64::try_from(head.wal.len()).ok()
                == head.sequence.checked_sub(checkpoint.checkpoint_sequence),
            "incomplete remote WAL tail"
        );
    }
    Ok(())
}

// The caller retains the commit owner through this capture and pin publication.
// Readers keep using the installed state while sealing waits for durability.
pub(super) fn capture(db: &Database) -> Result<Vec<SealedSegment>> {
    let inner = &db.inner;
    let (sequence, checkpoint_sequence) = {
        let s = db.lock()?;
        healthy(&s)?;
        if !s.catalog.segmented_journal {
            return Ok(Vec::new());
        }
        (s.sequence, s.catalog.checkpoint_sequence)
    };
    let mut journal = inner
        .journal
        .as_ref()
        .context("segmented root has no journal owner")?
        .lock()
        .map_err(|_| anyhow::anyhow!("journal owner poisoned"))?;
    ensure!(
        journal.stats().durable_sequence == sequence,
        "journal includes an uninstalled tail"
    );
    let mut disk = Some(lock_disk_admission(inner)?);
    ensure_budget(inner, 0)?;
    let result = journal.seal_snapshot_with_observer(|phase| {
        match phase {
            JournalIoPhase::BeforeWrite | JournalIoPhase::BeforeNamespaceChange => {
                if disk.is_none() {
                    disk = Some(lock_disk_admission(inner)?);
                }
            }
            JournalIoPhase::FileSync | JournalIoPhase::DirectorySync => {
                drop(disk.take());
                #[cfg(feature = "fault-injection")]
                db.block_maintenance_test_hook(match phase {
                    JournalIoPhase::FileSync => MaintenanceHookPhase::WalBeforeSync,
                    JournalIoPhase::DirectorySync => MaintenanceHookPhase::WalBeforeDirectorySync,
                    _ => unreachable!(),
                })?;
            }
        }
        Ok(())
    });
    let disk_bytes = journal.stats().disk_bytes;
    drop(disk);
    drop(journal);
    let segments = {
        let mut s = db.lock()?;
        s.wal_bytes = disk_bytes;
        match result {
            Ok(segments) => {
                ensure!(
                    s.sequence == sequence && s.catalog.checkpoint_sequence == checkpoint_sequence,
                    "journal capture frontier changed under commit owner"
                );
                segments
            }
            Err(error) => {
                s.fenced = Some(format!("ambiguous journal seal publication: {error:#}"));
                return Err(error);
            }
        }
    };
    let mut bytes = 0u64;
    let mut tail = Vec::new();
    for segment in segments {
        if segment.last_sequence <= checkpoint_sequence {
            continue;
        }
        file_name(&segment.relative_path)?;
        ensure!(
            segment.last_sequence <= sequence,
            "journal snapshot exceeds installed frontier"
        );
        bytes = bytes
            .checked_add(segment.bytes)
            .context("journal snapshot size overflow")?;
        ensure!(
            bytes <= inner.config.wal_max_bytes,
            "journal snapshot exceeds WAL budget"
        );
        tail.push(segment);
    }
    Ok(tail)
}

pub(super) fn upload(inner: &Inner, segments: &[SealedSegment]) -> Result<Vec<JournalRef>> {
    if segments.is_empty() {
        return Ok(Vec::new());
    }
    let remote = inner
        .remote
        .as_ref()
        .context("no remote store configured")?;
    let config = journal_config(&inner.config)?;
    let mut references = Vec::with_capacity(segments.len());
    for segment in segments {
        let name = file_name(&segment.relative_path)?;
        let path = inner.root.join("journal").join(&name);
        ensure!(
            segment.bytes <= config.segment_bytes && segment.bytes <= wal::MAX_FRAME_BYTES as u64,
            "journal object exceeds size limit"
        );
        let bytes = wal::read_bounded(&path, usize::try_from(segment.bytes)?)?;
        ensure!(
            bytes.len() as u64 == segment.bytes,
            "journal snapshot extent changed"
        );
        crate::journal::validate_sealed_bytes(&bytes, config, segment, |group| {
            for (sequence, bytes) in group.records() {
                ensure!(
                    wal::decode(bytes)?.sequence == sequence,
                    "journal/WAL sequence mismatch during upload"
                );
            }
            Ok(())
        })?;
        let object = ObjectRef::new(
            format!("journal/{}.jrn", blake3::hash(&bytes).to_hex()),
            &bytes,
        );
        remote.put_immutable(&object.key, &bytes)?;
        references.push(JournalRef {
            object,
            file_name: name,
            first_sequence: segment.first_sequence,
            last_sequence: segment.last_sequence,
        });
    }
    Ok(references)
}

// Admit the complete disk closure before downloading, but allocate only one
// bounded object at a time. Atomic writes include file and directory barriers.
pub(super) fn install(
    directory: &Path,
    config: &Config,
    remote: &dyn RemoteStore,
    head: &RemoteHead,
    used: &mut u64,
) -> Result<()> {
    validate_refs(head)?;
    let limits = journal_config(config)?;
    ensure!(
        head.journal.len() <= limits.max_segments,
        "remote journal exceeds segment count budget"
    );
    let mut journal_bytes = 0u64;
    for reference in &head.journal {
        ensure!(
            reference.object.bytes <= limits.segment_bytes,
            "remote journal exceeds segment byte budget"
        );
        journal_bytes = journal_bytes
            .checked_add(reference.object.bytes)
            .context("remote journal size overflow")?;
    }
    ensure!(
        journal_bytes <= limits.max_total_bytes,
        "remote journal exceeds WAL budget"
    );
    *used = used
        .checked_add(journal_bytes)
        .context("restore journal size overflow")?;
    ensure!(
        *used <= config.max_disk_bytes,
        "restore journal exceeds disk budget"
    );
    fs::create_dir(directory)?;
    for reference in &head.journal {
        let bytes = remote.get_bounded(
            &reference.object.key,
            usize::try_from(reference.object.bytes)?,
        )?;
        reference.object.verify(&bytes)?;
        wal::atomic_write(&directory.join(&reference.file_name), &bytes)?;
    }
    wal::sync_dir(directory)?;
    wal::sync_dir(
        directory
            .parent()
            .context("journal directory has no parent")?,
    )
}

pub(super) fn scan_installed(
    directory: &Path,
    config: &Config,
    head: &RemoteHead,
    checkpoint: &Manifest,
    mut visit: impl FnMut(u64, &[u8]) -> Result<()>,
) -> Result<()> {
    validate_transport(head, checkpoint)?;
    let limits = journal_config(config)?;
    // Validate each exact sealed object before open: recovery must never silently
    // repair an active or truncated remote object and acknowledge different bytes.
    for reference in &head.journal {
        crate::journal::validate_sealed_segment(
            directory.join(&reference.file_name),
            limits,
            &reference.descriptor(),
            |_| Ok(()),
        )?;
    }
    let journal = Journal::open_with_checkpoint(directory, limits, checkpoint.checkpoint_sequence)?;
    ensure!(
        journal.stats().durable_sequence == head.sequence
            && journal.stats().tail_bytes_discarded == 0,
        "remote journal replay frontier mismatch"
    );
    journal.scan(|group| {
        for (sequence, bytes) in group.records() {
            ensure!(
                wal::decode(bytes)?.sequence == sequence,
                "remote journal/WAL sequence mismatch"
            );
            if sequence > checkpoint.checkpoint_sequence {
                visit(sequence, bytes)?;
            }
        }
        Ok(())
    })
}

pub(super) fn visit_tail(
    inner: &Inner,
    head: &RemoteHead,
    checkpoint: &Manifest,
    mut visit: impl FnMut(u64, &[u8]) -> Result<()>,
) -> Result<()> {
    validate_transport(head, checkpoint)?;
    let remote = inner.remote.as_ref().context("no remote configured")?;
    if checkpoint.segmented_journal {
        let temporary = tempfile::Builder::new()
            .prefix("varve-journal-proof-")
            .tempdir()?;
        let directory = temporary.path().join("journal");
        let mut used = 0;
        install(&directory, &inner.config, remote.as_ref(), head, &mut used)?;
        scan_installed(&directory, &inner.config, head, checkpoint, visit)
    } else {
        let mut total = 0u64;
        for (index, object) in head.wal.iter().enumerate() {
            total = total
                .checked_add(object.bytes)
                .context("remote WAL proof size overflow")?;
            ensure!(
                total <= inner.config.wal_max_bytes,
                "remote WAL proof exceeds configured WAL budget"
            );
            let offset = u64::try_from(index)?
                .checked_add(1)
                .context("remote WAL index overflow")?;
            let sequence = checkpoint
                .checkpoint_sequence
                .checked_add(offset)
                .context("remote WAL sequence overflow")?;
            let bytes = remote.get_bounded(&object.key, usize::try_from(object.bytes)?)?;
            object.verify(&bytes)?;
            ensure!(
                object.key == format!("wal/{sequence:020}-{}.wal", object.digest),
                "remote WAL sequence/key mismatch"
            );
            visit(sequence, &bytes)?;
        }
        Ok(())
    }
}
