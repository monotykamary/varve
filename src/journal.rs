//! Single-owner, bounded, append-only journal foundation (not the legacy WAL format).
//!
//! `open` takes an exclusive OS `LOCK` in a dedicated directory. Its parent must
//! exist; only LOCK and canonical segment names are accepted. No automatic legacy
//! migration or remote durability is provided. `open_with_checkpoint` accepts an
//! externally proven durable root frontier; only that authority permits missing
//! covered history or reclamation. `open` requires the complete history from 1.
//! Capacity exhaustion backpressures the owner until covered segments are retired.
//! The directory is trusted: external writes, symlink races and lock-file removal
//! while open are outside this cooperative-process ownership contract.
//!
//! Format v1, all integers little-endian; no padding or native-layout structs:
//! - Segment header (64): magic[8], version:u32, flags:u32=0, id:u64,
//!   first_sequence:u64, BLAKE3(first 32 bytes)[32]. IDs and sequences start at 1.
//! - Group header (64): magic[8], frame_bytes:u64, first_sequence:u64,
//!   record_count:u64, BLAKE3(first 32 bytes)[32].
//! - Records: repeated length:u64 followed by opaque payload bytes.
//! - Commit trailer (64): magic[8], first:u64, last:u64, frame_bytes:u64,
//!   BLAKE3(group header + records + first 32 trailer bytes)[32].
//! - Seal (64): magic[8], segment_id:u64, next_sequence:u64, sealed_bytes:u64,
//!   BLAKE3(entire segment prefix + first 32 seal bytes)[32].
//!
//! A group never spans segments. Complete bad headers, frames, seals, unknown
//! versions, uncovered sequence/ID gaps and unsealed interior segments fail
//! closed. Only an incomplete physical LAST unsealed segment may lose its last
//! group: recovery truncates to the preceding commit and syncs before use. A
//! partial last segment header must match the expected header prefix. Complete
//! commit trailers are replayed even if a crash prevented returning a receipt.
//! Reopen syncs recovered files and the directory before exposing their frontier.
//! A physically truncated last tail is intrinsically ambiguous: this policy does
//! NOT diagnose all post-acknowledgment hardware corruption or lost whole final
//! segments. Checksums are integrity evidence, not authentication or redundancy.
//!
//! Rotation appends and syncs a seal, then creates/syncs the successor and syncs
//! the directory. A sealed file is never subsequently modified by this module;
//! it is the upload unit. Ordinary groups only append and sync one file.
//! Engine integration stores one existing `wal::EncodedRecord` byte string per
//! journal record, not one per inner request: journal and physical WAL sequences
//! must match. The generic journal preserves opaque bytes; WAL replay must also
//! validate the inner frame and its sequence independently.
//! Any write/sync/rotation error fences the instance until drop + reopen. Receipt
//! sequences establish local durability only, not publication, visibility or S3.
//!
//! Bounds are checked before append allocation and mutating I/O. Recovery first
//! admits directory metadata, then validates frame bounds before payload reads
//! or allocation. Scan retains at most one bounded frame (plus bounded segment
//! metadata); a callback that copies payloads must account for its own memory.

use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const HEADER: usize = 64;
const TRAILER: usize = 64;
/// Unwritten seal space that disk admission must reserve for the active segment.
/// Add this to `required_append_bytes` when admitting against physical disk usage.
pub const RESERVED_SEAL_BYTES: u64 = 64;
const SEAL: u64 = RESERVED_SEAL_BYTES;
const SEGMENT_MAGIC: &[u8; 8] = b"VRVJNL01";
const GROUP_MAGIC: &[u8; 8] = b"VRVGRP01";
const COMMIT_MAGIC: &[u8; 8] = b"VRVCMT01";
const SEAL_MAGIC: &[u8; 8] = b"VRVSEA01";
const VERSION: u32 = 1;

/// Logical encoded-byte bounds, not filesystem block quotas or process RSS limits.
#[derive(Clone, Copy, Debug)]
pub struct JournalConfig {
    /// Includes every segment header/frame/seal, including reserved active seal space.
    /// Does not include filesystem metadata or the zero-byte LOCK file.
    pub max_total_bytes: u64,
    pub segment_bytes: u64,
    pub max_segments: usize,
    /// Entire encoded group, including both headers and record length words.
    pub max_frame_bytes: usize,
    pub max_record_bytes: usize,
    pub max_records_per_group: usize,
}

impl Default for JournalConfig {
    fn default() -> Self {
        Self {
            max_total_bytes: 256 * 1024 * 1024,
            segment_bytes: 16 * 1024 * 1024,
            max_segments: 32,
            max_frame_bytes: 1024 * 1024,
            max_record_bytes: 512 * 1024,
            max_records_per_group: 4096,
        }
    }
}

impl JournalConfig {
    /// Hard implementation ceilings also apply to untrusted on-disk fields.
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (1..=65_536).contains(&self.max_segments),
            "invalid segment count bound"
        );
        ensure!(
            (1..=65_536).contains(&self.max_records_per_group),
            "invalid record count bound"
        );
        ensure!(
            (137..=16 * 1024 * 1024).contains(&self.max_frame_bytes),
            "invalid frame byte bound"
        );
        ensure!(
            self.max_record_bytes > 0 && self.max_record_bytes <= self.max_frame_bytes - 136,
            "record bound cannot fit a frame"
        );
        let minimum = self.max_frame_bytes as u64 + HEADER as u64 + SEAL;
        ensure!(
            self.segment_bytes >= minimum && self.segment_bytes <= 1024 * 1024 * 1024,
            "invalid segment byte bound"
        );
        ensure!(
            self.max_total_bytes >= minimum
                && self.max_total_bytes <= self.segment_bytes * self.max_segments as u64,
            "invalid total byte bound"
        );
        Ok(())
    }
}

/// Constructible only by a successful synced append. No visibility promise.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DurableGroup {
    first: u64,
    last: u64,
}

impl DurableGroup {
    pub fn first_sequence(&self) -> u64 {
        self.first
    }
    pub fn last_sequence(&self) -> u64 {
        self.last
    }
}

/// Borrowed, fully verified group. It cannot outlive its scan callback.
pub struct ReplayGroup<'a> {
    first: u64,
    count: u64,
    records: &'a [u8],
}

impl ReplayGroup<'_> {
    pub fn first_sequence(&self) -> u64 {
        self.first
    }
    pub fn last_sequence(&self) -> u64 {
        self.first + self.count - 1
    }
    pub fn records(&self) -> impl Iterator<Item = (u64, &[u8])> {
        let mut rest = self.records;
        (self.first..self.first + self.count).map(move |sequence| {
            // The complete group's lengths were checked before constructing this view.
            let len = word(rest, 0) as usize;
            let payload = &rest[8..8 + len];
            rest = &rest[8 + len..];
            (sequence, payload)
        })
    }
}

/// Counters are per-open, successful operations only (not inferred hardware I/O).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct JournalStats {
    pub file_syncs: u64,
    pub namespace_barriers: u64,
    pub bytes_written: u64,
    pub groups_appended: u64,
    pub records_appended: u64,
    pub recovered_records: u64,
    pub tail_bytes_discarded: u64,
    pub segments: usize,
    /// Last successfully installed encoded-byte accounting; additionally reserves
    /// an active seal during admission. May undercount physical bytes when fenced.
    pub disk_bytes: u64,
    pub durable_sequence: u64,
    pub fenced: bool,
}

/// Immutable upload identity relative to the journal directory.
/// Descriptors do not pin files: the owner must coordinate upload and reclamation.
/// Empty segments have no record range and are omitted from snapshots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealedSegment {
    pub relative_path: PathBuf,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub bytes: u64,
}

/// Called immediately before the named I/O. An integration can acquire disk
/// admission for mutation phases and release it for sync phases. The caller must
/// keep exclusive journal ownership throughout; observers must not re-enter it.
/// Observer errors during a mutating operation fence just like I/O errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JournalIoPhase {
    BeforeWrite,
    BeforeNamespaceChange,
    FileSync,
    DirectorySync,
}

struct AppendPlan {
    size: usize,
    next: u64,
    rotate: bool,
    growth: u64,
}

struct Segment {
    id: u64,
    len: u64,
    first: u64,
    // An empty segment has last == first - 1.
    last: u64,
}

struct Active {
    file: File,
    len: u64,
    hash: blake3::Hasher,
}

/// Deliberately not Clone: append/rotation requires the single mutable owner.
pub struct Journal {
    root: PathBuf,
    config: JournalConfig,
    _lock: File,
    segments: Vec<Segment>,
    active: Option<Active>,
    next_sequence: u64,
    next_segment_id: u64,
    // Process-local copy of caller authority, never an independently durable root.
    checkpoint_sequence: u64,
    stats: JournalStats,
    fault: Fault,
}

impl Drop for Journal {
    fn drop(&mut self) {
        // Close our writable handle before another logical owner can acquire
        // the directory. Teardown must not seal, sync or acknowledge new data.
        drop(self.active.take());
        // Duplicated or fork-inherited descriptions can outlive this owner.
        // Explicitly release its lock; on error, descriptor close is the fallback.
        let _ = FileExt::unlock(&self._lock);
    }
}

// Instance-local injection, absent in production; never global environment state.
#[derive(Default)]
struct Fault {
    #[cfg(test)]
    next: Option<FailAt>,
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum FailAt {
    Write,
    Sync,
    Namespace,
    Remove,
}

impl Journal {
    /// Config is validated before any filesystem operation. Parent must exist.
    /// Opening an existing directory with tighter bounds can reject existing data.
    pub fn open(path: impl AsRef<Path>, config: JournalConfig) -> Result<Self> {
        Self::open_with_checkpoint(path, config, 0)
    }

    /// The caller must have loaded and verified a durable checkpoint root and all
    /// its dependencies through `checkpoint_sequence`. This is the sole authority
    /// for missing covered history; every retained byte is still validated.
    /// Scan includes retained records at/below the checkpoint, so replay consumers
    /// must skip those records (a group can straddle the checkpoint).
    /// An empty directory cannot detect a stale root: always supply the current
    /// authoritative root, including after reclaiming every segment.
    pub fn open_with_checkpoint(
        path: impl AsRef<Path>,
        config: JournalConfig,
        checkpoint_sequence: u64,
    ) -> Result<Self> {
        config.validate()?;
        let floor_next = checkpoint_sequence
            .checked_add(1)
            .context("checkpoint sequence exhausted")?;
        let path = path.as_ref();
        match fs::symlink_metadata(path) {
            Ok(meta) => ensure!(meta.is_dir(), "journal path must be a real directory"),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => fs::create_dir(path)?,
            Err(error) => return Err(error.into()),
        }
        // Pin an absolute name so later process cwd changes cannot redirect I/O.
        let root = fs::canonicalize(path)?;
        let path = root.as_path();
        let parent = path.parent().unwrap_or(path);
        let lock_path = path.join("LOCK");
        if let Ok(meta) = fs::symlink_metadata(&lock_path) {
            ensure!(meta.is_file() && meta.len() == 0, "invalid journal LOCK");
        }
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)?;
        FileExt::try_lock_exclusive(&lock).context("journal directory already owned")?;
        let mut journal = Self {
            root: path.to_path_buf(),
            config,
            _lock: lock,
            segments: Vec::new(),
            active: None,
            next_sequence: floor_next,
            next_segment_id: 1,
            checkpoint_sequence,
            stats: JournalStats::default(),
            fault: Fault::default(),
        };
        // Admit all file sizes/counts before any frame reads. Unknown files include
        // legacy database layout: this is intentionally not a silent migration.
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            ensure!(kind.is_file(), "non-file in journal directory");
            let name = entry.file_name();
            if name == "LOCK" {
                continue;
            }
            ensure!(
                journal.segments.len() < config.max_segments,
                "segment count admission exceeded"
            );
            let name = name.to_str().context("non-UTF8 journal filename")?;
            let id = parse_name(name)?;
            let len = entry.metadata()?.len();
            ensure!(
                len <= config.segment_bytes,
                "segment byte admission exceeded"
            );
            journal.stats.disk_bytes = journal
                .stats
                .disk_bytes
                .checked_add(len)
                .context("disk byte overflow")?;
            ensure!(
                journal.stats.disk_bytes <= config.max_total_bytes,
                "disk admission exceeded"
            );
            journal.segments.push(Segment {
                id,
                len,
                first: 0,
                last: 0,
            });
        }
        journal.segments.sort_by_key(|s| s.id);
        let mut last_end = None;
        let mut previous_id = 0;
        let mut previous_next = 1;
        for index in 0..journal.segments.len() {
            let segment = &journal.segments[index];
            let segment_path = journal.segment_path(segment.id);
            // Read only enough to locate the first sequence. inspect below then
            // authenticates the entire header and every retained frame/seal.
            let mut prefix = [0u8; 32];
            let n = segment.len.min(prefix.len() as u64) as usize;
            File::open(&segment_path)?.read_exact(&mut prefix[..n])?;
            let first = if n == prefix.len() {
                word(&prefix, 24)
            } else {
                let contiguous = if index == 0 && segment.id != 1 {
                    floor_next
                } else {
                    previous_next
                };
                if prefix[..n] == segment_header(segment.id, contiguous)[..n] {
                    contiguous
                } else {
                    // A root can cover a missing suffix of an older retained
                    // segment. A newly created successor then starts at C + 1.
                    floor_next
                }
            };
            ensure!(first > 0, "zero record sequence");
            ensure!(
                if index == 0 {
                    first <= floor_next
                } else {
                    first >= previous_next && (first == previous_next || first <= floor_next)
                },
                "record sequence gap above checkpoint or overlapping history"
            );
            ensure!(
                segment.id == previous_id + 1
                    || (checkpoint_sequence > 0
                        && previous_next <= floor_next
                        && first <= floor_next),
                "segment ID gap above checkpoint"
            );
            let end = inspect(
                &segment_path,
                config,
                segment,
                first,
                index + 1 == journal.segments.len(),
                &mut |_| Ok(()),
            )?;
            ensure!(
                index + 1 == journal.segments.len() || end.sealed,
                "unsealed interior segment"
            );
            previous_id = segment.id;
            previous_next = end.next;
            journal.stats.recovered_records += end.next - first;
            journal.segments[index].first = first;
            journal.segments[index].last = end.next - 1;
            journal.next_sequence = end.next.max(floor_next);
            last_end = Some(end);
        }
        journal.next_segment_id = previous_id.checked_add(1).context("segment ID exhausted")?;
        // Full validation precedes all tail repair. Reserve seal space before repair.
        if let Some(end) = last_end {
            let segment = journal.segments.last_mut().expect("scanned segment");
            if !end.sealed {
                let repaired_total = journal.stats.disk_bytes - segment.len + end.valid_len;
                ensure!(
                    repaired_total
                        .checked_add(SEAL)
                        .is_some_and(|n| n <= config.max_total_bytes),
                    "no disk budget for recovered active segment"
                );
                ensure!(
                    end.valid_len + SEAL <= config.segment_bytes,
                    "no segment seal reserve"
                );
                let mut file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(path.join(segment_name(segment.id)))?;
                if end.repair_header {
                    let header = segment_header(segment.id, segment.first);
                    write_file(&mut file, &header, &mut journal.stats, &mut journal.fault)?;
                } else if segment.len != end.valid_len {
                    file.set_len(end.valid_len)?;
                    journal.stats.tail_bytes_discarded = segment.len - end.valid_len;
                }
                segment.len = end.valid_len;
                journal.stats.disk_bytes = repaired_total;
                file.seek(SeekFrom::Start(end.valid_len))?;
                journal.active = Some(Active {
                    file,
                    len: end.valid_len,
                    hash: end.hash,
                });
            }
        }
        // Re-establish durability even after an earlier process's ambiguous sync.
        // Sealed files are synced but never written. No payload is retained here.
        for segment in &journal.segments {
            let file = File::open(journal.segment_path(segment.id))?;
            sync_file(&file, &mut journal.stats, &mut journal.fault)?;
        }
        sync_file(&journal._lock, &mut journal.stats, &mut journal.fault)?;
        sync_namespace(path, &mut journal.stats, &mut journal.fault)?;
        // Also makes a newly created root durable, including retry after failed open.
        sync_namespace(parent, &mut journal.stats, &mut journal.fault)?;
        journal.stats.segments = journal.segments.len();
        journal.stats.durable_sequence = journal.next_sequence - 1;
        Ok(journal)
    }

    pub fn stats(&self) -> JournalStats {
        self.stats
    }

    /// Exact append disk growth after shape/sequence validation. Includes a new
    /// header and old active seal on rotation, but not `RESERVED_SEAL_BYTES` for
    /// the successor. Does not reject current disk/segment capacity pressure: the
    /// caller can estimate admission and checkpoint before attempting append.
    /// No frame allocation, mutation, or I/O is performed.
    pub fn required_append_bytes(&self, records: &[&[u8]]) -> Result<u64> {
        Ok(self.append_plan(records, false)?.growth)
    }

    fn append_plan(&self, records: &[&[u8]], check_capacity: bool) -> Result<AppendPlan> {
        ensure!(!self.stats.fenced, "journal fenced; drop and reopen");
        ensure!(
            !records.is_empty() && records.len() <= self.config.max_records_per_group,
            "group record count admission exceeded"
        );
        let mut size = HEADER + TRAILER;
        for record in records {
            ensure!(
                record.len() <= self.config.max_record_bytes,
                "record byte admission exceeded"
            );
            size = size
                .checked_add(8)
                .and_then(|n| n.checked_add(record.len()))
                .context("frame byte overflow")?;
        }
        ensure!(
            size <= self.config.max_frame_bytes,
            "frame byte admission exceeded"
        );
        let next = self
            .next_sequence
            .checked_add(records.len() as u64)
            .context("record sequence exhausted")?;
        let rotate = self.active.as_ref().is_none_or(|a| {
            a.len + size as u64 + SEAL > self.config.segment_bytes
                || self.segments.last().expect("active metadata").last + 1 != self.next_sequence
        });
        if rotate {
            self.next_segment_id
                .checked_add(1)
                .context("segment ID exhausted")?;
        }
        let reserve = if rotate {
            HEADER as u64 + SEAL + if self.active.is_some() { SEAL } else { 0 }
        } else {
            SEAL
        };
        if check_capacity {
            ensure!(
                !rotate || self.segments.len() < self.config.max_segments,
                "segment count admission exceeded"
            );
            let admitted = self
                .stats
                .disk_bytes
                .checked_add(size as u64)
                .and_then(|n| n.checked_add(reserve))
                .context("disk byte overflow")?;
            ensure!(
                admitted <= self.config.max_total_bytes,
                "disk byte admission exceeded"
            );
        }
        Ok(AppendPlan {
            size,
            next,
            rotate,
            growth: size as u64 + reserve - SEAL,
        })
    }

    /// Validation/admission errors do not mutate or fence. Once mutating I/O
    /// starts, every error fences, including ambiguous creation and directory sync.
    pub fn append_group(&mut self, records: &[&[u8]]) -> Result<DurableGroup> {
        self.append_group_with_observer(records, |_| Ok(()))
    }

    /// Mutation callbacks precede even partial writes and namespace creation; sync
    /// callbacks precede the existing fault-injected fsync operations. No observer
    /// is called on a validation/admission rejection.
    pub fn append_group_with_observer(
        &mut self,
        records: &[&[u8]],
        mut observer: impl FnMut(JournalIoPhase) -> Result<()>,
    ) -> Result<DurableGroup> {
        let AppendPlan {
            size, next, rotate, ..
        } = self.append_plan(records, true)?;
        if rotate {
            self.segments
                .try_reserve_exact(1)
                .context("segment metadata allocation admission")?;
        }
        let frame = encode(records, size, self.next_sequence, next - 1)?;
        let receipt = DurableGroup {
            first: self.next_sequence,
            last: next - 1,
        };
        self.stats.fenced = true;
        if rotate {
            self.rotate(&mut observer)?;
        }
        let active = self.active.as_mut().expect("admitted active segment");
        observer(JournalIoPhase::BeforeWrite)?;
        write_file(&mut active.file, &frame, &mut self.stats, &mut self.fault)?;
        observer(JournalIoPhase::FileSync)?;
        sync_file(&active.file, &mut self.stats, &mut self.fault)?;
        active.hash.update(&frame);
        active.len += frame.len() as u64;
        let segment = self.segments.last_mut().expect("active segment metadata");
        segment.len = active.len;
        segment.last = next - 1;
        self.stats.disk_bytes += frame.len() as u64;
        self.next_sequence = next;
        self.stats.durable_sequence = next - 1;
        self.stats.groups_appended += 1;
        self.stats.records_appended += records.len() as u64;
        self.stats.fenced = false;
        Ok(receipt)
    }

    /// Calls back only with complete verified groups, in contiguous order. Errors
    /// stop the scan; earlier callbacks are not undone. A consumer must publish
    /// atomically per group and not treat a partially completed scan as success.
    pub fn scan(&self, mut visit: impl FnMut(ReplayGroup<'_>) -> Result<()>) -> Result<()> {
        ensure!(!self.stats.fenced, "journal fenced; drop and reopen");
        let mut next = self.checkpoint_sequence + 1;
        for (index, segment) in self.segments.iter().enumerate() {
            let end = inspect(
                &self.segment_path(segment.id),
                self.config,
                segment,
                segment.first,
                false,
                &mut visit,
            )?;
            ensure!(
                index + 1 == self.segments.len() || end.sealed,
                "unsealed interior segment"
            );
            ensure!(end.next == segment.last + 1, "segment frontier changed");
            next = end.next.max(self.checkpoint_sequence + 1);
        }
        ensure!(next == self.next_sequence, "scan frontier changed");
        Ok(())
    }

    /// Seal the active file and return all retained nonempty immutable segments.
    /// No successor is created until the next append. Creation already synced the
    /// name, so sealing needs only a file sync, not another namespace transaction.
    /// The caller must prevent reclamation while descriptors are being uploaded.
    pub fn seal_snapshot(&mut self) -> Result<Vec<SealedSegment>> {
        self.seal_snapshot_with_observer(|_| Ok(()))
    }

    /// Snapshot sealing with the same disk-admission handoff as observed append.
    pub fn seal_snapshot_with_observer(
        &mut self,
        mut observer: impl FnMut(JournalIoPhase) -> Result<()>,
    ) -> Result<Vec<SealedSegment>> {
        ensure!(!self.stats.fenced, "journal fenced; drop and reopen");
        let mut snapshot = Vec::new();
        snapshot
            .try_reserve_exact(self.segments.len())
            .context("snapshot metadata allocation admission")?;
        for (index, segment) in self.segments.iter().enumerate() {
            if segment.last < segment.first {
                continue;
            }
            let active = self.active.is_some() && index + 1 == self.segments.len();
            snapshot.push(SealedSegment {
                relative_path: PathBuf::from(segment_name(segment.id)),
                first_sequence: segment.first,
                last_sequence: segment.last,
                bytes: segment.len + if active { SEAL } else { 0 },
            });
        }
        if self.active.is_some() {
            self.stats.fenced = true;
            self.seal_active(&mut observer)?;
            self.stats.fenced = false;
        }
        Ok(snapshot)
    }

    /// Retire only whole segments covered by an already durable checkpoint root.
    /// The caller must prove root/dependency durability BEFORE calling, and exclude
    /// uploads/readers using these paths. This method does not publish a root or
    /// establish checkpoint authority independently. Lower or future frontiers are
    /// rejected without I/O. A straddling segment is never rewritten or removed.
    /// Deletions are directory-synced before disk/count credits are returned; any
    /// ambiguous seal/unlink/sync failure fences every subsequent operation.
    pub fn reclaim_through(&mut self, checkpoint_sequence: u64) -> Result<()> {
        self.reclaim_through_with_observer(checkpoint_sequence, |_| Ok(()))
    }

    /// Retirement with mutation/sync handoff, including any active seal and each
    /// unlink. Disk credits are released only after the final directory barrier.
    pub fn reclaim_through_with_observer(
        &mut self,
        checkpoint_sequence: u64,
        mut observer: impl FnMut(JournalIoPhase) -> Result<()>,
    ) -> Result<()> {
        ensure!(!self.stats.fenced, "journal fenced; drop and reopen");
        ensure!(
            checkpoint_sequence >= self.checkpoint_sequence,
            "checkpoint authority regressed"
        );
        ensure!(
            checkpoint_sequence <= self.stats.durable_sequence,
            "checkpoint exceeds durable frontier"
        );
        let covered = self
            .segments
            .iter()
            .take_while(|segment| segment.last <= checkpoint_sequence)
            .count();
        // At floor zero ordinary open still requires IDs to begin at one. Keep
        // an empty prefix when a live tail follows it; deleting it would create
        // a gap without positive checkpoint authority. An entirely empty log can
        // be removed and restart its IDs after the deletion barrier succeeds.
        if covered == 0 || (checkpoint_sequence == 0 && covered < self.segments.len()) {
            self.checkpoint_sequence = checkpoint_sequence;
            return Ok(());
        }
        self.stats.fenced = true;
        if covered == self.segments.len() {
            // Close the handle before unlink, including on platforms that prohibit
            // deleting open files. Reserved seal bytes make this admission-safe.
            self.seal_active(&mut observer)?;
        }
        let mut retired_bytes = 0;
        // Back-to-front deletion leaves a contiguous prefix if retirement of an
        // entirely empty floor-zero log is interrupted. Positive root authority
        // also covers any temporary interior gap before a retained live tail.
        for segment in self.segments[..covered].iter().rev() {
            observer(JournalIoPhase::BeforeNamespaceChange)?;
            fs::remove_file(self.segment_path(segment.id))?;
            #[cfg(test)]
            if self.fault.next == Some(FailAt::Remove) {
                self.fault.next = None;
                anyhow::bail!("injected failure after segment unlink");
            }
            retired_bytes += segment.len;
        }
        observer(JournalIoPhase::DirectorySync)?;
        sync_namespace(&self.root, &mut self.stats, &mut self.fault)?;
        self.segments.drain(..covered);
        if checkpoint_sequence == 0 {
            // Only an entirely empty history can retire at floor zero. Preserve
            // ordinary open's strict initial ID on the next lazy creation.
            self.next_segment_id = 1;
        }
        self.stats.disk_bytes -= retired_bytes;
        self.stats.segments = self.segments.len();
        self.checkpoint_sequence = checkpoint_sequence;
        self.stats.fenced = false;
        Ok(())
    }

    fn segment_path(&self, id: u64) -> PathBuf {
        self.root.join(segment_name(id))
    }

    fn seal_active(
        &mut self,
        observer: &mut impl FnMut(JournalIoPhase) -> Result<()>,
    ) -> Result<()> {
        if let Some(mut active) = self.active.take() {
            let id = self.segments.last().expect("active segment").id;
            let mut seal = [0u8; 64];
            seal[..8].copy_from_slice(SEAL_MAGIC);
            put(&mut seal, 8, id);
            let next = self.segments.last().expect("active segment").last + 1;
            put(&mut seal, 16, next);
            put(&mut seal, 24, active.len + SEAL);
            active.hash.update(&seal[..32]);
            seal[32..].copy_from_slice(active.hash.finalize().as_bytes());
            observer(JournalIoPhase::BeforeWrite)?;
            write_file(&mut active.file, &seal, &mut self.stats, &mut self.fault)?;
            observer(JournalIoPhase::FileSync)?;
            sync_file(&active.file, &mut self.stats, &mut self.fault)?;
            self.segments.last_mut().expect("sealed segment").len += SEAL;
            self.stats.disk_bytes += SEAL;
        }
        Ok(())
    }

    fn rotate(&mut self, observer: &mut impl FnMut(JournalIoPhase) -> Result<()>) -> Result<()> {
        self.seal_active(observer)?;
        let id = self.next_segment_id;
        let header = segment_header(id, self.next_sequence);
        observer(JournalIoPhase::BeforeNamespaceChange)?;
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(self.segment_path(id))?;
        observer(JournalIoPhase::BeforeWrite)?;
        write_file(&mut file, &header, &mut self.stats, &mut self.fault)?;
        observer(JournalIoPhase::FileSync)?;
        sync_file(&file, &mut self.stats, &mut self.fault)?;
        observer(JournalIoPhase::DirectorySync)?;
        sync_namespace(&self.root, &mut self.stats, &mut self.fault)?;
        let mut hash = blake3::Hasher::new();
        hash.update(&header);
        self.active = Some(Active {
            file,
            len: HEADER as u64,
            hash,
        });
        self.segments.push(Segment {
            id,
            len: HEADER as u64,
            first: self.next_sequence,
            last: self.next_sequence - 1,
        });
        self.next_segment_id += 1;
        self.stats.disk_bytes += HEADER as u64;
        self.stats.segments = self.segments.len();
        Ok(())
    }
}

/// Verify one pinned immutable segment without journal ownership or tail repair.
/// Visitors may run before the final seal is checked: publish their effects only
/// after this function returns Ok. The caller must preserve the path identity.
pub fn validate_sealed_segment(
    path: impl AsRef<Path>,
    config: JournalConfig,
    expected: &SealedSegment,
    mut visit: impl FnMut(ReplayGroup<'_>) -> Result<()>,
) -> Result<()> {
    config.validate()?;
    let path = path.as_ref();
    ensure!(
        expected.relative_path.components().count() == 1,
        "segment descriptor must be one relative filename"
    );
    let name = expected
        .relative_path
        .to_str()
        .context("non-UTF8 segment descriptor")?;
    let id = parse_name(name)?;
    ensure!(
        path.file_name() == expected.relative_path.file_name(),
        "segment path/descriptor identity mismatch"
    );
    ensure!(
        expected.first_sequence > 0 && expected.last_sequence >= expected.first_sequence,
        "invalid sealed record range"
    );
    let next = expected
        .last_sequence
        .checked_add(1)
        .context("sealed range overflow")?;
    ensure!(
        expected.bytes >= HEADER as u64 + SEAL
            && expected.bytes <= config.segment_bytes
            && expected.bytes <= config.max_total_bytes,
        "sealed segment byte bounds"
    );
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_file()
            && !metadata.file_type().is_symlink()
            && metadata.len() == expected.bytes,
        "sealed segment file/size mismatch"
    );
    let segment = Segment {
        id,
        len: expected.bytes,
        first: expected.first_sequence,
        last: expected.last_sequence,
    };
    let end = inspect(
        path,
        config,
        &segment,
        expected.first_sequence,
        false,
        &mut visit,
    )?;
    ensure!(
        end.sealed && !end.repair_header && end.valid_len == expected.bytes && end.next == next,
        "sealed segment descriptor/range mismatch"
    );
    Ok(())
}

/// Validate the exact object buffer that will be uploaded. Uses the same bounded
/// parser as local recovery, with no repair; do not reopen a path after validation.
pub fn validate_sealed_bytes(
    bytes: &[u8],
    config: JournalConfig,
    expected: &SealedSegment,
    mut visit: impl FnMut(ReplayGroup<'_>) -> Result<()>,
) -> Result<()> {
    config.validate()?;
    ensure!(
        expected.relative_path.components().count() == 1,
        "segment descriptor must be one relative filename"
    );
    let id = parse_name(
        expected
            .relative_path
            .to_str()
            .context("non-UTF8 segment descriptor")?,
    )?;
    ensure!(
        expected.first_sequence > 0 && expected.last_sequence >= expected.first_sequence,
        "invalid sealed record range"
    );
    let next = expected
        .last_sequence
        .checked_add(1)
        .context("sealed range overflow")?;
    ensure!(
        expected.bytes == bytes.len() as u64
            && expected.bytes >= HEADER as u64 + SEAL
            && expected.bytes <= config.segment_bytes
            && expected.bytes <= config.max_total_bytes,
        "sealed buffer byte bounds"
    );
    let segment = Segment {
        id,
        len: expected.bytes,
        first: expected.first_sequence,
        last: expected.last_sequence,
    };
    let end = inspect_reader(
        bytes,
        config,
        &segment,
        expected.first_sequence,
        false,
        &mut visit,
    )?;
    ensure!(
        end.sealed && !end.repair_header && end.valid_len == expected.bytes && end.next == next,
        "sealed segment descriptor/range mismatch"
    );
    Ok(())
}

fn segment_name(id: u64) -> String {
    format!("segment-{id:020}.jrn")
}

fn parse_name(name: &str) -> Result<u64> {
    let id: u64 = name
        .strip_prefix("segment-")
        .and_then(|n| n.strip_suffix(".jrn"))
        .context("unknown journal file (explicit migration required)")?
        .parse()?;
    ensure!(
        id > 0 && name == segment_name(id),
        "noncanonical segment name"
    );
    Ok(id)
}

fn word(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("checked word extent"),
    )
}

fn put(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn segment_header(id: u64, first: u64) -> [u8; HEADER] {
    let mut header = [0u8; HEADER];
    header[..8].copy_from_slice(SEGMENT_MAGIC);
    header[8..12].copy_from_slice(&VERSION.to_le_bytes());
    put(&mut header, 16, id);
    put(&mut header, 24, first);
    let digest = blake3::hash(&header[..32]);
    header[32..].copy_from_slice(digest.as_bytes());
    header
}

fn encode(records: &[&[u8]], size: usize, first: u64, last: u64) -> Result<Vec<u8>> {
    let mut frame = Vec::new();
    frame
        .try_reserve_exact(size)
        .context("frame allocation admission")?;
    frame.resize(HEADER, 0);
    frame[..8].copy_from_slice(GROUP_MAGIC);
    put(&mut frame, 8, size as u64);
    put(&mut frame, 16, first);
    put(&mut frame, 24, records.len() as u64);
    let digest = blake3::hash(&frame[..32]);
    frame[32..HEADER].copy_from_slice(digest.as_bytes());
    for record in records {
        frame.extend_from_slice(&(record.len() as u64).to_le_bytes());
        frame.extend_from_slice(record);
    }
    let start = frame.len();
    frame.resize(size, 0);
    frame[start..start + 8].copy_from_slice(COMMIT_MAGIC);
    put(&mut frame, start + 8, first);
    put(&mut frame, start + 16, last);
    put(&mut frame, start + 24, size as u64);
    let digest = blake3::hash(&frame[..size - 32]);
    frame[size - 32..].copy_from_slice(digest.as_bytes());
    Ok(frame)
}

struct ScanEnd {
    next: u64,
    valid_len: u64,
    sealed: bool,
    repair_header: bool,
    hash: blake3::Hasher,
}

fn inspect(
    path: &Path,
    config: JournalConfig,
    segment: &Segment,
    first: u64,
    allow_tail: bool,
    visit: &mut impl FnMut(ReplayGroup<'_>) -> Result<()>,
) -> Result<ScanEnd> {
    let file = File::open(path)?;
    ensure!(
        file.metadata()?.len() == segment.len,
        "segment length changed during scan"
    );
    inspect_reader(file, config, segment, first, allow_tail, visit)
}

fn inspect_reader(
    mut file: impl Read,
    config: JournalConfig,
    segment: &Segment,
    first: u64,
    allow_tail: bool,
    visit: &mut impl FnMut(ReplayGroup<'_>) -> Result<()>,
) -> Result<ScanEnd> {
    let expected = segment_header(segment.id, first);
    let mut header = [0u8; HEADER];
    let n = segment.len.min(HEADER as u64) as usize;
    file.read_exact(&mut header[..n])?;
    ensure!(
        header[..n] == expected[..n],
        "segment header/version/sequence/checksum mismatch"
    );
    let mut hash = blake3::Hasher::new();
    hash.update(&expected);
    if n < HEADER {
        ensure!(allow_tail, "incomplete interior segment header");
        return Ok(ScanEnd {
            next: first,
            valid_len: HEADER as u64,
            sealed: false,
            repair_header: true,
            hash,
        });
    }
    let mut next = first;
    let mut pos = HEADER as u64;
    while pos < segment.len {
        let remaining = segment.len - pos;
        if remaining < HEADER as u64 {
            ensure!(allow_tail, "incomplete interior group/seal header");
            break;
        }
        file.read_exact(&mut header)?;
        if &header[..8] == SEAL_MAGIC {
            ensure!(
                word(&header, 8) == segment.id
                    && word(&header, 16) == next
                    && word(&header, 24) == pos + SEAL
                    && pos + SEAL == segment.len,
                "invalid segment seal extent/sequence"
            );
            hash.update(&header[..32]);
            ensure!(
                hash.finalize().as_bytes() == &header[32..],
                "segment seal checksum mismatch"
            );
            return Ok(ScanEnd {
                next,
                valid_len: segment.len,
                sealed: true,
                repair_header: false,
                hash,
            });
        }
        ensure!(&header[..8] == GROUP_MAGIC, "unknown group format");
        ensure!(
            blake3::hash(&header[..32]).as_bytes() == &header[32..],
            "group header checksum mismatch"
        );
        let size = word(&header, 8);
        let group_first = word(&header, 16);
        let count = word(&header, 24);
        ensure!(
            count > 0 && count <= config.max_records_per_group as u64,
            "group record count bound"
        );
        ensure!(
            size >= (HEADER + TRAILER) as u64 + count * 8 && size <= config.max_frame_bytes as u64,
            "group frame byte bound"
        );
        ensure!(group_first == next, "record sequence gap");
        let after = next
            .checked_add(count)
            .context("record sequence overflow")?;
        ensure!(
            pos + size + SEAL <= config.segment_bytes,
            "group exceeds segment seal reserve"
        );
        if remaining < size {
            ensure!(allow_tail, "incomplete interior group");
            break;
        }
        // Header checksum and bounds are verified before payload I/O/allocation.
        let size = size as usize;
        let mut frame = Vec::new();
        frame
            .try_reserve_exact(size)
            .context("replay frame allocation admission")?;
        frame.resize(size, 0);
        frame[..HEADER].copy_from_slice(&header);
        file.read_exact(&mut frame[HEADER..])?;
        let trailer = size - TRAILER;
        ensure!(
            &frame[trailer..trailer + 8] == COMMIT_MAGIC
                && word(&frame, trailer + 8) == next
                && word(&frame, trailer + 16) == after - 1
                && word(&frame, trailer + 24) == size as u64,
            "invalid complete commit trailer"
        );
        ensure!(
            blake3::hash(&frame[..size - 32]).as_bytes() == &frame[size - 32..],
            "complete group checksum mismatch"
        );
        let mut cursor = HEADER;
        for _ in 0..count {
            ensure!(cursor + 8 <= trailer, "missing record length");
            let len = word(&frame, cursor);
            ensure!(len <= config.max_record_bytes as u64, "record byte bound");
            cursor += 8;
            ensure!(
                len <= (trailer - cursor) as u64,
                "record crosses commit trailer"
            );
            cursor += len as usize;
        }
        ensure!(cursor == trailer, "group record count/extent mismatch");
        visit(ReplayGroup {
            first: next,
            count,
            records: &frame[HEADER..trailer],
        })?;
        hash.update(&frame);
        pos += size as u64;
        next = after;
    }
    // Callers require every interior segment to be sealed, even at a clean
    // group boundary. The final active segment may end without a seal.
    Ok(ScanEnd {
        next,
        valid_len: pos,
        sealed: false,
        repair_header: false,
        hash,
    })
}

fn write_file(
    file: &mut File,
    bytes: &[u8],
    stats: &mut JournalStats,
    _fault: &mut Fault,
) -> Result<()> {
    #[cfg(test)]
    if _fault.next == Some(FailAt::Write) {
        _fault.next = None;
        let partial = bytes.len().min(7);
        file.write_all(&bytes[..partial])?;
        stats.bytes_written += partial as u64;
        anyhow::bail!("injected partial append failure");
    }
    file.write_all(bytes)?;
    stats.bytes_written += bytes.len() as u64;
    Ok(())
}

fn sync_file(file: &File, stats: &mut JournalStats, _fault: &mut Fault) -> Result<()> {
    #[cfg(test)]
    if _fault.next == Some(FailAt::Sync) {
        _fault.next = None;
        anyhow::bail!("injected file sync failure");
    }
    file.sync_all()?;
    stats.file_syncs += 1;
    Ok(())
}

fn sync_namespace(path: &Path, stats: &mut JournalStats, _fault: &mut Fault) -> Result<()> {
    #[cfg(test)]
    if _fault.next == Some(FailAt::Namespace) {
        _fault.next = None;
        anyhow::bail!("injected directory sync failure");
    }
    File::open(path)?
        .sync_all()
        .with_context(|| format!("sync directory {}", path.display()))?;
    stats.namespace_barriers += 1;
    Ok(())
}

#[cfg(test)]
#[path = "journal_tests.rs"]
mod tests;
