use crate::model::{
    ContinuousAggregate, FORMAT_VERSION, JobDefinition, JobRun, LifecyclePolicy, RollupRow, Row,
    TableConfig,
};
use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"VARVEW01";
pub const MAX_FRAME_BYTES: usize = 128 * 1024 * 1024;
const RESERVATION_CHUNK_BYTES: usize = 64 * 1024;

pub struct ReservedFile {
    path: PathBuf,
    capacity: u64,
}

impl ReservedFile {
    pub fn open(path: PathBuf, capacity: u64) -> Result<Self> {
        let metadata = fs::metadata(&path)
            .with_context(|| format!("open reserved file {}", path.display()))?;
        ensure!(
            metadata.is_file() && metadata.len() > 0 && metadata.len() <= capacity,
            "reserved file has unexpected size"
        );
        Ok(Self {
            path,
            capacity: metadata.len(),
        })
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }
}

pub fn reserve_file(path: &Path, bytes: u64, failpoint_name: &str) -> Result<ReservedFile> {
    ensure!(bytes > 0, "reservation must contain at least one byte");
    let parent = path.parent().context("reserved file requires a parent")?;
    fs::create_dir_all(parent)?;
    injected_io(&format!("{failpoint_name}_before_create"))?;
    let mut created = false;
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)?;
        created = true;
        injected_io(&format!("{failpoint_name}_before_allocate"))?;
        file.allocate(bytes)?;
        let zeros = [0u8; RESERVATION_CHUNK_BYTES];
        let mut remaining = bytes;
        while remaining > 0 {
            injected_io(&format!("{failpoint_name}_during_write"))?;
            let chunk = usize::try_from(remaining.min(zeros.len() as u64))?;
            file.write_all(&zeros[..chunk])?;
            remaining -= chunk as u64;
        }
        injected_io(&format!("{failpoint_name}_before_sync"))?;
        file.sync_all()?;
        sync_dir(parent)?;
        Ok(ReservedFile {
            path: path.to_path_buf(),
            capacity: bytes,
        })
    })();
    if result.is_err() && created {
        let _ = fs::remove_file(path);
    }
    result
}

pub fn publish_reserved(
    mut reservation: ReservedFile,
    destination: &Path,
    bytes: &[u8],
    failpoint_name: &str,
) -> Result<()> {
    ensure!(
        bytes.len() as u64 <= reservation.capacity,
        "reserved file is smaller than publication"
    );
    ensure!(
        destination.parent() == reservation.path.parent(),
        "reserved publication must stay on one filesystem"
    );
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&reservation.path)?;
    injected_io(&format!("{failpoint_name}_before_write"))?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(bytes)?;
    injected_io(&format!("{failpoint_name}_before_reservation_sync"))?;
    file.sync_all()?;
    file.set_len(bytes.len() as u64)?;
    injected_io(&format!("{failpoint_name}_before_sync"))?;
    file.sync_all()?;
    drop(file);
    injected_io(&format!("{failpoint_name}_before_rename"))?;
    fs::rename(&reservation.path, destination)?;
    reservation.path = destination.to_path_buf();
    sync_dir(
        destination
            .parent()
            .context("publication requires a parent")?,
    )?;
    Ok(())
}

pub fn injected_io(name: &str) -> std::io::Result<()> {
    #[cfg(feature = "fault-injection")]
    if let Ok(configured) = std::env::var("VARVE_IO_FAILPOINT") {
        let (configured_name, kind) = configured
            .split_once(':')
            .map_or((configured.as_str(), "io"), |(name, kind)| (name, kind));
        if configured_name == name {
            return Err(match kind {
                "enospc" => std::io::Error::from_raw_os_error(28),
                _ => std::io::Error::other(format!("injected I/O failure at {name}")),
            });
        }
    }
    let _ = name;
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum Operation {
    CreateTable {
        name: String,
        config: TableConfig,
    },
    Append {
        table: String,
        request_id: String,
        digest: String,
        rows: Vec<Row>,
    },
    AppendGroup {
        items: Vec<AppendItem>,
    },
    SetPolicy {
        table: String,
        policy: LifecyclePolicy,
        stamp: String,
    },
    CreateContinuousAggregate {
        aggregate: ContinuousAggregate,
        backfill: BTreeMap<String, RollupRow>,
        stamp: String,
    },
    DropContinuousAggregate {
        name: String,
        stamp: String,
    },
    PutJob {
        job: JobDefinition,
        stamp: String,
    },
    DropJob {
        name: String,
        stamp: String,
    },
    JobStarted {
        name: String,
        run: JobRun,
        manual: bool,
    },
    JobFinished {
        name: String,
        run: JobRun,
        next_run_us: i64,
    },
}
// Format-level bounds are independent of the ingestion worker configuration.
pub const MAX_GROUP_REQUESTS: usize = 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppendItem {
    pub table: String,
    pub request_id: String,
    pub digest: String,
    pub rows: Vec<Row>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub now_us: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub format_version: u32,
    pub sequence: u64,
    pub operation: Operation,
}
impl Record {
    pub fn new(sequence: u64, operation: Operation) -> Self {
        Self {
            format_version: FORMAT_VERSION,
            sequence,
            operation,
        }
    }
}

/// Hash the entire logical record without allocating another encoded group.
/// Typed JSON preserves item/row order and includes sequence, membership and clocks.
/// Absent legacy clocks stay absent when re-encoded.
pub(crate) fn group_fingerprint(record: &Record) -> Result<Option<String>> {
    if !matches!(record.operation, Operation::AppendGroup { .. }) {
        return Ok(None);
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"varve/append-group-proof/v1\0");
    serde_json::to_writer(&mut hasher, record)?;
    Ok(Some(hasher.finalize().to_hex().to_string()))
}

/// Immutable encoded bytes shared by admission and publication. Construction binds
/// the sequence to the frame; callers cannot replace bytes after validation.
pub(crate) struct EncodedRecord {
    sequence: u64,
    bytes: Vec<u8>,
}

impl EncodedRecord {
    pub(crate) fn new(record: &Record) -> Result<Self> {
        Ok(Self {
            sequence: record.sequence,
            bytes: encode(record)?,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

pub fn encode(record: &Record) -> Result<Vec<u8>> {
    encode_with_limit(record, MAX_FRAME_BYTES)
}

fn encode_with_limit(record: &Record, limit: usize) -> Result<Vec<u8>> {
    ensure!(limit >= 48, "WAL frame exceeds format limit");
    let mut frame = Vec::with_capacity(limit.min(4096));
    frame.extend_from_slice(MAGIC);
    frame.extend_from_slice(&0u64.to_le_bytes());
    // Serialize directly into the bounded final frame instead of retaining a
    // second full payload allocation. The byte format remains VARVEW01.
    let mut writer = FrameWriter {
        bytes: &mut frame,
        limit: limit - 32,
    };
    serde_json::to_writer(&mut writer, record)?;
    let payload_len = (frame.len() - 16) as u64;
    frame[8..16].copy_from_slice(&payload_len.to_le_bytes());
    let hash = blake3::hash(&frame);
    frame.extend_from_slice(hash.as_bytes());
    Ok(frame)
}

struct FrameWriter<'a> {
    bytes: &'a mut Vec<u8>,
    limit: usize,
}

impl Write for FrameWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("WAL frame exceeds format limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub fn decode(bytes: &[u8]) -> Result<Record> {
    ensure!(
        bytes.len() >= 48 && bytes.len() <= MAX_FRAME_BYTES,
        "invalid WAL frame size"
    );
    ensure!(&bytes[..8] == MAGIC, "invalid WAL magic/version");
    let length = u64::from_le_bytes(bytes[8..16].try_into()?);
    ensure!(
        length == (bytes.len() - 48) as u64,
        "truncated or trailing WAL bytes"
    );
    let (content, digest) = bytes.split_at(bytes.len() - 32);
    ensure!(
        blake3::hash(content).as_bytes() == digest,
        "WAL checksum mismatch"
    );
    let record: Record = serde_json::from_slice(&bytes[16..bytes.len() - 32])?;
    ensure!(
        record.format_version == FORMAT_VERSION && record.sequence > 0,
        "unsupported WAL version or sequence"
    );
    Ok(record)
}

pub fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)?
        .sync_all()
        .context("directory fsync failed")
}

pub fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().context("atomic file requires a parent")?;
    fs::create_dir_all(parent)?;
    let target = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("unknown");
    injected_io(&format!("atomic_{target}_before_create"))?;
    let temp = parent.join(format!(".{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp)?;
        injected_io(&format!("atomic_{target}_before_write"))?;
        file.write_all(bytes)?;
        injected_io(&format!("atomic_{target}_before_sync"))?;
        file.sync_all()?;
        injected_io(&format!("atomic_{target}_before_rename"))?;
        fs::rename(&temp, path)?;
        injected_io(&format!("atomic_{target}_before_dir_sync"))?;
        sync_dir(parent)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

pub fn path(root: &Path, sequence: u64) -> PathBuf {
    root.join("wal").join(format!("{sequence:020}.wal"))
}

#[cfg(test)]
pub fn append(root: &Path, record: &Record) -> Result<usize> {
    append_encoded(root, &EncodedRecord::new(record)?, None)
}

pub(crate) fn append_encoded(
    root: &Path,
    encoded: &EncodedRecord,
    metrics: Option<&crate::metrics::Metrics>,
) -> Result<usize> {
    use crate::metrics::Phase;
    let _publication = metrics.map(|metrics| metrics.timer(Phase::WalWrite));
    let final_path = path(root, encoded.sequence);
    ensure!(!final_path.exists(), "WAL sequence already exists");
    let parent = final_path.parent().unwrap();
    let temp = parent.join(format!(".{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        injected_io("wal_before_write")?;
        file.write_all(encoded.as_bytes())?;
        injected_io("wal_before_sync")?;
        {
            let _sync = metrics.map(|metrics| metrics.timer(Phase::WalSync));
            file.sync_all()?;
        }
        failpoint("wal_synced");
        // The process lock excludes competing local publishers.
        injected_io("wal_before_rename")?;
        fs::rename(&temp, &final_path)?;
        injected_io("wal_before_dir_sync")?;
        {
            let _sync = metrics.map(|metrics| metrics.timer(Phase::WalSync));
            sync_dir(parent)?;
        }
        failpoint("wal_published");
        Ok(encoded.len())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temp);
    }
    result
}

pub struct Records {
    files: std::vec::IntoIter<(u64, PathBuf)>,
}

impl Iterator for Records {
    type Item = Result<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        self.files.next().map(|(sequence, path)| {
            let bytes = read_bounded(&path, MAX_FRAME_BYTES)?;
            let record = decode(&bytes)
                .with_context(|| format!("corrupt committed WAL {}", path.display()))?;
            ensure!(
                record.sequence == sequence,
                "WAL filename/payload sequence mismatch"
            );
            Ok(record)
        })
    }
}

pub fn records(root: &Path, after: u64, max_bytes: u64) -> Result<Records> {
    let mut files = Vec::new();
    let mut tail_bytes = 0u64;
    for entry in fs::read_dir(root.join("wal"))? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with('.') && name.ends_with(".tmp") {
            continue;
        }
        ensure!(
            name.len() == 24 && name.ends_with(".wal"),
            "unexpected WAL file {name}"
        );
        let sequence: u64 = name[..20].parse().context("invalid WAL filename")?;
        if sequence > after {
            let bytes = entry.metadata()?.len();
            ensure!(
                bytes > 0 && bytes <= MAX_FRAME_BYTES as u64,
                "invalid WAL frame size"
            );
            tail_bytes = tail_bytes
                .checked_add(bytes)
                .context("WAL tail size overflow")?;
            ensure!(
                tail_bytes <= max_bytes,
                "WAL recovery exceeds configured wal_max_bytes"
            );
            files.push((sequence, entry.path()));
        }
    }
    files.sort_by_key(|(sequence, _)| *sequence);
    let mut expected = after;
    for (sequence, _) in &files {
        expected = expected.checked_add(1).context("WAL sequence overflow")?;
        ensure!(
            *sequence == expected,
            "WAL sequence gap: expected {expected}, found {sequence}"
        );
    }
    Ok(Records {
        files: files.into_iter(),
    })
}

pub fn read_bounded(path: &Path, max: usize) -> Result<Vec<u8>> {
    let file = File::open(path)?;
    ensure!(
        file.metadata()?.len() <= max as u64,
        "file exceeds read budget: {}",
        path.display()
    );
    let mut bytes = Vec::new();
    file.take(max as u64 + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= max, "file grew beyond read budget");
    Ok(bytes)
}

pub fn failpoint(name: &str) {
    #[cfg(feature = "fault-injection")]
    if std::env::var("VARVE_FAILPOINT").ok().as_deref() == Some(name) {
        std::process::exit(86);
    }
    let _ = name;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_frame_encoding_is_byte_identical_to_legacy_layout() {
        let record = Record::new(
            9,
            Operation::CreateTable {
                name: "metrics_東京".into(),
                config: TableConfig::default(),
            },
        );
        let payload = serde_json::to_vec(&record).unwrap();
        let mut legacy = Vec::new();
        legacy.extend_from_slice(MAGIC);
        legacy.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        legacy.extend_from_slice(&payload);
        let hash = blake3::hash(&legacy);
        legacy.extend_from_slice(hash.as_bytes());
        let encoded = EncodedRecord::new(&record).unwrap();
        assert_eq!(encoded.as_bytes(), legacy);
        assert_eq!(encoded.len(), legacy.len());
        assert_eq!(encode_with_limit(&record, legacy.len()).unwrap(), legacy);
        assert!(encode_with_limit(&record, legacy.len() - 1).is_err());
        assert!(encode_with_limit(&record, 47).is_err());
    }

    #[test]
    fn preencoded_publication_preserves_replay_and_syncs() {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir(root.path().join("wal")).unwrap();
        let record = Record::new(
            1,
            Operation::CreateTable {
                name: "metrics".into(),
                config: TableConfig::default(),
            },
        );
        let encoded = EncodedRecord::new(&record).unwrap();
        let metrics = crate::metrics::Metrics::default();
        assert_eq!(
            append_encoded(root.path(), &encoded, Some(&metrics)).unwrap(),
            encoded.len()
        );
        let disk = fs::read(path(root.path(), 1)).unwrap();
        assert_eq!(disk, encoded.as_bytes());
        assert_eq!(decode(&disk).unwrap().sequence, 1);
        assert!(append_encoded(root.path(), &encoded, Some(&metrics)).is_err());
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.phases["wal_write"].count, 2);
        assert_eq!(snapshot.phases["wal_sync"].count, 2);
        assert_eq!(fs::read_dir(root.path().join("wal")).unwrap().count(), 1);
    }

    #[test]
    fn frame_integrity_and_versions() {
        let r = Record::new(
            1,
            Operation::CreateTable {
                name: "metrics".into(),
                config: TableConfig::default(),
            },
        );
        let bytes = encode(&r).unwrap();
        assert_eq!(decode(&bytes).unwrap().sequence, 1);
        let mut future = r.clone();
        future.format_version += 1;
        assert!(decode(&encode(&future).unwrap()).is_err());
        future.format_version = FORMAT_VERSION;
        future.sequence = 0;
        assert!(decode(&encode(&future).unwrap()).is_err());
        for i in [0, 8, 20, bytes.len() - 1] {
            let mut bad = bytes.clone();
            bad[i] ^= 1;
            assert!(decode(&bad).is_err());
        }
        for length in [0, 8, 16, bytes.len() - 1] {
            assert!(decode(&bytes[..length]).is_err());
        }
    }
}
