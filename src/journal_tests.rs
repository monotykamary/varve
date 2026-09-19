//! Acceptance ledger: execution remains Main's Railway responsibility; no local runs.
//! The original 22 tests and 23 lifecycle tests passed on Railway.
//! Read-only upload validation regressions are qualified separately by Main.
//! Added source coverage checks checkpoint
//! floors/gaps/corruption, whole-segment retirement and returned credits, immutable
//! snapshots, seal/unlink/sync fencing, exact physical WAL bytes/sequences, shared
//! append-growth admission, and mutation/sync observer handoff. These additions
//! retain runtime evidence separately; source review is not a passing result.
//! Fixtures stay below a few KiB and live under the test working directory, not
//! /tmp. No clocks, sleeps, global failpoints, external services or large fixtures.

use super::*;
use std::process::Command;
use tempfile::TempDir;

#[test]
fn read_only_sealed_validation_checks_descriptor_and_never_repairs() {
    let root = fixture();
    let mut journal = Journal::open(root.path(), config()).unwrap();
    journal.append_group(&[b"one", b"two"]).unwrap();
    let descriptor = journal.seal_snapshot().unwrap().pop().unwrap();
    let path = root.path().join(&descriptor.relative_path);
    let before = snapshot(root.path());
    let mut rows = Vec::new();
    validate_sealed_segment(&path, config(), &descriptor, |group| {
        rows.extend(
            group
                .records()
                .map(|(sequence, bytes)| (sequence, bytes.to_vec())),
        );
        Ok(())
    })
    .unwrap();
    assert_eq!(rows, vec![(1, b"one".to_vec()), (2, b"two".to_vec())]);
    let bytes = fs::read(&path).unwrap();
    validate_sealed_bytes(&bytes, config(), &descriptor, |_| Ok(())).unwrap();
    let mut corrupt = bytes.clone();
    corrupt[HEADER + HEADER + 8] ^= 1;
    assert!(validate_sealed_bytes(&corrupt, config(), &descriptor, |_| Ok(())).is_err());
    for mutation in 0..4 {
        let mut wrong = descriptor.clone();
        match mutation {
            0 => wrong.first_sequence += 1,
            1 => wrong.last_sequence += 1,
            2 => wrong.bytes -= 1,
            _ => wrong.relative_path = "../segment-00000000000000000001.jrn".into(),
        }
        assert!(validate_sealed_segment(&path, config(), &wrong, |_| Ok(())).is_err());
        assert_eq!(snapshot(root.path()), before);
    }
    let mut bytes = fs::read(&path).unwrap();
    bytes.pop();
    fs::write(&path, &bytes).unwrap();
    let mut truncated = descriptor.clone();
    truncated.bytes -= 1;
    assert!(validate_sealed_segment(&path, config(), &truncated, |_| Ok(())).is_err());
    assert_eq!(fs::read(&path).unwrap(), bytes);
}

#[test]
fn read_only_validation_rejects_clean_unsealed_and_complete_corrupt_files() {
    let root = fixture();
    let mut journal = Journal::open(root.path(), config()).unwrap();
    journal.append_group(&[b"one"]).unwrap();
    let path = root.path().join(segment_name(1));
    let before = fs::read(&path).unwrap();
    let descriptor = SealedSegment {
        relative_path: segment_name(1).into(),
        first_sequence: 1,
        last_sequence: 1,
        bytes: before.len() as u64,
    };
    assert!(validate_sealed_segment(&path, config(), &descriptor, |_| Ok(())).is_err());
    assert_eq!(fs::read(&path).unwrap(), before);
    let descriptor = journal.seal_snapshot().unwrap().pop().unwrap();
    let mut bytes = fs::read(&path).unwrap();
    bytes[HEADER + HEADER + 8] ^= 1;
    fs::write(&path, &bytes).unwrap();
    assert!(validate_sealed_segment(&path, config(), &descriptor, |_| Ok(())).is_err());
    assert_eq!(fs::read(&path).unwrap(), bytes);
}

fn fixture() -> TempDir {
    tempfile::Builder::new()
        .prefix(".journal-test-")
        .tempdir_in(std::env::current_dir().unwrap())
        .unwrap()
}

fn config() -> JournalConfig {
    JournalConfig {
        max_total_bytes: 4096,
        segment_bytes: 512,
        max_segments: 8,
        max_frame_bytes: 384,
        max_record_bytes: 200,
        max_records_per_group: 8,
    }
}

fn roomy() -> JournalConfig {
    JournalConfig {
        max_total_bytes: 32_768,
        segment_bytes: 4096,
        ..config()
    }
}

fn snapshot(path: &Path) -> Vec<(String, Vec<u8>)> {
    let mut files: Vec<_> = fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (
                entry.file_name().to_str().unwrap().to_owned(),
                fs::read(entry.path()).unwrap(),
            )
        })
        .collect();
    files.sort_by(|a, b| a.0.cmp(&b.0));
    files
}

fn replay(journal: &Journal) -> Vec<Vec<(u64, Vec<u8>)>> {
    // Only this tiny test oracle collects history. The production API does not.
    let mut groups = Vec::new();
    journal
        .scan(|group| {
            let rows: Vec<_> = group
                .records()
                .map(|(seq, data)| (seq, data.to_vec()))
                .collect();
            assert_eq!(rows.first().unwrap().0, group.first_sequence());
            assert_eq!(rows.last().unwrap().0, group.last_sequence());
            groups.push(rows);
            Ok(())
        })
        .unwrap();
    groups
}

#[test]
fn group_receipts_survive_reopen_and_keep_empty_records() {
    let dir = fixture();
    let mut journal = Journal::open(dir.path(), roomy()).unwrap();
    assert!(replay(&journal).is_empty());
    let a = journal.append_group(&[b"a", b"", b"bc"]).unwrap();
    let b = journal.append_group(&[b"def"]).unwrap();
    assert_eq!((a.first_sequence(), a.last_sequence()), (1, 3));
    assert_eq!((b.first_sequence(), b.last_sequence()), (4, 4));
    let expected = vec![
        vec![(1, b"a".to_vec()), (2, vec![]), (3, b"bc".to_vec())],
        vec![(4, b"def".to_vec())],
    ];
    assert_eq!(replay(&journal), expected);
    drop(journal);
    let mut journal = Journal::open(dir.path(), roomy()).unwrap();
    assert_eq!(journal.stats().recovered_records, 4);
    assert_eq!(journal.stats().durable_sequence, 4);
    assert_eq!(replay(&journal), expected);
    assert_eq!(
        journal.append_group(&[b"next"]).unwrap().first_sequence(),
        5
    );
}

#[test]
fn ordinary_groups_have_one_file_sync_and_zero_namespace_barriers() {
    let dir = fixture();
    let mut journal = Journal::open(dir.path(), roomy()).unwrap();
    let opening = journal.stats();
    journal.append_group(&[b"first"]).unwrap();
    let created = journal.stats();
    assert_eq!(created.file_syncs - opening.file_syncs, 2); // header + group
    assert_eq!(created.namespace_barriers - opening.namespace_barriers, 1);
    let names: Vec<_> = snapshot(dir.path())
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    for _ in 0..10 {
        journal.append_group(&[b"row", b"row2"]).unwrap();
    }
    let after = journal.stats();
    assert_eq!(after.file_syncs - created.file_syncs, 10);
    assert_eq!(after.namespace_barriers, created.namespace_barriers);
    assert_eq!(after.groups_appended - created.groups_appended, 10);
    assert_eq!(after.records_appended - created.records_appended, 20);
    assert_eq!(after.segments, 1);
    assert_eq!(
        snapshot(dir.path())
            .into_iter()
            .map(|(name, _)| name)
            .collect::<Vec<_>>(),
        names
    );
}

#[test]
fn rotation_seals_without_rewriting_and_respects_barriers() {
    let dir = fixture();
    let mut journal = Journal::open(dir.path(), config()).unwrap();
    journal.append_group(&[&[1; 200]]).unwrap();
    let first_path = dir.path().join(segment_name(1));
    let unsealed = fs::read(&first_path).unwrap();
    let before = journal.stats();
    journal.append_group(&[&[2; 200]]).unwrap();
    let after = journal.stats();
    assert_eq!(after.file_syncs - before.file_syncs, 3); // seal + header + group
    assert_eq!(after.namespace_barriers - before.namespace_barriers, 1);
    assert_eq!(after.segments, 2);
    let sealed = fs::read(&first_path).unwrap();
    assert_eq!(&sealed[..unsealed.len()], unsealed.as_slice());
    assert_eq!(&sealed[unsealed.len()..unsealed.len() + 8], SEAL_MAGIC);
    journal.append_group(&[&[3; 200]]).unwrap();
    assert_eq!(fs::read(&first_path).unwrap(), sealed);
    assert_eq!(journal.stats().disk_bytes, 464 + 464 + 400);
    drop(journal);
    let journal = Journal::open(dir.path(), config()).unwrap();
    assert_eq!(
        replay(&journal),
        vec![
            vec![(1, vec![1; 200])],
            vec![(2, vec![2; 200])],
            vec![(3, vec![3; 200])]
        ]
    );
    assert_eq!(fs::read(first_path).unwrap(), sealed);
}

#[test]
fn invalid_config_precedes_directory_creation() {
    let dir = fixture();
    let mut cases = Vec::new();
    let mut c = config();
    c.max_segments = 0;
    cases.push(c);
    let mut c = config();
    c.max_segments = 65_537;
    cases.push(c);
    let mut c = config();
    c.max_records_per_group = 0;
    cases.push(c);
    let mut c = config();
    c.max_records_per_group = 65_537;
    cases.push(c);
    let mut c = config();
    c.max_record_bytes = 0;
    cases.push(c);
    let mut c = config();
    c.max_record_bytes = 249;
    cases.push(c);
    let mut c = config();
    c.max_frame_bytes = usize::MAX;
    cases.push(c);
    let mut c = config();
    c.max_frame_bytes = 136;
    cases.push(c);
    let mut c = config();
    c.segment_bytes = 511;
    cases.push(c);
    let mut c = config();
    c.segment_bytes = u64::MAX;
    cases.push(c);
    let mut c = config();
    c.max_total_bytes = 511;
    cases.push(c);
    let mut c = config();
    c.max_total_bytes = u64::MAX;
    cases.push(c);
    for (index, c) in cases.into_iter().enumerate() {
        let root = dir.path().join(format!("invalid-{index}"));
        assert!(Journal::open(&root, c).is_err());
        assert!(!root.exists());
    }
}

#[test]
fn oversize_and_capacity_rejections_leave_bytes_counters_and_sequence_unchanged() {
    let dir = fixture();
    let mut journal = Journal::open(dir.path(), config()).unwrap();
    let disk = snapshot(dir.path());
    let stats = journal.stats();
    let empty: &[u8] = b"";
    for records in [
        vec![],
        vec![&[0; 201][..]],
        vec![&[0; 100][..]; 3],
        vec![empty; 9],
    ] {
        assert!(journal.append_group(&records).is_err());
        assert_eq!(journal.stats(), stats);
        assert_eq!(snapshot(dir.path()), disk);
    }
    assert_eq!(journal.append_group(&[b"ok"]).unwrap().first_sequence(), 1);
    drop(journal);

    for (max_segments, max_total_bytes) in [(8, 512), (1, 512)] {
        let dir = fixture();
        let c = JournalConfig {
            max_segments,
            max_total_bytes,
            ..config()
        };
        let mut journal = Journal::open(dir.path(), c).unwrap();
        journal.append_group(&[&[1; 200]]).unwrap();
        let before = snapshot(dir.path());
        let stats = journal.stats();
        assert!(journal.append_group(&[&[2; 200]]).is_err());
        assert_eq!(snapshot(dir.path()), before); // not even a seal was written
        assert_eq!(journal.stats(), stats);
        assert!(!journal.stats().fenced);
    }
}

#[test]
fn complete_corruption_at_every_byte_fails_closed_without_tail_repair() {
    let dir = fixture();
    let mut journal = Journal::open(dir.path(), roomy()).unwrap();
    journal.append_group(&[b"alpha", b"beta"]).unwrap();
    drop(journal);
    let path = dir.path().join(segment_name(1));
    let original = fs::read(&path).unwrap();
    for offset in 0..original.len() {
        let mut corrupt = original.clone();
        corrupt[offset] ^= 0x80;
        fs::write(&path, &corrupt).unwrap();
        assert!(
            Journal::open(dir.path(), roomy()).is_err(),
            "accepted byte {offset}"
        );
        assert_eq!(fs::read(&path).unwrap(), corrupt);
    }
}

#[test]
fn every_partial_last_group_recovers_all_or_none_and_reuses_no_committed_sequence() {
    let dir = fixture();
    let mut journal = Journal::open(dir.path(), roomy()).unwrap();
    journal.append_group(&[b"durable"]).unwrap();
    let prefix = fs::read(dir.path().join(segment_name(1))).unwrap();
    journal.append_group(&[b"one", b"two"]).unwrap();
    let complete = fs::read(dir.path().join(segment_name(1))).unwrap();
    let frame = &complete[prefix.len()..];
    drop(journal);
    // Physical truncation simulates an incomplete tail, not proof that arbitrary
    // hardware truncation after receipt can be distinguished from a crash append.
    for cut in 0..=frame.len() {
        let path = dir.path().join(segment_name(1));
        fs::write(&path, &complete[..prefix.len() + cut]).unwrap();
        let mut journal = Journal::open(dir.path(), roomy()).unwrap();
        let expected = if cut == frame.len() { 3 } else { 1 };
        assert_eq!(journal.stats().durable_sequence, expected, "cut {cut}");
        assert_eq!(replay(&journal).len(), if expected == 3 { 2 } else { 1 });
        assert_eq!(
            journal.stats().tail_bytes_discarded,
            if expected == 3 { 0 } else { cut as u64 }
        );
        assert_eq!(
            fs::metadata(&path).unwrap().len(),
            if expected == 3 {
                complete.len()
            } else {
                prefix.len()
            } as u64
        );
        assert_eq!(
            journal.append_group(&[b"retry"]).unwrap().first_sequence(),
            expected + 1
        );
        drop(journal);
    }
}

#[test]
fn partial_new_segment_header_is_repaired_only_at_physical_end() {
    let dir = fixture();
    let path = dir.path().join(segment_name(1));
    let header = segment_header(1, 1);
    for cut in 0..HEADER {
        fs::write(&path, &header[..cut]).unwrap();
        let mut journal = Journal::open(dir.path(), config()).unwrap();
        assert_eq!(fs::read(&path).unwrap(), header);
        assert_eq!(journal.stats().durable_sequence, 0);
        assert_eq!(journal.append_group(&[b"ok"]).unwrap().first_sequence(), 1);
        drop(journal);
    }
    fs::write(&path, b"wrong").unwrap();
    assert!(Journal::open(dir.path(), config()).is_err());
    fs::write(&path, &header[..7]).unwrap();
    fs::write(dir.path().join(segment_name(2)), segment_header(2, 1)).unwrap();
    assert!(Journal::open(dir.path(), config()).is_err());
    assert_eq!(fs::metadata(path).unwrap().len(), 7);
}

#[test]
fn sequence_gaps_duplicates_and_missing_segment_fail_closed() {
    for bad_first in [1, 3] {
        let dir = fixture();
        let mut journal = Journal::open(dir.path(), roomy()).unwrap();
        journal.append_group(&[b"first"]).unwrap();
        drop(journal);
        let path = dir.path().join(segment_name(1));
        let frame = encode(&[b"wrong sequence"], 128 + 8 + 14, bad_first, bad_first).unwrap();
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&frame)
            .unwrap();
        let before = fs::read(&path).unwrap();
        assert!(Journal::open(dir.path(), roomy()).is_err());
        assert_eq!(fs::read(&path).unwrap(), before);
    }
    for missing in [1, 2] {
        let dir = fixture();
        let mut journal = Journal::open(dir.path(), config()).unwrap();
        for _ in 0..3 {
            journal.append_group(&[&[1; 200]]).unwrap();
        }
        drop(journal);
        fs::remove_file(dir.path().join(segment_name(missing))).unwrap();
        assert!(Journal::open(dir.path(), config()).is_err());
    }
}

#[test]
fn incomplete_or_clean_unsealed_interior_and_corrupt_seals_are_rejected() {
    let dir = fixture();
    let mut journal = Journal::open(dir.path(), config()).unwrap();
    journal.append_group(&[&[1; 200]]).unwrap();
    journal.append_group(&[&[2; 200]]).unwrap();
    drop(journal);
    let path = dir.path().join(segment_name(1));
    let original = fs::read(&path).unwrap();
    for len in [7, 80, 399, 400, 401, 463] {
        fs::write(&path, &original[..len]).unwrap();
        assert!(
            Journal::open(dir.path(), config()).is_err(),
            "accepted interior length {len}"
        );
        assert_eq!(fs::metadata(&path).unwrap().len(), len as u64);
    }
    for offset in 400..original.len() {
        let mut corrupt = original.clone();
        corrupt[offset] ^= 1;
        fs::write(&path, &corrupt).unwrap();
        assert!(
            Journal::open(dir.path(), config()).is_err(),
            "accepted seal byte {offset}"
        );
    }
    // Bytes following a complete seal must not be treated as a discardable tail.
    let mut extra = original.clone();
    extra.push(0);
    fs::write(&path, extra).unwrap();
    fs::remove_file(dir.path().join(segment_name(2))).unwrap();
    assert!(Journal::open(dir.path(), config()).is_err());
}

#[test]
fn checksum_valid_untrusted_bounds_are_rejected_before_payload_read() {
    let dir = fixture();
    let path = dir.path().join(segment_name(1));
    for (size, count) in [(u64::MAX, 1), (385, 1), (136, u64::MAX), (136, 0), (128, 1)] {
        let mut header = [0u8; HEADER];
        header[..8].copy_from_slice(GROUP_MAGIC);
        put(&mut header, 8, size);
        put(&mut header, 16, 1);
        put(&mut header, 24, count);
        let digest = blake3::hash(&header[..32]);
        header[32..].copy_from_slice(digest.as_bytes());
        let mut bytes = segment_header(1, 1).to_vec();
        bytes.extend_from_slice(&header);
        fs::write(&path, &bytes).unwrap();
        assert!(Journal::open(dir.path(), config()).is_err());
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }
}

#[test]
fn checksum_valid_record_extent_and_count_mismatches_fail_closed() {
    let dir = fixture();
    let path = dir.path().join(segment_name(1));
    for bad_len in [0, 4, 201, u64::MAX] {
        let mut frame = encode(&[b"abc"], 139, 1, 1).unwrap();
        put(&mut frame, HEADER, bad_len);
        let end = frame.len() - 32;
        let digest = blake3::hash(&frame[..end]);
        frame[end..].copy_from_slice(digest.as_bytes());
        let mut bytes = segment_header(1, 1).to_vec();
        bytes.extend_from_slice(&frame);
        fs::write(&path, &bytes).unwrap();
        assert!(Journal::open(dir.path(), config()).is_err());
    }
}

#[test]
fn partial_write_fences_until_reopen_and_sync_ambiguity_can_recover_whole_group() {
    for fault in [FailAt::Write, FailAt::Sync] {
        let dir = fixture();
        let mut journal = Journal::open(dir.path(), roomy()).unwrap();
        journal.append_group(&[b"durable"]).unwrap();
        journal.fault.next = Some(fault);
        assert!(journal.append_group(&[b"a", b"b"]).is_err());
        assert!(journal.stats().fenced);
        assert_eq!(journal.stats().durable_sequence, 1);
        let disk = snapshot(dir.path());
        let stats = journal.stats();
        assert!(journal.append_group(&[b"forbidden"]).is_err());
        assert!(journal.scan(|_| Ok(())).is_err());
        assert_eq!(snapshot(dir.path()), disk);
        assert_eq!(journal.stats(), stats);
        drop(journal);
        let mut journal = Journal::open(dir.path(), roomy()).unwrap();
        let recovered = if fault == FailAt::Sync { 3 } else { 1 };
        assert_eq!(journal.stats().durable_sequence, recovered);
        assert_eq!(replay(&journal).len(), if recovered == 3 { 2 } else { 1 });
        assert_eq!(
            journal.append_group(&[b"after"]).unwrap().first_sequence(),
            recovered + 1
        );
    }
}

#[test]
fn rotation_write_sync_and_namespace_failures_fence_and_recover_prefix() {
    for fault in [FailAt::Write, FailAt::Sync, FailAt::Namespace] {
        let dir = fixture();
        let mut journal = Journal::open(dir.path(), config()).unwrap();
        journal.append_group(&[&[1; 200]]).unwrap();
        journal.fault.next = Some(fault);
        assert!(journal.append_group(&[&[2; 200]]).is_err());
        assert!(journal.stats().fenced);
        let before = snapshot(dir.path());
        assert!(journal.append_group(&[b"forbidden"]).is_err());
        assert_eq!(snapshot(dir.path()), before);
        drop(journal);
        let mut journal = Journal::open(dir.path(), config()).unwrap();
        assert_eq!(replay(&journal), vec![vec![(1, vec![1; 200])]]);
        assert_eq!(
            journal.append_group(&[&[2; 200]]).unwrap().first_sequence(),
            2
        );
        drop(journal);
        assert_eq!(
            Journal::open(dir.path(), config())
                .unwrap()
                .stats()
                .recovered_records,
            2
        );
    }
}

#[test]
fn failed_segment_creation_fences_even_without_a_group_write() {
    for fault in [FailAt::Write, FailAt::Sync, FailAt::Namespace] {
        let dir = fixture();
        let mut journal = Journal::open(dir.path(), config()).unwrap();
        journal.fault.next = Some(fault);
        assert!(journal.append_group(&[b"first"]).is_err());
        assert!(journal.stats().fenced);
        drop(journal);
        let mut journal = Journal::open(dir.path(), config()).unwrap();
        assert!(replay(&journal).is_empty());
        assert_eq!(
            journal.append_group(&[b"first"]).unwrap().first_sequence(),
            1
        );
    }
}

#[test]
fn directory_lock_excludes_handles_and_child_processes() {
    let dir = fixture();
    let journal = Journal::open(dir.path(), config()).unwrap();
    assert!(Journal::open(dir.path(), config()).is_err());
    run_ownership_child(dir.path(), true);
    drop(journal);
    run_ownership_child(dir.path(), false);
}

fn run_ownership_child(path: &Path, locked: bool) {
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "journal::tests::ownership_child", "--nocapture"])
        .env("VARVE_JOURNAL_CHILD_PATH", path)
        .env("VARVE_JOURNAL_CHILD_LOCKED", if locked { "1" } else { "0" })
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("JOURNAL_OWNERSHIP_CHECKED"),
        "child test was not registered: {}",
        String::from_utf8_lossy(&output.stdout)
    );
}

#[test]
fn ownership_child() {
    let Some(path) = std::env::var_os("VARVE_JOURNAL_CHILD_PATH") else {
        return;
    };
    let locked = std::env::var("VARVE_JOURNAL_CHILD_LOCKED").unwrap() == "1";
    let result = Journal::open(PathBuf::from(path), config());
    if locked {
        let error = result.err().expect("child acquired parent's directory");
        assert!(error.to_string().contains("already owned"), "{error:#}");
    } else {
        assert!(result.is_ok());
    }
    println!("JOURNAL_OWNERSHIP_CHECKED");
}

#[test]
fn scan_stops_at_callback_error_without_mutating_or_fencing() {
    let dir = fixture();
    let mut journal = Journal::open(dir.path(), roomy()).unwrap();
    for _ in 0..3 {
        journal.append_group(&[b"row"]).unwrap();
    }
    let mut seen = 0;
    let stats = journal.stats();
    assert!(
        journal
            .scan(|_| {
                seen += 1;
                anyhow::bail!("consumer failed")
            })
            .is_err()
    );
    assert_eq!(seen, 1);
    assert_eq!(journal.stats(), stats);
    assert_eq!(replay(&journal).len(), 3);
}

#[test]
fn new_directory_creation_and_exact_frame_admission_are_durable() {
    let parent = fixture();
    let path = parent.path().join("journal");
    let mut journal = Journal::open(&path, config()).unwrap();
    assert_eq!(journal.stats().namespace_barriers, 2); // LOCK/root and parent
    assert_eq!(journal.stats().file_syncs, 1); // LOCK
    // 128 fixed bytes + two length words + 240 payload bytes = 384 exactly.
    let receipt = journal.append_group(&[&[1; 120], &[2; 120]]).unwrap();
    assert_eq!(receipt.last_sequence(), 2);
    assert_eq!(journal.stats().disk_bytes + SEAL, 512);
    drop(journal);
    let journal = Journal::open(&path, config()).unwrap();
    assert_eq!(
        replay(&journal),
        vec![vec![(1, vec![1; 120]), (2, vec![2; 120])]]
    );
}

#[test]
fn metadata_admission_rejects_oversized_files_total_bytes_and_segment_counts() {
    let dir = fixture();
    let path = dir.path().join(segment_name(1));
    fs::write(&path, [0; 513]).unwrap();
    let error = Journal::open(dir.path(), config()).err().unwrap();
    assert!(error.to_string().contains("segment byte admission"));
    assert_eq!(fs::metadata(&path).unwrap().len(), 513);
    fs::remove_file(path).unwrap();
    for id in 1..=9 {
        fs::write(dir.path().join(segment_name(id)), segment_header(id, 1)).unwrap();
    }
    let error = Journal::open(dir.path(), config()).err().unwrap();
    assert!(error.to_string().contains("segment count admission"));

    let dir = fixture();
    let mut journal = Journal::open(dir.path(), config()).unwrap();
    for _ in 0..2 {
        journal.append_group(&[&[1; 200]]).unwrap();
    }
    drop(journal);
    let before = snapshot(dir.path());
    let error = Journal::open(
        dir.path(),
        JournalConfig {
            max_total_bytes: 512,
            ..config()
        },
    )
    .err()
    .unwrap();
    assert!(error.to_string().contains("disk admission"));
    assert_eq!(snapshot(dir.path()), before);
}

#[test]
fn unknown_version_and_flags_fail_even_with_valid_header_checksums() {
    let dir = fixture();
    let path = dir.path().join(segment_name(1));
    for (offset, value) in [(8, 2u32), (12, 1u32)] {
        let mut header = segment_header(1, 1);
        header[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
        let digest = blake3::hash(&header[..32]);
        header[32..].copy_from_slice(digest.as_bytes());
        fs::write(&path, header).unwrap();
        assert!(Journal::open(dir.path(), config()).is_err());
        assert_eq!(fs::read(&path).unwrap(), header);
    }
}

#[test]
fn foreign_files_and_tighter_reopen_bounds_are_rejected_without_deleting_history() {
    let dir = fixture();
    let mut journal = Journal::open(dir.path(), config()).unwrap();
    journal.append_group(&[&[1; 200]]).unwrap();
    drop(journal);
    let before = snapshot(dir.path());
    let smaller = JournalConfig {
        max_record_bytes: 199,
        ..config()
    };
    assert!(Journal::open(dir.path(), smaller).is_err());
    assert_eq!(snapshot(dir.path()), before);
    fs::write(dir.path().join("legacy.wal"), b"keep me").unwrap();
    assert!(Journal::open(dir.path(), config()).is_err());
    assert_eq!(fs::read(dir.path().join("legacy.wal")).unwrap(), b"keep me");
}

#[test]
fn reclaim_all_then_append_in_place_or_after_checkpoint_reopen() {
    for reopen in [false, true] {
        let dir = fixture();
        let mut journal = Journal::open(dir.path(), config()).unwrap();
        for value in 1..=3 {
            journal.append_group(&[&[value; 200]]).unwrap();
        }
        let before = journal.stats();
        journal.reclaim_through(3).unwrap();
        assert_eq!(journal.stats().disk_bytes, 0);
        assert_eq!(journal.stats().segments, 0);
        assert_eq!(journal.stats().durable_sequence, 3);
        assert_eq!(
            journal.stats().namespace_barriers,
            before.namespace_barriers + 1
        );
        assert_eq!(journal.stats().file_syncs, before.file_syncs + 1);
        assert!(journal.active.is_none());
        assert_eq!(snapshot(dir.path()), vec![("LOCK".to_owned(), vec![])]);
        assert!(journal.seal_snapshot().unwrap().is_empty());
        let retired = journal.stats();
        journal.reclaim_through(3).unwrap();
        assert_eq!(journal.stats(), retired);
        if reopen {
            drop(journal);
            journal = Journal::open_with_checkpoint(dir.path(), config(), 3).unwrap();
            assert_eq!(journal.stats().recovered_records, 0);
        }
        assert!(replay(&journal).is_empty());
        assert_eq!(
            journal.append_group(&[b"four"]).unwrap().first_sequence(),
            4
        );
        drop(journal);
        let journal = Journal::open_with_checkpoint(dir.path(), config(), 3).unwrap();
        assert_eq!(replay(&journal), vec![vec![(4, b"four".to_vec())]]);
    }
}

#[test]
fn reclaim_retains_a_straddling_group_and_mutable_tail_byte_for_byte() {
    let dir = fixture();
    let mut journal = Journal::open(dir.path(), roomy()).unwrap();
    journal.append_group(&[b"one", b"two"]).unwrap();
    journal.seal_snapshot().unwrap();
    journal.append_group(&[b"three", b"four"]).unwrap();
    let tail_path = dir.path().join(segment_name(2));
    let tail = fs::read(&tail_path).unwrap();
    let before = journal.stats();
    journal.reclaim_through(3).unwrap();
    assert!(!dir.path().join(segment_name(1)).exists());
    assert_eq!(fs::read(&tail_path).unwrap(), tail);
    assert_eq!(journal.stats().disk_bytes, tail.len() as u64);
    assert_eq!(journal.stats().segments, 1);
    assert_eq!(journal.stats().file_syncs, before.file_syncs);
    assert!(journal.active.is_some());
    let barriers = journal.stats().namespace_barriers;
    assert_eq!(
        journal.append_group(&[b"five"]).unwrap().first_sequence(),
        5
    );
    assert_eq!(journal.stats().namespace_barriers, barriers);
    drop(journal);
    let journal = Journal::open_with_checkpoint(dir.path(), roomy(), 3).unwrap();
    assert_eq!(
        replay(&journal),
        vec![
            vec![(3, b"three".to_vec()), (4, b"four".to_vec())],
            vec![(5, b"five".to_vec())]
        ]
    );
}

#[test]
fn root_authority_allows_only_covered_missing_prefix_or_interior_history() {
    for missing in [vec![1, 2], vec![2]] {
        let dir = fixture();
        let mut journal = Journal::open(dir.path(), config()).unwrap();
        for value in 1..=4 {
            journal.append_group(&[&[value; 200]]).unwrap();
        }
        drop(journal);
        for id in &missing {
            fs::remove_file(dir.path().join(segment_name(*id))).unwrap();
        }
        let before = snapshot(dir.path());
        assert!(Journal::open(dir.path(), config()).is_err());
        assert!(Journal::open_with_checkpoint(dir.path(), config(), 1).is_err());
        assert_eq!(snapshot(dir.path()), before);
        let journal = Journal::open_with_checkpoint(dir.path(), config(), 2).unwrap();
        let sequences: Vec<_> = replay(&journal)
            .into_iter()
            .flatten()
            .map(|(sequence, _)| sequence)
            .collect();
        let expected = if missing.len() == 2 {
            vec![3, 4]
        } else {
            vec![1, 3, 4]
        };
        assert_eq!(sequences, expected);
        assert_eq!(journal.stats().recovered_records, sequences.len() as u64);
        assert_eq!(journal.stats().durable_sequence, 4);
    }
}

#[test]
fn checkpoint_does_not_hide_corruption_in_retained_covered_segments() {
    let dir = fixture();
    let mut journal = Journal::open(dir.path(), config()).unwrap();
    journal.append_group(&[&[1; 200]]).unwrap();
    journal.append_group(&[&[2; 200]]).unwrap();
    drop(journal);
    let path = dir.path().join(segment_name(1));
    let original = fs::read(&path).unwrap();
    for offset in [0, 31, 63, 130, original.len() - 1] {
        let mut corrupt = original.clone();
        corrupt[offset] ^= 1;
        fs::write(&path, &corrupt).unwrap();
        let before = snapshot(dir.path());
        assert!(Journal::open_with_checkpoint(dir.path(), config(), 2).is_err());
        assert_eq!(snapshot(dir.path()), before);
    }
}

#[test]
fn lower_and_future_checkpoint_authority_fail_without_mutation() {
    let dir = fixture();
    let mut journal = Journal::open(dir.path(), config()).unwrap();
    for value in 1..=3 {
        journal.append_group(&[&[value; 200]]).unwrap();
    }
    journal.reclaim_through(2).unwrap();
    let before = snapshot(dir.path());
    let stats = journal.stats();
    for invalid in [0, 1, 4, u64::MAX] {
        assert!(journal.reclaim_through(invalid).is_err());
        assert_eq!(snapshot(dir.path()), before);
        assert_eq!(journal.stats(), stats);
    }
    drop(journal);
    assert!(Journal::open_with_checkpoint(dir.path(), config(), 1).is_err());
    assert_eq!(snapshot(dir.path()), before);
    let mut journal = Journal::open_with_checkpoint(dir.path(), config(), 2).unwrap();
    assert_eq!(
        journal.append_group(&[b"four"]).unwrap().first_sequence(),
        4
    );
}

#[test]
fn empty_checkpoint_log_starts_at_floor_plus_one_and_floor_overflow_is_read_only() {
    let parent = fixture();
    let path = parent.path().join("journal");
    assert!(Journal::open_with_checkpoint(&path, config(), u64::MAX).is_err());
    assert!(!path.exists());
    let mut journal = Journal::open_with_checkpoint(&path, config(), 42).unwrap();
    assert_eq!(journal.stats().durable_sequence, 42);
    assert!(replay(&journal).is_empty());
    assert!(journal.seal_snapshot().unwrap().is_empty());
    assert_eq!(
        journal.append_group(&[b"next"]).unwrap().first_sequence(),
        43
    );
    drop(journal);
    assert!(Journal::open(&path, config()).is_err());
    let journal = Journal::open_with_checkpoint(&path, config(), 42).unwrap();
    assert_eq!(replay(&journal), vec![vec![(43, b"next".to_vec())]]);
}

#[test]
fn newer_root_than_retained_tail_rotates_without_inventing_records() {
    let dir = fixture();
    let mut journal = Journal::open(dir.path(), roomy()).unwrap();
    journal.append_group(&[b"old"]).unwrap();
    drop(journal);
    let mut journal = Journal::open_with_checkpoint(dir.path(), roomy(), 10).unwrap();
    assert_eq!(journal.stats().recovered_records, 1);
    assert_eq!(journal.stats().durable_sequence, 10);
    assert_eq!(replay(&journal), vec![vec![(1, b"old".to_vec())]]);
    assert_eq!(
        journal.append_group(&[b"new"]).unwrap().first_sequence(),
        11
    );
    drop(journal);
    let journal = Journal::open_with_checkpoint(dir.path(), roomy(), 10).unwrap();
    assert_eq!(
        replay(&journal),
        vec![vec![(1, b"old".to_vec())], vec![(11, b"new".to_vec())]]
    );
}

#[test]
fn repeated_snapshots_are_immutable_and_successor_creation_is_lazy() {
    let dir = fixture();
    let mut journal = Journal::open(dir.path(), roomy()).unwrap();
    let empty = journal.stats();
    assert!(journal.seal_snapshot().unwrap().is_empty());
    assert_eq!(journal.stats(), empty);
    journal.append_group(&[b"one", b"two"]).unwrap();
    let before = journal.stats();
    let sealed = journal.seal_snapshot().unwrap();
    assert_eq!(sealed.len(), 1);
    assert_eq!(sealed[0].relative_path, PathBuf::from(segment_name(1)));
    assert_eq!((sealed[0].first_sequence, sealed[0].last_sequence), (1, 2));
    let path = dir.path().join(&sealed[0].relative_path);
    let bytes = fs::read(&path).unwrap();
    assert_eq!(sealed[0].bytes, bytes.len() as u64);
    assert_eq!(&bytes[bytes.len() - SEAL as usize..][..8], SEAL_MAGIC);
    assert_eq!(journal.stats().disk_bytes, before.disk_bytes + SEAL);
    assert_eq!(journal.stats().file_syncs, before.file_syncs + 1);
    assert_eq!(
        journal.stats().namespace_barriers,
        before.namespace_barriers
    );
    assert_eq!(journal.stats().segments, 1);
    assert!(journal.active.is_none());
    let once = journal.stats();
    assert_eq!(journal.seal_snapshot().unwrap(), sealed);
    assert_eq!(journal.stats(), once);
    assert_eq!(
        journal.append_group(&[b"three"]).unwrap().first_sequence(),
        3
    );
    assert_eq!(fs::read(&path).unwrap(), bytes);
    assert_eq!(journal.stats().segments, 2);
    let all = journal.seal_snapshot().unwrap();
    assert_eq!(all[0], sealed[0]);
    assert_eq!((all[1].first_sequence, all[1].last_sequence), (3, 3));
    drop(journal);
    let mut journal = Journal::open(dir.path(), roomy()).unwrap();
    assert_eq!(journal.seal_snapshot().unwrap(), all);
}

#[test]
fn seal_and_retire_seal_failures_fence_and_recover_without_losing_tail() {
    for retire in [false, true] {
        for fault in [FailAt::Write, FailAt::Sync] {
            let dir = fixture();
            let mut journal = Journal::open(dir.path(), roomy()).unwrap();
            journal.append_group(&[b"one", b"two"]).unwrap();
            journal.fault.next = Some(fault);
            if retire {
                assert!(journal.reclaim_through(2).is_err());
            } else {
                assert!(journal.seal_snapshot().is_err());
            }
            assert!(journal.stats().fenced);
            let before = snapshot(dir.path());
            assert!(journal.seal_snapshot().is_err());
            assert!(journal.reclaim_through(2).is_err());
            assert!(journal.append_group(&[b"no"]).is_err());
            assert!(journal.scan(|_| Ok(())).is_err());
            assert!(journal.required_append_bytes(&[b"no"]).is_err());
            assert_eq!(snapshot(dir.path()), before);
            drop(journal);
            let mut journal = Journal::open_with_checkpoint(dir.path(), roomy(), 2).unwrap();
            assert_eq!(
                replay(&journal),
                vec![vec![(1, b"one".to_vec()), (2, b"two".to_vec())]]
            );
            assert_eq!(
                journal.append_group(&[b"three"]).unwrap().first_sequence(),
                3
            );
        }
    }
}

#[test]
fn retirement_unlink_and_directory_failures_fence_without_releasing_credits() {
    for fault in [FailAt::Remove, FailAt::Namespace] {
        let dir = fixture();
        let mut journal = Journal::open(dir.path(), config()).unwrap();
        for value in 1..=3 {
            journal.append_group(&[&[value; 200]]).unwrap();
        }
        let before = journal.stats();
        let tail = fs::read(dir.path().join(segment_name(3))).unwrap();
        journal.fault.next = Some(fault);
        assert!(journal.reclaim_through(2).is_err());
        assert!(journal.stats().fenced);
        assert_eq!(journal.stats().disk_bytes, before.disk_bytes);
        assert_eq!(journal.stats().segments, before.segments);
        assert_eq!(
            journal.stats().namespace_barriers,
            before.namespace_barriers
        );
        assert_eq!(fs::read(dir.path().join(segment_name(3))).unwrap(), tail);
        let failed = snapshot(dir.path());
        assert!(journal.reclaim_through(2).is_err());
        assert!(journal.seal_snapshot().is_err());
        assert_eq!(snapshot(dir.path()), failed);
        drop(journal);
        let mut journal = Journal::open_with_checkpoint(dir.path(), config(), 2).unwrap();
        journal.reclaim_through(2).unwrap();
        assert_eq!(replay(&journal), vec![vec![(3, vec![3; 200])]]);
        assert_eq!(journal.stats().disk_bytes, tail.len() as u64);
        assert_eq!(
            journal.append_group(&[b"four"]).unwrap().first_sequence(),
            4
        );
    }
}

#[test]
fn reclaim_all_directory_failure_reopens_from_external_root() {
    let dir = fixture();
    let mut journal = Journal::open(dir.path(), roomy()).unwrap();
    journal.append_group(&[b"one", b"two"]).unwrap();
    journal.fault.next = Some(FailAt::Namespace);
    assert!(journal.reclaim_through(2).is_err());
    assert!(journal.stats().fenced);
    assert_eq!(snapshot(dir.path()), vec![("LOCK".to_owned(), vec![])]);
    drop(journal);
    let mut journal = Journal::open_with_checkpoint(dir.path(), roomy(), 2).unwrap();
    assert!(replay(&journal).is_empty());
    assert_eq!(
        journal.append_group(&[b"three"]).unwrap().first_sequence(),
        3
    );
}

#[test]
fn reclamation_returns_disk_and_segment_credits_without_reusing_sequences() {
    let dir = fixture();
    let bounded = JournalConfig {
        max_total_bytes: 512,
        max_segments: 1,
        ..config()
    };
    let mut journal = Journal::open(dir.path(), bounded).unwrap();
    for sequence in 1..=8 {
        assert_eq!(journal.required_append_bytes(&[&[1; 200]]).unwrap(), 400);
        assert_eq!(
            journal.append_group(&[&[1; 200]]).unwrap().first_sequence(),
            sequence
        );
        let before = journal.stats();
        assert_eq!(journal.required_append_bytes(&[&[2; 200]]).unwrap(), 464);
        assert!(journal.append_group(&[&[2; 200]]).is_err());
        assert_eq!(journal.stats(), before);
        journal.reclaim_through(sequence).unwrap();
        assert_eq!(journal.stats().disk_bytes, 0);
        assert_eq!(journal.stats().segments, 0);
    }
}

#[test]
fn journal_records_preserve_existing_encoded_wal_bytes_and_physical_sequences() {
    use crate::wal::{EncodedRecord, Operation, Record};

    let dir = fixture();
    let bounds = JournalConfig {
        max_frame_bytes: 2048,
        max_record_bytes: 1024,
        ..roomy()
    };
    let encoded: Vec<_> = (8..=9)
        .map(|sequence| {
            EncodedRecord::new(&Record::new(
                sequence,
                Operation::DropJob {
                    name: format!("job-{sequence}"),
                    stamp: "fixed-stamp".to_owned(),
                },
            ))
            .unwrap()
        })
        .collect();
    let records: Vec<_> = encoded.iter().map(EncodedRecord::as_bytes).collect();
    let mut journal = Journal::open_with_checkpoint(dir.path(), bounds, 7).unwrap();
    let receipt = journal.append_group(&records).unwrap();
    assert_eq!((receipt.first_sequence(), receipt.last_sequence()), (8, 9));
    journal.seal_snapshot().unwrap();
    drop(journal);
    let journal = Journal::open_with_checkpoint(dir.path(), bounds, 7).unwrap();
    let mut seen = 0;
    journal
        .scan(|group| {
            for (sequence, bytes) in group.records() {
                assert_eq!(bytes, records[seen]);
                assert_eq!(crate::wal::decode(bytes)?.sequence, sequence);
                seen += 1;
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(seen, 2);
}

#[test]
fn exact_append_growth_matches_creation_rotation_sealing_and_ordinary_groups() {
    let dir = fixture();
    let mut journal = Journal::open(dir.path(), config()).unwrap();
    for (value, expected) in [(1, 400), (2, 464)] {
        let before = journal.stats();
        let disk = snapshot(dir.path());
        assert_eq!(
            journal.required_append_bytes(&[&[value; 200]]).unwrap(),
            expected
        );
        assert_eq!(journal.stats(), before);
        assert_eq!(snapshot(dir.path()), disk);
        journal.append_group(&[&[value; 200]]).unwrap();
        assert_eq!(journal.stats().disk_bytes - before.disk_bytes, expected);
    }
    journal.seal_snapshot().unwrap();
    let before = journal.stats();
    assert_eq!(journal.required_append_bytes(&[b"x"]).unwrap(), 201);
    journal.append_group(&[b"x"]).unwrap();
    assert_eq!(journal.stats().disk_bytes - before.disk_bytes, 201);
    let before = journal.stats();
    assert_eq!(journal.required_append_bytes(&[b"y"]).unwrap(), 137);
    journal.append_group(&[b"y"]).unwrap();
    assert_eq!(journal.stats().disk_bytes - before.disk_bytes, 137);
    assert_eq!(
        journal.stats().namespace_barriers,
        before.namespace_barriers
    );
}

#[test]
fn append_observer_hands_off_every_mutation_and_sync_without_extra_barriers() {
    use JournalIoPhase::{BeforeNamespaceChange, BeforeWrite, DirectorySync, FileSync};

    let dir = fixture();
    let mut journal = Journal::open(dir.path(), config()).unwrap();
    let mut phases = Vec::new();
    journal
        .append_group_with_observer(&[&[1; 200]], |phase| {
            phases.push(phase);
            Ok(())
        })
        .unwrap();
    let creation = vec![
        BeforeNamespaceChange,
        BeforeWrite,
        FileSync,
        DirectorySync,
        BeforeWrite,
        FileSync,
    ];
    assert_eq!(phases, creation);
    phases.clear();
    journal
        .append_group_with_observer(&[b"x"], |phase| {
            phases.push(phase);
            Ok(())
        })
        .unwrap();
    let mut rotation = vec![BeforeWrite, FileSync];
    rotation.extend_from_slice(&creation);
    assert_eq!(phases, rotation);
    phases.clear();
    let before = journal.stats();
    journal
        .append_group_with_observer(&[b"y"], |phase| {
            phases.push(phase);
            Ok(())
        })
        .unwrap();
    assert_eq!(phases, vec![BeforeWrite, FileSync]);
    assert_eq!(
        journal.stats().namespace_barriers,
        before.namespace_barriers
    );
    phases.clear();
    journal
        .seal_snapshot_with_observer(|phase| {
            phases.push(phase);
            Ok(())
        })
        .unwrap();
    assert_eq!(phases, vec![BeforeWrite, FileSync]);
    phases.clear();
    journal
        .reclaim_through_with_observer(3, |phase| {
            phases.push(phase);
            Ok(())
        })
        .unwrap();
    assert_eq!(
        phases,
        vec![BeforeNamespaceChange, BeforeNamespaceChange, DirectorySync]
    );
    journal.append_group(&[b"z"]).unwrap();
    phases.clear();
    journal
        .reclaim_through_with_observer(4, |phase| {
            phases.push(phase);
            Ok(())
        })
        .unwrap();
    assert_eq!(
        phases,
        vec![BeforeWrite, FileSync, BeforeNamespaceChange, DirectorySync]
    );
}

#[test]
fn observer_failure_fences_and_admission_rejection_never_calls_observer() {
    for fail_index in 0..6 {
        let dir = fixture();
        let mut journal = Journal::open(dir.path(), roomy()).unwrap();
        let mut index = 0;
        assert!(
            journal
                .append_group_with_observer(&[b"one"], |_| {
                    let fail = index == fail_index;
                    index += 1;
                    ensure!(!fail, "injected observer failure");
                    Ok(())
                })
                .is_err()
        );
        assert_eq!(index, fail_index + 1);
        assert!(journal.stats().fenced);
        let before = snapshot(dir.path());
        assert!(journal.append_group(&[b"no"]).is_err());
        assert_eq!(snapshot(dir.path()), before);
        drop(journal);
        let journal = Journal::open(dir.path(), roomy()).unwrap();
        assert_eq!(journal.stats().durable_sequence, u64::from(fail_index == 5));
    }
    let dir = fixture();
    let mut journal = Journal::open(dir.path(), roomy()).unwrap();
    let before = journal.stats();
    assert!(journal.required_append_bytes(&[]).is_err());
    assert!(
        journal
            .append_group_with_observer(&[], |_| panic!("unexpected I/O"))
            .is_err()
    );
    assert_eq!(journal.stats(), before);
}

#[test]
fn checkpoint_successor_partial_headers_recover_without_reusing_root_sequences() {
    let dir = fixture();
    let mut journal = Journal::open(dir.path(), roomy()).unwrap();
    journal.append_group(&[b"old"]).unwrap();
    journal.seal_snapshot().unwrap();
    drop(journal);
    let original = fs::read(dir.path().join(segment_name(1))).unwrap();
    let header = segment_header(2, 11);
    for cut in 0..HEADER {
        // Each crash case starts from the same sealed predecessor and incomplete
        // successor; a short header has no acknowledged records to preserve.
        for entry in fs::read_dir(dir.path()).unwrap() {
            let entry = entry.unwrap();
            if entry.file_name() != "LOCK" && entry.file_name() != segment_name(1).as_str() {
                fs::remove_file(entry.path()).unwrap();
            }
        }
        fs::write(dir.path().join(segment_name(2)), &header[..cut]).unwrap();
        let mut journal = Journal::open_with_checkpoint(dir.path(), roomy(), 10).unwrap();
        assert_eq!(journal.stats().durable_sequence, 10, "cut {cut}");
        assert_eq!(
            journal.append_group(&[b"new"]).unwrap().first_sequence(),
            11
        );
        assert_eq!(
            fs::read(dir.path().join(segment_name(1))).unwrap(),
            original
        );
        drop(journal);
        let journal = Journal::open_with_checkpoint(dir.path(), roomy(), 10).unwrap();
        assert_eq!(
            replay(&journal),
            vec![vec![(1, b"old".to_vec())], vec![(11, b"new".to_vec())]],
            "cut {cut}"
        );
    }
}

#[test]
fn empty_recovered_segments_are_sealed_but_never_advertised_as_record_ranges() {
    let dir = fixture();
    fs::write(dir.path().join(segment_name(1)), segment_header(1, 8)).unwrap();
    let mut journal = Journal::open_with_checkpoint(dir.path(), config(), 7).unwrap();
    assert!(journal.seal_snapshot().unwrap().is_empty());
    assert!(journal.active.is_none());
    assert_eq!(journal.stats().disk_bytes, HEADER as u64 + SEAL);
    assert_eq!(
        journal.append_group(&[b"eight"]).unwrap().first_sequence(),
        8
    );
    let sealed = journal.seal_snapshot().unwrap();
    assert_eq!(sealed.len(), 1);
    assert_eq!(sealed[0].relative_path, PathBuf::from(segment_name(2)));
    assert_eq!((sealed[0].first_sequence, sealed[0].last_sequence), (8, 8));
    journal.reclaim_through(7).unwrap();
    assert_eq!(journal.stats().segments, 1);
    drop(journal);
    let journal = Journal::open_with_checkpoint(dir.path(), config(), 7).unwrap();
    assert_eq!(replay(&journal), vec![vec![(8, b"eight".to_vec())]]);
}

#[test]
fn lifecycle_observer_failures_fence_at_every_seal_and_retirement_boundary() {
    for retire in [false, true] {
        let phase_count = if retire { 4 } else { 2 };
        for fail_index in 0..phase_count {
            let dir = fixture();
            let mut journal = Journal::open(dir.path(), roomy()).unwrap();
            journal.append_group(&[b"one"]).unwrap();
            let before = journal.stats();
            let mut index = 0;
            let observer = |_| {
                let fail = index == fail_index;
                index += 1;
                ensure!(!fail, "injected lifecycle observer failure");
                Ok(())
            };
            if retire {
                assert!(journal.reclaim_through_with_observer(1, observer).is_err());
            } else {
                assert!(journal.seal_snapshot_with_observer(observer).is_err());
            }
            assert_eq!(index, fail_index + 1);
            assert!(journal.stats().fenced);
            assert!(journal.stats().disk_bytes >= before.disk_bytes);
            assert_eq!(journal.stats().segments, before.segments);
            drop(journal);
            let mut journal = Journal::open_with_checkpoint(dir.path(), roomy(), 1).unwrap();
            let expected = if retire && fail_index == 3 {
                vec![]
            } else {
                vec![vec![(1, b"one".to_vec())]]
            };
            assert_eq!(replay(&journal), expected);
            assert_eq!(journal.append_group(&[b"two"]).unwrap().first_sequence(), 2);
        }
    }
}

#[test]
fn zero_checkpoint_retirement_preserves_strict_ordinary_reopen() {
    for live_tail in [false, true] {
        let dir = fixture();
        fs::write(dir.path().join(segment_name(1)), segment_header(1, 1)).unwrap();
        let mut journal = Journal::open(dir.path(), config()).unwrap();
        assert!(journal.seal_snapshot().unwrap().is_empty());
        if live_tail {
            journal.append_group(&[b"one"]).unwrap();
        }
        let before = journal.stats();
        journal.reclaim_through(0).unwrap();
        if live_tail {
            assert_eq!(journal.stats(), before);
        } else {
            assert_eq!(journal.stats().disk_bytes, 0);
            assert_eq!(journal.stats().segments, 0);
            assert_eq!(journal.append_group(&[b"one"]).unwrap().first_sequence(), 1);
            assert!(dir.path().join(segment_name(1)).exists());
        }
        drop(journal);
        let mut journal = Journal::open(dir.path(), config()).unwrap();
        assert_eq!(replay(&journal), vec![vec![(1, b"one".to_vec())]]);
        journal.reclaim_through(1).unwrap();
        assert_eq!(journal.stats().disk_bytes, 0);
        assert_eq!(journal.append_group(&[b"two"]).unwrap().first_sequence(), 2);
    }
}

#[test]
fn interrupted_zero_checkpoint_retirement_keeps_a_reopenable_empty_prefix() {
    for fault in [FailAt::Remove, FailAt::Namespace] {
        let dir = fixture();
        fs::write(dir.path().join(segment_name(1)), segment_header(1, 1)).unwrap();
        let mut journal = Journal::open(dir.path(), config()).unwrap();
        assert!(journal.seal_snapshot().unwrap().is_empty());
        drop(journal);
        fs::write(dir.path().join(segment_name(2)), segment_header(2, 1)).unwrap();
        let mut journal = Journal::open(dir.path(), config()).unwrap();
        journal.fault.next = Some(fault);
        assert!(journal.reclaim_through(0).is_err());
        assert!(journal.stats().fenced);
        if fault == FailAt::Remove {
            assert!(dir.path().join(segment_name(1)).exists());
            assert!(!dir.path().join(segment_name(2)).exists());
        }
        drop(journal);
        let mut journal = Journal::open(dir.path(), config()).unwrap();
        assert!(replay(&journal).is_empty());
        assert_eq!(journal.append_group(&[b"one"]).unwrap().first_sequence(), 1);
        drop(journal);
        let journal = Journal::open(dir.path(), config()).unwrap();
        assert_eq!(replay(&journal), vec![vec![(1, b"one".to_vec())]]);
    }
}

#[test]
fn growth_estimation_precedes_disk_pressure_but_still_validates_record_shape() {
    let dir = fixture();
    let bounded = JournalConfig {
        max_total_bytes: 512,
        ..roomy()
    };
    let mut journal = Journal::open(dir.path(), bounded).unwrap();
    journal.append_group(&[&[1; 200]]).unwrap();
    let before = journal.stats();
    let disk = snapshot(dir.path());
    let growth = journal.required_append_bytes(&[b"x"]).unwrap();
    assert_eq!(growth, 137);
    assert_eq!(RESERVED_SEAL_BYTES, 64);
    assert!(before.disk_bytes + growth + RESERVED_SEAL_BYTES > bounded.max_total_bytes);
    assert!(
        journal
            .append_group_with_observer(&[b"x"], |_| panic!("capacity rejection did I/O"))
            .is_err()
    );
    assert!(journal.required_append_bytes(&[]).is_err());
    assert!(journal.required_append_bytes(&[&[0; 201]]).is_err());
    assert!(
        journal
            .required_append_bytes(&[&[0; 200], &[0; 200]])
            .is_err()
    );
    assert_eq!(journal.stats(), before);
    assert_eq!(snapshot(dir.path()), disk);
    journal.reclaim_through(1).unwrap();
    assert_eq!(journal.required_append_bytes(&[b"x"]).unwrap(), 201);
    assert_eq!(journal.append_group(&[b"x"]).unwrap().first_sequence(), 2);
}

#[test]
fn observed_append_preserves_existing_io_fault_injection() {
    for fault in [FailAt::Write, FailAt::Sync, FailAt::Namespace] {
        let dir = fixture();
        let mut journal = Journal::open(dir.path(), config()).unwrap();
        journal.append_group(&[&[1; 200]]).unwrap();
        journal.fault.next = Some(fault);
        let mut phases = Vec::new();
        assert!(
            journal
                .append_group_with_observer(&[&[2; 200]], |phase| {
                    phases.push(phase);
                    Ok(())
                })
                .is_err()
        );
        assert!(journal.stats().fenced);
        let failed_phase = match fault {
            FailAt::Write => JournalIoPhase::BeforeWrite,
            FailAt::Sync => JournalIoPhase::FileSync,
            FailAt::Namespace => JournalIoPhase::DirectorySync,
            FailAt::Remove => unreachable!(),
        };
        assert_eq!(phases.last(), Some(&failed_phase));
        drop(journal);
        let mut journal = Journal::open(dir.path(), config()).unwrap();
        assert_eq!(replay(&journal), vec![vec![(1, vec![1; 200])]]);
        assert_eq!(journal.append_group(&[b"two"]).unwrap().first_sequence(), 2);
    }
}
