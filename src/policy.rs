use crate::engine::*;
use crate::model::checked_cutoff;
use crate::tier::{ship_with_remote_gate, vacuum_with_remote_gate};
use anyhow::{Context, Result, ensure};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::sync::atomic::Ordering;

impl Database {
    pub fn compact(&self) -> Result<usize> {
        let remote_gate = self
            .inner
            .remote
            .as_ref()
            .map(|_| self.lock_remote_operation())
            .transpose()?;
        {
            let mut s = self.lock()?;
            healthy(&s)?;
            checkpoint_locked(&self.inner, &mut s)?;
        }
        let maintenance_pin = prefetch_maintenance_segments(self, None)?;
        let result = {
            let mut s = self.lock()?;
            healthy(&s)?;
            checkpoint_locked(&self.inner, &mut s)?;
            compact_locked(&self.inner, &mut s)
        };
        drop(maintenance_pin);
        drop(remote_gate);
        result
    }
    pub fn maintain(&self, now_us: i64) -> Result<MaintenanceReport> {
        let remote_gate = self
            .inner
            .remote
            .as_ref()
            .map(|_| self.lock_remote_operation())
            .transpose()?;
        let result = (|| {
            let mut report = MaintenanceReport::default();
            {
                let mut s = self.lock()?;
                healthy(&s)?;
                advance_idempotency_floors(&mut s, now_us);
                let has_retention = s.catalog.tables.values().any(|table| {
                    table.config.retention_us.is_some()
                        || table.config.rollup_retention_us.is_some()
                });
                let flush_due = s.first_hot_us.is_none_or(|at| {
                    now_us.saturating_sub(at) >= self.inner.config.flush_interval_us
                });
                if has_retention || flush_due {
                    report.flushed = s.catalog.checkpoint_sequence < s.sequence;
                    checkpoint_locked(&self.inner, &mut s)?;
                }
            }
            let maintenance_pin = prefetch_maintenance_segments(self, Some(now_us))?;
            let (ship_due, resume_gc, vacuum_due) = {
                let mut s = self.lock()?;
                healthy(&s)?;
                if s.catalog.checkpoint_sequence < s.sequence {
                    report.flushed = true;
                    checkpoint_locked(&self.inner, &mut s)?;
                }
                report.expired_rows = retention_locked(&self.inner, &mut s, now_us)?;
                report.compacted = compact_locked(&self.inner, &mut s)?;
                let ship_due = self.inner.remote.is_some()
                    && (s.last_ship_us.is_none_or(|at| {
                        now_us.saturating_sub(at) >= self.inner.config.ship_interval_us
                    }) || report.expired_rows > 0);
                let resume_gc = s.remote_head.as_ref().is_some_and(|head| {
                    head.lock
                        .as_ref()
                        .is_some_and(|lock| lock.kind == "gc" && lock.owner == s.remote_owner)
                });
                if report.expired_rows > 0 && self.inner.remote.is_some() {
                    s.remote_vacuum_pending = true;
                }
                let vacuum_due = s.remote_vacuum_pending || ship_due;
                (ship_due, resume_gc, vacuum_due)
            };
            drop(maintenance_pin);
            if let Some(gate) = remote_gate.as_ref() {
                if !resume_gc && ship_due {
                    report.shipped_sequence = Some(ship_with_remote_gate(self, gate)?);
                    let mut s = self.lock()?;
                    s.last_ship_us = Some(now_us);
                    report.evicted_files = archive_locked(&self.inner, &mut s, now_us)?;
                }
                if resume_gc || vacuum_due {
                    vacuum_with_remote_gate(self, gate)?;
                    self.lock()?.remote_vacuum_pending = false;
                }
            }
            let mut s = self.lock()?;
            report.reclaimed_files = gc_locked(&self.inner, &mut s)?;
            Ok(report)
        })();
        {
            let mut s = self.lock()?;
            match &result {
                Ok(_) => s.last_maintenance_error = None,
                Err(error) => s.last_maintenance_error = Some(format!("{error:#}")),
            }
        }
        result
    }
}

fn prefetch_maintenance_segments(db: &Database, now_us: Option<i64>) -> Result<Option<Pin>> {
    let (segments, pin) = {
        let s = db.lock()?;
        healthy(&s)?;
        let segments: Vec<_> = s
            .catalog
            .tables
            .values()
            .flat_map(|table| {
                table.segments.iter().filter(|segment| {
                    let needed_for_retention =
                        now_us
                            .zip(table.config.retention_us)
                            .is_some_and(|(now, age)| {
                                let cutoff = checked_cutoff(now, age)
                                    .max(table.cutoff_us.unwrap_or(i64::MIN));
                                segment.min_timestamp_us < cutoff
                                    && segment.max_timestamp_us >= cutoff
                            });
                    let needed_for_compaction = segment.rows < db.inner.config.segment_rows as u64;
                    needed_for_retention || needed_for_compaction
                })
            })
            .cloned()
            .collect();
        if segments.is_empty() {
            return Ok(None);
        }
        let ids = segments.iter().map(|segment| segment.id.clone()).collect();
        (segments, Pin::new(&db.inner, ids)?)
    };
    for segment in segments {
        let path = db.inner.root.join(segment.key());
        let cached = db
            .inner
            .root
            .join("cache")
            .join(format!("{}.parquet", segment.id));
        if path.exists() || cached.exists() {
            continue;
        }
        let remote = db
            .inner
            .remote
            .as_ref()
            .context("maintenance requires the configured remote store")?;
        let bytes = remote.get_bounded(&segment.key(), segment.bytes as usize)?;
        ensure!(
            bytes.len() as u64 == segment.bytes
                && blake3::hash(&bytes).to_hex().as_str() == segment.id,
            "remote segment integrity failure during maintenance"
        );
        let s = db.lock()?;
        healthy(&s)?;
        if !path.exists() && !cached.exists() {
            let _disk = lock_disk_admission(&db.inner)?;
            if !path.exists() && !cached.exists() {
                ensure_budget(&db.inner, segment.bytes)?;
                crate::wal::atomic_write(&path, &bytes)?;
            }
        }
    }
    Ok(Some(pin))
}

fn retention_locked(inner: &Inner, s: &mut State, now_us: i64) -> Result<u64> {
    let mut next = s.catalog.clone();
    let mut changed = false;
    let mut removed = 0;
    for table in next.tables.values_mut() {
        if let Some(age) = table.config.retention_us {
            let cutoff = checked_cutoff(now_us, age).max(table.cutoff_us.unwrap_or(i64::MIN));
            if table.cutoff_us != Some(cutoff) {
                table.cutoff_us = Some(cutoff);
                changed = true;
            }
            let mut segments = Vec::new();
            for seg in &table.segments {
                if seg.max_timestamp_us < cutoff {
                    removed += seg.rows;
                    changed = true;
                    continue;
                }
                if seg.min_timestamp_us < cutoff {
                    let old = read_segment_locked(inner, s, seg)?;
                    let rows: Vec<_> = old
                        .iter()
                        .filter(|r| r.row.timestamp_us >= cutoff)
                        .cloned()
                        .collect();
                    removed += seg.rows - rows.len() as u64;
                    segments.extend(write_partitioned(inner, &table.config, &rows)?);
                    changed = true;
                } else {
                    segments.push(seg.clone());
                }
            }
            table.segments = segments;
        }
        if let Some(age) = table.config.rollup_retention_us {
            let cutoff =
                checked_cutoff(now_us, age).max(table.rollup_cutoff_us.unwrap_or(i64::MIN));
            if table.rollup_cutoff_us != Some(cutoff) {
                table.rollup_cutoff_us = Some(cutoff);
                changed = true;
            }
            table
                .rollups
                .retain(|_, r| r.bucket_us.saturating_add(r.width_us) > cutoff);
        }
    }
    if changed {
        // Policy state is published only after all hot data has joined the checkpoint.
        ensure!(
            next.checkpoint_sequence == s.sequence,
            "retention requires a current checkpoint"
        );
        persist_manifest(inner, s, next)?;
        gc_locked(inner, s)?;
    }
    Ok(removed)
}

fn compact_locked(inner: &Inner, s: &mut State) -> Result<usize> {
    if s.catalog.checkpoint_sequence != s.sequence {
        return Ok(0);
    }
    let mut next = s.catalog.clone();
    let mut compacted = 0;
    for table in next.tables.values_mut() {
        let mut groups: BTreeMap<(u32, i64), Vec<Segment>> = BTreeMap::new();
        for seg in &table.segments {
            if seg.rows < inner.config.segment_rows as u64 {
                groups
                    .entry((seg.shard, seg.window_us))
                    .or_default()
                    .push(seg.clone());
            }
        }
        let mut retired = BTreeSet::new();
        let mut replacements = Vec::new();
        for candidates in groups.values() {
            let mut selected = Vec::new();
            let mut row_count = 0usize;
            for seg in candidates {
                if row_count.saturating_add(seg.rows as usize) > inner.config.hot_max_rows {
                    break;
                }
                row_count += seg.rows as usize;
                selected.push(seg);
            }
            if selected.len() < inner.config.compact_min_segments
                || row_count.div_ceil(inner.config.segment_rows) >= selected.len()
            {
                continue;
            }
            ensure!(
                selected
                    .iter()
                    .try_fold(0u64, |sum, seg| sum.checked_add(seg.decoded_bytes))
                    .is_some_and(|bytes| bytes <= inner.config.hot_max_bytes as u64),
                "compaction working set exceeds hot memory budget"
            );
            let mut rows = Vec::new();
            let mut bytes = 0usize;
            for seg in &selected {
                let batch = read_segment_locked(inner, s, seg)?;
                bytes += batch.iter().map(|r| r.row.estimated_bytes()).sum::<usize>();
                ensure!(
                    bytes <= inner.config.hot_max_bytes,
                    "compaction working set exceeds hot memory budget"
                );
                rows.extend(batch.iter().cloned());
            }
            let files = write_partitioned(inner, &table.config, &rows)?;
            for seg in selected {
                retired.insert(seg.id.clone());
            }
            replacements.extend(files);
            compacted += 1;
        }
        table.segments.retain(|seg| !retired.contains(&seg.id));
        table.segments.extend(replacements);
    }
    if compacted > 0 {
        persist_manifest(inner, s, next)?;
        gc_locked(inner, s)?;
    }
    Ok(compacted)
}

fn archive_locked(inner: &Inner, s: &mut State, now_us: i64) -> Result<usize> {
    if inner.readers.load(Ordering::SeqCst) > 0 {
        return Ok(0);
    }
    if s.remote_head.is_none() {
        return Ok(0);
    }
    let remote_ids = &s.remote_segment_ids;
    let mut removed = 0;
    for table in s.catalog.tables.values() {
        let Some(age) = table.config.archive_after_us else {
            continue;
        };
        let cutoff = checked_cutoff(now_us, age);
        for seg in &table.segments {
            if seg.max_timestamp_us >= cutoff || !remote_ids.contains(&seg.id) {
                continue;
            }
            let path = inner.root.join(seg.key());
            if path.exists() {
                fs::remove_file(path)?;
                removed += 1;
            }
        }
    }
    crate::wal::sync_dir(&inner.root.join("segments"))?;
    Ok(removed)
}
