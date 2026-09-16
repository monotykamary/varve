use crate::engine::*;
use crate::model::checked_cutoff;
use crate::segment;
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
        checkpoint_prepared(self)?;
        let result = compact_prepared(self, true);
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
            let (checkpoint_due, checkpoint_had_hot_work) = {
                let mut s = self.lock()?;
                healthy(&s)?;
                advance_idempotency_floors(&mut s, now_us);
                (
                    maintenance_checkpoint_due(&self.inner, &s, now_us),
                    s.catalog.checkpoint_sequence != s.sequence
                        || s.hot.values().any(|rows| !rows.is_empty()),
                )
            };
            let mut checkpoint_recheck = true;
            if checkpoint_due {
                report.flushed = checkpoint_prepared_scheduled(self)?;
                // A stale/contended expensive candidate is deferred for this tick.
                // Recheck only after no initial hot work or a successful publication.
                checkpoint_recheck = !checkpoint_had_hot_work || report.flushed;
            }
            let maintenance_pin = prefetch_maintenance_segments(self, Some(now_us))?;
            if checkpoint_recheck {
                let checkpoint_due = {
                    let s = self.lock()?;
                    healthy(&s)?;
                    maintenance_checkpoint_due(&self.inner, &s, now_us)
                };
                if checkpoint_due {
                    report.flushed |= checkpoint_prepared_scheduled(self)?;
                }
            }
            {
                let mut s = self.lock()?;
                healthy(&s)?;
                // A write racing after this recheck makes retention defer rather
                // than publish over a dirty WAL tail.
                report.expired_rows = retention_locked(&self.inner, &mut s, now_us)?;
            }
            drop(maintenance_pin);
            report.compacted = compact_prepared(self, false)?;
            let (ship_due, resume_gc, vacuum_due) = {
                let mut s = self.lock()?;
                healthy(&s)?;
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

fn has_retention(s: &State) -> bool {
    s.catalog.tables.values().any(|table| {
        table.config.retention_us.is_some() || table.config.rollup_retention_us.is_some()
    })
}

fn maintenance_checkpoint_due(inner: &Inner, s: &State, now_us: i64) -> bool {
    has_retention(s)
        || s.first_hot_us
            .is_none_or(|at| now_us.saturating_sub(at) >= inner.config.flush_interval_us)
}

// Select one deterministic group per preparation. Its decoded rows are bounded by
// hot_max_rows/hot_max_bytes; later groups remain eligible for the next tick.
fn compaction_plan(inner: &Inner, s: &State) -> BTreeMap<String, Vec<Vec<Segment>>> {
    for (name, table) in &s.catalog.tables {
        let mut groups: BTreeMap<(u32, i64), Vec<&Segment>> = BTreeMap::new();
        for seg in &table.segments {
            if seg.rows < inner.config.segment_rows as u64 {
                groups
                    .entry((seg.shard, seg.window_us))
                    .or_default()
                    .push(seg);
            }
        }
        for candidates in groups.values() {
            let mut row_count = 0usize;
            let mut decoded_bytes = 0u64;
            let mut count = 0;
            for seg in candidates {
                if row_count.saturating_add(seg.rows as usize) > inner.config.hot_max_rows
                    || decoded_bytes
                        .checked_add(seg.decoded_bytes)
                        .is_none_or(|bytes| bytes > inner.config.hot_max_bytes as u64)
                {
                    break;
                }
                row_count += seg.rows as usize;
                decoded_bytes += seg.decoded_bytes;
                count += 1;
            }
            if count < inner.config.compact_min_segments
                || row_count.div_ceil(inner.config.segment_rows) >= count
            {
                continue;
            }
            let selected = candidates[..count]
                .iter()
                .map(|seg| (*seg).clone())
                .collect();
            return BTreeMap::from([(name.clone(), vec![selected])]);
        }
    }
    BTreeMap::new()
}

fn prefetch_maintenance_segments(db: &Database, now_us: Option<i64>) -> Result<Option<Pin>> {
    let (segments, pin) = {
        let s = db.lock()?;
        healthy(&s)?;
        // Compaction cannot publish over a dirty checkpoint. If this tick will not
        // checkpoint (and has no retention), there is nothing to materialize.
        if s.catalog.checkpoint_sequence != s.sequence
            && now_us.is_some_and(|now| !maintenance_checkpoint_due(&db.inner, &s, now))
        {
            return Ok(None);
        }
        let compaction_ids: BTreeSet<_> = compaction_plan(&db.inner, &s)
            .into_values()
            .flatten()
            .flatten()
            .map(|segment| segment.id)
            .collect();
        let segments: Vec<_> =
            s.catalog
                .tables
                .values()
                .flat_map(|table| {
                    table.segments.iter().filter(|segment| {
                        let needed_for_retention = now_us
                            .zip(table.config.retention_us)
                            .is_some_and(|(now, age)| {
                                let cutoff = checked_cutoff(now, age)
                                    .max(table.cutoff_us.unwrap_or(i64::MIN));
                                segment.min_timestamp_us < cutoff
                                    && segment.max_timestamp_us >= cutoff
                            });
                        let needed_for_compaction = compaction_ids.contains(&segment.id);
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
    if !has_retention(s) || s.catalog.checkpoint_sequence != s.sequence {
        return Ok(0);
    }
    let _derived_working = reserve_catalog_clone(s, &inner.config)?;
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

fn compact_prepared(db: &Database, allow_remote_fetch: bool) -> Result<usize> {
    let _preparation_gate = db.lock_maintenance_preparation()?;
    let (generation, table_name, table_config, selected, mut pin) = {
        let s = db.lock()?;
        healthy(&s)?;
        if s.catalog.checkpoint_sequence != s.sequence {
            return Ok(0);
        }
        let Some((table_name, groups)) = compaction_plan(&db.inner, &s).into_iter().next() else {
            return Ok(0);
        };
        let selected = groups.into_iter().next().context("empty compaction plan")?;
        let table_config = s
            .catalog
            .tables
            .get(&table_name)
            .context("compaction table disappeared")?
            .config
            .clone();
        // Finish any in-flight cache eviction before publishing reader pins.
        let _disk = lock_disk_admission(&db.inner)?;
        let ids = selected.iter().map(|segment| segment.id.clone()).collect();
        let pin = Pin::new(&db.inner, ids)?;
        (s.generation, table_name, table_config, selected, pin)
    };

    #[cfg(feature = "fault-injection")]
    db.block_maintenance_test_hook(MaintenanceHookPhase::CompactionPrepare)?;
    if !allow_remote_fetch
        && selected.iter().any(|segment| {
            !db.inner.root.join(segment.key()).exists()
                && !db
                    .inner
                    .root
                    .join("cache")
                    .join(format!("{}.parquet", segment.id))
                    .exists()
        })
    {
        return Ok(0);
    }
    let prepare_timer = db
        .inner
        .metrics
        .timer(crate::metrics::Phase::CompactionPrepare);
    let preparation = (|| {
        let expected_bytes = selected
            .iter()
            .try_fold(0u64, |sum, segment| sum.checked_add(segment.decoded_bytes))
            .context("compaction decoded-size overflow")?;
        ensure!(
            expected_bytes <= db.inner.config.hot_max_bytes as u64,
            "compaction working set exceeds hot memory budget"
        );
        let mut rows = Vec::new();
        let mut bytes = 0usize;
        for descriptor in &selected {
            let path = resolve_segment(&db.inner, descriptor)?;
            let batch = segment::read(&path)?;
            ensure!(
                batch.len() as u64 == descriptor.rows,
                "segment row-count mismatch"
            );
            let decoded = batch
                .iter()
                .map(|stored| stored.row.estimated_bytes())
                .sum::<usize>();
            ensure!(
                decoded as u64 == descriptor.decoded_bytes,
                "segment decoded-size metadata mismatch"
            );
            bytes = bytes
                .checked_add(decoded)
                .context("compaction working-set overflow")?;
            ensure!(
                bytes <= db.inner.config.hot_max_bytes,
                "compaction working set exceeds hot memory budget"
            );
            rows.extend(batch);
        }
        write_partitioned_with_pin(&db.inner, &table_config, &rows, Some(&mut pin))
    })();
    drop(prepare_timer);
    let replacements = match preparation {
        Ok(replacements) => replacements,
        Err(error) => {
            drop(pin);
            let s = db.lock()?;
            if s.fenced.is_none() {
                cleanup_unpublished_segments(&db.inner, &s)?;
            }
            return Err(error);
        }
    };

    let mut s = db.lock()?;
    healthy(&s)?;
    let publish_timer = db
        .inner
        .metrics
        .timer(crate::metrics::Phase::CompactionPublish);
    if s.catalog.checkpoint_sequence != s.sequence {
        drop(publish_timer);
        drop(pin);
        cleanup_unpublished_segments(&db.inner, &s)?;
        return Ok(0);
    }
    let retired: BTreeSet<_> = selected.iter().map(|segment| segment.id.as_str()).collect();
    let current = s
        .catalog
        .tables
        .get(&table_name)
        .context("compaction table disappeared")?;
    let inputs_match = current.config == table_config
        && selected
            .iter()
            .all(|expected| current.segments.iter().any(|segment| segment == expected));
    if !inputs_match {
        drop(publish_timer);
        drop(pin);
        cleanup_unpublished_segments(&db.inner, &s)?;
        return Ok(0);
    }
    // A newer checkpoint may append unrelated immutable descriptors. Exact input
    // revalidation makes rebasing the replacement onto that generation safe.
    let _generation_changed = s.generation != generation;
    let _derived_working = reserve_catalog_clone(&s, &db.inner.config)?;
    let mut next = s.catalog.clone();
    let table = next
        .tables
        .get_mut(&table_name)
        .context("compaction table disappeared")?;
    table
        .segments
        .retain(|segment| !retired.contains(segment.id.as_str()));
    table.segments.extend(replacements);
    persist_manifest(&db.inner, &mut s, next)?;
    drop(publish_timer);
    drop(pin);
    gc_locked(&db.inner, &mut s)?;
    Ok(1)
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
