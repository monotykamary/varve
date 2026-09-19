//! Private touched-delta ownership across the durable append boundary.
use super::*;

/// A real, exclusively authorized private database epoch, not static validation.
/// Materialized row owners, frame credit and derived reservations survive the
/// handoff; consumed inputs are replaced by compact independently durable clocks.
pub(crate) struct PreparedEpoch {
    pub(super) db: Database,
    pub(super) results: Vec<Result<WriteReceipt>>,
    pub(super) duplicate_floors: BTreeMap<String, i64>,
    pub(super) publication: Option<PreparedPublication>,
}

/// Established BEFORE detaching credit or consuming even the first input. Field
/// order is deliberate: encoded bytes die before inputs or ANY frame envelope.
/// Keeping all inputs here also covers a failed split partway through detachment.
/// Once detached, consuming iterators may unwind without releasing frame credit.
pub(super) struct MaterializingEpoch {
    pub(super) encoded: Option<wal::EncodedRecord>,
    pub(super) inputs: Vec<OwnedGroupRequest>,
    pub(super) pending: Option<PendingAppend>,
    pub(super) envelopes: Vec<crate::raw_memory::RawReservation>,
}
impl Drop for MaterializingEpoch {
    fn drop(&mut self) {
        #[cfg(test)]
        if let Some(encoded) = &self.encoded {
            raw_pressure_tests::observe_frame_cleanup(encoded);
        }
        // All production cleanup is ordinary field drop in the order above.
    }
}

pub(super) struct PreparedPublication {
    pub(super) pending: PendingAppend,
    pub(super) encoded: wal::EncodedRecord,
    pub(super) _raw_envelopes: Vec<crate::raw_memory::RawReservation>,
    pub(super) sequence: u64,
    pub(super) generation: u64,
    pub(super) published_now: i64,
    pub(super) floors_before: BTreeMap<String, Option<i64>>,
    // Drop last: authority outlives all private allocations.
    pub(super) commit: CommitLease,
}

/// Constructible only by sync. Dropping this while uninstalled fences authority,
/// including normal drop and a panic caught by a worker outside this module.
pub(crate) struct DurableEpoch {
    epoch: PreparedEpoch,
    size: Option<usize>,
}

impl PreparedEpoch {
    pub(crate) fn publish(self) -> Vec<Result<WriteReceipt>> {
        self.sync().install()
    }

    pub(crate) fn sync(mut self) -> DurableEpoch {
        let size = if let Some(publication) = self.publication.as_mut() {
            publication.commit.arm();
            match publish_append(&self.db, &publication.encoded) {
                Ok(Ok(size)) => Some(size),
                Ok(Err(error)) => {
                    self.fail(error, true);
                    None
                }
                Err(error) => {
                    self.fail(error, false);
                    None
                }
            }
        } else {
            None
        };
        DurableEpoch { epoch: self, size }
    }

    fn fail(&mut self, error: anyhow::Error, ambiguous: bool) {
        let Some(mut publication) = self.publication.take() else {
            return;
        };
        let message = format!("{error:#}");
        if let Ok(mut s) = self.db.lock() {
            restore_group_floors(&mut s, &publication.floors_before);
            install_duplicate_floors(&mut s, &self.duplicate_floors);
            if ambiguous {
                s.fenced = Some(format!("ambiguous WAL publication: {message}"));
            }
        }
        for result in &mut self.results {
            if result
                .as_ref()
                .is_ok_and(|r| r.sequence == publication.sequence)
            {
                *result = Err(anyhow::anyhow!(message.clone()));
            }
        }
        if !ambiguous {
            publication.commit.disarm();
        }
        // An ambiguous error fences the gate even if State could not be locked.
        drop(publication);
    }
}

impl DurableEpoch {
    pub(crate) fn install(mut self) -> Vec<Result<WriteReceipt>> {
        if let Some(size) = self.size {
            #[cfg(feature = "fault-injection")]
            if let Err(error) = self
                .epoch
                .db
                .block_maintenance_test_hook(MaintenanceHookPhase::EpochBeforeInstall)
            {
                self.epoch.fail(error, true);
                return self.epoch.results;
            }
            let db = self.epoch.db.clone();
            let mut s = match db.lock() {
                Ok(s) => s,
                Err(error) => {
                    self.epoch.fail(error, true);
                    return self.epoch.results;
                }
            };
            // A sync already in flight may be durable, but an observed remote
            // fence forbids installing or acknowledging its pending publication.
            if let Err(error) = healthy(&s) {
                drop(s);
                self.epoch.fail(error, true);
                return self.epoch.results;
            }
            let publication = self.epoch.publication.take().expect("synced epoch");
            let PreparedPublication {
                pending,
                sequence,
                generation,
                published_now,
                mut commit,
                ..
            } = publication;
            wal::failpoint("group_before_apply");
            let install = db.inner.metrics.timer(Phase::CommitInstall);
            pending.install(&mut s);
            s.wal_bytes += size as u64;
            s.sequence = sequence;
            s.generation = generation;
            push_hot_epoch(&mut s, sequence, published_now);
            install_duplicate_floors(&mut s, &self.epoch.duplicate_floors);
            drop(install);
            wal::failpoint("group_applied");
            commit.disarm();
            drop(s);
            drop(commit);
        }
        self.epoch.results
    }
}

pub(super) fn install_duplicate_floors(s: &mut State, floors: &BTreeMap<String, i64>) {
    for (table, floor) in floors {
        let live = s
            .idempotency_floors
            .entry(table.clone())
            .or_insert(i64::MIN);
        *live = (*live).max(*floor);
    }
}
pub(super) struct PendingAppend {
    pub(super) delta: AppendDelta,
    // Private resident growth cannot become spare snapshot capacity during fsync.
    pub(super) _resident_growth: DerivedWorking,
}

impl PendingAppend {
    /// The commit gate excludes structural and append writers. Readers may have
    /// changed caches/pins, which we never replace. No fallible replay follows fsync.
    pub(super) fn install(self, s: &mut State) {
        for (name, touched) in self.delta.tables {
            s.raw_stamps.insert(name.clone(), fresh_raw_stamp());
            let index = s.rollup_indexes.entry(name.clone()).or_default();
            for (key, row) in &touched.rollups {
                index
                    .insert(key, row, usize::MAX)
                    .expect("prevalidated rollup index");
            }
            let table = s.catalog.tables.get_mut(&name).expect("prepared table");
            table.rollups.extend(touched.rollups);
            table.receipts.extend(touched.receipts);
            s.derived_accounting.replace(name, touched.accounting);
        }
        for (table, batch) in self.delta.batches {
            s.hot.entry(table).or_default().push(batch);
        }
        s.hot_bytes = self.delta.hot_bytes;
        s.metadata_bytes = self.delta.metadata_bytes;
        s.derived_resident_bytes = self.delta.resident;
        s.idempotency_floors.extend(self.delta.floors);
    }
}
