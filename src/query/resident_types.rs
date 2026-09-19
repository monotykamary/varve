use crate::raw_memory::SharedRawRows;
use std::path::PathBuf;

/// Immutable process-local identity. Reusing an ID for different bytes is forbidden.
#[derive(Clone, Debug)]
pub(crate) struct ResidentBatch {
    pub id: String,
    pub rows: SharedRawRows,
    pub charged_bytes: usize,
    // Set only for rows from the engine's verified content-addressed segment cache.
    pub verified_segment: bool,
    // Inclusive bounds, with min > max for an empty test batch. The fixed
    // 16 bytes fit the existing per-row 128-byte logical allocation charge;
    // empty batches are covered by ResidentRequest's per-batch metadata charge.
    pub min_timestamp_us: i64,
    pub max_timestamp_us: i64,
}

impl ResidentBatch {
    pub(crate) fn new(id: String, rows: SharedRawRows, charged_bytes: usize) -> Self {
        let (mut min_timestamp_us, mut max_timestamp_us) = (i64::MAX, i64::MIN);
        for row in rows.iter() {
            min_timestamp_us = min_timestamp_us.min(row.row.timestamp_us);
            max_timestamp_us = max_timestamp_us.max(row.row.timestamp_us);
        }
        Self {
            id,
            rows,
            charged_bytes,
            verified_segment: false,
            min_timestamp_us,
            max_timestamp_us,
        }
    }

    pub(crate) fn pinned(&self) -> Self {
        Self {
            rows: self.rows.pin(),
            ..self.clone()
        }
    }

    /// Keep whole immutable batches unless the existing proof excludes them.
    pub(crate) fn overlaps(
        &self,
        plan: Option<&crate::plan::ScanPlan>,
        cutoff: Option<i64>,
    ) -> bool {
        self.min_timestamp_us <= self.max_timestamp_us
            && cutoff.is_none_or(|cutoff| self.max_timestamp_us >= cutoff)
            && plan.is_none_or(|plan| {
                !plan.rollup
                    && !plan.empty
                    && plan
                        .start_us
                        .is_none_or(|start| self.max_timestamp_us >= start)
                    && plan.end_us.is_none_or(|end| self.min_timestamp_us < end)
            })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ResidentFile {
    pub id: String,
    pub path: PathBuf,
    pub rows: usize,
    pub charged_bytes: usize,
    pub min_timestamp_us: i64,
    pub max_timestamp_us: i64,
}

#[derive(Clone, Debug)]
pub(crate) struct ResidentTable {
    pub name: String,
    pub batches: Vec<ResidentBatch>,
    pub files: Vec<ResidentFile>,
}

#[derive(Clone, Debug)]
pub(crate) struct ResidentLineage {
    pub name: String,
    pub raw_stamp: u64,
    pub ids: Vec<String>,
}

/// Query input cache identity, never an acknowledgement or a durable frontier.
#[derive(Clone, Debug)]
pub(crate) struct ResidentSnapshot {
    pub namespace: String,
    pub sequence: u64,
    pub tables: Vec<ResidentTable>,
    pub lineage: Vec<ResidentLineage>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Row, StoredRow};
    use crate::plan::ScanPlan;

    fn batch(timestamps: &[i64]) -> ResidentBatch {
        let rows: Vec<_> = timestamps
            .iter()
            .map(|&timestamp_us| StoredRow {
                row: Row {
                    timestamp_us,
                    tenant: "t".into(),
                    series: "s".into(),
                    value: 1.0,
                    tags: Default::default(),
                },
                sequence: 1,
                ordinal: 0,
            })
            .collect();
        let charge = rows.iter().map(|row| row.row.estimated_bytes()).sum();
        ResidentBatch::new("a".repeat(64), SharedRawRows::test_rows(rows), charge)
    }

    #[test]
    fn immutable_bounds_fit_existing_logical_metadata_allowances() {
        for timestamps in [vec![], vec![1], vec![9, -3, 2]] {
            let batch = batch(&timestamps);
            // Even the longest engine ID (a segment digest) plus the batch
            // descriptor fits the existing fixed row allowance. This is a
            // logical metadata bound, not an allocator/RSS accounting claim.
            let metadata = std::mem::size_of::<ResidentBatch>() + batch.id.len();
            assert!(metadata <= 128);
            let row_charge: usize = batch.rows.iter().map(|row| row.row.estimated_bytes()).sum();
            assert_eq!(batch.charged_bytes, row_charge);
            if timestamps.is_empty() {
                assert!(metadata <= 256); // ResidentRequest also charges empty batches.
                assert!(!batch.overlaps(None, None));
            } else {
                assert_eq!(batch.min_timestamp_us, *timestamps.iter().min().unwrap());
                assert_eq!(batch.max_timestamp_us, *timestamps.iter().max().unwrap());
                assert!(metadata <= batch.charged_bytes);
                assert!(batch.overlaps(None, None));
            }
            let cloned = batch.clone();
            assert_eq!(cloned.id, batch.id);
            assert!(SharedRawRows::ptr_eq(&cloned.rows, &batch.rows));
            assert_eq!(cloned.charged_bytes, batch.charged_bytes);
        }
    }

    #[test]
    fn singleton_bounds_and_retention_are_exact_at_signed_extremes() {
        let limits = [None, Some(i64::MIN), Some(-1), Some(0), Some(i64::MAX)];
        for timestamp in [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX] {
            let batch = batch(&[timestamp]);
            for cutoff in limits {
                assert_eq!(
                    batch.overlaps(None, cutoff),
                    cutoff.is_none_or(|c| timestamp >= c)
                );
                for start_us in limits {
                    for end_us in limits {
                        let mut plan = ScanPlan {
                            table: "metrics".into(),
                            rollup: false,
                            rollup_width_us: None,
                            tenant: None,
                            series: None,
                            start_us,
                            end_us,
                            empty: false,
                        };
                        assert_eq!(
                            batch.overlaps(Some(&plan), cutoff),
                            cutoff.is_none_or(|c| timestamp >= c)
                                && start_us.is_none_or(|start| timestamp >= start)
                                && end_us.is_none_or(|end| timestamp < end)
                        );
                        plan.empty = true;
                        assert!(!batch.overlaps(Some(&plan), cutoff));
                        plan.empty = false;
                        plan.rollup = true;
                        assert!(!batch.overlaps(Some(&plan), cutoff));
                    }
                }
            }
        }
    }

    #[test]
    fn partial_overlap_keeps_the_original_whole_allocation() {
        let batch = batch(&[i64::MAX, i64::MIN, 0]);
        let plan = crate::plan::plan(
            "SELECT * FROM metrics WHERE timestamp_us >= 0 AND timestamp_us < 1",
            &["metrics".into()],
        )
        .unwrap();
        assert!(batch.overlaps(Some(&plan), Some(0)));
        let selected = batch.clone();
        assert!(SharedRawRows::ptr_eq(&batch.rows, &selected.rows));
        assert_eq!(selected.rows.len(), 3);
        assert_eq!(selected.id, batch.id);
    }
}
