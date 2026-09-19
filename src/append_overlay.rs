//! Private append preparation. The baseline is immutable for the entire attempt.
use super::*;

pub(super) struct AppendOverlay<'a> {
    pub(super) base: &'a State,
    pub(super) delta: AppendDelta,
    pub(super) items: Vec<&'a PreparedWrite>,
    pub(super) outcomes: Vec<Result<WriteReceipt>>,
    pub(super) ordinals: Vec<usize>,
    pub(super) accepted_rows: usize,
    private_bytes: usize,
}

pub(super) struct AppendDelta {
    pub(super) tables: BTreeMap<String, TouchedTable>,
    pub(super) batches: Vec<(String, ResidentBatch)>,
    pub(super) floors: BTreeMap<String, i64>,
    pub(super) durable_floors: BTreeMap<String, i64>,
    pub(super) hot_bytes: usize,
    pub(super) hot_rows: usize,
    pub(super) receipts: usize,
    pub(super) rollups: usize,
    pub(super) metadata_bytes: usize,
    pub(super) resident: usize,
    pub(super) root_bound: usize,
    pub(super) encoded_bound: usize,
    pub(super) oversized_sets: usize,
    pub(super) working: Vec<DerivedWorking>,
}

pub(super) struct TouchedTable {
    pub(super) receipts: BTreeMap<String, ReceiptEntry>,
    pub(super) rollups: BTreeMap<String, RollupRow>,
    pub(super) accounting: TableAccounting,
}

#[derive(Clone, Copy)]
pub(super) struct AppendView<'a> {
    pub(super) base: &'a State,
    delta: Option<&'a AppendDelta>,
}

impl std::ops::Deref for AppendView<'_> {
    type Target = State;
    fn deref(&self) -> &State {
        self.base
    }
}

impl<'a> AppendView<'a> {
    pub(super) fn committed(base: &'a State) -> Self {
        Self { base, delta: None }
    }
    pub(super) fn receipts_nonempty(&self, table: &str) -> bool {
        self.delta
            .and_then(|d| d.tables.get(table))
            .is_some_and(|t| !t.receipts.is_empty())
            || self
                .base
                .catalog
                .tables
                .get(table)
                .is_some_and(|t| !t.receipts.is_empty())
    }
    pub(super) fn rollups_nonempty(&self, table: &str) -> bool {
        self.delta
            .and_then(|d| d.tables.get(table))
            .is_some_and(|t| !t.rollups.is_empty())
            || self
                .base
                .catalog
                .tables
                .get(table)
                .is_some_and(|t| !t.rollups.is_empty())
    }
    pub(super) fn receipt(&self, table: &str, id: &str) -> Option<&ReceiptEntry> {
        self.delta
            .and_then(|d| d.tables.get(table))
            .and_then(|t| t.receipts.get(id))
            .or_else(|| self.base.catalog.tables.get(table)?.receipts.get(id))
    }
    pub(super) fn rollup(&self, table: &str, key: &str) -> Option<&RollupRow> {
        self.delta
            .and_then(|d| d.tables.get(table))
            .and_then(|t| t.rollups.get(key))
            .or_else(|| self.base.catalog.tables.get(table)?.rollups.get(key))
    }
    pub(super) fn accounting(&self, table: &str) -> Result<&TableAccounting> {
        self.delta
            .and_then(|d| d.tables.get(table))
            .map(|t| &t.accounting)
            .or_else(|| self.base.derived_accounting.tables.get(table))
            .context("missing derived accounting")
    }
    pub(super) fn hot_bytes(&self) -> usize {
        self.delta.map_or(self.base.hot_bytes, |d| d.hot_bytes)
    }
    pub(super) fn hot_rows(&self) -> usize {
        self.delta
            .map_or_else(|| hot_count(self.base), |d| d.hot_rows)
    }
    pub(super) fn receipts(&self) -> usize {
        self.delta.map_or_else(
            || {
                self.base
                    .catalog
                    .tables
                    .values()
                    .map(|t| t.receipts.len())
                    .sum()
            },
            |d| d.receipts,
        )
    }
    pub(super) fn rollups(&self) -> usize {
        self.delta.map_or_else(
            || {
                self.base
                    .catalog
                    .tables
                    .values()
                    .map(|t| t.rollups.len())
                    .sum()
            },
            |d| d.rollups,
        )
    }
    pub(super) fn metadata_bytes(&self) -> usize {
        self.delta
            .map_or(self.base.metadata_bytes, |d| d.metadata_bytes)
    }
    pub(super) fn resident(&self) -> usize {
        self.delta
            .map_or(self.base.derived_resident_bytes, |d| d.resident)
    }
    pub(super) fn root_bound(&self) -> usize {
        self.delta
            .map_or(self.base.derived_accounting.root_bound, |d| d.root_bound)
    }
    pub(super) fn encoded_bound(&self) -> usize {
        self.delta
            .map_or(self.base.derived_accounting.encoded_bound, |d| {
                d.encoded_bound
            })
    }
    pub(super) fn oversized_sets(&self) -> usize {
        self.delta
            .map_or(self.base.derived_accounting.oversized_sets, |d| {
                d.oversized_sets
            })
    }
}

impl<'a> AppendOverlay<'a> {
    pub(super) fn new(base: &'a State) -> Self {
        let view = AppendView::committed(base);
        Self {
            base,
            items: Vec::new(),
            outcomes: Vec::new(),
            ordinals: Vec::new(),
            accepted_rows: 0,
            private_bytes: 0,
            delta: AppendDelta {
                tables: BTreeMap::new(),
                batches: Vec::new(),
                floors: BTreeMap::new(),
                durable_floors: BTreeMap::new(),
                hot_bytes: view.hot_bytes(),
                hot_rows: view.hot_rows(),
                receipts: view.receipts(),
                rollups: view.rollups(),
                metadata_bytes: view.metadata_bytes(),
                resident: view.resident(),
                root_bound: view.root_bound(),
                encoded_bound: view.encoded_bound(),
                oversized_sets: view.oversized_sets(),
                working: Vec::new(),
            },
        }
    }
    pub(super) fn view(&self) -> AppendView<'_> {
        AppendView {
            base: self.base,
            delta: Some(&self.delta),
        }
    }

    pub(super) fn retry(&mut self, item: &wal::AppendItem) -> Result<Option<WriteReceipt>> {
        let table = self
            .base
            .catalog
            .tables
            .get(&item.table)
            .context("unknown table")?;
        let floor = self
            .delta
            .floors
            .get(&item.table)
            .or_else(|| self.base.idempotency_floors.get(&item.table))
            .copied();
        let (receipt, floor) = retry_with_receipt(
            table,
            self.view().receipt(&item.table, &item.request_id),
            floor,
            item,
            &item.digest,
        )?;
        if let Some(floor) = floor {
            self.delta.floors.insert(item.table.clone(), floor);
        }
        Ok(receipt)
    }

    pub(super) fn durable_duplicate(&mut self, item: &wal::AppendItem) {
        if let Some(floor) = duplicate_clock_floor(self.base, item) {
            let baseline = self
                .base
                .idempotency_floors
                .get(&item.table)
                .copied()
                .unwrap_or(i64::MIN);
            let durable = self
                .delta
                .durable_floors
                .entry(item.table.clone())
                .or_insert(baseline);
            *durable = (*durable).max(floor);
            let live = self
                .delta
                .floors
                .entry(item.table.clone())
                .or_insert(baseline);
            *live = (*live).max(floor);
        }
    }

    pub(super) fn idempotency_checkpoint_due(&self) -> bool {
        self.base.catalog.tables.iter().any(|(name, table)| {
            table.config.idempotency_window_us.is_some()
                && (table.receipts.values().any(|r| r.issued_us.is_none())
                    || self
                        .delta
                        .floors
                        .get(name)
                        .or_else(|| self.base.idempotency_floors.get(name))
                        .is_some_and(|floor| {
                            Some(*floor) != table.idempotency_floor_us
                                || table
                                    .receipts
                                    .values()
                                    .any(|r| r.issued_us.is_some_and(|issued| issued < *floor))
                        }))
        })
    }

    pub(super) fn check_lateness(&self, item: &PreparedWrite) -> Result<()> {
        let table = self
            .base
            .catalog
            .tables
            .get(&item.table)
            .context("unknown table")?;
        let now_us = item.now_us.context("live group request missing clock")?;
        if let Some(age) = table.config.late_after_us {
            for row in &item.rows {
                ensure!(
                    row.timestamp_us >= checked_cutoff(now_us, age),
                    "row exceeds allowed lateness"
                );
            }
        }
        Ok(())
    }

    pub(super) fn prepare_group_item(
        &mut self,
        item: &'a PreparedWrite,
        sequence: u64,
        config: &Config,
        metrics: Option<&Metrics>,
    ) -> Result<WriteReceipt> {
        if let Some(receipt) = self.retry(item)? {
            return Ok(receipt);
        }
        self.check_lateness(item)?;
        // Rejections and duplicates do not consume ordinals. Establish the global
        // accepted-row offset before building any stored rows for this request.
        let prepared = preflight_append(
            &self.view(),
            item.validated(),
            sequence,
            self.accepted_rows,
            Some(GROUP_PROOF_RESERVATION),
            config,
            metrics,
        )?
        .finish(&self.view(), config)?;
        self.accept(item, prepared, WriteMode::Group, config)
    }

    pub(super) fn accept(
        &mut self,
        item: &'a PreparedWrite,
        prepared: PreparedAppend,
        mode: WriteMode,
        config: &Config,
    ) -> Result<WriteReceipt> {
        self.private_bytes = mode.admit_private(self.private_bytes, &prepared, config)?;
        let receipt = receipt_for(&prepared.receipt, false);
        self.ordinals.push(self.accepted_rows);
        self.accepted_rows += item.rows.len();
        self.items.push(item);
        self.push(prepared);
        Ok(receipt)
    }

    pub(super) fn push(&mut self, prepared: PreparedAppend) {
        let old = self
            .view()
            .accounting(&prepared.table)
            .expect("prepared accounting")
            .clone();
        self.delta.root_bound = self
            .delta
            .root_bound
            .saturating_sub(old.root_bound)
            .saturating_add(prepared.derived.accounting.root_bound);
        self.delta.encoded_bound = self
            .delta
            .encoded_bound
            .saturating_sub(old.encoded_bound)
            .saturating_add(prepared.derived.accounting.encoded_bound);
        self.delta.oversized_sets = self.delta.oversized_sets - old.oversized_sets
            + prepared.derived.accounting.oversized_sets;
        self.delta.rollups += prepared
            .updates
            .keys()
            .filter(|key| self.view().rollup(&prepared.table, key).is_none())
            .count();
        self.delta.receipts += 1;
        self.delta.hot_rows += prepared.receipt.rows;
        self.delta.hot_bytes += prepared.bytes;
        self.delta.metadata_bytes = prepared.metadata_bytes;
        self.delta.resident = prepared.derived.resident;
        let table = self
            .delta
            .tables
            .entry(prepared.table.clone())
            .or_insert_with(|| TouchedTable {
                receipts: BTreeMap::new(),
                rollups: BTreeMap::new(),
                accounting: TableAccounting::default(),
            });
        table.receipts.insert(prepared.request_id, prepared.receipt);
        table.rollups.extend(prepared.updates);
        table.accounting = prepared.derived.accounting;
        if let Some(batch) = prepared.batch {
            self.delta.batches.push((prepared.table, batch));
        }
        self.delta.working.push(prepared.derived.working);
    }

    pub(super) fn fingerprint(&mut self, sequence: u64, fingerprint: Option<&str>) -> Result<()> {
        for table in self.delta.tables.values_mut() {
            for receipt in table.receipts.values_mut() {
                ensure!(
                    receipt.sequence == sequence,
                    "staged group sequence mismatch"
                );
                ensure!(
                    receipt.group_fingerprint.as_ref().map(String::len)
                        == fingerprint.map(str::len),
                    "group proof reservation mismatch"
                );
                receipt.group_fingerprint = fingerprint.map(str::to_owned);
            }
        }
        Ok(())
    }

    pub(super) fn into_pending(self) -> PendingAppend {
        let bytes = self
            .delta
            .resident
            .saturating_sub(self.base.derived_resident_bytes);
        self.base.derived_working.fetch_add(bytes, Ordering::SeqCst);
        PendingAppend {
            delta: self.delta,
            _resident_growth: DerivedWorking {
                counter: Arc::clone(&self.base.derived_working),
                bytes,
            },
        }
    }
}
