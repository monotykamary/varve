use super::super::{CatalogRelation, QueryCatalog, QueryTable, ResidentSnapshot};
use super::ffi::{
    self, Api, DuckBytes, DuckBytesInline, DuckBytesPointer, DuckBytesValue, DuckOpaque, DuckStr,
    Handle, OwnedHandle, TYPE_BIGINT, TYPE_BOOLEAN, TYPE_DOUBLE, TYPE_UBIGINT, TYPE_UINTEGER,
    TYPE_VARCHAR,
};
use crate::model::{RollupRow, StoredRow};
use crate::raw_memory::{RawMemoryBudget, RawReservation};
use anyhow::{Context, Result, bail, ensure};
use serde::Serialize;
use std::ffi::c_void;
use std::io::{self, Write};
use std::mem::size_of;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const OUTPUT_ROWS: usize = 1024;

#[cfg(test)]
static LIVE_OWNERS: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
static CALLBACK_FAILURE: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
pub(super) struct CallbackFailure;
#[cfg(test)]
impl Drop for CallbackFailure {
    fn drop(&mut self) {
        CALLBACK_FAILURE.store(0, Ordering::Release);
    }
}
#[cfg(test)]
pub(super) fn inject_callback_failure(stage: u8) -> CallbackFailure {
    CALLBACK_FAILURE.store(usize::from(stage), Ordering::Release);
    CallbackFailure
}
#[cfg(test)]
fn fail_after_handoff(stage: usize) -> Result<()> {
    let requested = CALLBACK_FAILURE.load(Ordering::Acquire);
    if requested != 0
        && (requested - 1) % 3 + 1 == stage
        && CALLBACK_FAILURE
            .compare_exchange(requested, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    {
        assert!(
            requested <= 3,
            "injected callback panic after ownership transfer"
        );
        bail!("injected callback failure after ownership transfer");
    }
    Ok(())
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ScratchUsage {
    pub reserved_fixed_bytes: usize,
    pub scanner_count: usize,
    pub raw_batch_capacity: usize,
    pub column_capacity: usize,
    pub callback_slots: usize,
    pub position_capacity_per_slot: usize,
    pub tag_capacity_per_slot: usize,
    pub max_concurrent_callbacks: usize,
    pub max_position_rows: usize,
    pub max_tag_json_bytes: usize,
    pub owner_live_highwater: usize,
    pub owner_bytes_highwater: usize,
    pub owner_live_after_close: usize,
    pub owner_bytes_after_close: usize,
    pub available_slots_after_close: usize,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct ScratchPlan {
    threads: usize,
    scanner_count: usize,
    raw_scanner_count: usize,
    raw_batch_capacity: usize,
    column_capacity: usize,
    column_name_capacity: usize,
    max_tags_json_bytes: usize,
}

impl ScratchPlan {
    pub fn for_query(
        tables: &[QueryTable],
        snapshot: Option<&ResidentSnapshot>,
        catalog: &QueryCatalog,
        threads: usize,
    ) -> Result<Self> {
        ensure!(threads > 0, "native scanner threads must be positive");
        let mut plan = Self {
            threads,
            ..Self::default()
        };
        for table in tables {
            let (batch_count, raw_max_tags) = if let Some(snapshot) = snapshot {
                if let Some(resident) = snapshot.tables.iter().find(|item| item.name == table.name)
                {
                    (
                        resident.batches.len(),
                        resident
                            .batches
                            .iter()
                            .map(|batch| batch.rows.max_tags_json_bytes())
                            .max()
                            .unwrap_or(0),
                    )
                } else {
                    (0, 0)
                }
            } else if table.hot.is_empty() {
                (0, 0)
            } else {
                (1, max_serialized_tags(&table.hot)?)
            };
            plan.raw_scanner_count = checked_add(plan.raw_scanner_count, 1)?;
            plan.raw_batch_capacity = checked_add(plan.raw_batch_capacity, batch_count)?;
            plan.max_tags_json_bytes = plan.max_tags_json_bytes.max(raw_max_tags);
            plan.add_columns(&[
                "timestamp_us",
                "tenant",
                "series",
                "value",
                "tags",
                "sequence",
                "ordinal",
            ])?;
            plan.max_tags_json_bytes = plan
                .max_tags_json_bytes
                .max(max_serialized_rollup_tags(&table.rollups)?);
            plan.add_columns(&[
                "width_us",
                "bucket_us",
                "tenant",
                "series",
                "tags",
                "count",
                "sum",
                "min",
                "max",
                "first",
                "last",
                "first_timestamp_us",
                "last_timestamp_us",
                "first_sequence",
                "first_ordinal",
                "last_sequence",
                "last_ordinal",
            ])?;
            plan.scanner_count = checked_add(plan.scanner_count, 2)?;
        }
        for relation in &catalog.relations {
            plan.scanner_count = checked_add(plan.scanner_count, 1)?;
            plan.column_capacity = checked_add(plan.column_capacity, relation.columns.len())?;
            for (name, _) in &relation.columns {
                plan.column_name_capacity = checked_add(plan.column_name_capacity, name.len())?;
            }
        }
        Ok(plan)
    }

    fn add_columns(&mut self, names: &[&str]) -> Result<()> {
        self.column_capacity = checked_add(self.column_capacity, names.len())?;
        for name in names {
            self.column_name_capacity = checked_add(self.column_name_capacity, name.len())?;
        }
        Ok(())
    }

    // This lease covers Rust scanner-owned fixed capacities only: RawBatch
    // handles, columns/names, ScannerSpec allocations and global callback
    // buffers. Caller-owned QueryTable/ResidentSnapshot/file authority, SQL/view
    // construction, DuckDB database/vector/arena memory and returned JSON output
    // remain under their existing independent input/script/DuckDB/output bounds.
    // Variable arrays use exact_slice (Box requested Layout::array), columns use
    // Box<[Column; N]>, and names use Box<str>. There is no Vec capacity/growth
    // assumption or shrink-copy overlap in these covered allocations. Counts
    // below bound REQUESTED Rust layouts; allocator-internal/usable padding is
    // excluded, not a claimed RSS bound. The 64-byte per-allocation allowance
    // covers Arc counters/alignment and conservative bookkeeping, not arbitrary
    // allocator size-class excess.
    fn fixed_bytes(&self) -> Result<usize> {
        if self.scanner_count == 0 {
            return Ok(0);
        }
        let mut bytes = size_of::<ScannerScratch>();
        bytes = checked_add(
            bytes,
            checked_mul(self.threads, size_of::<Option<CallbackScratch>>())?,
        )?;
        bytes = checked_add(
            bytes,
            checked_mul(
                self.threads,
                checked_mul(OUTPUT_ROWS, size_of::<Position>())?,
            )?,
        )?;
        bytes = checked_add(bytes, checked_mul(self.threads, self.max_tags_json_bytes)?)?;
        bytes = checked_add(
            bytes,
            checked_mul(self.raw_batch_capacity, size_of::<RawBatch<'static>>())?,
        )?;
        bytes = checked_add(
            bytes,
            checked_mul(self.column_capacity, size_of::<Column>())?,
        )?;
        bytes = checked_add(bytes, self.column_name_capacity)?;
        bytes = checked_add(
            bytes,
            checked_mul(self.scanner_count, size_of::<ScannerSpec<'static>>())?,
        )?;
        let allocations = checked_add(
            checked_add(2, checked_mul(self.threads, 2)?)?,
            checked_add(
                checked_add(self.scanner_count, self.raw_scanner_count)?,
                checked_add(self.scanner_count, self.column_capacity)?,
            )?,
        )?;
        checked_add(bytes, checked_mul(allocations, 64)?)
    }
}

/// Allocate precisely Layout::array::<T>(len), then initialize in place. The
/// planner reserves this requested layout before calling us; allocator-internal
/// usable size/padding is outside this Rust-owned allocation estimate.
/// Unlike Vec -> boxed-slice conversion, no spare-capacity shrink allocation or
/// old/new backing overlap can arise. Partial initialization is unwind-safe.
pub(super) fn exact_slice<T>(
    len: usize,
    mut make: impl FnMut(usize) -> Result<T>,
) -> Result<Box<[T]>> {
    std::alloc::Layout::array::<T>(len).context("native scanner array layout overflow")?;
    struct Initializing<T> {
        storage: Box<[std::mem::MaybeUninit<T>]>,
        initialized: usize,
    }
    impl<T> Drop for Initializing<T> {
        fn drop(&mut self) {
            // SAFETY: only the initialized prefix contains T. Slice drop glue
            // destroys the remaining prefix even if an element destructor panics.
            unsafe {
                std::ptr::drop_in_place(std::ptr::slice_from_raw_parts_mut(
                    self.storage.as_mut_ptr().cast::<T>(),
                    self.initialized,
                ));
            }
        }
    }
    let mut allocation = Initializing {
        storage: Box::<[T]>::new_uninit_slice(len),
        initialized: 0,
    };
    for index in 0..len {
        allocation.storage[index].write(make(index)?);
        allocation.initialized += 1;
    }
    allocation.initialized = 0;
    let storage = std::mem::replace(&mut allocation.storage, Box::new([]));
    // SAFETY: every element was written once above. The initialization guard no
    // longer owns any initialized element. This changes no borrowed lifetime.
    Ok(unsafe { storage.assume_init() })
}

fn checked_add(left: usize, right: usize) -> Result<usize> {
    left.checked_add(right)
        .context("native scanner scratch size overflow")
}

fn checked_mul(left: usize, right: usize) -> Result<usize> {
    left.checked_mul(right)
        .context("native scanner scratch size overflow")
}

fn max_serialized_tags(rows: &[StoredRow]) -> Result<usize> {
    rows.iter().try_fold(0, |maximum, row| {
        Ok(maximum.max(serialized_json_len(&row.row.tags)?))
    })
}

fn max_serialized_rollup_tags(rows: &[RollupRow]) -> Result<usize> {
    rows.iter().try_fold(0, |maximum, row| {
        Ok(maximum.max(serialized_json_len(&row.tags)?))
    })
}

fn serialized_json_len(value: &impl Serialize) -> Result<usize> {
    crate::raw_memory::json_bytes(value).context("count native scanner JSON bytes")
}

pub(super) struct ScannerScratch {
    budget: RawMemoryBudget,
    slots: Mutex<Box<[Option<CallbackScratch>]>>,
    #[cfg(test)]
    usage: ScratchUsage,
    planned_raw_batch_capacity: usize,
    planned_column_capacity: usize,
    actual_raw_batch_capacity: AtomicUsize,
    actual_column_capacity: AtomicUsize,
    active_callbacks: AtomicUsize,
    max_concurrent_callbacks: AtomicUsize,
    max_position_rows: AtomicUsize,
    max_tag_json_bytes: AtomicUsize,
    owner_live: AtomicUsize,
    owner_live_highwater: AtomicUsize,
    owner_bytes: AtomicUsize,
    owner_bytes_highwater: AtomicUsize,
    #[cfg(test)]
    _cleanup_probe: Option<CleanupProbe>,
}

/// Stack-scoped fixed credit. ScannerScratch must not carry this lease inside its
/// Arc: a last field is still destroyed BEFORE the enclosing Arc block is freed.
/// execute declares this before all specs/session handles and joins/closes them
/// before dropping it. No ScannerScratch Arc may escape that execution scope.
pub(super) struct ScannerLease {
    scratch: Arc<ScannerScratch>,
    _fixed: RawReservation,
}
impl ScannerLease {
    pub(super) fn scratch(&self) -> &Arc<ScannerScratch> {
        &self.scratch
    }
}
impl std::ops::Deref for ScannerLease {
    type Target = Arc<ScannerScratch>;
    fn deref(&self) -> &Self::Target {
        &self.scratch
    }
}

impl ScannerScratch {
    pub fn reserve(plan: ScratchPlan, budget: &RawMemoryBudget) -> Result<Option<ScannerLease>> {
        if plan.scanner_count == 0 {
            return Ok(None);
        }
        let fixed_bytes = plan.fixed_bytes()?;
        let fixed = budget.reserve(fixed_bytes)?;
        let slots = exact_slice(plan.threads, |_| {
            Ok(Some(CallbackScratch {
                positions: exact_slice(OUTPUT_ROWS, |_| Ok(Position::default()))?,
                position_len: 0,
                tags: JsonBuffer {
                    bytes: exact_slice(plan.max_tags_json_bytes, |_| Ok(0))?,
                    len: 0,
                },
            }))
        })?;
        let scratch = Arc::new(Self {
            budget: budget.clone(),
            slots: Mutex::new(slots),
            #[cfg(test)]
            usage: ScratchUsage {
                reserved_fixed_bytes: fixed_bytes,
                scanner_count: plan.scanner_count,
                callback_slots: plan.threads,
                position_capacity_per_slot: OUTPUT_ROWS,
                tag_capacity_per_slot: plan.max_tags_json_bytes,
                ..ScratchUsage::default()
            },
            planned_raw_batch_capacity: plan.raw_batch_capacity,
            planned_column_capacity: plan.column_capacity,
            actual_raw_batch_capacity: AtomicUsize::new(0),
            actual_column_capacity: AtomicUsize::new(0),
            active_callbacks: AtomicUsize::new(0),
            max_concurrent_callbacks: AtomicUsize::new(0),
            max_position_rows: AtomicUsize::new(0),
            max_tag_json_bytes: AtomicUsize::new(0),
            owner_live: AtomicUsize::new(0),
            owner_live_highwater: AtomicUsize::new(0),
            owner_bytes: AtomicUsize::new(0),
            owner_bytes_highwater: AtomicUsize::new(0),
            #[cfg(test)]
            _cleanup_probe: None,
        });
        Ok(Some(ScannerLease {
            scratch,
            _fixed: fixed,
        }))
    }

    fn checkout(self: &Arc<Self>) -> Result<ScratchGuard> {
        let mut slots = self.slots.lock().unwrap_or_else(|error| error.into_inner());
        let (slot, scratch) = slots
            .iter_mut()
            .enumerate()
            .find_map(|(index, scratch)| scratch.take().map(|scratch| (index, scratch)))
            .context("native scanner callback concurrency exceeded configured threads")?;
        drop(slots);
        let active = self.active_callbacks.fetch_add(1, Ordering::AcqRel) + 1;
        update_max(&self.max_concurrent_callbacks, active);
        Ok(ScratchGuard {
            owner: Arc::clone(self),
            slot,
            scratch: Some(scratch),
        })
    }

    fn reserve_owner<T>(self: &Arc<Self>) -> Result<OwnerReservation> {
        let bytes = checked_add(size_of::<T>(), 64)?;
        let reservation = self.budget.reserve(bytes)?;
        let live = self.owner_live.fetch_add(1, Ordering::AcqRel) + 1;
        let live_bytes = self.owner_bytes.fetch_add(bytes, Ordering::AcqRel) + bytes;
        update_max(&self.owner_live_highwater, live);
        update_max(&self.owner_bytes_highwater, live_bytes);
        Ok(OwnerReservation {
            _reservation: reservation,
            bytes,
            scratch: Arc::clone(self),
        })
    }

    fn record_metadata(&self, raw_batches: usize, columns: usize) -> Result<()> {
        let raw_total = checked_atomic_add(
            &self.actual_raw_batch_capacity,
            raw_batches,
            "native scanner raw batch capacity overflow",
        )?;
        let column_total = checked_atomic_add(
            &self.actual_column_capacity,
            columns,
            "native scanner column capacity overflow",
        )?;
        ensure!(
            raw_total <= self.planned_raw_batch_capacity
                && column_total <= self.planned_column_capacity,
            "native scanner metadata exceeded its pre-allocation reservation"
        );
        Ok(())
    }

    fn record_positions(&self, rows: usize) {
        update_max(&self.max_position_rows, rows);
    }

    fn record_tags(&self, bytes: usize) {
        update_max(&self.max_tag_json_bytes, bytes);
    }

    #[cfg(test)]
    pub fn usage(&self) -> ScratchUsage {
        let available_slots = self
            .slots
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .filter(|slot| slot.is_some())
            .count();
        ScratchUsage {
            raw_batch_capacity: self.actual_raw_batch_capacity.load(Ordering::Acquire),
            column_capacity: self.actual_column_capacity.load(Ordering::Acquire),
            max_concurrent_callbacks: self.max_concurrent_callbacks.load(Ordering::Acquire),
            max_position_rows: self.max_position_rows.load(Ordering::Acquire),
            max_tag_json_bytes: self.max_tag_json_bytes.load(Ordering::Acquire),
            owner_live_highwater: self.owner_live_highwater.load(Ordering::Acquire),
            owner_bytes_highwater: self.owner_bytes_highwater.load(Ordering::Acquire),
            owner_live_after_close: self.owner_live.load(Ordering::Acquire),
            owner_bytes_after_close: self.owner_bytes.load(Ordering::Acquire),
            available_slots_after_close: available_slots,
            ..self.usage
        }
    }
}

struct OwnerReservation {
    _reservation: RawReservation,
    bytes: usize,
    scratch: Arc<ScannerScratch>,
}

impl Drop for OwnerReservation {
    fn drop(&mut self) {
        self.scratch.owner_live.fetch_sub(1, Ordering::AcqRel);
        self.scratch
            .owner_bytes
            .fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

fn checked_atomic_add(
    target: &AtomicUsize,
    additional: usize,
    context: &'static str,
) -> Result<usize> {
    let mut current = target.load(Ordering::Acquire);
    loop {
        let next = current.checked_add(additional).context(context)?;
        match target.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return Ok(next),
            Err(observed) => current = observed,
        }
    }
}

fn update_max(target: &AtomicUsize, value: usize) {
    let mut current = target.load(Ordering::Acquire);
    while value > current {
        match target.compare_exchange_weak(current, value, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

struct ScratchGuard {
    owner: Arc<ScannerScratch>,
    slot: usize,
    scratch: Option<CallbackScratch>,
}

impl ScratchGuard {
    fn scratch_mut(&mut self) -> &mut CallbackScratch {
        self.scratch
            .as_mut()
            .expect("native scanner scratch is held")
    }
}

impl Drop for ScratchGuard {
    fn drop(&mut self) {
        if let Some(scratch) = self.scratch.take() {
            self.owner.active_callbacks.fetch_sub(1, Ordering::AcqRel);
            let mut slots = self
                .owner
                .slots
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            debug_assert!(slots[self.slot].is_none());
            slots[self.slot] = Some(scratch);
        }
    }
}

struct CallbackScratch {
    positions: Box<[Position]>,
    position_len: usize,
    tags: JsonBuffer,
}

struct JsonBuffer {
    bytes: Box<[u8]>,
    len: usize,
}

impl JsonBuffer {
    fn serialize(&mut self, value: &impl Serialize) -> Result<&str> {
        let mut writer = SliceWriter {
            bytes: &mut self.bytes,
            written: 0,
        };
        serde_json::to_writer(&mut writer, value).context("encode native scanner JSON")?;
        self.len = writer.written;
        std::str::from_utf8(&self.bytes[..self.len]).context("native scanner JSON is not UTF-8")
    }
}

struct SliceWriter<'a> {
    bytes: &'a mut [u8],
    written: usize,
}

impl Write for SliceWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let end = self
            .written
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("native scanner JSON length overflow"))?;
        if end > self.bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "native scanner JSON exceeded reserved buffer",
            ));
        }
        self.bytes[self.written..end].copy_from_slice(bytes);
        self.written = end;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub(super) struct Column {
    pub name: Box<str>,
    pub kind: ffi::TypeId,
}

pub(super) struct CatalogData<'a> {
    pub columns: Box<[Column]>,
    relation: &'a CatalogRelation,
}

impl<'a> CatalogData<'a> {
    pub fn borrowed(relation: &'a CatalogRelation, columns: Box<[Column]>) -> Self {
        Self { columns, relation }
    }

    fn relation(&self) -> &CatalogRelation {
        self.relation
    }
}

pub(super) struct RollupRows<'a> {
    rows: &'a [RollupRow],
}

impl<'a> RollupRows<'a> {
    fn borrowed(rows: &'a [RollupRow]) -> Self {
        Self { rows }
    }
    fn get(&self, index: usize) -> Option<&RollupRow> {
        self.rows.get(index)
    }
}

pub(super) enum ScannerRows<'a> {
    Raw(Box<[RawBatch<'a>]>),
    Rollup(RollupRows<'a>),
    Catalog(CatalogData<'a>),
}

pub(super) enum RawBatch<'a> {
    Shared(crate::raw_memory::SharedRawRows),
    Borrowed(&'a [StoredRow]),
}

impl<'a> RawBatch<'a> {
    pub fn shared(rows: crate::raw_memory::SharedRawRows) -> Self {
        Self::Shared(rows)
    }
    pub fn borrowed(rows: &'a [StoredRow]) -> Self {
        Self::Borrowed(rows)
    }
    fn len(&self) -> usize {
        match self {
            Self::Shared(rows) => rows.len(),
            Self::Borrowed(rows) => rows.len(),
        }
    }
    fn get(&self, index: usize) -> Option<&StoredRow> {
        match self {
            Self::Shared(rows) => rows.get(index),
            Self::Borrowed(rows) => rows.get(index),
        }
    }
}

pub(super) struct ScannerSpec<'a> {
    columns: Box<[Column]>,
    rows: ScannerRows<'a>,
    threads: usize,
    scratch: Arc<ScannerScratch>,
}

impl<'a> ScannerSpec<'a> {
    pub fn raw(
        batches: Box<[RawBatch<'a>]>,
        threads: usize,
        scratch: Arc<ScannerScratch>,
    ) -> Result<Arc<Self>> {
        let columns = Box::new([
            column("timestamp_us", TYPE_BIGINT),
            column("tenant", TYPE_VARCHAR),
            column("series", TYPE_VARCHAR),
            column("value", TYPE_DOUBLE),
            column("tags", TYPE_VARCHAR),
            column("sequence", TYPE_UBIGINT),
            column("ordinal", TYPE_UINTEGER),
        ]);
        scratch.record_metadata(batches.len(), columns.len())?;
        Ok(Arc::new(Self {
            columns,
            rows: ScannerRows::Raw(batches),
            threads,
            scratch,
        }))
    }

    pub fn rollup(
        rows: &'a [RollupRow],
        threads: usize,
        scratch: Arc<ScannerScratch>,
    ) -> Result<Arc<Self>> {
        let columns = Box::new([
            column("width_us", TYPE_BIGINT),
            column("bucket_us", TYPE_BIGINT),
            column("tenant", TYPE_VARCHAR),
            column("series", TYPE_VARCHAR),
            column("tags", TYPE_VARCHAR),
            column("count", TYPE_UBIGINT),
            column("sum", TYPE_DOUBLE),
            column("min", TYPE_DOUBLE),
            column("max", TYPE_DOUBLE),
            column("first", TYPE_DOUBLE),
            column("last", TYPE_DOUBLE),
            column("first_timestamp_us", TYPE_BIGINT),
            column("last_timestamp_us", TYPE_BIGINT),
            column("first_sequence", TYPE_UBIGINT),
            column("first_ordinal", TYPE_UINTEGER),
            column("last_sequence", TYPE_UBIGINT),
            column("last_ordinal", TYPE_UINTEGER),
        ]);
        scratch.record_metadata(0, columns.len())?;
        Ok(Arc::new(Self {
            columns,
            rows: ScannerRows::Rollup(RollupRows::borrowed(rows)),
            threads,
            scratch,
        }))
    }

    pub fn catalog(
        mut data: CatalogData<'a>,
        threads: usize,
        scratch: Arc<ScannerScratch>,
    ) -> Result<Arc<Self>> {
        let columns = std::mem::take(&mut data.columns);
        scratch.record_metadata(0, columns.len())?;
        Ok(Arc::new(Self {
            columns,
            rows: ScannerRows::Catalog(data),
            threads,
            scratch,
        }))
    }

    fn work_count(&self) -> usize {
        match &self.rows {
            ScannerRows::Raw(batches) => batches.len(),
            ScannerRows::Rollup(rows) => usize::from(!rows.rows.is_empty()),
            ScannerRows::Catalog(data) => usize::from(!data.relation().rows.is_empty()),
        }
    }

    fn cardinality(&self) -> Result<usize> {
        match &self.rows {
            ScannerRows::Raw(batches) => batches.iter().try_fold(0_usize, |total, batch| {
                total
                    .checked_add(batch.len())
                    .context("native scanner cardinality overflow")
            }),
            ScannerRows::Rollup(rows) => Ok(rows.rows.len()),
            ScannerRows::Catalog(data) => Ok(data.relation().rows.len()),
        }
    }

    fn work_len(&self, work: usize) -> usize {
        match &self.rows {
            ScannerRows::Raw(batches) => batches.get(work).map_or(0, |batch| batch.len()),
            ScannerRows::Rollup(rows) => usize::from(work == 0) * rows.rows.len(),
            ScannerRows::Catalog(data) => usize::from(work == 0) * data.relation().rows.len(),
        }
    }

    fn cell<'b>(
        &'b self,
        position: Position,
        column: usize,
        tags: &'b mut JsonBuffer,
    ) -> Result<Cell<'b>> {
        match &self.rows {
            ScannerRows::Raw(batches) => {
                let row = batches
                    .get(position.work)
                    .and_then(|batch| batch.get(position.row))
                    .context("raw scanner cursor is out of bounds")?;
                match column {
                    0 => Ok(Cell::I64(row.row.timestamp_us)),
                    1 => Ok(Cell::String(&row.row.tenant)),
                    2 => Ok(Cell::String(&row.row.series)),
                    3 => Ok(Cell::F64(row.row.value)),
                    4 => Ok(Cell::String(tags.serialize(&row.row.tags)?)),
                    5 => Ok(Cell::U64(row.sequence)),
                    6 => Ok(Cell::U32(row.ordinal)),
                    _ => bail!("unknown raw scanner column {column}"),
                }
            }
            ScannerRows::Rollup(rows) => {
                let row = rows
                    .get(position.row)
                    .context("rollup scanner cursor is out of bounds")?;
                match column {
                    0 => Ok(Cell::I64(row.width_us)),
                    1 => Ok(Cell::I64(row.bucket_us)),
                    2 => Ok(Cell::String(&row.tenant)),
                    3 => Ok(Cell::String(&row.series)),
                    4 => Ok(Cell::String(tags.serialize(&row.tags)?)),
                    5 => Ok(Cell::U64(row.count)),
                    6 => Ok(Cell::F64(row.sum)),
                    7 => Ok(Cell::F64(row.min)),
                    8 => Ok(Cell::F64(row.max)),
                    9 => Ok(Cell::F64(row.first)),
                    10 => Ok(Cell::F64(row.last)),
                    11 => Ok(Cell::I64(row.first_timestamp_us)),
                    12 => Ok(Cell::I64(row.last_timestamp_us)),
                    13 => Ok(Cell::U64(row.first_sequence)),
                    14 => Ok(Cell::U32(row.first_ordinal)),
                    15 => Ok(Cell::U64(row.last_sequence)),
                    16 => Ok(Cell::U32(row.last_ordinal)),
                    _ => bail!("unknown rollup scanner column {column}"),
                }
            }
            ScannerRows::Catalog(data) => {
                let relation = data.relation();
                let row = relation
                    .rows
                    .get(position.row)
                    .context("catalog scanner cursor is out of bounds")?;
                let (name, kind) = relation
                    .columns
                    .get(column)
                    .context("catalog scanner column is out of bounds")?;
                let value = match row {
                    serde_json::Value::Array(values) => values.get(column),
                    serde_json::Value::Object(values) => values.get(name),
                    _ => None,
                }
                .context("validated catalog row changed during native execution")?;
                if value.is_null() {
                    return Ok(Cell::Null);
                }
                match kind.as_str() {
                    "VARCHAR" => Ok(Cell::String(
                        value
                            .as_str()
                            .context("validated catalog VARCHAR changed")?,
                    )),
                    "BIGINT" => Ok(Cell::I64(
                        value.as_i64().context("validated catalog BIGINT changed")?,
                    )),
                    "UBIGINT" => Ok(Cell::U64(
                        value
                            .as_u64()
                            .context("validated catalog UBIGINT changed")?,
                    )),
                    "DOUBLE" => Ok(Cell::F64(
                        value.as_f64().context("validated catalog DOUBLE changed")?,
                    )),
                    "BOOLEAN" => Ok(Cell::Bool(
                        value
                            .as_bool()
                            .context("validated catalog BOOLEAN changed")?,
                    )),
                    _ => bail!("unsupported catalog column type {kind:?}"),
                }
            }
        }
    }
}

fn column(name: &str, kind: ffi::TypeId) -> Column {
    Column {
        name: name.into(),
        kind,
    }
}

struct UserData<'a> {
    spec: Arc<ScannerSpec<'a>>,
    abort: Arc<AtomicBool>,
    reservation: Option<OwnerReservation>,
}

struct BindState<'a> {
    spec: Arc<ScannerSpec<'a>>,
    abort: Arc<AtomicBool>,
    reservation: Option<OwnerReservation>,
}

struct GlobalState<'a> {
    spec: Arc<ScannerSpec<'a>>,
    abort: Arc<AtomicBool>,
    next_work: AtomicUsize,
    reservation: Option<OwnerReservation>,
}

struct LocalState {
    work: Option<usize>,
    row: usize,
    reservation: Option<OwnerReservation>,
    #[cfg(test)]
    _cleanup_probe: Option<CleanupProbe>,
}

#[derive(Clone, Copy, Default)]
struct Position {
    work: usize,
    row: usize,
}

enum Cell<'a> {
    Null,
    Bool(bool),
    I64(i64),
    U32(u32),
    U64(u64),
    F64(f64),
    String(&'a str),
}

/// Registers callbacks that borrow immutable Rust sources.
///
/// # Safety
/// The connection/database must be fully torn down, including all callback
/// owners, before the source lifetime `'a` ends. No callback may outlive it.
pub(super) unsafe fn register<'a>(
    api: &Arc<Api>,
    connection: Handle,
    name: &str,
    spec: Arc<ScannerSpec<'a>>,
    abort: Arc<AtomicBool>,
) -> Result<()> {
    let mut error = ptr::null_mut();
    let mut function = ptr::null_mut();
    let code = unsafe {
        (api.table_function_create_with_connection)(connection, &mut function, &mut error)
    };
    unsafe { api.check(code, &mut error, "create native table function")? };
    let function = OwnedHandle::new(Arc::clone(api), function, api.table_function_destroy);

    let mut name_view = DuckStr::from_bytes(name.as_bytes());
    let code = unsafe { (api.table_function_set_name)(function.get(), &mut name_view, &mut error) };
    unsafe { api.check(code, &mut error, "name native table function")? };

    let reservation = spec.scratch.reserve_owner::<UserData<'_>>()?;
    let user = CallbackBox::new(UserData {
        spec,
        abort,
        reservation: Some(reservation),
    });
    let raw_user = user.into_raw();
    let mut opaque = DuckOpaque {
        ptr: raw_user.cast(),
        destroy: Some(drop_user),
        equals: None,
    };
    let code =
        unsafe { (api.table_function_set_user_data)(function.get(), &mut opaque, &mut error) };
    if code != ffi::ERROR_NONE {
        drop_user(raw_user.cast());
        unsafe { api.check(code, &mut error, "set native table function user data")? };
    }

    for (operation, setter, callback) in [
        (
            "set native bind callback",
            api.table_function_set_bind_callback,
            bind_callback as ffi::Callback,
        ),
        (
            "set native global-init callback",
            api.table_function_set_init_global_callback,
            global_callback as ffi::Callback,
        ),
        (
            "set native local-init callback",
            api.table_function_set_init_local_callback,
            local_callback as ffi::Callback,
        ),
        (
            "set native exec callback",
            api.table_function_set_exec_callback,
            exec_callback as ffi::Callback,
        ),
    ] {
        let code = unsafe { setter(function.get(), callback, &mut error) };
        unsafe { api.check(code, &mut error, operation)? };
    }
    let code =
        unsafe { (api.table_function_set_projection_pushdown)(function.get(), true, &mut error) };
    unsafe { api.check(code, &mut error, "enable native projection pushdown")? };
    let code = unsafe { (api.table_function_register)(function.get(), &mut error) };
    unsafe { api.check(code, &mut error, "register native table function")? };
    Ok(())
}

extern "C" fn bind_callback(info: Handle, context: Handle, error: *mut Handle) {
    callback_boundary(error, || unsafe { bind_impl(info, context, error) });
}

unsafe fn bind_impl(info: Handle, context: Handle, error: *mut Handle) -> Result<()> {
    let api = callback_api()?;
    let mut raw = ptr::null_mut();
    callback_call(
        unsafe { (api.table_function_bind_get_user_data)(info, &mut raw, error) },
        "get native scanner user data",
    )?;
    ensure!(!raw.is_null(), "native scanner user data is null");
    let user = unsafe { &*raw.cast::<UserData<'_>>() };
    for column in &user.spec.columns {
        let mut logical_type = ptr::null_mut();
        callback_call(
            unsafe {
                (api.context_create_type_from_id)(
                    context,
                    column.kind,
                    ptr::null(),
                    ptr::null(),
                    0,
                    &mut logical_type,
                    error,
                )
            },
            "create native scanner result type",
        )?;
        let logical_type =
            OwnedHandle::new(Arc::clone(api), logical_type, api.logical_type_destroy);
        callback_call(
            unsafe {
                (api.table_function_bind_add_result_column)(
                    info,
                    DuckStr::from_bytes(column.name.as_bytes()),
                    logical_type.get(),
                    error,
                )
            },
            "declare native scanner result column",
        )?;
    }

    let reservation = user.spec.scratch.reserve_owner::<BindState<'_>>()?;
    let bind = CallbackBox::new(BindState {
        spec: Arc::clone(&user.spec),
        abort: Arc::clone(&user.abort),
        reservation: Some(reservation),
    });
    let raw_bind = bind.into_raw();
    let mut opaque = DuckOpaque {
        ptr: raw_bind.cast(),
        destroy: Some(drop_bind),
        equals: None,
    };
    let code = unsafe { (api.table_function_bind_set_bind_data)(info, &mut opaque, error) };
    if code != ffi::ERROR_NONE {
        drop_bind(raw_bind.cast());
        callback_call(code, "set native scanner bind data")?;
    }
    #[cfg(test)]
    fail_after_handoff(1)?;
    callback_call(
        unsafe {
            (api.table_function_bind_set_cardinality)(info, user.spec.cardinality()?, true, error)
        },
        "set native scanner cardinality",
    )?;
    Ok(())
}

extern "C" fn global_callback(info: Handle, _context: Handle, error: *mut Handle) {
    callback_boundary(error, || unsafe { global_impl(info, error) });
}

unsafe fn global_impl(info: Handle, error: *mut Handle) -> Result<()> {
    let api = callback_api()?;
    let mut raw = ptr::null_mut();
    callback_call(
        unsafe { (api.table_function_init_global_get_bind_data)(info, &mut raw, error) },
        "get native scanner bind data",
    )?;
    ensure!(!raw.is_null(), "native scanner bind data is null");
    let bind = unsafe { &*raw.cast::<BindState<'_>>() };
    let reservation = bind.spec.scratch.reserve_owner::<GlobalState<'_>>()?;
    let global = CallbackBox::new(GlobalState {
        spec: Arc::clone(&bind.spec),
        abort: Arc::clone(&bind.abort),
        next_work: AtomicUsize::new(0),
        reservation: Some(reservation),
    });
    let raw_global = global.into_raw();
    let mut opaque = DuckOpaque {
        ptr: raw_global.cast(),
        destroy: Some(drop_global),
        equals: None,
    };
    let code =
        unsafe { (api.table_function_init_global_set_global_state)(info, &mut opaque, error) };
    if code != ffi::ERROR_NONE {
        drop_global(raw_global.cast());
        callback_call(code, "set native scanner global state")?;
    }
    #[cfg(test)]
    fail_after_handoff(2)?;
    let work = bind.spec.work_count().max(1);
    let threads = bind.spec.threads.max(1).min(work);
    callback_call(
        unsafe { (api.table_function_init_global_set_max_threads)(info, threads, error) },
        "set native scanner thread bound",
    )?;
    Ok(())
}

extern "C" fn local_callback(info: Handle, _context: Handle, error: *mut Handle) {
    callback_boundary(error, || unsafe { local_impl(info, error) });
}

unsafe fn local_impl(info: Handle, error: *mut Handle) -> Result<()> {
    let api = callback_api()?;
    let mut raw_global = ptr::null_mut();
    callback_call(
        unsafe { (api.table_function_init_local_get_global_state)(info, &mut raw_global, error) },
        "get native scanner global state",
    )?;
    ensure!(!raw_global.is_null(), "native scanner global state is null");
    let global = unsafe { &*raw_global.cast::<GlobalState<'_>>() };
    let reservation = global.spec.scratch.reserve_owner::<LocalState>()?;
    let local = CallbackBox::new(LocalState {
        work: None,
        row: 0,
        reservation: Some(reservation),
        #[cfg(test)]
        _cleanup_probe: None,
    });
    let raw_local = local.into_raw();
    let mut opaque = DuckOpaque {
        ptr: raw_local.cast(),
        destroy: Some(drop_local),
        equals: None,
    };
    let code = unsafe { (api.table_function_init_local_set_local_state)(info, &mut opaque, error) };
    if code != ffi::ERROR_NONE {
        drop_local(raw_local.cast());
        callback_call(code, "set native scanner local state")?;
    }
    #[cfg(test)]
    fail_after_handoff(3)?;
    Ok(())
}

extern "C" fn exec_callback(info: Handle, _context: Handle, error: *mut Handle) {
    callback_boundary(error, || unsafe { exec_impl(info, error) });
}

unsafe fn exec_impl(info: Handle, error: *mut Handle) -> Result<()> {
    let api = callback_api()?;
    let mut raw_global = ptr::null_mut();
    let mut raw_local = ptr::null_mut();
    let mut output = ptr::null_mut();
    callback_call(
        unsafe { (api.table_function_exec_get_global_state)(info, &mut raw_global, error) },
        "get native scanner global state",
    )?;
    callback_call(
        unsafe { (api.table_function_exec_get_local_state)(info, &mut raw_local, error) },
        "get native scanner local state",
    )?;
    callback_call(
        unsafe { (api.table_function_exec_get_output_chunk)(info, &mut output, error) },
        "get native scanner output chunk",
    )?;
    ensure!(
        !raw_global.is_null() && !raw_local.is_null() && !output.is_null(),
        "native scanner callback state is null"
    );
    let global = unsafe { &*raw_global.cast::<GlobalState<'_>>() };
    let local = unsafe { &mut *raw_local.cast::<LocalState>() };
    ensure!(
        !global.abort.load(Ordering::Acquire),
        "native scanner cancelled"
    );

    // DuckDB's database-wide thread setting is the primary concurrency bound.
    // Checkout enforces the same bound across every registered relation, including
    // repeated bindings and sequential pipelines that retain many LocalStates.
    let mut scratch_guard = global.spec.scratch.checkout()?;
    {
        let scratch = scratch_guard.scratch_mut();
        scratch.position_len = 0;
        while scratch.position_len < scratch.positions.len() {
            if local.work.is_none() {
                let work = global.next_work.fetch_add(1, Ordering::AcqRel);
                if work >= global.spec.work_count() {
                    break;
                }
                local.work = Some(work);
                local.row = 0;
            }
            let work = local.work.expect("native scanner work was assigned");
            let length = global.spec.work_len(work);
            while local.row < length && scratch.position_len < scratch.positions.len() {
                scratch.positions[scratch.position_len] = Position {
                    work,
                    row: local.row,
                };
                scratch.position_len += 1;
                local.row += 1;
            }
            if local.row >= length {
                local.work = None;
            }
            if global.abort.load(Ordering::Acquire) {
                bail!("native scanner cancelled");
            }
        }
    }
    let position_len = scratch_guard.scratch_mut().position_len;
    global.spec.scratch.record_positions(position_len);
    if position_len == 0 {
        return Ok(());
    }

    let mut projected = 0;
    callback_call(
        unsafe { (api.table_function_exec_get_column_count)(info, &mut projected, error) },
        "get native scanner projected column count",
    )?;
    ensure!(projected > 0, "native scanner projected no output vectors");
    for output_column in 0..projected {
        let mut source_column = 0;
        let mut vector = ptr::null_mut();
        callback_call(
            unsafe {
                (api.table_function_exec_get_column_index)(
                    info,
                    output_column,
                    &mut source_column,
                    error,
                )
            },
            "get native scanner projected column",
        )?;
        ensure!(
            source_column < global.spec.columns.len(),
            "native scanner projected an unknown column"
        );
        callback_call(
            unsafe { (api.data_chunk_get_vector)(output, output_column, &mut vector, error) },
            "get native scanner output vector",
        )?;
        let scratch = scratch_guard.scratch_mut();
        unsafe {
            write_vector(
                api,
                vector,
                &global.spec,
                source_column,
                &scratch.positions[..position_len],
                &mut scratch.tags,
                error,
            )?;
        }
    }
    let mut first = ptr::null_mut();
    callback_call(
        unsafe { (api.data_chunk_get_vector)(output, 0, &mut first, error) },
        "get native scanner first output vector",
    )?;
    callback_call(
        unsafe { (api.vector_set_size)(first, position_len, error) },
        "publish native scanner output size",
    )?;
    Ok(())
}

unsafe fn write_vector(
    api: &Api,
    vector: Handle,
    spec: &ScannerSpec<'_>,
    column: usize,
    positions: &[Position],
    tags: &mut JsonBuffer,
    error: *mut Handle,
) -> Result<()> {
    let mut data = ptr::null_mut();
    let mut validity = ptr::null_mut();
    callback_call(
        unsafe { (api.vector_get_data_mutable)(vector, &mut data, error) },
        "get native scanner vector data",
    )?;
    callback_call(
        unsafe { (api.vector_flat_get_validity_mutable)(vector, &mut validity, error) },
        "get native scanner validity",
    )?;
    ensure!(
        !data.is_null() && !validity.is_null(),
        "native scanner vector storage is null"
    );

    let kind = spec.columns[column].kind;
    let mut arena = ptr::null_mut();
    if kind == TYPE_VARCHAR {
        callback_call(
            unsafe { (api.vector_get_arena)(vector, &mut arena, error) },
            "get native scanner string arena",
        )?;
        ensure!(!arena.is_null(), "native scanner string arena is null");
    }

    for (row_index, position) in positions.iter().copied().enumerate() {
        let cell = spec.cell(position, column, tags)?;
        unsafe { set_valid(validity, row_index, !matches!(&cell, Cell::Null)) };
        match (kind, cell) {
            (_, Cell::Null) => {}
            (TYPE_BOOLEAN, Cell::Bool(value)) => unsafe {
                *data.cast::<u8>().add(row_index) = u8::from(value);
            },
            (TYPE_BIGINT, Cell::I64(value)) => unsafe {
                *data.cast::<i64>().add(row_index) = value;
            },
            (TYPE_UINTEGER, Cell::U32(value)) => unsafe {
                *data.cast::<u32>().add(row_index) = value;
            },
            (TYPE_UBIGINT, Cell::U64(value)) => unsafe {
                *data.cast::<u64>().add(row_index) = value;
            },
            (TYPE_DOUBLE, Cell::F64(value)) => unsafe {
                *data.cast::<f64>().add(row_index) = value;
            },
            (TYPE_VARCHAR, Cell::String(value)) => {
                let encoded = unsafe { encode_string(api, arena, value.as_bytes(), error)? };
                unsafe { *data.cast::<DuckBytes>().add(row_index) = encoded };
            }
            _ => bail!("native scanner value does not match its declared type"),
        }
        spec.scratch.record_tags(tags.len);
    }
    Ok(())
}

unsafe fn set_valid(validity: *mut u64, row: usize, valid: bool) {
    let word = unsafe { validity.add(row / 64) };
    let mask = 1_u64 << (row % 64);
    if valid {
        unsafe { *word |= mask };
    } else {
        unsafe { *word &= !mask };
    }
}

unsafe fn encode_string(
    api: &Api,
    arena: Handle,
    bytes: &[u8],
    error: *mut Handle,
) -> Result<DuckBytes> {
    let length = u32::try_from(bytes.len()).context("native scanner string exceeds u32")?;
    if bytes.len() <= 12 {
        let mut inlined = [0_u8; 12];
        inlined[..bytes.len()].copy_from_slice(bytes);
        return Ok(DuckBytes {
            value: DuckBytesValue {
                inlined: DuckBytesInline { length, inlined },
            },
        });
    }
    let mut allocation = ptr::null_mut();
    callback_call(
        unsafe { (api.arena_allocate)(arena, bytes.len(), &mut allocation, error) },
        "allocate native scanner string",
    )?;
    ensure!(
        !allocation.is_null(),
        "native scanner string allocation is null"
    );
    unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), allocation, bytes.len()) };
    let mut prefix = [0_u8; 4];
    prefix.copy_from_slice(&bytes[..4]);
    Ok(DuckBytes {
        value: DuckBytesValue {
            pointer: DuckBytesPointer {
                length,
                prefix,
                ptr: allocation,
            },
        },
    })
}

fn callback_api() -> Result<&'static Arc<Api>> {
    ffi::callback_api().context("DuckDB callback API is not initialized")
}

fn callback_call(code: ffi::ErrorCode, operation: &str) -> Result<()> {
    ensure!(code == ffi::ERROR_NONE, "{operation} ({code})");
    Ok(())
}

fn callback_boundary(error: *mut Handle, body: impl FnOnce() -> Result<()>) {
    let outcome = catch_unwind(AssertUnwindSafe(body));
    let message = match outcome {
        Ok(Ok(())) => return,
        Ok(Err(error)) => format!("{error:#}"),
        Err(_) => "panic in native DuckDB scanner callback".to_owned(),
    };
    if let Some(api) = ffi::callback_api() {
        unsafe { api.callback_error(error, &message) };
    }
}

extern "C" fn drop_user(pointer: *mut c_void) {
    destroy_box::<UserData<'_>>(pointer);
}

extern "C" fn drop_bind(pointer: *mut c_void) {
    destroy_box::<BindState<'_>>(pointer);
}

extern "C" fn drop_global(pointer: *mut c_void) {
    destroy_box::<GlobalState<'_>>(pointer);
}

extern "C" fn drop_local(pointer: *mut c_void) {
    destroy_box::<LocalState>(pointer);
}

trait CallbackAllocation {
    fn reservation(&mut self) -> &mut Option<OwnerReservation>;
}
macro_rules! callback_allocation {
    ($($ty:ty),+ $(,)?) => { $(impl CallbackAllocation for $ty {
        fn reservation(&mut self) -> &mut Option<OwnerReservation> { &mut self.reservation }
    })+ };
}
callback_allocation!(UserData<'_>, BindState<'_>, GlobalState<'_>, LocalState);

/// Pre-handoff RAII and post-handoff destruction use the same outer guard.
/// The FFI allocation contains a guard only while DuckDB owns the opaque pointer;
/// destroy_box extracts it before running any payload destructor/deallocation.
struct CallbackBox<T: CallbackAllocation> {
    value: Option<Box<T>>,
    reservation: Option<OwnerReservation>,
}
impl<T: CallbackAllocation> CallbackBox<T> {
    fn new(mut value: T) -> Self {
        let reservation = value.reservation().take();
        let value = Some(Box::new(value));
        owner_added();
        Self { value, reservation }
    }
    fn into_raw(mut self) -> *mut T {
        let mut value = self.value.take().expect("callback box is owned");
        *value.reservation() = self.reservation.take();
        Box::into_raw(value)
    }
}
impl<T: CallbackAllocation> Drop for CallbackBox<T> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            drop(value);
            owner_removed();
        }
        // reservation is a field of this OUTER stack owner, never of the Box
        // being destroyed. Rust also drops it after Box cleanup during unwind.
    }
}

fn destroy_box<T: CallbackAllocation>(pointer: *mut c_void) {
    if pointer.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let mut value = unsafe { Box::from_raw(pointer.cast::<T>()) };
        let reservation = value.reservation().take();
        // The extracted guard is outside the allocation it accounts for. The
        // Box (including its backing storage) drops before the guard, on unwind too.
        drop(CallbackBox {
            value: Some(value),
            reservation,
        });
    }));
}

fn owner_added() {
    #[cfg(test)]
    LIVE_OWNERS.fetch_add(1, Ordering::Relaxed);
}

fn owner_removed() {
    #[cfg(test)]
    LIVE_OWNERS.fetch_sub(1, Ordering::Relaxed);
}

#[cfg(test)]
pub(super) fn live_owners() -> usize {
    LIVE_OWNERS.load(Ordering::Acquire)
}

pub(super) fn catalog_type(kind: &str) -> Result<ffi::TypeId> {
    match kind {
        "VARCHAR" => Ok(TYPE_VARCHAR),
        "BIGINT" => Ok(TYPE_BIGINT),
        "UBIGINT" => Ok(TYPE_UBIGINT),
        "DOUBLE" => Ok(TYPE_DOUBLE),
        "BOOLEAN" => Ok(TYPE_BOOLEAN),
        _ => bail!("unsupported catalog column type {kind:?}"),
    }
}
/// Observes from the final field destructor, while the enclosing Box/Arc backing
/// allocation still exists. No allocator interception or timing race is needed.
#[cfg(test)]
struct CleanupProbe {
    budget: RawMemoryBudget,
    observed: Arc<AtomicUsize>,
    admitted: Arc<AtomicBool>,
}
#[cfg(test)]
impl Drop for CleanupProbe {
    fn drop(&mut self) {
        self.observed
            .store(self.budget.status().reserved_bytes, Ordering::Release);
        self.admitted
            .store(self.budget.reserve(1).is_ok(), Ordering::Release);
    }
}

#[cfg(test)]
mod scratch_tests {
    use super::*;

    fn minimal_plan(threads: usize) -> ScratchPlan {
        ScratchPlan {
            threads,
            scanner_count: 1,
            raw_scanner_count: 1,
            raw_batch_capacity: 0,
            column_capacity: 1,
            column_name_capacity: 1,
            max_tags_json_bytes: 128,
        }
    }

    #[test]
    fn callback_box_credit_survives_final_field_cleanup() {
        let _serial = crate::query::native_tests::native_test_guard();
        let plan = minimal_plan(1);
        let fixed = plan.fixed_bytes().unwrap();
        let owner = size_of::<LocalState>() + 64;
        let budget = RawMemoryBudget::new(fixed + owner, 1).unwrap();
        let scratch = ScannerScratch::reserve(plan, &budget).unwrap().unwrap();
        let observed = Arc::new(AtomicUsize::new(usize::MAX));
        let admitted = Arc::new(AtomicBool::new(false));
        for path in 0..3 {
            let local = CallbackBox::new(LocalState {
                work: None,
                row: 0,
                reservation: Some(scratch.reserve_owner::<LocalState>().unwrap()),
                _cleanup_probe: Some(CleanupProbe {
                    budget: budget.clone(),
                    observed: observed.clone(),
                    admitted: admitted.clone(),
                }),
            });
            match path {
                0 => drop_local(local.into_raw().cast()),
                1 => drop(local), // ordinary failure before the FFI handoff
                _ => {
                    assert!(
                        catch_unwind(AssertUnwindSafe(move || {
                            let _local = local;
                            panic!("pre-handoff unwind");
                        }))
                        .is_err()
                    );
                }
            }
            assert_eq!(
                observed.load(Ordering::Acquire),
                fixed + owner,
                "Box credit was refunded during field cleanup, before Box deallocation"
            );
            assert!(
                !admitted.load(Ordering::Acquire),
                "cleanup exposed premature admission credit"
            );
            assert_eq!(budget.status().reserved_bytes, fixed);
        }
        drop(scratch);
        assert_eq!(budget.status().reserved_bytes, 0);
    }

    #[test]
    fn fixed_credit_survives_final_scratch_field_cleanup() {
        let plan = minimal_plan(1);
        let fixed = plan.fixed_bytes().unwrap();
        let budget = RawMemoryBudget::new(fixed, 1).unwrap();
        let mut scratch = ScannerScratch::reserve(plan, &budget).unwrap().unwrap();
        let observed = Arc::new(AtomicUsize::new(usize::MAX));
        let admitted = Arc::new(AtomicBool::new(false));
        Arc::get_mut(&mut scratch.scratch).unwrap()._cleanup_probe = Some(CleanupProbe {
            budget: budget.clone(),
            observed: observed.clone(),
            admitted: admitted.clone(),
        });
        drop(scratch);
        assert_eq!(
            observed.load(Ordering::Acquire),
            fixed,
            "fixed credit was refunded before the final Scratch Arc block was deallocated"
        );
        assert!(!admitted.load(Ordering::Acquire));
        assert_eq!(budget.status().reserved_bytes, 0);
    }

    #[test]
    fn fixed_credit_survives_scanner_setup_error_and_unwind_cleanup() {
        for panic in [false, true] {
            let plan = minimal_plan(1);
            let fixed = plan.fixed_bytes().unwrap();
            let budget = RawMemoryBudget::new(fixed, 1).unwrap();
            let observed = Arc::new(AtomicUsize::new(usize::MAX));
            let admitted = Arc::new(AtomicBool::new(false));
            let result = catch_unwind(AssertUnwindSafe(|| -> Result<()> {
                let mut scratch = ScannerScratch::reserve(plan, &budget)?.unwrap();
                Arc::get_mut(&mut scratch.scratch).unwrap()._cleanup_probe = Some(CleanupProbe {
                    budget: budget.clone(),
                    observed: observed.clone(),
                    admitted: admitted.clone(),
                });
                // Same lexical order as execution: all spec/session aliases
                // are declared after the outer lease and unwind before it.
                let _spec_alias = Arc::clone(scratch.scratch());
                assert!(!panic, "scanner setup panic");
                bail!("scanner setup failure");
            }));
            assert_eq!(result.is_err(), panic);
            if let Ok(result) = result {
                assert!(result.is_err());
            }
            assert_eq!(observed.load(Ordering::Acquire), fixed);
            assert!(!admitted.load(Ordering::Acquire));
            assert_eq!(budget.status().reserved_bytes, 0);
        }
    }

    #[test]
    fn exact_array_partial_initialization_cleanup_is_unwind_safe() {
        struct Counted(Arc<AtomicUsize>);
        impl Drop for Counted {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        for panic in [false, true] {
            let dropped = Arc::new(AtomicUsize::new(0));
            let result = catch_unwind(AssertUnwindSafe(|| {
                exact_slice(7, |index| {
                    if index == 3 {
                        assert!(!panic, "array initialization panic");
                        bail!("array initialization error");
                    }
                    Ok(Counted(dropped.clone()))
                })
            }));
            assert_eq!(result.is_err(), panic);
            if let Ok(result) = result {
                assert!(result.is_err());
            }
            assert_eq!(dropped.load(Ordering::SeqCst), 3);
        }
        for len in [0, 1, 7, OUTPUT_ROWS] {
            let values = exact_slice(len, Ok).unwrap();
            assert_eq!(values.len(), len);
            assert_eq!(
                std::mem::size_of_val(values.as_ref()),
                len * size_of::<usize>()
            );
            assert!(
                values
                    .iter()
                    .enumerate()
                    .all(|(index, value)| index == *value)
            );
        }
        assert!(exact_slice::<usize>(usize::MAX, |_| unreachable!()).is_err());
    }

    #[test]
    fn scratch_size_overflow_fails_before_reservation() {
        let budget = RawMemoryBudget::new(1024, 1).unwrap();
        let mut plan = minimal_plan(1);
        plan.max_tags_json_bytes = usize::MAX;
        let error = match ScannerScratch::reserve(plan, &budget) {
            Ok(_) => panic!("overflowed scratch plan was admitted"),
            Err(error) => error,
        };
        assert!(format!("{error:#}").contains("scratch size overflow"));
        assert_eq!(budget.status().reserved_bytes, 0);
    }

    #[test]
    fn callback_slots_enforce_global_thread_bound_and_refund() {
        let budget = RawMemoryBudget::new(1024 * 1024, 1).unwrap();
        let scratch = ScannerScratch::reserve(minimal_plan(2), &budget)
            .unwrap()
            .unwrap();
        let fixed = scratch.usage().reserved_fixed_bytes;
        assert_eq!(budget.status().reserved_bytes, fixed);
        let first = scratch.checkout().unwrap();
        let second = scratch.checkout().unwrap();
        assert!(scratch.checkout().is_err());
        assert_eq!(scratch.usage().max_concurrent_callbacks, 2);
        drop(first);
        drop(second);
        assert_eq!(scratch.usage().available_slots_after_close, 2);
        drop(scratch);
        assert_eq!(budget.status().reserved_bytes, 0);
    }

    #[test]
    fn owner_reservation_failure_refunds_fixed_scratch() {
        let plan = minimal_plan(1);
        let fixed = plan.fixed_bytes().unwrap();
        let budget = RawMemoryBudget::new(fixed, 1).unwrap();
        let scratch = ScannerScratch::reserve(plan, &budget).unwrap().unwrap();
        assert!(scratch.reserve_owner::<UserData<'_>>().is_err());
        assert_eq!(scratch.usage().owner_live_highwater, 0);
        drop(scratch);
        assert_eq!(budget.status().reserved_bytes, 0);
    }

    #[test]
    fn position_layout_and_fixed_capacity_are_exact() {
        assert_eq!(size_of::<Position>(), 2 * size_of::<usize>());
        let budget = RawMemoryBudget::new(1024 * 1024, 1).unwrap();
        let scratch = ScannerScratch::reserve(minimal_plan(1), &budget)
            .unwrap()
            .unwrap();
        let usage = scratch.usage();
        assert_eq!(usage.position_capacity_per_slot, OUTPUT_ROWS);
        assert_eq!(usage.tag_capacity_per_slot, 128);
        assert_eq!(usage.callback_slots, 1);
    }
}
