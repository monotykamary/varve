//! Ownership-backed estimates for engine raw data, not an allocator/RSS quota.
use crate::model::StoredRow;
use anyhow::{Result, ensure};
use serde::Serialize;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct RawMemoryStatus {
    pub limit_bytes: usize,
    pub working_limit_bytes: usize,
    /// All outstanding reservations, including live allocations and working space.
    pub reserved_bytes: usize,
    /// Reservations attached to immutable row allocations (a subset of reserved).
    pub live_bytes: usize,
    pub working_bytes: usize,
    /// Unique allocations pinned by queries/checkpoint captures; a subset of live.
    pub pinned_bytes: usize,
    pub peak_bytes: usize,
    pub rejections: u64,
}

#[derive(Clone, Debug)]
pub struct RawMemoryBudget(Arc<Mutex<RawMemoryStatus>>, Arc<tokio::sync::Notify>);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawReservationError {
    TooLarge,
    Pressure,
}
impl std::fmt::Display for RawReservationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::TooLarge => "raw memory budget exceeded: reservation exceeds pool capacity",
            Self::Pressure => "raw memory budget exceeded: temporary pressure",
        })
    }
}
impl std::error::Error for RawReservationError {}

impl RawMemoryBudget {
    pub fn new(limit_bytes: usize, working_limit_bytes: usize) -> Result<Self> {
        ensure!(
            limit_bytes > 0
                && working_limit_bytes > 0
                && limit_bytes.checked_add(working_limit_bytes).is_some(),
            "raw memory limits must be positive and their sum must fit usize"
        );
        Ok(Self(
            Arc::new(Mutex::new(RawMemoryStatus {
                limit_bytes,
                working_limit_bytes,
                ..RawMemoryStatus::default()
            })),
            Arc::new(tokio::sync::Notify::new()),
        ))
    }
    fn lock(&self) -> MutexGuard<'_, RawMemoryStatus> {
        // No user code runs with this lock held. Drop must release even after a panic.
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
    pub fn status(&self) -> RawMemoryStatus {
        *self.lock()
    }
    pub fn released(&self) -> &tokio::sync::Notify {
        &self.1
    }
    pub fn reserve(
        &self,
        bytes: usize,
    ) -> std::result::Result<RawReservation, RawReservationError> {
        self.acquire(bytes, false)
    }
    pub fn reserve_working(&self, bytes: usize) -> Result<RawReservation> {
        Ok(self.acquire(bytes, true)?)
    }
    fn acquire(
        &self,
        bytes: usize,
        working: bool,
    ) -> std::result::Result<RawReservation, RawReservationError> {
        let mut s = self.lock();
        let (used, limit) = if working {
            (s.working_bytes, s.working_limit_bytes)
        } else {
            (s.reserved_bytes - s.working_bytes, s.limit_bytes)
        };
        if bytes > limit - used {
            s.rejections = s.rejections.saturating_add(1);
            return Err(if bytes > limit {
                RawReservationError::TooLarge
            } else {
                RawReservationError::Pressure
            });
        }
        s.reserved_bytes += bytes;
        if working {
            s.working_bytes += bytes;
        }
        s.peak_bytes = s.peak_bytes.max(s.reserved_bytes);
        Ok(RawReservation {
            budget: self.clone(),
            bytes,
            working,
            live: false,
        })
    }
}

/// Linear credit: cannot be cloned. Acquire before allocation; move with ownership.
#[derive(Debug)]
pub struct RawReservation {
    budget: RawMemoryBudget,
    bytes: usize,
    working: bool,
    live: bool,
}
impl RawReservation {
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Resize in one accounting transaction; failure leaves the credit unchanged.
    pub fn resize(&mut self, bytes: usize) -> std::result::Result<(), RawReservationError> {
        let mut s = self.budget.lock();
        let (used, limit) = if self.working {
            (s.working_bytes, s.working_limit_bytes)
        } else {
            (s.reserved_bytes - s.working_bytes, s.limit_bytes)
        };
        if bytes > limit || bytes > limit - (used - self.bytes) {
            s.rejections = s.rejections.saturating_add(1);
            return Err(if bytes > limit {
                RawReservationError::TooLarge
            } else {
                RawReservationError::Pressure
            });
        }
        s.reserved_bytes = s.reserved_bytes - self.bytes + bytes;
        if self.working {
            s.working_bytes = s.working_bytes - self.bytes + bytes;
        }
        if self.live {
            s.live_bytes = s.live_bytes - self.bytes + bytes;
        }
        s.peak_bytes = s.peak_bytes.max(s.reserved_bytes);
        let released = bytes < self.bytes;
        self.bytes = bytes;
        drop(s);
        if released {
            self.budget.1.notify_waiters();
        }
        Ok(())
    }

    pub fn shrink(&mut self, bytes: usize) -> Result<()> {
        ensure!(bytes <= self.bytes, "raw credit shrink cannot grow");
        self.resize(bytes)?;
        Ok(())
    }

    /// Transfer credit without ever making it available to another admission.
    pub fn split(&mut self, bytes: usize) -> Result<Self> {
        ensure!(
            !self.live && bytes <= self.bytes,
            "invalid raw credit split"
        );
        self.bytes -= bytes;
        Ok(Self {
            budget: self.budget.clone(),
            bytes,
            working: self.working,
            live: false,
        })
    }
}
impl Drop for RawReservation {
    fn drop(&mut self) {
        let mut s = self.budget.lock();
        s.reserved_bytes -= self.bytes;
        if self.working {
            s.working_bytes -= self.bytes;
        }
        if self.live {
            s.live_bytes -= self.bytes;
        }
        drop(s);
        if self.bytes != 0 {
            self.budget.1.notify_waiters();
        }
    }
}

/// Private, no-Weak Arc ownership. EVERY strong alias is consumed by into_inner:
/// the last concurrent caller receives T only AFTER the Arc backing allocation
/// has been released. Dropping T can then refund credit covering that container.
/// try_unwrap followed by ordinary Arc drop does not have this winner guarantee.
/// Never expose the internal Arc or create Weak aliases from it.
#[derive(Debug)]
struct OrderedArc<T>(Option<Arc<T>>);
impl<T> OrderedArc<T> {
    fn new(value: T) -> Self {
        Self(Some(Arc::new(value)))
    }
    fn strong_count(&self) -> usize {
        Arc::strong_count(self.0.as_ref().expect("owned Arc"))
    }
    #[cfg(test)]
    fn get_mut(&mut self) -> Option<&mut T> {
        Arc::get_mut(self.0.as_mut().expect("owned Arc"))
    }
}
impl<T> Clone for OrderedArc<T> {
    fn clone(&self) -> Self {
        Self(Some(Arc::clone(self.0.as_ref().expect("owned Arc"))))
    }
}
impl<T> Deref for OrderedArc<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.0.as_deref().expect("owned Arc")
    }
}
impl<T> Drop for OrderedArc<T> {
    fn drop(&mut self) {
        if let Some(arc) = self.0.take() {
            drop(Arc::into_inner(arc));
        }
    }
}

fn next_raw_identity() -> Result<u64> {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    checked_raw_identity(&NEXT)
}
fn checked_raw_identity(next: &AtomicU64) -> Result<u64> {
    next.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        value.checked_add(1)
    })
    .map_err(|_| anyhow::anyhow!("raw allocation identity exhausted"))
}

#[derive(Debug)]
struct RawAllocation {
    // Rows are destroyed before credit is returned. No Arc<Vec<_>> can escape.
    rows: Vec<StoredRow>,
    identity: u64,
    max_tags_json_bytes: usize,
    pins: Mutex<usize>,
    reservation: RawReservation,
    #[cfg(test)]
    cleanup_location: CleanupLocation,
}

#[derive(Debug)]
struct RawHandle {
    allocation: OrderedArc<RawAllocation>,
    pinned: bool,
    #[cfg(test)]
    cleanup_location: CleanupLocation,
}
impl Drop for RawHandle {
    fn drop(&mut self) {
        if self.pinned {
            let allocation = &self.allocation;
            let mut pins = allocation.pins.lock().unwrap_or_else(|e| e.into_inner());
            *pins -= 1;
            if *pins == 0 {
                allocation.reservation.budget.lock().pinned_bytes -= allocation.reservation.bytes;
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct SharedRawRows(OrderedArc<RawHandle>);

impl SharedRawRows {
    /// The closure is invoked only after admission by the supplied reservation.
    pub(crate) fn build(
        mut reservation: RawReservation,
        allocate: impl FnOnce() -> Result<Vec<StoredRow>>,
    ) -> Result<Self> {
        let rows = allocate()?;
        // Integrity check, not admission: every production caller precomputes its bound.
        ensure!(
            row_charge(logical_bytes(&rows)) <= reservation.bytes,
            "raw allocation exceeds its pre-allocation reservation"
        );
        let max_tags_json_bytes = rows.iter().try_fold(0usize, |max, row| -> Result<usize> {
            Ok(max.max(json_bytes(&row.row.tags)?))
        })?;
        let identity = next_raw_identity()?;
        reservation.budget.lock().live_bytes += reservation.bytes;
        reservation.live = true;
        Ok(Self(OrderedArc::new(RawHandle {
            allocation: OrderedArc::new(RawAllocation {
                rows,
                identity,
                max_tags_json_bytes,
                pins: Mutex::new(0),
                reservation,
                #[cfg(test)]
                cleanup_location: CleanupLocation::default(),
            }),
            pinned: false,
            #[cfg(test)]
            cleanup_location: CleanupLocation::default(),
        })))
    }
    pub(crate) fn pin_metadata_bytes() -> usize {
        std::mem::size_of::<RawHandle>() + 2 * std::mem::size_of::<usize>()
    }

    /// Capture a query/checkpoint role. Multiple roles count each allocation once,
    /// including after its hot/cache designation is retired.
    pub(crate) fn pin(&self) -> Self {
        let allocation = &self.0.allocation;
        let mut pins = allocation.pins.lock().unwrap_or_else(|e| e.into_inner());
        if *pins == 0 {
            allocation.reservation.budget.lock().pinned_bytes += allocation.reservation.bytes;
        }
        *pins += 1;
        Self(OrderedArc::new(RawHandle {
            allocation: allocation.clone(),
            pinned: true,
            #[cfg(test)]
            cleanup_location: CleanupLocation::default(),
        }))
    }
    /// Computed once at immutable construction, never by scanning history under State.
    pub(crate) fn max_tags_json_bytes(&self) -> usize {
        self.0.allocation.max_tags_json_bytes
    }

    pub fn downgrade(&self) -> WeakRawRows {
        WeakRawRows(self.0.allocation.identity)
    }
    pub fn ptr_eq(a: &Self, b: &Self) -> bool {
        a.0.allocation.identity == b.0.allocation.identity
    }
    pub fn strong_count(&self) -> usize {
        self.0.strong_count()
    }
    #[cfg(test)]
    pub(crate) fn test_capacity(&self) -> usize {
        self.0.allocation.rows.capacity()
    }

    #[cfg(test)]
    pub(crate) fn test_rows(rows: Vec<StoredRow>) -> Self {
        let bytes = row_charge(logical_bytes(&rows));
        Self::build(
            RawMemoryBudget::new(bytes.max(1), 1)
                .unwrap()
                .reserve(bytes)
                .unwrap(),
            || Ok(rows),
        )
        .unwrap()
    }
    #[cfg(test)]
    pub(crate) fn test_mut(&mut self) -> &mut Vec<StoredRow> {
        let handle = self.0.get_mut().expect("unique test handle");
        &mut handle.allocation.get_mut().expect("unique test rows").rows
    }
}
impl Deref for SharedRawRows {
    type Target = [StoredRow];
    fn deref(&self) -> &Self::Target {
        &self.0.allocation.rows
    }
}
impl AsRef<[StoredRow]> for SharedRawRows {
    fn as_ref(&self) -> &[StoredRow] {
        self
    }
}
/// Opaque identity only, never upgradeable. A checked process-wide ID avoids
/// retaining a charged Arc control block after its rows and all pins are gone.
#[derive(Debug)]
pub struct WeakRawRows(u64);
impl WeakRawRows {
    pub fn ptr_eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

/// A drop-location witness: ordinary Arc drop destroys fields in the original
/// allocation. into_inner must first extract T, releasing the no-Weak Arc block,
/// then destroy T elsewhere. Compare addresses only, never dereference freed data.
#[cfg(test)]
#[derive(Debug, Default)]
struct CleanupLocation {
    original: usize,
    observed: Option<Arc<std::sync::atomic::AtomicBool>>,
}
#[cfg(test)]
impl CleanupLocation {
    fn arm(&mut self, observed: Arc<std::sync::atomic::AtomicBool>) {
        self.observed = Some(observed);
        self.original = self as *const Self as usize;
    }
}
#[cfg(test)]
impl Drop for CleanupLocation {
    fn drop(&mut self) {
        if let Some(observed) = &self.observed {
            observed.store(
                self.original != self as *const Self as usize,
                std::sync::atomic::Ordering::SeqCst,
            );
        }
    }
}

pub(crate) fn json_bytes(value: &impl Serialize) -> Result<usize> {
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| std::io::Error::other("JSON byte count overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value)?;
    Ok(counter.0)
}

pub(crate) fn logical_bytes(rows: &[StoredRow]) -> usize {
    rows.iter()
        .fold(0usize, |n, r| n.saturating_add(r.row.estimated_bytes()))
}
/// Eight times the existing logical row estimate, plus allocation metadata. Covers
/// vector growth, StoredRow layout, strings and conservative BTreeMap slack.
/// The allocation and primary handle Arc headers are included; OrderedArc frees
/// both containers before payload credit. Shared budget/Notify bookkeeping and
/// allocator-internal usable padding are not per-row allocations or an RSS quota.
pub(crate) fn row_charge(logical: usize) -> usize {
    logical.saturating_mul(8).saturating_add(256)
}
/// Temporary Arrow/Parquet/JSON buffers, compression workspace and vector slack.
pub(crate) fn codec_charge(logical: usize, encoded: usize) -> usize {
    logical
        .saturating_mul(16)
        .saturating_add(encoded.saturating_mul(2))
        .saturating_add(8 * 1024 * 1024)
}

#[cfg(test)]
mod cleanup_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn raw_identity_does_not_pin_credit_or_reuse_across_budgets() {
        let budget = RawMemoryBudget::new(256, 1).unwrap();
        let create = |budget: &RawMemoryBudget| {
            SharedRawRows::build(budget.reserve(256).unwrap(), || Ok(Vec::new())).unwrap()
        };
        let first = create(&budget);
        let old = first.downgrade();
        assert!(old.ptr_eq(&first.clone().downgrade()));
        assert!(old.ptr_eq(&first.pin().downgrade()));
        drop(first);
        assert_eq!(budget.status().reserved_bytes, 0);
        for _ in 0..128 {
            let next = create(&budget);
            assert!(!old.ptr_eq(&next.downgrade()));
            let separate = create(&RawMemoryBudget::new(256, 1).unwrap());
            assert!(!next.downgrade().ptr_eq(&separate.downgrade()));
            assert!(!old.ptr_eq(&separate.downgrade()));
        }
        assert_eq!(budget.status().reserved_bytes, 0);
        let exhausted = AtomicU64::new(u64::MAX - 1);
        assert_eq!(checked_raw_identity(&exhausted).unwrap(), u64::MAX - 1);
        assert!(checked_raw_identity(&exhausted).is_err());
        assert!(checked_raw_identity(&exhausted).is_err());
        assert_eq!(exhausted.load(Ordering::SeqCst), u64::MAX);
    }

    #[test]
    fn raw_construction_error_and_unwind_release_after_owned_payloads() {
        let budget = RawMemoryBudget::new(256, 1).unwrap();
        assert!(
            SharedRawRows::build(budget.reserve(256).unwrap(), || anyhow::bail!(
                "allocation rejected"
            ))
            .is_err()
        );
        assert_eq!(budget.status().reserved_bytes, 0);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = SharedRawRows::build(budget.reserve(256).unwrap(), || {
                    panic!("allocation panic")
                });
            }))
            .is_err()
        );
        assert_eq!(budget.status().reserved_bytes, 0);
        // Includes both primary Arc containers and their counters within the
        // unchanged row_charge metadata allowance (also true with test probes).
        assert!(
            std::mem::size_of::<RawAllocation>()
                + std::mem::size_of::<RawHandle>()
                + 4 * std::mem::size_of::<usize>()
                <= 256
        );
    }

    #[test]
    fn charged_raw_containers_are_deallocated_before_payload_credit_cleanup() {
        let budget = RawMemoryBudget::new(256, 1).unwrap();
        let mut rows =
            SharedRawRows::build(budget.reserve(256).unwrap(), || Ok(Vec::new())).unwrap();
        let handle_moved = Arc::new(AtomicBool::new(false));
        let allocation_moved = Arc::new(AtomicBool::new(false));
        let handle = rows.0.get_mut().unwrap();
        handle.cleanup_location.arm(handle_moved.clone());
        handle
            .allocation
            .get_mut()
            .unwrap()
            .cleanup_location
            .arm(allocation_moved.clone());
        let identity = rows.downgrade();
        let pin = rows.pin();
        assert!(identity.ptr_eq(&pin.downgrade()));
        let barrier = Arc::new(std::sync::Barrier::new(5));
        let threads: Vec<_> = [rows.clone(), rows.clone(), pin.clone(), pin.clone()]
            .into_iter()
            .map(|alias| {
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    drop(alias);
                })
            })
            .collect();
        drop(rows);
        drop(pin);
        assert_eq!(budget.status().reserved_bytes, 256);
        barrier.wait();
        for thread in threads {
            thread.join().unwrap();
        }
        assert!(
            handle_moved.load(Ordering::SeqCst),
            "RawHandle fields were destroyed inside the still-allocated charged Arc block"
        );
        assert!(
            allocation_moved.load(Ordering::SeqCst),
            "RawAllocation credit was destroyed inside the still-allocated charged Arc block"
        );
        assert_eq!(budget.status().reserved_bytes, 0);
        assert_eq!(budget.status().pinned_bytes, 0);
        // An opaque identity must not hold row credit after all actual users end.
        assert!(identity.ptr_eq(&identity));
        assert_eq!(budget.status().reserved_bytes, 0);
    }
}
