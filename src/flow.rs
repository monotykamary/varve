//! Bounded sequenced broadcast with explicit consumer dependencies.
//!
//! Crossbeam channels carry coalesced wakeups, never the event stream. Each
//! registered stage observes every slot in sequence. Slots and caller-declared
//! payload credits are reclaimed only after all stages finish. This primitive
//! is not a durability protocol, an allocator/RSS guarantee, or a lock-free claim.
use crossbeam_channel::{Receiver, Sender, bounded as wake_channel};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Instant;
use tokio::sync::Notify;

const CLOSED: u64 = 1 << 63;
const SEQUENCE_MASK: u64 = CLOSED - 1;
const MAX_STAGES: usize = 64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FlowError {
    InvalidConfig(&'static str),
    Full,
    TooLarge,
    Closed,
    Deadline,
    Failed(String),
}
impl fmt::Display for FlowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(f, "invalid flow: {message}"),
            Self::Full => f.write_str("flow capacity exhausted"),
            Self::TooLarge => f.write_str("payload charge exceeds flow capacity"),
            Self::Closed => f.write_str("flow admission closed"),
            Self::Deadline => f.write_str("flow wait deadline elapsed"),
            Self::Failed(message) => write!(f, "flow fenced: {message}"),
        }
    }
}
impl std::error::Error for FlowError {}

type Result<T> = std::result::Result<T, FlowError>;
fn lock<T>(value: &Mutex<T>) -> MutexGuard<'_, T> {
    value.lock().unwrap_or_else(|poison| poison.into_inner())
}

struct Entry<T> {
    charge: usize,
    value: Option<Arc<T>>,
}
#[repr(align(128))]
struct Slot<T> {
    published: AtomicU64,
    entry: Mutex<Option<Entry<T>>>,
}
#[repr(align(128))]
struct Progress {
    finished: AtomicU64,
    dependencies: Vec<usize>,
    wake: Sender<()>,
    gates_reclamation: bool,
}
struct Core<T> {
    slots: Box<[Slot<T>]>,
    stages: Vec<Progress>,
    head: AtomicU64,
    reclaimed: AtomicU64,
    reclaim_gate: Mutex<()>,
    charged: AtomicUsize,
    max_bytes: usize,
    capacity_epoch: Mutex<u64>,
    capacity_changed: Condvar,
    async_capacity: Notify,
    failure: Mutex<Option<String>>,
    fenced: AtomicBool,
    #[cfg(test)]
    admission_attempts: AtomicUsize,
    #[cfg(test)]
    blocking_waiters: AtomicUsize,
}
impl<T> Core<T> {
    fn wake_stages(&self) {
        for stage in &self.stages {
            let _ = stage.wake.try_send(());
        }
    }
    fn wake_all(&self) {
        self.wake_stages();
        self.wake_capacity();
    }
    fn wake_capacity(&self) {
        let mut epoch = lock(&self.capacity_epoch);
        *epoch = epoch.wrapping_add(1);
        self.capacity_changed.notify_all();
        drop(epoch);
        self.async_capacity.notify_waiters();
    }
    fn failure(&self) -> Option<FlowError> {
        if !self.fenced.load(Ordering::Acquire) {
            return None;
        }
        lock(&self.failure)
            .as_ref()
            .map(|message| FlowError::Failed(message.clone()))
    }
    fn closed_error(&self) -> FlowError {
        self.failure().unwrap_or(FlowError::Closed)
    }
    fn fence(&self, message: &str) {
        let mut failure = lock(&self.failure);
        if failure.is_none() {
            *failure = Some(message.chars().take(256).collect());
        }
        self.fenced.store(true, Ordering::Release);
        self.head.fetch_or(CLOSED, Ordering::AcqRel);
        drop(failure);
        self.wake_all();
    }
    fn publish(&self, sequence: u64, charge: usize, value: Option<T>) {
        let slot = &self.slots[((sequence - 1) % self.slots.len() as u64) as usize];
        *lock(&slot.entry) = Some(Entry {
            charge,
            value: value.map(Arc::new),
        });
        slot.published.store(sequence, Ordering::Release);
        self.wake_stages();
    }
    fn reclaim(&self, completed_stage: usize) {
        if !self.stages[completed_stage].gates_reclamation {
            return;
        }
        // Acquire before inspecting completion. Otherwise two independent sinks
        // may both observe an old peer frontier and permanently miss reclamation.
        let _gate = lock(&self.reclaim_gate);
        let minimum = self
            .stages
            .iter()
            .filter(|stage| stage.gates_reclamation)
            .map(|stage| stage.finished.load(Ordering::Acquire))
            .min()
            .unwrap();
        if minimum <= self.reclaimed.load(Ordering::Acquire) {
            return;
        }
        let mut next = self.reclaimed.load(Ordering::Relaxed);
        while next < minimum {
            let slot = &self.slots[(next % self.slots.len() as u64) as usize];
            let entry = lock(&slot.entry)
                .take()
                .expect("finished slot retains its entry");
            slot.published.store(0, Ordering::Relaxed);
            let charge = entry.charge;
            drop(entry);
            self.charged.fetch_sub(charge, Ordering::AcqRel);
            next += 1;
            // A producer may reuse the slot only after payload destruction.
            self.reclaimed.store(next, Ordering::Release);
        }
        self.wake_capacity();
    }
}

/// Observational counters; cross-field values need not form an atomic snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlowStats {
    pub claimed: u64,
    pub reclaimed: u64,
    pub charged_bytes: usize,
    pub stage_finished: Vec<u64>,
    pub admission_closed: bool,
    pub fenced: bool,
}

/// A clonable admission endpoint. Reserving a slot is never a commit receipt.
pub struct Producer<T> {
    core: Arc<Core<T>>,
}
impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
        }
    }
}
/// Explicit admission closure and fail-closed lifecycle control.
pub struct Control<T> {
    core: Arc<Core<T>>,
}
impl<T> Clone for Control<T> {
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
        }
    }
}
/// Exclusive stage owner. It cannot be cloned or advance out of order.
pub struct Stage<T> {
    core: Arc<Core<T>>,
    index: usize,
    wake: Receiver<()>,
}
/// An unpublished reservation. Dropping it publishes a cancellation marker so
/// a missing producer cannot leave a hole in the contiguous consumer stream.
pub struct Claim<T> {
    core: Arc<Core<T>>,
    sequence: u64,
    charge: usize,
    published: bool,
}
/// One stage's borrow of an event. Abandoning unfinished delivery fences the
/// entire graph; it never silently advances the stage or recycles the slot.
pub struct Delivery<'a, T> {
    stage: Option<&'a mut Stage<T>>,
    sequence: u64,
    value: Option<Arc<T>>,
    finished: bool,
}

/// Create a bounded broadcast graph. Stage indices must be topologically
/// ordered: dependencies name earlier stages. All stages gate reclamation.
/// `max_bytes` accounts the producer's declared payload charge; slot metadata is
/// separately bounded by `capacity`. Callers must charge their real owned input.
#[allow(clippy::type_complexity)]
pub fn bounded<T>(
    capacity: usize,
    max_bytes: usize,
    dependencies: &[Vec<usize>],
) -> Result<(Producer<T>, Vec<Stage<T>>, Control<T>)> {
    if capacity == 0
        || capacity > 1_000_000
        || max_bytes == 0
        || dependencies.is_empty()
        || dependencies.len() > MAX_STAGES
    {
        return Err(FlowError::InvalidConfig(
            "nonzero bounded slots, bytes and stages required",
        ));
    }
    for (index, parents) in dependencies.iter().enumerate() {
        if parents.len() > MAX_STAGES || parents.iter().any(|parent| *parent >= index) {
            return Err(FlowError::InvalidConfig(
                "dependencies must name earlier stages",
            ));
        }
        for (offset, parent) in parents.iter().enumerate() {
            if parents[..offset].contains(parent) {
                return Err(FlowError::InvalidConfig("duplicate dependency"));
            }
        }
    }
    let mut waits = Vec::with_capacity(dependencies.len());
    let stages = dependencies
        .iter()
        .enumerate()
        .map(|(index, parents)| {
            let (wake, wait) = wake_channel(1);
            waits.push(wait);
            Progress {
                finished: AtomicU64::new(0),
                dependencies: parents.clone(),
                wake,
                gates_reclamation: !dependencies
                    .iter()
                    .any(|children| children.contains(&index)),
            }
        })
        .collect();
    let core = Arc::new(Core {
        slots: (0..capacity)
            .map(|_| Slot {
                published: AtomicU64::new(0),
                entry: Mutex::new(None),
            })
            .collect(),
        stages,
        head: AtomicU64::new(0),
        reclaimed: AtomicU64::new(0),
        reclaim_gate: Mutex::new(()),
        charged: AtomicUsize::new(0),
        max_bytes,
        capacity_epoch: Mutex::new(0),
        capacity_changed: Condvar::new(),
        async_capacity: Notify::new(),
        failure: Mutex::new(None),
        fenced: AtomicBool::new(false),
        #[cfg(test)]
        admission_attempts: AtomicUsize::new(0),
        #[cfg(test)]
        blocking_waiters: AtomicUsize::new(0),
    });
    let stages = waits
        .into_iter()
        .enumerate()
        .map(|(index, wake)| Stage {
            core: Arc::clone(&core),
            index,
            wake,
        })
        .collect();
    Ok((
        Producer {
            core: Arc::clone(&core),
        },
        stages,
        Control { core },
    ))
}

impl<T> Producer<T> {
    pub fn try_claim(&self, charge: usize) -> Result<Claim<T>> {
        #[cfg(test)]
        self.core.admission_attempts.fetch_add(1, Ordering::Relaxed);
        if charge > self.core.max_bytes {
            return Err(FlowError::TooLarge);
        }
        let head = self.core.head.load(Ordering::Acquire);
        if head & CLOSED != 0 {
            return Err(self.core.closed_error());
        }
        if head.saturating_sub(self.core.reclaimed.load(Ordering::Acquire))
            >= self.core.slots.len() as u64
        {
            return Err(FlowError::Full);
        }
        self.core
            .charged
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current
                    .checked_add(charge)
                    .filter(|next| *next <= self.core.max_bytes)
            })
            .map_err(|_| FlowError::Full)?;
        let claim = self
            .core
            .head
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |head| {
                if head & CLOSED != 0 || head == SEQUENCE_MASK {
                    return None;
                }
                let tail = self.core.reclaimed.load(Ordering::Acquire);
                (head.saturating_sub(tail) < self.core.slots.len() as u64).then_some(head + 1)
            });
        match claim {
            Ok(head) => Ok(Claim {
                core: Arc::clone(&self.core),
                sequence: head + 1,
                charge,
                published: false,
            }),
            Err(head) => {
                self.core.charged.fetch_sub(charge, Ordering::AcqRel);
                self.core.wake_capacity();
                if head & CLOSED != 0 {
                    Err(self.core.closed_error())
                } else if head == SEQUENCE_MASK {
                    self.core.fence("sequence exhausted");
                    Err(self.core.closed_error())
                } else {
                    Err(FlowError::Full)
                }
            }
        }
    }
    /// Blocking admission keeps the caller's absolute deadline across wakeups.
    pub fn claim_until(&self, charge: usize, deadline: Instant) -> Result<Claim<T>> {
        if Instant::now() >= deadline {
            return Err(FlowError::Deadline);
        }
        match self.try_claim(charge) {
            Err(FlowError::Full) => {}
            result => return result,
        }
        #[cfg(test)]
        struct Waiting<'a>(&'a AtomicUsize);
        #[cfg(test)]
        impl Drop for Waiting<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        #[cfg(test)]
        let _waiting = {
            self.core.blocking_waiters.fetch_add(1, Ordering::SeqCst);
            Waiting(&self.core.blocking_waiters)
        };
        loop {
            if Instant::now() >= deadline {
                return Err(FlowError::Deadline);
            }
            // Read the generation before rechecking capacity. Never hold the
            // wait lock across try_claim, which may refund a raced reservation.
            let observed = *lock(&self.core.capacity_epoch);
            match self.try_claim(charge) {
                Err(FlowError::Full) => {
                    let epoch = lock(&self.core.capacity_epoch);
                    let (epoch, timeout) = self
                        .core
                        .capacity_changed
                        .wait_timeout_while(
                            epoch,
                            deadline.saturating_duration_since(Instant::now()),
                            |current| *current == observed,
                        )
                        .unwrap_or_else(|poison| poison.into_inner());
                    drop(epoch);
                    if timeout.timed_out() {
                        return Err(FlowError::Deadline);
                    }
                }
                result => return result,
            }
        }
    }
    /// Async admission registers its wakeup before checking credits. Dropping
    /// this future before success has not claimed a sequence or retained credit.
    pub async fn claim_async(&self, charge: usize, deadline: Instant) -> Result<Claim<T>> {
        loop {
            if Instant::now() >= deadline {
                return Err(FlowError::Deadline);
            }
            let notified = self.core.async_capacity.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.try_claim(charge) {
                Err(FlowError::Full) => {
                    tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), notified)
                        .await
                        .map_err(|_| FlowError::Deadline)?;
                }
                result => return result,
            }
        }
    }
}
impl<T> Claim<T> {
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn publish(mut self, value: T) -> u64 {
        self.core.publish(self.sequence, self.charge, Some(value));
        self.published = true;
        self.sequence
    }
}
impl<T> Drop for Claim<T> {
    fn drop(&mut self) {
        if !self.published {
            self.core.publish(self.sequence, self.charge, None);
        }
    }
}
impl<T> Control<T> {
    /// Stop new reservations. Already claimed slots can still be published and
    /// all stages must drain them; this operation itself does not join workers.
    pub fn close(&self) {
        self.core.head.fetch_or(CLOSED, Ordering::AcqRel);
        self.core.wake_all();
    }
    pub fn fence(&self, reason: &str) {
        self.core.fence(reason);
    }
    pub fn stats(&self) -> FlowStats {
        let head = self.core.head.load(Ordering::Acquire);
        FlowStats {
            claimed: head & SEQUENCE_MASK,
            reclaimed: self.core.reclaimed.load(Ordering::Acquire),
            charged_bytes: self.core.charged.load(Ordering::Acquire),
            stage_finished: self
                .core
                .stages
                .iter()
                .map(|stage| stage.finished.load(Ordering::Acquire))
                .collect(),
            admission_closed: head & CLOSED != 0,
            fenced: self.core.failure().is_some(),
        }
    }
}
impl<T> Stage<T> {
    pub fn index(&self) -> usize {
        self.index
    }
    pub fn next_until(&mut self, deadline: Instant) -> Result<Option<Delivery<'_, T>>> {
        loop {
            if let Some(error) = self.core.failure() {
                return Err(error);
            }
            let next = self.core.stages[self.index]
                .finished
                .load(Ordering::Acquire)
                + 1;
            let head = self.core.head.load(Ordering::Acquire);
            if head & CLOSED != 0 && next > (head & SEQUENCE_MASK) {
                return Ok(None);
            }
            if Instant::now() >= deadline {
                return Err(FlowError::Deadline);
            }
            let progress = &self.core.stages[self.index];
            let slot = &self.core.slots[((next - 1) % self.core.slots.len() as u64) as usize];
            if progress
                .dependencies
                .iter()
                .all(|parent| self.core.stages[*parent].finished.load(Ordering::Acquire) >= next)
                && slot.published.load(Ordering::Acquire) == next
            {
                let value = lock(&slot.entry)
                    .as_ref()
                    .expect("published slot has entry")
                    .value
                    .clone();
                return Ok(Some(Delivery {
                    stage: Some(self),
                    sequence: next,
                    value,
                    finished: false,
                }));
            }
            self.wake
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .map_err(|_| FlowError::Deadline)?;
        }
    }
}
impl<T> Drop for Stage<T> {
    fn drop(&mut self) {
        let head = self.core.head.load(Ordering::Acquire);
        if head & CLOSED == 0
            || self.core.stages[self.index]
                .finished
                .load(Ordering::Acquire)
                < (head & SEQUENCE_MASK)
        {
            self.core
                .fence("required stage abandoned before closed stream drained");
        }
    }
}
impl<'a, T> Delivery<'a, T> {
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    /// None represents a cancelled unpublished claim, not a missing event.
    pub fn value(&self) -> Option<&T> {
        self.value.as_deref()
    }
    /// Coalesce currently ready contiguous events without advancing this stage.
    /// The caller may stop on a byte limit. This makes one WAL group sync cover
    /// many single inserts while downstream stages remain gated until finish.
    pub fn coalesce(
        mut self,
        max_items: usize,
        mut accept_next: impl FnMut(Option<&T>) -> bool,
    ) -> Result<BatchDelivery<'a, T>> {
        if max_items == 0 {
            return Err(FlowError::InvalidConfig("batch must permit one item"));
        }
        let stage = self.stage.as_ref().expect("live delivery owns stage");
        if let Some(error) = stage.core.failure() {
            return Err(error);
        }
        let limit = max_items.min(stage.core.slots.len());
        let mut values = Vec::with_capacity(limit);
        values.push(self.value.take());
        while values.len() < limit {
            if let Some(error) = stage.core.failure() {
                return Err(error);
            }
            let next = self.sequence + values.len() as u64;
            let progress = &stage.core.stages[stage.index];
            let slot = &stage.core.slots[((next - 1) % stage.core.slots.len() as u64) as usize];
            if !progress
                .dependencies
                .iter()
                .all(|parent| stage.core.stages[*parent].finished.load(Ordering::Acquire) >= next)
                || slot.published.load(Ordering::Acquire) != next
            {
                break;
            }
            let value = lock(&slot.entry)
                .as_ref()
                .expect("published slot has entry")
                .value
                .clone();
            let accepted = accept_next(value.as_deref());
            if let Some(error) = stage.core.failure() {
                return Err(error);
            }
            if !accepted {
                break;
            }
            values.push(value);
        }
        if let Some(error) = stage.core.failure() {
            return Err(error);
        }
        self.finished = true;
        Ok(BatchDelivery {
            stage: self.stage.take().expect("live delivery owns stage"),
            first: self.sequence,
            values,
            finished: false,
        })
    }
    pub fn finish(mut self) {
        self.value.take();
        let stage = self.stage.as_ref().expect("live delivery owns stage");
        stage.core.stages[stage.index]
            .finished
            .store(self.sequence, Ordering::Release);
        self.finished = true;
        stage.core.reclaim(stage.index);
        stage.core.wake_stages();
    }
}
impl<T> Drop for Delivery<'_, T> {
    fn drop(&mut self) {
        if !self.finished {
            self.stage
                .as_ref()
                .expect("live delivery owns stage")
                .core
                .fence("required delivery abandoned without completion");
        }
    }
}

/// Several contiguous deliveries whose completion is one dependency barrier.
/// External persistence still belongs to the journal owner, not this type.
pub struct BatchDelivery<'a, T> {
    stage: &'a mut Stage<T>,
    first: u64,
    values: Vec<Option<Arc<T>>>,
    finished: bool,
}
impl<T> BatchDelivery<'_, T> {
    pub fn first_sequence(&self) -> u64 {
        self.first
    }
    pub fn last_sequence(&self) -> u64 {
        self.first + self.values.len() as u64 - 1
    }
    pub fn len(&self) -> usize {
        self.values.len()
    }
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
    pub fn values(&self) -> impl Iterator<Item = Option<&T>> {
        self.values.iter().map(Option::as_deref)
    }
    pub fn finish(mut self) {
        let last = self.last_sequence();
        self.values.clear();
        self.stage.core.stages[self.stage.index]
            .finished
            .store(last, Ordering::Release);
        self.finished = true;
        self.stage.core.reclaim(self.stage.index);
        self.stage.core.wake_stages();
    }
}
impl<T> Drop for BatchDelivery<'_, T> {
    fn drop(&mut self) {
        if !self.finished {
            self.stage
                .core
                .fence("required batch abandoned without completion");
        }
    }
}

#[cfg(test)]
#[path = "flow_tests.rs"]
mod tests;
