//! Owned exclusive publication authority. No OS mutex guard crosses threads.
use anyhow::{Result, ensure};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};

#[derive(Default)]
pub(crate) struct PublicationGate {
    held: Mutex<bool>,
    changed: Condvar,
    poisoned: AtomicBool,
    #[cfg(all(test, feature = "fault-injection"))]
    wait_probe: Mutex<Option<std::sync::mpsc::Sender<()>>>,
}

impl PublicationGate {
    pub(crate) fn lock(self: &Arc<Self>) -> Result<CommitLease> {
        let mut held = self.held.lock().unwrap_or_else(|p| p.into_inner());
        while *held && !self.is_poisoned() {
            #[cfg(all(test, feature = "fault-injection"))]
            if let Some(probe) = self.wait_probe.lock().unwrap().take() {
                let _ = probe.send(());
            }
            held = self.changed.wait(held).unwrap_or_else(|p| p.into_inner());
        }
        ensure!(
            !self.is_poisoned(),
            "commit gate poisoned; reopen for recovery"
        );
        *held = true;
        Ok(CommitLease {
            gate: self.clone(),
            armed: false,
        })
    }

    #[cfg(all(test, feature = "fault-injection"))]
    pub(crate) fn notify_next_wait(&self, probe: std::sync::mpsc::Sender<()>) {
        *self.wait_probe.lock().unwrap() = Some(probe);
    }

    pub(crate) fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    #[cfg(all(test, feature = "fault-injection"))]
    pub(crate) fn try_lock(self: &Arc<Self>) -> Result<CommitLease> {
        let mut held = self.held.lock().unwrap_or_else(|p| p.into_inner());
        ensure!(!*held && !self.is_poisoned(), "commit gate unavailable");
        *held = true;
        Ok(CommitLease {
            gate: self.clone(),
            armed: false,
        })
    }
}

pub(crate) struct CommitLease {
    gate: Arc<PublicationGate>,
    // Armed before possibly ambiguous I/O, cleared only after install or a
    // publisher-proven pre-I/O failure. Normal drop while armed also fences.
    armed: bool,
}
impl CommitLease {
    pub(crate) fn arm(&mut self) {
        self.armed = true;
    }
    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}
impl Drop for CommitLease {
    fn drop(&mut self) {
        // Never acquire State here: this also runs during an install unwind.
        let mut held = self.gate.held.lock().unwrap_or_else(|p| p.into_inner());
        if self.armed || std::thread::panicking() {
            self.gate.poisoned.store(true, Ordering::Release);
        }
        *held = false;
        self.gate.changed.notify_all();
    }
}
