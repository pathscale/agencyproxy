//! A set-once flag that can be awaited.
//!
//! This is what the proxy used `tokio::sync::watch::channel(false)` for: the
//! daemon's shutdown request and each run's completion. Both only ever went
//! from `false` to `true` and were only ever waited on for `true`, so the
//! channel's value, versioning and `borrow` were never the point. nagoya's
//! `sync` has no watch; an atomic plus a `Notify` is the whole of what was used.
//!
//! One difference from the watch it replaces: a watch receiver saw an error
//! when its sender was dropped and its waiters broke out on that. Here the
//! waiter holds the flag itself, so there is no sender to lose; every place
//! that dropped the sender either set it first or never let it go.

use nagoya::sync::Notify;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug, Default)]
pub(crate) struct Flag {
    set: AtomicBool,
    changed: Notify,
}

impl Flag {
    /// Set the flag and wake everything waiting on it. Idempotent.
    pub(crate) fn set(&self) {
        self.set.store(true, Ordering::SeqCst);
        self.changed.notify_waiters();
    }

    pub(crate) fn is_set(&self) -> bool {
        self.set.load(Ordering::SeqCst)
    }

    /// Resolve once the flag is set, immediately if it already is.
    pub(crate) async fn wait(&self) {
        loop {
            // Created before the check: `Notify` snapshots its broadcast
            // generation here, so a `set` landing between the check and the
            // await still wakes this waiter.
            let changed = self.changed.notified();
            if self.is_set() {
                return;
            }
            changed.await;
        }
    }
}
