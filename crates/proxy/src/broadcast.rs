//! A bounded broadcast channel whose slow receivers lag rather than block.
//!
//! This stands in for `tokio::sync::broadcast`, which the run journal and its
//! attachments were built on. nagoya's `sync` has no broadcast, and the shape
//! the callers depend on is small enough to keep here rather than take a crate
//! for: every receiver sees every value sent after it subscribed, a sender never
//! waits, and a receiver that falls more than `capacity` values behind is told
//! how many it missed (`RecvError::Lagged`) and resumes at the oldest value
//! still held. The callers answer a lag by replaying from the run journal, so
//! the lag report is the part that has to be exact.
//!
//! What differs from tokio: the capacity is used as given rather than rounded
//! up to a power of two, and waiting is a `nagoya::sync::Notify` wake of every
//! receiver per send rather than a per-receiver waiter list. Nothing here sends
//! fast enough for the second to matter, and neither changes what a receiver
//! observes.
//!
//! The client crate carries the same file. The protocol crate is the only one
//! both depend on, and it is deliberately vocabulary with no runtime behaviour.

use nagoya::sync::Notify;
use std::{
    collections::VecDeque,
    fmt,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

/// A channel holding at most `capacity` values for its slowest receiver.
///
/// A capacity of zero is treated as one, where tokio would panic.
pub fn channel<T: Clone>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            buffer: VecDeque::new(),
            head: 0,
            senders: 1,
            receivers: 1,
        }),
        capacity: capacity.max(1),
        changed: Notify::new(),
    });
    (
        Sender {
            shared: Arc::clone(&shared),
        },
        Receiver { shared, next: 0 },
    )
}

struct Shared<T> {
    state: Mutex<State<T>>,
    capacity: usize,
    /// Woken on every send and when the last sender goes.
    changed: Notify,
}

struct State<T> {
    buffer: VecDeque<T>,
    /// The sequence position of `buffer[0]`.
    head: u64,
    senders: usize,
    receivers: usize,
}

impl<T> Shared<T> {
    fn lock(&self) -> MutexGuard<'_, State<T>> {
        // Nothing panics while holding this lock, and a poisoned buffer of
        // cloned values is still a valid buffer.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The sending half. Cloning it adds a sender; the channel closes when the
/// last one is dropped.
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

/// A send found no receiver, and the value is handed back.
#[derive(Debug)]
pub struct SendError<T>(pub T);

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the broadcast channel has no receivers")
    }
}

impl<T: fmt::Debug> std::error::Error for SendError<T> {}

impl<T> Sender<T> {
    /// Send `value` to every current receiver and return how many there are.
    ///
    /// Never waits. With no receiver the value is not kept, as with tokio, so a
    /// later subscriber does not see it.
    pub fn send(&self, value: T) -> Result<usize, SendError<T>> {
        let receivers = {
            let mut state = self.shared.lock();
            if state.receivers == 0 {
                return Err(SendError(value));
            }
            state.buffer.push_back(value);
            if state.buffer.len() > self.shared.capacity {
                state.buffer.pop_front();
                state.head += 1;
            }
            state.receivers
        };
        self.shared.changed.notify_waiters();
        Ok(receivers)
    }

    /// A receiver that sees every value sent from now on.
    pub fn subscribe(&self) -> Receiver<T> {
        let mut state = self.shared.lock();
        state.receivers += 1;
        let next = state.head + state.buffer.len() as u64;
        Receiver {
            shared: Arc::clone(&self.shared),
            next,
        }
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared.lock().senders += 1;
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let last = {
            let mut state = self.shared.lock();
            state.senders -= 1;
            state.senders == 0
        };
        if last {
            // Parked receivers have drained what is buffered or will, and then
            // they must learn the channel is closed rather than wait forever.
            self.shared.changed.notify_waiters();
        }
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Sender").finish_non_exhaustive()
    }
}

/// Why [`Receiver::recv`] returned no value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecvError {
    /// Every sender is gone and every buffered value has been received.
    Closed,
    /// This receiver fell behind and this many values were dropped before it
    /// saw them. The next `recv` returns the oldest value still held.
    Lagged(u64),
}

impl fmt::Display for RecvError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => formatter.write_str("the broadcast channel is closed"),
            Self::Lagged(missed) => write!(formatter, "the receiver lagged by {missed} value(s)"),
        }
    }
}

impl std::error::Error for RecvError {}

/// The receiving half. Each receiver has its own position in the stream.
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    /// The sequence position of the next value this receiver will return.
    next: u64,
}

impl<T: Clone> Receiver<T> {
    /// The next value, waiting for one if none is buffered.
    ///
    /// Cancel safe: dropping the future before it completes loses nothing.
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        let shared = Arc::clone(&self.shared);
        loop {
            // Created before the buffer is checked: `Notify` snapshots its
            // broadcast generation here, so a send landing between the check
            // and the await still wakes this receiver.
            let changed = shared.changed.notified();
            if let Some(result) = take(&shared, &mut self.next) {
                return result;
            }
            changed.await;
        }
    }
}

fn take<T: Clone>(shared: &Shared<T>, next: &mut u64) -> Option<Result<T, RecvError>> {
    let state = shared.lock();
    if *next < state.head {
        let missed = state.head - *next;
        *next = state.head;
        return Some(Err(RecvError::Lagged(missed)));
    }
    let offset = usize::try_from(*next - state.head).unwrap_or(usize::MAX);
    if let Some(value) = state.buffer.get(offset) {
        *next += 1;
        return Some(Ok(value.clone()));
    }
    if state.senders == 0 {
        return Some(Err(RecvError::Closed));
    }
    None
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.shared.lock().receivers -= 1;
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Receiver")
            .field("next", &self.next)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_receiver_sees_values_sent_after_it_subscribed() {
        nagoya::block_on(async {
            let (sender, mut first) = channel(4);
            sender.send(1).expect("a receiver exists");
            let mut second = sender.subscribe();
            sender.send(2).expect("receivers exist");
            assert_eq!(first.recv().await, Ok(1));
            assert_eq!(first.recv().await, Ok(2));
            assert_eq!(second.recv().await, Ok(2));
        });
    }

    #[test]
    fn a_slow_receiver_is_told_how_far_it_lagged() {
        nagoya::block_on(async {
            let (sender, mut receiver) = channel(2);
            for value in 1..=5 {
                sender.send(value).expect("a receiver exists");
            }
            assert_eq!(receiver.recv().await, Err(RecvError::Lagged(3)));
            assert_eq!(receiver.recv().await, Ok(4));
            assert_eq!(receiver.recv().await, Ok(5));
        });
    }

    #[test]
    fn dropping_the_last_sender_closes_after_the_buffer_drains() {
        nagoya::block_on(async {
            let (sender, mut receiver) = channel(2);
            sender.send(1).expect("a receiver exists");
            drop(sender);
            assert_eq!(receiver.recv().await, Ok(1));
            assert_eq!(receiver.recv().await, Err(RecvError::Closed));
        });
    }

    #[test]
    fn a_parked_receiver_wakes_on_send() {
        nagoya::block_on(async {
            let (sender, mut receiver) = channel(2);
            let waiter = nagoya::spawn(async move { receiver.recv().await });
            nagoya::sleep(std::time::Duration::from_millis(20)).await;
            sender.send(7).expect("a receiver exists");
            assert_eq!(waiter.await, Some(Ok(7)));
        });
    }
}
