use std::{sync::Arc, time::Duration};

use super::receive_wait::ReceiveWaiters;

/// The waiting receives of one process's queue manager, without the manager.
/// Another domain that commits a message into a queue of the same store (an
/// SNS delivery or a subscription dead-letter move, W3 design 5.2) wakes them
/// through it. In-process only: a hint, never a claim.
#[derive(Clone)]
pub struct QueueWakeHandle {
    waiters: Arc<ReceiveWaiters>,
}

impl QueueWakeHandle {
    pub(super) fn new(waiters: Arc<ReceiveWaiters>) -> Self {
        Self { waiters }
    }

    /// Wake the waiting receives of `queue_url` after another domain
    /// committed a message into it, as a send does.
    pub fn signal_committed_send(&self, queue_url: &str, delay: Duration) {
        self.waiters.signal(queue_url, delay);
    }
}
