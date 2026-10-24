use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::oneshot;

use crate::admission::{
    admission_types::AdmissionClass,
    controller::PermitGrant,
    metrics::{record_queue_waits, record_state_metrics, state_metrics_snapshot},
    state::{ControllerState, Inner, lock_state},
};

#[derive(Debug)]
pub(crate) struct Waiter {
    pub(super) id: u64,
    pub(super) class: AdmissionClass,
    pub(super) enqueued_at: Instant,
    pub(super) sender: oneshot::Sender<PermitGrant>,
}

pub(super) struct WaiterGuard {
    pub(super) controller: Arc<Inner>,
    pub(super) id: u64,
    pub(super) active: bool,
}

impl WaiterGuard {
    pub(super) fn disarm(mut self) {
        self.active = false;
    }
}

impl Drop for WaiterGuard {
    fn drop(&mut self) {
        if self.active {
            remove_waiter(&self.controller, self.id);
        }
    }
}

pub(super) fn remove_waiter(controller: &Arc<Inner>, waiter_id: u64) {
    let mut state = lock_state(controller);
    let now = Instant::now();
    let queue_depth = state.queue.len();
    state.window.observe_queue(now, queue_depth);
    if let Some(index) = state.queue.iter().position(|waiter| waiter.id == waiter_id) {
        state.queue.remove(index);
    }
    let queue_depth = state.queue.len();
    state.window.observe_queue(now, queue_depth);
    let mut queue_waits = Vec::new();
    grant_waiters_from_locked(controller, &mut state, &mut queue_waits);
    let snapshot = state_metrics_snapshot(&state, None);
    drop(state);
    controller.native_idle.notify_waiters();
    record_queue_waits(controller, queue_waits);
    record_state_metrics(controller, snapshot);
}

pub(crate) fn grant_waiters_from_locked(
    controller: &Arc<Inner>,
    state: &mut ControllerState,
    queue_waits: &mut Vec<Duration>,
) {
    let now = Instant::now();
    let queue_depth = state.queue.len();
    state.window.observe_queue(now, queue_depth);
    while state.in_flight < state.desired_limit {
        let Some(waiter) = state.queue.pop_front() else {
            break;
        };
        state.in_flight = state.in_flight.saturating_add(1);
        state.window.peak_in_flight = state.window.peak_in_flight.max(state.in_flight);
        let grant = PermitGrant {
            controller: Arc::clone(controller),
            class: waiter.class,
            reclaimed: false,
        };
        state.in_flight_by_class[waiter.class.index()] =
            state.in_flight_by_class[waiter.class.index()].saturating_add(1);
        match waiter.sender.send(grant) {
            Ok(()) => queue_waits.push(waiter.enqueued_at.elapsed()),
            Err(grant) => {
                // `send` returns the grant when the waiter was cancelled.
                // Reclaim it while the state lock is held; its Drop
                // implementation must not recursively lock the same mutex.
                grant.reclaim_locked(state);
            }
        }
    }
    let queue_depth = state.queue.len();
    state.window.observe_queue(now, queue_depth);
}
