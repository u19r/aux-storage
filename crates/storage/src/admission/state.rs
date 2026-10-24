use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, MutexGuard},
    time::Instant,
};

use tokio::sync::Semaphore;

use crate::admission::{
    admission_transition::window::Window,
    admission_types::{AdmissionConfig, AdmissionState},
    metrics::AdmissionMetrics,
    waiters::Waiter,
};

#[derive(Debug)]
pub(crate) struct ControllerState {
    pub(super) desired_limit: usize,
    pub(super) in_flight: usize,
    pub(super) in_flight_by_class: [usize; 3],
    pub(super) control_in_flight: usize,
    pub(super) rejection_count: u64,
    pub(super) next_waiter_id: u64,
    pub(super) queue: VecDeque<Waiter>,
    pub(super) state: AdmissionState,
    pub(super) window: Window,
    pub(super) baselines_ms: [f64; 3],
    pub(super) low_load_windows: usize,
    pub(super) last_baseline_raise_at: Option<Instant>,
    pub(super) healthy_windows: usize,
    pub(super) congested_windows: usize,
    pub(super) non_congested_windows: usize,
    pub(super) emergency_clear_windows: usize,
    pub(super) saturated_healthy_windows: usize,
    pub(super) previous_saturated_goodput: Option<f64>,
    pub(super) probe_previous_limit: Option<usize>,
    pub(super) probe_due: Instant,
    pub(super) rng: u64,
}

pub(crate) struct Inner {
    pub(super) connection_id: Arc<str>,
    pub(super) config: AdmissionConfig,
    pub(super) metrics: AdmissionMetrics,
    pub(super) state: Mutex<ControllerState>,
    /// Existing recovery reserve, with FIFO cancellation-safe acquisition.
    pub(super) control_available: Arc<Semaphore>,
    pub(super) native_idle: tokio::sync::Notify,
    /// Queued and running control work, including unbounded zero-reserve work.
    pub(super) control_outstanding: std::sync::atomic::AtomicUsize,
}

pub(crate) fn lock_state(inner: &Inner) -> MutexGuard<'_, ControllerState> {
    inner
        .state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
