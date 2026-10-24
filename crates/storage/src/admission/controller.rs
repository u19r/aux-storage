use std::{
    collections::VecDeque,
    fmt,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use tokio::sync::oneshot;

use crate::admission::{
    admission_transition::{
        constants::WINDOW,
        transition::{evaluate_pressure, evaluate_window},
        window::Window,
    },
    admission_types::{
        AdmissionClass, AdmissionConfig, AdmissionConfigError, AdmissionOutcome,
        AdmissionRejection, AdmissionRejectionReason, AdmissionSnapshot, AdmissionState,
    },
    metrics::{
        AdmissionMetrics, pressure_outcome_index, record_decision, record_queue_waits,
        record_rejection, record_state_metrics, service_outcome_index, state_metrics_snapshot,
    },
    state::{ControllerState, Inner, lock_state},
    waiters::{Waiter, WaiterGuard, grant_waiters_from_locked},
};

pub(crate) struct PermitGrant {
    pub(super) controller: Arc<Inner>,
    pub(super) class: AdmissionClass,
    pub(super) reclaimed: bool,
}

impl fmt::Debug for PermitGrant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PermitGrant(..)")
    }
}

impl PermitGrant {
    pub(super) fn claim(mut self) -> AdmissionPermit {
        self.reclaimed = true;
        AdmissionPermit::new(Arc::clone(&self.controller), self.class)
    }

    fn reclaim_slot_locked(&mut self, state: &mut ControllerState) {
        if self.reclaimed {
            return;
        }
        state.in_flight = state.in_flight.saturating_sub(1);
        state.in_flight_by_class[self.class.index()] =
            state.in_flight_by_class[self.class.index()].saturating_sub(1);
        self.reclaimed = true;
        self.controller.native_idle.notify_waiters();
    }

    pub(super) fn reclaim_locked(mut self, state: &mut ControllerState) {
        self.reclaim_slot_locked(state);
    }
}

impl Drop for PermitGrant {
    fn drop(&mut self) {
        if self.reclaimed {
            return;
        }
        let controller = Arc::clone(&self.controller);
        let mut state = lock_state(&controller);
        self.reclaim_slot_locked(&mut state);
        let mut queue_waits = Vec::new();
        grant_waiters_from_locked(&controller, &mut state, &mut queue_waits);
        let snapshot = state_metrics_snapshot(&state, None);
        drop(state);
        record_queue_waits(&controller, queue_waits);
        record_state_metrics(&controller, snapshot);
    }
}

/// One adaptive controller for one physical storage connection.
#[derive(Clone)]
pub struct AdmissionController {
    pub(super) inner: Arc<Inner>,
}

impl fmt::Debug for AdmissionController {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdmissionController")
            .field("connection_id", &self.inner.connection_id)
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl AdmissionController {
    #[must_use = "the controller must be retained to enforce admission"]
    pub fn new(
        connection_id: impl Into<Arc<str>>,
        config: AdmissionConfig,
    ) -> Result<Self, AdmissionConfigError> {
        Self::try_new(connection_id, config)
    }

    pub fn try_new(
        connection_id: impl Into<Arc<str>>,
        config: AdmissionConfig,
    ) -> Result<Self, AdmissionConfigError> {
        Self::validate_config(config)?;
        let now = Instant::now();
        let bootstrap = if config.enabled {
            config.bootstrap_limit()?
        } else {
            config.effective_maximum()
        };
        let connection_id = connection_id.into();
        let metrics = AdmissionMetrics::new(&connection_id, config);
        let seed = connection_id
            .bytes()
            .fold(0x9e37_79b9_7f4a_7c15_u64, |seed, byte| {
                seed.rotate_left(7) ^ u64::from(byte)
            });
        let controller = Self {
            inner: Arc::new(Inner {
                connection_id,
                config,
                metrics,
                state: Mutex::new(ControllerState {
                    desired_limit: bootstrap,
                    in_flight: 0,
                    in_flight_by_class: [0; 3],
                    control_in_flight: 0,
                    rejection_count: 0,
                    next_waiter_id: 1,
                    queue: VecDeque::new(),
                    state: if config.enabled {
                        AdmissionState::Warmup
                    } else {
                        AdmissionState::Stable
                    },
                    window: Window::new(now),
                    baselines_ms: [config.initial_latency_estimate_ms as f64; 3],
                    low_load_windows: 0,
                    last_baseline_raise_at: None,
                    healthy_windows: 0,
                    congested_windows: 0,
                    non_congested_windows: 0,
                    emergency_clear_windows: 0,
                    saturated_healthy_windows: 0,
                    previous_saturated_goodput: None,
                    probe_previous_limit: None,
                    probe_due: now + Duration::from_secs(4),
                    rng: seed,
                }),
                control_available: Arc::new(tokio::sync::Semaphore::new(
                    config.control_reserve_concurrency,
                )),
                native_idle: tokio::sync::Notify::new(),
                control_outstanding: std::sync::atomic::AtomicUsize::new(0),
            }),
        };
        let snapshot = {
            let state = lock_state(&controller.inner);
            state_metrics_snapshot(&state, None)
        };
        record_state_metrics(&controller.inner, snapshot);
        Ok(controller)
    }

    pub(super) fn validate_config(config: AdmissionConfig) -> Result<(), AdmissionConfigError> {
        if config.control_reserve_concurrency > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(AdmissionConfigError(
                "control reserve exceeds native semaphore capacity",
            ));
        }
        AdmissionConfig::try_new(config).map(|_| ())
    }

    #[must_use]
    pub fn config(&self) -> AdmissionConfig {
        self.inner.config
    }

    pub fn try_acquire(
        &self,
        class: AdmissionClass,
    ) -> Result<AdmissionPermit, AdmissionRejection> {
        let mut state = lock_state(&self.inner);
        let result = self.try_grant_locked(&mut state, class).ok_or_else(|| {
            AdmissionRejection::new(
                AdmissionRejectionReason::QueueFull,
                self.inner.config.max_queue_wait_ms,
            )
        });
        self.finish_admission(class, state, Some("admit"), result)
    }

    pub async fn acquire(
        &self,
        class: AdmissionClass,
    ) -> Result<AdmissionPermit, AdmissionRejection> {
        let (receiver, waiter_id) = {
            let mut state = lock_state(&self.inner);
            if let Some(permit) = self.try_grant_locked(&mut state, class) {
                return self.finish_admission(class, state, Some("admit"), Ok(permit));
            }
            if state.queue.len() >= self.inner.config.queue_capacity {
                let rejection = AdmissionRejection::new(
                    AdmissionRejectionReason::QueueFull,
                    self.inner.config.max_queue_wait_ms,
                );
                return self.finish_admission(class, state, None, Err(rejection));
            }
            let (sender, receiver) = oneshot::channel();
            let now = Instant::now();
            let queue_depth = state.queue.len();
            state.window.observe_queue(now, queue_depth);
            let waiter_id = state.next_waiter_id;
            state.next_waiter_id = state.next_waiter_id.wrapping_add(1).max(1);
            state.queue.push_back(Waiter {
                id: waiter_id,
                class,
                enqueued_at: now,
                sender,
            });
            state
                .window
                .observe_queue(now, queue_depth.saturating_add(1));
            state.window.queue_waits = state.window.queue_waits.saturating_add(1);
            let _ = self.finish_admission(class, state, Some("queued"), Ok(()));
            (receiver, waiter_id)
        };
        let guard = WaiterGuard {
            controller: Arc::clone(&self.inner),
            id: waiter_id,
            active: true,
        };
        match tokio::time::timeout(
            Duration::from_millis(self.inner.config.max_queue_wait_ms),
            receiver,
        )
        .await
        {
            Ok(Ok(grant)) => {
                guard.disarm();
                Ok(grant.claim())
            }
            Ok(Err(_)) => {
                guard.disarm();
                let state = lock_state(&self.inner);
                let rejection = AdmissionRejection::new(
                    AdmissionRejectionReason::QueueTimedOut,
                    self.inner.config.max_queue_wait_ms,
                );
                self.finish_admission(class, state, None, Err(rejection))
            }
            Err(_) => {
                let mut state = lock_state(&self.inner);
                state.window.queue_timeout = true;
                // Queue timeout is an explicit overload signal, just like a
                // provider throttle. Evaluate it immediately so a fresh
                // window cannot continue admitting at the old limit for a
                // full sampling interval.
                let now = Instant::now();
                evaluate_pressure(&self.inner.config, &mut state, now);
                let rejection = AdmissionRejection::new(
                    AdmissionRejectionReason::QueueTimedOut,
                    self.inner.config.max_queue_wait_ms,
                );
                self.finish_admission(class, state, None, Err(rejection))
            }
        }
    }

    fn try_grant_locked(
        &self,
        state: &mut ControllerState,
        class: AdmissionClass,
    ) -> Option<AdmissionPermit> {
        if state.in_flight >= state.desired_limit || !state.queue.is_empty() {
            return None;
        }
        state.in_flight = state.in_flight.saturating_add(1);
        state.in_flight_by_class[class.index()] =
            state.in_flight_by_class[class.index()].saturating_add(1);
        state.window.peak_in_flight = state.window.peak_in_flight.max(state.in_flight);
        Some(AdmissionPermit::new(Arc::clone(&self.inner), class))
    }

    pub(super) fn finish_admission<T>(
        &self,
        class: AdmissionClass,
        mut state: MutexGuard<'_, ControllerState>,
        decision: Option<&'static str>,
        result: Result<T, AdmissionRejection>,
    ) -> Result<T, AdmissionRejection> {
        if result.is_err() {
            state.rejection_count = state.rejection_count.saturating_add(1);
        }
        let snapshot = state_metrics_snapshot(&state, None);
        drop(state);
        match (&result, decision) {
            (Ok(_), Some(decision)) => record_decision(&self.inner, class, decision),
            (Err(rejection), _) => record_rejection(&self.inner, class, rejection.reason),
            _ => {}
        }
        record_state_metrics(&self.inner, snapshot);
        result
    }

    #[must_use]
    pub fn snapshot(&self) -> AdmissionSnapshot {
        let state = lock_state(&self.inner);
        AdmissionSnapshot {
            connection_id: self.inner.connection_id.to_string(),
            enabled: self.inner.config.enabled,
            state: state.state,
            desired_limit: state.desired_limit,
            minimum_concurrency: self.inner.config.minimum_concurrency,
            effective_maximum: self.inner.config.effective_maximum(),
            in_flight: state.in_flight,
            control_in_flight: state.control_in_flight,
            queue_depth: state.queue.len(),
            rejection_count: state.rejection_count,
            baselines_ms: state.baselines_ms,
        }
    }

    fn complete(&self, class: AdmissionClass, outcome: AdmissionOutcome) {
        let now = Instant::now();
        let mut state = lock_state(&self.inner);
        state.in_flight = state.in_flight.saturating_sub(1);
        state.in_flight_by_class[class.index()] =
            state.in_flight_by_class[class.index()].saturating_sub(1);
        self.inner.native_idle.notify_waiters();
        let queue_depth = state.queue.len();
        state.window.observe_queue(now, queue_depth);
        if let Some(latency) = outcome.latency() {
            state.window.add_sample(class, latency);
        }
        if outcome.is_success() {
            state.window.successes = state.window.successes.saturating_add(1);
        }
        if outcome.is_pressure() {
            state.window.explicit_pressure = true;
        }
        let elapsed = now.saturating_duration_since(state.window.started_at);
        let goodput = if elapsed.is_zero() {
            0.0
        } else {
            state.window.successes as f64 / elapsed.as_secs_f64()
        };
        let previous_state = state.state;
        let previous_limit = state.desired_limit;
        let mut limit_change = None;
        let evaluation = if now.saturating_duration_since(state.window.started_at) >= WINDOW {
            Some(evaluate_window(&self.inner.config, &mut state, now))
        } else if outcome.is_pressure() {
            Some(evaluate_pressure(&self.inner.config, &mut state, now))
        } else {
            None
        };
        if let Some((saturated, pressure, gradient, window_goodput)) = evaluation
            && state.desired_limit != previous_limit
        {
            let reason = if pressure {
                "pressure"
            } else if state.desired_limit < previous_limit {
                "congestion"
            } else if state.state == AdmissionState::Probe {
                "probe"
            } else {
                "recovery"
            };
            limit_change = Some((
                previous_limit,
                state.desired_limit,
                state.state,
                gradient,
                window_goodput,
                saturated,
                reason,
            ));
        }
        let entered_probe =
            previous_state != AdmissionState::Probe && state.state == AdmissionState::Probe;
        let mut queue_waits = Vec::new();
        grant_waiters_from_locked(&self.inner, &mut state, &mut queue_waits);
        let snapshot = state_metrics_snapshot(&state, Some(goodput));
        drop(state);
        if entered_probe {
            self.inner.metrics.probe_started.increment(1);
        }
        record_queue_waits(&self.inner, queue_waits);
        if let Some(latency) = outcome.latency()
            && let Some(index) = service_outcome_index(outcome)
        {
            self.inner.metrics.service[class.index()][index]
                .record(latency.as_secs_f64() * 1_000.0);
        }
        if let Some(index) = pressure_outcome_index(outcome) {
            self.inner.metrics.provider_pressure[index].increment(1);
        }
        if let Some((old_limit, new_limit, state, gradient, goodput, saturated, reason)) =
            limit_change
        {
            tracing::info!(
                connection = %self.inner.connection_id,
                old_limit,
                new_limit,
                state = ?state,
                gradient,
                goodput_rps = goodput,
                saturated,
                reason,
                "storage admission limit changed"
            );
        }
        record_state_metrics(&self.inner, snapshot);
    }

    pub(crate) fn record_control_pressure(&self, provider_signal: bool, error: bool) {
        if provider_signal {
            self.inner.metrics.control_pressure[0].increment(1);
        }
        if error {
            self.inner.metrics.control_pressure[1].increment(1);
        }
    }
}

/// A foreground permit.  Dropping it records cancellation and releases the
/// slot exactly once.
pub struct AdmissionPermit {
    controller: Option<Arc<Inner>>,
    class: AdmissionClass,
}

impl fmt::Debug for AdmissionPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdmissionPermit")
            .field("class", &self.class)
            .finish()
    }
}

impl AdmissionPermit {
    fn new(controller: Arc<Inner>, class: AdmissionClass) -> Self {
        Self {
            controller: Some(controller),
            class,
        }
    }

    pub fn complete(mut self, outcome: AdmissionOutcome) {
        if let Some(controller) = self.controller.take() {
            AdmissionController { inner: controller }.complete(self.class, outcome);
        }
    }
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        if let Some(controller) = self.controller.take() {
            AdmissionController { inner: controller }
                .complete(self.class, AdmissionOutcome::Cancelled);
        }
    }
}
