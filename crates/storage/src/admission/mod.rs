//! Bounded, per-connection foreground admission control.
//!
//! The controller is deliberately independent of a provider implementation.  A
//! caller acquires a permit before starting a provider future and completes the
//! permit with the provider outcome.  This keeps the adaptive state machine and
//! the queue useful while the individual provider call sites are migrated.

use std::{
    collections::{HashMap, VecDeque},
    fmt,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use tokio::sync::oneshot;

mod admission_transition;

mod admission_types;

use admission_transition::{WINDOW, Window, evaluate_pressure, evaluate_window};
pub use admission_types::{
    AdmissionClass, AdmissionConfig, AdmissionConfigError, AdmissionOutcome, AdmissionRejection,
    AdmissionRejectionReason, AdmissionSnapshot, AdmissionState,
};

#[cfg(test)]
mod admission_tests;
#[cfg(test)]
mod quint_admission_gate_tests;
#[cfg(test)]
mod quint_admission_tests;

struct PermitGrant {
    controller: Arc<Inner>,
    class: AdmissionClass,
    reclaimed: bool,
}

impl fmt::Debug for PermitGrant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PermitGrant(..)")
    }
}

impl PermitGrant {
    fn claim(mut self) -> AdmissionPermit {
        self.reclaimed = true;
        AdmissionPermit::new(Arc::clone(&self.controller), self.class)
    }

    fn reclaim_locked(mut self, state: &mut ControllerState) {
        state.in_flight = state.in_flight.saturating_sub(1);
        state.in_flight_by_class[self.class.index()] =
            state.in_flight_by_class[self.class.index()].saturating_sub(1);
        self.reclaimed = true;
    }
}

impl Drop for PermitGrant {
    fn drop(&mut self) {
        if self.reclaimed {
            return;
        }
        let mut state = lock_state(&self.controller);
        state.in_flight = state.in_flight.saturating_sub(1);
        state.in_flight_by_class[self.class.index()] =
            state.in_flight_by_class[self.class.index()].saturating_sub(1);
    }
}

#[derive(Debug)]
struct Waiter {
    id: u64,
    class: AdmissionClass,
    enqueued_at: Instant,
    sender: oneshot::Sender<PermitGrant>,
}

struct WaiterGuard {
    controller: Arc<Inner>,
    id: u64,
    active: bool,
}

impl WaiterGuard {
    fn disarm(mut self) {
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

#[derive(Debug)]
struct ControllerState {
    desired_limit: usize,
    in_flight: usize,
    in_flight_by_class: [usize; 3],
    control_in_flight: usize,
    rejection_count: u64,
    next_waiter_id: u64,
    queue: VecDeque<Waiter>,
    state: AdmissionState,
    window: Window,
    baselines_ms: [f64; 3],
    low_load_windows: usize,
    last_baseline_raise_at: Option<Instant>,
    healthy_windows: usize,
    congested_windows: usize,
    non_congested_windows: usize,
    emergency_clear_windows: usize,
    saturated_healthy_windows: usize,
    previous_saturated_goodput: Option<f64>,
    probe_previous_limit: Option<usize>,
    probe_due: Instant,
    rng: u64,
}

struct Inner {
    connection_id: Arc<str>,
    config: AdmissionConfig,
    metrics: AdmissionMetrics,
    state: Mutex<ControllerState>,
}

/// Metric handles are built once per connection.  Constructing a `metrics!`
/// call with dynamic labels creates a label vector, so keeping the handles on
/// the controller removes that allocation from the immediate admission path.
struct AdmissionMetrics {
    decision_admit: metrics::Counter,
    decision_queued: metrics::Counter,
    decision_control: metrics::Counter,
    reject_queue_full: metrics::Counter,
    reject_queue_timeout: metrics::Counter,
    reject_control_exhausted: metrics::Counter,
    limit: metrics::Gauge,
    minimum: metrics::Gauge,
    effective_maximum: metrics::Gauge,
    in_flight: [metrics::Gauge; 3],
    control_in_flight: metrics::Gauge,
    queue_depth: metrics::Gauge,
    state: [metrics::Gauge; 6],
    baseline: [metrics::Gauge; 3],
    goodput: metrics::Gauge,
    probe_started: metrics::Counter,
    service: [[metrics::Histogram; 4]; 3],
    provider_pressure: [metrics::Counter; 4],
    control_pressure: [metrics::Counter; 2],
    queue_wait_granted: metrics::Histogram,
}

const SERVICE_OUTCOMES: [&str; 4] = [
    "success",
    "success_after_pressure",
    "failure",
    "retryable_pressure",
];
const PRESSURE_OUTCOMES: [&str; 4] = [
    "success_after_pressure",
    "retryable_pressure",
    "timeout",
    "explicit_throttle",
];
const CONTROL_PRESSURE_OUTCOMES: [&str; 2] = ["provider_signal", "error"];

impl AdmissionMetrics {
    fn new(connection: &Arc<str>) -> Self {
        let decision = |reason: &'static str| {
            metrics::counter!(
                "storage.admission.decision.total",
                "connection" => Arc::clone(connection),
                "reason" => reason
            )
        };
        let rejection = |reason: &'static str| {
            metrics::counter!(
                "storage.admission.reject.total",
                "connection" => Arc::clone(connection),
                "reason" => reason
            )
        };
        let gauge =
            |name: &'static str| metrics::gauge!(name, "connection" => Arc::clone(connection));
        let class_gauge = |name: &'static str, class: AdmissionClass| {
            metrics::gauge!(
                name,
                "connection" => Arc::clone(connection),
                "class" => class_label(class)
            )
        };
        let state_gauge = |state: AdmissionState| {
            metrics::gauge!(
                "storage.admission.state",
                "connection" => Arc::clone(connection),
                "state" => state_label(state)
            )
        };
        let service = AdmissionClass::ALL.map(|class| {
            std::array::from_fn(|index| {
                metrics::histogram!(
                    "storage.admission.service.ms",
                    "connection" => Arc::clone(connection),
                    "class" => class_label(class),
                    "outcome" => SERVICE_OUTCOMES[index]
                )
            })
        });
        let provider_pressure = std::array::from_fn(|index| {
            metrics::counter!(
                "storage.admission.provider.pressure.total",
                "connection" => Arc::clone(connection),
                "reason" => PRESSURE_OUTCOMES[index]
            )
        });
        let control_pressure = std::array::from_fn(|index| {
            metrics::counter!(
                "storage.admission.control.pressure.total",
                "connection" => Arc::clone(connection),
                "reason" => CONTROL_PRESSURE_OUTCOMES[index]
            )
        });

        Self {
            decision_admit: decision("admit"),
            decision_queued: decision("queued"),
            decision_control: decision("control"),
            reject_queue_full: rejection("queue_full"),
            reject_queue_timeout: rejection("queue_timeout"),
            reject_control_exhausted: rejection("control_reserve_exhausted"),
            limit: gauge("storage.admission.limit"),
            minimum: gauge("storage.admission.minimum"),
            effective_maximum: gauge("storage.admission.effective_maximum"),
            in_flight: AdmissionClass::ALL
                .map(|class| class_gauge("storage.admission.in_flight", class)),
            control_in_flight: gauge("storage.admission.control.in_flight"),
            queue_depth: gauge("storage.admission.queue.depth"),
            state: AdmissionState::ALL.map(state_gauge),
            baseline: AdmissionClass::ALL
                .map(|class| class_gauge("storage.admission.baseline.ms", class)),
            goodput: gauge("storage.admission.goodput.rps"),
            probe_started: metrics::counter!(
                "storage.admission.probe.total",
                "connection" => Arc::clone(connection),
                "outcome" => "started"
            ),
            service,
            provider_pressure,
            control_pressure,
            queue_wait_granted: metrics::histogram!(
                "storage.admission.queue.wait.ms",
                "connection" => Arc::clone(connection),
                "outcome" => "granted"
            ),
        }
    }
}

#[derive(Clone, Copy)]
struct StateMetricsSnapshot {
    desired_limit: usize,
    in_flight_by_class: [usize; 3],
    control_in_flight: usize,
    queue_depth: usize,
    state: AdmissionState,
    baselines_ms: [f64; 3],
    goodput_rps: Option<f64>,
}

/// One adaptive controller for one physical storage connection.
#[derive(Clone)]
pub struct AdmissionController {
    inner: Arc<Inner>,
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
        AdmissionConfig::try_new(config)?;
        let now = Instant::now();
        let bootstrap = if config.enabled {
            config.bootstrap_limit()?
        } else {
            config.effective_maximum()
        };
        let connection_id = connection_id.into();
        let metrics = AdmissionMetrics::new(&connection_id);
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
            }),
        };
        let snapshot = {
            let state = lock_state(&controller.inner);
            state_metrics_snapshot(&state, None)
        };
        record_state_metrics(&controller.inner, snapshot);
        Ok(controller)
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
        let result = if state.in_flight >= state.desired_limit || !state.queue.is_empty() {
            state.rejection_count = state.rejection_count.saturating_add(1);
            Err(AdmissionRejection::new(
                AdmissionRejectionReason::QueueFull,
                self.inner.config.max_queue_wait_ms,
            ))
        } else {
            state.in_flight = state.in_flight.saturating_add(1);
            state.in_flight_by_class[class.index()] =
                state.in_flight_by_class[class.index()].saturating_add(1);
            state.window.peak_in_flight = state.window.peak_in_flight.max(state.in_flight);
            Ok(AdmissionPermit::new(Arc::clone(&self.inner), class))
        };
        let snapshot = state_metrics_snapshot(&state, None);
        drop(state);
        match &result {
            Ok(_) => record_decision(&self.inner, class, "admit"),
            Err(rejection) => record_rejection(&self.inner, class, rejection.reason),
        }
        record_state_metrics(&self.inner, snapshot);
        result
    }

    pub async fn acquire(
        &self,
        class: AdmissionClass,
    ) -> Result<AdmissionPermit, AdmissionRejection> {
        let (receiver, waiter_id) = {
            let mut state = lock_state(&self.inner);
            if state.in_flight < state.desired_limit && state.queue.is_empty() {
                state.in_flight = state.in_flight.saturating_add(1);
                state.in_flight_by_class[class.index()] =
                    state.in_flight_by_class[class.index()].saturating_add(1);
                state.window.peak_in_flight = state.window.peak_in_flight.max(state.in_flight);
                let snapshot = state_metrics_snapshot(&state, None);
                drop(state);
                record_decision(&self.inner, class, "admit");
                record_state_metrics(&self.inner, snapshot);
                return Ok(AdmissionPermit::new(Arc::clone(&self.inner), class));
            }
            if state.queue.len() >= self.inner.config.queue_capacity {
                state.rejection_count = state.rejection_count.saturating_add(1);
                let rejection = AdmissionRejection::new(
                    AdmissionRejectionReason::QueueFull,
                    self.inner.config.max_queue_wait_ms,
                );
                let snapshot = state_metrics_snapshot(&state, None);
                drop(state);
                record_rejection(&self.inner, class, rejection.reason);
                record_state_metrics(&self.inner, snapshot);
                return Err(rejection);
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
            let snapshot = state_metrics_snapshot(&state, None);
            drop(state);
            record_decision(&self.inner, class, "queued");
            record_state_metrics(&self.inner, snapshot);
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
                let mut state = lock_state(&self.inner);
                state.rejection_count = state.rejection_count.saturating_add(1);
                let rejection = AdmissionRejection::new(
                    AdmissionRejectionReason::QueueTimedOut,
                    self.inner.config.max_queue_wait_ms,
                );
                let snapshot = state_metrics_snapshot(&state, None);
                drop(state);
                record_rejection(&self.inner, class, rejection.reason);
                record_state_metrics(&self.inner, snapshot);
                Err(rejection)
            }
            Err(_) => {
                let mut state = lock_state(&self.inner);
                state.rejection_count = state.rejection_count.saturating_add(1);
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
                let snapshot = state_metrics_snapshot(&state, None);
                drop(state);
                record_rejection(&self.inner, class, rejection.reason);
                record_state_metrics(&self.inner, snapshot);
                Err(rejection)
            }
        }
    }

    pub fn try_acquire_control(&self) -> Result<ControlPermit, AdmissionRejection> {
        let mut state = lock_state(&self.inner);
        if state.control_in_flight >= self.inner.config.control_reserve_concurrency {
            state.rejection_count = state.rejection_count.saturating_add(1);
            let rejection = AdmissionRejection::new(
                AdmissionRejectionReason::ControlReserveExhausted,
                self.inner.config.max_queue_wait_ms,
            );
            let snapshot = state_metrics_snapshot(&state, None);
            drop(state);
            record_rejection(&self.inner, AdmissionClass::PointRead, rejection.reason);
            record_state_metrics(&self.inner, snapshot);
            return Err(rejection);
        }
        state.control_in_flight = state.control_in_flight.saturating_add(1);
        let snapshot = state_metrics_snapshot(&state, None);
        drop(state);
        record_decision(&self.inner, AdmissionClass::PointRead, "control");
        record_state_metrics(&self.inner, snapshot);
        Ok(ControlPermit {
            controller: Some(Arc::clone(&self.inner)),
        })
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

/// A reserved control/health slot.  Control work is intentionally not part of
/// foreground latency or goodput feedback.
pub struct ControlPermit {
    controller: Option<Arc<Inner>>,
}

impl Drop for ControlPermit {
    fn drop(&mut self) {
        if let Some(controller) = self.controller.take() {
            let mut state = lock_state(&controller);
            state.control_in_flight = state.control_in_flight.saturating_sub(1);
            let snapshot = state_metrics_snapshot(&state, None);
            drop(state);
            record_state_metrics(&controller, snapshot);
        }
    }
}

/// Immutable registry of one controller per configured physical connection.
#[derive(Debug, Clone)]
pub struct AdmissionRegistry {
    controllers: Arc<HashMap<String, AdmissionController>>,
    default_controller: AdmissionController,
}

impl AdmissionRegistry {
    pub fn new(
        default_connection_id: impl Into<Arc<str>>,
        connection_ids: impl IntoIterator<Item = String>,
        config: AdmissionConfig,
    ) -> Result<Self, AdmissionConfigError> {
        let connection_ids = connection_ids
            .into_iter()
            .map(|connection_id| (connection_id, config));
        Self::new_with_connection_configs(default_connection_id, connection_ids, config)
    }

    /// Build controllers with an optional provider-specific configuration for
    /// each connection. The fallback keeps the legacy single-backend shape
    /// usable when the caller only has one common configuration.
    pub fn new_with_connection_configs(
        default_connection_id: impl Into<Arc<str>>,
        connection_configs: impl IntoIterator<Item = (String, AdmissionConfig)>,
        fallback_config: AdmissionConfig,
    ) -> Result<Self, AdmissionConfigError> {
        AdmissionConfig::try_new(fallback_config)?;
        let default_connection_id = default_connection_id.into();
        let mut controllers = HashMap::new();
        for (connection_id, config) in connection_configs {
            AdmissionConfig::try_new(config)?;
            controllers.insert(
                connection_id.clone(),
                AdmissionController::new(connection_id, config)?,
            );
        }
        let default_controller = match controllers.get(default_connection_id.as_ref()) {
            Some(controller) => controller.clone(),
            None => {
                let controller =
                    AdmissionController::new(Arc::clone(&default_connection_id), fallback_config)?;
                controllers.insert(default_connection_id.to_string(), controller.clone());
                controller
            }
        };
        Ok(Self {
            controllers: Arc::new(controllers),
            default_controller,
        })
    }

    pub fn for_connection(&self, connection_id: &str) -> Option<&AdmissionController> {
        self.controllers.get(connection_id)
    }

    #[must_use]
    pub fn default_controller(&self) -> &AdmissionController {
        &self.default_controller
    }

    pub fn connection_ids(&self) -> impl Iterator<Item = &str> {
        self.controllers.keys().map(String::as_str)
    }

    #[must_use]
    pub fn fixed_ingress_limit(&self) -> usize {
        const MINIMUM: usize = 64;
        const MAXIMUM: usize = 2_048;
        let configured = self
            .controllers
            .values()
            .map(|controller| {
                let config = controller.config();
                config
                    .maximum_concurrency
                    .saturating_add(config.queue_capacity)
            })
            .fold(0usize, usize::saturating_add);
        configured.clamp(MINIMUM, MAXIMUM)
    }
}

fn lock_state(inner: &Inner) -> MutexGuard<'_, ControllerState> {
    inner
        .state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn remove_waiter(controller: &Arc<Inner>, waiter_id: u64) {
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
    record_queue_waits(controller, queue_waits);
    record_state_metrics(controller, snapshot);
}

fn grant_waiters_from_locked(
    controller: &Arc<Inner>,
    state: &mut ControllerState,
    queue_waits: &mut Vec<(AdmissionClass, Duration)>,
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
            Ok(()) => queue_waits.push((waiter.class, waiter.enqueued_at.elapsed())),
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

fn record_queue_waits(controller: &Arc<Inner>, queue_waits: Vec<(AdmissionClass, Duration)>) {
    for (_class, wait) in queue_waits {
        controller
            .metrics
            .queue_wait_granted
            .record(wait.as_secs_f64() * 1_000.0);
    }
}

fn class_label(class: AdmissionClass) -> &'static str {
    match class {
        AdmissionClass::PointRead => "point_read",
        AdmissionClass::RangeRead => "range_read",
        AdmissionClass::Write => "write",
    }
}

fn record_decision(inner: &Inner, _class: AdmissionClass, decision: &'static str) {
    let counter = match decision {
        "admit" => &inner.metrics.decision_admit,
        "queued" => &inner.metrics.decision_queued,
        "control" => &inner.metrics.decision_control,
        _ => return,
    };
    counter.increment(1);
}

fn record_rejection(inner: &Inner, _class: AdmissionClass, reason: AdmissionRejectionReason) {
    let counter = match reason {
        AdmissionRejectionReason::QueueFull => &inner.metrics.reject_queue_full,
        AdmissionRejectionReason::QueueTimedOut => &inner.metrics.reject_queue_timeout,
        AdmissionRejectionReason::ControlReserveExhausted => {
            &inner.metrics.reject_control_exhausted
        }
    };
    counter.increment(1);
}

fn state_metrics_snapshot(
    state: &ControllerState,
    goodput_rps: Option<f64>,
) -> StateMetricsSnapshot {
    StateMetricsSnapshot {
        desired_limit: state.desired_limit,
        in_flight_by_class: state.in_flight_by_class,
        control_in_flight: state.control_in_flight,
        queue_depth: state.queue.len(),
        state: state.state,
        baselines_ms: state.baselines_ms,
        goodput_rps,
    }
}

fn record_state_metrics(inner: &Inner, snapshot: StateMetricsSnapshot) {
    inner.metrics.limit.set(snapshot.desired_limit as f64);
    inner
        .metrics
        .minimum
        .set(inner.config.minimum_concurrency as f64);
    inner
        .metrics
        .effective_maximum
        .set(inner.config.effective_maximum() as f64);
    for class in AdmissionClass::ALL {
        inner.metrics.in_flight[class.index()]
            .set(snapshot.in_flight_by_class[class.index()] as f64);
    }
    inner
        .metrics
        .control_in_flight
        .set(snapshot.control_in_flight as f64);
    inner.metrics.queue_depth.set(snapshot.queue_depth as f64);
    for (index, state) in AdmissionState::ALL.into_iter().enumerate() {
        inner.metrics.state[index].set(if state == snapshot.state { 1.0 } else { 0.0 });
    }
    for class in AdmissionClass::ALL {
        inner.metrics.baseline[class.index()].set(snapshot.baselines_ms[class.index()]);
    }
    if let Some(goodput) = snapshot.goodput_rps {
        inner.metrics.goodput.set(goodput);
    }
}

fn service_outcome_index(outcome: AdmissionOutcome) -> Option<usize> {
    match outcome {
        AdmissionOutcome::Success(_) => Some(0),
        AdmissionOutcome::SuccessAfterPressure(_) => Some(1),
        AdmissionOutcome::Failure(_) => Some(2),
        AdmissionOutcome::RetryablePressure(_) => Some(3),
        AdmissionOutcome::Timeout
        | AdmissionOutcome::ExplicitThrottle
        | AdmissionOutcome::Cancelled => None,
    }
}

fn pressure_outcome_index(outcome: AdmissionOutcome) -> Option<usize> {
    match outcome {
        AdmissionOutcome::SuccessAfterPressure(_) => Some(0),
        AdmissionOutcome::RetryablePressure(_) => Some(1),
        AdmissionOutcome::Timeout => Some(2),
        AdmissionOutcome::ExplicitThrottle => Some(3),
        AdmissionOutcome::Success(_)
        | AdmissionOutcome::Failure(_)
        | AdmissionOutcome::Cancelled => None,
    }
}

fn state_label(state: AdmissionState) -> &'static str {
    match state {
        AdmissionState::Warmup => "warmup",
        AdmissionState::Stable => "stable",
        AdmissionState::Probe => "probe",
        AdmissionState::Backoff => "backoff",
        AdmissionState::Recovering => "recovering",
        AdmissionState::Emergency => "emergency",
    }
}
