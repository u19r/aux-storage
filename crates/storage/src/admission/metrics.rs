use std::{sync::Arc, time::Duration};

use crate::admission::{
    admission_types::{
        AdmissionClass, AdmissionConfig, AdmissionOutcome, AdmissionRejectionReason, AdmissionState,
    },
    constants::{CONTROL_PRESSURE_OUTCOMES, PRESSURE_OUTCOMES, SERVICE_OUTCOMES},
    state::{ControllerState, Inner},
};

/// Metric handles are built once per connection.  Constructing a `metrics!`
/// call with dynamic labels creates a label vector, so keeping the handles on
/// the controller removes that allocation from the immediate admission path.
pub(super) struct AdmissionMetrics {
    pub(super) decision_admit: metrics::Counter,
    pub(super) decision_queued: metrics::Counter,
    pub(super) decision_control: metrics::Counter,
    pub(super) reject_queue_full: metrics::Counter,
    pub(super) reject_queue_timeout: metrics::Counter,
    pub(super) reject_control_exhausted: metrics::Counter,
    pub(super) limit: metrics::Gauge,
    pub(super) in_flight: [metrics::Gauge; 3],
    pub(super) control_in_flight: metrics::Gauge,
    pub(super) queue_depth: metrics::Gauge,
    pub(super) state: [metrics::Gauge; 6],
    pub(super) baseline: [metrics::Gauge; 3],
    pub(super) goodput: metrics::Gauge,
    pub(super) probe_started: metrics::Counter,
    pub(super) service: [[metrics::Histogram; 4]; 3],
    pub(super) provider_pressure: [metrics::Counter; 4],
    pub(super) control_pressure: [metrics::Counter; 2],
    pub(super) queue_wait_granted: metrics::Histogram,
}

impl AdmissionMetrics {
    pub(super) fn new(connection: &Arc<str>, config: AdmissionConfig) -> Self {
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

        gauge("storage.admission.minimum").set(config.minimum_concurrency as f64);
        gauge("storage.admission.effective_maximum").set(config.effective_maximum() as f64);

        Self {
            decision_admit: decision("admit"),
            decision_queued: decision("queued"),
            decision_control: decision("control"),
            reject_queue_full: rejection("queue_full"),
            reject_queue_timeout: rejection("queue_timeout"),
            reject_control_exhausted: rejection("control_reserve_exhausted"),
            limit: gauge("storage.admission.limit"),
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
pub(super) struct StateMetricsSnapshot {
    pub(super) desired_limit: usize,
    pub(super) in_flight_by_class: [usize; 3],
    pub(super) control_in_flight: usize,
    pub(super) queue_depth: usize,
    pub(super) state: AdmissionState,
    pub(super) baselines_ms: [f64; 3],
    pub(super) goodput_rps: Option<f64>,
}

pub(super) fn record_queue_waits(controller: &Arc<Inner>, queue_waits: Vec<Duration>) {
    for wait in queue_waits {
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

pub(super) fn record_decision(inner: &Inner, _class: AdmissionClass, decision: &'static str) {
    let counter = match decision {
        "admit" => &inner.metrics.decision_admit,
        "queued" => &inner.metrics.decision_queued,
        "control" => &inner.metrics.decision_control,
        _ => return,
    };
    counter.increment(1);
}

pub(super) fn record_rejection(
    inner: &Inner,
    _class: AdmissionClass,
    reason: AdmissionRejectionReason,
) {
    let counter = match reason {
        AdmissionRejectionReason::QueueFull => &inner.metrics.reject_queue_full,
        AdmissionRejectionReason::QueueTimedOut => &inner.metrics.reject_queue_timeout,
        AdmissionRejectionReason::ControlReserveExhausted => {
            &inner.metrics.reject_control_exhausted
        }
    };
    counter.increment(1);
}

pub(super) fn state_metrics_snapshot(
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

pub(super) fn record_state_metrics(inner: &Inner, snapshot: StateMetricsSnapshot) {
    inner.metrics.limit.set(snapshot.desired_limit as f64);
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

pub(super) fn service_outcome_index(outcome: AdmissionOutcome) -> Option<usize> {
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

pub(super) fn pressure_outcome_index(outcome: AdmissionOutcome) -> Option<usize> {
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
