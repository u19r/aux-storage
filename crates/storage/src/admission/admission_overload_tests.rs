use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};

use super::{
    AdmissionClass, AdmissionConfig, AdmissionController, AdmissionOutcome,
    AdmissionRejectionReason, AdmissionState, admission_transition::constants::WINDOW,
};

/// Records counters by name and reason label; gauges and histograms are inert.
#[derive(Default)]
struct CounterRecorder {
    counters: Mutex<HashMap<(String, String), Arc<AtomicU64>>>,
}

impl CounterRecorder {
    fn total(&self, name: &str, reason: &str) -> u64 {
        self.counters
            .lock()
            .unwrap()
            .iter()
            .filter(|((counter, label), _)| counter == name && label == reason)
            .map(|(_, value)| value.load(Ordering::Relaxed))
            .sum()
    }

    fn name_total(&self, name: &str) -> u64 {
        self.counters
            .lock()
            .unwrap()
            .iter()
            .filter(|((counter, _), _)| counter == name)
            .map(|(_, value)| value.load(Ordering::Relaxed))
            .sum()
    }
}

impl Recorder for CounterRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        let reason = key
            .labels()
            .find(|label| label.key() == "reason")
            .map_or_else(String::new, |label| label.value().to_owned());
        let value = self
            .counters
            .lock()
            .unwrap()
            .entry((key.name().to_owned(), reason))
            .or_default()
            .clone();
        Counter::from_arc(value)
    }

    fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}

/// Sleeps past the sampling window so the next release evaluates it.
async fn cross_window_boundary() {
    tokio::time::sleep(WINDOW + Duration::from_millis(50)).await;
}

#[tokio::test]
async fn queue_timeouts_in_an_overload_burst_do_not_pin_the_controller_in_emergency() {
    let recorder = CounterRecorder::default();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let config = AdmissionConfig {
        initial_sustainable_throughput_rps: 1,
        initial_latency_estimate_ms: 1,
        minimum_concurrency: 2,
        maximum_concurrency: 4,
        control_reserve_concurrency: 1,
        queue_capacity: 8,
        max_queue_wait_ms: 20,
        ..AdmissionConfig::default()
    };
    let controller = AdmissionController::new("overload-burst", config).expect("valid config");
    let limit = controller.snapshot().desired_limit;

    // Burst: demand exceeds the local limit, so queued acquires time out.
    let holders: Vec<_> = (0..limit)
        .map(|_| {
            controller
                .try_acquire(AdmissionClass::PointRead)
                .expect("holder fits the limit")
        })
        .collect();
    let mut waiters = Vec::new();
    for _ in 0..4 {
        let waiter = controller.clone();
        waiters.push(tokio::spawn(async move {
            waiter.acquire(AdmissionClass::PointRead).await
        }));
    }
    for waiter in waiters {
        let rejection = waiter
            .await
            .expect("waiter task")
            .expect_err("saturated controller times out the waiter");
        assert_eq!(rejection.reason, AdmissionRejectionReason::QueueTimedOut);
    }

    // Close the burst window through the real release path.
    cross_window_boundary().await;
    for holder in holders {
        holder.complete(AdmissionOutcome::Success(Duration::from_millis(1)));
    }
    assert_ne!(controller.snapshot().state, AdmissionState::Emergency);

    // Light phase: sequential requests across further window boundaries.
    for _ in 0..1 {
        cross_window_boundary().await;
        controller
            .acquire(AdmissionClass::PointRead)
            .await
            .expect("light load is admitted immediately")
            .complete(AdmissionOutcome::Success(Duration::from_millis(1)));
        assert_ne!(controller.snapshot().state, AdmissionState::Emergency);
    }

    let snapshot = controller.snapshot();
    assert_ne!(snapshot.state, AdmissionState::Emergency);
    assert!(snapshot.desired_limit >= snapshot.minimum_concurrency);
    assert_eq!(snapshot.queue_depth, 0);
    assert_eq!(snapshot.in_flight, 0);
    assert_eq!(
        recorder.total("storage.admission.reject.total", "queue_timeout"),
        4
    );
    assert_eq!(
        recorder.name_total("storage.admission.provider.pressure.total"),
        0
    );
}

/// Short sampling window so a multi-window recovery runs in milliseconds.
const TEST_WINDOW: Duration = Duration::from_millis(20);
/// Latency far above the 1 ms estimate: a real degradation.
const DEGRADED: Duration = Duration::from_millis(50);
/// Latency at the 1 ms estimate: gradient 1.2, healthy.
const HEALTHY: Duration = Duration::from_millis(1);

fn recovery_controller(name: &str) -> AdmissionController {
    let config = AdmissionConfig {
        initial_sustainable_throughput_rps: 40_000,
        initial_latency_estimate_ms: 1,
        minimum_concurrency: 2,
        maximum_concurrency: 64,
        control_reserve_concurrency: 1,
        queue_capacity: 8,
        max_queue_wait_ms: 5,
        ..AdmissionConfig::default()
    };
    AdmissionController::try_new_with_window(name, config, TEST_WINDOW).expect("valid config")
}

/// Fill the current limit repeatedly, completing every permit with `latency`,
/// then cross the window boundary. The window is saturated and carries enough
/// samples for a latency gradient.
async fn saturated_window(controller: &AdmissionController, latency: Duration) {
    let limit = controller.snapshot().desired_limit;
    for _ in 0..(32 / limit + 2) {
        let permits: Vec<_> = (0..limit)
            .map_while(|_| controller.try_acquire(AdmissionClass::PointRead).ok())
            .collect();
        for permit in permits {
            permit.complete(AdmissionOutcome::Success(latency));
        }
    }
    tokio::time::sleep(TEST_WINDOW + Duration::from_millis(5)).await;
}

/// One healthy request, then cross the window boundary.
async fn light_window(controller: &AdmissionController) {
    controller
        .try_acquire(AdmissionClass::PointRead)
        .expect("light load is admitted")
        .complete(AdmissionOutcome::Success(HEALTHY));
    tokio::time::sleep(TEST_WINDOW + Duration::from_millis(5)).await;
}

/// Degrade latency under saturation until the controller has backed off.
async fn overload(controller: &AdmissionController) {
    let starting = controller.snapshot().desired_limit;
    for _ in 0..4 {
        saturated_window(controller, DEGRADED).await;
    }
    let reduced = controller.snapshot().desired_limit;
    assert!(
        reduced < starting,
        "overload lowers {starting} -> {reduced}"
    );
}

/// Benchmark regression: after a latency backoff, saturated windows at healthy
/// service latency must restore the starting limit within a few windows.
#[tokio::test]
async fn saturated_healthy_windows_restore_the_starting_limit_after_backoff() {
    let recorder = CounterRecorder::default();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let controller = recovery_controller("saturated-recovery");
    let starting = controller.snapshot().desired_limit;
    overload(&controller).await;
    // The first healthy window's first completion closes the last degraded
    // window, which may still back off. Healthy windows follow.
    saturated_window(&controller, HEALTHY).await;
    let mut lowest = controller.snapshot().desired_limit;
    for _ in 0..10 {
        saturated_window(&controller, HEALTHY).await;
        let limit = controller.snapshot().desired_limit;
        assert!(
            limit >= lowest,
            "healthy gradient lowered {lowest} -> {limit}"
        );
        lowest = limit;
    }
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.desired_limit, starting);
    assert_eq!(snapshot.state, AdmissionState::Stable);
    assert_eq!(
        recorder.name_total("storage.admission.provider.pressure.total"),
        0
    );
    // Recovery to the starting limit is not a probe; probes start above it.
    assert_eq!(recorder.name_total("storage.admission.probe.total"), 0);
}

/// Benchmark regression: an idle rest after a backoff must not leave the
/// limit low. Light healthy traffic after the rest restores it.
#[tokio::test]
async fn idle_rest_then_light_load_restores_the_starting_limit_after_backoff() {
    let recorder = CounterRecorder::default();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let controller = recovery_controller("idle-recovery");
    let starting = controller.snapshot().desired_limit;
    overload(&controller).await;
    tokio::time::sleep(TEST_WINDOW * 10).await;
    for _ in 0..10 {
        light_window(&controller).await;
    }
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.desired_limit, starting);
    assert_eq!(snapshot.state, AdmissionState::Stable);
    assert_eq!(
        recorder.name_total("storage.admission.provider.pressure.total"),
        0
    );
    assert_eq!(recorder.name_total("storage.admission.probe.total"), 0);
}
