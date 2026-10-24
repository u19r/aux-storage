use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use alloc_counter::AllocationGuard;
use cpu_time::ProcessTime;
use metrics::{
    Counter, Gauge, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};

use super::{
    AdmissionClass, AdmissionConfig, AdmissionController, AdmissionOutcome,
    metrics::{record_state_metrics, state_metrics_snapshot},
    state::lock_state,
};

#[derive(Default)]
struct AtomicRecorder {
    gauges: Mutex<HashMap<Key, Arc<AtomicU64>>>,
}

struct AtomicHistogram(AtomicU64);

impl HistogramFn for AtomicHistogram {
    fn record(&self, value: f64) {
        self.0.store(value.to_bits(), Ordering::Relaxed);
    }
}

impl Recorder for AtomicRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, _: &Key, _: &Metadata<'_>) -> Counter {
        Counter::from_arc(Arc::new(AtomicU64::new(0)))
    }

    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        let gauge = self
            .gauges
            .lock()
            .unwrap()
            .entry(key.clone())
            .or_default()
            .clone();
        Gauge::from_arc(gauge)
    }

    fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::from_arc(Arc::new(AtomicHistogram(AtomicU64::new(0))))
    }
}

fn measure_admission(label: &str, iterations: usize, mut operation: impl FnMut()) {
    for _ in 0..100 {
        operation();
    }
    let guard = AllocationGuard::start(
        module_path!(),
        "admission_metrics",
        file!(),
        line!(),
        Some(label),
    );
    let started = ProcessTime::now();
    for _ in 0..iterations {
        operation();
    }
    let cpu = started.elapsed();
    let report = guard.finish();
    alloc_counter::emit_report(&report).unwrap();
    eprintln!(
        "admission_metrics label={label} iterations={iterations} cpu_ns={}",
        cpu.as_nanos()
    );
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_atomic_metric_handles_when_recorded_then_report_cpu_and_allocations() {
    let recorder = AtomicRecorder::default();
    metrics::with_local_recorder(&recorder, || {
        let config = AdmissionConfig {
            enabled: false,
            minimum_concurrency: 1,
            maximum_concurrency: 100,
            control_reserve_concurrency: 1,
            ..AdmissionConfig::default()
        };
        let controller = AdmissionController::try_new("metrics-performance", config).unwrap();
        let snapshot = state_metrics_snapshot(&lock_state(&controller.inner), Some(100.0));
        measure_admission("record_state", 200_000, || {
            record_state_metrics(&controller.inner, std::hint::black_box(snapshot))
        });
        measure_admission("acquire_complete", 100_000, || {
            let permit = controller.try_acquire(AdmissionClass::PointRead).unwrap();
            permit.complete(AdmissionOutcome::Success(Duration::from_millis(1)));
        });
    });
}

#[test]
fn given_atomic_recorder_when_admission_completes_then_publish_configuration_and_release_slot() {
    let recorder = AtomicRecorder::default();
    metrics::with_local_recorder(&recorder, || {
        let controller =
            AdmissionController::try_new("metrics-invariants", AdmissionConfig::default()).unwrap();
        for class in AdmissionClass::ALL {
            controller
                .try_acquire(class)
                .unwrap()
                .complete(AdmissionOutcome::Success(Duration::from_millis(1)));
        }
        let gauges = recorder.gauges.lock().unwrap();
        let value = |name: &str| {
            let (_, value) = gauges.iter().find(|(key, _)| key.name() == name).unwrap();
            f64::from_bits(value.load(Ordering::Relaxed))
        };
        assert_eq!(
            value("storage.admission.minimum"),
            controller.config().minimum_concurrency as f64
        );
        assert_eq!(
            value("storage.admission.effective_maximum"),
            controller.config().effective_maximum() as f64
        );
        assert_eq!(value("storage.admission.control.in_flight"), 0.0);
        assert_eq!(value("storage.admission.queue.depth"), 0.0);
        for (key, value) in gauges
            .iter()
            .filter(|(key, _)| key.name() == "storage.admission.in_flight")
        {
            assert_eq!(
                f64::from_bits(value.load(Ordering::Relaxed)),
                0.0,
                "{key:?}"
            );
        }
    });
}
