use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use metrics::{
    Counter, Gauge, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};

use super::{constants::STORAGE_OPERATIONS, operation_metrics::record_storage_operation};

#[derive(Default)]
struct OperationRecorder {
    counters: Mutex<HashMap<Key, Arc<AtomicU64>>>,
    histograms: Mutex<HashMap<Key, Arc<OperationHistogram>>>,
}

#[derive(Default)]
struct OperationHistogram(AtomicU64);

impl HistogramFn for OperationHistogram {
    fn record(&self, _: f64) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

impl Recorder for OperationRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        Counter::from_arc(
            self.counters
                .lock()
                .unwrap()
                .entry(key.clone())
                .or_default()
                .clone(),
        )
    }

    fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::from_arc(
            self.histograms
                .lock()
                .unwrap()
                .entry(key.clone())
                .or_default()
                .clone(),
        )
    }
}

#[test]
fn given_registered_storage_operations_when_recorded_then_counts_and_latency_keep_their_operation_labels()
 {
    let recorder = OperationRecorder::default();
    // Exercise the shared metric boundary directly: provider-specific behavior
    // cannot prove label identity for every operation, including fallback
    // labels.
    let operations: Vec<_> = STORAGE_OPERATIONS
        .iter()
        .copied()
        .chain(["query_source_page", "transact_write_items"])
        .collect();
    metrics::with_local_recorder(&recorder, || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                for (index, operation) in operations.iter().enumerate() {
                    for _ in 0..=index {
                        record_storage_operation(
                            operation,
                            std::future::ready(Ok::<_, storage_types::StorageError>(())),
                        )
                        .await
                        .unwrap();
                    }
                }
            });
    });
    let counters = recorder.counters.lock().unwrap();
    let histograms = recorder.histograms.lock().unwrap();
    for (index, operation) in operations.iter().enumerate() {
        let labels = [("operation", *operation)];
        let counter = Key::from_parts("storage.operation.total", &labels);
        let histogram = Key::from_parts("storage.operation.latency.ms", &labels);
        assert_eq!(
            counters[&counter].load(Ordering::Relaxed),
            (index + 1) as u64,
            "counter label: {operation}"
        );
        assert_eq!(
            histograms[&histogram].0.load(Ordering::Relaxed),
            (index + 1) as u64,
            "latency label: {operation}"
        );
    }
}
