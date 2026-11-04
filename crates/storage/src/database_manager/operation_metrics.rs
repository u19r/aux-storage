use std::{
    collections::HashMap,
    sync::LazyLock,
    time::{Duration, Instant},
};

use metrics::{Counter, Histogram};
use storage_types::StorageResult;

use crate::{
    constants::{STORAGE_OPERATION_LATENCY_MS_METRIC, STORAGE_OPERATION_TOTAL_METRIC},
    database_manager::constants::STORAGE_OPERATIONS,
};

struct StorageOperationMetricHandles {
    total: Counter,
    latency_ms: Histogram,
}

static STORAGE_OPERATION_METRICS: LazyLock<HashMap<&'static str, StorageOperationMetricHandles>> =
    LazyLock::new(|| {
        STORAGE_OPERATIONS
            .iter()
            .map(|operation| {
                (
                    *operation,
                    StorageOperationMetricHandles {
                        total: metrics::counter!(
                            STORAGE_OPERATION_TOTAL_METRIC.name(),
                            "operation" => *operation,
                        ),
                        latency_ms: metrics::histogram!(
                            STORAGE_OPERATION_LATENCY_MS_METRIC.name(),
                            "operation" => *operation,
                        ),
                    },
                )
            })
            .collect()
    });

// Construct the observed future in the admission factory, before native
// handoff. Its opaque metric handle retains attribution; its timer starts when
// polled.
#[cfg(not(feature = "opt-loop-profiling"))]
pub(crate) fn record_storage_operation<T, Fut>(
    operation: &'static str,
    fut: Fut,
) -> impl std::future::Future<Output = StorageResult<T>>
where
    Fut: std::future::Future<Output = StorageResult<T>>,
{
    let request_metric = metrics_facade::request_storage_operation_histogram(operation);
    async move {
        let database_call = metrics_facade::begin_database_call(operation);
        let start = Instant::now();
        let result = fut.await;
        drop(database_call);
        let elapsed = start.elapsed();
        record_operation_metrics(operation, elapsed);
        request_metric.record(elapsed.as_secs_f64());
        result
    }
}

#[cfg(feature = "opt-loop-profiling")]
pub(crate) fn record_storage_operation<T, Fut>(
    operation: &'static str,
    fut: Fut,
) -> impl std::future::Future<Output = StorageResult<T>>
where
    Fut: std::future::Future<Output = StorageResult<T>>,
    T: serde::Serialize,
{
    let request_metric = metrics_facade::request_storage_operation_histogram(operation);
    async move {
        let database_call = metrics_facade::begin_database_call(operation);
        let start = Instant::now();
        let result = fut.await;
        drop(database_call);
        let elapsed = start.elapsed();
        record_operation_metrics(operation, elapsed);
        request_metric.record(elapsed.as_secs_f64());

        let response_bytes = result
            .as_ref()
            .map(opt_loop_probe::estimate_json_bytes)
            .unwrap_or(0);
        opt_loop_probe::record_storage_call(operation, response_bytes);

        result
    }
}

fn record_operation_metrics(operation: &'static str, elapsed: Duration) {
    let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
    if let Some(handles) = STORAGE_OPERATION_METRICS.get(operation) {
        handles.total.increment(1);
        handles.latency_ms.record(elapsed_ms);
        return;
    }
    metrics_facade::counter!(STORAGE_OPERATION_TOTAL_METRIC, "operation" => operation).increment(1);
    metrics_facade::histogram!(STORAGE_OPERATION_LATENCY_MS_METRIC, "operation" => operation)
        .record(elapsed_ms);
}
