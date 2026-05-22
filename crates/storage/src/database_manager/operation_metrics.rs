use std::time::Instant;

use storage_types::StorageResult;

use crate::{
    constants::{STORAGE_OPERATION_LATENCY_MS_METRIC, STORAGE_OPERATION_TOTAL_METRIC},
    database_manager::RoutedWriteTargetRole,
};

#[cfg(not(feature = "opt-loop-profiling"))]
pub(crate) async fn record_storage_operation<T, Fut>(
    operation: &'static str,
    fut: Fut,
) -> StorageResult<T>
where
    Fut: std::future::Future<Output = StorageResult<T>>,
{
    let start = Instant::now();
    let result = fut.await;
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    metrics_facade::counter!(STORAGE_OPERATION_TOTAL_METRIC, "operation" => operation).increment(1);
    metrics_facade::histogram!(STORAGE_OPERATION_LATENCY_MS_METRIC, "operation" => operation)
        .record(elapsed_ms);
    result
}

#[cfg(not(feature = "opt-loop-profiling"))]
pub(crate) async fn record_storage_operation_for_target<T, Fut>(
    operation: &'static str,
    target: RoutedWriteTargetRole,
    fut: Fut,
) -> StorageResult<T>
where
    Fut: std::future::Future<Output = StorageResult<T>>,
{
    let start = Instant::now();
    let result = fut.await;
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    metrics_facade::counter!(
        STORAGE_OPERATION_TOTAL_METRIC,
        "operation" => operation,
        "target" => target.metric_label()
    )
    .increment(1);
    metrics_facade::histogram!(
        STORAGE_OPERATION_LATENCY_MS_METRIC,
        "operation" => operation,
        "target" => target.metric_label()
    )
    .record(elapsed_ms);
    result
}

#[cfg(feature = "opt-loop-profiling")]
pub(crate) async fn record_storage_operation<T, Fut>(
    operation: &'static str,
    fut: Fut,
) -> StorageResult<T>
where
    Fut: std::future::Future<Output = StorageResult<T>>,
    T: serde::Serialize,
{
    let start = Instant::now();
    let result = fut.await;
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    metrics_facade::counter!(STORAGE_OPERATION_TOTAL_METRIC, "operation" => operation).increment(1);
    metrics_facade::histogram!(STORAGE_OPERATION_LATENCY_MS_METRIC, "operation" => operation)
        .record(elapsed_ms);

    let response_bytes = result
        .as_ref()
        .map(opt_loop_probe::estimate_json_bytes)
        .unwrap_or(0);
    opt_loop_probe::record_storage_call(operation, response_bytes);

    result
}

#[cfg(feature = "opt-loop-profiling")]
pub(crate) async fn record_storage_operation_for_target<T, Fut>(
    operation: &'static str,
    target: RoutedWriteTargetRole,
    fut: Fut,
) -> StorageResult<T>
where
    Fut: std::future::Future<Output = StorageResult<T>>,
    T: serde::Serialize,
{
    let start = Instant::now();
    let result = fut.await;
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    metrics_facade::counter!(
        STORAGE_OPERATION_TOTAL_METRIC,
        "operation" => operation,
        "target" => target.metric_label()
    )
    .increment(1);
    metrics_facade::histogram!(
        STORAGE_OPERATION_LATENCY_MS_METRIC,
        "operation" => operation,
        "target" => target.metric_label()
    )
    .record(elapsed_ms);

    let response_bytes = result
        .as_ref()
        .map(opt_loop_probe::estimate_json_bytes)
        .unwrap_or(0);
    opt_loop_probe::record_storage_call(operation, response_bytes);

    result
}
