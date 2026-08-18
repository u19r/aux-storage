use std::{sync::LazyLock, time::Instant};

use metrics::{Counter, Histogram};
use storage_types::StorageResult;

use crate::{
    constants::{STORAGE_OPERATION_LATENCY_MS_METRIC, STORAGE_OPERATION_TOTAL_METRIC},
    database_manager::RoutedWriteTargetRole,
};

const STORAGE_OPERATIONS: &[&str] = &[
    "batch_get_item",
    "create_table",
    "delete_item",
    "delete_table",
    "get_item",
    "get_table_info",
    "guarded_delete_item",
    "guarded_put_item",
    "guarded_update_item",
    "list_tables",
    "put_item",
    "query_table",
    "scan_table",
    "table_exists",
    "update_item",
    "update_table",
];

struct StorageOperationMetricHandles {
    total: Counter,
    latency_ms: Histogram,
}

struct RoutedStorageOperationMetricHandles {
    primary: StorageOperationMetricHandles,
    migration: StorageOperationMetricHandles,
}

static STORAGE_OPERATION_METRICS: LazyLock<Vec<StorageOperationMetricHandles>> =
    LazyLock::new(|| {
        STORAGE_OPERATIONS
            .iter()
            .map(|operation| StorageOperationMetricHandles {
                total: metrics::counter!(
                    STORAGE_OPERATION_TOTAL_METRIC.name(),
                    "operation" => *operation,
                ),
                latency_ms: metrics::histogram!(
                    STORAGE_OPERATION_LATENCY_MS_METRIC.name(),
                    "operation" => *operation,
                ),
            })
            .collect()
    });

static ROUTED_STORAGE_OPERATION_METRICS: LazyLock<Vec<RoutedStorageOperationMetricHandles>> =
    LazyLock::new(|| {
        STORAGE_OPERATIONS
            .iter()
            .map(|operation| RoutedStorageOperationMetricHandles {
                primary: StorageOperationMetricHandles {
                    total: metrics::counter!(
                        STORAGE_OPERATION_TOTAL_METRIC.name(),
                        "operation" => *operation,
                        "target" => "primary",
                    ),
                    latency_ms: metrics::histogram!(
                        STORAGE_OPERATION_LATENCY_MS_METRIC.name(),
                        "operation" => *operation,
                        "target" => "primary",
                    ),
                },
                migration: StorageOperationMetricHandles {
                    total: metrics::counter!(
                        STORAGE_OPERATION_TOTAL_METRIC.name(),
                        "operation" => *operation,
                        "target" => "migration",
                    ),
                    latency_ms: metrics::histogram!(
                        STORAGE_OPERATION_LATENCY_MS_METRIC.name(),
                        "operation" => *operation,
                        "target" => "migration",
                    ),
                },
            })
            .collect()
    });

#[cfg(not(feature = "opt-loop-profiling"))]
pub(crate) async fn record_storage_operation<T, Fut>(
    operation: &'static str,
    fut: Fut,
) -> StorageResult<T>
where
    Fut: std::future::Future<Output = StorageResult<T>>,
{
    let database_call = metrics_facade::begin_database_call(operation);
    let start = Instant::now();
    let result = fut.await;
    drop(database_call);
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    record_operation_metrics(operation, elapsed_ms);
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
    let database_call = metrics_facade::begin_database_call(operation);
    let start = Instant::now();
    let result = fut.await;
    drop(database_call);
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    record_routed_operation_metrics(operation, target, elapsed_ms);
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
    let database_call = metrics_facade::begin_database_call(operation);
    let start = Instant::now();
    let result = fut.await;
    drop(database_call);
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    record_operation_metrics(operation, elapsed_ms);

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
    let database_call = metrics_facade::begin_database_call(operation);
    let start = Instant::now();
    let result = fut.await;
    drop(database_call);
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    record_routed_operation_metrics(operation, target, elapsed_ms);

    let response_bytes = result
        .as_ref()
        .map(opt_loop_probe::estimate_json_bytes)
        .unwrap_or(0);
    opt_loop_probe::record_storage_call(operation, response_bytes);

    result
}

fn record_operation_metrics(operation: &'static str, elapsed_ms: f64) {
    if let Some(handles) = storage_operation_metric_handles(operation) {
        handles.total.increment(1);
        handles.latency_ms.record(elapsed_ms);
        return;
    }
    metrics_facade::counter!(STORAGE_OPERATION_TOTAL_METRIC, "operation" => operation).increment(1);
    metrics_facade::histogram!(STORAGE_OPERATION_LATENCY_MS_METRIC, "operation" => operation)
        .record(elapsed_ms);
}

fn record_routed_operation_metrics(
    operation: &'static str,
    target: RoutedWriteTargetRole,
    elapsed_ms: f64,
) {
    if let Some(handles) = routed_storage_operation_metric_handles(operation, target) {
        handles.total.increment(1);
        handles.latency_ms.record(elapsed_ms);
        return;
    }
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
}

fn storage_operation_metric_handles(
    operation: &'static str,
) -> Option<&'static StorageOperationMetricHandles> {
    operation_index(operation).map(|index| &STORAGE_OPERATION_METRICS[index])
}

fn routed_storage_operation_metric_handles(
    operation: &'static str,
    target: RoutedWriteTargetRole,
) -> Option<&'static StorageOperationMetricHandles> {
    let handles = &ROUTED_STORAGE_OPERATION_METRICS[operation_index(operation)?];
    match target {
        RoutedWriteTargetRole::Primary => Some(&handles.primary),
        RoutedWriteTargetRole::Migration => Some(&handles.migration),
    }
}

fn operation_index(operation: &'static str) -> Option<usize> {
    match operation {
        "batch_get_item" => Some(0),
        "create_table" => Some(1),
        "delete_item" => Some(2),
        "delete_table" => Some(3),
        "get_item" => Some(4),
        "get_table_info" => Some(5),
        "guarded_delete_item" => Some(6),
        "guarded_put_item" => Some(7),
        "guarded_update_item" => Some(8),
        "list_tables" => Some(9),
        "put_item" => Some(10),
        "query_table" => Some(11),
        "scan_table" => Some(12),
        "table_exists" => Some(13),
        "update_item" => Some(14),
        "update_table" => Some(15),
        _ => None,
    }
}
