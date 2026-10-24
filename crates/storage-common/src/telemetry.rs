//! Provider-stage telemetry shared by the kv and sql backends.
//!
//! Span fields and stage counters use identical names and semantics on every
//! backend. Common hot operation/stage pairs use cached handles; other pairs
//! use the facade recording path.

use std::{sync::LazyLock, time::Duration};

use metrics::{Counter, Histogram};
use metrics_facade::{CounterMetric, HistogramMetric};

/// Record items/bytes returned on the current tracing span.
pub fn record_read(items: usize, bytes: usize) {
    let span = tracing::Span::current();
    span.record("items_returned", items as u64);
    span.record("bytes_read", bytes as u64);
}

/// Record items/bytes written on the current tracing span.
pub fn record_write(items: usize, bytes: usize) {
    let span = tracing::Span::current();
    span.record("items_updated", items as u64);
    span.record("bytes_written", bytes as u64);
}

/// Record one timed provider stage (count + latency histogram).
pub fn record_provider_stage(operation: &'static str, stage: &'static str, elapsed: Duration) {
    if let Some(handles) = provider_stage_metric_handles(operation, stage) {
        handles.total.increment(1);
        handles.latency_ms.record(elapsed.as_secs_f64() * 1000.0);
        return;
    }
    metrics_facade::counter!(
        CounterMetric::StorageProviderStageTotalMetric,
        "operation" => operation,
        "stage" => stage,
    )
    .increment(1);
    metrics_facade::histogram!(
        HistogramMetric::StorageProviderStageLatencyMsMetric,
        "operation" => operation,
        "stage" => stage,
    )
    .record(elapsed.as_secs_f64() * 1000.0);
}

struct ProviderStageMetricHandles {
    total: Counter,
    latency_ms: Histogram,
}

static PROVIDER_STAGE_METRICS: LazyLock<[ProviderStageMetricHandles; 14]> = LazyLock::new(|| {
    [
        provider_stage_metric("batch_get_item", "decode"),
        provider_stage_metric("batch_get_item", "fdb_wait"),
        provider_stage_metric("batch_get_item", "response_materialization"),
        provider_stage_metric("get_item", "decode"),
        provider_stage_metric("get_item", "fdb_wait"),
        provider_stage_metric("get_item", "metadata"),
        provider_stage_metric("query", "decode"),
        provider_stage_metric("query", "fdb_wait"),
        provider_stage_metric("query", "gsi_readiness"),
        provider_stage_metric("query", "key_plan"),
        provider_stage_metric("query", "metadata"),
        provider_stage_metric("query", "primary_revision_batch"),
        provider_stage_metric("query", "projection"),
        provider_stage_metric("query", "response_materialization"),
    ]
});

// TODO: migrate provider operation and stage callers to typed metric labels.
fn provider_stage_metric(
    operation: &'static str,
    stage: &'static str,
) -> ProviderStageMetricHandles {
    ProviderStageMetricHandles {
        total: metrics::counter!(
            CounterMetric::StorageProviderStageTotalMetric.name(),
            "operation" => operation,
            "stage" => stage,
        ),
        latency_ms: metrics::histogram!(
            HistogramMetric::StorageProviderStageLatencyMsMetric.name(),
            "operation" => operation,
            "stage" => stage,
        ),
    }
}

// TODO: migrate provider operation and stage callers to typed metric labels.
fn provider_stage_metric_handles(
    operation: &'static str,
    stage: &'static str,
) -> Option<&'static ProviderStageMetricHandles> {
    let index = match (operation, stage) {
        ("batch_get_item", "decode") => 0,
        ("batch_get_item", "fdb_wait") => 1,
        ("batch_get_item", "response_materialization") => 2,
        ("get_item", "decode") => 3,
        ("get_item", "fdb_wait") => 4,
        ("get_item", "metadata") => 5,
        ("query", "decode") => 6,
        ("query", "fdb_wait") => 7,
        ("query", "gsi_readiness") => 8,
        ("query", "key_plan") => 9,
        ("query", "metadata") => 10,
        ("query", "primary_revision_batch") => 11,
        ("query", "projection") => 12,
        ("query", "response_materialization") => 13,
        _ => return None,
    };
    Some(&PROVIDER_STAGE_METRICS[index])
}

/// Record a per-request cardinality value alongside the timed provider stages.
// TODO: migrate provider operation and stage callers to typed metric labels.
pub fn record_provider_count(operation: &'static str, stage: &'static str, value: usize) {
    metrics_facade::counter!(
        CounterMetric::StorageProviderStageTotalMetric,
        "operation" => operation,
        "stage" => stage,
    )
    .increment(value as u64);
}
