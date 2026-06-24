use std::{collections::HashMap, sync::LazyLock, time::Duration};

use storage_types::{AttributeValue, StorageResult, WireItem};
use tracing::Span;

use crate::{
    billing_metrics::{record_read_cost, wire_items_payload_bytes},
    constants,
};

type QueryItemsPage = (Vec<WireItem>, Option<String>);

pub(crate) fn record_read(items: usize, bytes: usize) {
    let span = Span::current();
    span.record("items_returned", items as u64);
    span.record("bytes_read", bytes as u64);
}

pub(crate) fn record_write(items: usize, bytes: usize) {
    let span = Span::current();
    span.record("items_updated", items as u64);
    span.record("bytes_written", bytes as u64);
}

pub(crate) fn compute_items_bytes(
    items: &[HashMap<String, AttributeValue>],
) -> StorageResult<usize> {
    let mut total = 0_usize;
    for item in items {
        total += storage_types::storage_serde::to_bytes(item)?.len();
    }
    Ok(total)
}

pub(crate) fn record_query_result(result: QueryItemsPage) -> QueryItemsPage {
    let (items, lek) = result;
    let bytes = wire_items_payload_bytes(&items);
    record_read(items.len(), bytes as usize);
    record_read_cost("query_table", "query", 1, bytes);
    (items, lek)
}

pub(crate) fn record_provider_stage(
    operation: &'static str,
    stage: &'static str,
    elapsed: Duration,
) {
    let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
    if let Some(handles) = provider_stage_metric_handles(operation, stage) {
        handles.total.increment(1);
        handles.latency_ms.record(elapsed_ms);
        return;
    }
    metrics_facade::counter!(
        constants::STORAGE_PROVIDER_STAGE_TOTAL_METRIC,
        "operation" => operation,
        "stage" => stage,
    )
    .increment(1);
    metrics_facade::histogram!(
        constants::STORAGE_PROVIDER_STAGE_LATENCY_MS_METRIC,
        "operation" => operation,
        "stage" => stage,
    )
    .record(elapsed_ms);
}

struct ProviderStageMetricHandles {
    total: metrics::Counter,
    latency_ms: metrics::Histogram,
}

static PROVIDER_STAGE_METRICS: LazyLock<[ProviderStageMetricHandles; 5]> = LazyLock::new(|| {
    [
        provider_stage_metric("batch_get_item", "decode"),
        provider_stage_metric("batch_get_item", "fdb_wait"),
        provider_stage_metric("batch_get_item", "response_materialization"),
        provider_stage_metric("query", "decode"),
        provider_stage_metric("query", "fdb_wait"),
    ]
});

fn provider_stage_metric(
    operation: &'static str,
    stage: &'static str,
) -> ProviderStageMetricHandles {
    ProviderStageMetricHandles {
        total: metrics::counter!(
            constants::STORAGE_PROVIDER_STAGE_TOTAL_METRIC.name(),
            "operation" => operation,
            "stage" => stage,
        ),
        latency_ms: metrics::histogram!(
            constants::STORAGE_PROVIDER_STAGE_LATENCY_MS_METRIC.name(),
            "operation" => operation,
            "stage" => stage,
        ),
    }
}

fn provider_stage_metric_handles(
    operation: &'static str,
    stage: &'static str,
) -> Option<&'static ProviderStageMetricHandles> {
    let index = match (operation, stage) {
        ("batch_get_item", "decode") => 0,
        ("batch_get_item", "fdb_wait") => 1,
        ("batch_get_item", "response_materialization") => 2,
        ("query", "decode") => 3,
        ("query", "fdb_wait") => 4,
        _ => return None,
    };
    Some(&PROVIDER_STAGE_METRICS[index])
}
