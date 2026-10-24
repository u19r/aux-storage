//! Low-overhead provider counters used by bounded ReadSequence evidence.
//!
//! The counters are deliberately source-owned rather than inferred from API
//! request counts.  A provider call is one invocation of a provider read
//! operation; a serialized call is one operation which passes through a
//! transaction-context serialization gate; a blocking call is one for which
//! contention was observed at that gate.  The depth gauge is the largest
//! number of in-flight calls observed for the same context, including calls
//! waiting at the gate.

use std::{
    fmt::Write as _,
    sync::atomic::{AtomicU64, Ordering},
};

const BACKENDS: [&str; 4] = ["postgres", "sqlite", "turso", "rocksdb"];
const OPERATIONS: [&str; 4] = [
    "get_item",
    "batch_get_item",
    "query_table",
    "read_sequence_plan",
];

struct ProviderMetrics {
    requests: AtomicU64,
    serial_requests: AtomicU64,
    blocking_requests: AtomicU64,
    database_awaits: AtomicU64,
    database_serial_waves: AtomicU64,
    database_potentially_blocking_awaits: AtomicU64,
    database_max_serial_depth: AtomicU64,
    items: AtomicU64,
    bytes: AtomicU64,
    pages: AtomicU64,
    continuation_pages: AtomicU64,
    terminal_pages: AtomicU64,
    max_serial_depth: AtomicU64,
}

/// One provider-boundary ReadSequence observation.  Callers construct this
/// from the counts and contention state they already own, keeping the metric
/// event's dimensions together instead of relying on positional arguments.
#[derive(Debug, Clone, Copy)]
pub struct ReadSequenceProviderCall {
    pub backend: &'static str,
    pub operation: &'static str,
    pub item_count: u64,
    pub byte_count: u64,
    pub serialized: bool,
    pub blocking: bool,
    pub serial_depth: u64,
    pub has_continuation: Option<bool>,
}

impl ProviderMetrics {
    const fn new() -> Self {
        Self {
            requests: AtomicU64::new(0),
            serial_requests: AtomicU64::new(0),
            blocking_requests: AtomicU64::new(0),
            database_awaits: AtomicU64::new(0),
            database_serial_waves: AtomicU64::new(0),
            database_potentially_blocking_awaits: AtomicU64::new(0),
            database_max_serial_depth: AtomicU64::new(0),
            items: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            pages: AtomicU64::new(0),
            continuation_pages: AtomicU64::new(0),
            terminal_pages: AtomicU64::new(0),
            max_serial_depth: AtomicU64::new(0),
        }
    }

    fn is_empty(&self) -> bool {
        self.requests.load(Ordering::Relaxed) == 0
            && self.serial_requests.load(Ordering::Relaxed) == 0
            && self.blocking_requests.load(Ordering::Relaxed) == 0
            && self.database_awaits.load(Ordering::Relaxed) == 0
            && self.database_serial_waves.load(Ordering::Relaxed) == 0
            && self
                .database_potentially_blocking_awaits
                .load(Ordering::Relaxed)
                == 0
            && self.database_max_serial_depth.load(Ordering::Relaxed) == 0
            && self.items.load(Ordering::Relaxed) == 0
            && self.bytes.load(Ordering::Relaxed) == 0
            && self.pages.load(Ordering::Relaxed) == 0
            && self.continuation_pages.load(Ordering::Relaxed) == 0
            && self.terminal_pages.load(Ordering::Relaxed) == 0
            && self.max_serial_depth.load(Ordering::Relaxed) == 0
    }
}

static METRICS: [[ProviderMetrics; OPERATIONS.len()]; BACKENDS.len()] = [
    [
        ProviderMetrics::new(),
        ProviderMetrics::new(),
        ProviderMetrics::new(),
        ProviderMetrics::new(),
    ],
    [
        ProviderMetrics::new(),
        ProviderMetrics::new(),
        ProviderMetrics::new(),
        ProviderMetrics::new(),
    ],
    [
        ProviderMetrics::new(),
        ProviderMetrics::new(),
        ProviderMetrics::new(),
        ProviderMetrics::new(),
    ],
    [
        ProviderMetrics::new(),
        ProviderMetrics::new(),
        ProviderMetrics::new(),
        ProviderMetrics::new(),
    ],
];

/// Record one provider operation without allocating or iterating returned
/// items.  The caller supplies aggregate item and byte counts already
/// available at its backend boundary.
pub fn record_read_sequence_provider_call(call: ReadSequenceProviderCall) {
    let Some(backend_index) = backend_index(call.backend) else {
        return;
    };
    let Some(operation_index) = operation_index(call.operation) else {
        return;
    };
    let metrics = &METRICS[backend_index][operation_index];
    metrics.requests.fetch_add(1, Ordering::Relaxed);
    if call.serialized {
        metrics.serial_requests.fetch_add(1, Ordering::Relaxed);
    }
    if call.blocking {
        metrics.blocking_requests.fetch_add(1, Ordering::Relaxed);
    }
    metrics.items.fetch_add(call.item_count, Ordering::Relaxed);
    metrics.bytes.fetch_add(call.byte_count, Ordering::Relaxed);
    if let Some(has_continuation) = call.has_continuation {
        metrics.pages.fetch_add(1, Ordering::Relaxed);
        if has_continuation {
            metrics.continuation_pages.fetch_add(1, Ordering::Relaxed);
        } else {
            metrics.terminal_pages.fetch_add(1, Ordering::Relaxed);
        }
    }
    if call.serial_depth > 0 {
        let mut current = metrics.max_serial_depth.load(Ordering::Relaxed);
        while call.serial_depth > current {
            match metrics.max_serial_depth.compare_exchange_weak(
                current,
                call.serial_depth,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }
}

/// Record one actual SQL database future for a ReadSequence operation.
///
/// This is deliberately separate from the provider-operation counter: the
/// latter describes returned items and pages, while this counter is emitted
/// at the backend query await and prices one local access as exactly 1 ms for
/// the Section 8.1 projection.
pub fn record_read_sequence_database_await(
    backend: &'static str,
    operation: &'static str,
    serial_wave: bool,
    potentially_blocking: bool,
    serial_depth: u64,
) {
    let Some(backend_index) = backend_index(backend) else {
        return;
    };
    let Some(operation_index) = operation_index(operation) else {
        return;
    };
    let metrics = &METRICS[backend_index][operation_index];
    metrics.database_awaits.fetch_add(1, Ordering::Relaxed);
    if serial_wave {
        metrics
            .database_serial_waves
            .fetch_add(1, Ordering::Relaxed);
    }
    if potentially_blocking {
        metrics
            .database_potentially_blocking_awaits
            .fetch_add(1, Ordering::Relaxed);
    }
    if serial_depth > 0 {
        let mut current = metrics.database_max_serial_depth.load(Ordering::Relaxed);
        while serial_depth > current {
            match metrics.database_max_serial_depth.compare_exchange_weak(
                current,
                serial_depth,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }
}

fn backend_index(backend: &str) -> Option<usize> {
    match backend {
        "postgres" => Some(0),
        "sqlite" => Some(1),
        "turso" => Some(2),
        "rocksdb" => Some(3),
        _ => None,
    }
}

fn operation_index(operation: &str) -> Option<usize> {
    match operation {
        "get_item" => Some(0),
        "batch_get_item" => Some(1),
        "query_table" => Some(2),
        "read_sequence_plan" => Some(3),
        _ => None,
    }
}

/// Render the counters as a Prometheus text-format fragment.
pub fn read_sequence_provider_metrics_snapshot() -> String {
    let mut output = String::new();
    let has_metrics = METRICS
        .iter()
        .flat_map(|backend| backend.iter())
        .any(|metrics| !metrics.is_empty());
    if !has_metrics {
        return output;
    }
    append_metric_headers(&mut output);
    for (backend_index, backend) in BACKENDS.iter().enumerate() {
        for (operation_index, operation) in OPERATIONS.iter().enumerate() {
            let metrics = &METRICS[backend_index][operation_index];
            if metrics.is_empty() {
                continue;
            }
            push_counter_value(
                &mut output,
                "storage_read_sequence_provider_requests_total",
                backend,
                operation,
                metrics.requests.load(Ordering::Relaxed),
            );
            push_counter_value(
                &mut output,
                "storage_read_sequence_provider_serial_requests_total",
                backend,
                operation,
                metrics.serial_requests.load(Ordering::Relaxed),
            );
            push_counter_value(
                &mut output,
                "storage_read_sequence_provider_blocking_requests_total",
                backend,
                operation,
                metrics.blocking_requests.load(Ordering::Relaxed),
            );
            push_counter_value(
                &mut output,
                "storage_read_sequence_database_awaits_total",
                backend,
                operation,
                metrics.database_awaits.load(Ordering::Relaxed),
            );
            push_counter_value(
                &mut output,
                "storage_read_sequence_database_serial_waves_total",
                backend,
                operation,
                metrics.database_serial_waves.load(Ordering::Relaxed),
            );
            push_counter_value(
                &mut output,
                "storage_read_sequence_database_potentially_blocking_awaits_total",
                backend,
                operation,
                metrics
                    .database_potentially_blocking_awaits
                    .load(Ordering::Relaxed),
            );
            push_counter_value(
                &mut output,
                "storage_read_sequence_database_1ms_projection_total",
                backend,
                operation,
                metrics.database_awaits.load(Ordering::Relaxed),
            );
            push_gauge_value(
                &mut output,
                "storage_read_sequence_database_max_serial_depth",
                backend,
                operation,
                metrics.database_max_serial_depth.load(Ordering::Relaxed),
            );
            push_counter_value(
                &mut output,
                "storage_read_sequence_provider_items_total",
                backend,
                operation,
                metrics.items.load(Ordering::Relaxed),
            );
            push_counter_value(
                &mut output,
                "storage_read_sequence_provider_bytes_total",
                backend,
                operation,
                metrics.bytes.load(Ordering::Relaxed),
            );
            if metrics.pages.load(Ordering::Relaxed) > 0 {
                push_counter_value(
                    &mut output,
                    "storage_read_sequence_provider_pages_total",
                    backend,
                    operation,
                    metrics.pages.load(Ordering::Relaxed),
                );
                push_counter_value(
                    &mut output,
                    "storage_read_sequence_provider_continuation_pages_total",
                    backend,
                    operation,
                    metrics.continuation_pages.load(Ordering::Relaxed),
                );
                push_counter_value(
                    &mut output,
                    "storage_read_sequence_provider_terminal_pages_total",
                    backend,
                    operation,
                    metrics.terminal_pages.load(Ordering::Relaxed),
                );
            }
            push_gauge_value(
                &mut output,
                "storage_read_sequence_provider_max_serial_depth",
                backend,
                operation,
                metrics.max_serial_depth.load(Ordering::Relaxed),
            );
        }
    }
    output
}

fn append_metric_headers(output: &mut String) {
    const COUNTERS: &[(&str, &str)] = &[
        (
            "storage_read_sequence_provider_requests_total",
            "Provider read operation attempts.",
        ),
        (
            "storage_read_sequence_provider_serial_requests_total",
            "Provider reads which passed through a serialized context gate.",
        ),
        (
            "storage_read_sequence_provider_blocking_requests_total",
            "Provider reads for which context-gate contention was observed.",
        ),
        (
            "storage_read_sequence_database_awaits_total",
            "Actual SQL database futures awaited by ReadSequence provider paths.",
        ),
        (
            "storage_read_sequence_database_serial_waves_total",
            "Actual SQL database await waves serialized by a ReadSequence context.",
        ),
        (
            "storage_read_sequence_database_potentially_blocking_awaits_total",
            "Actual SQL database awaits which may block on the database or connection.",
        ),
        (
            "storage_read_sequence_database_1ms_projection_total",
            "One-millisecond local database-access projection per actual await.",
        ),
        (
            "storage_read_sequence_provider_items_total",
            "Items returned by provider reads.",
        ),
        (
            "storage_read_sequence_provider_bytes_total",
            "Payload bytes returned by provider reads.",
        ),
        (
            "storage_read_sequence_provider_pages_total",
            "Provider query pages returned.",
        ),
        (
            "storage_read_sequence_provider_continuation_pages_total",
            "Provider query pages with a continuation cursor.",
        ),
        (
            "storage_read_sequence_provider_terminal_pages_total",
            "Provider query pages without a continuation cursor.",
        ),
    ];
    for (name, help) in COUNTERS {
        push_metric_header(output, name, "counter", help);
    }
    push_metric_header(
        output,
        "storage_read_sequence_provider_max_serial_depth",
        "gauge",
        "Maximum in-flight calls observed for a provider context.",
    );
}

pub(super) fn push_counter_value(
    output: &mut String,
    name: &str,
    backend: &str,
    operation: &str,
    value: u64,
) {
    let _ = writeln!(
        output,
        "{name}{{backend=\"{backend}\",operation=\"{operation}\"}} {value}"
    );
}

fn push_gauge_value(output: &mut String, name: &str, backend: &str, operation: &str, value: u64) {
    let _ = writeln!(
        output,
        "{name}{{backend=\"{backend}\",operation=\"{operation}\"}} {value}"
    );
}

fn push_metric_header(output: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(output, "# HELP {name} {help}");
    let _ = writeln!(output, "# TYPE {name} {kind}");
}
