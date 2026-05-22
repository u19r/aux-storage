//! Shared tracing helpers to standardize span fields across backends.
use tracing::{Level, Span, field};

/// Start an operation span with standardized fields.
pub fn start_op_span(op: &str, table: &str) -> Span {
    tracing::span!(Level::INFO, "storage_op", op, table)
}

/// Record optional limit info.
pub fn record_limit(effective: u32, requested: Option<u32>) {
    Span::current().record("limit_effective", field::display(effective));
    if let Some(r) = requested {
        Span::current().record("limit_requested", field::display(r));
    }
}

/// Record result metrics.
pub fn record_result(count: usize, has_more: bool, elapsed_ms: u64) {
    Span::current().record("items", field::display(count));
    Span::current().record("has_more", field::display(has_more));
    Span::current().record("elapsed_ms", field::display(elapsed_ms));
}
