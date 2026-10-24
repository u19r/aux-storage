use tracing::{Level, field::Empty};

use super::observation::observe;

/// Implementation rule: one observation exposes recorded span fields and
/// events.
#[tokio::test]
async fn given_recorded_span_when_operation_is_observed_then_latest_span_fields_are_available() {
    let observed = observe(async {
        let span = tracing::info_span!("provider_read", backend = "sqlite", outcome = Empty);
        span.record("outcome", "busy");
        let _entered = span.enter();
        tracing::info!(operation = "read_sequence.get_item", "provider read failed");
        tracing::warn!(operation = "read_sequence.get_item", "provider read failed");
    })
    .await;

    observed.assert_database_calls_exact(&[]);
    observed.one_span_with(
        "provider_read",
        &[("backend", "sqlite"), ("outcome", "busy")],
    );
    observed.one_event_with(
        Level::WARN,
        &[
            ("operation", "read_sequence.get_item"),
            ("message", "provider read failed"),
        ],
    );
    assert_eq!(observed.events.len(), 2);
}
