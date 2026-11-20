use storage_observability_test_support::observation::observe;
use storage_types::StorageError;
use tracing::Level;

use super::errors::*;

#[tokio::test]
async fn given_internal_failure_when_logged_then_warn_with_class_and_kind_only() {
    let internal = StorageError::internal("secret detail");

    let observation = observe(async { log_storage_failure("dynamodb", &internal) }).await;

    observation.assert_only_event_exact(
        Level::WARN,
        &[
            ("service", "dynamodb"),
            ("class", "internal"),
            ("kind", "internal_server_error"),
            ("message", "storage operation failed"),
        ],
    );
}

#[tokio::test]
async fn given_commit_unknown_when_logged_then_warn_with_failure() {
    let unknown = StorageError::commit_unknown("lost");

    let observation = observe(async { log_storage_failure("dynamodb", &unknown) }).await;

    observation.assert_only_event_exact(
        Level::WARN,
        &[
            ("service", "dynamodb"),
            ("class", "commit_unknown"),
            ("kind", "commit_unknown"),
            ("failure", "commit outcome unknown: lost"),
            ("message", "storage operation failed"),
        ],
    );
}

#[tokio::test]
async fn given_admission_refusal_when_logged_then_debug() {
    let admission = StorageError::service_unavailable(7);

    let observation = observe(async { log_storage_failure("sqs", &admission) }).await;

    assert_eq!(observation.events.len(), 1, "{:?}", observation.events);
    assert_eq!(observation.events[0].level, Level::DEBUG);
    assert_eq!(
        observation.events[0]
            .fields
            .get("class")
            .map(String::as_str),
        Some("retryable")
    );
}

#[tokio::test]
async fn given_client_error_when_logged_then_no_event() {
    let client = StorageError::table_not_found("t");

    let observation = observe(async { log_storage_failure("sns", &client) }).await;

    assert!(observation.events.is_empty(), "{:?}", observation.events);
}
