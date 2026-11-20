use regional_replication::message::{RegionalRejection, RegionalResponse};
use storage_observability_test_support::observation::observe;
use storage_types::{AttemptPhase, StorageError, expired_in};
use tracing::Level;

use super::service::*;

#[tokio::test]
async fn given_data_step_internal_error_when_rejected_then_line_omits_message() {
    let internal = StorageError::internal("secret detail");

    let observation = observe(async { unavailable("receive_page", internal) }).await;

    assert!(matches!(
        observation.result,
        RegionalResponse::Rejected(RegionalRejection::Unavailable)
    ));
    observation.assert_only_event_exact(
        Level::WARN,
        &[
            ("step", "receive_page"),
            ("class", "internal"),
            ("kind", "internal_server_error"),
            ("message", "regional request failed"),
        ],
    );
}

#[tokio::test]
async fn given_data_step_attempt_failure_when_rejected_then_line_prints_failure() {
    let deadline = expired_in(AttemptPhase::Statement);
    let failure = deadline
        .attempt_failure()
        .expect("deadline carries an attempt failure")
        .to_string();

    let observation = observe(async { unavailable("apply_scan_page", deadline) }).await;

    assert!(matches!(
        observation.result,
        RegionalResponse::Rejected(RegionalRejection::Unavailable)
    ));
    observation.assert_only_event_exact(
        Level::WARN,
        &[
            ("step", "apply_scan_page"),
            ("class", "retryable"),
            ("failure", failure.as_str()),
            ("message", "regional request failed"),
        ],
    );
}

#[tokio::test]
async fn given_catalog_step_internal_error_when_rejected_then_line_prints_cause() {
    let error = StorageError::internal("catalog row missing");

    let observation = observe(async { unavailable("prepare_target", error) }).await;

    assert!(matches!(
        observation.result,
        RegionalResponse::Rejected(RegionalRejection::Unavailable)
    ));
    observation.assert_only_event_exact(
        Level::WARN,
        &[
            ("step", "prepare_target"),
            ("class", "internal"),
            ("error", "internal error: catalog row missing"),
            ("message", "regional request failed"),
        ],
    );
}
