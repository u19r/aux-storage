use http_error::{AWS_QUERY_SERVICE_UNAVAILABLE_MESSAGE, HttpApiError};

use crate::{
    PubsubError, PubsubValidationKind,
    constants::{
        SNS_INTERNAL_ERROR_TYPE, SNS_INVALID_PARAMETER_ERROR_TYPE, SNS_NOT_FOUND_ERROR_TYPE,
    },
};

#[test]
fn pubsub_validation_kinds_map_to_expected_aws_query_errors() {
    let cases = [
        (
            PubsubValidationKind::InvalidTopicName,
            None,
            "Invalid parameter: Topic Name",
        ),
        (
            PubsubValidationKind::InvalidTopicArn,
            None,
            "Invalid parameter: TopicArn Reason: An ARN must have at least 6 elements, not 1",
        ),
        (
            PubsubValidationKind::InvalidSubscriptionArn,
            None,
            "Invalid parameter: SubscriptionArn Reason: An ARN must have at least 6 elements, not \
             1",
        ),
        (
            PubsubValidationKind::InvalidEndpoint,
            None,
            "Invalid parameter: Endpoint",
        ),
        (
            PubsubValidationKind::UnsupportedProtocol,
            Some("ftp"),
            "Invalid parameter: Amazon SNS does not support this protocol string: ftp",
        ),
        (
            PubsubValidationKind::UnsupportedAttribute,
            Some("Invalid"),
            "Invalid parameter: AttributeName",
        ),
        (
            PubsubValidationKind::EmptyMessage,
            None,
            "Invalid parameter: Empty message",
        ),
        (PubsubValidationKind::InvalidToken, None, "Invalid token"),
    ];

    for (kind, detail, expected_message) in cases {
        assert_eq!(
            kind.aws_query_error_type(),
            SNS_INVALID_PARAMETER_ERROR_TYPE
        );
        assert_eq!(kind.aws_query_message(detail), expected_message);
    }
}

#[test]
fn pubsub_errors_map_to_expected_aws_query_shapes() {
    let cases = [
        (
            PubsubError::topic_not_found("arn:aws:sns:us-east-1:000000000000:missing"),
            SNS_NOT_FOUND_ERROR_TYPE,
            "Topic does not exist",
            404,
        ),
        (
            PubsubError::subscription_not_found(
                "arn:aws:sns:us-east-1:000000000000:orders:missing",
            ),
            SNS_NOT_FOUND_ERROR_TYPE,
            "Subscription does not exist",
            404,
        ),
        (
            PubsubError::storage("database unavailable"),
            SNS_INTERNAL_ERROR_TYPE,
            "Internal server error: storage",
            500,
        ),
    ];

    for (error, expected_type, expected_message, expected_status) in cases {
        assert_eq!(error.aws_query_error_type(), expected_type);
        assert_eq!(error.aws_query_message(), expected_message);
        assert_eq!(error.aws_query_status_code(), expected_status);
    }
}

#[test]
fn pubsub_error_maps_topic_not_found_to_shared_http_error() {
    let error = PubsubError::topic_not_found("arn:aws:sns:us-east-1:000000000000:missing");

    let http_error = HttpApiError::from(error);

    assert_eq!(http_error.error_type, "NotFound");
    assert_eq!(http_error.message, "Topic does not exist");
    assert_eq!(http_error.status_code, 404);
}

#[test]
fn given_unknown_commit_when_pubsub_maps_storage_error_then_outcome_and_http_shape_survive() {
    let error = PubsubError::from(storage_types::StorageError::commit_unknown(
        "native commit outcome unavailable",
    ));
    assert!(matches!(&error, PubsubError::Storage(source)
        if matches!(source.as_ref(), storage_types::StorageEnum::Attempt(storage_types::AttemptFailure::CommitUnknown(_)))));
    assert_eq!(error.aws_query_error_type(), SNS_INTERNAL_ERROR_TYPE);
    assert_eq!(error.aws_query_status_code(), 500);
    assert_eq!(error.aws_query_message(), "Internal server error: storage");
    let http_error = HttpApiError::from(error);
    assert_eq!(http_error.status_code, 500);
    assert_eq!(http_error.message, "Internal server error: storage");
}

#[test]
fn native_unavailable_maps_to_retryable_503() {
    let error = PubsubError::from(storage_types::StorageError::from(
        storage_types::AttemptFailure::Unavailable(
            storage_types::NativeUnavailable::WriterQueueFull,
        ),
    ));
    let http = HttpApiError::from(error);

    assert_eq!(http.status_code, 503);
    assert!(
        http.response_headers
            .iter()
            .any(|(name, value)| name == "Retry-After" && value == "1")
    );
}

#[test]
fn given_storage_internal_when_mapped_then_fixed_wire_and_cause_in_display() {
    let error = PubsubError::from(storage_types::StorageError::internal("detail"));
    assert_eq!(error.aws_query_message(), "Internal server error: storage");
    assert_eq!(error.to_string(), "storage: internal error: detail");
}

#[test]
fn given_storage_deadline_when_mapped_then_503() {
    let error = PubsubError::from(storage_types::expired_in(
        storage_types::AttemptPhase::Statement,
    ));
    assert_eq!(HttpApiError::from(error).status_code, 503);
}

#[test]
fn given_admission_rejection_when_mapped_then_503_with_its_retry_after() {
    let error = PubsubError::Storage(storage_types::StorageError::service_unavailable(7));
    let http = HttpApiError::from(&error);

    assert_eq!(http.status_code, 503);
    assert_eq!(http.error_type, "ServiceUnavailable");
    assert_eq!(http.message, AWS_QUERY_SERVICE_UNAVAILABLE_MESSAGE);
    assert!(
        http.response_headers
            .iter()
            .any(|(name, value)| name == "Retry-After" && value == "7")
    );
}

#[test]
fn given_storage_conflict_or_capacity_when_mapped_then_503_like_sqs() {
    let errors = [
        storage_types::StorageError::logical_conflict("test conflict"),
        storage_types::StorageError::from(storage_types::AttemptFailure::CAPACITY),
    ];
    for storage in errors {
        let error = PubsubError::Storage(storage);
        assert_eq!(error.aws_query_status_code(), 503);
        assert_eq!(error.aws_query_error_type(), "ServiceUnavailable");

        let http = HttpApiError::from(&error);

        assert_eq!(http.status_code, 503);
        assert!(
            http.response_headers
                .iter()
                .any(|(name, value)| name == "Retry-After" && value == "1"),
            "{:?}",
            http.response_headers
        );
    }
}

#[test]
fn given_commit_unknown_when_mapped_then_500_internal_storage() {
    let error = PubsubError::Storage(storage_types::StorageError::commit_unknown("lost"));

    let http = HttpApiError::from(&error);

    assert_eq!(http.status_code, 500);
    assert_eq!(http.error_type, SNS_INTERNAL_ERROR_TYPE);
    assert_eq!(http.message, "Internal server error: storage");
    assert!(
        !http
            .response_headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("Retry-After"))
    );
    assert_eq!(error.aws_query_status_code(), 500);
    assert_eq!(error.aws_query_error_type(), SNS_INTERNAL_ERROR_TYPE);
}
