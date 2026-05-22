use http_error::HttpApiError;

use crate::{
    PubsubError, PubsubValidationKind,
    errors::{SNS_INTERNAL_ERROR_TYPE, SNS_INVALID_PARAMETER_ERROR_TYPE, SNS_NOT_FOUND_ERROR_TYPE},
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
            400,
        ),
        (
            PubsubError::subscription_not_found(
                "arn:aws:sns:us-east-1:000000000000:orders:missing",
            ),
            SNS_NOT_FOUND_ERROR_TYPE,
            "Subscription does not exist",
            400,
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
    assert_eq!(http_error.status_code, 400);
}
