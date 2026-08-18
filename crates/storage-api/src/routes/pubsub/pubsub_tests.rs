use http_error::HttpApiError;

use crate::routes::pubsub::is_provider_pressure_error;

#[test]
fn given_provider_pressure_errors_when_classifying_pubsub_responses_then_retries_are_signalled() {
    for (error_type, status_code) in [
        ("RequestTimeout", 504),
        ("RequestTimeoutException", 500),
        ("com.amazonaws.dynamodb.v20120810#RequestTimeout", 504),
        ("ThrottlingException", 429),
        ("com.amazonaws.dynamodb.v20120810#ThrottlingException", 429),
        ("ServiceUnavailableException", 503),
        (
            "com.amazonaws.dynamodb.v20120810#ServiceUnavailableException",
            503,
        ),
    ] {
        let error = HttpApiError::aws_query_error(error_type, "provider pressure", status_code);
        assert!(
            is_provider_pressure_error(&error),
            "{error_type} should be classified as provider pressure"
        );
    }
}

#[test]
fn given_client_errors_when_classifying_pubsub_responses_then_retries_are_not_signalled() {
    for (error_type, status_code) in [
        ("InvalidParameter", 400),
        ("NotFound", 404),
        ("AuthorizationError", 403),
        ("InternalError", 500),
    ] {
        let error = HttpApiError::aws_query_error(error_type, "client error", status_code);
        assert!(
            !is_provider_pressure_error(&error),
            "{error_type} should not be classified as provider pressure"
        );
    }
}
