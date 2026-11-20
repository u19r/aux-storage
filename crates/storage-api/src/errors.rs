use axum::{
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::Json,
};
use http_error::{ErrorResponse, HttpApiError};
use storage_types::{ErrorClass, StorageError};

pub type StorageApiError = (StatusCode, Json<ErrorResponse>);

pub(crate) fn http_api_error_headers(error: &HttpApiError) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in &error.response_headers {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            headers.insert(name, value);
        }
    }
    headers
}

pub fn validation_error(message: impl Into<String>) -> StorageApiError {
    HttpApiError::dynamodb_protocol_error("ValidationException", message, 400).into()
}

/// Logs a storage failure and converts it to the DynamoDB wire error.
pub(crate) fn dynamodb_storage_error(error: StorageError) -> HttpApiError {
    log_storage_failure("dynamodb", &error);
    HttpApiError::from(error)
}

/// One log line for a storage failure that a wire mapping answers. The request
/// may carry item data, so the line prints the class, the label and the attempt
/// failure, never the error's Display or Debug. A client-class error is the
/// caller's and is not logged; a retryable one (admission refusal, conflict
/// after retries, deadline) logs at DEBUG so an overload does not flood the
/// log; capacity, unknown outcome and internal failures log at WARN.
pub(crate) fn log_storage_failure(service: &'static str, error: &StorageError) {
    let class = error.class();
    let kind = error.label();
    match (class, error.attempt_failure()) {
        (ErrorClass::Client, _) => {}
        (ErrorClass::Retryable, Some(failure)) => {
            tracing::debug!(service, class = class.as_str(), kind, %failure, "storage operation failed");
        }
        (ErrorClass::Retryable, None) => {
            tracing::debug!(
                service,
                class = class.as_str(),
                kind,
                "storage operation failed"
            );
        }
        (
            ErrorClass::Capacity | ErrorClass::CommitUnknown | ErrorClass::Internal,
            Some(failure),
        ) => {
            tracing::warn!(service, class = class.as_str(), kind, %failure, "storage operation failed");
        }
        (ErrorClass::Capacity | ErrorClass::CommitUnknown | ErrorClass::Internal, None) => {
            tracing::warn!(
                service,
                class = class.as_str(),
                kind,
                "storage operation failed"
            );
        }
    }
}
