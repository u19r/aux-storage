//! The AWS query-protocol (SQS, SNS) answer to a storage error.

use storage_types::{ErrorClass, StorageEnum, StorageError, context::WrappedError as _};

use super::model::HttpApiError;

pub const AWS_QUERY_SERVICE_UNAVAILABLE_ERROR_TYPE: &str = "ServiceUnavailable";
pub const AWS_QUERY_SERVICE_UNAVAILABLE_MESSAGE: &str =
    "The request has failed due to a temporary failure of the server.";

/// A service's 500 answer to a storage failure: its own error type and fixed
/// message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AwsQueryInternal {
    pub error_type: &'static str,
    pub message: &'static str,
}

/// What an SQS or SNS response says for a storage error. Chosen by the class
/// only: the wire never carries the storage error's Display.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AwsQueryAnswer {
    pub error_type: &'static str,
    pub message: &'static str,
    pub status_code: u16,
    /// `Some` exactly when `status_code` is 503.
    pub retry_after_seconds: Option<u64>,
}

impl AwsQueryAnswer {
    /// `Retryable` and `Capacity` are known not committed: 503 with
    /// Retry-After (the admission hint, else one second). A request of these
    /// protocols is bounded by AWS limits far below a native transaction limit,
    /// so a capacity refusal is load and a repeat can succeed. `CommitUnknown`,
    /// `Internal`, and a `Client` error that reached the storage arm: 500 with
    /// the service's text; a 500 never says that nothing committed.
    #[must_use]
    pub fn for_storage(error: &StorageError, internal: AwsQueryInternal) -> Self {
        match error.class() {
            ErrorClass::Retryable | ErrorClass::Capacity => Self {
                error_type: AWS_QUERY_SERVICE_UNAVAILABLE_ERROR_TYPE,
                message: AWS_QUERY_SERVICE_UNAVAILABLE_MESSAGE,
                status_code: 503,
                retry_after_seconds: Some(match error.to_enum() {
                    StorageEnum::ServiceUnavailable {
                        retry_after_seconds,
                        ..
                    } => (*retry_after_seconds).max(1),
                    _ => 1,
                }),
            },
            ErrorClass::CommitUnknown | ErrorClass::Client | ErrorClass::Internal => Self {
                error_type: internal.error_type,
                message: internal.message,
                status_code: 500,
                retry_after_seconds: None,
            },
        }
    }

    #[must_use]
    pub fn into_http(self) -> HttpApiError {
        let error = HttpApiError::aws_query_error(self.error_type, self.message, self.status_code);
        match self.retry_after_seconds {
            Some(seconds) => error.with_response_header("Retry-After", seconds.to_string()),
            None => error,
        }
    }
}
