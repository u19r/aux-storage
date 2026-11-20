use http_error::{AWS_QUERY_SERVICE_UNAVAILABLE_MESSAGE, HttpApiError};

use crate::{
    QueueError, QueueInternalKind, QueueValidationKind, SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE,
    SQS_NON_EXISTENT_QUEUE_ERROR_TYPE,
    constants::{
        SQS_INTERNAL_ERROR_MESSAGE, SQS_INTERNAL_ERROR_TYPE, SQS_QUEUE_ALREADY_EXISTS_ERROR_TYPE,
        SQS_RECEIPT_HANDLE_INVALID_ERROR_TYPE, SQS_RECEIPT_HANDLE_INVALID_MESSAGE,
    },
};

#[test]
fn queue_validation_kinds_map_to_expected_aws_query_errors() {
    let cases = [
        (
            QueueValidationKind::InvalidQueueUrlFormat,
            SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE,
            "The queue URL is not valid for this endpoint.",
        ),
        (
            QueueValidationKind::MessageNotFoundOrAlreadyProcessed,
            SQS_RECEIPT_HANDLE_INVALID_ERROR_TYPE,
            SQS_RECEIPT_HANDLE_INVALID_MESSAGE,
        ),
        (
            QueueValidationKind::MessageNotFound,
            SQS_RECEIPT_HANDLE_INVALID_ERROR_TYPE,
            SQS_RECEIPT_HANDLE_INVALID_MESSAGE,
        ),
        (
            QueueValidationKind::CannotOperateVisibleMessage,
            SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE,
            "The message is not in flight.",
        ),
    ];

    for (kind, expected_type, expected_message) in cases {
        assert_eq!(kind.aws_query_error_type(), expected_type);
        assert_eq!(kind.aws_query_message(None), expected_message);
    }
}

#[test]
fn queue_errors_map_to_expected_aws_query_shapes() {
    let cases = [
        (
            QueueError::queue_already_exists("orders"),
            SQS_QUEUE_ALREADY_EXISTS_ERROR_TYPE,
            "Requested resource already exists: queue: orders already exists",
            400,
        ),
        (
            QueueError::ResourceNotFound {
                resource_type: "queue",
                resource_id: "orders".to_string(),
            },
            SQS_NON_EXISTENT_QUEUE_ERROR_TYPE,
            "The specified queue does not exist.",
            400,
        ),
        (
            QueueError::ResourceNotFound {
                resource_type: "receipt_handle",
                resource_id: "stale".to_string(),
            },
            SQS_RECEIPT_HANDLE_INVALID_ERROR_TYPE,
            "The input receipt handle \"stale\" is not a valid receipt handle.",
            404,
        ),
        (
            QueueError::internal(QueueInternalKind::SQLiteBackendDisabled),
            SQS_INTERNAL_ERROR_TYPE,
            "Internal server error: sqlite_backend_disabled",
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
fn queue_error_maps_receipt_handle_validation_to_shared_http_error() {
    let error = QueueError::validation_with_detail(
        QueueValidationKind::MessageNotFound,
        "receipt handle expired",
    );

    let http_error = HttpApiError::from(error);

    assert_eq!(http_error.error_type, "ReceiptHandleIsInvalid");
    assert_eq!(http_error.message, "receipt handle expired");
    assert_eq!(http_error.status_code, 404);
}

#[test]
fn native_unavailable_maps_to_retryable_503() {
    let error = QueueError::from(storage_types::StorageError::from(
        storage_types::AttemptFailure::Unavailable(
            storage_types::NativeUnavailable::ConnectionLost,
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

/// W2 D5: a native rejection proves no commit, so retry exhaustion is a
/// retryable 503 `ServiceUnavailable`; an unknown commit outcome stays 500.
#[test]
fn given_native_rejection_or_unknown_outcome_when_mapped_then_503_or_500() {
    let conflict = QueueError::from(storage_types::StorageError::from(
        storage_types::StorageEnum::Attempt(storage_types::AttemptFailure::Conflict(
            storage_types::ConflictCause::Logical("retries exhausted"),
        )),
    ));
    assert!(
        conflict
            .storage_answer()
            .is_some_and(|a| a.status_code == 503)
    );
    assert_eq!(conflict.aws_query_status_code(), 503);
    assert_eq!(conflict.aws_query_error_type(), "ServiceUnavailable");
    assert!(!conflict.is_sender_fault());
    assert_eq!(
        crate::sqs_json_error_type(conflict.aws_query_error_type()),
        "com.amazon.coral.service#ServiceUnavailableException"
    );
    let http = HttpApiError::from(conflict);
    assert_eq!(
        (http.status_code, http.error_type.as_str()),
        (503, "ServiceUnavailable")
    );

    let unknown = QueueError::from(storage_types::StorageError::commit_unknown(
        "commit outcome unknown",
    ));
    assert!(
        unknown
            .storage_answer()
            .is_some_and(|a| a.status_code == 500)
    );
    assert_eq!(unknown.aws_query_status_code(), 500);
    assert_eq!(HttpApiError::from(unknown).status_code, 500);
}

#[test]
fn given_internal_storage_error_when_mapped_then_sqs_internal_constant() {
    let error = QueueError::from(storage_types::StorageError::internal("detail"));
    assert_eq!(error.aws_query_error_type(), SQS_INTERNAL_ERROR_TYPE);
    assert_eq!(error.aws_query_status_code(), 500);
    assert_eq!(error.aws_query_message(), SQS_INTERNAL_ERROR_MESSAGE);
}

#[test]
fn given_commit_unknown_when_mapped_then_not_native_rejection() {
    let error = QueueError::from(storage_types::StorageError::commit_unknown("lost"));
    assert!(error.storage_answer().is_some_and(|a| a.status_code == 500));
    assert_eq!(error.aws_query_status_code(), 500);
}

#[test]
fn given_write_miss_after_retries_when_mapped_then_503() {
    let error = QueueError::from(storage_types::StorageError::write_miss(
        "queue_catalog",
        storage_types::MissExpectation::Revision,
    ));
    assert!(error.storage_answer().is_some_and(|a| a.status_code == 503));
    assert_eq!(error.aws_query_status_code(), 503);
}

#[test]
fn given_admission_refusal_when_mapped_then_batch_fields_and_http_agree() {
    let error = QueueError::StorageError(storage_types::StorageError::service_unavailable(7));

    assert_eq!(error.aws_query_error_type(), "ServiceUnavailable");
    assert_eq!(error.aws_query_status_code(), 503);
    assert_eq!(
        error.aws_query_message(),
        AWS_QUERY_SERVICE_UNAVAILABLE_MESSAGE
    );
    assert!(!error.is_sender_fault());

    let http = HttpApiError::from(error);

    assert_eq!(http.status_code, 503);
    assert!(
        http.response_headers
            .iter()
            .any(|(name, value)| name == "Retry-After" && value == "7"),
        "{:?}",
        http.response_headers
    );
}

#[test]
fn given_throttled_storage_when_mapped_then_503() {
    let error = QueueError::StorageError(
        storage_types::StorageEnum::Throttled {
            message: "x".into(),
        }
        .into(),
    );

    assert_eq!(error.aws_query_status_code(), 503);
    assert_eq!(error.aws_query_error_type(), "ServiceUnavailable");
    assert_eq!(
        crate::sqs_json_error_type(error.aws_query_error_type()),
        "com.amazon.coral.service#ServiceUnavailableException"
    );
}

#[test]
fn given_unknown_outcome_in_transact_write_when_mapped_then_500_internal() {
    let error = QueueError::TransactWrite(storage_types::StorageError::commit_unknown("lost"));

    let http = HttpApiError::from(error);

    assert_eq!(http.status_code, 500);
    assert_eq!(http.error_type, SQS_INTERNAL_ERROR_TYPE);
    assert_eq!(http.message, SQS_INTERNAL_ERROR_MESSAGE);
    assert!(
        !http
            .response_headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("retry-after")),
        "{:?}",
        http.response_headers
    );
}
