use http_error::HttpApiError;

use crate::{
    QueueError, QueueInternalKind, QueueValidationKind, SQS_INTERNAL_ERROR_TYPE,
    SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE, SQS_NON_EXISTENT_QUEUE_ERROR_TYPE,
    SQS_QUEUE_NAME_EXISTS_ERROR_TYPE, SQS_RECEIPT_HANDLE_INVALID_ERROR_TYPE,
    constants::SQS_RECEIPT_HANDLE_INVALID_MESSAGE,
};

#[test]
fn queue_validation_kinds_map_to_expected_aws_query_errors() {
    let cases = [
        (
            QueueValidationKind::InvalidQueueUrlFormat,
            SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE,
            "validation_error",
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
            "validation_error",
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
            SQS_QUEUE_NAME_EXISTS_ERROR_TYPE,
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
