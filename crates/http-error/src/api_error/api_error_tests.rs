use axum::http::StatusCode;
use serde_json::json;
use storage_types::{
    AttributeMap, DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE,
    DYNAMODB_REQUEST_LIMIT_EXCEEDED_MESSAGE, StorageEnum, StorageError,
};

use super::{ErrorEnvelope, HttpApiError, IntoApiError};

#[test]
fn conditional_check_failed_http_message_contains_canonical_text() {
    let storage_err: StorageError = StorageEnum::ConditionalCheckFailed.into();
    let handler: HttpApiError = storage_err.into();
    assert_eq!(handler.message, DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE);
    assert_eq!(
        handler.error_type,
        "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException"
    );
}

#[test]
fn conditional_check_failed_response_can_include_all_old_item() {
    let item: AttributeMap = serde_json::from_value(json!({
        "pk": {"S": "p"},
        "note": {"S": "old"}
    }))
    .expect("attribute map");
    let storage_err: StorageError =
        StorageEnum::ConditionalCheckFailedWithItem { item: item.clone() }.into();
    let handler: HttpApiError = storage_err.into();

    let (status, axum::response::Json(body)): (StatusCode, _) = handler.into();

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body.error_type,
        "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException"
    );
    assert_eq!(body.message, DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE);
    assert_eq!(body.item, Some(item));
}

#[test]
fn transaction_conflict_passes_through_storage_message() {
    let storage_err: StorageError = StorageEnum::TransactionConflict {
        message: "custom conflict message".to_string(),
    }
    .into();
    let handler: HttpApiError = storage_err.into();
    assert_eq!(handler.message, "custom conflict message");
    assert_eq!(
        handler.error_type,
        "com.amazonaws.dynamodb.v20120810#TransactionConflictException"
    );
}

#[test]
fn transaction_canceled_response_uses_dynamodb_cancellation_shape() {
    let storage_err: StorageError = StorageEnum::TransactionCanceled {
        reasons: vec![
            "None".to_string(),
            "ValidationError\tAn operand in the update expression has an incorrect data type"
                .to_string(),
        ],
    }
    .into();
    let handler: HttpApiError = storage_err.into();

    let (status, axum::response::Json(body)): (StatusCode, _) = handler.into();

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body.error_type,
        "com.amazonaws.dynamodb.v20120810#TransactionCanceledException"
    );
    assert_eq!(body.message, "");
    assert_eq!(
        body.transaction_message.as_deref(),
        Some(
            "Transaction cancelled, please refer cancellation reasons for specific reasons [None, \
             ValidationError]"
        )
    );
    let reasons = body
        .cancellation_reasons
        .as_ref()
        .expect("cancellation reasons");
    assert_eq!(reasons.len(), 2);
    assert_eq!(reasons[0].code, "None");
    assert_eq!(reasons[0].message, None);
    assert_eq!(reasons[1].code, "ValidationError");
    assert_eq!(
        reasons[1].message.as_deref(),
        Some("An operand in the update expression has an incorrect data type")
    );
}

#[test]
fn transaction_canceled_response_can_include_all_old_item_in_reason() {
    let item: AttributeMap = serde_json::from_value(json!({
        "pk": {"S": "p"},
        "sk": {"S": "s"},
        "note": {"S": "old"}
    }))
    .expect("attribute map");
    let item_json = serde_json::to_string(&item).expect("serialize item");
    let storage_err: StorageError = StorageEnum::TransactionCanceled {
        reasons: vec![format!(
            "ConditionalCheckFailed\t{DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE}\t{item_json}"
        )],
    }
    .into();
    let handler: HttpApiError = storage_err.into();

    let (status, axum::response::Json(body)): (StatusCode, _) = handler.into();

    assert_eq!(status, StatusCode::BAD_REQUEST);
    let reasons = body
        .cancellation_reasons
        .as_ref()
        .expect("cancellation reasons");
    assert_eq!(reasons[0].code, "ConditionalCheckFailed");
    assert_eq!(
        reasons[0].message.as_deref(),
        Some(DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE)
    );
    let returned_item = reasons[0].item.as_ref().expect("old item");
    assert_eq!(
        returned_item.get("note"),
        Some(&storage_types::AttributeValue::S("old".to_string()))
    );
}

#[test]
fn error_response_uses_aws_compatible_type_and_message_fields() {
    let error = HttpApiError::validation_error("bad input");

    let response = error.error_response();

    assert_eq!(response.error_type, "ValidationException");
    assert_eq!(response.message, "bad input");
    assert_eq!(response.request_id, None);
}

#[test]
fn error_envelope_builder_preserves_optional_field_path() {
    let envelope = ErrorEnvelope::new("invalid_request", "Invalid request")
        .with_field_path(Some("queueName"), Some("body.queueName"));

    assert_eq!(envelope.code, "invalid_request");
    assert_eq!(envelope.message, "Invalid request");
    assert_eq!(envelope.field.as_deref(), Some("queueName"));
    assert_eq!(envelope.path.as_deref(), Some("body.queueName"));
}

#[test]
fn storage_throttle_and_auth_errors_map_to_aws_error_types() {
    let request_limit: HttpApiError =
        Into::<StorageError>::into(StorageEnum::RequestLimitExceeded).into();
    let auth: HttpApiError = Into::<StorageError>::into(StorageEnum::Authentication {
        message: "bad token".to_string(),
    })
    .into();
    let denied: HttpApiError = Into::<StorageError>::into(StorageEnum::AccessDenied {
        message: "denied".to_string(),
    })
    .into();

    assert_eq!(
        request_limit.error_type,
        "com.amazonaws.dynamodb.v20120810#RequestLimitExceeded"
    );
    assert_eq!(
        request_limit.message,
        DYNAMODB_REQUEST_LIMIT_EXCEEDED_MESSAGE
    );
    assert_eq!(request_limit.status_code, 400);
    assert_eq!(auth.error_type, "UnrecognizedClientException");
    assert_eq!(auth.status_code, 401);
    assert_eq!(denied.error_type, "AccessDeniedException");
    assert_eq!(denied.status_code, 403);
}

#[test]
fn storage_validation_adds_dynamodb_prefix_for_update_expression_messages() {
    let storage_err: StorageError = StorageEnum::Validation {
        message: "Invalid UpdateExpression: Syntax error; token: \"<EOF>\", near: \"SET\""
            .to_string(),
    }
    .into();

    let handler: HttpApiError = storage_err.into();

    assert_eq!(
        handler.error_type,
        "com.amazon.coral.validate#ValidationException"
    );
    assert_eq!(
        handler.message,
        "1 validation error detected: Invalid UpdateExpression: Syntax error; token: \"<EOF>\", \
         near: \"SET\""
    );
}

#[test]
fn http_api_error_converts_unknown_status_to_internal_server_error_response() {
    let error = HttpApiError::dynamodb_error("CustomException", "custom", 418);

    let (status, axum::response::Json(body)): (StatusCode, _) = error.into();

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body.error_type, "CustomException");
    assert_eq!(body.message, "custom");
}
