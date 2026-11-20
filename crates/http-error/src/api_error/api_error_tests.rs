use axum::http::StatusCode;
use serde_json::json;
use storage_types::{
    AttemptFailure, AttemptPhase, AttributeMap, CancellationReason,
    DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE, DYNAMODB_INTERNAL_SERVER_ERROR_MESSAGE,
    DYNAMODB_REQUEST_LIMIT_EXCEEDED_MESSAGE, DYNAMODB_TRANSACTION_CANCELED_MESSAGE,
    DYNAMODB_TRANSACTION_CONFLICT_MESSAGE, ErrorClass, InfrastructureContext,
    InfrastructureResultKind, NativeUnavailable, StorageEnum, StorageError, StorageValidationKind,
    expired_in,
};

use super::HttpApiError;

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
fn transaction_conflict_uses_constant_wire_message() {
    let storage_err: StorageError = StorageEnum::Attempt(storage_types::AttemptFailure::Conflict(
        storage_types::ConflictCause::Logical("custom conflict message"),
    ))
    .into();
    let handler: HttpApiError = storage_err.into();
    assert_eq!(handler.message, DYNAMODB_TRANSACTION_CONFLICT_MESSAGE);
    assert_eq!(
        handler.error_type,
        "com.amazonaws.dynamodb.v20120810#TransactionConflictException"
    );
}

#[test]
fn transaction_canceled_response_uses_dynamodb_cancellation_shape() {
    let storage_err: StorageError = StorageEnum::TransactionCanceled {
        reasons: vec![
            CancellationReason::none(),
            CancellationReason::validation_error(
                "An operand in the update expression has an incorrect data type",
            ),
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
    let storage_err: StorageError = StorageEnum::TransactionCanceled {
        reasons: vec![CancellationReason::conditional_check_failed(Some(item))],
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
fn admission_rejection_maps_to_503_with_retry_hint() {
    let storage_err = StorageError::service_unavailable(2);
    let handler: HttpApiError = storage_err.into();

    assert_eq!(handler.status_code, 503);
    assert_eq!(
        handler.error_type,
        "com.amazonaws.dynamodb.v20120810#ServiceUnavailable"
    );
    assert_eq!(
        handler
            .response_headers
            .iter()
            .find(|(name, _)| name == "Retry-After")
            .map(|(_, value)| value.as_str()),
        Some("2")
    );

    let (status, axum::response::Json(body)): (StatusCode, _) = handler.into();
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body.retry_after_seconds, Some(2));
}

#[test]
fn storage_validation_adds_dynamodb_prefix_for_update_expression_messages() {
    let storage_err: StorageError = StorageEnum::Validation {
        kind: storage_types::StorageValidationKind::Message,
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

#[test]
fn abandoned_attempt_maps_to_503_with_retry_hint() {
    let handler: HttpApiError = StorageError::attempt_abandoned().into();

    assert_eq!(handler.status_code, 503);
    assert_eq!(
        handler
            .response_headers
            .iter()
            .find(|(name, _)| name == "Retry-After")
            .map(|(_, value)| value.as_str()),
        Some("1")
    );
}

#[test]
fn native_unavailable_maps_to_503_with_retry_hint() {
    let storage_err: StorageError = storage_types::AttemptFailure::Unavailable(
        storage_types::NativeUnavailable::Deadline(storage_types::AttemptPhase::Statement),
    )
    .into();
    let handler: HttpApiError = storage_err.into();

    assert_eq!(handler.status_code, 503);
    assert_eq!(
        handler
            .response_headers
            .iter()
            .find(|(name, _)| name == "Retry-After")
            .map(|(_, value)| value.as_str()),
        Some("1")
    );
}

const DYNAMODB_INTERNAL_TYPE: &str = "com.amazonaws.dynamodb.v20120810#InternalServerError";

fn retry_after(error: &HttpApiError) -> Option<&str> {
    error
        .response_headers
        .iter()
        .find(|(name, _)| name == "Retry-After")
        .map(|(_, value)| value.as_str())
}

/// One value per `StorageEnum` variant (35) and the seven attempt kinds.
fn every_storage_error() -> Vec<StorageError> {
    let variants: Vec<StorageEnum> = vec![
        StorageEnum::Database(std::fmt::Error),
        StorageEnum::AwsSerialization("x".into()),
        StorageEnum::Serialization(
            serde_json::from_str::<u8>("x")
                .expect_err("invalid json")
                .into(),
        ),
        StorageEnum::InfrastructureResult(InfrastructureResultKind::Encoding {
            context: InfrastructureContext::RecordEncoding,
        }),
        StorageEnum::Attempt(AttemptFailure::FRESH_VALIDATION),
        StorageEnum::ResourceNotFound {
            resource_type: "cursor",
            resource_id: "x".into(),
        },
        StorageEnum::ResourceExists {
            resource_type: "cursor",
            resource_id: "x".into(),
        },
        StorageEnum::IndexNotFound {
            index_name: "i".into(),
            table_name: "t".into(),
        },
        StorageEnum::KeyValidation(
            storage_types::TableKey::from_key_schema(
                storage_types::TableName::new("t"),
                &[],
                &AttributeMap::new(),
            )
            .expect_err("missing hash key schema"),
        ),
        StorageEnum::InternalServerError {
            message: "x".into(),
        },
        StorageEnum::HistoryUnavailable,
        StorageEnum::HistoryExpired,
        StorageEnum::TrimmedDataAccess,
        StorageEnum::ConditionalCheckFailed,
        StorageEnum::ConditionalCheckFailedWithItem {
            item: AttributeMap::new(),
        },
        StorageEnum::GuardConflict {
            message: "x".into(),
        },
        StorageEnum::Unsupported {
            message: "x".into(),
        },
        StorageEnum::TableAlreadyExists { name: "t".into() },
        StorageEnum::TableNotFound {
            name: "t".into(),
            message: "x".into(),
        },
        StorageEnum::Validation {
            kind: StorageValidationKind::Message,
            message: "x".into(),
        },
        StorageEnum::RawValidation {
            message: "x".into(),
        },
        StorageEnum::DeletionProtectionEnabled {
            table_name: "t".into(),
            message: "x".into(),
        },
        StorageEnum::TransactionInProgress {
            message: "x".into(),
        },
        StorageEnum::IdempotentParameterMismatch,
        StorageEnum::IdempotencyReplay { response: vec![1] },
        StorageEnum::TransactionCanceled {
            reasons: vec![CancellationReason::none()],
        },
        StorageEnum::ProvisionedThroughputExceeded {
            message: "x".into(),
        },
        StorageEnum::Throttled {
            message: "x".into(),
        },
        StorageEnum::ServiceUnavailable {
            message: "x".into(),
            retry_after_seconds: 3,
        },
        StorageEnum::LimitExceeded {
            message: "x".into(),
        },
        StorageEnum::Authentication {
            message: "x".into(),
        },
        StorageEnum::AccessDenied {
            message: "x".into(),
        },
        StorageEnum::RequestLimitExceeded,
        StorageEnum::MissingAuthenticationToken,
        StorageEnum::AwsService {
            code: None,
            message: "x".into(),
        },
    ];
    let mut errors: Vec<StorageError> = variants.into_iter().map(StorageError::from).collect();
    errors.extend([
        StorageError::logical_conflict("test conflict"),
        StorageError::from(AttemptFailure::CAPACITY),
        StorageError::from(AttemptFailure::TRANSACTION_BYTES),
        StorageError::from(AttemptFailure::Unavailable(
            NativeUnavailable::ConnectionLost,
        )),
        expired_in(AttemptPhase::Statement),
        StorageError::commit_unknown("lost"),
    ]);
    errors
}

#[test]
fn given_every_storage_variant_when_mapped_to_dynamodb_then_status_follows_class() {
    let errors = every_storage_error();
    assert_eq!(
        errors.len(),
        35 + 6,
        "one value per variant and attempt kind"
    );
    for error in errors {
        let class = error.class();
        let label = error.label();
        let is_exception = matches!(
            error.as_ref(),
            StorageEnum::Unsupported { .. } | StorageEnum::AwsService { .. }
        );
        let http = HttpApiError::from(error);
        match class {
            ErrorClass::CommitUnknown | ErrorClass::Capacity => {
                assert_eq!(http.status_code, 500, "{label}");
                assert_eq!(http.error_type, DYNAMODB_INTERNAL_TYPE, "{label}");
                assert_eq!(
                    http.message, DYNAMODB_INTERNAL_SERVER_ERROR_MESSAGE,
                    "{label}"
                );
            }
            ErrorClass::Retryable => {
                assert!(
                    matches!(http.status_code, 400 | 429 | 503),
                    "{label}: {}",
                    http.status_code
                );
            }
            ErrorClass::Client => {
                assert!(
                    (400..=499).contains(&http.status_code),
                    "{label}: {}",
                    http.status_code
                );
            }
            ErrorClass::Internal => {
                assert_eq!(http.status_code, 500, "{label}");
                if !is_exception {
                    assert_eq!(http.error_type, DYNAMODB_INTERNAL_TYPE, "{label}");
                    assert_eq!(
                        http.message, DYNAMODB_INTERNAL_SERVER_ERROR_MESSAGE,
                        "{label}"
                    );
                }
            }
        }
    }
}

#[test]
fn given_fresh_validation_when_mapped_to_dynamodb_then_503_retry_after_one() {
    let http = HttpApiError::from(StorageError::from(AttemptFailure::FRESH_VALIDATION));

    assert_eq!(http.status_code, 503);
    assert_eq!(retry_after(&http), Some("1"));
}

#[test]
fn given_serialization_or_guard_conflict_when_mapped_to_dynamodb_then_constant_internal_text() {
    let errors: [StorageEnum; 4] = [
        StorageEnum::GuardConflict {
            message: "secret".into(),
        },
        StorageEnum::AwsSerialization("secret".into()),
        StorageEnum::Database(std::fmt::Error),
        StorageEnum::IdempotencyReplay { response: vec![1] },
    ];
    for error in errors {
        let http = HttpApiError::from(StorageError::from(error));

        assert_eq!(http.status_code, 500);
        assert_eq!(http.error_type, DYNAMODB_INTERNAL_TYPE);
        assert_eq!(http.message, DYNAMODB_INTERNAL_SERVER_ERROR_MESSAGE);
        assert!(!http.message.contains("secret"));
    }
}

#[test]
fn given_typed_reasons_when_serialized_then_aws_json_bytes() {
    let storage_err: StorageError = StorageEnum::TransactionCanceled {
        reasons: vec![
            CancellationReason::none(),
            CancellationReason::conditional_check_failed(None),
        ],
    }
    .into();
    let handler: HttpApiError = storage_err.into();

    let (_, axum::response::Json(body)): (StatusCode, _) = handler.into();

    assert_eq!(
        serde_json::to_string(&body).expect("serialize body"),
        concat!(
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#TransactionCanceledException","#,
            r#""CancellationReasons":[{"Code":"None"},"#,
            r#"{"Code":"ConditionalCheckFailed","Message":"The conditional request failed"}],"#,
            r#""Message":"Transaction cancelled, please refer cancellation reasons for "#,
            r#"specific reasons [None, ConditionalCheckFailed]"}"#
        )
    );
}

#[test]
fn given_no_reasons_when_mapped_then_constant_message() {
    let storage_err: StorageError = StorageEnum::TransactionCanceled { reasons: vec![] }.into();
    let handler: HttpApiError = storage_err.into();

    let (_, axum::response::Json(body)): (StatusCode, _) = handler.into();

    assert_eq!(
        body.transaction_message.as_deref(),
        Some(DYNAMODB_TRANSACTION_CANCELED_MESSAGE)
    );
    assert!(
        body.cancellation_reasons
            .as_deref()
            .is_some_and(<[_]>::is_empty)
    );
}
