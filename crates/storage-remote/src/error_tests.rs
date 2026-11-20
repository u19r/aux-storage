use http::StatusCode;
use storage_types::{
    AttemptFailure, AttributeMap, CancellationCode, CancellationReason, ConflictCause,
    DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE, StorageEnum, context::WrappedError as _,
};

use crate::error::{RemoteCancellationReason, RemoteErrorResponse, classify_error_response};

#[test]
fn resource_in_use_exception_maps_to_table_already_exists() {
    let (error, retryable, code) = classify_error_response(
        StatusCode::BAD_REQUEST,
        RemoteErrorResponse {
            error_type: Some("ResourceInUseException".to_string()),
            code: None,
            message: Some("The resource which you are attempting to change is in use.".to_string()),
            item: None,
            cancellation_reasons: None,
        },
    );

    assert!(!retryable);
    assert_eq!(code.as_deref(), Some("ResourceInUseException"));
    assert!(matches!(
        error.to_enum(),
        StorageEnum::TableAlreadyExists { .. }
    ));
}

#[test]
fn resource_in_use_message_does_not_duplicate_table_already_exists_prefix() {
    let (error, _, _) = classify_error_response(
        StatusCode::BAD_REQUEST,
        RemoteErrorResponse {
            error_type: Some("ResourceInUseException".to_string()),
            code: None,
            message: Some("Table already exists: Users".to_string()),
            item: None,
            cancellation_reasons: None,
        },
    );

    assert!(matches!(
        error.to_enum(),
        StorageEnum::TableAlreadyExists { name } if name == "Users"
    ));
}

#[test]
fn conditional_failure_item_is_preserved_from_remote_error() {
    let (error, retryable, code) = classify_error_response(
        StatusCode::BAD_REQUEST,
        RemoteErrorResponse {
            error_type: Some("ConditionalCheckFailedException".to_string()),
            code: None,
            message: Some("The conditional request failed".to_string()),
            item: Some(serde_json::json!({"pk": {"S": "item-1"}})),
            cancellation_reasons: None,
        },
    );

    assert!(!retryable);
    assert_eq!(code.as_deref(), Some("ConditionalCheckFailedException"));
    assert!(matches!(
        error.to_enum(),
        StorageEnum::ConditionalCheckFailedWithItem { item }
            if item.get("pk") == Some(&storage_types::AttributeValue::S("item-1".to_string()))
    ));
}

#[test]
fn idempotent_parameter_mismatch_maps_to_typed_storage_error() {
    let (error, retryable, code) = classify_error_response(
        StatusCode::BAD_REQUEST,
        RemoteErrorResponse {
            error_type: Some(
                "com.amazonaws.dynamodb.v20120810#IdempotentParameterMismatchException".to_string(),
            ),
            code: None,
            message: Some("request token was reused".to_string()),
            item: None,
            cancellation_reasons: None,
        },
    );

    assert!(!retryable);
    assert_eq!(
        code.as_deref(),
        Some("IdempotentParameterMismatchException")
    );
    assert!(matches!(
        error.to_enum(),
        StorageEnum::IdempotentParameterMismatch
    ));
}

#[test]
fn throttling_error_codes_are_retryable() {
    for code in [
        "ProvisionedThroughputExceededException",
        "com.amazonaws.dynamodb.v20120810#ThrottlingException",
        "RequestLimitExceededException",
        "LimitExceededException",
    ] {
        let (error, retryable, normalized) = classify_error_response(
            StatusCode::TOO_MANY_REQUESTS,
            RemoteErrorResponse {
                error_type: Some(code.to_string()),
                code: None,
                message: Some("slow down".to_string()),
                item: None,
                cancellation_reasons: None,
            },
        );

        assert!(retryable, "{code} should be retryable");
        assert!(
            normalized
                .as_deref()
                .is_some_and(|value| !value.contains('#'))
        );
        assert!(matches!(
            error.to_enum(),
            StorageEnum::ProvisionedThroughputExceeded { .. }
                | StorageEnum::Throttled { .. }
                | StorageEnum::RequestLimitExceeded
                | StorageEnum::LimitExceeded { .. }
        ));
    }
}

#[test]
fn table_not_found_messages_map_to_table_not_found_when_table_name_is_present() {
    let (error, retryable, code) = classify_error_response(
        StatusCode::BAD_REQUEST,
        RemoteErrorResponse {
            error_type: Some("ResourceNotFoundException".to_string()),
            code: None,
            message: Some("Requested resource not found: Table: `Users` not found".to_string()),
            item: None,
            cancellation_reasons: None,
        },
    );

    assert!(!retryable);
    assert_eq!(code.as_deref(), Some("ResourceNotFoundException"));
    assert!(matches!(error.to_enum(), StorageEnum::TableNotFound { .. }));
}

#[test]
fn unknown_error_uses_status_code_to_decide_retryability() {
    let (server_error, retryable_server, server_code) = classify_error_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        RemoteErrorResponse {
            error_type: Some("CustomError".to_string()),
            code: None,
            message: Some("failed".to_string()),
            item: None,
            cancellation_reasons: None,
        },
    );
    let (client_error, retryable_client, client_code) = classify_error_response(
        StatusCode::BAD_REQUEST,
        RemoteErrorResponse {
            error_type: None,
            code: None,
            message: Some("bad request".to_string()),
            item: None,
            cancellation_reasons: None,
        },
    );

    assert!(retryable_server);
    assert_eq!(server_code.as_deref(), Some("CustomError"));
    assert!(matches!(
        server_error.to_enum(),
        StorageEnum::AwsService { code: Some(_), .. }
    ));
    assert!(!retryable_client);
    assert!(client_code.is_none());
    assert!(matches!(
        client_error.to_enum(),
        StorageEnum::AwsService { code: None, .. }
    ));
}

#[test]
fn not_leader_exception_is_retryable_for_leader_cache_refresh() {
    let (error, retryable, code) = classify_error_response(
        StatusCode::BAD_REQUEST,
        RemoteErrorResponse {
            error_type: Some("NotLeaderException".to_string()),
            code: None,
            message: Some("retry against the current leader".to_string()),
            item: None,
            cancellation_reasons: None,
        },
    );

    assert!(retryable);
    assert_eq!(code.as_deref(), Some("NotLeaderException"));
    assert!(matches!(
        error.to_enum(),
        StorageEnum::AwsService { code: Some(_), .. }
    ));
}

#[test]
fn transaction_cancellation_preserves_conditional_reason_without_retrying_transport() {
    let (error, retryable, code) = classify_error_response(
        StatusCode::BAD_REQUEST,
        RemoteErrorResponse {
            error_type: Some("TransactionCanceledException".to_string()),
            code: None,
            message: Some("transaction cancelled".to_string()),
            item: None,
            cancellation_reasons: Some(vec![
                RemoteCancellationReason {
                    code: Some("ConditionalCheckFailed".to_string()),
                    message: Some("The conditional request failed.".to_string()),
                    item: None,
                },
                RemoteCancellationReason {
                    code: Some("None".to_string()),
                    message: None,
                    item: None,
                },
            ]),
        },
    );

    assert!(!retryable);
    assert_eq!(code.as_deref(), Some("TransactionCanceledException"));
    assert!(matches!(
        error.to_enum(),
        StorageEnum::TransactionCanceled { reasons }
            if reasons.as_slice() == [
                CancellationReason::from_parts(
                    CancellationCode::ConditionalCheckFailed,
                    Some("The conditional request failed.".into()),
                    None,
                ),
                CancellationReason::none(),
            ]
    ));
}

#[test]
fn transaction_cancellation_is_never_retryable_for_any_reason() {
    for code in [
        "ItemCollectionSizeLimitExceeded",
        "TransactionConflict",
        "ProvisionedThroughputExceeded",
        "ThrottlingError",
        "ValidationError",
    ] {
        let (_, retryable, _) = classify_error_response(
            StatusCode::BAD_REQUEST,
            RemoteErrorResponse {
                error_type: Some("TransactionCanceledException".to_string()),
                code: None,
                message: None,
                item: None,
                cancellation_reasons: Some(vec![RemoteCancellationReason {
                    code: Some(code.to_string()),
                    message: None,
                    item: None,
                }]),
            },
        );
        assert!(!retryable, "{code} cancellation must not be retried");
    }
}

#[test]
fn transaction_cancellation_preserves_reason_message_and_item() {
    let item = serde_json::json!({"pk": {"S": "item-1"}});
    let (error, retryable, _) = classify_error_response(
        StatusCode::BAD_REQUEST,
        RemoteErrorResponse {
            error_type: Some("TransactionCanceledException".to_string()),
            code: None,
            message: None,
            item: None,
            cancellation_reasons: Some(vec![RemoteCancellationReason {
                code: Some("ConditionalCheckFailed".to_string()),
                message: Some("The conditional request failed.".to_string()),
                item: Some(item),
            }]),
        },
    );

    assert!(!retryable);
    let expected_item: AttributeMap =
        serde_json::from_value(serde_json::json!({"pk": {"S": "item-1"}})).unwrap();
    assert!(matches!(
        error.to_enum(),
        StorageEnum::TransactionCanceled { reasons }
            if reasons.as_slice() == [CancellationReason::from_parts(
                CancellationCode::ConditionalCheckFailed,
                Some("The conditional request failed.".into()),
                Some(expected_item),
            )]
    ));
}

#[test]
fn transaction_cancellation_defaults_null_code_to_none() {
    let (error, retryable, _) = classify_error_response(
        StatusCode::BAD_REQUEST,
        RemoteErrorResponse {
            error_type: Some("TransactionCanceledException".to_string()),
            code: None,
            message: None,
            item: None,
            cancellation_reasons: Some(vec![RemoteCancellationReason {
                code: None,
                message: None,
                item: None,
            }]),
        },
    );

    assert!(!retryable);
    assert!(matches!(
        error.to_enum(),
        StorageEnum::TransactionCanceled { reasons }
            if reasons.as_slice() == [CancellationReason::none()]
    ));
}

#[test]
fn given_trimmed_stream_response_when_classified_then_typed_error_is_not_retryable() {
    let (error, retryable, code) = classify_error_response(
        StatusCode::BAD_REQUEST,
        RemoteErrorResponse {
            error_type: Some("com.amazonaws.dynamodb.v20120810#TrimmedDataAccessException".into()),
            code: None,
            message: Some("The data you are trying to access has been trimmed.".into()),
            item: None,
            cancellation_reasons: None,
        },
    );
    assert!(!retryable);
    assert_eq!(code.as_deref(), Some("TrimmedDataAccessException"));
    assert!(matches!(error.to_enum(), StorageEnum::TrimmedDataAccess));
}

#[test]
fn given_remote_transaction_conflict_when_mapped_then_remote_conflict_retryable() {
    let (error, retryable, _) = classify_error_response(
        StatusCode::BAD_REQUEST,
        RemoteErrorResponse {
            error_type: Some("TransactionConflictException".to_string()),
            code: None,
            message: Some("conflict".to_string()),
            item: None,
            cancellation_reasons: None,
        },
    );
    assert!(retryable);
    assert_eq!(
        error.attempt_failure(),
        Some(&AttemptFailure::Conflict(ConflictCause::Remote))
    );
}

#[test]
fn given_remote_internal_error_body_when_mapped_then_text_not_kept() {
    let (error, _, _) = classify_error_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        RemoteErrorResponse {
            error_type: Some("InternalServerError".to_string()),
            code: None,
            message: Some("remote secret detail".to_string()),
            item: None,
            cancellation_reasons: None,
        },
    );
    assert_eq!(
        error.to_string(),
        "internal error: remote InternalServerError"
    );
}

fn cancellation_body(
    message: Option<&str>,
    reasons: Vec<RemoteCancellationReason>,
) -> RemoteErrorResponse {
    RemoteErrorResponse {
        error_type: Some("TransactionCanceledException".to_string()),
        code: None,
        message: message.map(str::to_string),
        item: None,
        cancellation_reasons: Some(reasons),
    }
}

fn remote_reason(code: &str) -> RemoteCancellationReason {
    RemoteCancellationReason {
        code: Some(code.to_string()),
        message: None,
        item: None,
    }
}

#[test]
fn given_reasons_without_message_when_classified_then_code_default_message() {
    let (error, retryable, _) = classify_error_response(
        StatusCode::BAD_REQUEST,
        cancellation_body(
            None,
            vec![
                remote_reason("ConditionalCheckFailed"),
                remote_reason("ValidationError"),
                remote_reason("TransactionConflict"),
            ],
        ),
    );

    assert!(!retryable);
    let StorageEnum::TransactionCanceled { reasons } = error.to_enum() else {
        panic!(
            "expected TransactionCanceled, got variant {}",
            error.label()
        );
    };
    let messages: Vec<Option<&str>> = reasons.iter().map(|r| r.message.as_deref()).collect();
    assert_eq!(
        messages,
        [
            Some(DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE),
            Some(""),
            None
        ]
    );
}

#[test]
fn given_unknown_reason_code_when_classified_then_aws_service_not_retryable() {
    let (error, retryable, code) = classify_error_response(
        StatusCode::BAD_REQUEST,
        cancellation_body(
            Some("cancelled"),
            vec![remote_reason("None"), remote_reason("SomethingNew")],
        ),
    );

    assert!(!retryable);
    assert_eq!(code.as_deref(), Some("TransactionCanceledException"));
    assert!(matches!(
        error.to_enum(),
        StorageEnum::AwsService { code: Some(code), message }
            if code == "TransactionCanceledException" && message == "cancelled"
    ));
}

#[test]
fn given_reason_item_not_attribute_map_when_classified_then_item_dropped() {
    let (error, _, _) = classify_error_response(
        StatusCode::BAD_REQUEST,
        cancellation_body(
            None,
            vec![RemoteCancellationReason {
                code: Some("ConditionalCheckFailed".to_string()),
                message: None,
                item: Some(serde_json::json!("text")),
            }],
        ),
    );

    assert!(matches!(
        error.to_enum(),
        StorageEnum::TransactionCanceled { reasons }
            if reasons.as_slice() == [CancellationReason::conditional_check_failed(None)]
    ));
}
