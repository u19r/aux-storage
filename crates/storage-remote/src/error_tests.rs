use http::StatusCode;
use storage_types::{StorageEnum, context::WrappedError as _};

use crate::error::{RemoteCancellationReason, RemoteErrorResponse, classify_error_response};

#[test]
fn resource_in_use_exception_maps_to_table_already_exists() {
    let (error, retryable, code) = classify_error_response(
        StatusCode::BAD_REQUEST,
        RemoteErrorResponse {
            error_type: Some("ResourceInUseException".to_string()),
            code: None,
            message: Some("The resource which you are attempting to change is in use.".to_string()),
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
            cancellation_reasons: None,
        },
    );
    let (client_error, retryable_client, client_code) = classify_error_response(
        StatusCode::BAD_REQUEST,
        RemoteErrorResponse {
            error_type: None,
            code: None,
            message: Some("bad request".to_string()),
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
            if reasons.iter().map(String::as_str).eq([
                "ConditionalCheckFailed\tThe conditional request failed.",
                "None"
            ])
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
            cancellation_reasons: Some(vec![RemoteCancellationReason {
                code: Some("ConditionalCheckFailed".to_string()),
                message: Some("The conditional request failed.".to_string()),
                item: Some(item),
            }]),
        },
    );

    assert!(!retryable);
    assert!(matches!(
        error.to_enum(),
        StorageEnum::TransactionCanceled { reasons }
            if reasons.iter().map(String::as_str).eq([
                "ConditionalCheckFailed\tThe conditional request failed.\t{\"pk\":{\"S\":\"item-1\"}}"
            ])
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
            if reasons.iter().map(String::as_str).eq(["None"])
    ));
}
