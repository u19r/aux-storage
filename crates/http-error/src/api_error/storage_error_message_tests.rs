//! Tests ensuring StorageError -> HttpApiError mapping keeps canonical DynamoDB
//! messages.
use storage_types::{
    AttemptPhase, DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE,
    DYNAMODB_IDEMPOTENT_PARAMETER_MISMATCH_MESSAGE, DYNAMODB_INTERNAL_SERVER_ERROR_MESSAGE,
    DYNAMODB_TRANSACTION_CONFLICT_MESSAGE, InfrastructureContext, InfrastructureResultKind,
    NativeCallPhase, NativeCause, NativeCode, NativeEngine, StorageEnum, StorageError, expired_in,
};

use crate::HttpApiError;

#[test]
fn given_private_storage_failures_when_mapped_to_http_then_internal_authority_is_hidden() {
    let kinds = [
        InfrastructureResultKind::Encoding {
            context: InfrastructureContext::RecordEncoding,
        },
        InfrastructureResultKind::UnsupportedVersion {
            context: InfrastructureContext::PhysicalLayout,
        },
        InfrastructureResultKind::InvalidBound {
            context: InfrastructureContext::Bounds,
        },
    ];
    let errors = [
        StorageError::internal("synthetic native read failure"),
        StorageError::commit_unknown("private replication state"),
    ];
    for error in errors.into_iter().chain(kinds.map(StorageError::from)) {
        let response = HttpApiError::from(error);
        assert_eq!(response.status_code, 500);
        assert_eq!(
            response.error_type,
            "com.amazonaws.dynamodb.v20120810#InternalServerError"
        );
        assert_eq!(response.message, DYNAMODB_INTERNAL_SERVER_ERROR_MESSAGE);
        assert!(response.response_headers.is_empty());
    }
}

#[test]
fn conditional_check_failed_http_message() {
    let storage_err: StorageError = StorageEnum::ConditionalCheckFailed.into();
    let handler: HttpApiError = storage_err.into();
    assert_eq!(
        handler.error_type,
        "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException"
    );
    assert_eq!(handler.message, DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE);
}

#[test]
fn idempotent_parameter_mismatch_maps_to_dynamodb_error() {
    let storage_error: StorageError = StorageEnum::IdempotentParameterMismatch.into();
    let response = HttpApiError::from(storage_error);

    assert_eq!(response.status_code, 400);
    assert_eq!(
        response.error_type,
        "com.amazonaws.dynamodb.v20120810#IdempotentParameterMismatchException"
    );
    assert_eq!(
        response.message,
        DYNAMODB_IDEMPOTENT_PARAMETER_MISMATCH_MESSAGE
    );
    assert!(response.response_headers.is_empty());
}

#[test]
fn given_native_conflict_when_mapped_then_transaction_conflict_constant_text() {
    let error = StorageError::native_conflict(NativeCause {
        engine: NativeEngine::Sqlite,
        code: NativeCode::Sqlite(5),
        phase: NativeCallPhase::Commit,
    });
    let response = HttpApiError::from(error);
    assert_eq!(
        response.error_type,
        "com.amazonaws.dynamodb.v20120810#TransactionConflictException"
    );
    assert_eq!(response.status_code, 400);
    assert_eq!(response.message, DYNAMODB_TRANSACTION_CONFLICT_MESSAGE);
    assert!(!response.message.to_lowercase().contains("sqlite"));
}

#[test]
fn given_commit_unknown_when_mapped_then_internal_server_error_constant() {
    let response = HttpApiError::from(StorageError::commit_unknown("SQLite attempt task stopped"));
    assert_eq!(response.status_code, 500);
    assert_eq!(response.message, DYNAMODB_INTERNAL_SERVER_ERROR_MESSAGE);
}

#[test]
fn given_attempt_unavailable_when_mapped_then_503_retry_after() {
    let response = HttpApiError::from(expired_in(AttemptPhase::Statement));
    assert_eq!(response.status_code, 503);
    assert!(
        response
            .response_headers
            .iter()
            .any(|(name, value)| name.eq_ignore_ascii_case("retry-after") && value == "1"),
        "{:?}",
        response.response_headers
    );
}

#[test]
fn given_internal_with_detail_when_mapped_then_wire_hides_detail() {
    let error = StorageError::internal("secret-detail");
    assert!(error.to_string().contains("secret-detail"));
    let response = HttpApiError::from(error);
    assert_eq!(response.message, DYNAMODB_INTERNAL_SERVER_ERROR_MESSAGE);
    assert!(!response.message.contains("secret-detail"));
}
