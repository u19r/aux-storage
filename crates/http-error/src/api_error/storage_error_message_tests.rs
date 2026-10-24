//! Tests ensuring StorageError -> HttpApiError mapping keeps canonical DynamoDB
//! messages.
use storage_types::{
    DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE, DYNAMODB_IDEMPOTENT_PARAMETER_MISMATCH_MESSAGE,
    DYNAMODB_INTERNAL_SERVER_ERROR_MESSAGE, InfrastructureContext, InfrastructureResultKind,
    StorageEnum, StorageError,
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
        InfrastructureResultKind::ProtectedConflict {
            context: InfrastructureContext::Revision,
        },
    ];
    let errors = [
        StorageError::internal("synthetic native read failure"),
        StorageError::sync_not_leader(Some("private-host".to_string())),
        StorageEnum::SyncSubmissionAmbiguous {
            proposal_id: Some("private-proposal".to_string()),
            message: "private replication state".to_string(),
        }
        .into(),
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
