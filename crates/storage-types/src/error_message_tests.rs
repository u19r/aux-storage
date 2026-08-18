//! Tests asserting that StorageEnum Display strings match DynamoDB canonical
//! messages.

use crate::{
    DYNAMODB_ACCESS_DENIED_MESSAGE, DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE,
    DYNAMODB_INTERNAL_SERVER_ERROR_MESSAGE, DYNAMODB_LIMIT_EXCEEDED_MESSAGE,
    DYNAMODB_MISSING_AUTH_TOKEN_MESSAGE, DYNAMODB_PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE,
    DYNAMODB_REQUEST_LIMIT_EXCEEDED_MESSAGE, DYNAMODB_THROTTLING_MESSAGE,
    DYNAMODB_TRANSACTION_CANCELED_MESSAGE, DYNAMODB_TRANSACTION_CONFLICT_MESSAGE,
    DYNAMODB_TRANSACTION_IN_PROGRESS_MESSAGE, DYNAMODB_UNRECOGNIZED_CLIENT_MESSAGE, StorageEnum,
    StorageError, StorageValidationKind, context::WrappedError as _,
    dynamodb_table_not_found_message,
};

fn display(err: StorageError) -> String {
    format!("{}", err)
}

#[test]
fn conditional_check_failed_message_exact() {
    let err: StorageError = StorageEnum::ConditionalCheckFailed.into();
    assert_eq!(display(err), DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE);
}

#[test]
fn table_not_found_message_canonical() {
    let err = StorageError::table_not_found("my_table");
    assert_eq!(display(err), dynamodb_table_not_found_message("my_table"));
}

#[test]
fn table_already_exists_message() {
    let err = StorageError::table_already_exists("foo");
    assert_eq!(display(err), "Table already exists: foo");
}

#[test]
fn internal_server_error_message_canonical() {
    let err = StorageError::internal("some internal detail");
    assert_eq!(display(err), DYNAMODB_INTERNAL_SERVER_ERROR_MESSAGE);
}

#[test]
fn transaction_conflict_message() {
    let err: StorageError = StorageEnum::TransactionConflict {
        message: "details".into(),
    }
    .into();
    assert_eq!(display(err), DYNAMODB_TRANSACTION_CONFLICT_MESSAGE);
}

#[test]
fn transaction_in_progress_message() {
    let err: StorageError = StorageEnum::TransactionInProgress {
        message: "details".into(),
    }
    .into();
    assert_eq!(display(err), DYNAMODB_TRANSACTION_IN_PROGRESS_MESSAGE);
}

#[test]
fn transaction_canceled_message() {
    let err: StorageError = StorageEnum::TransactionCanceled {
        reasons: vec!["ConditionalCheckFailed".into()],
    }
    .into();
    assert_eq!(display(err), DYNAMODB_TRANSACTION_CANCELED_MESSAGE);
}

#[test]
fn validation_message_passthrough() {
    let err = StorageError::validation(
        "One or more parameter values were invalid: Invalid or missing key",
    );
    assert_eq!(
        display(err),
        "One or more parameter values were invalid: Invalid or missing key"
    );
}

#[test]
fn validation_kind_uses_canonical_message() {
    let err = StorageError::validation(StorageValidationKind::InvalidOrMissingKey);
    assert_eq!(
        display(err),
        "One or more parameter values were invalid: Invalid or missing key"
    );
}

#[test]
fn provisioned_throughput_message_canonical() {
    let err: StorageError = StorageEnum::ProvisionedThroughputExceeded {
        message: "details".into(),
    }
    .into();
    assert_eq!(
        display(err),
        DYNAMODB_PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE
    );
}

#[test]
fn throttling_message_canonical() {
    let err: StorageError = StorageEnum::Throttled {
        message: "details".into(),
    }
    .into();
    assert_eq!(display(err), DYNAMODB_THROTTLING_MESSAGE);
}

#[test]
fn local_admission_rejection_is_safe_retryable_and_distinct_from_throttling() {
    let error = StorageError::service_unavailable(0);
    let StorageEnum::ServiceUnavailable {
        message,
        retry_after_seconds,
    } = error.to_enum()
    else {
        panic!("local admission must use the ServiceUnavailable variant");
    };

    assert_eq!(message, "Storage is temporarily unavailable.");
    assert_eq!(*retry_after_seconds, 1);
    assert_eq!(display(error), "Storage is temporarily unavailable.");

    let upstream = StorageError::Base(StorageEnum::Throttled {
        message: "provider pressure".to_string(),
    });
    assert!(matches!(upstream.to_enum(), StorageEnum::Throttled { .. }));
}

#[test]
fn limit_exceeded_message_canonical() {
    let err: StorageError = StorageEnum::LimitExceeded {
        message: "details".into(),
    }
    .into();
    assert_eq!(display(err), DYNAMODB_LIMIT_EXCEEDED_MESSAGE);
}

#[test]
fn request_limit_exceeded_message_canonical() {
    let err: StorageError = StorageEnum::RequestLimitExceeded.into();
    assert_eq!(display(err), DYNAMODB_REQUEST_LIMIT_EXCEEDED_MESSAGE);
}

#[test]
fn missing_authentication_token_message_canonical() {
    let err: StorageError = StorageEnum::MissingAuthenticationToken.into();
    assert_eq!(display(err), DYNAMODB_MISSING_AUTH_TOKEN_MESSAGE);
}

#[test]
fn authentication_message_canonical() {
    let err: StorageError = StorageEnum::Authentication {
        message: "details".into(),
    }
    .into();
    assert_eq!(display(err), DYNAMODB_UNRECOGNIZED_CLIENT_MESSAGE);
}

#[test]
fn access_denied_message_canonical() {
    let err: StorageError = StorageEnum::AccessDenied {
        message: "details".into(),
    }
    .into();
    assert_eq!(display(err), DYNAMODB_ACCESS_DENIED_MESSAGE);
}
