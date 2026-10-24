use crate::{
    StorageEnum, StorageError, StorageValidationKind,
    context::{ErrorContext as _, WrappedError as _},
    dynamodb_table_not_found_message,
};

#[test]
fn storage_validation_helper_uses_canonical_key_validation_message() {
    let error = StorageError::invalid_or_missing_key();

    assert!(
        matches!(error.base_error(), Some(StorageEnum::Validation { message, kind: StorageValidationKind::InvalidOrMissingKey })
                if message == StorageValidationKind::InvalidOrMissingKey.message()),
        "unexpected error: {error:?}"
    );
}

#[test]
fn storage_table_helpers_preserve_resource_name_and_dynamodb_message() {
    let already_exists = StorageError::table_already_exists("Orders");
    let not_found = StorageError::table_not_found("Orders");

    assert!(
        matches!(already_exists.base_error(), Some(StorageEnum::TableAlreadyExists { name }) if name == "Orders"),
        "unexpected error: {already_exists:?}"
    );
    assert!(
        matches!(not_found.base_error(), Some(StorageEnum::TableNotFound { name, message })
                if name == "Orders" && message == &dynamodb_table_not_found_message("Orders")),
        "unexpected error: {not_found:?}"
    );
}

#[test]
fn storage_cursor_helpers_map_to_internal_resource_errors() {
    let not_found = StorageError::cursor_not_found("cursor-a");
    let exists = StorageError::cursor_already_exists("cursor-a");

    assert!(
        matches!(not_found.base_error(), Some(StorageEnum::ResourceNotFound {
                resource_type: "cursor",
                resource_id,
            }) if resource_id == "cursor-a"),
        "unexpected error: {not_found:?}"
    );
    assert!(
        matches!(exists.base_error(), Some(StorageEnum::ResourceExists {
                resource_type: "cursor",
                resource_id,
            }) if resource_id == "cursor-a"),
        "unexpected error: {exists:?}"
    );
}

#[test]
fn storage_error_context_preserves_underlying_error_and_context_stack() {
    let result: Result<(), StorageError> = Err(StorageError::internal("write failed"));
    let error = result
        .context("commit item")
        .expect_err("context should wrap error");

    let (inner, contexts) = error.recursive_context(Vec::new());

    assert!(
        matches!(
            inner,
            StorageEnum::InternalServerError { message } if message == "write failed"
        ),
        "unexpected inner error: {inner:?}"
    );
    assert_eq!(contexts, vec!["commit item".to_string()]);
    assert!(format!("{error:?}").contains("Context:"));
}

#[test]
fn storage_guard_and_unsupported_helpers_preserve_internal_messages() {
    let guard = StorageError::guard_conflict("revision mismatch");
    let unsupported = StorageError::unsupported("not implemented");

    assert!(
        matches!(guard.base_error(), Some(StorageEnum::GuardConflict { message })
                if message == "revision mismatch"),
        "unexpected error: {guard:?}"
    );
    assert!(
        matches!(unsupported.base_error(), Some(StorageEnum::Unsupported { message })
                if message == "not implemented"),
        "unexpected error: {unsupported:?}"
    );
}

#[test]
fn transaction_canceled_errors_are_not_retryable_writes() {
    let error: StorageError = StorageEnum::TransactionCanceled {
        reasons: vec!["TransactionConflict".to_string()],
    }
    .into();

    assert!(!error.is_retryable_write());
}

#[test]
fn conditional_write_contention_requires_only_conditional_cancellation_reasons() {
    let direct: StorageError = StorageEnum::ConditionalCheckFailed.into();
    assert!(direct.is_conditional_write_contention());

    let conditional: StorageError = StorageEnum::TransactionCanceled {
        reasons: vec![
            "None".to_string(),
            "ConditionalCheckFailed\tcondition did not match".to_string(),
        ],
    }
    .into();
    assert!(conditional.is_conditional_write_contention());

    let mixed: StorageError = StorageEnum::TransactionCanceled {
        reasons: vec![
            "ConditionalCheckFailed".to_string(),
            "TransactionConflict".to_string(),
        ],
    }
    .into();
    assert!(!mixed.is_conditional_write_contention());
}
