use crate::{
    StorageEnum, StorageError, StorageValidationKind,
    context::{ErrorContext as _, WrappedError as _},
    dynamodb_table_not_found_message,
};

#[test]
fn storage_validation_helper_uses_canonical_key_validation_message() {
    let error = StorageError::invalid_or_missing_key();

    assert!(
        matches!(
            error,
            StorageError::Base(StorageEnum::Validation { ref message })
                if message == StorageValidationKind::InvalidOrMissingKey.message()
        ),
        "unexpected error: {error:?}"
    );
}

#[test]
fn storage_table_helpers_preserve_resource_name_and_dynamodb_message() {
    let already_exists = StorageError::table_already_exists("Orders");
    let not_found = StorageError::table_not_found("Orders");

    assert!(
        matches!(
            already_exists,
            StorageError::Base(StorageEnum::TableAlreadyExists { ref name }) if name == "Orders"
        ),
        "unexpected error: {already_exists:?}"
    );
    assert!(
        matches!(
            not_found,
            StorageError::Base(StorageEnum::TableNotFound { ref name, ref message })
                if name == "Orders" && message == &dynamodb_table_not_found_message("Orders")
        ),
        "unexpected error: {not_found:?}"
    );
}

#[test]
fn storage_cursor_helpers_map_to_internal_resource_errors() {
    let not_found = StorageError::cursor_not_found("cursor-a");
    let exists = StorageError::cursor_already_exists("cursor-a");

    assert!(
        matches!(
            not_found,
            StorageError::Base(StorageEnum::ResourceNotFound {
                resource_type: "cursor",
                ref resource_id,
            }) if resource_id == "cursor-a"
        ),
        "unexpected error: {not_found:?}"
    );
    assert!(
        matches!(
            exists,
            StorageError::Base(StorageEnum::ResourceExists {
                resource_type: "cursor",
                ref resource_id,
            }) if resource_id == "cursor-a"
        ),
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
        matches!(
            guard,
            StorageError::Base(StorageEnum::GuardConflict { ref message })
                if message == "revision mismatch"
        ),
        "unexpected error: {guard:?}"
    );
    assert!(
        matches!(
            unsupported,
            StorageError::Base(StorageEnum::Unsupported { ref message })
                if message == "not implemented"
        ),
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
