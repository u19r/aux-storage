//! Common error helper utilities to reduce ad-hoc construction patterns
//! across storage backends. Keep extremely small – this is not a new error
//! taxonomy, only ergonomics wrappers around existing `StorageError` APIs.
use storage_types::StorageError;

/// Build a validation error with a composed field + message pattern.
/// Usage: `err_validation("limit", "must_be_positive")`.
pub fn err_validation(field: &str, msg: &str) -> StorageError {
    StorageError::validation(format!("{field}:{msg}"))
}

/// Create an internal error with contextual message.
pub fn err_internal(msg: &(impl ToString + ?Sized)) -> StorageError {
    StorageError::internal(msg)
}

/// Guard that a condition holds, otherwise returns validation error.
pub fn ensure(cond: bool, field: &str, msg: &str) -> Result<(), StorageError> {
    if cond {
        Ok(())
    } else {
        Err(err_validation(field, msg))
    }
}
