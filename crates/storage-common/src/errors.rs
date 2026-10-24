//! Common error helper utilities to reduce ad-hoc construction patterns
//! across storage backends. Keep extremely small – this is not a new error
//! taxonomy, only ergonomics wrappers around existing `StorageError` APIs.
use storage_types::StorageError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageJobParseError {
    Unsupported,
}

impl std::fmt::Display for StorageJobParseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("unsupported storage job")
    }
}

impl std::error::Error for StorageJobParseError {}

/// Build a validation error with a composed field + message pattern.
/// Usage: `err_validation("limit", "must_be_positive")`.
pub fn err_validation(field: &str, msg: &str) -> StorageError {
    StorageError::validation(format!("{field}:{msg}"))
}

/// Guard that a condition holds, otherwise returns validation error.
pub fn ensure(cond: bool, field: &str, msg: &str) -> Result<(), StorageError> {
    if cond {
        Ok(())
    } else {
        Err(err_validation(field, msg))
    }
}
