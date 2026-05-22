//! Pagination helpers shared by storage backends.
//!
//! DynamoDB-like semantics:
//! - A user requested limit (optional) is normalized with a default and max.
//! - A limit of 0 is invalid.
//! - Values above the max are clamped.
//! - Absent limit uses the provided default.
use storage_types::{StorageError, StorageResult};

pub const DEFAULT_GENERIC_LIMIT: u32 = 100;
pub const MAX_GENERIC_LIMIT: u32 = 10_000;

/// Normalize a requested limit into an effective limit enforcing default and
/// max.
pub fn normalize_limit(
    requested: Option<u32>,
    default_limit: u32,
    max_limit: u32,
) -> StorageResult<u32> {
    let limit = requested.unwrap_or(default_limit);
    if limit == 0 {
        return Err(StorageError::validation("limit must be > 0"));
    }
    Ok(limit.min(max_limit))
}
