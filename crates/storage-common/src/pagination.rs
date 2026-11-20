//! Pagination helpers shared by storage backends.
//!
//! DynamoDB-like semantics:
//! - A limit of 0 is invalid.
//! - Values above the max are clamped.
//! - An absent limit uses the max; the 1 MB evaluated-bytes budget ends the
//!   page first for all but small items.
use storage_types::{StorageError, StorageResult};

/// Normalize a requested limit into an effective item limit.
pub fn normalize_limit(requested: Option<u32>, max_limit: u32) -> StorageResult<u32> {
    let limit = requested.unwrap_or(max_limit);
    if limit == 0 {
        return Err(StorageError::validation(
            "1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: \
             Member must have value greater than or equal to 1",
        ));
    }
    Ok(limit.min(max_limit))
}

/// `normalize_limit` for Query, whose refusal of a zero limit is worded
/// differently from Scan's (AWS behavioural suite,
/// `query-limit-zero-rejected`).
pub fn normalize_query_limit(requested: Option<u32>, max_limit: u32) -> StorageResult<u32> {
    if requested == Some(0) {
        return Err(StorageError::validation(
            "1 validation error detected: Value at 'Limit' failed to satisfy constraint: Member \
             must have value greater than or equal to 1",
        ));
    }
    normalize_limit(requested, max_limit)
}
