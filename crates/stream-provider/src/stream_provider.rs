//! Stream limit validation.
use crate::{
    constants::STREAM_LIMITS,
    errors::{StreamError, StreamResult, StreamValidationKind},
};

/// Validate that a limit is between 1 and 1000 inclusive
pub fn validate_limit(limit: u32) -> StreamResult<()> {
    STREAM_LIMITS
        .validate(limit)
        .map_err(|_| StreamError::validation(StreamValidationKind::InvalidLimit))
        .map(|_| ())
}
