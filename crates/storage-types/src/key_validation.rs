use thiserror::Error;

use crate::constants::KEY_VALIDATION_MAX_SEGMENT_BYTES;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum KeyValidationError {
    #[error("Key empty.")]
    Empty,
    #[error("Key too long: {0} > {1}.")]
    TooLong(usize, usize),
    #[error("Forbidden character '#' in key.")]
    ForbiddenChar,
    #[error("Whitespace detected in key.")]
    Whitespace,
}

pub fn validate_key_segment(seg: &str) -> Result<(), KeyValidationError> {
    if seg.is_empty() {
        return Err(KeyValidationError::Empty);
    }
    if seg.len() > KEY_VALIDATION_MAX_SEGMENT_BYTES {
        return Err(KeyValidationError::TooLong(
            seg.len(),
            KEY_VALIDATION_MAX_SEGMENT_BYTES,
        ));
    }
    if seg.contains('#') {
        return Err(KeyValidationError::ForbiddenChar);
    }
    if seg.chars().any(char::is_whitespace) {
        return Err(KeyValidationError::Whitespace);
    }
    Ok(())
}

pub fn validate_full_key(pk: &str, sk: &str) -> Result<(), KeyValidationError> {
    // pk and sk may contain '#'; split and validate segments individually
    for part in pk.split('#') {
        validate_key_segment(part)?;
    }
    for part in sk.split('#') {
        validate_key_segment(part)?;
    }
    Ok(())
}
