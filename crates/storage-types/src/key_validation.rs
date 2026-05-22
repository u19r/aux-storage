use thiserror::Error;

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
    #[expect(
        clippy::items_after_statements,
        reason = "Constant defined close to usage for clarity; acceptable here."
    )]
    const MAX: usize = 512; // conservative upper bound
    if seg.len() > MAX {
        return Err(KeyValidationError::TooLong(seg.len(), MAX));
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
