use crate::{KeyValidationError, validate_key_segment};

#[test]
fn validate_ok() {
    assert!(validate_key_segment("USER").is_ok());
}

#[test]
fn forbidden() {
    assert_eq!(
        validate_key_segment("BAD#SEG"),
        Err(KeyValidationError::ForbiddenChar)
    );
}

#[test]
fn empty() {
    assert_eq!(validate_key_segment(""), Err(KeyValidationError::Empty));
}
