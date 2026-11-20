use storage_types::UserStreamName;

use super::validation::*;
use crate::{CursorName, StreamEnum, StreamError, StreamValidationKind};

#[test]
fn given_unicode_and_separators_when_validating_names_then_valid_names_are_accepted() {
    for name in ["a", "é", "中", "é_a-中", "a.b", &"a".repeat(255)] {
        validate_user_stream_name(&UserStreamName::new(name)).unwrap();
    }
    for name in ["a", "é", "中", "é_a-中", &"a".repeat(64)] {
        validate_cursor_name(&CursorName::new(name)).unwrap();
    }
    validate_name_format("", "Name", &[]).unwrap();
}

#[test]
fn given_invalid_names_when_validating_then_error_kinds_messages_and_precedence_are_preserved() {
    for (name, kind, detail) in [
        (
            "",
            StreamValidationKind::EmptyName,
            "Stream name cannot be empty",
        ),
        (
            "_a",
            StreamValidationKind::InvalidNameBoundary,
            "Stream name must start and end with alphanumeric characters",
        ),
        (
            "a_",
            StreamValidationKind::InvalidNameBoundary,
            "Stream name must start and end with alphanumeric characters",
        ),
        (
            "a!_",
            StreamValidationKind::InvalidNameCharacters,
            "Stream name can only contain alphanumeric characters, _, -, .",
        ),
        (
            "_!a",
            StreamValidationKind::InvalidNameBoundary,
            "Stream name must start and end with alphanumeric characters",
        ),
        (
            "a\u{301}a",
            StreamValidationKind::InvalidNameCharacters,
            "Stream name can only contain alphanumeric characters, _, -, .",
        ),
    ] {
        assert_validation_error(
            validate_user_stream_name(&UserStreamName::new(name)).unwrap_err(),
            kind,
            detail,
        );
    }
    assert_validation_error(
        validate_cursor_name(&CursorName::new("a.b")).unwrap_err(),
        StreamValidationKind::InvalidNameCharacters,
        "Cursor name can only contain alphanumeric characters, _, -",
    );
    assert_validation_error(
        validate_name_format("a!b", "Name", &[]).unwrap_err(),
        StreamValidationKind::InvalidNameCharacters,
        "Name can only contain alphanumeric characters",
    );
}

#[test]
fn given_multibyte_names_when_validating_length_then_limits_count_bytes() {
    validate_user_stream_name(&UserStreamName::new(&format!("{}a", "é".repeat(127)))).unwrap();
    validate_cursor_name(&CursorName::new(&"é".repeat(32))).unwrap();
    assert_validation_error(
        validate_user_stream_name(&UserStreamName::new(&"é".repeat(128))).unwrap_err(),
        StreamValidationKind::NameTooLong,
        "Stream name must not exceed 255 characters",
    );
    assert_validation_error(
        validate_cursor_name(&CursorName::new(&format!("{}a", "é".repeat(32)))).unwrap_err(),
        StreamValidationKind::NameTooLong,
        "Cursor name must not exceed 64 characters",
    );
}

#[test]
fn given_ttl_bounds_when_validating_then_one_second_to_one_year_is_accepted() {
    validate_ttl_seconds(1).unwrap();
    validate_ttl_seconds(31_536_000).unwrap();
    for ttl in [0, 31_536_001] {
        assert_validation_error(
            validate_ttl_seconds(ttl).unwrap_err(),
            StreamValidationKind::InvalidTtl,
            "TTL must be between 1 second and 1 year (31,536,000 seconds)",
        );
    }
}

fn assert_validation_error(
    error: StreamError,
    expected_kind: StreamValidationKind,
    expected_message: &str,
) {
    match error.base_error() {
        Some(StreamEnum::Validation { kind, message }) => {
            assert_eq!(*kind, expected_kind);
            assert_eq!(message, expected_message);
        }
        error => panic!("unexpected validation error: {error:?}"),
    }
}
