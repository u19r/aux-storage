use storage_types::UserStreamName;
use stream_provider::{CursorName, StreamEnum, StreamError, StreamValidationKind};

use crate::validation::{CursorNameValidation, UserStreamNameValidation, validate_name_format};

#[test]
fn given_unicode_and_separators_when_validating_names_then_valid_names_are_accepted() {
    for name in ["a", "é", "中", "é_a-中", "a.b", &"a".repeat(255)] {
        UserStreamName::new(name).validate_stream_name().unwrap();
    }
    for name in ["a", "é", "中", "é_a-中", &"a".repeat(64)] {
        CursorName::new(name).validate_cursor_name().unwrap();
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
            UserStreamName::new(name)
                .validate_stream_name()
                .unwrap_err(),
            kind,
            detail,
        );
    }
    assert_validation_error(
        CursorName::new("a.b").validate_cursor_name().unwrap_err(),
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
    UserStreamName::new(&format!("{}a", "é".repeat(127)))
        .validate_stream_name()
        .unwrap();
    CursorName::new(&"é".repeat(32))
        .validate_cursor_name()
        .unwrap();
    assert_validation_error(
        UserStreamName::new(&"é".repeat(128))
            .validate_stream_name()
            .unwrap_err(),
        StreamValidationKind::NameTooLong,
        "Stream name must not exceed 255 characters",
    );
    assert_validation_error(
        CursorName::new(&format!("{}a", "é".repeat(32)))
            .validate_cursor_name()
            .unwrap_err(),
        StreamValidationKind::NameTooLong,
        "Cursor name must not exceed 64 characters",
    );
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

#[tokio::test]
async fn given_missing_or_recreated_stream_when_reading_then_current_identity_and_errors_are_preserved()
 {
    let manager = crate::stream_test_support_tests::create_test_manager().await;
    let name = "é-stream";
    let error = manager.append_item(name, b"payload").await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "Requested resource not found: stream: é-stream not found"
    );
    let old = manager.create_stream(name, None).await.unwrap();
    manager.append_item(name, b"old payload").await.unwrap();
    manager.delete_stream(name).await.unwrap();
    let new = manager.create_stream(name, None).await.unwrap();
    assert_ne!(old, new);
    let page = manager
        .read_forward(UserStreamName::new(name), None, None)
        .await
        .unwrap();
    assert!(page.items.is_empty());
    assert!(manager.create_stream("a!b", None).await.is_err());
    assert!(manager.get_stream("a!b").await.unwrap().is_none());
}
