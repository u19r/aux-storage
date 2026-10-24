use storage_types::{StreamItemId, StreamName};

use crate::{
    BootstrapCursorName, ProtectedCursorPageEnd, ProtectedCursorReadRequest, StreamEnum,
    StreamValidationKind,
};

#[test]
fn protected_read_request_validates_its_limit() {
    let boundary = StreamItemId::from([1; 12]);
    let resume = StreamItemId::from([0; 12]);
    let error = ProtectedCursorReadRequest::try_new(
        StreamName::system_table_stream(),
        BootstrapCursorName::try_new("table", "peer", "generation")
            .expect("identity should be valid"),
        boundary,
        resume,
        0,
    )
    .expect_err("a protected read cannot use a zero page limit");
    assert!(matches!(
        error.base_error(),
        Some(StreamEnum::Validation {
            kind: StreamValidationKind::InvalidLimit,
            ..
        })
    ));
}

#[test]
fn bootstrap_cursor_identity_is_canonical_and_bounded() {
    let identity = BootstrapCursorName::try_new("table/a", "peer:one", "generation-1")
        .expect("identity should be valid");
    let parsed = BootstrapCursorName::parse(identity.cursor_name()).expect("identity parses");
    assert_eq!(parsed, identity);
    assert!(BootstrapCursorName::parse(&crate::CursorName::new("ordinary-cursor")).is_err());
}

#[test]
fn bootstrap_cursor_rejects_uppercase_hex_without_reencoding() {
    let identity = BootstrapCursorName::try_new("table", "peer", "generation")
        .expect("identity should be valid");
    let canonical = identity.cursor_name().as_str();
    let index = canonical
        .char_indices()
        .find_map(|(index, value)| {
            (index >= crate::constants::BOOTSTRAP_CURSOR_PREFIX.len() && value.is_ascii_lowercase())
                .then_some(index)
        })
        .expect("encoded components contain a lowercase hex digit");
    let mut uppercase = canonical.to_string();
    let replacement = canonical
        .get(index..index + 1)
        .expect("ASCII hex digit is one byte")
        .to_ascii_uppercase();
    uppercase.replace_range(index..index + 1, &replacement);
    let uppercase = crate::CursorName::from(uppercase);

    assert!(BootstrapCursorName::parse(&uppercase).is_err());
}

#[test]
fn bootstrap_cursor_rejects_component_overflow_before_identity_build() {
    let component = "x".repeat(crate::constants::MAX_CURSOR_NAME_BYTES + 1);

    assert!(BootstrapCursorName::try_new(&component, "peer", "generation").is_err());
}

#[test]
fn terminal_is_an_explicit_protected_page_result() {
    assert!(ProtectedCursorPageEnd::Terminal.is_terminal());
    assert!(!ProtectedCursorPageEnd::More.is_terminal());
}
