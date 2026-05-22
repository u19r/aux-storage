use crate::CursorName;

#[test]
fn cursor_name_preserves_caller_supplied_cursor_identifier() {
    let cursor = CursorName::new("worker-a");

    assert_eq!(cursor.as_str(), "worker-a");
    assert_eq!(cursor.to_string(), "worker-a");
    assert_eq!(&*cursor, "worker-a");
}

#[test]
fn cursor_name_can_be_built_from_owned_strings() {
    let cursor = CursorName::from("worker-b".to_string());

    assert_eq!(cursor.as_str(), "worker-b");
}
