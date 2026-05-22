use crate::IndexName;

#[test]
fn index_name_sanitizes_sql_control_characters_for_backend_identifiers() {
    let index_name = IndexName::new("by'customer\";status");

    assert_eq!(index_name.sanitized_name(), "bycustomerstatus");
    assert_eq!(index_name.to_string(), "by'customer\";status");
    assert_eq!(index_name.as_ref(), "by'customer\";status");
}

#[test]
fn index_name_converts_borrowed_value_to_string_without_sanitizing() {
    let index_name = IndexName::new("ByCustomer");

    assert_eq!(String::from(&index_name), "ByCustomer");
}
