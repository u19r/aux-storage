use crate::{IndexName, TableName};

#[test]
fn sanitized_name_allows_alphanumeric() {
    let name = TableName::new("table123ABC");
    assert_eq!(name.sanitized_name(), "table123ABC");
}

#[test]
fn sanitized_name_allows_underscore() {
    let name = TableName::new("table_name_123");
    assert_eq!(name.sanitized_name(), "table_name_123");
}

#[test]
fn sanitized_name_allows_dot() {
    let name = TableName::new("table.name.123");
    assert_eq!(name.sanitized_name(), "table.name.123");
}

#[test]
fn sanitized_name_allows_hyphen() {
    let name = TableName::new("table-name-123");
    assert_eq!(name.sanitized_name(), "table-name-123");
}

#[test]
fn sanitized_name_removes_single_quote() {
    let name = TableName::new("table'name");
    assert_eq!(name.sanitized_name(), "tablename");
}

#[test]
fn sanitized_name_removes_double_quote() {
    let name = TableName::new("table\"name");
    assert_eq!(name.sanitized_name(), "tablename");
}

#[test]
fn sanitized_name_removes_semicolon() {
    let name = TableName::new("table;name");
    assert_eq!(name.sanitized_name(), "tablename");
}

#[test]
fn sanitized_name_removes_slash() {
    let name = TableName::new("table/name");
    assert_eq!(name.sanitized_name(), "tablename");
}

#[test]
fn sanitized_name_removes_spaces() {
    let name = TableName::new("table name 123");
    assert_eq!(name.sanitized_name(), "tablename123");
}

#[test]
fn sanitized_name_removes_special_characters() {
    let name = TableName::new("table@name#123$test%");
    assert_eq!(name.sanitized_name(), "tablename123test");
}

#[test]
fn sanitized_name_removes_unicode() {
    let name = TableName::new("table_name_日本語");
    assert_eq!(name.sanitized_name(), "table_name_");
}

#[test]
fn sanitized_name_mixed_valid_invalid() {
    let name = TableName::new("table-123_name.test';/");
    assert_eq!(name.sanitized_name(), "table-123_name.test");
}

#[test]
fn sanitized_name_all_valid_characters() {
    let name = TableName::new("abcXYZ012_.-");
    assert_eq!(name.sanitized_name(), "abcXYZ012_.-");
}

#[test]
fn sanitized_name_empty_after_filtering() {
    let name = TableName::new("';/@#$%");
    assert_eq!(name.sanitized_name(), "");
}

#[test]
fn given_table_name_when_physical_names_are_built_then_prefixes_match_sql_backends() {
    let table = TableName::new("orders");
    let index = IndexName::new("by-status");

    assert_eq!(table.physical_table_name(), "table_orders");
    assert_eq!(
        table.physical_gsi_table_name(&index),
        "gsi_orders_by-status"
    );
    assert_eq!(table.physical_ttl_index_table_name(), "ttl_index_orders");
}

#[test]
fn given_unsafe_table_name_when_physical_names_are_built_then_parts_are_sanitized() {
    let table = TableName::new("ord';ers");
    let index = IndexName::new("by\"status");

    assert_eq!(table.physical_table_name(), "table_orders");
    assert_eq!(table.physical_gsi_table_name(&index), "gsi_orders_bystatus");
}
