use storage_types::{AttributeValue, KeyAttributes};

use crate::item_lock::item_lock_statement;

#[test]
fn given_equivalent_numeric_and_binary_keys_when_locking_then_identity_is_canonical() {
    let table = crate::test_support::table();
    let mut left = KeyAttributes::new();
    left.insert("pk", AttributeValue::N("1.0".to_owned()));
    left.insert("sk", AttributeValue::B("YQ==".to_owned()));
    let mut right = KeyAttributes::new();
    right.insert("sk", AttributeValue::B("YQ".to_owned()));
    right.insert("pk", AttributeValue::N("1E0".to_owned()));
    let (left_sql, left_binds) = item_lock_statement(std::iter::once((&table, &left)))
        .unwrap()
        .into_parts();
    let (right_sql, right_binds) = item_lock_statement(std::iter::once((&table, &right)))
        .unwrap()
        .into_parts();
    assert_eq!(left_sql, right_sql);
    assert_eq!(text(&left_binds), text(&right_binds));
    assert!(left_sql.contains("ORDER BY lock_id"));
    let mut replacement = table.clone();
    replacement.history_id = uuid::Uuid::from_bytes([2; 16]).into();
    let (_, replacement_binds) = item_lock_statement(std::iter::once((&replacement, &left)))
        .unwrap()
        .into_parts();
    assert_ne!(text(&left_binds), text(&replacement_binds));
}

fn text(binds: &[crate::SqlValue]) -> &str {
    let [crate::SqlValue::Text(value)] = binds else {
        panic!("one text identity expected")
    };
    value
}
