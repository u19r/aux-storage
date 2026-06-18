use crate::{AttributeValue, IndexName, ItemKey, SerializesToKey, TableKey, TableName};

#[test]
fn sorted_storage_suffix_matches_legacy_primary_storage_suffix() {
    let table_name = TableName::new("orders");
    let item_key = ItemKey::table_key(
        table_name.clone(),
        AttributeValue::S("account#1".to_string()),
        Some(AttributeValue::N("42".to_string())),
    );
    let full_key = item_key
        .serialize_to_bytes()
        .expect("legacy item key should serialize");
    let legacy_prefix = ItemKey::table_prefix_from_name(&table_name);

    assert_eq!(
        item_key.sorted_storage_suffix().expect("sorted suffix"),
        full_key
            .strip_prefix(legacy_prefix.as_slice())
            .expect("legacy key should include table prefix")
    );
}

#[test]
fn sorted_storage_suffix_matches_legacy_gsi_storage_suffix() {
    let table_name = TableName::new("orders");
    let index_name = IndexName::new("by_status");
    let table_key = TableKey::new(
        table_name.clone(),
        AttributeValue::S("account#1".to_string()),
        Some(AttributeValue::N("42".to_string())),
    );
    let item_key = ItemKey::index_key(
        table_name.clone(),
        index_name.clone(),
        AttributeValue::S("open".to_string()),
        Some(AttributeValue::S("2026-06-16".to_string())),
        table_key,
    );
    let full_key = item_key
        .serialize_to_bytes()
        .expect("legacy gsi key should serialize");
    let legacy_prefix = ItemKey::index_prefix_from_name(&table_name, &index_name);

    assert_eq!(
        item_key.sorted_storage_suffix().expect("sorted suffix"),
        full_key
            .strip_prefix(legacy_prefix.as_slice())
            .expect("legacy key should include index prefix")
    );
}
