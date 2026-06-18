use storage_types::TableName;

use crate::storage_ops::{
    CHANGE_INDEX_PREFIX, change_index_key, change_index_slot,
    write_helpers::CHANGE_INDEX_SLOT_COUNT,
};

#[test]
fn given_table_name_when_slot_is_computed_then_result_is_stable_and_bounded() {
    let table = TableName::new("source_users");

    let first = change_index_slot(&table);
    let second = change_index_slot(&table);

    assert_eq!(first, second);
    assert!(first < CHANGE_INDEX_SLOT_COUNT);
}

#[test]
fn given_stream_item_when_change_index_key_is_built_then_slot_version_and_table_are_encoded() {
    let table = TableName::new("source_users");
    let slot = change_index_slot(&table);
    let key = change_index_key(slot, b"stream:source_users:42:abc", &table);
    let key = String::from_utf8(key).expect("change index key is utf8");

    assert_eq!(
        key,
        format!("{CHANGE_INDEX_PREFIX}/slot/{slot}/stream:source_users:42:abc/source_users")
    );
}
