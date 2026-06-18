use storage_types::{StreamItemId, TableName};

use crate::change_index::{slot_for_table, sortable_version};

#[test]
fn given_table_name_when_slot_is_computed_then_result_is_stable_and_bounded() {
    let table_name = TableName::new("orders");

    let slot = slot_for_table(&table_name);

    assert_eq!(slot, slot_for_table(&table_name));
    assert!(slot < 256);
}

#[test]
fn given_stream_version_when_formatted_then_lexical_order_matches_numeric_order() {
    let first = sortable_version(StreamItemId::from([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9]));
    let second = sortable_version(StreamItemId::from([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10]));

    assert!(first < second);
    assert_eq!(first, "000000000000000000000009");
}
