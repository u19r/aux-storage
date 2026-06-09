use storage_provider::{StreamTrimDueMarker, StreamTrimScope};
use storage_types::{
    StreamItemId, StreamName, StreamRetentionDuration, TableName, TimestampMillis,
};

use super::stream_duration::{
    due_marker_key, item_stream_key_hash, item_stream_policy_version, item_stream_scope_id,
    stream_name_for_scope, stream_pointer_index_key, stream_pointer_item_prefix,
    stream_pointer_table_key, stream_pointer_table_prefix, table_stream_policy_version,
};

#[test]
fn kv_due_marker_keys_sort_by_bucket_scope_and_policy_version() {
    let table = TableName::new("orders");
    let early_b = marker(1_000, "scope-b", table.clone(), 1);
    let later_a = marker(4_000_000, "scope-a", table.clone(), 1);
    let early_a_v2 = marker(1_000, "scope-a", table.clone(), 2);
    let early_a_v1 = marker(1_000, "scope-a", table, 1);

    let mut keys = [
        due_marker_key(&early_b),
        due_marker_key(&later_a),
        due_marker_key(&early_a_v2),
        due_marker_key(&early_a_v1),
    ];
    keys.sort();

    assert_eq!(keys[0], due_marker_key(&early_a_v1));
    assert_eq!(keys[1], due_marker_key(&early_a_v2));
    assert_eq!(keys[2], due_marker_key(&early_b));
    assert_eq!(keys[3], due_marker_key(&later_a));
}

#[test]
fn kv_due_marker_keys_escape_scope_separators() {
    let marker = marker(1_000, "table/with%chars", TableName::new("orders"), 1);
    let key = due_marker_key(&marker);
    let key = String::from_utf8(key).expect("key is ascii");

    assert!(key.contains("table%2fwith%25chars"));
}

#[test]
fn kv_stream_pointer_index_keys_group_by_table_and_item_stream() {
    let table = TableName::new("orders");
    let item_stream = StreamName::new(b"orders/stream-item/example");
    let item_id = StreamItemId::random();

    let key = stream_pointer_index_key(&table, &item_stream, item_id);
    let prefix = stream_pointer_item_prefix(&table, &item_stream);

    assert!(key.starts_with(&prefix));
    assert_eq!(&key[prefix.len()..], item_id.as_bytes());

    let table_key = stream_pointer_table_key(&table, item_id);
    let table_prefix = stream_pointer_table_prefix(&table);
    assert!(table_key.starts_with(&table_prefix));
    assert_eq!(&table_key[table_prefix.len()..], item_id.as_bytes());
}

#[test]
fn kv_item_stream_scope_ids_round_trip_non_utf8_stream_names() {
    let stream_name = StreamName::new(b"orders/stream-item/\xff\x00binary");
    let scope_id = item_stream_scope_id(&stream_name);
    let key_hash = item_stream_key_hash(&stream_name);
    let scope = StreamTrimScope::item(scope_id, TableName::new("orders"), key_hash.clone());

    let decoded = stream_name_for_scope(&scope).expect("scope id should decode");

    assert_eq!(decoded.as_ref(), stream_name.as_ref());
    assert!(key_hash.starts_with("kv-key:"));
    assert!(key_hash.len() < scope.scope_id.len());
}

#[test]
fn kv_stream_duration_policy_versions_are_stable_for_identical_duration_policy() {
    let one_day = StreamRetentionDuration::FiniteHours(24);
    let two_days = StreamRetentionDuration::FiniteHours(48);
    let one_day_table = table_stream_policy_version(one_day, one_day);
    let repeated_one_day_table = table_stream_policy_version(one_day, one_day);
    let two_day_table = table_stream_policy_version(two_days, one_day);

    assert_eq!(one_day_table, repeated_one_day_table);
    assert_ne!(one_day_table, two_day_table);

    let one_day_item = item_stream_policy_version(one_day, two_days);
    let repeated_one_day_item = item_stream_policy_version(one_day, two_days);
    let forever_item = item_stream_policy_version(StreamRetentionDuration::Forever, two_days);

    assert_eq!(one_day_item, repeated_one_day_item);
    assert_ne!(one_day_item, forever_item);
}

fn marker(
    due_at: i64,
    scope_id: &str,
    table_name: TableName,
    policy_version: u64,
) -> StreamTrimDueMarker {
    StreamTrimDueMarker::new(
        TimestampMillis::from_timestamp(due_at),
        StreamTrimScope::table(scope_id, table_name),
        policy_version,
    )
}
