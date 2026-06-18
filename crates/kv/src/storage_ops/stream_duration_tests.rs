use storage_provider::StreamTrimScope;
use storage_types::{StreamItemId, StreamName, StreamRetentionDuration, TableName};

use super::stream_duration::{
    item_stream_key_hash, item_stream_policy_version, item_stream_scope_id, stream_name_for_scope,
    table_stream_policy_version,
};
use crate::keyspace::{
    compact::{self, TableStorageId},
    stream_keys,
    table_identity::TableIdentity,
};

#[test]
fn kv_due_marker_keys_sort_by_bucket_scope_and_policy_version() {
    let scope_a = b"scope-a";
    let scope_b = b"scope-b";

    let mut keys = [
        compact::stream_trim_due_key(1_000, scope_b, 1),
        compact::stream_trim_due_key(4_000_000, scope_a, 1),
        compact::stream_trim_due_key(1_000, scope_a, 2),
        compact::stream_trim_due_key(1_000, scope_a, 1),
    ];
    keys.sort();

    assert_eq!(keys[0], compact::stream_trim_due_key(1_000, scope_a, 1));
    assert_eq!(keys[1], compact::stream_trim_due_key(1_000, scope_a, 2));
    assert_eq!(keys[2], compact::stream_trim_due_key(1_000, scope_b, 1));
    assert_eq!(keys[3], compact::stream_trim_due_key(4_000_000, scope_a, 1));
}

#[test]
fn kv_due_marker_keys_are_compact_binary_keys() {
    let key = compact::stream_trim_due_key(1_000, b"table/with%chars", 1);

    assert_eq!(key[0], compact::KeyFamily::StreamTrimDue.code());
    assert!(!key.starts_with(b"sys/stream-duration/due/"));
}

#[test]
fn kv_stream_pointer_index_keys_group_by_table_and_item_stream() {
    let table = TableName::new("orders");
    let table_identity = TableIdentity::new(TableStorageId::new(42), table.clone(), Vec::new());
    let item_stream = StreamName::new(b"orders/stream-item/example");
    let item_id = StreamItemId::random();

    let key =
        stream_keys::stream_pointer_item_key_for_stream(&table_identity, &item_stream, item_id)
            .expect("item pointer key");
    let prefix = stream_keys::stream_pointer_item_prefix_for_stream(&table_identity, &item_stream)
        .expect("item pointer prefix");

    assert_eq!(key[0], compact::KeyFamily::StreamPointerItemIndex.code());
    assert!(key.starts_with(&prefix.start));
    assert_eq!(&key[prefix.start.len()..], item_id.as_bytes());
    assert!(!key.starts_with(b"sys/stream-duration/pointer/"));

    let table_key = stream_keys::stream_pointer_table_key_for_stream(&table_identity, item_id);
    let table_prefix = compact::stream_pointer_table_prefix(table_identity.table_id);
    assert_eq!(
        table_key[0],
        compact::KeyFamily::StreamPointerTableIndex.code()
    );
    assert!(table_key.starts_with(&table_prefix.start));
    assert_eq!(&table_key[table_prefix.start.len()..], item_id.as_bytes());
    assert!(!table_key.starts_with(b"sys/stream-duration/pointer/"));
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
