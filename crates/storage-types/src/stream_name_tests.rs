use crate::{
    AttributeValue, ItemKey, StreamName, TableName, stream_name::STREAM_ITEM_KEY_MAX_BYTES,
};

#[test]
fn table_item_stream_keeps_small_key_payload() {
    let table = TableName::new("users");
    let key = ItemKey::table_key(
        table.clone(),
        AttributeValue::S("pk".to_string()),
        Some(AttributeValue::S("sk".to_string())),
    );
    let stream = StreamName::table_item_stream(&table, &key).expect("stream");
    let mut expected = table.sanitized_name().as_bytes().to_vec();
    expected.extend(b"/stream-item/");
    expected.extend(key.hash_range_key_part().expect("key part"));
    assert_eq!(Vec::<u8>::from(&stream), expected);
}

#[test]
fn table_item_stream_hashes_oversized_key_payload() {
    let table = TableName::new("users");
    let long_scope = "x".repeat(STREAM_ITEM_KEY_MAX_BYTES * 2);
    let key = ItemKey::table_key(
        table.clone(),
        AttributeValue::S("pk".to_string()),
        Some(AttributeValue::S(long_scope)),
    );

    let stream_one = StreamName::table_item_stream(&table, &key).expect("stream");
    let stream_two = StreamName::table_item_stream(&table, &key).expect("stream");
    assert_eq!(
        stream_one, stream_two,
        "stream naming must be deterministic"
    );

    let stream_name: String = (&stream_one).into();
    assert!(
        stream_name.contains("/stream-item/hash/"),
        "oversized keys must use hashed stream name"
    );
    assert!(
        stream_name.len() < 128,
        "hashed stream names should remain short"
    );
}

#[test]
fn stream_name_table_stream_uses_sanitized_table_name() {
    let table = TableName::new("orders/2026");

    let stream = StreamName::table_stream(&table);

    assert_eq!(String::from(stream), "orders2026/stream-table");
}

#[test]
fn stream_name_converts_between_owned_and_borrowed_bytes() {
    let stream = StreamName::from("events");

    assert_eq!(Vec::<u8>::from(&stream), b"events");
    assert_eq!(String::from(stream.clone()), "events");
    assert_eq!(stream.as_ref(), b"events");
}
