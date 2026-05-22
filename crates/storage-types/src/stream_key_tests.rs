use crate::{SerializesToKey, StreamItemId, StreamKey, TableName};

#[test]
fn stream_key_for_system_stream_appends_item_id_to_system_stream_name() {
    let item_id = StreamItemId::from([0x01; 12]);

    let key = StreamKey::for_system_stream(&item_id);

    assert_eq!(
        key.serialize_to_bytes()
            .expect("stream key should serialize"),
        [
            b"system-streams/tables/".as_slice(),
            item_id.as_bytes().as_slice()
        ]
        .concat()
    );
}

#[test]
fn stream_key_for_table_stream_uses_sanitized_table_name() {
    let table = TableName::new("orders/2026");
    let item_id = StreamItemId::from([0x02; 12]);

    let key = StreamKey::for_table_stream(&table, &item_id);

    assert_eq!(
        key.as_ref(),
        [
            b"orders2026/stream-table/".as_slice(),
            item_id.as_bytes().as_slice()
        ]
        .concat()
    );
}

#[test]
fn stream_key_debug_renders_lossy_bytes_for_operator_diagnostics() {
    let key = StreamKey::new(b"stream/key");

    assert_eq!(format!("{key:?}"), "StreamKey(stream/key)");
}
