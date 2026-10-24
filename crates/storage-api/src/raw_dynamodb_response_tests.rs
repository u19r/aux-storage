use storage_types::{AttributeValue, WireItem, WireItemKeyAttributes};

use crate::raw_dynamodb_response::write_wire_item;

#[test]
fn local_split_wire_item_serialization_uses_keys_over_blob_duplicates() {
    let primary_key = WireItemKeyAttributes::new(
        "pk".to_owned(),
        AttributeValue::S("tenant#0000".to_owned()),
        None,
        None,
    );
    let item = WireItem::local_split(
        primary_key,
        None,
        Some(br#"{"pk":{"S":"stale"},"payload":{"S":"value"}}"#.to_vec()),
    );
    let mut out = Vec::new();
    write_wire_item(&mut out, item).unwrap();
    let decoded: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(decoded["pk"]["S"], "tenant#0000");
    assert_eq!(decoded["payload"]["S"], "value");
}
