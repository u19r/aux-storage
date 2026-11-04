use serde_json::json;

use crate::{AttributeMap, StreamRecord};

#[test]
fn given_snapshot_or_delete_when_stream_record_is_serialized_then_one_explicit_item_is_required() {
    let image: AttributeMap = serde_json::from_value(json!({
        "pk": {"S": "key"}, "value": {"S": "snapshot"}
    }))
    .unwrap();
    for item in [Some(image), None] {
        let record = StreamRecord {
            cursor: None,
            source_table_name: Some(crate::TableName::new("orders")),
            keys: serde_json::from_value(json!({"pk": {"S": "key"}})).unwrap(),
            sequence_number: "1".into(),
            item: item.clone(),
            system_position: None,
        };
        let wire = serde_json::to_value(&record).unwrap();
        assert_eq!(
            wire,
            json!({
                "SourceTableName": "orders", "Keys": {"pk": {"S": "key"}},
                "SequenceNumber": "1", "Item": item
            })
        );
        let decoded: StreamRecord = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(decoded.item, record.item);
        assert_eq!(decoded.keys, record.keys);
        for obsolete in ["OldImage", "NewImage"] {
            let mut malformed = wire.clone();
            malformed[obsolete] = json!({"pk": {"S": "key"}});
            assert!(serde_json::from_value::<StreamRecord>(malformed).is_err());
        }
        let mut missing = wire;
        missing.as_object_mut().unwrap().remove("Item");
        assert!(serde_json::from_value::<StreamRecord>(missing).is_err());
    }
}
