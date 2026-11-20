use storage_types::{
    ConsumedCapacity, GetItemResponse, ReturnConsumedCapacity, TableName, WireItem,
};

const ITEM_JSON: &[u8] = br#"{"pk":{"S":"tenant#001"},"sk":{"S":"item#001"},"payload":{"S":"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"},"counter":{"N":"42"},"active":{"BOOL":true}}"#;

#[test]
fn get_item_wire_serde_preserves_json_shape_and_rejects_non_objects() {
    let response = GetItemResponse {
        item: Some(WireItem::dynamo_json(ITEM_JSON.to_vec())),
        consumed_capacity: None,
    };
    let json = serde_json::to_vec(&response).unwrap();
    let actual: serde_json::Value = serde_json::from_slice(&json).unwrap();
    let expected: serde_json::Value = serde_json::from_slice(ITEM_JSON).unwrap();
    assert_eq!(actual["Item"], expected);
    let decoded: GetItemResponse = serde_json::from_slice(&json).unwrap();
    assert!(matches!(decoded.item, Some(WireItem::DynamoJson { .. })));
    assert!(serde_json::from_str::<GetItemResponse>(r#"{"Item":42}"#).is_err());
    assert!(serde_json::from_str::<GetItemResponse>(r#"{"Item":[]}"#).is_err());
}

#[test]
fn given_capacity_with_and_without_item_when_raw_get_item_is_written_then_json_matches_serde() {
    let capacity = ConsumedCapacity::zero(
        Some(ReturnConsumedCapacity::Indexes),
        &TableName::new("items"),
        None,
    )
    .expect("capacity requested");
    for item in [Some(WireItem::dynamo_json(ITEM_JSON.to_vec())), None] {
        let response = GetItemResponse {
            item,
            consumed_capacity: Some(capacity.clone()),
        };
        let expected = serde_json::to_value(&response).unwrap();
        let raw = crate::get_item_response::into_json_bytes(response).unwrap();
        let actual: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(
            actual["ConsumedCapacity"],
            serde_json::json!({"TableName": "items", "CapacityUnits": 0.0,
                "Table": {"CapacityUnits": 0.0}})
        );
    }
}
