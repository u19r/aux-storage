use serde::Serialize;
use serde_json::json;
use storage_types::{AttributeMap, BatchGetItemResponse, TableName, VecMap, WireItem};

#[derive(Clone, Serialize)]
struct MapResponse {
    #[serde(rename = "Responses")]
    responses: VecMap<TableName, Vec<AttributeMap>>,
}

fn responses(items_per_table: usize) -> (MapResponse, BatchGetItemResponse) {
    let mut map_tables = VecMap::new();
    let mut wire_tables = VecMap::new();
    for table in ["batch-a", "batch-b"] {
        let mut maps = Vec::with_capacity(items_per_table);
        let mut wires = Vec::with_capacity(items_per_table);
        for index in 0..items_per_table {
            let item = json!({
                "pk": {"S": format!("item#{index}")},
                "payload": {"S": "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"},
                "counter": {"N": "42"}
            });
            let bytes = serde_json::to_vec(&item).unwrap();
            maps.push(serde_json::from_slice(&bytes).unwrap());
            wires.push(WireItem::dynamo_json(bytes));
        }
        map_tables.insert(TableName::new(table), maps);
        wire_tables.insert(TableName::new(table), wires);
    }
    (
        MapResponse {
            responses: map_tables,
        },
        BatchGetItemResponse {
            responses: Some(wire_tables),
            unprocessed_keys: None,
            consumed_capacity: None,
        },
    )
}

#[test]
fn batch_get_wire_serde_preserves_response_shape() {
    let (map, wire) = responses(2);
    let expected: serde_json::Value = serde_json::to_value(map).unwrap();
    let actual: serde_json::Value = serde_json::to_value(&wire).unwrap();
    assert_eq!(actual, expected);
    let decoded: BatchGetItemResponse = serde_json::from_value(actual).unwrap();
    assert_eq!(
        decoded
            .responses
            .unwrap()
            .get(&TableName::new("batch-a"))
            .unwrap()
            .len(),
        2
    );
    assert!(
        serde_json::from_str::<BatchGetItemResponse>(r#"{"Responses":{"batch-a":[42]}}"#).is_err()
    );
}
