use serde::Serialize;
use serde_json::json;
use storage_types::{
    AttributeMap, AttributeProjection, AttributeValue, QueryResponse, ScanResponse, WireItem,
};

#[test]
fn given_attribute_list_with_duplicates_when_projecting_then_values_match() {
    let item = AttributeMap::from(std::collections::HashMap::from([
        ("pk".to_owned(), AttributeValue::S("tenant".to_owned())),
        ("payload".to_owned(), AttributeValue::S("data".to_owned())),
    ]));
    let selected = ["pk".to_owned(), "missing".to_owned(), "pk".to_owned()];
    let result = AttributeProjection::from_attributes(&selected).project(&item);
    assert_eq!(result.len(), 1);
    assert_eq!(result.get("pk"), item.get("pk"));
}

#[derive(Clone, Serialize)]
struct MapRangeResponse {
    #[serde(rename = "Items")]
    items: Vec<AttributeMap>,
    #[serde(rename = "Count")]
    count: u32,
    #[serde(rename = "ScannedCount")]
    scanned_count: u32,
}

fn responses(count: usize) -> (MapRangeResponse, QueryResponse, ScanResponse) {
    let mut maps = Vec::with_capacity(count);
    let mut wires = Vec::with_capacity(count);
    for index in 0..count {
        let item = json!({
            "pk": {"S": format!("item#{index}")},
            "payload": {"S": "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"},
            "counter": {"N": "42"}
        });
        let bytes = serde_json::to_vec(&item).unwrap();
        maps.push(serde_json::from_slice(&bytes).unwrap());
        wires.push(WireItem::dynamo_json(bytes));
    }
    let map = MapRangeResponse {
        items: maps,
        count: count as u32,
        scanned_count: count as u32,
    };
    let query = QueryResponse {
        items: Some(wires.clone()),
        count: count as u32,
        scanned_count: count as u32,
        last_evaluated_key: None,
        consumed_capacity: None,
    };
    let scan = ScanResponse {
        items: Some(wires),
        count: count as u32,
        scanned_count: count as u32,
        last_evaluated_key: None,
        consumed_capacity: None,
    };
    (map, query, scan)
}

#[test]
fn query_and_scan_wire_serde_preserve_response_shape() {
    let (map, query, scan) = responses(2);
    let expected = serde_json::to_value(map).unwrap();
    assert_eq!(serde_json::to_value(&query).unwrap(), expected);
    assert_eq!(serde_json::to_value(&scan).unwrap(), expected);
    let query: QueryResponse = serde_json::from_value(expected.clone()).unwrap();
    let scan: ScanResponse = serde_json::from_value(expected).unwrap();
    assert_eq!(query.items.unwrap().len(), 2);
    assert_eq!(scan.items.unwrap().len(), 2);
    assert!(
        serde_json::from_str::<QueryResponse>(r#"{"Items":[42],"Count":1,"ScannedCount":1}"#)
            .is_err()
    );
}
