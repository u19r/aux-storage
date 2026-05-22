use std::collections::HashMap;

use serde_json::json;

use crate::{AttributeMap, AttributeValue, AttributeValueLookup};

#[test]
fn attribute_map_preserves_insertion_order_and_replaces_existing_names() {
    let mut map = AttributeMap::new();

    map.insert("id", AttributeValue::S("order-1".to_string()));
    map.insert("status", AttributeValue::S("pending".to_string()));
    map.insert("id", AttributeValue::S("order-2".to_string()));

    let entries = map.iter().collect::<Vec<_>>();
    assert_eq!(entries.len(), 2);
    assert_eq!(
        entries[0],
        ("id", &AttributeValue::S("order-2".to_string()))
    );
    assert_eq!(
        entries[1],
        ("status", &AttributeValue::S("pending".to_string()))
    );
    assert!(map.contains_key("id"));
    assert!(!map.is_empty());
}

#[test]
fn attribute_map_serializes_as_dynamodb_attribute_object() {
    let map = AttributeMap::from_iter([
        ("id".to_string(), AttributeValue::S("order-1".to_string())),
        ("attempts".to_string(), AttributeValue::N("3".to_string())),
    ]);

    let encoded = serde_json::to_value(&map).expect("attribute map should serialize");
    let decoded: AttributeMap =
        serde_json::from_value(encoded.clone()).expect("attribute map should deserialize");

    assert_eq!(
        encoded,
        json!({
            "id": { "S": "order-1" },
            "attempts": { "N": "3" }
        })
    );
    assert_eq!(
        decoded.get("id"),
        Some(&AttributeValue::S("order-1".to_string()))
    );
    assert_eq!(decoded.attribute_count(), 2);
}

#[test]
fn attribute_map_converts_to_and_from_hashmap_at_api_boundaries() {
    let source = HashMap::from([
        ("id".to_string(), AttributeValue::S("order-1".to_string())),
        ("active".to_string(), AttributeValue::BOOL(true)),
    ]);

    let map = AttributeMap::from(source.clone());
    let cloned = map.to_hashmap();
    let consumed: HashMap<String, AttributeValue> = map.into();

    assert_eq!(cloned, source);
    assert_eq!(consumed, source);
}
