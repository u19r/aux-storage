use serde_json::json;
use storage_types::{AttributeValue, PutItemRequest, TableName};

#[test]
fn attribute_value_string_serialize() {
    let attr = AttributeValue::S("hello world".to_string());
    let json = serde_json::to_value(&attr).unwrap();
    assert_eq!(json, json!({"S": "hello world"}));
}

#[test]
fn attribute_value_string_deserialize() {
    let json = json!({"S": "hello world"});
    let attr: AttributeValue = serde_json::from_value(json).unwrap();
    assert_eq!(attr, AttributeValue::S("hello world".to_string()));
}

#[test]
fn attribute_value_number_serialize() {
    let attr = AttributeValue::N("123.45".to_string());
    let json = serde_json::to_value(&attr).unwrap();
    assert_eq!(json, json!({"N": "123.45"}));
}

#[test]
fn attribute_value_number_deserialize() {
    let json = json!({"N": "123.45"});
    let attr: AttributeValue = serde_json::from_value(json).unwrap();
    assert_eq!(attr, AttributeValue::N("123.45".to_string()));
}

#[test]
fn attribute_value_binary_serialize() {
    let attr = AttributeValue::B("aGVsbG8gd29ybGQ=".to_string());
    let json = serde_json::to_value(&attr).unwrap();
    assert_eq!(json, json!({"B": "aGVsbG8gd29ybGQ="}));
}

#[test]
fn attribute_value_binary_deserialize() {
    let json = json!({"B": "aGVsbG8gd29ybGQ="});
    let attr: AttributeValue = serde_json::from_value(json).unwrap();
    assert_eq!(attr, AttributeValue::B("aGVsbG8gd29ybGQ=".to_string()));
}

#[test]
fn attribute_value_string_set_serialize() {
    let attr = AttributeValue::SS(vec![
        "apple".to_string(),
        "banana".to_string(),
        "cherry".to_string(),
    ]);
    let json = serde_json::to_value(&attr).unwrap();
    assert_eq!(json, json!({"SS": ["apple", "banana", "cherry"]}));
}

#[test]
fn attribute_value_string_set_deserialize() {
    let json = json!({"SS": ["apple", "banana", "cherry"]});
    let attr: AttributeValue = serde_json::from_value(json).unwrap();
    assert_eq!(
        attr,
        AttributeValue::SS(vec![
            "apple".to_string(),
            "banana".to_string(),
            "cherry".to_string()
        ])
    );
}

#[test]
fn attribute_value_number_set_serialize() {
    let attr = AttributeValue::NS(vec!["1".to_string(), "2.5".to_string(), "100".to_string()]);
    let json = serde_json::to_value(&attr).unwrap();
    assert_eq!(json, json!({"NS": ["1", "2.5", "100"]}));
}

#[test]
fn attribute_value_number_set_deserialize() {
    let json = json!({"NS": ["1", "2.5", "100"]});
    let attr: AttributeValue = serde_json::from_value(json).unwrap();
    assert_eq!(
        attr,
        AttributeValue::NS(vec!["1".to_string(), "2.5".to_string(), "100".to_string()])
    );
}

#[test]
fn attribute_value_binary_set_serialize() {
    let attr = AttributeValue::BS(vec!["YWJjZGVm".to_string(), "Z2hpams=".to_string()]);
    let json = serde_json::to_value(&attr).unwrap();
    assert_eq!(json, json!({"BS": ["YWJjZGVm", "Z2hpams="]}));
}

#[test]
fn attribute_value_binary_set_deserialize() {
    let json = json!({"BS": ["YWJjZGVm", "Z2hpams="]});
    let attr: AttributeValue = serde_json::from_value(json).unwrap();
    assert_eq!(
        attr,
        AttributeValue::BS(vec!["YWJjZGVm".to_string(), "Z2hpams=".to_string()])
    );
}

#[test]
fn attribute_value_bool_true_serialize() {
    let attr = AttributeValue::BOOL(true);
    let json = serde_json::to_value(&attr).unwrap();
    assert_eq!(json, json!({"BOOL": true}));
}

#[test]
fn attribute_value_bool_true_deserialize() {
    let json = json!({"BOOL": true});
    let attr: AttributeValue = serde_json::from_value(json).unwrap();
    assert_eq!(attr, AttributeValue::BOOL(true));
}

#[test]
fn attribute_value_bool_false_serialize() {
    let attr = AttributeValue::BOOL(false);
    let json = serde_json::to_value(&attr).unwrap();
    assert_eq!(json, json!({"BOOL": false}));
}

#[test]
fn attribute_value_bool_false_deserialize() {
    let json = json!({"BOOL": false});
    let attr: AttributeValue = serde_json::from_value(json).unwrap();
    assert_eq!(attr, AttributeValue::BOOL(false));
}

#[test]
fn attribute_value_null_serialize() {
    let attr = AttributeValue::NULL(true);
    let json = serde_json::to_value(&attr).unwrap();
    assert_eq!(json, json!({"NULL": true}));
}

#[test]
fn attribute_value_null_deserialize() {
    let json = json!({"NULL": true});
    let attr: AttributeValue = serde_json::from_value(json).unwrap();
    assert_eq!(attr, AttributeValue::NULL(true));
}

#[test]
fn attribute_value_empty_string_set() {
    let attr = AttributeValue::SS(vec![]);
    let json = serde_json::to_value(&attr).unwrap();
    assert_eq!(json, json!({"SS": []}));

    let deserialized: AttributeValue = serde_json::from_value(json).unwrap();
    assert_eq!(deserialized, attr);
}

#[test]
fn attribute_value_empty_number_set() {
    let attr = AttributeValue::NS(vec![]);
    let json = serde_json::to_value(&attr).unwrap();
    assert_eq!(json, json!({"NS": []}));

    let deserialized: AttributeValue = serde_json::from_value(json).unwrap();
    assert_eq!(deserialized, attr);
}

#[test]
fn attribute_value_empty_binary_set() {
    let attr = AttributeValue::BS(vec![]);
    let json = serde_json::to_value(&attr).unwrap();
    assert_eq!(json, json!({"BS": []}));

    let deserialized: AttributeValue = serde_json::from_value(json).unwrap();
    assert_eq!(deserialized, attr);
}

#[test]
fn attribute_value_special_characters() {
    let attr = AttributeValue::S(
        "Special chars: !@#$%^&*()[]{}|;':\",./<>? Unicode: 你好世界 🌍".to_string(),
    );
    let json = serde_json::to_value(&attr).unwrap();
    let deserialized: AttributeValue = serde_json::from_value(json).unwrap();
    assert_eq!(deserialized, attr);
}

#[test]
fn attribute_value_round_trip_all_types() {
    let test_cases = vec![
        AttributeValue::S("test string".to_string()),
        AttributeValue::N("42.5".to_string()),
        AttributeValue::B("dGVzdA==".to_string()),
        AttributeValue::SS(vec!["a".to_string(), "b".to_string()]),
        AttributeValue::NS(vec!["1".to_string(), "2".to_string()]),
        AttributeValue::BS(vec!["dGVzdA==".to_string(), "YWJjZA==".to_string()]),
        AttributeValue::BOOL(true),
        AttributeValue::BOOL(false),
        AttributeValue::NULL(true),
    ];

    for original in test_cases {
        let json = serde_json::to_value(&original).unwrap();
        let deserialized: AttributeValue = serde_json::from_value(json).unwrap();
        assert_eq!(deserialized, original);
    }
}

#[test]
fn attribute_value_invalid_empty_object() {
    let json = json!({});
    let result: Result<AttributeValue, _> = serde_json::from_value(json);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("AttributeValue cannot be empty")
    );
}

#[test]
fn attribute_value_invalid_multiple_fields() {
    let json = json!({"S": "test", "N": "123"});
    let result: Result<AttributeValue, _> = serde_json::from_value(json);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("AttributeValue must have exactly one field")
    );
}

#[test]
fn attribute_value_invalid_unknown_field() {
    let json = json!({"INVALID": "test"});
    let result: Result<AttributeValue, _> = serde_json::from_value(json);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("unknown field"));
}

#[test]
fn attribute_value_complex_item_serialization() {
    let mut item = std::collections::HashMap::new();
    item.insert("id".to_string(), AttributeValue::S("user123".to_string()));
    item.insert(
        "name".to_string(),
        AttributeValue::S("John Doe".to_string()),
    );
    item.insert("age".to_string(), AttributeValue::N("30".to_string()));
    item.insert("active".to_string(), AttributeValue::BOOL(true));
    item.insert(
        "tags".to_string(),
        AttributeValue::SS(vec!["admin".to_string(), "user".to_string()]),
    );
    item.insert(
        "scores".to_string(),
        AttributeValue::NS(vec!["100".to_string(), "95".to_string()]),
    );

    let json = serde_json::to_value(&item).unwrap();
    let expected = json!({
        "id": {"S": "user123"},
        "name": {"S": "John Doe"},
        "age": {"N": "30"},
        "active": {"BOOL": true},
        "tags": {"SS": ["admin", "user"]},
        "scores": {"NS": ["100", "95"]}
    });

    assert_eq!(json, expected);

    let deserialized: std::collections::HashMap<String, AttributeValue> =
        serde_json::from_value(json).unwrap();
    assert_eq!(deserialized, item);
}

#[test]
fn put_item_request_serialization() {
    let mut item = std::collections::HashMap::new();
    item.insert("id".to_string(), AttributeValue::S("test123".to_string()));
    item.insert(
        "data".to_string(),
        AttributeValue::S("test data".to_string()),
    );

    let request = PutItemRequest::new(TableName::new("TestTable"), item);

    let json = serde_json::to_value(&request).unwrap();
    let expected = json!({
        "TableName": "TestTable",
        "Item": {
            "id": {"S": "test123"},
            "data": {"S": "test data"}
        }
    });

    assert_eq!(json, expected);

    let deserialized: PutItemRequest = serde_json::from_value(json).unwrap();
    assert_eq!(deserialized.table_name, request.table_name);
    assert_eq!(deserialized.item, request.item);
}

#[test]
fn attribute_value_list_serialize() {
    let attr = AttributeValue::L(vec![
        AttributeValue::S("item1".to_string()),
        AttributeValue::N("42".to_string()),
        AttributeValue::BOOL(true),
    ]);
    let json = serde_json::to_value(&attr).unwrap();
    let expected = json!({
        "L": [
            {"S": "item1"},
            {"N": "42"},
            {"BOOL": true}
        ]
    });
    assert_eq!(json, expected);
}

#[test]
fn attribute_value_list_deserialize() {
    let json = json!({
        "L": [
            {"S": "item1"},
            {"N": "42"},
            {"BOOL": true}
        ]
    });
    let attr: AttributeValue = serde_json::from_value(json).unwrap();
    let expected = AttributeValue::L(vec![
        AttributeValue::S("item1".to_string()),
        AttributeValue::N("42".to_string()),
        AttributeValue::BOOL(true),
    ]);
    assert_eq!(attr, expected);
}

#[test]
fn attribute_value_map_serialize() {
    let mut map = std::collections::HashMap::new();
    map.insert("key1".to_string(), AttributeValue::S("value1".to_string()));
    map.insert("key2".to_string(), AttributeValue::N("123".to_string()));
    map.insert("key3".to_string(), AttributeValue::BOOL(false));

    let attr = AttributeValue::M(map);
    let json = serde_json::to_value(&attr).unwrap();
    let expected = json!({
        "M": {
            "key1": {"S": "value1"},
            "key2": {"N": "123"},
            "key3": {"BOOL": false}
        }
    });
    assert_eq!(json, expected);
}

#[test]
fn attribute_value_map_deserialize() {
    let json = json!({
        "M": {
            "key1": {"S": "value1"},
            "key2": {"N": "123"},
            "key3": {"BOOL": false}
        }
    });
    let attr: AttributeValue = serde_json::from_value(json).unwrap();

    let mut expected_map = std::collections::HashMap::new();
    expected_map.insert("key1".to_string(), AttributeValue::S("value1".to_string()));
    expected_map.insert("key2".to_string(), AttributeValue::N("123".to_string()));
    expected_map.insert("key3".to_string(), AttributeValue::BOOL(false));

    let expected = AttributeValue::M(expected_map);
    assert_eq!(attr, expected);
}

#[test]
fn attribute_value_nested_list_and_map() {
    // Test nested structures: a list containing a map
    let mut inner_map = std::collections::HashMap::new();
    inner_map.insert(
        "nested_key".to_string(),
        AttributeValue::S("nested_value".to_string()),
    );

    let attr = AttributeValue::L(vec![
        AttributeValue::S("string_item".to_string()),
        AttributeValue::M(inner_map),
        AttributeValue::N("456".to_string()),
    ]);

    let json = serde_json::to_value(&attr).unwrap();
    let expected = json!({
        "L": [
            {"S": "string_item"},
            {"M": {"nested_key": {"S": "nested_value"}}},
            {"N": "456"}
        ]
    });
    assert_eq!(json, expected);

    // Test round-trip
    let deserialized: AttributeValue = serde_json::from_value(json).unwrap();
    assert_eq!(deserialized, attr);
}

#[test]
fn attribute_value_empty_list_and_map() {
    // Test empty list
    let empty_list = AttributeValue::L(vec![]);
    let json = serde_json::to_value(&empty_list).unwrap();
    assert_eq!(json, json!({"L": []}));

    let deserialized: AttributeValue = serde_json::from_value(json).unwrap();
    assert_eq!(deserialized, empty_list);

    // Test empty map
    let empty_map = AttributeValue::M(std::collections::HashMap::new());
    let json = serde_json::to_value(&empty_map).unwrap();
    assert_eq!(json, json!({"M": {}}));

    let deserialized: AttributeValue = serde_json::from_value(json).unwrap();
    assert_eq!(deserialized, empty_map);
}
