use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{AttributeValue, from_hashmap, to_hashmap};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct TestStruct {
    name: String,
    age: u32,
    active: bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct TimestampNestedStruct {
    expires_at: crate::TimestampMillis,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct TimestampStruct {
    ttl: crate::TimestampSeconds,
    created_at: crate::TimestampMillis,
    updated_at: Option<crate::TimestampMillis>,
    #[serde(rename = "validAt")]
    valid_at: crate::TimestampMillis,
    nested: TimestampNestedStruct,
}

#[test]
fn to_hashmap_conversion() {
    let test_struct = TestStruct {
        name: "John".to_string(),
        age: 30,
        active: true,
    };

    let result = to_hashmap(&test_struct).unwrap();
    assert_eq!(result.len(), 3);

    match result.get("name").unwrap() {
        AttributeValue::S(s) => assert_eq!(s, "John"),
        _ => panic!("Expected string attribute"),
    }

    match result.get("age").unwrap() {
        AttributeValue::N(n) => assert_eq!(n, "30"),
        _ => panic!("Expected number attribute"),
    }

    match result.get("active").unwrap() {
        AttributeValue::BOOL(b) => assert!(*b),
        _ => panic!("Expected boolean attribute"),
    }
}

#[test]
fn from_hashmap_conversion() {
    let mut map = HashMap::new();
    map.insert("name".to_string(), AttributeValue::S("Jane".to_string()));
    map.insert("age".to_string(), AttributeValue::N("25".to_string()));
    map.insert("active".to_string(), AttributeValue::BOOL(false));

    let result: TestStruct = from_hashmap(map).unwrap();
    assert_eq!(result.name, "Jane");
    assert_eq!(result.age, 25);
    assert!(!result.active);
}

#[test]
fn round_trip_conversion() {
    let original = TestStruct {
        name: "Alice".to_string(),
        age: 42,
        active: true,
    };

    let hashmap = to_hashmap(&original.clone()).unwrap();
    let converted_back: TestStruct = from_hashmap(hashmap).unwrap();

    assert_eq!(original, converted_back);
}

#[test]
fn to_plain_json() {
    // Create a HashMap with various AttributeValue types
    let mut map = HashMap::new();
    map.insert("name".to_string(), AttributeValue::S("John".to_string()));
    map.insert("age".to_string(), AttributeValue::N("30".to_string()));
    map.insert("active".to_string(), AttributeValue::BOOL(true));
    map.insert(
        "tags".to_string(),
        AttributeValue::SS(vec!["developer".to_string(), "rust".to_string()]),
    );

    let attr_value = AttributeValue::M(map);
    let json_string = attr_value.to_plain_json().unwrap();

    // Parse the JSON to verify structure
    let parsed: serde_json::Value = serde_json::from_str(&json_string).unwrap();

    // Verify it's a plain JSON object without DynamoDB structure
    assert_eq!(parsed["name"], "John");
    assert_eq!(parsed["age"], 30); // Should be parsed as number
    assert_eq!(parsed["active"], true);
    assert_eq!(parsed["tags"][0], "developer");
    assert_eq!(parsed["tags"][1], "rust");

    // Verify it's valid JSON
    assert!(serde_json::from_str::<serde_json::Value>(&json_string).is_ok());
}

#[test]
fn to_hashmap_normalizes_timestamp_fields_to_numbers() {
    let created_at = crate::TimestampMillis::from_timestamp(1_700_000_123_456);
    let updated_at = crate::TimestampMillis::from_timestamp(1_700_000_223_456);
    let ttl = crate::TimestampSeconds::from_timestamp(1_700_000_300);
    let valid_at = crate::TimestampMillis::from_timestamp(1_700_000_323_456);
    let nested_expires_at = crate::TimestampMillis::from_timestamp(1_700_000_423_456);
    let fixture = TimestampStruct {
        ttl,
        created_at,
        updated_at: Some(updated_at),
        valid_at,
        nested: TimestampNestedStruct {
            expires_at: nested_expires_at,
        },
    };

    let result = to_hashmap(&fixture).expect("convert timestamp fixture");

    assert!(matches!(
        result.get("ttl"),
        Some(AttributeValue::N(value)) if value == &ttl.as_seconds().to_string()
    ));
    assert!(matches!(
        result.get("created_at"),
        Some(AttributeValue::N(value)) if value == &(*created_at).to_string()
    ));
    assert!(matches!(
        result.get("updated_at"),
        Some(AttributeValue::N(value)) if value == &(*updated_at).to_string()
    ));
    assert!(matches!(
        result.get("validAt"),
        Some(AttributeValue::N(value)) if value == &(*valid_at).to_string()
    ));

    match result.get("nested") {
        Some(AttributeValue::M(nested)) => {
            assert!(matches!(
                nested.get("expires_at"),
                Some(AttributeValue::N(value))
                    if value == &(*nested_expires_at).to_string()
            ));
        }
        _ => panic!("Expected nested map attribute"),
    }
}

#[test]
fn from_hashmap_decodes_numeric_timestamps_for_datetime_fields() {
    let created_at = crate::TimestampMillis::from_timestamp(1_700_001_123_456);
    let updated_at = crate::TimestampMillis::from_timestamp(1_700_001_223_456);
    let ttl = crate::TimestampSeconds::from_timestamp(1_700_001_300);
    let valid_at = crate::TimestampMillis::from_timestamp(1_700_001_323_456);
    let nested_expires_at = crate::TimestampMillis::from_timestamp(1_700_001_423_456);

    let mut nested = HashMap::new();
    nested.insert(
        "expires_at".to_string(),
        AttributeValue::N((*nested_expires_at).to_string()),
    );

    let mut map = HashMap::new();
    map.insert(
        "ttl".to_string(),
        AttributeValue::N(ttl.as_seconds().to_string()),
    );
    map.insert(
        "created_at".to_string(),
        AttributeValue::N((*created_at).to_string()),
    );
    map.insert(
        "updated_at".to_string(),
        AttributeValue::N((*updated_at).to_string()),
    );
    map.insert(
        "validAt".to_string(),
        AttributeValue::N((*valid_at).to_string()),
    );
    map.insert("nested".to_string(), AttributeValue::M(nested));

    let decoded: TimestampStruct = from_hashmap(map).expect("decode timestamp fixture");
    assert_eq!(
        decoded,
        TimestampStruct {
            ttl,
            created_at,
            updated_at: Some(updated_at),
            valid_at,
            nested: TimestampNestedStruct {
                expires_at: nested_expires_at,
            },
        }
    );
}
