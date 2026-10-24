use std::collections::{BTreeMap, HashMap};

use alloc_counter::AllocationGuard;
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

#[test]
fn consuming_attribute_value_conversion_matches_borrowed_conversion() {
    let mut nested = HashMap::new();
    nested.insert("number".to_string(), AttributeValue::N("1150".to_string()));
    nested.insert(
        "fallback".to_string(),
        AttributeValue::N("1e-130".to_string()),
    );
    nested.insert("binary".to_string(), AttributeValue::B("AQI=".to_string()));
    let value = AttributeValue::M(nested);

    let expected = value.to_json_value().expect("borrowed conversion");
    let actual = value.into_json_value().expect("consuming conversion");

    assert_eq!(actual, expected);
}

#[test]
fn consuming_attribute_value_conversion_preserves_null_error() {
    let borrowed = AttributeValue::NULL(false).to_json_value();
    let consuming = AttributeValue::NULL(false).into_json_value();

    assert!(matches!(
        borrowed,
        Err(crate::ConversionError::UnsupportedType(message)) if message == "NULL(false)"
    ));
    assert!(matches!(
        consuming,
        Err(crate::ConversionError::UnsupportedType(message)) if message == "NULL(false)"
    ));
}

fn canonical_map_fixture(width: usize) -> HashMap<String, AttributeValue> {
    (0..width)
        .map(|index| {
            let name = match index {
                0 => "field-雪".to_string(),
                1 => "field-quote\n".to_string(),
                _ => format!("field-{:03}", width.saturating_sub(index)),
            };
            let value = match index % 10 {
                0 => AttributeValue::S(format!("text-{index}")),
                1 => AttributeValue::N(if index == 1 {
                    "1e-130".to_string()
                } else if index == 11 {
                    "not-a-number".to_string()
                } else {
                    (index * 17).to_string()
                }),
                2 => AttributeValue::B("AQID".to_string()),
                3 => AttributeValue::SS(vec!["alpha".to_string(), "βeta".to_string()]),
                4 => AttributeValue::NS(vec!["1".to_string(), "2.5".to_string()]),
                5 => AttributeValue::BS(vec!["AQI=".to_string(), "AwQ=".to_string()]),
                6 => AttributeValue::BOOL(index % 2 == 0),
                7 => AttributeValue::NULL(true),
                8 => AttributeValue::L(vec![
                    AttributeValue::S("nested".to_string()),
                    AttributeValue::N("1e-130".to_string()),
                ]),
                9 => AttributeValue::M(HashMap::from([
                    ("zeta".to_string(), AttributeValue::S("last".to_string())),
                    (
                        "alpha".to_string(),
                        AttributeValue::N("not-a-number".to_string()),
                    ),
                ])),
                _ => unreachable!(),
            };
            (name, value)
        })
        .collect()
}

fn legacy_canonical_map_json(map: &HashMap<String, AttributeValue>) -> Result<String, String> {
    let ordered = map
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    serde_json::to_string(&ordered).map_err(|error| error.to_string())
}

fn current_canonical_map_json(map: &HashMap<String, AttributeValue>) -> String {
    crate::canonical_dynamo_map_json(map).expect("canonical map should serialize")
}

#[test]
fn canonical_dynamo_map_borrowed_entries_preserve_bytes_for_supported_shapes() {
    for width in [0, 1, 10, 100] {
        let map = canonical_map_fixture(width);
        let expected = legacy_canonical_map_json(&map);
        let actual = crate::canonical_dynamo_map_json(&map).map_err(|error| error.to_string());
        assert_eq!(actual, expected, "attribute width {width}");
    }
}

fn measure_canonical_map_allocations(
    label: &'static str,
    map: &HashMap<String, AttributeValue>,
    encode: fn(&HashMap<String, AttributeValue>) -> String,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "canonical_dynamo_map_allocation_profile",
        file!(),
        line!(),
        Some(label),
    );
    for _ in 0..128 {
        std::hint::black_box(encode(std::hint::black_box(map)));
    }
    guard.finish()
}

#[test]
#[ignore = "manual paired allocation probe; run in an isolated process"]
fn canonical_dynamo_map_borrowed_entries_allocation_profile() {
    for width in [1, 10, 100] {
        let map = canonical_map_fixture(width);
        let legacy = measure_canonical_map_allocations("legacy_clone_btree", &map, |map| {
            legacy_canonical_map_json(map).expect("legacy map should serialize")
        });
        let current = measure_canonical_map_allocations(
            "borrowed_sorted_entries",
            &map,
            current_canonical_map_json,
        );
        let _ = alloc_counter::emit_report(&legacy);
        let _ = alloc_counter::emit_report(&current);
        println!(
            "canonical_dynamo_map width={width} legacy_allocations={} current_allocations={} \
             legacy_bytes={} current_bytes={}",
            legacy.allocation_count / 128,
            current.allocation_count / 128,
            legacy.allocated_bytes / 128,
            current.allocated_bytes / 128,
        );
        assert!(current.allocation_count < legacy.allocation_count);
        assert!(current.allocated_bytes < legacy.allocated_bytes);
    }
}
