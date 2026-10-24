use std::{collections::HashMap, time::Instant};

use alloc_counter::AllocationGuard;
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

fn legacy_from_hashmap(value: HashMap<String, AttributeValue>) -> AttributeMap {
    value.into_iter().collect()
}

fn allocation_fixture() -> HashMap<String, AttributeValue> {
    (0..1_000)
        .map(|index| (format!("field-{index}"), AttributeValue::S("v".repeat(100))))
        .collect()
}

fn measure_conversion(
    label: &'static str,
    convert: fn(HashMap<String, AttributeValue>) -> AttributeMap,
) -> alloc_counter::AllocationReport<'static> {
    let source = allocation_fixture();
    let guard = AllocationGuard::start(
        module_path!(),
        "attribute_map_from_hashmap_allocation_profile_tests",
        file!(),
        line!(),
        Some(label),
    );
    let converted = convert(source.clone());
    std::hint::black_box(converted);
    guard.finish()
}

#[test]
fn attribute_map_from_hashmap_preserves_unique_entries_without_duplicate_scan() {
    let source = allocation_fixture();
    let direct = AttributeMap::from(source.clone());
    let legacy = legacy_from_hashmap(source);
    assert_eq!(direct.len(), legacy.len());
    for (name, value) in direct.iter() {
        assert_eq!(legacy.get(name), Some(value));
    }
}

#[test]
#[ignore = "manual allocation profile; run in an isolated process"]
fn attribute_map_from_hashmap_allocation_profile() {
    let legacy = measure_conversion("legacy_insert_scan", legacy_from_hashmap);
    let direct = measure_conversion("direct_unique_entries", AttributeMap::from);
    let _ = alloc_counter::emit_report(&legacy);
    let _ = alloc_counter::emit_report(&direct);
}

fn measure_runtime(
    convert: fn(HashMap<String, AttributeValue>) -> AttributeMap,
    source: &HashMap<String, AttributeValue>,
) -> std::time::Duration {
    let started = Instant::now();
    for _ in 0..32 {
        std::hint::black_box(convert(source.clone()));
    }
    started.elapsed()
}

#[test]
#[ignore = "manual runtime probe for the known-unique HashMap conversion"]
fn attribute_map_from_hashmap_runtime_probe() {
    let source = allocation_fixture();
    let legacy = measure_runtime(legacy_from_hashmap, &source);
    let direct = measure_runtime(AttributeMap::from, &source);
    println!("attribute_map_from_hashmap legacy={legacy:?} direct={direct:?}");
    assert!(direct < legacy, "direct={direct:?} legacy={legacy:?}");
}

#[test]
fn given_borrowed_unique_entries_when_converted_then_own_names_and_preserve_replacements() {
    let converted = {
        let first = "étiquette".to_owned();
        let second = "value".to_owned();
        let mut source = crate::VecMap::new();
        source.insert(first.as_str(), AttributeValue::N("1".to_owned()));
        source.insert(second.as_str(), AttributeValue::BOOL(true));
        source.insert(first.as_str(), AttributeValue::N("2".to_owned()));
        AttributeMap::from(source)
    };
    assert_eq!(converted.len(), 2);
    assert_eq!(
        converted.get("étiquette"),
        Some(&AttributeValue::N("2".to_owned()))
    );
    let encoded = serde_json::to_string(&converted).unwrap();
    let decoded: AttributeMap = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded, converted);
    assert!(AttributeMap::from(crate::VecMap::<&str, AttributeValue>::new()).is_empty());
}
