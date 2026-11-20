use std::collections::HashMap;

use alloc_counter::AllocationGuard;

use crate::{
    AttributeValue, KeyAttributes, KeySchemaElement, KeyType, MissingKeyAttribute, StorageResult,
    canonicalize_top_level_number_value, schema_key_values,
};

#[test]
fn given_number_set_when_canonicalized_then_valid_members_change_and_invalid_ones_survive() {
    let mut value = AttributeValue::NS(vec![
        "1E2".to_owned(),
        "2.00".to_owned(),
        "not-a-number".to_owned(),
    ]);

    canonicalize_top_level_number_value(&mut value);

    assert_eq!(
        value,
        AttributeValue::NS(vec![
            "100".to_owned(),
            "2".to_owned(),
            "not-a-number".to_owned(),
        ])
    );
}

#[test]
fn given_same_keys_inserted_in_reverse_order_when_compared_then_keys_are_equal() {
    let forward = KeyAttributes::from([
        ("pk".to_owned(), AttributeValue::S("partition".to_owned())),
        ("sk".to_owned(), AttributeValue::S("sort".to_owned())),
    ]);
    let reverse = KeyAttributes::from([
        ("sk".to_owned(), AttributeValue::S("sort".to_owned())),
        ("pk".to_owned(), AttributeValue::S("partition".to_owned())),
    ]);

    assert_eq!(forward, reverse);
}

#[test]
fn given_hash_map_keys_when_canonically_serialized_and_deserialized_then_keys_are_equal() {
    let from_hash_map = KeyAttributes::from(HashMap::from([
        ("pk".to_owned(), AttributeValue::S("partition".to_owned())),
        ("sk".to_owned(), AttributeValue::S("sort".to_owned())),
    ]));
    let canonical = from_hash_map
        .canonical_dynamo_json()
        .expect("canonical key serialization");
    let round_trip: KeyAttributes =
        serde_json::from_str(&canonical).expect("canonical key deserialization");

    assert_eq!(from_hash_map, round_trip);
}

#[test]
fn given_equivalent_numeric_key_forms_when_canonically_serialized_then_json_is_identical() {
    let scientific =
        KeyAttributes::from([("pk".to_owned(), AttributeValue::N("1E-130".to_owned()))]);
    let expanded = KeyAttributes::from([(
        "pk".to_owned(),
        AttributeValue::N(
            "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001".to_owned(),
        ),
    )]);

    assert_eq!(
        scientific.canonical_dynamo_json().unwrap(),
        expanded.canonical_dynamo_json().unwrap()
    );
}

#[test]
fn given_mixed_keys_when_canonically_serialized_then_bytes_and_errors_are_preserved() {
    let keys = KeyAttributes::from([
        ("sk".to_owned(), AttributeValue::N("1.0".to_owned())),
        ("pk".to_owned(), AttributeValue::S("a\"b".to_owned())),
    ]);
    assert_eq!(
        keys.canonical_dynamo_json().expect("canonical key"),
        r#"{"pk":{"S":"a\"b"},"sk":{"N":"1"}}"#
    );
    let invalid = KeyAttributes::from([("pk".to_owned(), AttributeValue::N("NaN".to_owned()))]);
    assert!(invalid.canonical_dynamo_json().is_err());
}

#[test]
fn given_different_key_name_value_or_cardinality_when_compared_then_keys_are_not_equal() {
    let base = KeyAttributes::from([("pk".to_owned(), AttributeValue::S("partition".to_owned()))]);
    let different_name = KeyAttributes::from([(
        "other".to_owned(),
        AttributeValue::S("partition".to_owned()),
    )]);
    let different_value =
        KeyAttributes::from([("pk".to_owned(), AttributeValue::S("different".to_owned()))]);
    let different_cardinality = KeyAttributes::from([
        ("pk".to_owned(), AttributeValue::S("partition".to_owned())),
        ("sk".to_owned(), AttributeValue::S("sort".to_owned())),
    ]);

    assert_ne!(base, different_name);
    assert_ne!(base, different_value);
    assert_ne!(base, different_cardinality);
}

#[test]
fn given_duplicate_key_name_when_inserted_then_value_is_replaced_without_growth() {
    let mut keys = KeyAttributes::new();
    keys.insert("pk", AttributeValue::S("old".to_owned()));
    keys.insert("pk", AttributeValue::S("new".to_owned()));

    assert_eq!(keys.len(), 1);
    assert_eq!(keys.get("pk"), Some(&AttributeValue::S("new".to_owned())));
}

#[test]
fn key_only_extraction_avoids_full_split_clones() {
    let schema = vec![
        KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        },
        KeySchemaElement {
            attribute_name: "sk".to_string(),
            key_type: KeyType::Range,
        },
    ];
    let item = [
        ("pk".to_string(), AttributeValue::S("tenant".to_string())),
        ("sk".to_string(), AttributeValue::S("item".to_string())),
        ("payload_a".to_string(), AttributeValue::S("a".repeat(256))),
        ("payload_b".to_string(), AttributeValue::S("b".repeat(256))),
        ("payload_c".to_string(), AttributeValue::S("c".repeat(256))),
    ]
    .into_iter()
    .collect::<std::collections::HashMap<_, _>>();

    let expected = legacy_full_split_key_attributes(&item, &schema).expect("legacy keys");
    let actual = KeyAttributes::from_schema(&schema, &item).expect("key-only keys");
    assert_eq!(actual, expected);

    let baseline = measure_legacy(&item, &schema);
    let current = measure_current(&item, &schema);
    let _ = alloc_counter::emit_report(&baseline);
    let _ = alloc_counter::emit_report(&current);
    assert!(
        current.allocation_count < baseline.allocation_count,
        "expected key-only extraction to allocate less often, baseline={} current={}",
        baseline.allocation_count,
        current.allocation_count
    );
    assert!(
        current.allocated_bytes < baseline.allocated_bytes,
        "expected key-only extraction to allocate fewer bytes, baseline={} current={}",
        baseline.allocated_bytes,
        current.allocated_bytes
    );
}

#[test]
fn key_only_extraction_preserves_missing_and_empty_schema_cases() {
    let schema = [KeySchemaElement {
        attribute_name: "pk".to_string(),
        key_type: KeyType::Hash,
    }];
    let missing = std::collections::HashMap::from([(
        "other".to_string(),
        AttributeValue::S("value".to_string()),
    )]);
    assert_eq!(
        KeyAttributes::from_schema(&schema, &missing),
        Err(MissingKeyAttribute { name: "pk" })
    );

    let empty = std::collections::HashMap::<String, AttributeValue>::new();
    assert!(
        KeyAttributes::from_schema(&[], &empty)
            .expect("empty schema")
            .is_empty()
    );
}

fn legacy_full_split_key_attributes(
    item: &std::collections::HashMap<String, AttributeValue>,
    key_schema: &[KeySchemaElement],
) -> StorageResult<KeyAttributes> {
    let mut key_attributes = KeyAttributes::with_capacity(key_schema.len());
    let mut non_key_attributes =
        std::collections::HashMap::with_capacity(item.len().saturating_sub(key_schema.len()));
    for (name, value) in item {
        if key_schema.iter().any(|key| key.attribute_name == *name) {
            key_attributes.insert(name.clone(), value.clone());
        } else {
            non_key_attributes.insert(name.clone(), value.clone());
        }
    }
    for key in key_schema {
        if !key_attributes.contains_key(&key.attribute_name) {
            return Err(crate::StorageError::invalid_or_missing_key());
        }
    }
    std::hint::black_box(non_key_attributes);
    Ok(key_attributes)
}

fn measure_legacy(
    item: &std::collections::HashMap<String, AttributeValue>,
    schema: &[KeySchemaElement],
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "key_only_extraction_full_split_baseline",
        file!(),
        line!(),
        Some("scan_all_attributes"),
    );
    for _ in 0..256 {
        std::hint::black_box(legacy_full_split_key_attributes(item, schema).expect("keys"));
    }
    guard.finish()
}

fn measure_current(
    item: &std::collections::HashMap<String, AttributeValue>,
    schema: &[KeySchemaElement],
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "key_only_extraction_schema_direct_current",
        file!(),
        line!(),
        Some("schema_direct_lookup"),
    );
    for _ in 0..256 {
        std::hint::black_box(KeyAttributes::from_schema(schema, item).expect("keys"));
    }
    guard.finish()
}

fn element(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_string(),
        key_type,
    }
}

#[test]
fn from_schema_reports_first_missing_in_schema_order() {
    let schema = [element("sk", KeyType::Range), element("pk", KeyType::Hash)];
    let item = HashMap::<String, AttributeValue>::new();
    assert_eq!(
        KeyAttributes::from_schema(&schema, &item),
        Err(MissingKeyAttribute { name: "sk" })
    );
}

#[test]
fn schema_key_values_yields_schema_order() {
    let schema = [element("sk", KeyType::Range), element("pk", KeyType::Hash)];
    let item = HashMap::from([
        ("pk".to_string(), AttributeValue::S("p".to_string())),
        ("sk".to_string(), AttributeValue::S("s".to_string())),
    ]);
    let names = schema_key_values(&schema, &item)
        .map(|entry| entry.expect("present").0.attribute_name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(names, ["sk", "pk"]);
}
