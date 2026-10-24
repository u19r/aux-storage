use alloc_counter::AllocationGuard;
use storage_types::{AttributeValue, KeyAttributes, KeySchemaElement, KeyType, StorageResult};

use super::table_item_split::key_attributes_from_item;

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
    let actual = key_attributes_from_item(&item, &schema).expect("key-only keys");
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
    assert!(key_attributes_from_item(&missing, &schema).is_err());

    let empty = std::collections::HashMap::new();
    assert!(
        key_attributes_from_item(&empty, &[])
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
            return Err(storage_types::StorageError::invalid_or_missing_key());
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
        std::hint::black_box(key_attributes_from_item(item, schema).expect("keys"));
    }
    guard.finish()
}
