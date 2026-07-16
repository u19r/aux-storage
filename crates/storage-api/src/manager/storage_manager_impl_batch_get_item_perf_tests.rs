use std::{
    collections::{HashMap, HashSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
};

use alloc_counter::AllocationGuard;
use storage_types::{
    AttributeDefinition, AttributeValue, KeyAttributeType, KeyAttributes, KeySchemaElement, KeyType,
    StoredTableInfo, TableName, TableStatus, TimestampMillis,
};

use super::storage_manager_impl_batch_get_item::BatchGetKeyIdentity;

const ITERATIONS: usize = 10_000;

#[test]
fn batch_get_typed_key_fingerprint_avoids_canonical_json_allocations() {
    let table_info = table_info();
    let key = KeyAttributes::from(HashMap::from([
        ("pk".to_string(), AttributeValue::S("tenant-123".to_string())),
        ("sk".to_string(), AttributeValue::N("12300".to_string())),
    ]));

    let legacy = AllocationGuard::start(
        module_path!(),
        "batch_get_key_fingerprint_legacy_json",
        file!(),
        line!(),
        Some("legacy"),
    );
    for _ in 0..ITERATIONS {
        std::hint::black_box(key.canonical_dynamo_json().expect("canonical JSON"));
    }
    let legacy = legacy.finish();

    let typed = AllocationGuard::start(
        module_path!(),
        "batch_get_key_fingerprint_typed",
        file!(),
        line!(),
        Some("typed"),
    );
    for _ in 0..ITERATIONS {
        let mut hasher = DefaultHasher::new();
        BatchGetKeyIdentity::new(&table_info.key_schema, &key).hash(&mut hasher);
        std::hint::black_box(hasher.finish());
    }
    let typed = typed.finish();

    alloc_counter::emit_report(&legacy);
    alloc_counter::emit_report(&typed);
    assert!(
        typed.allocation_count < legacy.allocation_count,
        "typed fingerprint allocations={} legacy={}",
        typed.allocation_count,
        legacy.allocation_count
    );
    assert!(
        typed.allocated_bytes < legacy.allocated_bytes,
        "typed fingerprint bytes={} legacy={}",
        typed.allocated_bytes,
        legacy.allocated_bytes
    );
}

#[test]
fn batch_get_key_identity_normalizes_equivalent_numbers() {
    let table_info = table_info();
    let first = KeyAttributes::from(HashMap::from([
        ("pk".to_string(), AttributeValue::S("tenant-123".to_string())),
        ("sk".to_string(), AttributeValue::N("1.23E4".to_string())),
    ]));
    let second = KeyAttributes::from(HashMap::from([
        ("pk".to_string(), AttributeValue::S("tenant-123".to_string())),
        ("sk".to_string(), AttributeValue::N("12300".to_string())),
    ]));
    let mut seen = HashSet::new();

    assert!(seen.insert(BatchGetKeyIdentity::new(&table_info.key_schema, &first)));
    assert!(!seen.insert(BatchGetKeyIdentity::new(
        &table_info.key_schema,
        &second
    )));
}

fn table_info() -> StoredTableInfo {
    StoredTableInfo {
        table_name: TableName::new("batch_get_fingerprint"),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::from_timestamp(0),
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::N,
            },
        ],
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        global_secondary_indexes: None,
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
        table_stream_duration: storage_types::StreamRetentionDuration::default(),
        default_item_stream_duration: storage_types::StreamRetentionDuration::default(),
        deletion_protection_enabled: false,
    }
}
