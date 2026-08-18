use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use alloc_counter::AllocationGuard;

use crate::{
    AttributeDefinition, AttributeValue, KeyAttributeType, KeyAttributes, KeySchemaElement,
    KeyType, StorageEnum, StorageResult, StoredTableInfo, TableName, TableStatus, TimestampMillis,
    context::WrappedError as _, normalize_dynamodb_number_for_write,
    preflight_transact_put_item_key_with_table_info, preflight_transact_write_key_with_table_info,
    transact_put_item_key_fingerprint, validate_no_duplicate_transact_item_keys,
};

const FINGERPRINT_ITERATIONS: usize = 10_000;

#[test]
fn transaction_key_preflight_normalizes_equivalent_number_fingerprints() {
    let table_info = number_table_info();
    let put_preflight = preflight_transact_put_item_key_with_table_info(
        &table_info,
        &HashMap::from([
            ("pk".to_string(), AttributeValue::N("1E-130".to_string())),
            ("sk".to_string(), AttributeValue::N("1".to_string())),
        ]),
    )
    .expect("put preflight");
    let delete_preflight = preflight_transact_write_key_with_table_info(
        &table_info,
        &KeyAttributes::from(HashMap::from([
            (
                "pk".to_string(),
                AttributeValue::N(
                    "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001"
                        .to_string(),
                ),
            ),
            ("sk".to_string(), AttributeValue::N("1".to_string())),
        ])),
    )
    .expect("delete preflight");

    let error = validate_no_duplicate_transact_item_keys(&[put_preflight, delete_preflight])
        .expect_err("equivalent number keys should be duplicate transaction targets");

    let StorageEnum::Validation { message } = error.to_enum() else {
        panic!("expected validation error, got {error:?}");
    };
    assert_eq!(
        message,
        "Transaction request cannot include multiple operations on one item"
    );
}

#[test]
fn transaction_put_key_fingerprint_avoids_temporary_key_map_and_json_allocations_tests() {
    let table_info = number_table_info();
    let item = fingerprint_item();
    let legacy = measure_fingerprint_allocations(
        "transaction_key_fingerprint_legacy_json",
        "legacy",
        || legacy_transact_put_item_key_fingerprint(&table_info, &item),
    );
    let optimized = measure_fingerprint_allocations(
        "transaction_key_fingerprint_schema_ordered",
        "optimized",
        || transact_put_item_key_fingerprint(&table_info, &item),
    );

    alloc_counter::emit_report(&legacy);
    alloc_counter::emit_report(&optimized);

    assert!(
        optimized.allocation_count <= legacy.allocation_count,
        "expected schema-ordered fingerprint not to allocate more often, legacy={} optimized={}",
        legacy.allocation_count,
        optimized.allocation_count
    );
    assert!(
        optimized.allocated_bytes < legacy.allocated_bytes,
        "expected schema-ordered fingerprint to allocate fewer bytes, legacy={} optimized={}",
        legacy.allocated_bytes,
        optimized.allocated_bytes
    );
}

#[test]
#[ignore = "manual runtime perf probe; run with --ignored --nocapture before/after transaction \
            fingerprint changes"]
fn transaction_key_fingerprint_runtime_perf_probe() {
    let table_info = number_table_info();
    let item = fingerprint_item();
    for measurement in 1..=3 {
        let legacy = measure_fingerprint_runtime(|| {
            legacy_transact_put_item_key_fingerprint(&table_info, &item)
        });
        let optimized =
            measure_fingerprint_runtime(|| transact_put_item_key_fingerprint(&table_info, &item));
        println!(
            "transaction_key_fingerprint measurement={measurement} legacy_ms={:.3} \
             optimized_ms={:.3} legacy_ns_per_iter={:.2} optimized_ns_per_iter={:.2}",
            legacy.as_secs_f64() * 1_000.0,
            optimized.as_secs_f64() * 1_000.0,
            legacy.as_nanos() as f64 / FINGERPRINT_ITERATIONS as f64,
            optimized.as_nanos() as f64 / FINGERPRINT_ITERATIONS as f64
        );
    }
}

fn measure_fingerprint_allocations(
    test_name: &'static str,
    label: &'static str,
    mut fingerprint: impl FnMut() -> StorageResult<String>,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(module_path!(), test_name, file!(), line!(), Some(label));
    for _ in 0..FINGERPRINT_ITERATIONS {
        std::hint::black_box(fingerprint().expect("fingerprint key"));
    }
    guard.finish()
}

fn measure_fingerprint_runtime(mut fingerprint: impl FnMut() -> StorageResult<String>) -> Duration {
    let started = Instant::now();
    for _ in 0..FINGERPRINT_ITERATIONS {
        std::hint::black_box(fingerprint().expect("fingerprint key"));
    }
    started.elapsed()
}

fn legacy_transact_put_item_key_fingerprint(
    table_info: &StoredTableInfo,
    item: &HashMap<String, AttributeValue>,
) -> StorageResult<String> {
    let mut key = KeyAttributes::with_capacity(table_info.key_schema.len());
    for key_schema in &table_info.key_schema {
        let value = item.get(&key_schema.attribute_name).expect("key attribute");
        key.insert(
            key_schema.attribute_name.clone(),
            match value {
                AttributeValue::N(number) => {
                    AttributeValue::N(normalize_dynamodb_number_for_write(number).into_owned())
                }
                value => value.clone(),
            },
        );
    }
    let key_json = key.canonical_dynamo_json().expect("canonical key json");
    Ok(format!(
        "{}\t{}",
        table_info.table_name.dynamodb_resource_name(),
        key_json
    ))
}

fn fingerprint_item() -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_string(), AttributeValue::N("1.23E4".to_string())),
        ("sk".to_string(), AttributeValue::N("9.87E-2".to_string())),
        ("payload".to_string(), AttributeValue::S("x".repeat(1024))),
    ])
}

fn number_table_info() -> StoredTableInfo {
    StoredTableInfo {
        table_name: TableName::new("transaction_runtime_number_keys"),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::now(),
        max_indexers: crate::MaxIndexers::ZERO,
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::N,
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
        table_stream_duration: crate::StreamRetentionDuration::default(),
        default_item_stream_duration: crate::StreamRetentionDuration::default(),
        deletion_protection_enabled: false,
    }
}
