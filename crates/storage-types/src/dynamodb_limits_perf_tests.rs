use std::time::{Duration, Instant};

use alloc_counter::AllocationGuard;

use crate::{
    AttributeValue, KeyAttributes, KeySchemaElement, KeyType,
    validate_key_attribute_value_for_schema, validate_key_attributes_for_schema,
};

const ITERATIONS: usize = 30_000;

fn schemas() -> [KeySchemaElement; 3] {
    [
        KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        },
        KeySchemaElement {
            attribute_name: "nkey".to_string(),
            key_type: KeyType::Range,
        },
        KeySchemaElement {
            attribute_name: "bkey".to_string(),
            key_type: KeyType::Range,
        },
    ]
}

fn key_values() -> [(String, AttributeValue); 3] {
    [
        ("pk".to_string(), AttributeValue::S("k".repeat(100))),
        (
            "nkey".to_string(),
            AttributeValue::N("12345678901234567890123456789012345678".to_string()),
        ),
        ("bkey".to_string(), AttributeValue::B("b".repeat(100))),
    ]
}

fn measure_direct_validation_allocations() -> alloc_counter::AllocationReport<'static> {
    let schemas = schemas();
    let values = key_values();
    let guard = AllocationGuard::start(
        module_path!(),
        "direct_key_attribute_value_validation_allocation_profile_tests",
        file!(),
        line!(),
        Some("direct"),
    );

    for _ in 0..ITERATIONS {
        for (schema, (_, value)) in schemas.iter().zip(values.iter()) {
            validate_key_attribute_value_for_schema(schema, value).expect("valid key value");
        }
    }

    guard.finish()
}

fn measure_legacy_validation_allocations() -> alloc_counter::AllocationReport<'static> {
    let schemas = schemas();
    let values = key_values();
    let guard = AllocationGuard::start(
        module_path!(),
        "legacy_key_attributes_validation_allocation_profile_tests",
        file!(),
        line!(),
        Some("legacy"),
    );

    for _ in 0..ITERATIONS {
        for (schema, (name, value)) in schemas.iter().zip(values.iter()) {
            let mut key_attributes = KeyAttributes::with_capacity(1);
            key_attributes.insert(name.clone(), value.clone());
            validate_key_attributes_for_schema(std::slice::from_ref(schema), &key_attributes)
                .expect("valid key value");
        }
    }

    guard.finish()
}

fn measure_runtime(
    validate: fn(&[KeySchemaElement; 3], &[(String, AttributeValue); 3]),
) -> Duration {
    let schemas = schemas();
    let values = key_values();
    let started = Instant::now();
    for _ in 0..ITERATIONS {
        validate(&schemas, &values);
    }
    started.elapsed()
}

#[test]
fn direct_key_attribute_value_validation_allocation_profile_tests() {
    let legacy = measure_legacy_validation_allocations();
    let direct = measure_direct_validation_allocations();
    alloc_counter::emit_report(&legacy);
    alloc_counter::emit_report(&direct);
    assert!(direct.allocation_count < legacy.allocation_count);
    assert!(direct.allocated_bytes < legacy.allocated_bytes);
}

#[test]
#[ignore = "manual runtime perf probe; run with --ignored --nocapture --test-threads=1"]
fn direct_key_attribute_value_validation_runtime_perf_probe() {
    let legacy = measure_runtime(|schemas, values| {
        for (schema, (name, value)) in schemas.iter().zip(values.iter()) {
            let mut key_attributes = KeyAttributes::with_capacity(1);
            key_attributes.insert(name.clone(), value.clone());
            validate_key_attributes_for_schema(std::slice::from_ref(schema), &key_attributes)
                .expect("valid key value");
        }
    });
    let direct = measure_runtime(|schemas, values| {
        for (schema, (_, value)) in schemas.iter().zip(values.iter()) {
            validate_key_attribute_value_for_schema(schema, value).expect("valid key value");
        }
    });

    println!(
        "legacy_key_attribute_validation iterations={ITERATIONS} elapsed_ms={:.3} \
         ns_per_iter={:.2}",
        legacy.as_secs_f64() * 1_000.0,
        legacy.as_nanos() as f64 / ITERATIONS as f64
    );
    println!(
        "direct_key_attribute_validation iterations={ITERATIONS} elapsed_ms={:.3} \
         ns_per_iter={:.2}",
        direct.as_secs_f64() * 1_000.0,
        direct.as_nanos() as f64 / ITERATIONS as f64
    );
    assert!(direct.as_nanos() > 0);
}

#[test]
fn realistic_key_values_cover_string_number_and_binary_tests() {
    let schemas = schemas();
    let values = key_values();

    assert!(matches!(values[0].1, AttributeValue::S(_)));
    assert!(matches!(values[1].1, AttributeValue::N(_)));
    assert!(matches!(values[2].1, AttributeValue::B(_)));

    for (schema, (_, value)) in schemas.iter().zip(values.iter()) {
        validate_key_attribute_value_for_schema(schema, value).expect("valid key value");
    }
}
