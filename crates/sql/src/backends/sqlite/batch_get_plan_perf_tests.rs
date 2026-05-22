use std::time::Instant;

use alloc_counter::AllocationGuard;
use storage_types::{AttributeValue, KeyAttributes, KeySchemaElement, KeyType};

use super::provider_read::plan_batch_get_select;

const ITERATIONS: usize = 10_000;
const KEY_COUNT: usize = 25;

fn hash_key_schema() -> Vec<KeySchemaElement> {
    vec![KeySchemaElement {
        attribute_name: "pk".to_string(),
        key_type: KeyType::Hash,
    }]
}

fn hash_range_key_schema() -> Vec<KeySchemaElement> {
    vec![
        KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        },
        KeySchemaElement {
            attribute_name: "sk".to_string(),
            key_type: KeyType::Range,
        },
    ]
}

fn hash_keys() -> Vec<KeyAttributes> {
    (0..KEY_COUNT)
        .map(|index| {
            KeyAttributes::from([(
                "pk".to_string(),
                AttributeValue::S(format!("tenant#{index:04}")),
            )])
        })
        .collect()
}

fn hash_range_keys() -> Vec<KeyAttributes> {
    (0..KEY_COUNT)
        .map(|index| {
            KeyAttributes::from([
                (
                    "pk".to_string(),
                    AttributeValue::S(format!("tenant#{:04}", index % 4)),
                ),
                (
                    "sk".to_string(),
                    AttributeValue::S(format!("item#{index:04}")),
                ),
            ])
        })
        .collect()
}

fn measure_allocations(
    label: &'static str,
    key_schema: &[KeySchemaElement],
    keys: &[KeyAttributes],
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "sqlite_batch_get_select_plan_allocation_profile_tests",
        file!(),
        line!(),
        Some(label),
    );
    for _ in 0..ITERATIONS {
        let plan = plan_batch_get_select("table_BatchGetPlan", key_schema, keys)
            .expect("plan batch get")
            .expect("non-empty plan");
        std::hint::black_box(plan.sql.len() + plan.params.len());
    }
    guard.finish()
}

fn measure_runtime(label: &str, key_schema: &[KeySchemaElement], keys: &[KeyAttributes]) -> f64 {
    let started = Instant::now();
    let mut checksum = 0usize;
    for _ in 0..ITERATIONS {
        let plan = plan_batch_get_select("table_BatchGetPlan", key_schema, keys)
            .expect("plan batch get")
            .expect("non-empty plan");
        checksum ^= plan.sql.len() ^ plan.params.len();
    }
    std::hint::black_box(checksum);
    let elapsed = started.elapsed();
    let ns_per_iter = elapsed.as_nanos() as f64 / ITERATIONS as f64;
    println!(
        "{label} iterations={ITERATIONS} checksum={checksum} elapsed_ms={:.3} \
         ns_per_iter={ns_per_iter:.2}",
        elapsed.as_secs_f64() * 1_000.0,
    );
    ns_per_iter
}

#[test]
fn sqlite_batch_get_select_plan_allocation_profile_tests() {
    let hash_schema = hash_key_schema();
    let hash_range_schema = hash_range_key_schema();
    let hash_keys = hash_keys();
    let hash_range_keys = hash_range_keys();

    let hash_report = measure_allocations("hash_key", &hash_schema, &hash_keys);
    let hash_range_report =
        measure_allocations("hash_range_key", &hash_range_schema, &hash_range_keys);
    alloc_counter::emit_report(&hash_report);
    alloc_counter::emit_report(&hash_range_report);

    assert!(hash_report.allocation_count > 0);
    assert!(hash_range_report.allocation_count > 0);
    assert!(hash_range_report.allocated_bytes > hash_report.allocated_bytes);
}

#[test]
#[ignore = "manual runtime perf probe; run with --ignored --nocapture before/after batch get \
            planning changes"]
fn sqlite_batch_get_select_plan_runtime_perf_probe() {
    let hash_schema = hash_key_schema();
    let hash_range_schema = hash_range_key_schema();
    let hash_keys = hash_keys();
    let hash_range_keys = hash_range_keys();

    let hash = measure_runtime(
        "sqlite_batch_get_select_plan_hash_key",
        &hash_schema,
        &hash_keys,
    );
    let hash_range = measure_runtime(
        "sqlite_batch_get_select_plan_hash_range_key",
        &hash_range_schema,
        &hash_range_keys,
    );

    assert!(hash > 0.0);
    assert!(hash_range > 0.0);
}

#[test]
fn sqlite_batch_get_select_plan_validates_key_shape_tests() {
    let schema = vec![KeySchemaElement {
        attribute_name: "pk".to_string(),
        key_type: KeyType::Hash,
    }];
    let err = plan_batch_get_select(
        "table_BatchGetPlan",
        &schema,
        &[KeyAttributes::from([(
            "wrong_pk".to_string(),
            AttributeValue::S("tenant#0001".to_string()),
        )])],
    )
    .expect_err("missing batch get key should fail");

    assert!(err.to_string().contains("Invalid or missing key"));
}
