use std::{collections::HashMap, sync::LazyLock};

use alloc_counter::AllocationGuard;
use storage_condition::evaluate_condition;
use storage_provider::{apply_bound_update_operations, before_update_item};
use storage_types::{AttributeValue, KeyAttributes};

use crate::provider_core::write::plan_update_from_existing_item;

const PLAN_UPDATE_ITERATIONS: usize = 2048;

#[test]
fn provider_core_write_update_planning_allocation_profile_tests() {
    assert_plan_update_condition_evaluation_borrows_existing_item();
}

fn assert_plan_update_condition_evaluation_borrows_existing_item() {
    let baseline = measure_plan_update_condition_clone_baseline();
    let borrowed = measure_plan_update_condition_borrowed();

    alloc_counter::emit_report(&baseline);
    alloc_counter::emit_report(&borrowed);

    assert!(
        borrowed.allocation_count < baseline.allocation_count,
        "expected borrowed condition planning to allocate less often, baseline={} borrowed={}",
        baseline.allocation_count,
        borrowed.allocation_count
    );
    assert!(
        borrowed.allocated_bytes < baseline.allocated_bytes,
        "expected borrowed condition planning to allocate fewer bytes, baseline={} borrowed={}",
        baseline.allocated_bytes,
        borrowed.allocated_bytes
    );
}

fn measure_plan_update_condition_clone_baseline() -> alloc_counter::AllocationReport<'static> {
    let existing_item = realistic_update_item();
    let key = key_attributes();
    let (operations, condition) = update_plan();
    let condition = condition.expect("condition");

    let guard = AllocationGuard::start(
        module_path!(),
        "plan_update_condition_clone_baseline",
        file!(),
        line!(),
        Some("clone_baseline"),
    );
    for _ in 0..PLAN_UPDATE_ITERATIONS {
        let existing_item = std::hint::black_box(true).then(|| existing_item.clone());
        let item_for_condition = existing_item.clone().unwrap_or_default();
        if evaluate_condition(&item_for_condition, &condition) {
            let item_to_update = existing_item.unwrap_or_else(|| key.to_attribute_map());
            let updated_item = apply_bound_update_operations(item_to_update.clone(), &operations)
                .expect("apply update");
            std::hint::black_box(updated_item.len());
        }
    }
    guard.finish()
}

fn measure_plan_update_condition_borrowed() -> alloc_counter::AllocationReport<'static> {
    let existing_item = realistic_update_item();
    let key = key_attributes();
    let (operations, condition) = update_plan();

    let guard = AllocationGuard::start(
        module_path!(),
        "plan_update_condition_borrowed",
        file!(),
        line!(),
        Some("borrowed"),
    );
    for _ in 0..PLAN_UPDATE_ITERATIONS {
        let existing_item = Some(existing_item.clone());
        let (_, updated_item) = plan_update_from_existing_item(
            existing_item,
            &key,
            &operations,
            condition.as_ref(),
            false,
        )
        .expect("plan update");
        std::hint::black_box(updated_item.len());
    }
    guard.finish()
}

fn update_plan() -> (
    Vec<storage_provider::BoundUpdateOperation<'static>>,
    Option<storage_condition::Condition>,
) {
    static NAMES: LazyLock<HashMap<String, String>> = LazyLock::new(|| {
        HashMap::from([
            ("#payload".to_string(), "payload".to_string()),
            ("#counter".to_string(), "counter".to_string()),
            ("#status".to_string(), "status".to_string()),
            ("#search".to_string(), "search".to_string()),
        ])
    });
    static VALUES: LazyLock<HashMap<String, AttributeValue>> = LazyLock::new(|| {
        HashMap::from([
            (":payload".to_string(), AttributeValue::S("y".repeat(1024))),
            (":inc".to_string(), AttributeValue::N("1".to_string())),
            (
                ":active".to_string(),
                AttributeValue::S("active".to_string()),
            ),
            (
                ":prefix".to_string(),
                AttributeValue::S("prefix".to_string()),
            ),
        ])
    });

    before_update_item(
        "SET #payload = :payload, #counter = #counter + :inc",
        Some("#status = :active AND begins_with(#search, :prefix)"),
        Some(&NAMES),
        Some(&VALUES),
    )
    .expect("build update plan")
}

fn key_attributes() -> KeyAttributes {
    KeyAttributes::from([
        ("pk".to_string(), AttributeValue::S("ORG#ALLOC".to_string())),
        ("sk".to_string(), AttributeValue::S("ITEM#0042".to_string())),
    ])
}

fn realistic_update_item() -> HashMap<String, AttributeValue> {
    let mut item = HashMap::from([
        ("pk".to_string(), AttributeValue::S("ORG#ALLOC".to_string())),
        ("sk".to_string(), AttributeValue::S("ITEM#0042".to_string())),
        (
            "entity_type".to_string(),
            AttributeValue::S("ALLOC_PROFILE".to_string()),
        ),
        ("revision".to_string(), AttributeValue::N("42".to_string())),
        (
            "ttl".to_string(),
            AttributeValue::N("2200000042".to_string()),
        ),
        (
            "status".to_string(),
            AttributeValue::S("active".to_string()),
        ),
        (
            "owner".to_string(),
            AttributeValue::S("tenant-a".repeat(16)),
        ),
        (
            "search".to_string(),
            AttributeValue::S("prefix-value".to_string()),
        ),
        ("counter".to_string(), AttributeValue::N("100".to_string())),
    ]);
    item.insert(
        "payload".to_string(),
        AttributeValue::M(HashMap::from([
            (
                "status".to_string(),
                AttributeValue::S("active".to_string()),
            ),
            ("body".to_string(), AttributeValue::S("x".repeat(1024))),
        ])),
    );
    item
}
