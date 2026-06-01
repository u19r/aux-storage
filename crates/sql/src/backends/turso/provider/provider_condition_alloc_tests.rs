use std::collections::HashMap;

use alloc_counter::AllocationGuard;
use storage_condition::{evaluate_condition, parse_condition_expression};
use storage_types::AttributeValue;

use super::core::condition_item_ref;

const CONDITION_EVAL_ITERATIONS: usize = 2048;

#[test]
fn turso_condition_evaluation_borrows_old_item_tests() {
    let baseline = measure_condition_old_item_clone_baseline();
    let borrowed = measure_condition_old_item_borrowed();

    alloc_counter::emit_report(&baseline);
    alloc_counter::emit_report(&borrowed);

    assert!(
        borrowed.allocation_count < baseline.allocation_count,
        "expected borrowed condition evaluation to allocate less often, baseline={} borrowed={}",
        baseline.allocation_count,
        borrowed.allocation_count
    );
    assert!(
        borrowed.allocated_bytes < baseline.allocated_bytes,
        "expected borrowed condition evaluation to allocate fewer bytes, baseline={} borrowed={}",
        baseline.allocated_bytes,
        borrowed.allocated_bytes
    );
}

fn measure_condition_old_item_clone_baseline() -> alloc_counter::AllocationReport<'static> {
    let item = realistic_condition_item();
    let condition = condition();

    let guard = AllocationGuard::start(
        module_path!(),
        "turso_condition_old_item_clone_baseline",
        file!(),
        line!(),
        Some("clone_baseline"),
    );
    for _ in 0..CONDITION_EVAL_ITERATIONS {
        let old_item = Some(item.clone());
        let condition_item = old_item.clone().unwrap_or_default();
        let matched = evaluate_condition(&condition_item, &condition);
        std::hint::black_box(matched);
    }
    guard.finish()
}

fn measure_condition_old_item_borrowed() -> alloc_counter::AllocationReport<'static> {
    let item = realistic_condition_item();
    let condition = condition();

    let guard = AllocationGuard::start(
        module_path!(),
        "turso_condition_old_item_borrowed",
        file!(),
        line!(),
        Some("borrowed"),
    );
    for _ in 0..CONDITION_EVAL_ITERATIONS {
        let old_item = Some(item.clone());
        let matched = evaluate_condition(condition_item_ref(old_item.as_ref()), &condition);
        std::hint::black_box(matched);
    }
    guard.finish()
}

fn condition() -> storage_condition::Condition {
    parse_condition_expression(
        "#status = :active AND begins_with(#search, :prefix)",
        Some(&HashMap::from([
            ("#status".to_string(), "status".to_string()),
            ("#search".to_string(), "search".to_string()),
        ])),
        Some(&HashMap::from([
            (
                ":active".to_string(),
                AttributeValue::S("active".to_string()),
            ),
            (
                ":prefix".to_string(),
                AttributeValue::S("prefix".to_string()),
            ),
        ])),
    )
    .expect("parse condition")
}

fn realistic_condition_item() -> HashMap<String, AttributeValue> {
    let mut item = HashMap::from([
        ("pk".to_string(), AttributeValue::S("ORG#ALLOC".to_string())),
        ("sk".to_string(), AttributeValue::S("ITEM#0042".to_string())),
        (
            "status".to_string(),
            AttributeValue::S("active".to_string()),
        ),
        (
            "search".to_string(),
            AttributeValue::S(format!("prefix-{}", "x".repeat(128))),
        ),
        (
            "ttl".to_string(),
            AttributeValue::N("1780000000".to_string()),
        ),
    ]);

    for index in 0..10 {
        item.insert(
            format!("payload_{index}"),
            AttributeValue::S(format!("value-{index}-{}", "x".repeat(96))),
        );
    }
    item
}
