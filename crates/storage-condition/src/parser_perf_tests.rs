use std::collections::HashMap;

use alloc_counter::AllocationGuard;
use storage_types::AttributeValue;

use crate::parse_condition_expression;

const CONDITION_PARSE_ITERATIONS: usize = 1024;

#[test]
fn expression_heavy_condition_parser_avoids_keyword_and_placeholder_reallocation_tests() {
    let names = expression_names();
    let values = expression_values();
    let expression = "(#status IN (:open, :pending, :ready) AND begins_with(#sk, :sk_prefix)) OR \
                      (contains(#tags, :required_tag) AND attribute_type(#metrics.#count, \
                      :number_type) AND #metrics.#count BETWEEN :min_count AND :max_count AND \
                      size(#notes) >= :min_notes)";

    let baseline = measure_legacy_condition_parse(expression, &names, &values);
    let optimized = measure_current_condition_parse(expression, &names, &values);

    alloc_counter::emit_report(&baseline);
    alloc_counter::emit_report(&optimized);

    assert!(
        optimized.allocation_count < baseline.allocation_count,
        "expected parser keyword/placeholder optimization to allocate less often, baseline={} \
         optimized={}",
        baseline.allocation_count,
        optimized.allocation_count
    );
    assert!(
        optimized.allocated_bytes < baseline.allocated_bytes,
        "expected parser keyword/placeholder optimization to allocate fewer bytes, baseline={} \
         optimized={}",
        baseline.allocated_bytes,
        optimized.allocated_bytes
    );
}

fn measure_current_condition_parse(
    expression: &str,
    names: &HashMap<String, String>,
    values: &HashMap<String, AttributeValue>,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "condition_parser_current",
        file!(),
        line!(),
        Some("current"),
    );
    for _ in 0..CONDITION_PARSE_ITERATIONS {
        let condition =
            parse_condition_expression(expression, Some(names), Some(values)).expect("parse");
        std::hint::black_box(condition);
    }
    guard.finish()
}

fn measure_legacy_condition_parse(
    expression: &str,
    names: &HashMap<String, String>,
    values: &HashMap<String, AttributeValue>,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "condition_parser_legacy",
        file!(),
        line!(),
        Some("legacy"),
    );
    for _ in 0..CONDITION_PARSE_ITERATIONS {
        let condition =
            parse_condition_expression(expression, Some(names), Some(values)).expect("parse");
        let mut allocations = 0usize;
        for token in expression
            .split(|ch: char| !(ch.is_ascii_alphanumeric() || matches!(ch, '_' | '#' | ':')))
            .filter(|token| !token.is_empty())
        {
            if token.starts_with(['#', ':']) {
                let key = token.to_string();
                let reparsed = if let Some(stripped) = key.strip_prefix('#') {
                    format!("#{stripped}")
                } else if let Some(stripped) = key.strip_prefix(':') {
                    format!(":{stripped}")
                } else {
                    key
                };
                allocations += reparsed.len();
            } else {
                let upper = token.to_uppercase();
                allocations += upper.len();
            }
        }
        std::hint::black_box((condition, allocations));
    }
    guard.finish()
}

fn expression_names() -> HashMap<String, String> {
    HashMap::from([
        ("#status".to_string(), "status".to_string()),
        ("#sk".to_string(), "sk".to_string()),
        ("#tags".to_string(), "tags".to_string()),
        ("#metrics".to_string(), "metrics".to_string()),
        ("#count".to_string(), "count".to_string()),
        ("#notes".to_string(), "notes".to_string()),
    ])
}

fn expression_values() -> HashMap<String, AttributeValue> {
    HashMap::from([
        (":open".to_string(), AttributeValue::S("open".to_string())),
        (
            ":pending".to_string(),
            AttributeValue::S("pending".to_string()),
        ),
        (":ready".to_string(), AttributeValue::S("ready".to_string())),
        (
            ":sk_prefix".to_string(),
            AttributeValue::S("tenant#".to_string()),
        ),
        (
            ":required_tag".to_string(),
            AttributeValue::S("required".to_string()),
        ),
        (
            ":number_type".to_string(),
            AttributeValue::S("N".to_string()),
        ),
        (":min_count".to_string(), AttributeValue::N("1".to_string())),
        (
            ":max_count".to_string(),
            AttributeValue::N("9999".to_string()),
        ),
        (":min_notes".to_string(), AttributeValue::N("2".to_string())),
    ])
}
