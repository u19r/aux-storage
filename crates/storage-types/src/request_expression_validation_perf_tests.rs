use std::{collections::HashMap, hint::black_box, time::Instant};

use alloc_counter::AllocationGuard;

use crate::{
    AttributeValue, extract_expression_attribute_placeholders,
    request_expression_validation::{
        is_dynamodb_reserved_word, is_dynamodb_reserved_word_linear, validate_expression_set,
    },
};

const EXPRESSION_VALIDATION_ITERATIONS: usize = 1024;
const RESERVED_LOOKUP_ITERATIONS: usize = 100_000;

#[test]
fn given_reserved_words_when_looked_up_then_case_and_boundaries_are_preserved() {
    for word in ["ABORT", "comment", "uPdAtE", "ZONE"] {
        assert!(is_dynamodb_reserved_word(word), "missing {word}");
    }
    for word in ["", "comments", "tenant_comment", "zone2", "not_reserved"] {
        assert!(!is_dynamodb_reserved_word(word), "unexpected {word}");
    }
}

#[test]
#[ignore = "manual debug comparison of the former full scan and current static set"]
fn given_repeated_tokens_when_looked_up_then_static_set_replaces_full_word_scan() {
    let tokens = ["tenant_id", "payload", "comment", "sort_key", "updated_at"];
    is_dynamodb_reserved_word("warmup");

    let baseline_started = Instant::now();
    let mut baseline_matches = 0usize;
    for _ in 0..RESERVED_LOOKUP_ITERATIONS {
        for token in tokens {
            baseline_matches += is_dynamodb_reserved_word_linear(black_box(token)) as usize;
        }
    }
    let baseline = baseline_started.elapsed();

    let candidate_started = Instant::now();
    let mut candidate_matches = 0usize;
    for _ in 0..RESERVED_LOOKUP_ITERATIONS {
        for token in tokens {
            candidate_matches += is_dynamodb_reserved_word(black_box(token)) as usize;
        }
    }
    let candidate = candidate_started.elapsed();

    assert_eq!(candidate_matches, baseline_matches);
    assert!(
        candidate < baseline,
        "baseline={baseline:?} candidate={candidate:?}"
    );
    eprintln!(
        "p2-057a lookups={} baseline_ns={} candidate_ns={}",
        RESERVED_LOOKUP_ITERATIONS * tokens.len(),
        baseline.as_nanos(),
        candidate.as_nanos()
    );
}

#[test]
fn expression_heavy_update_validation_reuses_placeholder_scans_tests() {
    let names = expression_names();
    let values = expression_values();
    let update_expression = "SET #payload = :payload, #notes = list_append(if_not_exists(#notes, \
                             :empty_list), :new_notes), #metrics.#count = \
                             if_not_exists(#metrics.#count, :zero) + :inc REMOVE #obsolete.#field \
                             ADD #score :inc DELETE #tags :remove_tag";
    let condition_expression = "#status IN (:open, :pending, :ready) AND begins_with(#sk, \
                                :sk_prefix) AND contains(#tags, :required_tag) AND \
                                attribute_type(#metrics.#count, :number_type)";

    let baseline = measure_legacy_expression_validation(
        update_expression,
        condition_expression,
        &names,
        &values,
    );
    let optimized = measure_current_expression_validation(
        update_expression,
        condition_expression,
        &names,
        &values,
    );

    alloc_counter::emit_report(&baseline);
    alloc_counter::emit_report(&optimized);

    assert!(
        optimized.allocation_count < baseline.allocation_count,
        "expected reused placeholder scans to allocate less often, baseline={} optimized={}",
        baseline.allocation_count,
        optimized.allocation_count
    );
    assert!(
        optimized.allocated_bytes < baseline.allocated_bytes,
        "expected reused placeholder scans to allocate fewer bytes, baseline={} optimized={}",
        baseline.allocated_bytes,
        optimized.allocated_bytes
    );
}

fn measure_current_expression_validation(
    update_expression: &str,
    condition_expression: &str,
    names: &HashMap<String, String>,
    values: &HashMap<String, AttributeValue>,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "expression_validation_reused_scans_current",
        file!(),
        line!(),
        Some("current"),
    );
    for _ in 0..EXPRESSION_VALIDATION_ITERATIONS {
        validate_expression_set(
            [
                (Some(update_expression), "UpdateExpression"),
                (Some(condition_expression), "ConditionExpression"),
            ],
            Some(names),
            Some(values),
            false,
        )
        .expect("valid expression set");
    }
    guard.finish()
}

fn measure_legacy_expression_validation(
    update_expression: &str,
    condition_expression: &str,
    names: &HashMap<String, String>,
    values: &HashMap<String, AttributeValue>,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "expression_validation_repeated_scans_legacy",
        file!(),
        line!(),
        Some("legacy"),
    );
    for _ in 0..EXPRESSION_VALIDATION_ITERATIONS {
        validate_expression_set(
            [
                (Some(update_expression), "UpdateExpression"),
                (Some(condition_expression), "ConditionExpression"),
            ],
            Some(names),
            Some(values),
            false,
        )
        .expect("valid expression set");

        // The previous validation path rebuilt owned placeholder sets for
        // expression-name checks, expression-value checks, and unused-key
        // checks.
        let _ = extract_expression_attribute_placeholders(update_expression);
        let _ = extract_expression_attribute_placeholders(condition_expression);
        let _ = extract_expression_attribute_placeholders(update_expression);
        let _ = extract_expression_attribute_placeholders(condition_expression);
    }
    guard.finish()
}

fn expression_names() -> HashMap<String, String> {
    HashMap::from([
        ("#payload".to_string(), "payload".to_string()),
        ("#notes".to_string(), "notes".to_string()),
        ("#metrics".to_string(), "metrics".to_string()),
        ("#count".to_string(), "count".to_string()),
        ("#obsolete".to_string(), "obsolete".to_string()),
        ("#field".to_string(), "field".to_string()),
        ("#score".to_string(), "score".to_string()),
        ("#tags".to_string(), "tags".to_string()),
        ("#status".to_string(), "status".to_string()),
        ("#sk".to_string(), "sk".to_string()),
    ])
}

fn expression_values() -> HashMap<String, AttributeValue> {
    HashMap::from([
        (":payload".to_string(), AttributeValue::S("x".repeat(1024))),
        (":empty_list".to_string(), AttributeValue::L(Vec::new())),
        (
            ":new_notes".to_string(),
            AttributeValue::L(vec![AttributeValue::S("new-note".to_string())]),
        ),
        (":zero".to_string(), AttributeValue::N("0".to_string())),
        (":inc".to_string(), AttributeValue::N("1".to_string())),
        (
            ":remove_tag".to_string(),
            AttributeValue::SS(vec!["old".to_string()]),
        ),
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
    ])
}
