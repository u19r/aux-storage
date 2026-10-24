use super::{DYNAMODB_RESERVED_WORDS, ExpressionKind};

fn is_dynamodb_reserved_word_linear(token: &str) -> bool {
    DYNAMODB_RESERVED_WORDS
        .split_ascii_whitespace()
        .any(|word| word.eq_ignore_ascii_case(token))
}

use std::{hint::black_box, time::Instant};

use alloc_counter::AllocationGuard;

use crate::{
    AttributeValue,
    constants_tests::{REQUEST_EXPRESSION_VALIDATION_ITERATIONS, RESERVED_LOOKUP_ITERATIONS},
    extract_expression_attribute_placeholders,
    request_expression_validation::{is_dynamodb_reserved_word, validate_expression_set},
};

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
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
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
        "reserved-word lookup: lookups={} baseline_ns={} candidate_ns={}",
        RESERVED_LOOKUP_ITERATIONS * tokens.len(),
        baseline.as_nanos(),
        candidate.as_nanos()
    );
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
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

    let _ = alloc_counter::emit_report(&baseline);
    let _ = alloc_counter::emit_report(&optimized);

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
    names: &crate::VecMap<String, String>,
    values: &crate::AttributeMap,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "expression_validation_reused_scans_current",
        file!(),
        line!(),
        Some("current"),
    );
    for _ in 0..REQUEST_EXPRESSION_VALIDATION_ITERATIONS {
        validate_expression_set(
            [
                (Some(update_expression), ExpressionKind::Update),
                (Some(condition_expression), ExpressionKind::Condition),
            ],
            Some(names),
            Some(values),
            false,
        )
        .unwrap();
    }
    guard.finish()
}

fn measure_legacy_expression_validation(
    update_expression: &str,
    condition_expression: &str,
    names: &crate::VecMap<String, String>,
    values: &crate::AttributeMap,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "expression_validation_repeated_scans_legacy",
        file!(),
        line!(),
        Some("legacy"),
    );
    for _ in 0..REQUEST_EXPRESSION_VALIDATION_ITERATIONS {
        validate_expression_set(
            [
                (Some(update_expression), ExpressionKind::Update),
                (Some(condition_expression), ExpressionKind::Condition),
            ],
            Some(names),
            Some(values),
            false,
        )
        .unwrap();

        // The previous validation path rebuilt owned placeholder sets for
        // expression-name checks, expression-value checks, and unused-key
        // checks.
        for expression in [
            update_expression,
            condition_expression,
            update_expression,
            condition_expression,
        ] {
            let (names, values) = extract_expression_attribute_placeholders(expression);
            std::hint::black_box((
                names
                    .into_iter()
                    .map(str::to_owned)
                    .collect::<std::collections::HashSet<_>>(),
                values
                    .into_iter()
                    .map(str::to_owned)
                    .collect::<std::collections::HashSet<_>>(),
            ));
        }
    }
    guard.finish()
}

fn expression_names() -> crate::VecMap<String, String> {
    crate::VecMap::from_iter([
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

fn expression_values() -> crate::AttributeMap {
    crate::AttributeMap::from_iter([
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

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_expression_validation_shapes_when_repeated_then_report_cpu_and_allocations() {
    let values = crate::AttributeMap::from_iter(
        (0..101).map(|index| (format!(":v{index}"), AttributeValue::N(index.to_string()))),
    );
    let operands = (0..16)
        .map(|index| format!(":v{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let oversized = (0..101)
        .map(|index| format!(":v{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let cases = [
        (
            "condition_simple",
            "tenant_key = :v0".to_owned(),
            ExpressionKind::Condition,
            true,
        ),
        (
            "condition_in",
            format!("tenant_key IN ({operands})"),
            ExpressionKind::Condition,
            true,
        ),
        (
            "condition_in_lower",
            format!("tenant_key in ({operands})"),
            ExpressionKind::Condition,
            true,
        ),
        (
            "condition_functions",
            "attribute_exists(tenant_key) AND attribute_not_exists(payload_key)".to_owned(),
            ExpressionKind::Condition,
            true,
        ),
        (
            "projection_short",
            "tenant_key, payload_key".to_owned(),
            ExpressionKind::Projection,
            true,
        ),
        (
            "projection_long",
            (0..32)
                .map(|index| format!("field_{index}"))
                .collect::<Vec<_>>()
                .join(", "),
            ExpressionKind::Projection,
            true,
        ),
        (
            "update_simple",
            "SET tenant_key = :v0".to_owned(),
            ExpressionKind::Update,
            true,
        ),
        (
            "condition_in_overflow",
            format!("tenant_key IN ({oversized})"),
            ExpressionKind::Condition,
            false,
        ),
    ];
    for (label, expression, kind, valid) in cases {
        let selected: crate::AttributeMap = values
            .iter()
            .filter(|(name, _)| {
                crate::extract_expression_attribute_placeholders(&expression)
                    .1
                    .contains(name)
            })
            .map(|(name, value)| (name.to_owned(), value.clone()))
            .collect();
        let operation = || {
            validate_expression_set(
                [(Some(expression.as_str()), kind)],
                None,
                (!selected.is_empty()).then_some(&selected),
                false,
            )
        };
        assert_eq!(operation().is_ok(), valid, "{label}");
        for _ in 0..100 {
            std::hint::black_box(operation()).ok();
        }
        let guard = AllocationGuard::start(
            module_path!(),
            "expression_validation",
            file!(),
            line!(),
            Some(label),
        );
        let started = cpu_time::ProcessTime::now();
        for _ in 0..10_000 {
            std::hint::black_box(operation()).ok();
        }
        let cpu = started.elapsed();
        let report = guard.finish();
        alloc_counter::emit_report(&report).unwrap();
        eprintln!(
            "expression_validation label={label} iterations=10000 cpu_ns={}",
            cpu.as_nanos()
        );
    }
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_update_value_shapes_when_validated_then_report_cpu_and_allocations() {
    let scenarios = [
        ("add_number", "ADD", AttributeValue::N("1".to_owned())),
        (
            "add_number_set",
            "ADD",
            AttributeValue::NS((0..32).map(|index| index.to_string()).collect()),
        ),
        (
            "add_string_set",
            "ADD",
            AttributeValue::SS((0..32).map(|index| format!("value_{index}")).collect()),
        ),
        (
            "delete_string_set",
            "DELETE",
            AttributeValue::SS((0..32).map(|index| format!("value_{index}")).collect()),
        ),
        (
            "delete_binary_set",
            "DELETE",
            AttributeValue::BS(vec!["AA==".to_owned(), "AQ==".to_owned()]),
        ),
    ];
    for (label, keyword, value) in scenarios {
        let values = crate::AttributeMap::from_iter([(":delta".to_owned(), value)]);
        let expression = format!("{keyword} tally_key :delta");
        let operation = || {
            validate_expression_set(
                [(Some(expression.as_str()), ExpressionKind::Update)],
                None,
                Some(&values),
                false,
            )
        };
        operation().unwrap();
        for _ in 0..100 {
            std::hint::black_box(operation()).unwrap();
        }
        let guard = AllocationGuard::start(
            module_path!(),
            "update_values",
            file!(),
            line!(),
            Some(label),
        );
        let started = cpu_time::ProcessTime::now();
        for _ in 0..10_000 {
            std::hint::black_box(operation()).unwrap();
        }
        let cpu = started.elapsed();
        let report = guard.finish();
        alloc_counter::emit_report(&report).unwrap();
        eprintln!(
            "expression_validation label={label} iterations=10000 cpu_ns={}",
            cpu.as_nanos()
        );
    }
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_request_validation_outer_shapes_when_decoded_then_report_cpu_and_allocations() {
    let projection = (0..32)
        .map(|index| format!("field_{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let cases = [
        (
            "outer_query_simple",
            serde_json::json!({"TableName":"benchmark", "KeyConditionExpression":"#pk = :key", "ExpressionAttributeNames":{"#pk":"partition_key"}, "ExpressionAttributeValues":{":key":{"S":"tenant_1"}}}),
        ),
        (
            "outer_query_projected",
            serde_json::json!({"TableName":"benchmark", "KeyConditionExpression":"#pk = :key", "ProjectionExpression":projection, "ExpressionAttributeNames":{"#pk":"partition_key"}, "ExpressionAttributeValues":{":key":{"S":"tenant_1"}}}),
        ),
        (
            "outer_query_filtered",
            serde_json::json!({"TableName":"benchmark", "KeyConditionExpression":"#pk = :key", "FilterExpression":"#status = :status", "ProjectionExpression":"field_0, field_1", "ExpressionAttributeNames":{"#pk":"partition_key", "#status":"status_key"}, "ExpressionAttributeValues":{":key":{"S":"tenant_1"}, ":status":{"S":"active"}}}),
        ),
    ];
    for (label, value) in cases {
        let operation = || crate::QueryRequest::try_from(std::hint::black_box(value.clone()));
        operation().unwrap();
        for _ in 0..100 {
            std::hint::black_box(operation()).unwrap();
        }
        let guard = AllocationGuard::start(
            module_path!(),
            "outer_validation",
            file!(),
            line!(),
            Some(label),
        );
        let started = cpu_time::ProcessTime::now();
        for _ in 0..5000 {
            std::hint::black_box(operation()).unwrap();
        }
        let cpu = started.elapsed();
        alloc_counter::emit_report(&guard.finish()).unwrap();
        eprintln!(
            "expression_validation label={label} iterations=5000 cpu_ns={}",
            cpu.as_nanos()
        );
    }
}
