//! Independently runnable PutItem cases copied from dynamo-conformance.
//!
//! The fixture snapshot is from ../dynamo-conformance at
//! 2158288d7883de4be6c17da00226d8398faa723a. Each case retains its source
//! path and compares the entire Dynamo response through the replacement store.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::{Value, json};
use storage_observability_test_support::observation::observe;
use storage_types::{AttributeMap, GetItemRequest, KeyAttributes, PutItemRequest, TableName};

use crate::test_support::{ConformanceFixtures, TestStore};

static FIXTURES: ConformanceFixtures = ConformanceFixtures::new();

fn fixture(source: &str) -> Value {
    FIXTURES.case(include_str!("../fixtures/put_item_cases.jsonl"), source)
}

/// The primary key attributes (`pk`, and `sk` when present) of an item.
fn item_key(item: &Value) -> Value {
    let mut key = serde_json::Map::new();
    for name in ["pk", "sk"] {
        if let Some(value) = item.get(name) {
            key.insert(name.to_owned(), value.clone());
        }
    }
    Value::Object(key)
}

async fn run_case(store: &TestStore, case: &Value) {
    assert_eq!(case["operation"], "PutItem", "fixture operation");
    let table_kind = case["tables"][0].as_str().unwrap();
    let table = TableName::new(case["request"]["TableName"].as_str().unwrap());
    store
        .create_conformance_table(table_kind, table.clone())
        .await;

    let preconditions = case["preconditions"].as_array().unwrap();
    for precondition in preconditions {
        assert_eq!(
            precondition["operation"], "PutItem",
            "precondition operation"
        );
        let request = PutItemRequest::try_from(precondition["request"].clone()).unwrap();
        store.operations.put_item(request).await.unwrap();
    }

    let parsed = observe(async { PutItemRequest::try_from(case["request"].clone()) }).await;
    parsed.assert_no_warning_or_error();
    parsed.assert_database_calls_exact(&[]);
    let (status, body) = match parsed.result {
        Ok(request) => {
            let observed = observe(store.operations.put_item(request)).await;
            observed.assert_no_warning_or_error();
            let operation = observed.one_span_with("dynamo_operation", &[("operation", "PutItem")]);
            assert_eq!(
                operation.fields.len(),
                1,
                "PutItem span must contain only safe context"
            );
            match observed.result {
                Ok(response) => (200, serde_json::to_value(response).unwrap()),
                Err(error) => {
                    let (status, axum::Json(body)): (_, axum::Json<http_error::ErrorResponse>) =
                        http_error::HttpApiError::from(error).into();
                    (status.as_u16(), serde_json::to_value(body).unwrap())
                }
            }
        }
        Err(message) => {
            // The route's request-conversion boundary produces this error
            // shape.
            (
                400,
                json!({
                    "__type": "com.amazon.coral.validate#ValidationException",
                    "message": message
                }),
            )
        }
    };
    let expected_status = case["expected"]["status"].as_u64().unwrap() as u16;
    assert_eq!(status, expected_status, "{}", case["source"]);
    assert_eq!(
        serde_json::to_string(&body).unwrap(),
        serde_json::to_string(&case["expected"]["body"]).unwrap(),
        "{}",
        case["source"],
    );

    let expected_item = if status == 200 {
        Some(&case["request"]["Item"])
    } else {
        preconditions
            .last()
            .map(|precondition| &precondition["request"]["Item"])
    };
    if let Some(expected_item) = expected_item {
        let key: KeyAttributes = serde_json::from_value(item_key(expected_item)).unwrap();
        let read = store
            .operations
            .get_item(GetItemRequest::new(table, key))
            .await
            .unwrap();
        let read = serde_json::to_value(read).unwrap();
        let mut normalized_expected: AttributeMap =
            serde_json::from_value(expected_item.clone()).unwrap();
        normalized_expected.normalize_numbers_for_write();
        assert_eq!(
            read["Item"],
            serde_json::to_value(normalized_expected).unwrap(),
            "{} readback",
            case["source"],
        );
    } else if case["source"]
        .as_str()
        .unwrap()
        .starts_with("field-condition_expression/")
    {
        // These expression failures have valid keys and no seeded item.
        let item = &case["request"]["Item"];
        let pk = serde_json::from_value(item["pk"].clone()).unwrap();
        let sk = serde_json::from_value(item["sk"].clone()).unwrap();
        let read = store
            .operations
            .get_item(GetItemRequest::new(
                table,
                [("pk".to_owned(), pk), ("sk".to_owned(), sk)],
            ))
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(read).unwrap(),
            json!({}),
            "{} must not write an item",
            case["source"],
        );
    }
}

macro_rules! put_item_case {
    ($name:ident, $source:literal) => {
        #[tokio::test]
        async fn $name() {
            let case = fixture($source);
            let store = TestStore::new().await;
            let outcome = AssertUnwindSafe(run_case(&store, &case))
                .catch_unwind()
                .await;
            store.finish().await;
            if let Err(panic) = outcome {
                resume_unwind(panic);
            }
        }
    };
}

// Format rule: unknown request fields leave the successful response unchanged.
put_item_case!(
    given_unknown_top_level_field_is_ignored_when_put_item_runs_then_conformance_response_matches,
    "200/unknown_top_level_field_is_ignored.json"
);

// Business rules: true conditions replace the item and return exact responses.
put_item_case!(
    given_aliased_nested_reserved_word_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/aliased_nested_reserved_word.json"
);
put_item_case!(
    given_bulk_and_precedence_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/bulk_and_precedence.json"
);
put_item_case!(
    given_bulk_attribute_exists_and_contains_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/bulk_attribute_exists_and_contains.json"
);
put_item_case!(
    given_bulk_attribute_exists_nested_child_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/bulk_attribute_exists_nested_child.json"
);
put_item_case!(
    given_bulk_attribute_exists_pk_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/bulk_attribute_exists_pk.json"
);
put_item_case!(given_bulk_attribute_not_exists_missing_child_when_put_item_runs_then_conformance_response_matches, "field-condition_expression/200/bulk_attribute_not_exists_missing_child.json");
put_item_case!(
    given_bulk_attribute_type_bool_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/bulk_attribute_type_bool.json"
);
put_item_case!(
    given_bulk_attribute_type_null_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/bulk_attribute_type_null.json"
);
put_item_case!(
    given_bulk_attribute_type_number_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/bulk_attribute_type_number.json"
);
put_item_case!(
    given_bulk_attribute_type_string_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/bulk_attribute_type_string.json"
);
put_item_case!(
    given_bulk_begins_with_string_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/bulk_begins_with_string.json"
);
put_item_case!(
    given_bulk_contains_number_set_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/bulk_contains_number_set.json"
);
put_item_case!(
    given_bulk_contains_string_set_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/bulk_contains_string_set.json"
);
put_item_case!(
    given_bulk_contains_string_substring_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/bulk_contains_string_substring.json"
);
put_item_case!(
    given_bulk_deep_nested_equality_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/bulk_deep_nested_equality.json"
);
put_item_case!(
    given_bulk_list_index_map_child_equality_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/bulk_list_index_map_child_equality.json"
);
put_item_case!(
    given_bulk_list_index_string_equality_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/bulk_list_index_string_equality.json"
);
put_item_case!(
    given_bulk_nested_map_equality_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/bulk_nested_map_equality.json"
);
put_item_case!(
    given_bulk_nested_number_comparison_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/bulk_nested_number_comparison.json"
);
put_item_case!(
    given_bulk_not_attribute_exists_missing_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/bulk_not_attribute_exists_missing.json"
);
put_item_case!(
    given_bulk_not_less_than_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/bulk_not_less_than.json"
);
put_item_case!(
    given_bulk_number_between_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/bulk_number_between.json"
);
put_item_case!(
    given_bulk_number_in_list_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/bulk_number_in_list.json"
);
put_item_case!(
    given_bulk_parenthesized_or_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/bulk_parenthesized_or.json"
);
put_item_case!(
    given_bulk_size_string_between_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/bulk_size_string_between.json"
);
put_item_case!(
    given_bulk_size_string_equals_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/bulk_size_string_equals.json"
);
put_item_case!(
    given_parenthesized_conjunction_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/parenthesized_conjunction.json"
);
put_item_case!(
    given_parenthesized_operands_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/parenthesized_operands.json"
);
put_item_case!(given_parser_condition_alias_nested_reserved_word_valid_when_put_item_runs_then_conformance_response_matches, "field-condition_expression/200/parser_condition_alias_nested_reserved_word_valid.json");
put_item_case!(
    given_parser_condition_newlines_tabs_valid_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/parser_condition_newlines_tabs_valid.json"
);
put_item_case!(given_semantic_attribute_type_binary_set_true_when_put_item_runs_then_conformance_response_matches, "field-condition_expression/200/semantic_attribute_type_binary_set_true.json");
put_item_case!(given_semantic_condition_reads_existing_item_before_replace_when_put_item_runs_then_conformance_response_matches, "field-condition_expression/200/semantic_condition_reads_existing_item_before_replace.json");
put_item_case!(given_semantic_contains_binary_set_member_true_when_put_item_runs_then_conformance_response_matches, "field-condition_expression/200/semantic_contains_binary_set_member_true.json");
put_item_case!(given_semantic_exists_and_missing_nested_child_true_when_put_item_runs_then_conformance_response_matches, "field-condition_expression/200/semantic_exists_and_missing_nested_child_true.json");
put_item_case!(
    given_semantic_in_matches_map_structurally_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/semantic_in_matches_map_structurally.json"
);
put_item_case!(given_single_parenthesized_attribute_exists_when_put_item_runs_then_conformance_response_matches, "field-condition_expression/200/single_parenthesized_attribute_exists.json");
put_item_case!(
    given_single_parenthesized_comparison_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/single_parenthesized_comparison.json"
);

// Negative rules: invalid or false conditions preserve the prior item.
put_item_case!(
    given_attribute_exists_reserved_word_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/400/attribute_exists_reserved_word.json"
);
put_item_case!(
    given_attribute_not_exists_failure_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/400/attribute_not_exists_failure.json"
);
put_item_case!(
    given_begins_with_nested_reserved_word_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/400/begins_with_nested_reserved_word.json"
);
put_item_case!(
    given_contains_nested_reserved_word_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/400/contains_nested_reserved_word.json"
);
put_item_case!(given_invalid_expression_attribute_value_key_when_put_item_runs_then_conformance_response_matches, "field-condition_expression/400/invalid_expression_attribute_value_key.json");
put_item_case!(
    given_nested_reserved_word_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/400/nested_reserved_word.json"
);
put_item_case!(given_parser_condition_begins_with_case_sensitive_rejected_when_put_item_runs_then_conformance_response_matches, "field-condition_expression/400/parser_condition_begins_with_case_sensitive_rejected.json");
put_item_case!(given_parser_unused_name_precedes_condition_failure_when_put_item_runs_then_conformance_response_matches, "field-condition_expression/400/parser_unused_name_precedes_condition_failure.json");
put_item_case!(given_redundant_parenthesized_attribute_exists_when_put_item_runs_then_conformance_response_matches, "field-condition_expression/400/redundant_parenthesized_attribute_exists.json");
put_item_case!(
    given_redundant_parenthesized_comparison_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/400/redundant_parenthesized_comparison.json"
);
put_item_case!(
    given_redundant_parenthesized_conjunction_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/400/redundant_parenthesized_conjunction.json"
);
put_item_case!(
    given_redundant_parenthesized_path_operand_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/400/redundant_parenthesized_path_operand.json"
);
put_item_case!(given_redundant_parenthesized_value_operand_when_put_item_runs_then_conformance_response_matches, "field-condition_expression/400/redundant_parenthesized_value_operand.json");
put_item_case!(given_semantic_attribute_not_exists_pk_existing_item_fails_when_put_item_runs_then_conformance_response_matches, "field-condition_expression/400/semantic_attribute_not_exists_pk_existing_item_fails.json");
put_item_case!(given_semantic_between_string_reversed_bounds_validation_when_put_item_runs_then_conformance_response_matches, "field-condition_expression/400/semantic_between_string_reversed_bounds_validation.json");
put_item_case!(given_semantic_condition_failure_returns_all_old_complex_item_when_put_item_runs_then_conformance_response_matches, "field-condition_expression/400/semantic_condition_failure_returns_all_old_complex_item.json");
put_item_case!(given_semantic_contains_function_name_case_sensitive_when_put_item_runs_then_conformance_response_matches, "field-condition_expression/400/semantic_contains_function_name_case_sensitive.json");
put_item_case!(given_semantic_contains_list_partial_map_false_when_put_item_runs_then_conformance_response_matches, "field-condition_expression/400/semantic_contains_list_partial_map_false.json");
put_item_case!(given_semantic_number_string_equality_false_when_put_item_runs_then_conformance_response_matches, "field-condition_expression/400/semantic_number_string_equality_false.json");
put_item_case!(given_semantic_size_missing_attribute_false_when_put_item_runs_then_conformance_response_matches, "field-condition_expression/400/semantic_size_missing_attribute_false.json");
put_item_case!(
    given_size_nested_reserved_word_when_put_item_runs_then_conformance_response_matches,
    "field-condition_expression/400/size_nested_reserved_word.json"
);

// Boundary rules: valid binary and numeric keys write and read back.
put_item_case!(given_docs_limit_binary_partition_key_1_byte_valid_when_put_item_runs_then_conformance_response_matches, "field-item/200/docs_limit_binary_partition_key_1_byte_valid.json");
put_item_case!(given_docs_limit_binary_partition_key_2048_bytes_valid_when_put_item_runs_then_conformance_response_matches, "field-item/200/docs_limit_binary_partition_key_2048_bytes_valid.json");
put_item_case!(given_docs_limit_binary_sort_key_1024_bytes_valid_when_put_item_runs_then_conformance_response_matches, "field-item/200/docs_limit_binary_sort_key_1024_bytes_valid.json");
put_item_case!(given_docs_limit_binary_sort_key_1_byte_valid_when_put_item_runs_then_conformance_response_matches, "field-item/200/docs_limit_binary_sort_key_1_byte_valid.json");
put_item_case!(given_docs_limit_number_partition_key_38_digits_valid_when_put_item_runs_then_conformance_response_matches, "field-item/200/docs_limit_number_partition_key_38_digits_valid.json");
put_item_case!(given_docs_limit_number_partition_key_positive_min_exponent_valid_when_put_item_runs_then_conformance_response_matches, "field-item/200/docs_limit_number_partition_key_positive_min_exponent_valid.json");
put_item_case!(given_docs_limit_number_partition_key_zero_valid_when_put_item_runs_then_conformance_response_matches, "field-item/200/docs_limit_number_partition_key_zero_valid.json");
put_item_case!(given_docs_limit_number_sort_key_38_digits_valid_when_put_item_runs_then_conformance_response_matches, "field-item/200/docs_limit_number_sort_key_38_digits_valid.json");
put_item_case!(given_docs_limit_number_sort_key_positive_min_exponent_valid_when_put_item_runs_then_conformance_response_matches, "field-item/200/docs_limit_number_sort_key_positive_min_exponent_valid.json");
put_item_case!(given_docs_limit_number_sort_key_zero_valid_when_put_item_runs_then_conformance_response_matches, "field-item/200/docs_limit_number_sort_key_zero_valid.json");

// Negative boundary rules: invalid binary and numeric keys return exact errors.
put_item_case!(given_docs_limit_binary_partition_key_2049_bytes_rejected_when_put_item_runs_then_conformance_response_matches, "field-item/400/docs_limit_binary_partition_key_2049_bytes_rejected.json");
put_item_case!(given_docs_limit_binary_partition_key_empty_rejected_when_put_item_runs_then_conformance_response_matches, "field-item/400/docs_limit_binary_partition_key_empty_rejected.json");
put_item_case!(given_docs_limit_binary_sort_key_1025_bytes_rejected_when_put_item_runs_then_conformance_response_matches, "field-item/400/docs_limit_binary_sort_key_1025_bytes_rejected.json");
put_item_case!(given_docs_limit_binary_sort_key_empty_rejected_when_put_item_runs_then_conformance_response_matches, "field-item/400/docs_limit_binary_sort_key_empty_rejected.json");
put_item_case!(given_docs_limit_number_partition_key_39_digits_rejected_when_put_item_runs_then_conformance_response_matches, "field-item/400/docs_limit_number_partition_key_39_digits_rejected.json");
put_item_case!(given_docs_limit_number_partition_key_empty_rejected_when_put_item_runs_then_conformance_response_matches, "field-item/400/docs_limit_number_partition_key_empty_rejected.json");
put_item_case!(given_docs_limit_number_partition_key_under_min_exponent_rejected_when_put_item_runs_then_conformance_response_matches, "field-item/400/docs_limit_number_partition_key_under_min_exponent_rejected.json");
put_item_case!(given_docs_limit_number_sort_key_39_digits_rejected_when_put_item_runs_then_conformance_response_matches, "field-item/400/docs_limit_number_sort_key_39_digits_rejected.json");
put_item_case!(given_docs_limit_number_sort_key_empty_rejected_when_put_item_runs_then_conformance_response_matches, "field-item/400/docs_limit_number_sort_key_empty_rejected.json");
put_item_case!(given_docs_limit_number_sort_key_under_min_exponent_rejected_when_put_item_runs_then_conformance_response_matches, "field-item/400/docs_limit_number_sort_key_under_min_exponent_rejected.json");

// DDB-PAR-4 (AWS probe P15): an expression attribute name is one attribute
// name, so `#a` -> `a.b` reads the top-level `a.b`, never `a` -> `b`.
put_item_case!(
    given_dotted_alias_condition_when_put_runs_then_top_level_attribute_is_compared,
    "local/ddb-par-4/dotted_alias_condition_put.json"
);
put_item_case!(
    given_dotted_alias_condition_on_nested_value_when_put_runs_then_condition_fails,
    "local/ddb-par-4/dotted_alias_condition_nested_put.json"
);
// AWS probe P11.
put_item_case!(
    given_conditional_operator_when_put_runs_then_legacy_parameter_is_rejected,
    "local/ddb-par-2/given_conditional_operator_when_put_runs_then_legacy_parameter_is_rejected.\
     json"
);
// AWS probe P11.
put_item_case!(
    given_expected_and_condition_expression_when_put_runs_then_mixed_parameters_are_rejected,
    "local/ddb-par-2/\
     given_expected_and_condition_expression_when_put_runs_then_mixed_parameters_are_rejected.json"
);

// AWS behavioural suite
put_item_case!(given_check_attribute_name_length_limit_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_attribute_name_length_limit.json");
put_item_case!(given_check_attribute_name_length_limit_over_65536_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_attribute_name_length_limit--over-65536.json");
put_item_case!(given_check_attribute_value_union_requires_exactly_one_member_control_one_member_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_attribute_value_union_requires_exactly_one_member--control-one-member.json");
put_item_case!(given_check_attribute_value_union_requires_exactly_one_member_zero_top_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_attribute_value_union_requires_exactly_one_member--zero-top.json");
put_item_case!(given_check_attribute_value_union_requires_exactly_one_member_zero_in_m_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_attribute_value_union_requires_exactly_one_member--zero-in-m.json");
put_item_case!(given_check_attribute_value_union_requires_exactly_one_member_rejected_control_one_member_at_every_depth_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_attribute_value_union_requires_exactly_one_member_rejected--control-one-member-at-every-depth.json");
put_item_case!(given_check_attribute_value_union_requires_exactly_one_member_rejected_zero_in_l_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_attribute_value_union_requires_exactly_one_member_rejected--zero-in-l.json");
put_item_case!(given_check_attribute_value_union_requires_exactly_one_member_rejected_two_top_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_attribute_value_union_requires_exactly_one_member_rejected--two-top.json");
put_item_case!(given_check_attribute_value_union_requires_exactly_one_member_rejected_two_in_m_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_attribute_value_union_requires_exactly_one_member_rejected--two-in-m.json");
put_item_case!(given_check_attribute_value_union_requires_exactly_one_member_rejected_two_in_l_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_attribute_value_union_requires_exactly_one_member_rejected--two-in-l.json");
put_item_case!(given_check_put_item_key_type_mismatch_rejected_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_put_item_key_type_mismatch_rejected.json");
put_item_case!(given_check_batch_write_item_key_defects_message_diverges_from_put_item_put_wrong_typed_key_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_batch_write_item_key_defects_message_diverges_from_put_item--put-wrong-typed-key.json");
put_item_case!(given_check_batch_write_item_key_defects_message_diverges_from_put_item_put_list_typed_key_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_batch_write_item_key_defects_message_diverges_from_put_item--put-list-typed-key.json");
put_item_case!(given_check_batch_write_item_key_defects_message_diverges_from_put_item_put_missing_key_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_batch_write_item_key_defects_message_diverges_from_put_item--put-missing-key.json");
put_item_case!(given_check_condition_attribute_exists_absent_flag_refused_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_condition_attribute_exists--absent-flag-refused.json");
put_item_case!(given_check_condition_attribute_exists_present_flag_applies_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_condition_attribute_exists--present-flag-applies.json");
put_item_case!(given_check_condition_attribute_type_gates_the_write_wrong_stored_type_refused_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_condition_attribute_type_gates_the_write--wrong-stored-type-refused.json");
put_item_case!(given_check_condition_attribute_type_gates_the_write_matching_type_applies_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_condition_attribute_type_gates_the_write--matching-type-applies.json");
put_item_case!(
    given_check_condition_on_missing_item_when_put_item_runs_then_aws_behavioural_response_matches,
    "local/aws-behav/check_condition_on_missing_item.json"
);
put_item_case!(given_check_ddb_numeric_sort_key_magnitude_ceiling_rejected_ceiling_accepted_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_ddb_numeric_sort_key_magnitude_ceiling_rejected--ceiling-accepted.json");
put_item_case!(given_check_ddb_numeric_sort_key_magnitude_ceiling_rejected_over_ceiling_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_ddb_numeric_sort_key_magnitude_ceiling_rejected--over-ceiling.json");
put_item_case!(given_check_ddb_numeric_sort_key_magnitude_ceiling_rejected_under_negative_ceiling_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_ddb_numeric_sort_key_magnitude_ceiling_rejected--under-negative-ceiling.json");
put_item_case!(given_check_nesting_depth_limit_enforced_at_32_levels_map_32_levels_accepted_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_nesting_depth_limit_enforced_at_32_levels--map-32-levels-accepted.json");
put_item_case!(given_check_nesting_depth_limit_enforced_at_32_levels_map_33_levels_refused_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_nesting_depth_limit_enforced_at_32_levels--map-33-levels-refused.json");
put_item_case!(given_check_nesting_depth_limit_enforced_at_32_levels_list_33_levels_refused_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_nesting_depth_limit_enforced_at_32_levels--list-33-levels-refused.json");
put_item_case!(given_check_put_and_delete_item_reject_return_values_all_new_refuse_all_new_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_put_and_delete_item_reject_return_values_all_new--refuse-all-new.json");
put_item_case!(given_check_put_and_delete_item_reject_return_values_all_new_refuse_updated_old_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_put_and_delete_item_reject_return_values_all_new--refuse-updated-old.json");
put_item_case!(given_check_put_and_delete_item_reject_return_values_all_new_refuse_updated_new_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_put_and_delete_item_reject_return_values_all_new--refuse-updated-new.json");
put_item_case!(given_check_put_item_default_response_omits_attributes_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_put_item_default_response_omits_attributes.json");
put_item_case!(given_check_put_item_and_search_vectors_return_consumed_capacity_invalid_enum_rejected_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_put_item_and_search_vectors_return_consumed_capacity_invalid_enum_rejected.json");
put_item_case!(given_check_put_update_delete_item_return_consumed_capacity_invalid_enum_rejected_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_put_update_delete_item_return_consumed_capacity_invalid_enum_rejected.json");
put_item_case!(given_check_put_item_nested_set_in_list_or_map_empty_rejected_empty_string_member_round_trips_empty_ss_in_list_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_put_item_nested_set_in_list_or_map_empty_rejected_empty_string_member_round_trips--empty-ss-in-list.json");
put_item_case!(given_check_put_item_nested_set_in_list_or_map_empty_rejected_empty_string_member_round_trips_empty_ns_in_list_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_put_item_nested_set_in_list_or_map_empty_rejected_empty_string_member_round_trips--empty-ns-in-list.json");
put_item_case!(given_check_put_item_nested_set_in_list_or_map_empty_rejected_empty_string_member_round_trips_empty_bs_in_list_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_put_item_nested_set_in_list_or_map_empty_rejected_empty_string_member_round_trips--empty-bs-in-list.json");
put_item_case!(given_check_put_item_nested_set_in_list_or_map_empty_rejected_empty_string_member_round_trips_empty_ss_in_map_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_put_item_nested_set_in_list_or_map_empty_rejected_empty_string_member_round_trips--empty-ss-in-map.json");
put_item_case!(given_check_put_item_nested_set_in_list_or_map_empty_rejected_empty_string_member_round_trips_empty_ns_in_map_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_put_item_nested_set_in_list_or_map_empty_rejected_empty_string_member_round_trips--empty-ns-in-map.json");
put_item_case!(given_check_put_item_nested_set_in_list_or_map_empty_rejected_empty_string_member_round_trips_empty_bs_in_map_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_put_item_nested_set_in_list_or_map_empty_rejected_empty_string_member_round_trips--empty-bs-in-map.json");
put_item_case!(given_check_put_item_nested_set_in_list_or_map_empty_rejected_empty_string_member_round_trips_ss_empty_member_in_list_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_put_item_nested_set_in_list_or_map_empty_rejected_empty_string_member_round_trips--ss-empty-member-in-list.json");
put_item_case!(given_check_put_item_nested_set_in_list_or_map_empty_rejected_empty_string_member_round_trips_ss_empty_member_in_map_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_put_item_nested_set_in_list_or_map_empty_rejected_empty_string_member_round_trips--ss-empty-member-in-map.json");
put_item_case!(given_check_put_item_two_empty_members_in_ss_and_bs_rejected_as_duplicate_collections_ss_two_empty_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_put_item_two_empty_members_in_ss_and_bs_rejected_as_duplicate_collections--ss-two-empty.json");
put_item_case!(given_check_put_item_two_empty_members_in_ss_and_bs_rejected_as_duplicate_collections_bs_two_empty_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_put_item_two_empty_members_in_ss_and_bs_rejected_as_duplicate_collections--bs-two-empty.json");
put_item_case!(given_check_put_item_two_empty_members_in_ss_and_bs_rejected_as_duplicate_collections_ss_one_empty_round_trips_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_put_item_two_empty_members_in_ss_and_bs_rejected_as_duplicate_collections--ss-one-empty-round-trips.json");
put_item_case!(given_check_put_item_two_empty_members_in_ss_and_bs_rejected_as_duplicate_collections_bs_one_empty_round_trips_when_put_item_runs_then_aws_behavioural_response_matches, "local/aws-behav/check_put_item_two_empty_members_in_ss_and_bs_rejected_as_duplicate_collections--bs-one-empty-round-trips.json");
