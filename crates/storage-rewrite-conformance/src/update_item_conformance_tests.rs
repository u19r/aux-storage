//! Independently runnable UpdateItem cases copied from dynamo-conformance.
//!
//! The fixture snapshot is from ../dynamo-conformance at
//! 2158288d7883de4be6c17da00226d8398faa723a. Each case retains its source
//! path and compares the entire Dynamo response through the replacement store.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::Value;
use storage_observability_test_support::observation::observe;
use storage_types::{
    AttributeMap, GetItemRequest, KeyAttributes, PutItemRequest, TableName, UpdateItemRequest,
};

use crate::test_support::{ConformanceFixtures, TestStore};

static FIXTURES: ConformanceFixtures = ConformanceFixtures::new();

fn fixture(source: &str) -> Value {
    FIXTURES.case(include_str!("../fixtures/update_item_cases.jsonl"), source)
}

fn assert_expected_response(case: &Value, status: u16, body: &Value) {
    let expected_status = case["expected"]["status"].as_u64().unwrap() as u16;
    assert_eq!(
        status, expected_status,
        "{} returned {body}",
        case["source"]
    );
    assert_eq!(
        serde_json::to_string(body).unwrap(),
        serde_json::to_string(&case["expected"]["body"]).unwrap(),
        "{}",
        case["source"],
    );
}

async fn run_case(store: &TestStore, case: &Value, request: UpdateItemRequest) {
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

    let observed = observe(store.operations.update_item(request)).await;
    observed.assert_no_warning_or_error();
    let operation = observed.one_span_with("dynamo_operation", &[("operation", "UpdateItem")]);
    assert_eq!(
        operation.fields.len(),
        1,
        "UpdateItem span must contain only safe context"
    );
    let (status, body) = match observed.result {
        Ok(response) => (200, serde_json::to_value(response).unwrap()),
        Err(error) => {
            let (status, axum::Json(body)): (_, axum::Json<http_error::ErrorResponse>) =
                http_error::HttpApiError::from(error).into();
            (status.as_u16(), serde_json::to_value(body).unwrap())
        }
    };
    assert_expected_response(case, status, &body);

    // A rejected mutation cannot change the seeded item. For a successful
    // mutation, read the persisted item, even when ReturnValues is NONE.
    let read_key = if status == 400 {
        preconditions.last().map_or_else(
            || case["request"]["Key"].clone(),
            |precondition| {
                let item = &precondition["request"]["Item"];
                let mut key = serde_json::Map::new();
                key.insert("pk".to_owned(), item["pk"].clone());
                if table_kind != "basic_pk" {
                    key.insert("sk".to_owned(), item["sk"].clone());
                }
                Value::Object(key)
            },
        )
    } else {
        case["request"]["Key"].clone()
    };
    let invalid_key_case = case["source"]
        .as_str()
        .is_some_and(|source| source.starts_with("field-key/400"));
    let Ok(read_key) = serde_json::from_value::<KeyAttributes>(read_key) else {
        assert!(
            status == 400 && preconditions.is_empty() && invalid_key_case,
            "{} invalid readback key",
            case["source"]
        );
        return;
    };
    let read = store
        .operations
        .get_item(GetItemRequest::new(table, read_key))
        .await;
    if status == 400 && preconditions.is_empty() {
        let read = if invalid_key_case {
            read.ok()
        } else {
            Some(read.unwrap())
        };
        let Some(read) = read else { return };
        let read = serde_json::to_value(read).unwrap();
        assert!(
            read.get("Item").is_none(),
            "{} rejected mutation created an item",
            case["source"]
        );
        return;
    }
    let read = serde_json::to_value(read.unwrap()).unwrap();
    if status == 400 {
        let seeded_item = &preconditions.last().unwrap()["request"]["Item"];
        let mut expected_item: AttributeMap = serde_json::from_value(seeded_item.clone()).unwrap();
        expected_item.normalize_numbers_for_write();
        assert_eq!(
            read["Item"],
            serde_json::to_value(expected_item).unwrap(),
            "{} rejected mutation changed stored item",
            case["source"],
        );
    } else {
        assert!(
            read.get("Item").is_some(),
            "{} successful mutation stored no item",
            case["source"]
        );
        if case["request"]["ReturnValues"] == "ALL_NEW" {
            assert_eq!(
                read["Item"], body["Attributes"],
                "{} ALL_NEW readback",
                case["source"]
            );
        }
    }
}

macro_rules! update_item_case {
    ($name:ident, $source:literal) => {
        #[tokio::test]
        async fn $name() {
            let case = fixture($source);
            assert_eq!(case["operation"], "UpdateItem", "fixture operation");
            let parsed =
                observe(async { UpdateItemRequest::try_from(case["request"].clone()) }).await;
            parsed.assert_no_warning_or_error();
            parsed.assert_database_calls_exact(&[]);
            let request = match parsed.result {
                Ok(request) => request,
                Err(message) => {
                    let (status, axum::Json(body)): (_, axum::Json<http_error::ErrorResponse>) =
                        http_error::HttpApiError::dynamodb_protocol_error(
                            "ValidationException",
                            message,
                            400,
                        )
                        .into();
                    let body = serde_json::to_value(body).unwrap();
                    assert_expected_response(&case, status.as_u16(), &body);
                    return;
                }
            };
            let store = TestStore::new().await;
            let outcome = AssertUnwindSafe(run_case(&store, &case, request))
                .catch_unwind()
                .await;
            store.finish().await;
            if let Err(panic) = outcome {
                resume_unwind(panic);
            }
        }
    };
}

// Business rule: field-condition_expression/200.
update_item_case!(
    given_and_binds_inside_or_when_update_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/and_binds_inside_or.json"
);
update_item_case!(given_attribute_type_matches_scalar_and_document_types_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/200/attribute_type_matches_scalar_and_document_types.json");
update_item_case!(
    given_begins_with_binary_prefix_when_update_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/begins_with_binary_prefix.json"
);
update_item_case!(
    given_begins_with_string_prefix_when_update_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/begins_with_string_prefix.json"
);
update_item_case!(
    given_between_consumes_inner_and_when_update_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/between_consumes_inner_and.json"
);
update_item_case!(given_contains_number_set_binary_set_and_list_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/200/contains_number_set_binary_set_and_list.json");
update_item_case!(
    given_contains_string_substring_when_update_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/contains_string_substring.json"
);
update_item_case!(given_docs_limit_condition_expression_4096_bytes_valid_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/200/docs_limit_condition_expression_4096_bytes_valid.json");
update_item_case!(
    given_docs_limit_in_100_operands_valid_when_update_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/docs_limit_in_100_operands_valid.json"
);
update_item_case!(
    given_function_binds_before_not_and_or_when_update_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/function_binds_before_not_and_or.json"
);
update_item_case!(
    given_not_binds_inside_or_when_update_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/not_binds_inside_or.json"
);
update_item_case!(given_parser_condition_alias_reserved_child_after_list_valid_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/200/parser_condition_alias_reserved_child_after_list_valid.json");
update_item_case!(given_parser_condition_newlines_tabs_valid_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/200/parser_condition_newlines_tabs_valid.json");
update_item_case!(given_parser_condition_parenthesized_value_operand_accepted_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/200/parser_condition_parenthesized_value_operand_accepted.json");
update_item_case!(given_semantic_attribute_type_binary_set_true_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/200/semantic_attribute_type_binary_set_true.json");
update_item_case!(given_semantic_attribute_type_number_set_true_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/200/semantic_attribute_type_number_set_true.json");
update_item_case!(given_semantic_attribute_type_string_set_true_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/200/semantic_attribute_type_string_set_true.json");
update_item_case!(given_semantic_begins_with_binary_prefix_true_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/200/semantic_begins_with_binary_prefix_true.json");
update_item_case!(
    given_semantic_bool_equality_true_when_update_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/semantic_bool_equality_true.json"
);
update_item_case!(given_semantic_contains_binary_set_member_true_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/200/semantic_contains_binary_set_member_true.json");
update_item_case!(given_semantic_contains_list_map_element_true_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/200/semantic_contains_list_map_element_true.json");
update_item_case!(given_semantic_contains_string_set_member_true_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/200/semantic_contains_string_set_member_true.json");
update_item_case!(given_semantic_in_matches_map_structurally_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/200/semantic_in_matches_map_structurally.json");
update_item_case!(
    given_semantic_list_equality_true_when_update_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/semantic_list_equality_true.json"
);
update_item_case!(
    given_semantic_map_equality_true_when_update_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/semantic_map_equality_true.json"
);
update_item_case!(
    given_semantic_null_equality_true_when_update_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/semantic_null_equality_true.json"
);
update_item_case!(
    given_semantic_number_greater_than_true_when_update_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/semantic_number_greater_than_true.json"
);
update_item_case!(given_semantic_parenthesized_path_operand_accepted_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/200/semantic_parenthesized_path_operand_accepted.json");
update_item_case!(given_semantic_size_list_greater_than_true_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/200/semantic_size_list_greater_than_true.json");
update_item_case!(
    given_semantic_string_less_than_true_when_update_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/semantic_string_less_than_true.json"
);
update_item_case!(
    given_size_binary_counts_decoded_bytes_when_update_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/size_binary_counts_decoded_bytes.json"
);
update_item_case!(given_size_list_and_map_counts_direct_children_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/200/size_list_and_map_counts_direct_children.json");
update_item_case!(
    given_size_sets_counts_elements_when_update_item_runs_then_conformance_response_matches,
    "field-condition_expression/200/size_sets_counts_elements.json"
);
update_item_case!(given_size_string_matches_utf8_bytes_or_code_units_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/200/size_string_matches_utf8_bytes_or_code_units.json");

// Validation and negative rule: field-condition_expression/400.
update_item_case!(
    given_attribute_type_invalid_type_name_when_update_item_runs_then_conformance_response_matches,
    "field-condition_expression/400/attribute_type_invalid_type_name.json"
);
update_item_case!(
    given_begins_with_extra_operand_count_when_update_item_runs_then_conformance_response_matches,
    "field-condition_expression/400/begins_with_extra_operand_count.json"
);
update_item_case!(given_begins_with_number_first_operand_is_condition_failure_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/begins_with_number_first_operand_is_condition_failure.json");
update_item_case!(given_begins_with_number_second_operand_is_validation_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/begins_with_number_second_operand_is_validation.json");
update_item_case!(given_condition_error_precedes_update_error_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/condition_error_precedes_update_error.json");
update_item_case!(
    given_contains_extra_operand_count_when_update_item_runs_then_conformance_response_matches,
    "field-condition_expression/400/contains_extra_operand_count.json"
);
update_item_case!(given_contains_list_substring_is_condition_failure_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/contains_list_substring_is_condition_failure.json");
update_item_case!(
    given_contains_missing_operand_count_when_update_item_runs_then_conformance_response_matches,
    "field-condition_expression/400/contains_missing_operand_count.json"
);
update_item_case!(given_contains_missing_operands_syntax_error_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/contains_missing_operands_syntax_error.json");
update_item_case!(
    given_contains_same_path_and_operand_when_update_item_runs_then_conformance_response_matches,
    "field-condition_expression/400/contains_same_path_and_operand.json"
);
update_item_case!(given_contains_set_substring_is_condition_failure_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/contains_set_substring_is_condition_failure.json");
update_item_case!(given_docs_limit_condition_expression_4097_bytes_rejected_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/docs_limit_condition_expression_4097_bytes_rejected.json");
update_item_case!(given_docs_limit_expression_substitution_values_over_2mb_rejected_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/docs_limit_expression_substitution_values_over_2mb_rejected.json");
update_item_case!(given_docs_limit_in_101_operands_rejected_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/docs_limit_in_101_operands_rejected.json");
update_item_case!(given_if_not_exists_function_not_allowed_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/if_not_exists_function_not_allowed.json");
update_item_case!(
    given_list_append_function_not_allowed_when_update_item_runs_then_conformance_response_matches,
    "field-condition_expression/400/list_append_function_not_allowed.json"
);
update_item_case!(given_parenthesized_not_or_is_condition_failure_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/parenthesized_not_or_is_condition_failure.json");
update_item_case!(given_parenthesized_or_and_is_condition_failure_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/parenthesized_or_and_is_condition_failure.json");
update_item_case!(given_parser_condition_begins_with_case_sensitive_rejected_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/parser_condition_begins_with_case_sensitive_rejected.json");
update_item_case!(given_parser_condition_double_parenthesized_comparison_rejected_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/parser_condition_double_parenthesized_comparison_rejected.json");
update_item_case!(given_parser_condition_raw_reserved_child_after_list_rejected_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/parser_condition_raw_reserved_child_after_list_rejected.json");
update_item_case!(given_parser_missing_name_precedes_condition_failure_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/parser_missing_name_precedes_condition_failure.json");
update_item_case!(given_parser_unused_value_precedes_condition_failure_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/parser_unused_value_precedes_condition_failure.json");
update_item_case!(given_semantic_attribute_type_mismatch_false_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/semantic_attribute_type_mismatch_false.json");
update_item_case!(given_semantic_begins_with_binary_non_prefix_false_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/semantic_begins_with_binary_non_prefix_false.json");
update_item_case!(given_semantic_between_string_reversed_bounds_false_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/semantic_between_string_reversed_bounds_false.json");
update_item_case!(given_semantic_contains_function_name_case_sensitive_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/semantic_contains_function_name_case_sensitive.json");
update_item_case!(given_semantic_contains_list_partial_map_false_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/semantic_contains_list_partial_map_false.json");
update_item_case!(given_semantic_contains_string_case_sensitive_false_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/semantic_contains_string_case_sensitive_false.json");
update_item_case!(given_semantic_missing_attribute_comparison_false_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/semantic_missing_attribute_comparison_false.json");
update_item_case!(given_semantic_number_string_equality_is_false_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/semantic_number_string_equality_is_false.json");
update_item_case!(given_semantic_size_missing_attribute_false_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/semantic_size_missing_attribute_false.json");
update_item_case!(given_semantic_string_less_than_equal_is_false_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/semantic_string_less_than_equal_is_false.json");
update_item_case!(given_size_binary_mismatch_is_condition_failure_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/size_binary_mismatch_is_condition_failure.json");
update_item_case!(
    given_size_nested_contains_not_allowed_when_update_item_runs_then_conformance_response_matches,
    "field-condition_expression/400/size_nested_contains_not_allowed.json"
);
update_item_case!(given_size_string_mismatch_is_condition_failure_when_update_item_runs_then_conformance_response_matches, "field-condition_expression/400/size_string_mismatch_is_condition_failure.json");
update_item_case!(
    given_size_without_comparison_when_update_item_runs_then_conformance_response_matches,
    "field-condition_expression/400/size_without_comparison.json"
);

// Business rule: field-key/200.
update_item_case!(given_docs_limit_binary_partition_key_1_byte_valid_when_update_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_binary_partition_key_1_byte_valid.json");
update_item_case!(given_docs_limit_binary_partition_key_2048_bytes_valid_when_update_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_binary_partition_key_2048_bytes_valid.json");
update_item_case!(given_docs_limit_binary_sort_key_1024_bytes_valid_when_update_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_binary_sort_key_1024_bytes_valid.json");
update_item_case!(given_docs_limit_number_partition_key_38_digits_valid_when_update_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_number_partition_key_38_digits_valid.json");
update_item_case!(given_docs_limit_number_partition_key_positive_min_exponent_valid_when_update_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_number_partition_key_positive_min_exponent_valid.json");
update_item_case!(given_docs_limit_number_partition_key_zero_valid_when_update_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_number_partition_key_zero_valid.json");
update_item_case!(given_docs_limit_number_sort_key_38_digits_valid_when_update_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_number_sort_key_38_digits_valid.json");
update_item_case!(given_docs_limit_number_sort_key_positive_min_exponent_valid_when_update_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_number_sort_key_positive_min_exponent_valid.json");
update_item_case!(given_docs_limit_number_sort_key_zero_valid_when_update_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_number_sort_key_zero_valid.json");
update_item_case!(given_docs_limit_string_partition_key_2048_bytes_valid_when_update_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_string_partition_key_2048_bytes_valid.json");
update_item_case!(given_docs_limit_string_sort_key_1024_bytes_valid_when_update_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_string_sort_key_1024_bytes_valid.json");

// Validation and negative rule: field-key/400.
update_item_case!(given_docs_limit_binary_partition_key_2049_bytes_rejected_when_update_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_binary_partition_key_2049_bytes_rejected.json");
update_item_case!(given_docs_limit_binary_partition_key_empty_rejected_when_update_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_binary_partition_key_empty_rejected.json");
update_item_case!(given_docs_limit_binary_sort_key_1025_bytes_rejected_when_update_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_binary_sort_key_1025_bytes_rejected.json");
update_item_case!(given_docs_limit_binary_sort_key_empty_rejected_when_update_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_binary_sort_key_empty_rejected.json");
update_item_case!(given_docs_limit_number_partition_key_39_digits_rejected_when_update_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_number_partition_key_39_digits_rejected.json");
update_item_case!(given_docs_limit_number_partition_key_empty_rejected_when_update_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_number_partition_key_empty_rejected.json");
update_item_case!(given_docs_limit_number_partition_key_under_min_exponent_rejected_when_update_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_number_partition_key_under_min_exponent_rejected.json");
update_item_case!(given_docs_limit_number_sort_key_39_digits_rejected_when_update_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_number_sort_key_39_digits_rejected.json");
update_item_case!(given_docs_limit_number_sort_key_empty_rejected_when_update_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_number_sort_key_empty_rejected.json");
update_item_case!(given_docs_limit_number_sort_key_under_min_exponent_rejected_when_update_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_number_sort_key_under_min_exponent_rejected.json");
update_item_case!(given_docs_limit_string_partition_key_2049_bytes_rejected_when_update_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_string_partition_key_2049_bytes_rejected.json");
update_item_case!(given_docs_limit_string_partition_key_empty_rejected_when_update_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_string_partition_key_empty_rejected.json");
update_item_case!(given_docs_limit_string_sort_key_1025_bytes_rejected_when_update_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_string_sort_key_1025_bytes_rejected.json");
update_item_case!(given_docs_limit_string_sort_key_empty_rejected_when_update_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_string_sort_key_empty_rejected.json");

// Business rule: field-update_expression/200.
update_item_case!(given_action_sections_do_not_need_canonical_order_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/action_sections_do_not_need_canonical_order.json");
update_item_case!(
    given_add_nested_numeric_path_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/add_nested_numeric_path.json"
);
update_item_case!(
    given_alias_reserved_list_child_path_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/alias_reserved_list_child_path.json"
);
update_item_case!(
    given_alias_reserved_nested_path_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/alias_reserved_nested_path.json"
);
update_item_case!(given_arith_add_decimal_precision_all_new_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/arith_add_decimal_precision_all_new.json");
update_item_case!(given_arith_add_max_38_digit_number_valid_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/arith_add_max_38_digit_number_valid.json");
update_item_case!(given_arith_add_negative_decimal_all_new_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/arith_add_negative_decimal_all_new.json");
update_item_case!(given_arith_add_over_38_digit_result_valid_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/arith_add_over_38_digit_result_valid.json");
update_item_case!(
    given_arith_set_decimal_add_updated_new_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/arith_set_decimal_add_updated_new.json"
);
update_item_case!(given_arith_set_decimal_subtract_updated_old_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/arith_set_decimal_subtract_updated_old.json");
update_item_case!(given_arith_set_missing_result_created_from_existing_number_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/arith_set_missing_result_created_from_existing_number.json");
update_item_case!(given_arith_set_over_38_digit_result_valid_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/arith_set_over_38_digit_result_valid.json");
update_item_case!(
    given_arithmetic_add_left_path_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/arithmetic_add_left_path.json"
);
update_item_case!(
    given_arithmetic_add_left_value_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/arithmetic_add_left_value.json"
);
update_item_case!(
    given_bulk_add_number_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/bulk_add_number.json"
);
update_item_case!(
    given_bulk_add_number_set_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/bulk_add_number_set.json"
);
update_item_case!(
    given_bulk_add_string_set_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/bulk_add_string_set.json"
);
update_item_case!(
    given_bulk_append_list_by_high_index_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/bulk_append_list_by_high_index.json"
);
update_item_case!(
    given_bulk_arithmetic_subtract_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/bulk_arithmetic_subtract.json"
);
update_item_case!(
    given_bulk_delete_number_set_member_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/bulk_delete_number_set_member.json"
);
update_item_case!(
    given_bulk_delete_string_set_member_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/bulk_delete_string_set_member.json"
);
update_item_case!(
    given_bulk_if_not_exists_existing_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/bulk_if_not_exists_existing.json"
);
update_item_case!(
    given_bulk_if_not_exists_missing_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/bulk_if_not_exists_missing.json"
);
update_item_case!(
    given_bulk_list_append_head_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/bulk_list_append_head.json"
);
update_item_case!(
    given_bulk_list_append_tail_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/bulk_list_append_tail.json"
);
update_item_case!(
    given_bulk_remove_deep_nested_leaf_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/bulk_remove_deep_nested_leaf.json"
);
update_item_case!(
    given_bulk_remove_list_index_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/bulk_remove_list_index.json"
);
update_item_case!(
    given_bulk_remove_nested_child_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/bulk_remove_nested_child.json"
);
update_item_case!(
    given_bulk_remove_scalar_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/bulk_remove_scalar.json"
);
update_item_case!(
    given_bulk_set_deep_nested_leaf_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/bulk_set_deep_nested_leaf.json"
);
update_item_case!(
    given_bulk_set_list_index_child_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/bulk_set_list_index_child.json"
);
update_item_case!(
    given_bulk_set_list_index_map_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/bulk_set_list_index_map.json"
);
update_item_case!(
    given_bulk_set_list_index_string_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/bulk_set_list_index_string.json"
);
update_item_case!(
    given_bulk_set_nested_map_child_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/bulk_set_nested_map_child.json"
);
update_item_case!(
    given_bulk_set_scalar_bool_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/bulk_set_scalar_bool.json"
);
update_item_case!(
    given_bulk_set_scalar_null_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/bulk_set_scalar_null.json"
);
update_item_case!(
    given_bulk_set_scalar_number_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/bulk_set_scalar_number.json"
);
update_item_case!(
    given_bulk_set_scalar_string_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/bulk_set_scalar_string.json"
);
update_item_case!(
    given_bulk_set_then_remove_order_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/bulk_set_then_remove_order.json"
);
update_item_case!(
    given_delete_nested_set_path_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/delete_nested_set_path.json"
);
update_item_case!(given_docpath_remove_out_of_range_list_index_is_noop_all_new_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/docpath_remove_out_of_range_list_index_is_noop_all_new.json");
update_item_case!(given_docpath_return_all_new_complete_item_deep_map_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/docpath_return_all_new_complete_item_deep_map.json");
update_item_case!(given_docpath_return_all_new_complete_list_after_child_update_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/docpath_return_all_new_complete_list_after_child_update.json");
update_item_case!(given_docpath_return_all_old_complete_item_deep_map_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/docpath_return_all_old_complete_item_deep_map.json");
update_item_case!(given_docpath_return_all_old_complete_list_before_child_update_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/docpath_return_all_old_complete_list_before_child_update.json");
update_item_case!(given_docpath_updated_new_list_child_field_only_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/docpath_updated_new_list_child_field_only.json");
update_item_case!(given_docpath_updated_new_list_element_replacement_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/docpath_updated_new_list_element_replacement.json");
update_item_case!(given_docpath_updated_new_merges_sibling_nested_leaves_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/docpath_updated_new_merges_sibling_nested_leaves.json");
update_item_case!(given_docpath_updated_new_nested_add_number_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/docpath_updated_new_nested_add_number.json");
update_item_case!(given_docpath_updated_new_nested_map_leaf_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/docpath_updated_new_nested_map_leaf.json");
update_item_case!(given_docpath_updated_new_out_of_range_list_append_empty_body_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/docpath_updated_new_out_of_range_list_append_empty_body.json");
update_item_case!(given_docpath_updated_new_remove_deleted_leaf_empty_body_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/docpath_updated_new_remove_deleted_leaf_empty_body.json");
update_item_case!(given_docpath_updated_new_remove_list_index_shifted_element_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/docpath_updated_new_remove_list_index_shifted_element.json");
update_item_case!(given_docpath_updated_old_list_child_field_only_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/docpath_updated_old_list_child_field_only.json");
update_item_case!(given_docpath_updated_old_nested_map_leaf_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/docpath_updated_old_nested_map_leaf.json");
update_item_case!(given_docpath_updated_old_remove_deleted_leaf_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/docpath_updated_old_remove_deleted_leaf.json");
update_item_case!(given_docpath_updated_old_remove_list_index_removed_element_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/docpath_updated_old_remove_list_index_removed_element.json");
update_item_case!(given_docs_limit_expression_attribute_name_token_255_bytes_valid_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/docs_limit_expression_attribute_name_token_255_bytes_valid.json");
update_item_case!(given_docs_limit_expression_attribute_value_token_255_bytes_valid_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/docs_limit_expression_attribute_value_token_255_bytes_valid.json");
update_item_case!(given_docs_limit_update_expression_4096_bytes_valid_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/docs_limit_update_expression_4096_bytes_valid.json");
update_item_case!(given_list_index_at_or_beyond_length_appends_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/list_index_at_or_beyond_length_appends.json");
update_item_case!(given_parser_update_alias_reserved_child_after_list_valid_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/parser_update_alias_reserved_child_after_list_valid.json");
update_item_case!(given_parser_update_expression_commas_with_newlines_valid_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/parser_update_expression_commas_with_newlines_valid.json");
update_item_case!(given_parser_update_expression_lowercase_action_valid_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/parser_update_expression_lowercase_action_valid.json");
update_item_case!(given_parser_update_expression_newlines_and_tabs_valid_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/parser_update_expression_newlines_and_tabs_valid.json");
update_item_case!(given_semantic_add_existing_number_all_new_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/semantic_add_existing_number_all_new.json");
update_item_case!(given_semantic_add_existing_number_set_union_all_new_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/semantic_add_existing_number_set_union_all_new.json");
update_item_case!(given_semantic_add_existing_string_set_union_all_new_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/semantic_add_existing_string_set_union_all_new.json");
update_item_case!(given_semantic_add_missing_number_starts_from_zero_all_new_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/semantic_add_missing_number_starts_from_zero_all_new.json");
update_item_case!(given_semantic_add_missing_string_set_all_new_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/semantic_add_missing_string_set_all_new.json");
update_item_case!(given_semantic_add_negative_number_all_new_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/semantic_add_negative_number_all_new.json");
update_item_case!(given_semantic_delete_missing_set_noop_all_new_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/semantic_delete_missing_set_noop_all_new.json");
update_item_case!(given_semantic_delete_number_set_subset_all_new_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/semantic_delete_number_set_subset_all_new.json");
update_item_case!(given_semantic_delete_string_set_subset_all_new_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/semantic_delete_string_set_subset_all_new.json");
update_item_case!(given_semantic_if_not_exists_keeps_existing_all_new_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/semantic_if_not_exists_keeps_existing_all_new.json");
update_item_case!(given_semantic_if_not_exists_writes_missing_all_new_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/semantic_if_not_exists_writes_missing_all_new.json");
update_item_case!(
    given_semantic_list_append_head_all_new_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/semantic_list_append_head_all_new.json"
);
update_item_case!(given_semantic_list_append_if_not_exists_missing_all_new_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/semantic_list_append_if_not_exists_missing_all_new.json");
update_item_case!(
    given_semantic_list_append_tail_all_new_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/semantic_list_append_tail_all_new.json"
);
update_item_case!(given_semantic_remove_list_index_shifts_all_new_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/semantic_remove_list_index_shifts_all_new.json");
update_item_case!(given_semantic_remove_missing_attribute_noop_all_new_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/semantic_remove_missing_attribute_noop_all_new.json");
update_item_case!(
    given_semantic_remove_scalar_all_new_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/semantic_remove_scalar_all_new.json"
);
update_item_case!(
    given_semantic_return_values_all_old_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/semantic_return_values_all_old.json"
);
update_item_case!(given_semantic_return_values_updated_new_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/semantic_return_values_updated_new.json");
update_item_case!(given_semantic_return_values_updated_old_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/semantic_return_values_updated_old.json");
update_item_case!(given_semantic_set_arithmetic_path_plus_value_all_new_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/semantic_set_arithmetic_path_plus_value_all_new.json");
update_item_case!(given_semantic_set_arithmetic_subtract_all_new_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/semantic_set_arithmetic_subtract_all_new.json");
update_item_case!(given_semantic_set_arithmetic_value_plus_path_all_new_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/semantic_set_arithmetic_value_plus_path_all_new.json");
update_item_case!(given_semantic_set_high_list_index_appends_all_new_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/semantic_set_high_list_index_appends_all_new.json");
update_item_case!(given_semantic_set_list_index_replace_all_new_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/semantic_set_list_index_replace_all_new.json");
update_item_case!(given_semantic_set_nested_map_child_all_new_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/semantic_set_nested_map_child_all_new.json");
update_item_case!(
    given_semantic_set_scalar_all_new_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/semantic_set_scalar_all_new.json"
);
update_item_case!(
    given_set_add_binary_set_union_all_new_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/set_add_binary_set_union_all_new.json"
);
update_item_case!(
    given_set_add_binary_set_updated_new_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/200/set_add_binary_set_updated_new.json"
);
update_item_case!(given_set_add_missing_number_set_updated_new_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/set_add_missing_number_set_updated_new.json");
update_item_case!(given_set_delete_all_string_set_members_removes_attribute_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/set_delete_all_string_set_members_removes_attribute.json");
update_item_case!(given_set_delete_binary_set_subset_all_new_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/set_delete_binary_set_subset_all_new.json");
update_item_case!(given_set_delete_string_set_non_member_noop_all_new_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/set_delete_string_set_non_member_noop_all_new.json");
update_item_case!(given_set_delete_updated_new_returns_remaining_set_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/set_delete_updated_new_returns_remaining_set.json");
update_item_case!(given_set_delete_updated_old_returns_old_subset_when_update_item_runs_then_conformance_response_matches, "field-update_expression/200/set_delete_updated_old_returns_old_subset.json");

// Validation and negative rule: field-update_expression/400.
update_item_case!(
    given_add_list_operand_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/add_list_operand.json"
);
update_item_case!(
    given_add_literal_value_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/add_literal_value.json"
);
update_item_case!(
    given_add_without_value_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/add_without_value.json"
);
update_item_case!(
    given_alpha_list_index_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/alpha_list_index.json"
);
update_item_case!(given_arith_add_number_to_string_attribute_rejected_when_update_item_runs_then_conformance_response_matches, "field-update_expression/400/arith_add_number_to_string_attribute_rejected.json");
update_item_case!(given_arith_set_number_plus_string_path_rejected_when_update_item_runs_then_conformance_response_matches, "field-update_expression/400/arith_set_number_plus_string_path_rejected.json");
update_item_case!(
    given_arithmetic_missing_left_path_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/arithmetic_missing_left_path.json"
);
update_item_case!(
    given_arithmetic_missing_right_path_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/arithmetic_missing_right_path.json"
);
update_item_case!(
    given_arithmetic_scalar_right_path_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/arithmetic_scalar_right_path.json"
);
update_item_case!(given_attribute_type_function_not_allowed_when_update_item_runs_then_conformance_response_matches, "field-update_expression/400/attribute_type_function_not_allowed.json");
update_item_case!(
    given_begins_with_function_not_allowed_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/begins_with_function_not_allowed.json"
);
update_item_case!(
    given_contains_function_not_allowed_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/contains_function_not_allowed.json"
);
update_item_case!(
    given_delete_string_operand_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/delete_string_operand.json"
);
update_item_case!(
    given_delete_without_value_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/delete_without_value.json"
);
update_item_case!(given_docpath_overlap_alias_parent_child_rejected_when_update_item_runs_then_conformance_response_matches, "field-update_expression/400/docpath_overlap_alias_parent_child_rejected.json");
update_item_case!(given_docpath_overlap_alias_same_list_index_rejected_when_update_item_runs_then_conformance_response_matches, "field-update_expression/400/docpath_overlap_alias_same_list_index_rejected.json");
update_item_case!(given_docpath_overlap_remove_and_set_same_path_rejected_when_update_item_runs_then_conformance_response_matches, "field-update_expression/400/docpath_overlap_remove_and_set_same_path_rejected.json");
update_item_case!(given_docpath_remove_missing_nested_path_rejected_when_update_item_runs_then_conformance_response_matches, "field-update_expression/400/docpath_remove_missing_nested_path_rejected.json");
update_item_case!(given_docpath_set_list_scalar_child_rejected_when_update_item_runs_then_conformance_response_matches, "field-update_expression/400/docpath_set_list_scalar_child_rejected.json");
update_item_case!(given_docpath_set_missing_nested_map_parent_rejected_when_update_item_runs_then_conformance_response_matches, "field-update_expression/400/docpath_set_missing_nested_map_parent_rejected.json");
update_item_case!(given_docpath_set_missing_top_map_parent_rejected_when_update_item_runs_then_conformance_response_matches, "field-update_expression/400/docpath_set_missing_top_map_parent_rejected.json");
update_item_case!(given_docpath_set_scalar_map_parent_rejected_when_update_item_runs_then_conformance_response_matches, "field-update_expression/400/docpath_set_scalar_map_parent_rejected.json");
update_item_case!(given_docs_limit_expression_attribute_name_token_256_bytes_rejected_when_update_item_runs_then_conformance_response_matches, "field-update_expression/400/docs_limit_expression_attribute_name_token_256_bytes_rejected.json");
update_item_case!(given_docs_limit_expression_attribute_value_token_256_bytes_rejected_when_update_item_runs_then_conformance_response_matches, "field-update_expression/400/docs_limit_expression_attribute_value_token_256_bytes_rejected.json");
update_item_case!(given_docs_limit_update_expression_4097_bytes_rejected_when_update_item_runs_then_conformance_response_matches, "field-update_expression/400/docs_limit_update_expression_4097_bytes_rejected.json");
update_item_case!(
    given_document_path_list_scalar_element_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/document_path_list_scalar_element.json"
);
update_item_case!(given_document_path_missing_nested_parent_when_update_item_runs_then_conformance_response_matches, "field-update_expression/400/document_path_missing_nested_parent.json");
update_item_case!(given_document_path_missing_top_level_parent_when_update_item_runs_then_conformance_response_matches, "field-update_expression/400/document_path_missing_top_level_parent.json");
update_item_case!(
    given_document_path_scalar_parent_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/document_path_scalar_parent.json"
);
update_item_case!(
    given_duplicate_set_section_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/duplicate_set_section.json"
);
update_item_case!(
    given_empty_list_index_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/empty_list_index.json"
);
update_item_case!(given_if_not_exists_nested_size_not_allowed_when_update_item_runs_then_conformance_response_matches, "field-update_expression/400/if_not_exists_nested_size_not_allowed.json");
update_item_case!(given_invalid_expression_attribute_name_key_when_update_item_runs_then_conformance_response_matches, "field-update_expression/400/invalid_expression_attribute_name_key.json");
update_item_case!(given_invalid_expression_attribute_value_key_when_update_item_runs_then_conformance_response_matches, "field-update_expression/400/invalid_expression_attribute_value_key.json");
update_item_case!(given_list_append_nested_contains_not_allowed_when_update_item_runs_then_conformance_response_matches, "field-update_expression/400/list_append_nested_contains_not_allowed.json");
update_item_case!(
    given_list_child_reserved_word_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/list_child_reserved_word.json"
);
update_item_case!(
    given_missing_expression_attribute_name_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/missing_expression_attribute_name.json"
);
update_item_case!(
    given_nested_if_not_exists_not_allowed_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/nested_if_not_exists_not_allowed.json"
);
update_item_case!(
    given_nested_list_append_not_allowed_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/nested_list_append_not_allowed.json"
);
update_item_case!(
    given_overlapping_list_index_paths_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/overlapping_list_index_paths.json"
);
update_item_case!(
    given_overlapping_map_paths_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/overlapping_map_paths.json"
);
update_item_case!(given_parser_update_expression_if_not_exists_case_sensitive_rejected_when_update_item_runs_then_conformance_response_matches, "field-update_expression/400/parser_update_expression_if_not_exists_case_sensitive_rejected.json");
update_item_case!(given_parser_update_expression_mixed_case_list_append_rejected_when_update_item_runs_then_conformance_response_matches, "field-update_expression/400/parser_update_expression_mixed_case_list_append_rejected.json");
update_item_case!(given_parser_update_raw_reserved_child_after_list_rejected_when_update_item_runs_then_conformance_response_matches, "field-update_expression/400/parser_update_raw_reserved_child_after_list_rejected.json");
update_item_case!(
    given_repeated_path_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/repeated_path.json"
);
update_item_case!(
    given_reserved_word_count_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/reserved_word_count.json"
);
update_item_case!(given_semantic_add_list_operand_validation_when_update_item_runs_then_conformance_response_matches, "field-update_expression/400/semantic_add_list_operand_validation.json");
update_item_case!(given_semantic_delete_string_operand_validation_when_update_item_runs_then_conformance_response_matches, "field-update_expression/400/semantic_delete_string_operand_validation.json");
update_item_case!(given_semantic_list_append_scalar_left_operand_validation_when_update_item_runs_then_conformance_response_matches, "field-update_expression/400/semantic_list_append_scalar_left_operand_validation.json");
update_item_case!(
    given_set_add_empty_binary_set_rejected_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/set_add_empty_binary_set_rejected.json"
);
update_item_case!(
    given_set_add_empty_number_set_rejected_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/set_add_empty_number_set_rejected.json"
);
update_item_case!(
    given_set_add_empty_string_set_rejected_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/set_add_empty_string_set_rejected.json"
);
update_item_case!(
    given_set_add_set_to_scalar_rejected_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/set_add_set_to_scalar_rejected.json"
);
update_item_case!(
    given_set_add_wrong_set_type_rejected_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/set_add_wrong_set_type_rejected.json"
);
update_item_case!(given_set_delete_empty_string_set_rejected_when_update_item_runs_then_conformance_response_matches, "field-update_expression/400/set_delete_empty_string_set_rejected.json");
update_item_case!(given_set_delete_wrong_set_type_rejected_when_update_item_runs_then_conformance_response_matches, "field-update_expression/400/set_delete_wrong_set_type_rejected.json");
update_item_case!(
    given_set_trailing_plus_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/set_trailing_plus.json"
);
update_item_case!(
    given_set_without_assignment_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/set_without_assignment.json"
);
update_item_case!(
    given_size_function_not_allowed_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/size_function_not_allowed.json"
);
update_item_case!(
    given_unclosed_list_index_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/unclosed_list_index.json"
);
update_item_case!(
    given_unused_expression_attribute_name_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/unused_expression_attribute_name.json"
);
update_item_case!(
    given_unused_expression_attribute_value_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/unused_expression_attribute_value.json"
);
update_item_case!(
    given_update_expression_missing_value_when_update_item_runs_then_conformance_response_matches,
    "field-update_expression/400/update_expression_missing_value.json"
);
