//! Independently runnable DeleteItem cases from dynamo-conformance and local
//! rules.
//!
//! The fixture snapshot is from ../dynamo-conformance at
//! 2158288d7883de4be6c17da00226d8398faa723a. Each case retains its source
//! path and compares the complete Dynamo response through the replacement
//! store. Local cases use a `local/` source path.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::{Value, json};
use storage_observability_test_support::observation::observe;
use storage_types::{
    AttributeMap, DeleteItemRequest, GetItemRequest, KeyAttributes, PutItemRequest, TableName,
};

use super::test_support::{ConformanceFixtures, TestStore, unique_table_name};

static FIXTURES: ConformanceFixtures = ConformanceFixtures::new();

fn load_fixture(source: &str) -> Value {
    FIXTURES.case(include_str!("../fixtures/delete_item_cases.jsonl"), source)
}

async fn assert_delete_case(store: &TestStore, case: &Value) {
    assert_eq!(case["operation"], "DeleteItem", "fixture operation");
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

    let parsed = observe(async { DeleteItemRequest::try_from(case["request"].clone()) }).await;
    parsed.assert_no_warning_or_error();
    parsed.assert_database_calls_exact(&[]);
    let (status, body) = match parsed.result {
        Ok(request) => {
            let observed = observe(store.operations.delete_item(request)).await;
            observed.assert_no_warning_or_error();
            let operation =
                observed.one_span_with("dynamo_operation", &[("operation", "DeleteItem")]);
            assert_eq!(
                operation.fields.len(),
                1,
                "DeleteItem span must contain only safe context"
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
    assert_eq!(
        status, expected_status,
        "{} returned {body}",
        case["source"]
    );
    assert_eq!(
        serde_json::to_string(&body).unwrap(),
        serde_json::to_string(&case["expected"]["body"]).unwrap(),
        "{}",
        case["source"],
    );

    // A successful delete removes the target; a rejected delete retains any
    // seeded item. Read both outcomes through the shared Dynamo operation.
    let original_item = preconditions
        .last()
        .map(|precondition| &precondition["request"]["Item"]);
    let key = original_item.map_or_else(
        || case["request"]["Key"].clone(),
        |item| json!({"pk": item["pk"], "sk": item["sk"]}),
    );
    let invalid_key_case = case["source"]
        .as_str()
        .is_some_and(|source| source.starts_with("field-key/400"));
    let Ok(key) = serde_json::from_value::<KeyAttributes>(key) else {
        assert!(status == 400 && original_item.is_none() && invalid_key_case);
        return;
    };
    let read = store
        .operations
        .get_item(GetItemRequest::new(table, key))
        .await;
    let read = match read {
        Ok(read) => read,
        Err(_) if status == 400 && original_item.is_none() && invalid_key_case => return,
        Err(error) => panic!("{} readback failed: {error:?}", case["source"]),
    };
    let read = serde_json::to_value(read).unwrap();
    if status == 400 {
        if let Some(item) = original_item {
            let mut expected_item: AttributeMap = serde_json::from_value(item.clone()).unwrap();
            expected_item.normalize_numbers_for_write();
            assert_eq!(
                read["Item"],
                serde_json::to_value(expected_item).unwrap(),
                "{} rejected DeleteItem changed the item",
                case["source"],
            );
        } else {
            assert!(
                read.get("Item").is_none(),
                "{} created an item",
                case["source"]
            );
        }
    } else {
        assert!(
            read.get("Item").is_none(),
            "{} did not delete the item",
            case["source"]
        );
    }
}

macro_rules! delete_case {
    ($name:ident, $source:literal) => {
        #[tokio::test]
        async fn $name() {
            let case = load_fixture($source);
            let store = TestStore::new().await;
            let outcome = AssertUnwindSafe(assert_delete_case(&store, &case))
                .catch_unwind()
                .await;
            store.finish().await;
            if let Err(panic) = outcome {
                resume_unwind(panic);
            }
        }
    };
}

// Business rule from Cucumber: deleting an absent item has no old image.
delete_case!(
    given_absent_item_when_delete_requests_all_old_then_response_has_no_attributes,
    "local/absent_all_old.json"
);
delete_case!(
    given_absent_item_when_delete_checks_key_exists_then_condition_fails,
    "local/absent_attribute_exists.json"
);
delete_case!(
    given_absent_item_when_delete_checks_key_missing_then_noop_succeeds,
    "local/absent_attribute_not_exists.json"
);
delete_case!(
    given_absent_item_when_delete_compares_key_then_condition_fails,
    "local/absent_key_equality.json"
);

// Business and format rule: field-condition_expression/200.
delete_case!(given_field_condition_expression_200_bulk_and_precedence_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/bulk_and_precedence.json");
delete_case!(given_field_condition_expression_200_bulk_attribute_exists_and_contains_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/bulk_attribute_exists_and_contains.json");
delete_case!(given_field_condition_expression_200_bulk_attribute_exists_nested_child_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/bulk_attribute_exists_nested_child.json");
delete_case!(given_field_condition_expression_200_bulk_attribute_exists_pk_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/bulk_attribute_exists_pk.json");
delete_case!(given_field_condition_expression_200_bulk_attribute_not_exists_missing_child_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/bulk_attribute_not_exists_missing_child.json");
delete_case!(given_field_condition_expression_200_bulk_attribute_type_bool_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/bulk_attribute_type_bool.json");
delete_case!(given_field_condition_expression_200_bulk_attribute_type_null_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/bulk_attribute_type_null.json");
delete_case!(given_field_condition_expression_200_bulk_attribute_type_number_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/bulk_attribute_type_number.json");
delete_case!(given_field_condition_expression_200_bulk_attribute_type_string_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/bulk_attribute_type_string.json");
delete_case!(given_field_condition_expression_200_bulk_begins_with_string_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/bulk_begins_with_string.json");
delete_case!(given_field_condition_expression_200_bulk_contains_number_set_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/bulk_contains_number_set.json");
delete_case!(given_field_condition_expression_200_bulk_contains_string_set_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/bulk_contains_string_set.json");
delete_case!(given_field_condition_expression_200_bulk_contains_string_substring_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/bulk_contains_string_substring.json");
delete_case!(given_field_condition_expression_200_bulk_deep_nested_equality_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/bulk_deep_nested_equality.json");
delete_case!(given_field_condition_expression_200_bulk_list_index_map_child_equality_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/bulk_list_index_map_child_equality.json");
delete_case!(given_field_condition_expression_200_bulk_list_index_string_equality_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/bulk_list_index_string_equality.json");
delete_case!(given_field_condition_expression_200_bulk_nested_map_equality_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/bulk_nested_map_equality.json");
delete_case!(given_field_condition_expression_200_bulk_nested_number_comparison_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/bulk_nested_number_comparison.json");
delete_case!(given_field_condition_expression_200_bulk_not_attribute_exists_missing_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/bulk_not_attribute_exists_missing.json");
delete_case!(given_field_condition_expression_200_bulk_not_less_than_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/bulk_not_less_than.json");
delete_case!(given_field_condition_expression_200_bulk_number_between_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/bulk_number_between.json");
delete_case!(given_field_condition_expression_200_bulk_number_in_list_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/bulk_number_in_list.json");
delete_case!(given_field_condition_expression_200_bulk_parenthesized_or_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/bulk_parenthesized_or.json");
delete_case!(given_field_condition_expression_200_bulk_size_string_between_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/bulk_size_string_between.json");
delete_case!(given_field_condition_expression_200_bulk_size_string_equals_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/bulk_size_string_equals.json");
delete_case!(given_field_condition_expression_200_docs_limit_condition_expression_4096_bytes_valid_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/docs_limit_condition_expression_4096_bytes_valid.json");
delete_case!(given_field_condition_expression_200_docs_limit_expression_attribute_name_token_255_bytes_valid_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/docs_limit_expression_attribute_name_token_255_bytes_valid.json");
delete_case!(given_field_condition_expression_200_docs_limit_expression_attribute_value_token_255_bytes_valid_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/docs_limit_expression_attribute_value_token_255_bytes_valid.json");
delete_case!(given_field_condition_expression_200_docs_limit_in_100_operands_valid_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/docs_limit_in_100_operands_valid.json");
delete_case!(given_field_condition_expression_200_parser_condition_alias_nested_reserved_word_valid_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/parser_condition_alias_nested_reserved_word_valid.json");
delete_case!(given_field_condition_expression_200_parser_condition_newlines_tabs_valid_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/parser_condition_newlines_tabs_valid.json");
delete_case!(given_field_condition_expression_200_semantic_attribute_not_exists_missing_nested_child_true_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/semantic_attribute_not_exists_missing_nested_child_true.json");
delete_case!(given_field_condition_expression_200_semantic_attribute_type_binary_set_true_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/semantic_attribute_type_binary_set_true.json");
delete_case!(given_field_condition_expression_200_semantic_begins_with_binary_prefix_true_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/semantic_begins_with_binary_prefix_true.json");
delete_case!(given_field_condition_expression_200_semantic_condition_true_return_values_all_old_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/semantic_condition_true_return_values_all_old.json");
delete_case!(given_field_condition_expression_200_semantic_contains_binary_set_member_true_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/semantic_contains_binary_set_member_true.json");
delete_case!(given_field_condition_expression_200_semantic_in_matches_map_structurally_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/200/semantic_in_matches_map_structurally.json");

// Validation and negative rule: field-condition_expression/400.
delete_case!(given_field_condition_expression_400_alpha_list_index_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/400/alpha_list_index.json");
delete_case!(given_field_condition_expression_400_condition_failure_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/400/condition_failure.json");
delete_case!(given_field_condition_expression_400_condition_failure_returns_all_old_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/400/condition_failure_returns_all_old.json");
delete_case!(given_field_condition_expression_400_docs_limit_condition_expression_4097_bytes_rejected_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/400/docs_limit_condition_expression_4097_bytes_rejected.json");
delete_case!(given_field_condition_expression_400_docs_limit_expression_attribute_name_token_256_bytes_rejected_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/400/docs_limit_expression_attribute_name_token_256_bytes_rejected.json");
delete_case!(given_field_condition_expression_400_docs_limit_expression_attribute_value_token_256_bytes_rejected_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/400/docs_limit_expression_attribute_value_token_256_bytes_rejected.json");
delete_case!(given_field_condition_expression_400_docs_limit_expression_substitution_values_over_2mb_rejected_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/400/docs_limit_expression_substitution_values_over_2mb_rejected.json");
delete_case!(given_field_condition_expression_400_docs_limit_in_101_operands_rejected_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/400/docs_limit_in_101_operands_rejected.json");
delete_case!(given_field_condition_expression_400_list_child_reserved_word_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/400/list_child_reserved_word.json");
delete_case!(given_field_condition_expression_400_parser_condition_begins_with_case_sensitive_rejected_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/400/parser_condition_begins_with_case_sensitive_rejected.json");
delete_case!(given_field_condition_expression_400_parser_condition_double_parenthesized_comparison_rejected_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/400/parser_condition_double_parenthesized_comparison_rejected.json");
delete_case!(given_field_condition_expression_400_parser_missing_value_precedes_condition_failure_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/400/parser_missing_value_precedes_condition_failure.json");
delete_case!(given_field_condition_expression_400_semantic_between_string_reversed_bounds_validation_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/400/semantic_between_string_reversed_bounds_validation.json");
delete_case!(given_field_condition_expression_400_semantic_condition_failure_returns_all_old_complex_item_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/400/semantic_condition_failure_returns_all_old_complex_item.json");
delete_case!(given_field_condition_expression_400_semantic_contains_function_name_case_sensitive_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/400/semantic_contains_function_name_case_sensitive.json");
delete_case!(given_field_condition_expression_400_semantic_contains_list_partial_map_false_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/400/semantic_contains_list_partial_map_false.json");
delete_case!(given_field_condition_expression_400_semantic_number_string_equality_false_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/400/semantic_number_string_equality_false.json");
delete_case!(given_field_condition_expression_400_semantic_size_missing_attribute_false_when_delete_item_runs_then_conformance_response_matches, "field-condition_expression/400/semantic_size_missing_attribute_false.json");

// Boundary rule: field-key/200.
delete_case!(given_field_key_200_docs_limit_binary_partition_key_1_byte_valid_when_delete_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_binary_partition_key_1_byte_valid.json");
delete_case!(given_field_key_200_docs_limit_binary_partition_key_2048_bytes_valid_when_delete_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_binary_partition_key_2048_bytes_valid.json");
delete_case!(given_field_key_200_docs_limit_binary_sort_key_1024_bytes_valid_when_delete_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_binary_sort_key_1024_bytes_valid.json");
delete_case!(given_field_key_200_docs_limit_number_partition_key_38_digits_valid_when_delete_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_number_partition_key_38_digits_valid.json");
delete_case!(given_field_key_200_docs_limit_number_partition_key_positive_min_exponent_valid_when_delete_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_number_partition_key_positive_min_exponent_valid.json");
delete_case!(given_field_key_200_docs_limit_number_partition_key_zero_valid_when_delete_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_number_partition_key_zero_valid.json");
delete_case!(given_field_key_200_docs_limit_number_sort_key_38_digits_valid_when_delete_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_number_sort_key_38_digits_valid.json");
delete_case!(given_field_key_200_docs_limit_number_sort_key_positive_min_exponent_valid_when_delete_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_number_sort_key_positive_min_exponent_valid.json");
delete_case!(given_field_key_200_docs_limit_number_sort_key_zero_valid_when_delete_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_number_sort_key_zero_valid.json");
delete_case!(given_field_key_200_docs_limit_string_partition_key_2048_bytes_valid_when_delete_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_string_partition_key_2048_bytes_valid.json");
delete_case!(given_field_key_200_docs_limit_string_sort_key_1024_bytes_valid_when_delete_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_string_sort_key_1024_bytes_valid.json");

// Validation and negative rule: field-key/400.
delete_case!(given_field_key_400_docs_limit_binary_partition_key_2049_bytes_rejected_when_delete_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_binary_partition_key_2049_bytes_rejected.json");
delete_case!(given_field_key_400_docs_limit_binary_partition_key_empty_rejected_when_delete_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_binary_partition_key_empty_rejected.json");
delete_case!(given_field_key_400_docs_limit_binary_sort_key_1025_bytes_rejected_when_delete_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_binary_sort_key_1025_bytes_rejected.json");
delete_case!(given_field_key_400_docs_limit_binary_sort_key_empty_rejected_when_delete_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_binary_sort_key_empty_rejected.json");
delete_case!(given_field_key_400_docs_limit_number_partition_key_39_digits_rejected_when_delete_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_number_partition_key_39_digits_rejected.json");
delete_case!(given_field_key_400_docs_limit_number_partition_key_empty_rejected_when_delete_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_number_partition_key_empty_rejected.json");
delete_case!(given_field_key_400_docs_limit_number_partition_key_under_min_exponent_rejected_when_delete_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_number_partition_key_under_min_exponent_rejected.json");
delete_case!(given_field_key_400_docs_limit_number_sort_key_39_digits_rejected_when_delete_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_number_sort_key_39_digits_rejected.json");
delete_case!(given_field_key_400_docs_limit_number_sort_key_empty_rejected_when_delete_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_number_sort_key_empty_rejected.json");
delete_case!(given_field_key_400_docs_limit_number_sort_key_under_min_exponent_rejected_when_delete_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_number_sort_key_under_min_exponent_rejected.json");

delete_case!(given_field_key_400_docs_limit_string_partition_key_2049_bytes_rejected_when_delete_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_string_partition_key_2049_bytes_rejected.json");
delete_case!(given_field_key_400_docs_limit_string_partition_key_empty_rejected_when_delete_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_string_partition_key_empty_rejected.json");
delete_case!(given_field_key_400_docs_limit_string_sort_key_1025_bytes_rejected_when_delete_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_string_sort_key_1025_bytes_rejected.json");
delete_case!(given_field_key_400_docs_limit_string_sort_key_empty_rejected_when_delete_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_string_sort_key_empty_rejected.json");

/// Business rule from legacy KV DeleteItem and the DeleteItem Cucumber feature.
#[tokio::test]
async fn given_an_existing_item_when_delete_item_runs_twice_then_both_responses_are_empty_and_item_stays_absent()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        store
            .create_conformance_table("basic_pk_sk", table.clone())
            .await;
        let key = json!({"pk": {"S": "group"}, "sk": {"S": "item"}});
        let put = PutItemRequest::try_from(json!({
            "TableName": table.to_string(),
            "Item": {"pk": {"S": "group"}, "sk": {"S": "item"}, "value": {"S": "before"}}
        }))
        .unwrap();
        store.operations.put_item(put).await.unwrap();

        for _ in 0..2 {
            let request = DeleteItemRequest::new(
                table.clone(),
                serde_json::from_value::<KeyAttributes>(key.clone()).unwrap(),
            );
            let observed = observe(store.operations.delete_item(request)).await;
            observed.assert_no_warning_or_error();
            let response = observed.result.unwrap();
            assert_eq!(serde_json::to_value(response).unwrap(), json!({}));
            let read = store
                .operations
                .get_item(GetItemRequest::new(
                    table.clone(),
                    serde_json::from_value::<KeyAttributes>(key.clone()).unwrap(),
                ))
                .await
                .unwrap();
            assert_eq!(serde_json::to_value(read).unwrap(), json!({}));
        }
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
