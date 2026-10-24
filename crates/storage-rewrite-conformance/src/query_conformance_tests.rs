//! Independently runnable Query cases from dynamo-conformance and local rules.
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
    AttributeMap, CreateTableRequest, GetItemRequest, KeyAttributes, PutItemRequest, QueryRequest,
    TableName,
};

use crate::test_support::{ConformanceFixtures, TestStore, unique_table_name};

static FIXTURES: ConformanceFixtures = ConformanceFixtures::new();

fn fixture(source: &str) -> Value {
    FIXTURES.case(include_str!("../fixtures/query_cases.jsonl"), source)
}

async fn create_fixture_table(store: &TestStore, kind: &str, table: TableName) {
    if kind != "basic_with_gsi" {
        store.create_conformance_table(kind, table).await;
        return;
    }
    let request = CreateTableRequest::try_from(json!({
        "TableName": table.to_string(),
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gsi_pk", "AttributeType": "S"},
            {"AttributeName": "gsi_sk", "AttributeType": "S"}
        ],
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "GlobalSecondaryIndexes": [{
            "IndexName": "gsi1",
            "KeySchema": [
                {"AttributeName": "gsi_pk", "KeyType": "HASH"},
                {"AttributeName": "gsi_sk", "KeyType": "RANGE"}
            ],
            "Projection": {"ProjectionType": "ALL"}
        }],
        "BillingMode": "PAY_PER_REQUEST"
    }))
    .unwrap();
    store.operations.create_table(request).await.unwrap();
}

async fn run_case(store: &TestStore, case: &Value) {
    assert_eq!(case["operation"], "Query", "fixture operation");
    let table_kind = case["tables"][0].as_str().unwrap();
    let table = TableName::new(case["request"]["TableName"].as_str().unwrap());
    create_fixture_table(store, table_kind, table.clone()).await;

    let preconditions = case["preconditions"].as_array().unwrap();
    for precondition in preconditions {
        assert_eq!(
            precondition["operation"], "PutItem",
            "precondition operation"
        );
        let request = PutItemRequest::try_from(precondition["request"].clone()).unwrap();
        store.operations.put_item(request).await.unwrap();
    }

    let parsed = observe(async { QueryRequest::try_from(case["request"].clone()) }).await;
    parsed.assert_no_warning_or_error();
    parsed.assert_database_calls_exact(&[]);
    let (status, body) = match parsed.result {
        Ok(request) => {
            let observed = observe(store.operations.query(request)).await;
            observed.assert_no_warning_or_error();
            let operation = observed.one_span_with("dynamo_operation", &[("operation", "Query")]);
            assert_eq!(
                operation.fields.len(),
                1,
                "Query span must contain only safe context"
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

    // A Query is read-only, including validation failures and partial pages.
    for precondition in preconditions {
        let item = &precondition["request"]["Item"];
        let mut key = serde_json::Map::new();
        let partition = if table_kind == "count_hash_key" {
            "COUNT"
        } else {
            "pk"
        };
        key.insert(partition.to_owned(), item[partition].clone());
        if table_kind != "count_hash_key" {
            key.insert("sk".to_owned(), item["sk"].clone());
        }
        let read = store
            .operations
            .get_item(GetItemRequest::new(
                table.clone(),
                serde_json::from_value::<KeyAttributes>(Value::Object(key)).unwrap(),
            ))
            .await
            .unwrap();
        let read = serde_json::to_value(read).unwrap();
        let mut expected_item: AttributeMap = serde_json::from_value(item.clone()).unwrap();
        expected_item.normalize_numbers_for_write();
        assert_eq!(
            read["Item"],
            serde_json::to_value(expected_item).unwrap(),
            "{} read-only Query changed stored item",
            case["source"],
        );
    }
}

macro_rules! query_case {
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

// Boundary rule from legacy SQL: a 38 digit numeric key survives both cursor
// directions.
query_case!(
    given_precise_numeric_cursor_when_query_resumes_forward_then_next_item_and_key_are_exact,
    "local/precise_numeric_forward_cursor.json"
);
query_case!(
    given_precise_numeric_cursor_when_query_resumes_reverse_then_next_item_and_key_are_exact,
    "local/precise_numeric_reverse_cursor.json"
);

// Business and format rule from legacy SQL and canary: GSI reads expose
// complete public items.
query_case!(
    given_gsi_rows_when_query_projects_attributes_then_ordered_public_items_are_exact,
    "local/gsi_query_projected_rows.json"
);

// Validation and negative rule: 400/gsi_consistent_read_validation.json.
query_case!(
    given_400_gsi_consistent_read_validation_when_query_runs_then_conformance_response_matches,
    "400/gsi_consistent_read_validation.json"
);

// Boundary rule: field-exclusive_start_key/200.
query_case!(given_field_exclusive_start_key_200_nonexistent_start_key_returns_empty_page_when_query_runs_then_conformance_response_matches, "field-exclusive_start_key/200/nonexistent_start_key_returns_empty_page.json");

// Validation and negative rule: field-exclusive_start_key/400.
query_case!(given_field_exclusive_start_key_400_gsi_missing_base_table_keys_when_query_runs_then_conformance_response_matches, "field-exclusive_start_key/400/gsi_missing_base_table_keys.json");
query_case!(given_field_exclusive_start_key_400_missing_sort_key_when_query_runs_then_conformance_response_matches, "field-exclusive_start_key/400/missing_sort_key.json");
query_case!(given_field_exclusive_start_key_400_wrong_hash_key_type_when_query_runs_then_conformance_response_matches, "field-exclusive_start_key/400/wrong_hash_key_type.json");

// Business and format rule: field-filter_expression/200.
query_case!(given_field_filter_expression_200_parity_filter_and_precedence_inside_or_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_and_precedence_inside_or.json");
query_case!(given_field_filter_expression_200_parity_filter_attribute_exists_and_contains_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_attribute_exists_and_contains.json");
query_case!(given_field_filter_expression_200_parity_filter_attribute_exists_nested_child_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_attribute_exists_nested_child.json");
query_case!(given_field_filter_expression_200_parity_filter_attribute_exists_pk_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_attribute_exists_pk.json");
query_case!(given_field_filter_expression_200_parity_filter_attribute_not_exists_absent_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_attribute_not_exists_absent.json");
query_case!(given_field_filter_expression_200_parity_filter_attribute_not_exists_nested_absent_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_attribute_not_exists_nested_absent.json");
query_case!(given_field_filter_expression_200_parity_filter_attribute_type_binary_set_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_attribute_type_binary_set.json");
query_case!(given_field_filter_expression_200_parity_filter_attribute_type_bool_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_attribute_type_bool.json");
query_case!(given_field_filter_expression_200_parity_filter_attribute_type_list_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_attribute_type_list.json");
query_case!(given_field_filter_expression_200_parity_filter_attribute_type_map_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_attribute_type_map.json");
query_case!(given_field_filter_expression_200_parity_filter_attribute_type_null_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_attribute_type_null.json");
query_case!(given_field_filter_expression_200_parity_filter_attribute_type_number_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_attribute_type_number.json");
query_case!(given_field_filter_expression_200_parity_filter_attribute_type_number_set_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_attribute_type_number_set.json");
query_case!(given_field_filter_expression_200_parity_filter_attribute_type_string_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_attribute_type_string.json");
query_case!(given_field_filter_expression_200_parity_filter_attribute_type_string_set_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_attribute_type_string_set.json");
query_case!(given_field_filter_expression_200_parity_filter_begins_with_binary_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_begins_with_binary.json");
query_case!(given_field_filter_expression_200_parity_filter_begins_with_string_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_begins_with_string.json");
query_case!(given_field_filter_expression_200_parity_filter_between_then_and_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_between_then_and.json");
query_case!(given_field_filter_expression_200_parity_filter_bool_equality_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_bool_equality.json");
query_case!(given_field_filter_expression_200_parity_filter_contains_binary_set_exact_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_contains_binary_set_exact.json");
query_case!(given_field_filter_expression_200_parity_filter_contains_list_number_exact_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_contains_list_number_exact.json");
query_case!(given_field_filter_expression_200_parity_filter_contains_list_string_exact_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_contains_list_string_exact.json");
query_case!(given_field_filter_expression_200_parity_filter_contains_number_set_exact_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_contains_number_set_exact.json");
query_case!(given_field_filter_expression_200_parity_filter_contains_string_set_exact_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_contains_string_set_exact.json");
query_case!(given_field_filter_expression_200_parity_filter_contains_string_substring_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_contains_string_substring.json");
query_case!(given_field_filter_expression_200_parity_filter_deep_nested_leaf_equality_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_deep_nested_leaf_equality.json");
query_case!(given_field_filter_expression_200_parity_filter_list_index_map_child_equality_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_list_index_map_child_equality.json");
query_case!(given_field_filter_expression_200_parity_filter_list_index_number_equality_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_list_index_number_equality.json");
query_case!(given_field_filter_expression_200_parity_filter_list_index_string_equality_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_list_index_string_equality.json");
query_case!(given_field_filter_expression_200_parity_filter_nested_map_child_equality_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_nested_map_child_equality.json");
query_case!(given_field_filter_expression_200_parity_filter_not_begins_with_or_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_not_begins_with_or.json");
query_case!(given_field_filter_expression_200_parity_filter_not_false_equality_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_not_false_equality.json");
query_case!(given_field_filter_expression_200_parity_filter_null_equality_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_null_equality.json");
query_case!(given_field_filter_expression_200_parity_filter_number_between_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_number_between.json");
query_case!(given_field_filter_expression_200_parity_filter_number_greater_equal_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_number_greater_equal.json");
query_case!(given_field_filter_expression_200_parity_filter_number_greater_than_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_number_greater_than.json");
query_case!(given_field_filter_expression_200_parity_filter_number_in_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_number_in.json");
query_case!(given_field_filter_expression_200_parity_filter_number_less_equal_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_number_less_equal.json");
query_case!(given_field_filter_expression_200_parity_filter_number_less_than_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_number_less_than.json");
query_case!(given_field_filter_expression_200_parity_filter_parenthesized_or_then_and_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_parenthesized_or_then_and.json");
query_case!(given_field_filter_expression_200_parity_filter_size_binary_equal_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_size_binary_equal.json");
query_case!(given_field_filter_expression_200_parity_filter_size_binary_set_equal_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_size_binary_set_equal.json");
query_case!(given_field_filter_expression_200_parity_filter_size_list_equal_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_size_list_equal.json");
query_case!(given_field_filter_expression_200_parity_filter_size_map_equal_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_size_map_equal.json");
query_case!(given_field_filter_expression_200_parity_filter_size_nested_map_equal_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_size_nested_map_equal.json");
query_case!(given_field_filter_expression_200_parity_filter_size_number_set_equal_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_size_number_set_equal.json");
query_case!(given_field_filter_expression_200_parity_filter_size_string_equal_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_size_string_equal.json");
query_case!(given_field_filter_expression_200_parity_filter_size_string_set_equal_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_size_string_set_equal.json");
query_case!(given_field_filter_expression_200_parity_filter_string_equality_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_string_equality.json");
query_case!(given_field_filter_expression_200_parity_filter_string_in_when_query_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_string_in.json");

// Validation and negative rule: field-filter_expression/400.
query_case!(given_field_filter_expression_400_parity_alpha_list_index_when_query_runs_then_conformance_response_matches, "field-filter_expression/400/parity_alpha_list_index.json");
query_case!(given_field_filter_expression_400_parity_begins_with_no_operands_when_query_runs_then_conformance_response_matches, "field-filter_expression/400/parity_begins_with_no_operands.json");
query_case!(given_field_filter_expression_400_parity_contains_no_operands_when_query_runs_then_conformance_response_matches, "field-filter_expression/400/parity_contains_no_operands.json");
query_case!(given_field_filter_expression_400_parity_contains_one_operand_when_query_runs_then_conformance_response_matches, "field-filter_expression/400/parity_contains_one_operand.json");
query_case!(given_field_filter_expression_400_parity_empty_list_index_when_query_runs_then_conformance_response_matches, "field-filter_expression/400/parity_empty_list_index.json");
query_case!(given_field_filter_expression_400_parity_invalid_expression_attribute_name_key_when_query_runs_then_conformance_response_matches, "field-filter_expression/400/parity_invalid_expression_attribute_name_key.json");
query_case!(given_field_filter_expression_400_parity_invalid_expression_attribute_value_key_when_query_runs_then_conformance_response_matches, "field-filter_expression/400/parity_invalid_expression_attribute_value_key.json");
query_case!(given_field_filter_expression_400_parity_list_raw_reserved_word_when_query_runs_then_conformance_response_matches, "field-filter_expression/400/parity_list_raw_reserved_word.json");
query_case!(given_field_filter_expression_400_parity_missing_expression_attribute_name_when_query_runs_then_conformance_response_matches, "field-filter_expression/400/parity_missing_expression_attribute_name.json");
query_case!(given_field_filter_expression_400_parity_missing_expression_attribute_value_when_query_runs_then_conformance_response_matches, "field-filter_expression/400/parity_missing_expression_attribute_value.json");
query_case!(given_field_filter_expression_400_parity_negative_list_index_when_query_runs_then_conformance_response_matches, "field-filter_expression/400/parity_negative_list_index.json");
query_case!(given_field_filter_expression_400_parity_nested_reserved_word_comment_when_query_runs_then_conformance_response_matches, "field-filter_expression/400/parity_nested_reserved_word_comment.json");
query_case!(given_field_filter_expression_400_parity_reserved_word_comment_when_query_runs_then_conformance_response_matches, "field-filter_expression/400/parity_reserved_word_comment.json");
query_case!(given_field_filter_expression_400_parity_size_without_comparison_when_query_runs_then_conformance_response_matches, "field-filter_expression/400/parity_size_without_comparison.json");
query_case!(given_field_filter_expression_400_parity_unused_expression_attribute_name_when_query_runs_then_conformance_response_matches, "field-filter_expression/400/parity_unused_expression_attribute_name.json");
query_case!(given_field_filter_expression_400_parity_unused_expression_attribute_value_when_query_runs_then_conformance_response_matches, "field-filter_expression/400/parity_unused_expression_attribute_value.json");

// Business and format rule: field-key_condition_expression/200.
query_case!(given_field_key_condition_expression_200_aliased_reserved_word_count_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/aliased_reserved_word_count.json");
query_case!(given_field_key_condition_expression_200_docs_limit_binary_partition_key_1_byte_valid_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/docs_limit_binary_partition_key_1_byte_valid.json");
query_case!(given_field_key_condition_expression_200_docs_limit_binary_partition_key_2048_bytes_valid_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/docs_limit_binary_partition_key_2048_bytes_valid.json");
query_case!(given_field_key_condition_expression_200_docs_limit_binary_sort_key_1024_bytes_valid_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/docs_limit_binary_sort_key_1024_bytes_valid.json");
query_case!(given_field_key_condition_expression_200_docs_limit_number_partition_key_38_digits_valid_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/docs_limit_number_partition_key_38_digits_valid.json");
query_case!(given_field_key_condition_expression_200_docs_limit_number_partition_key_positive_min_exponent_valid_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/docs_limit_number_partition_key_positive_min_exponent_valid.json");
query_case!(given_field_key_condition_expression_200_docs_limit_number_sort_key_38_digits_valid_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/docs_limit_number_sort_key_38_digits_valid.json");
query_case!(given_field_key_condition_expression_200_parity_key_alias_both_begins_with_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_alias_both_begins_with.json");
query_case!(given_field_key_condition_expression_200_parity_key_alias_both_between_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_alias_both_between.json");
query_case!(given_field_key_condition_expression_200_parity_key_alias_pk_eq_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_alias_pk_eq.json");
query_case!(given_field_key_condition_expression_200_parity_key_alias_sk_eq_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_alias_sk_eq.json");
query_case!(given_field_key_condition_expression_200_parity_key_begins_absent_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_begins_absent.json");
query_case!(given_field_key_condition_expression_200_parity_key_begins_full_first_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_begins_full_first.json");
query_case!(given_field_key_condition_expression_200_parity_key_begins_full_last_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_begins_full_last.json");
query_case!(given_field_key_condition_expression_200_parity_key_begins_sk_0_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_begins_sk_0.json");
query_case!(given_field_key_condition_expression_200_parity_key_begins_sk_00_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_begins_sk_00.json");
query_case!(given_field_key_condition_expression_200_parity_key_begins_sk_01_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_begins_sk_01.json");
query_case!(given_field_key_condition_expression_200_parity_key_begins_sk_02_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_begins_sk_02.json");
query_case!(given_field_key_condition_expression_200_parity_key_begins_sk_dash_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_begins_sk_dash.json");
query_case!(given_field_key_condition_expression_200_parity_key_begins_zz_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_begins_zz.json");
query_case!(given_field_key_condition_expression_200_parity_key_between_after_all_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_between_after_all.json");
query_case!(given_field_key_condition_expression_200_parity_key_between_all_sk_prefix_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_between_all_sk_prefix.json");
query_case!(given_field_key_condition_expression_200_parity_key_between_before_all_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_between_before_all.json");
query_case!(given_field_key_condition_expression_200_parity_key_between_exact_last_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_between_exact_last.json");
query_case!(given_field_key_condition_expression_200_parity_key_between_first_two_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_between_first_two.json");
query_case!(given_field_key_condition_expression_200_parity_key_between_last_two_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_between_last_two.json");
query_case!(given_field_key_condition_expression_200_parity_key_between_middle_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_between_middle.json");
query_case!(given_field_key_condition_expression_200_parity_key_between_none_gap_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_between_none_gap.json");
query_case!(given_field_key_condition_expression_200_parity_key_between_single_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_between_single.json");
query_case!(given_field_key_condition_expression_200_parity_key_between_upper_tail_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_between_upper_tail.json");
query_case!(given_field_key_condition_expression_200_parity_key_eq_1_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_eq_1.json");
query_case!(given_field_key_condition_expression_200_parity_key_eq_2_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_eq_2.json");
query_case!(given_field_key_condition_expression_200_parity_key_eq_3_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_eq_3.json");
query_case!(given_field_key_condition_expression_200_parity_key_eq_4_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_eq_4.json");
query_case!(given_field_key_condition_expression_200_parity_key_eq_5_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_eq_5.json");
query_case!(given_field_key_condition_expression_200_parity_key_extra_spaces_eq_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_extra_spaces_eq.json");
query_case!(given_field_key_condition_expression_200_parity_key_ge_1_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_ge_1.json");
query_case!(given_field_key_condition_expression_200_parity_key_ge_2_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_ge_2.json");
query_case!(given_field_key_condition_expression_200_parity_key_ge_3_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_ge_3.json");
query_case!(given_field_key_condition_expression_200_parity_key_ge_4_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_ge_4.json");
query_case!(given_field_key_condition_expression_200_parity_key_ge_5_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_ge_5.json");
query_case!(given_field_key_condition_expression_200_parity_key_gt_1_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_gt_1.json");
query_case!(given_field_key_condition_expression_200_parity_key_gt_2_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_gt_2.json");
query_case!(given_field_key_condition_expression_200_parity_key_gt_3_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_gt_3.json");
query_case!(given_field_key_condition_expression_200_parity_key_gt_4_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_gt_4.json");
query_case!(given_field_key_condition_expression_200_parity_key_gt_5_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_gt_5.json");
query_case!(given_field_key_condition_expression_200_parity_key_hash_only_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_hash_only.json");
query_case!(given_field_key_condition_expression_200_parity_key_hash_only_alias_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_hash_only_alias.json");
query_case!(given_field_key_condition_expression_200_parity_key_hash_only_extra_spaces_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_hash_only_extra_spaces.json");
query_case!(given_field_key_condition_expression_200_parity_key_hash_only_newline_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_hash_only_newline.json");
query_case!(given_field_key_condition_expression_200_parity_key_hash_only_tabs_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_hash_only_tabs.json");
query_case!(given_field_key_condition_expression_200_parity_key_le_1_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_le_1.json");
query_case!(given_field_key_condition_expression_200_parity_key_le_2_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_le_2.json");
query_case!(given_field_key_condition_expression_200_parity_key_le_3_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_le_3.json");
query_case!(given_field_key_condition_expression_200_parity_key_le_4_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_le_4.json");
query_case!(given_field_key_condition_expression_200_parity_key_le_5_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_le_5.json");
query_case!(given_field_key_condition_expression_200_parity_key_lt_1_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_lt_1.json");
query_case!(given_field_key_condition_expression_200_parity_key_lt_2_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_lt_2.json");
query_case!(given_field_key_condition_expression_200_parity_key_lt_3_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_lt_3.json");
query_case!(given_field_key_condition_expression_200_parity_key_lt_4_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_lt_4.json");
query_case!(given_field_key_condition_expression_200_parity_key_lt_5_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_lt_5.json");
query_case!(given_field_key_condition_expression_200_parity_key_mixed_case_begins_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_mixed_case_begins.json");
query_case!(given_field_key_condition_expression_200_parity_key_newline_between_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_newline_between.json");
query_case!(given_field_key_condition_expression_200_parity_key_tab_begins_with_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_tab_begins_with.json");
query_case!(given_field_key_condition_expression_200_parity_key_uppercase_and_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/200/parity_key_uppercase_and.json");

// Validation and negative rule: field-key_condition_expression/400.
query_case!(given_field_key_condition_expression_400_docs_limit_binary_partition_key_2049_bytes_rejected_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/400/docs_limit_binary_partition_key_2049_bytes_rejected.json");
query_case!(given_field_key_condition_expression_400_docs_limit_binary_partition_key_empty_rejected_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/400/docs_limit_binary_partition_key_empty_rejected.json");
query_case!(given_field_key_condition_expression_400_docs_limit_binary_sort_key_1025_bytes_rejected_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/400/docs_limit_binary_sort_key_1025_bytes_rejected.json");
query_case!(given_field_key_condition_expression_400_docs_limit_binary_sort_key_empty_rejected_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/400/docs_limit_binary_sort_key_empty_rejected.json");
query_case!(given_field_key_condition_expression_400_docs_limit_number_partition_key_39_digits_rejected_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/400/docs_limit_number_partition_key_39_digits_rejected.json");
query_case!(given_field_key_condition_expression_400_docs_limit_number_partition_key_empty_rejected_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/400/docs_limit_number_partition_key_empty_rejected.json");
query_case!(given_field_key_condition_expression_400_docs_limit_number_partition_key_under_min_exponent_rejected_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/400/docs_limit_number_partition_key_under_min_exponent_rejected.json");
query_case!(given_field_key_condition_expression_400_docs_limit_number_sort_key_39_digits_rejected_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/400/docs_limit_number_sort_key_39_digits_rejected.json");
query_case!(given_field_key_condition_expression_400_docs_limit_number_sort_key_empty_rejected_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/400/docs_limit_number_sort_key_empty_rejected.json");
query_case!(given_field_key_condition_expression_400_docs_limit_number_sort_key_under_min_exponent_rejected_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/400/docs_limit_number_sort_key_under_min_exponent_rejected.json");
query_case!(given_field_key_condition_expression_400_invalid_expression_attribute_value_key_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/400/invalid_expression_attribute_value_key.json");
query_case!(given_field_key_condition_expression_400_missing_expression_attribute_value_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/400/missing_expression_attribute_value.json");
query_case!(given_field_key_condition_expression_400_parity_key_begins_empty_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/400/parity_key_begins_empty.json");
query_case!(given_field_key_condition_expression_400_reserved_word_count_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/400/reserved_word_count.json");
query_case!(given_field_key_condition_expression_400_trailing_equals_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/400/trailing_equals.json");
query_case!(given_field_key_condition_expression_400_unused_expression_attribute_value_when_query_runs_then_conformance_response_matches, "field-key_condition_expression/400/unused_expression_attribute_value.json");

// Boundary rule: field-limit/200.
query_case!(given_field_limit_200_parity_limit_exclusive_start_after_sk_001_when_query_runs_then_conformance_response_matches, "field-limit/200/parity_limit_exclusive_start_after_sk-001.json");
query_case!(given_field_limit_200_parity_limit_exclusive_start_after_sk_001_limit_1_when_query_runs_then_conformance_response_matches, "field-limit/200/parity_limit_exclusive_start_after_sk-001_limit_1.json");
query_case!(given_field_limit_200_parity_limit_exclusive_start_after_sk_002_when_query_runs_then_conformance_response_matches, "field-limit/200/parity_limit_exclusive_start_after_sk-002.json");
query_case!(given_field_limit_200_parity_limit_exclusive_start_after_sk_002_limit_1_when_query_runs_then_conformance_response_matches, "field-limit/200/parity_limit_exclusive_start_after_sk-002_limit_1.json");
query_case!(given_field_limit_200_parity_limit_exclusive_start_after_sk_002_limit_2_when_query_runs_then_conformance_response_matches, "field-limit/200/parity_limit_exclusive_start_after_sk-002_limit_2.json");
query_case!(given_field_limit_200_parity_limit_exclusive_start_after_sk_010_when_query_runs_then_conformance_response_matches, "field-limit/200/parity_limit_exclusive_start_after_sk-010.json");
query_case!(given_field_limit_200_parity_limit_exclusive_start_after_sk_010_limit_1_when_query_runs_then_conformance_response_matches, "field-limit/200/parity_limit_exclusive_start_after_sk-010_limit_1.json");
query_case!(given_field_limit_200_parity_limit_exclusive_start_after_sk_020_when_query_runs_then_conformance_response_matches, "field-limit/200/parity_limit_exclusive_start_after_sk-020.json");
query_case!(given_field_limit_200_parity_limit_forward_limit_1_when_query_runs_then_conformance_response_matches, "field-limit/200/parity_limit_forward_limit_1.json");
query_case!(given_field_limit_200_parity_limit_forward_limit_2_when_query_runs_then_conformance_response_matches, "field-limit/200/parity_limit_forward_limit_2.json");
query_case!(given_field_limit_200_parity_limit_forward_limit_3_when_query_runs_then_conformance_response_matches, "field-limit/200/parity_limit_forward_limit_3.json");
query_case!(given_field_limit_200_parity_limit_forward_limit_4_when_query_runs_then_conformance_response_matches, "field-limit/200/parity_limit_forward_limit_4.json");
query_case!(given_field_limit_200_parity_limit_forward_limit_5_when_query_runs_then_conformance_response_matches, "field-limit/200/parity_limit_forward_limit_5.json");
query_case!(given_field_limit_200_parity_limit_forward_limit_6_when_query_runs_then_conformance_response_matches, "field-limit/200/parity_limit_forward_limit_6.json");
query_case!(given_field_limit_200_parity_limit_limit_3_filter_tail_when_query_runs_then_conformance_response_matches, "field-limit/200/parity_limit_limit_3_filter_tail.json");
query_case!(given_field_limit_200_parity_limit_limit_4_filter_g2_when_query_runs_then_conformance_response_matches, "field-limit/200/parity_limit_limit_4_filter_g2.json");
query_case!(given_field_limit_200_parity_limit_reverse_exclusive_start_after_sk_001_when_query_runs_then_conformance_response_matches, "field-limit/200/parity_limit_reverse_exclusive_start_after_sk-001.json");
query_case!(given_field_limit_200_parity_limit_reverse_exclusive_start_after_sk_002_when_query_runs_then_conformance_response_matches, "field-limit/200/parity_limit_reverse_exclusive_start_after_sk-002.json");
query_case!(given_field_limit_200_parity_limit_reverse_exclusive_start_after_sk_010_when_query_runs_then_conformance_response_matches, "field-limit/200/parity_limit_reverse_exclusive_start_after_sk-010.json");
query_case!(given_field_limit_200_parity_limit_reverse_exclusive_start_after_sk_020_when_query_runs_then_conformance_response_matches, "field-limit/200/parity_limit_reverse_exclusive_start_after_sk-020.json");
query_case!(given_field_limit_200_parity_limit_reverse_limit_1_when_query_runs_then_conformance_response_matches, "field-limit/200/parity_limit_reverse_limit_1.json");
query_case!(given_field_limit_200_parity_limit_reverse_limit_2_when_query_runs_then_conformance_response_matches, "field-limit/200/parity_limit_reverse_limit_2.json");
query_case!(given_field_limit_200_parity_limit_reverse_limit_3_when_query_runs_then_conformance_response_matches, "field-limit/200/parity_limit_reverse_limit_3.json");
query_case!(given_field_limit_200_parity_limit_reverse_limit_4_when_query_runs_then_conformance_response_matches, "field-limit/200/parity_limit_reverse_limit_4.json");
query_case!(given_field_limit_200_parity_limit_reverse_limit_5_when_query_runs_then_conformance_response_matches, "field-limit/200/parity_limit_reverse_limit_5.json");
query_case!(given_field_limit_200_parity_limit_reverse_limit_6_when_query_runs_then_conformance_response_matches, "field-limit/200/parity_limit_reverse_limit_6.json");

// Validation and negative rule: field-projection_expression/400.
query_case!(given_field_projection_expression_400_invalid_expression_attribute_name_key_when_query_runs_then_conformance_response_matches, "field-projection_expression/400/invalid_expression_attribute_name_key.json");
query_case!(given_field_projection_expression_400_missing_expression_attribute_name_when_query_runs_then_conformance_response_matches, "field-projection_expression/400/missing_expression_attribute_name.json");
query_case!(given_field_projection_expression_400_parity_alpha_list_index_when_query_runs_then_conformance_response_matches, "field-projection_expression/400/parity_alpha_list_index.json");
query_case!(given_field_projection_expression_400_parity_empty_list_index_when_query_runs_then_conformance_response_matches, "field-projection_expression/400/parity_empty_list_index.json");
query_case!(given_field_projection_expression_400_parity_invalid_expression_attribute_name_key_when_query_runs_then_conformance_response_matches, "field-projection_expression/400/parity_invalid_expression_attribute_name_key.json");
query_case!(given_field_projection_expression_400_parity_list_raw_reserved_word_when_query_runs_then_conformance_response_matches, "field-projection_expression/400/parity_list_raw_reserved_word.json");
query_case!(given_field_projection_expression_400_parity_missing_expression_attribute_name_when_query_runs_then_conformance_response_matches, "field-projection_expression/400/parity_missing_expression_attribute_name.json");
query_case!(given_field_projection_expression_400_parity_negative_list_index_when_query_runs_then_conformance_response_matches, "field-projection_expression/400/parity_negative_list_index.json");
query_case!(given_field_projection_expression_400_parity_nested_reserved_word_comment_when_query_runs_then_conformance_response_matches, "field-projection_expression/400/parity_nested_reserved_word_comment.json");
query_case!(given_field_projection_expression_400_parity_overlapping_parent_and_child_when_query_runs_then_conformance_response_matches, "field-projection_expression/400/parity_overlapping_parent_and_child.json");
query_case!(given_field_projection_expression_400_parity_reserved_word_comment_when_query_runs_then_conformance_response_matches, "field-projection_expression/400/parity_reserved_word_comment.json");
query_case!(given_field_projection_expression_400_parity_unused_expression_attribute_name_when_query_runs_then_conformance_response_matches, "field-projection_expression/400/parity_unused_expression_attribute_name.json");

// Business and format rule: field-select/200.
query_case!(given_field_select_200_parity_select_count_eq_1_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_eq_1.json");
query_case!(given_field_select_200_parity_select_count_eq_2_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_eq_2.json");
query_case!(given_field_select_200_parity_select_count_eq_3_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_eq_3.json");
query_case!(given_field_select_200_parity_select_count_eq_4_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_eq_4.json");
query_case!(given_field_select_200_parity_select_count_eq_5_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_eq_5.json");
query_case!(given_field_select_200_parity_select_count_exclusive_start_sk_001_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_exclusive_start_sk-001.json");
query_case!(given_field_select_200_parity_select_count_exclusive_start_sk_002_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_exclusive_start_sk-002.json");
query_case!(given_field_select_200_parity_select_count_exclusive_start_sk_010_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_exclusive_start_sk-010.json");
query_case!(given_field_select_200_parity_select_count_exclusive_start_sk_020_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_exclusive_start_sk-020.json");
query_case!(given_field_select_200_parity_select_count_exclusive_start_zz_001_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_exclusive_start_zz-001.json");
query_case!(given_field_select_200_parity_select_count_ge_1_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_ge_1.json");
query_case!(given_field_select_200_parity_select_count_ge_2_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_ge_2.json");
query_case!(given_field_select_200_parity_select_count_gt_1_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_gt_1.json");
query_case!(given_field_select_200_parity_select_count_gt_2_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_gt_2.json");
query_case!(given_field_select_200_parity_select_count_hash_only_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_hash_only.json");
query_case!(given_field_select_200_parity_select_count_hash_only_alias_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_hash_only_alias.json");
query_case!(given_field_select_200_parity_select_count_hash_only_extra_spaces_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_hash_only_extra_spaces.json");
query_case!(given_field_select_200_parity_select_count_hash_only_newline_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_hash_only_newline.json");
query_case!(given_field_select_200_parity_select_count_hash_only_tabs_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_hash_only_tabs.json");
query_case!(given_field_select_200_parity_select_count_le_1_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_le_1.json");
query_case!(given_field_select_200_parity_select_count_le_2_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_le_2.json");
query_case!(given_field_select_200_parity_select_count_le_3_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_le_3.json");
query_case!(given_field_select_200_parity_select_count_limit_1_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_limit_1.json");
query_case!(given_field_select_200_parity_select_count_limit_2_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_limit_2.json");
query_case!(given_field_select_200_parity_select_count_limit_3_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_limit_3.json");
query_case!(given_field_select_200_parity_select_count_limit_4_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_limit_4.json");
query_case!(given_field_select_200_parity_select_count_limit_6_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_limit_6.json");
query_case!(given_field_select_200_parity_select_count_lt_1_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_lt_1.json");
query_case!(given_field_select_200_parity_select_count_lt_2_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_lt_2.json");
query_case!(given_field_select_200_parity_select_count_lt_3_when_query_runs_then_conformance_response_matches, "field-select/200/parity_select_count_lt_3.json");

/// Business rule from legacy SQL GSI reads: Query returns the full matching
/// projected items in index-key order through the shared Dynamo operation.
#[tokio::test]
async fn given_gsi_items_when_querying_one_partition_then_full_items_follow_index_order() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        create_fixture_table(&store, "basic_with_gsi", table.clone()).await;
        for (pk, sk, gsi_pk, gsi_sk, value) in [
            ("a", "1", "group", "20", "second"),
            ("b", "2", "group", "10", "first"),
            ("c", "3", "other", "10", "outside"),
        ] {
            let request = PutItemRequest::try_from(json!({
                "TableName": table.to_string(),
                "Item": {
                    "pk": {"S": pk},
                    "sk": {"S": sk},
                    "gsi_pk": {"S": gsi_pk},
                    "gsi_sk": {"S": gsi_sk},
                    "value": {"S": value}
                }
            }))
            .unwrap();
            store.operations.put_item(request).await.unwrap();
        }

        let request = QueryRequest::try_from(json!({
            "TableName": table.to_string(),
            "IndexName": "gsi1",
            "KeyConditionExpression": "gsi_pk = :group",
            "ExpressionAttributeValues": {":group": {"S": "group"}}
        }))
        .unwrap();
        let observed = observe(store.operations.query(request)).await;
        observed.assert_no_warning_or_error();
        let response = observed.result.unwrap();
        assert_eq!(
            serde_json::to_value(response).unwrap(),
            json!({
                "Count": 2,
                "ScannedCount": 2,
                "Items": [
                    {"pk": {"S": "b"}, "sk": {"S": "2"}, "gsi_pk": {"S": "group"},
                     "gsi_sk": {"S": "10"}, "value": {"S": "first"}},
                    {"pk": {"S": "a"}, "sk": {"S": "1"}, "gsi_pk": {"S": "group"},
                     "gsi_sk": {"S": "20"}, "value": {"S": "second"}}
                ]
            })
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

/// Validation rule retained from the legacy SQL limit test. The shared Limit
/// error text is pinned by dynamo-conformance's Scan Limit 0 case.
#[tokio::test]
async fn given_zero_limit_when_query_runs_then_aws_limit_error_is_exact() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        store.create_hash_range_table(table.clone(), storage_types::KeyAttributeType::S).await;
        let request = QueryRequest::try_from(json!({
            "TableName": table.to_string(),
            "KeyConditionExpression": "pk = :pk",
            "ExpressionAttributeValues": {":pk": {"S": "one"}},
            "Limit": 0
        }))
        .unwrap();
        let observed = observe(store.operations.query(request)).await;
        observed.assert_no_warning_or_error();
        let error = observed.result.unwrap_err();
        let (status, axum::Json(body)): (_, axum::Json<http_error::ErrorResponse>) =
            http_error::HttpApiError::from(error).into();
        assert_eq!(status.as_u16(), 400);
        assert_eq!(
            serde_json::to_value(body).unwrap(),
            json!({
                "__type": "com.amazon.coral.validate#ValidationException",
                "message": "1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"
            })
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
