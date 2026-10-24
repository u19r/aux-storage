//! Independently runnable Scan cases from dynamo-conformance and local rules.
//!
//! The fixture snapshot is from ../dynamo-conformance at
//! 2158288d7883de4be6c17da00226d8398faa723a. Each case retains its source
//! path and compares the complete Dynamo response through the replacement
//! store. Local cases use a `local/` source path.

use std::{
    collections::BTreeSet,
    panic::{AssertUnwindSafe, resume_unwind},
};

use futures::FutureExt as _;
use serde_json::{Value, json};
use storage_observability_test_support::observation::observe;
use storage_types::{
    AttributeMap, CreateTableRequest, GetItemRequest, IndexName, KeyAttributes, PutItemRequest,
    ScanRequest, TableName,
};

use super::test_support::{ConformanceFixtures, TestStore, unique_table_name};

static FIXTURES: ConformanceFixtures = ConformanceFixtures::new();

fn load_fixture(source: &str) -> Value {
    FIXTURES.case(include_str!("../fixtures/scan_cases.jsonl"), source)
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

async fn assert_scan_case(store: &TestStore, case: &Value) {
    assert_eq!(case["operation"], "Scan", "fixture operation");
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

    let parsed = observe(async { ScanRequest::try_from(case["request"].clone()) }).await;
    parsed.assert_no_warning_or_error();
    parsed.assert_database_calls_exact(&[]);
    let (status, body) = match parsed.result {
        Ok(request) => {
            let observed = observe(store.operations.scan(request)).await;
            observed.assert_no_warning_or_error();
            let operation = observed.one_span_with("dynamo_operation", &[("operation", "Scan")]);
            assert_eq!(
                operation.fields.len(),
                1,
                "Scan span must contain only safe context"
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

    // Scan is read-only, including validation failures and filtered pages.
    for precondition in preconditions {
        let item = &precondition["request"]["Item"];
        let mut key = serde_json::Map::new();
        key.insert("pk".to_owned(), item["pk"].clone());
        key.insert("sk".to_owned(), item["sk"].clone());
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
            "{} read-only Scan changed stored item",
            case["source"],
        );
    }
}

macro_rules! scan_case {
    ($name:ident, $source:literal) => {
        #[tokio::test]
        async fn $name() {
            let case = load_fixture($source);
            let store = TestStore::new().await;
            let outcome = AssertUnwindSafe(assert_scan_case(&store, &case))
                .catch_unwind()
                .await;
            store.finish().await;
            if let Err(panic) = outcome {
                resume_unwind(panic);
            }
        }
    };
}

// Boundary rule from legacy SQL: a 38 digit numeric key survives Scan
// continuation.
scan_case!(
    given_precise_numeric_cursor_when_scan_resumes_then_next_item_is_exact,
    "local/precise_numeric_cursor.json"
);

// Validation rule from legacy SQL/KV and Cucumber; error text from
// dynamo-conformance Query.
scan_case!(
    given_gsi_when_scan_requests_consistent_read_then_aws_error_is_exact,
    "local/gsi_consistent_read_validation.json"
);

// Business rule from legacy SQL/KV: Scan GSI returns projected rows and skips
// non-indexed rows.
scan_case!(
    given_indexed_and_nonindexed_rows_when_scan_gsi_then_projected_public_item_is_exact,
    "local/gsi_scan_projected_item.json"
);

// Business and format rule: 200.
scan_case!(
    given_200_unknown_top_level_field_is_ignored_when_scan_runs_then_conformance_response_matches,
    "200/unknown_top_level_field_is_ignored.json"
);

// Validation and negative rule: field-exclusive_start_key/400.
scan_case!(given_field_exclusive_start_key_400_missing_sort_key_when_scan_runs_then_conformance_response_matches, "field-exclusive_start_key/400/missing_sort_key.json");

// Business and format rule: field-filter_expression/200.
scan_case!(given_field_filter_expression_200_aliased_nested_reserved_word_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/aliased_nested_reserved_word.json");
scan_case!(given_field_filter_expression_200_bulk_filter_alias_nested_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/bulk_filter_alias_nested.json");
scan_case!(given_field_filter_expression_200_bulk_filter_attribute_exists_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/bulk_filter_attribute_exists.json");
scan_case!(given_field_filter_expression_200_bulk_filter_attribute_not_exists_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/bulk_filter_attribute_not_exists.json");
scan_case!(given_field_filter_expression_200_bulk_filter_attribute_type_string_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/bulk_filter_attribute_type_string.json");
scan_case!(given_field_filter_expression_200_bulk_filter_begins_with_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/bulk_filter_begins_with.json");
scan_case!(given_field_filter_expression_200_bulk_filter_between_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/bulk_filter_between.json");
scan_case!(given_field_filter_expression_200_bulk_filter_contains_set_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/bulk_filter_contains_set.json");
scan_case!(given_field_filter_expression_200_bulk_filter_contains_string_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/bulk_filter_contains_string.json");
scan_case!(given_field_filter_expression_200_bulk_filter_in_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/bulk_filter_in.json");
scan_case!(given_field_filter_expression_200_bulk_filter_list_child_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/bulk_filter_list_child.json");
scan_case!(given_field_filter_expression_200_bulk_filter_nested_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/bulk_filter_nested.json");
scan_case!(given_field_filter_expression_200_bulk_filter_not_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/bulk_filter_not.json");
scan_case!(given_field_filter_expression_200_bulk_filter_or_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/bulk_filter_or.json");
scan_case!(given_field_filter_expression_200_bulk_filter_parenthesized_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/bulk_filter_parenthesized.json");
scan_case!(given_field_filter_expression_200_bulk_filter_size_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/bulk_filter_size.json");
scan_case!(given_field_filter_expression_200_parity_filter_and_precedence_inside_or_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_and_precedence_inside_or.json");
scan_case!(given_field_filter_expression_200_parity_filter_attribute_exists_and_contains_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_attribute_exists_and_contains.json");
scan_case!(given_field_filter_expression_200_parity_filter_attribute_exists_nested_child_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_attribute_exists_nested_child.json");
scan_case!(given_field_filter_expression_200_parity_filter_attribute_exists_pk_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_attribute_exists_pk.json");
scan_case!(given_field_filter_expression_200_parity_filter_attribute_not_exists_absent_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_attribute_not_exists_absent.json");
scan_case!(given_field_filter_expression_200_parity_filter_attribute_not_exists_nested_absent_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_attribute_not_exists_nested_absent.json");
scan_case!(given_field_filter_expression_200_parity_filter_attribute_type_binary_set_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_attribute_type_binary_set.json");
scan_case!(given_field_filter_expression_200_parity_filter_attribute_type_bool_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_attribute_type_bool.json");
scan_case!(given_field_filter_expression_200_parity_filter_attribute_type_list_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_attribute_type_list.json");
scan_case!(given_field_filter_expression_200_parity_filter_attribute_type_map_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_attribute_type_map.json");
scan_case!(given_field_filter_expression_200_parity_filter_attribute_type_null_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_attribute_type_null.json");
scan_case!(given_field_filter_expression_200_parity_filter_attribute_type_number_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_attribute_type_number.json");
scan_case!(given_field_filter_expression_200_parity_filter_attribute_type_number_set_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_attribute_type_number_set.json");
scan_case!(given_field_filter_expression_200_parity_filter_attribute_type_string_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_attribute_type_string.json");
scan_case!(given_field_filter_expression_200_parity_filter_attribute_type_string_set_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_attribute_type_string_set.json");
scan_case!(given_field_filter_expression_200_parity_filter_begins_with_binary_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_begins_with_binary.json");
scan_case!(given_field_filter_expression_200_parity_filter_begins_with_string_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_begins_with_string.json");
scan_case!(given_field_filter_expression_200_parity_filter_between_then_and_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_between_then_and.json");
scan_case!(given_field_filter_expression_200_parity_filter_bool_equality_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_bool_equality.json");
scan_case!(given_field_filter_expression_200_parity_filter_contains_binary_set_exact_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_contains_binary_set_exact.json");
scan_case!(given_field_filter_expression_200_parity_filter_contains_list_number_exact_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_contains_list_number_exact.json");
scan_case!(given_field_filter_expression_200_parity_filter_contains_list_string_exact_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_contains_list_string_exact.json");
scan_case!(given_field_filter_expression_200_parity_filter_contains_number_set_exact_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_contains_number_set_exact.json");
scan_case!(given_field_filter_expression_200_parity_filter_contains_string_set_exact_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_contains_string_set_exact.json");
scan_case!(given_field_filter_expression_200_parity_filter_contains_string_substring_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_contains_string_substring.json");
scan_case!(given_field_filter_expression_200_parity_filter_deep_nested_leaf_equality_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_deep_nested_leaf_equality.json");
scan_case!(given_field_filter_expression_200_parity_filter_list_index_map_child_equality_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_list_index_map_child_equality.json");
scan_case!(given_field_filter_expression_200_parity_filter_list_index_number_equality_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_list_index_number_equality.json");
scan_case!(given_field_filter_expression_200_parity_filter_list_index_string_equality_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_list_index_string_equality.json");
scan_case!(given_field_filter_expression_200_parity_filter_nested_map_child_equality_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_nested_map_child_equality.json");
scan_case!(given_field_filter_expression_200_parity_filter_not_begins_with_or_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_not_begins_with_or.json");
scan_case!(given_field_filter_expression_200_parity_filter_not_false_equality_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_not_false_equality.json");
scan_case!(given_field_filter_expression_200_parity_filter_null_equality_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_null_equality.json");
scan_case!(given_field_filter_expression_200_parity_filter_number_between_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_number_between.json");
scan_case!(given_field_filter_expression_200_parity_filter_number_greater_equal_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_number_greater_equal.json");
scan_case!(given_field_filter_expression_200_parity_filter_number_greater_than_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_number_greater_than.json");
scan_case!(given_field_filter_expression_200_parity_filter_number_in_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_number_in.json");
scan_case!(given_field_filter_expression_200_parity_filter_number_less_equal_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_number_less_equal.json");
scan_case!(given_field_filter_expression_200_parity_filter_number_less_than_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_number_less_than.json");
scan_case!(given_field_filter_expression_200_parity_filter_parenthesized_or_then_and_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_parenthesized_or_then_and.json");
scan_case!(given_field_filter_expression_200_parity_filter_size_binary_equal_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_size_binary_equal.json");
scan_case!(given_field_filter_expression_200_parity_filter_size_binary_set_equal_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_size_binary_set_equal.json");
scan_case!(given_field_filter_expression_200_parity_filter_size_list_equal_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_size_list_equal.json");
scan_case!(given_field_filter_expression_200_parity_filter_size_map_equal_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_size_map_equal.json");
scan_case!(given_field_filter_expression_200_parity_filter_size_nested_map_equal_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_size_nested_map_equal.json");
scan_case!(given_field_filter_expression_200_parity_filter_size_number_set_equal_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_size_number_set_equal.json");
scan_case!(given_field_filter_expression_200_parity_filter_size_string_equal_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_size_string_equal.json");
scan_case!(given_field_filter_expression_200_parity_filter_size_string_set_equal_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_size_string_set_equal.json");
scan_case!(given_field_filter_expression_200_parity_filter_string_equality_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_string_equality.json");
scan_case!(given_field_filter_expression_200_parity_filter_string_in_when_scan_runs_then_conformance_response_matches, "field-filter_expression/200/parity_filter_string_in.json");

// Validation and negative rule: field-filter_expression/400.
scan_case!(given_field_filter_expression_400_attribute_exists_reserved_word_when_scan_runs_then_conformance_response_matches, "field-filter_expression/400/attribute_exists_reserved_word.json");
scan_case!(given_field_filter_expression_400_begins_with_nested_reserved_word_when_scan_runs_then_conformance_response_matches, "field-filter_expression/400/begins_with_nested_reserved_word.json");
scan_case!(given_field_filter_expression_400_empty_list_index_when_scan_runs_then_conformance_response_matches, "field-filter_expression/400/empty_list_index.json");
scan_case!(given_field_filter_expression_400_negative_list_index_when_scan_runs_then_conformance_response_matches, "field-filter_expression/400/negative_list_index.json");
scan_case!(given_field_filter_expression_400_parity_alpha_list_index_when_scan_runs_then_conformance_response_matches, "field-filter_expression/400/parity_alpha_list_index.json");
scan_case!(given_field_filter_expression_400_parity_begins_with_no_operands_when_scan_runs_then_conformance_response_matches, "field-filter_expression/400/parity_begins_with_no_operands.json");
scan_case!(given_field_filter_expression_400_parity_contains_no_operands_when_scan_runs_then_conformance_response_matches, "field-filter_expression/400/parity_contains_no_operands.json");
scan_case!(given_field_filter_expression_400_parity_contains_one_operand_when_scan_runs_then_conformance_response_matches, "field-filter_expression/400/parity_contains_one_operand.json");
scan_case!(given_field_filter_expression_400_parity_empty_list_index_when_scan_runs_then_conformance_response_matches, "field-filter_expression/400/parity_empty_list_index.json");
scan_case!(given_field_filter_expression_400_parity_invalid_expression_attribute_name_key_when_scan_runs_then_conformance_response_matches, "field-filter_expression/400/parity_invalid_expression_attribute_name_key.json");
scan_case!(given_field_filter_expression_400_parity_invalid_expression_attribute_value_key_when_scan_runs_then_conformance_response_matches, "field-filter_expression/400/parity_invalid_expression_attribute_value_key.json");
scan_case!(given_field_filter_expression_400_parity_list_raw_reserved_word_when_scan_runs_then_conformance_response_matches, "field-filter_expression/400/parity_list_raw_reserved_word.json");
scan_case!(given_field_filter_expression_400_parity_missing_expression_attribute_name_when_scan_runs_then_conformance_response_matches, "field-filter_expression/400/parity_missing_expression_attribute_name.json");
scan_case!(given_field_filter_expression_400_parity_missing_expression_attribute_value_when_scan_runs_then_conformance_response_matches, "field-filter_expression/400/parity_missing_expression_attribute_value.json");
scan_case!(given_field_filter_expression_400_parity_negative_list_index_when_scan_runs_then_conformance_response_matches, "field-filter_expression/400/parity_negative_list_index.json");
scan_case!(given_field_filter_expression_400_parity_nested_reserved_word_comment_when_scan_runs_then_conformance_response_matches, "field-filter_expression/400/parity_nested_reserved_word_comment.json");
scan_case!(given_field_filter_expression_400_parity_reserved_word_comment_when_scan_runs_then_conformance_response_matches, "field-filter_expression/400/parity_reserved_word_comment.json");
scan_case!(given_field_filter_expression_400_parity_size_without_comparison_when_scan_runs_then_conformance_response_matches, "field-filter_expression/400/parity_size_without_comparison.json");
scan_case!(given_field_filter_expression_400_parity_unused_expression_attribute_name_when_scan_runs_then_conformance_response_matches, "field-filter_expression/400/parity_unused_expression_attribute_name.json");
scan_case!(given_field_filter_expression_400_parity_unused_expression_attribute_value_when_scan_runs_then_conformance_response_matches, "field-filter_expression/400/parity_unused_expression_attribute_value.json");
scan_case!(given_field_filter_expression_400_reserved_word_nested_path_when_scan_runs_then_conformance_response_matches, "field-filter_expression/400/reserved_word_nested_path.json");

// Boundary rule: field-limit/200.
scan_case!(given_field_limit_200_docs_limit_binary_partition_key_1_byte_item_when_scan_runs_then_conformance_response_matches, "field-limit/200/docs_limit_binary_partition_key_1_byte_item.json");
scan_case!(given_field_limit_200_docs_limit_binary_partition_key_2048_bytes_item_when_scan_runs_then_conformance_response_matches, "field-limit/200/docs_limit_binary_partition_key_2048_bytes_item.json");
scan_case!(given_field_limit_200_docs_limit_binary_sort_key_1024_bytes_item_when_scan_runs_then_conformance_response_matches, "field-limit/200/docs_limit_binary_sort_key_1024_bytes_item.json");
scan_case!(
    given_field_limit_200_docs_limit_limit_1_valid_when_scan_runs_then_conformance_response_matches,
    "field-limit/200/docs_limit_limit_1_valid.json"
);
scan_case!(given_field_limit_200_docs_limit_number_partition_key_38_digits_item_when_scan_runs_then_conformance_response_matches, "field-limit/200/docs_limit_number_partition_key_38_digits_item.json");
scan_case!(given_field_limit_200_docs_limit_number_partition_key_positive_min_exponent_item_when_scan_runs_then_conformance_response_matches, "field-limit/200/docs_limit_number_partition_key_positive_min_exponent_item.json");
scan_case!(given_field_limit_200_docs_limit_number_partition_key_zero_item_when_scan_runs_then_conformance_response_matches, "field-limit/200/docs_limit_number_partition_key_zero_item.json");
scan_case!(given_field_limit_200_docs_limit_number_sort_key_38_digits_item_when_scan_runs_then_conformance_response_matches, "field-limit/200/docs_limit_number_sort_key_38_digits_item.json");

// Validation and negative rule: field-limit/400.
scan_case!(given_field_limit_400_docs_limit_limit_0_rejected_when_scan_runs_then_conformance_response_matches, "field-limit/400/docs_limit_limit_0_rejected.json");

/// Boundary rule from legacy KV Scan pagination and the Scan Cucumber feature.
#[tokio::test]
async fn given_five_items_when_scan_paginates_then_each_item_appears_once_and_only_nonfinal_pages_have_a_cursor()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        store
            .create_conformance_table("basic_pk_sk", table.clone())
            .await;
        for ordinal in 1..=5 {
            let request = PutItemRequest::try_from(json!({
                "TableName": table.to_string(),
                "Item": {
                    "pk": {"S": "group"},
                    "sk": {"S": format!("item{ordinal}")},
                    "value": {"N": ordinal.to_string()}
                }
            }))
            .unwrap();
            store.operations.put_item(request).await.unwrap();
        }

        let mut cursor = None;
        let mut seen = BTreeSet::new();
        for page_number in 0..3 {
            let mut request = ScanRequest::new(table.clone());
            request.limit = Some(2);
            request.exclusive_start_key = cursor;
            let observed = observe(store.operations.scan(request)).await;
            observed.assert_no_warning_or_error();
            let response = observed.result.unwrap();
            let expected_count = if page_number == 2 { 1 } else { 2 };
            assert_eq!(response.count, expected_count);
            assert_eq!(response.scanned_count, expected_count);
            let items = response.items.unwrap();
            assert_eq!(items.len(), expected_count as usize);
            for item in items {
                let item = item.into_attribute_map().unwrap();
                assert_eq!(
                    item.get("pk"),
                    Some(&storage_types::AttributeValue::S("group".to_owned()))
                );
                let Some(storage_types::AttributeValue::S(sort_key)) = item.get("sk") else {
                    panic!("Scan sort key is a string");
                };
                assert!(
                    seen.insert(sort_key.clone()),
                    "duplicate Scan item: {sort_key}"
                );
            }
            cursor = response.last_evaluated_key.map(Into::into);
            assert_eq!(
                cursor.is_some(),
                page_number < 2,
                "page {page_number} cursor"
            );
        }
        assert_eq!(
            seen,
            BTreeSet::from_iter((1..=5).map(|n| format!("item{n}")))
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

/// Boundary rule from legacy SQL/KV: GSI Scan pagination retains the full
/// public item and resumes without repeating or skipping indexed rows.
#[tokio::test]
async fn given_five_indexed_items_when_scan_gsi_pages_then_each_item_appears_once_and_only_nonfinal_pages_have_a_cursor()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        create_fixture_table(&store, "basic_with_gsi", table.clone()).await;
        for ordinal in 1..=5 {
            let request = PutItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Item": {
                    "pk": {"S": format!("base#{ordinal}")},
                    "sk": {"S": "row"},
                    "gsi_pk": {"S": "group"},
                    "gsi_sk": {"S": format!("index#{ordinal}")},
                    "note": {"S": format!("note#{ordinal}")}
                }
            }))
            .unwrap();
            store.operations.put_item(request).await.unwrap();
        }

        let mut cursor = None;
        let mut seen = BTreeSet::new();
        for page_number in 0..3 {
            let mut request = ScanRequest::new(table.clone());
            request.index_name = Some(IndexName::new("gsi1"));
            request.limit = Some(2);
            request.exclusive_start_key = cursor;
            let observed = observe(store.operations.scan(request)).await;
            observed.assert_no_warning_or_error();
            let operation = observed.one_span_with("dynamo_operation", &[("operation", "Scan")]);
            assert_eq!(operation.fields.len(), 1);
            let response = observed.result.unwrap();
            let expected_count = if page_number == 2 { 1 } else { 2 };
            assert_eq!(response.count, expected_count);
            assert_eq!(response.scanned_count, expected_count);
            let items = response.items.unwrap();
            assert_eq!(items.len(), expected_count as usize);
            let mut last_pk = None;
            for item in items {
                let item = item.into_attribute_map().unwrap();
                let Some(storage_types::AttributeValue::S(pk)) = item.get("pk") else {
                    panic!("GSI Scan item omitted its base key");
                };
                assert_eq!(
                    item.get("sk"),
                    Some(&storage_types::AttributeValue::S("row".into()))
                );
                assert_eq!(
                    item.get("gsi_pk"),
                    Some(&storage_types::AttributeValue::S("group".into()))
                );
                let ordinal = pk.strip_prefix("base#").unwrap();
                assert_eq!(
                    item.get("gsi_sk"),
                    Some(&storage_types::AttributeValue::S(format!(
                        "index#{ordinal}"
                    )))
                );
                assert_eq!(
                    item.get("note"),
                    Some(&storage_types::AttributeValue::S(format!("note#{ordinal}")))
                );
                assert_eq!(item.len(), 5, "GSI Scan must return only the public item");
                assert!(seen.insert(pk.clone()), "duplicate GSI Scan item: {pk}");
                last_pk = Some(pk.clone());
            }
            if let Some(key) = response.last_evaluated_key.as_ref() {
                let last_pk = last_pk.unwrap();
                let ordinal = last_pk.strip_prefix("base#").unwrap();
                assert_eq!(
                    serde_json::to_value(key).unwrap(),
                    json!({
                        "pk": {"S": last_pk},
                        "sk": {"S": "row"},
                        "gsi_pk": {"S": "group"},
                        "gsi_sk": {"S": format!("index#{ordinal}")}
                    })
                );
            }
            cursor = response.last_evaluated_key.map(Into::into);
            assert_eq!(
                cursor.is_some(),
                page_number < 2,
                "page {page_number} cursor"
            );
        }
        assert_eq!(
            seen,
            BTreeSet::from_iter((1..=5).map(|n| format!("base#{n}")))
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
