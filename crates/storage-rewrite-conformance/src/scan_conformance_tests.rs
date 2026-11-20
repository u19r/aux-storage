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
    AttributeMap, GetItemRequest, IndexName, KeyAttributes, PutItemRequest, ScanRequest, TableName,
};

use super::test_support::{ConformanceFixtures, TestStore, unique_table_name};

static FIXTURES: ConformanceFixtures = ConformanceFixtures::new();

fn load_fixture(source: &str) -> Value {
    FIXTURES.case(include_str!("../fixtures/scan_cases.jsonl"), source)
}

fn sorted_by_pk(mut body: Value) -> Value {
    if let Some(items) = body.get_mut("Items").and_then(Value::as_array_mut) {
        items.sort_by_key(|item| item["pk"].to_string());
    }
    body
}

async fn assert_scan_case(store: &TestStore, case: &Value) {
    assert_eq!(case["operation"], "Scan", "fixture operation");
    // A local case with `"tables": []` runs against no table (missing-table and
    // empty-name rules).
    let table_kind = case["tables"]
        .as_array()
        .unwrap()
        .first()
        .map(|kind| kind.as_str().unwrap());
    let table = TableName::new(case["request"]["TableName"].as_str().unwrap());
    if let Some(kind) = table_kind {
        store.create_conformance_table(kind, table.clone()).await;
    }

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
    // Scan order across partitions is backend-defined, so compare the matching
    // set.
    assert_eq!(
        serde_json::to_string(&sorted_by_pk(body)).unwrap(),
        serde_json::to_string(&sorted_by_pk(case["expected"]["body"].clone())).unwrap(),
        "{}",
        case["source"],
    );

    // Scan is read-only, including validation failures and filtered pages.
    for precondition in preconditions {
        let item = &precondition["request"]["Item"];
        let mut key = serde_json::Map::new();
        key.insert("pk".to_owned(), item["pk"].clone());
        // Hash-only AWS behavioural tables carry no sort key.
        if let Some(sort_key) = item.get("sk") {
            key.insert("sk".to_owned(), sort_key.clone());
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
        store
            .create_conformance_table("basic_with_gsi", table.clone())
            .await;
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

// Negative rule from Cucumber "Scan non-existent table": a missing table is
// ResourceNotFound.
scan_case!(
    given_missing_table_when_scan_runs_then_resource_not_found_is_exact,
    "local/missing_table.json"
);

// Validation rule from Cucumber "Scan with empty table name".
scan_case!(
    given_empty_table_name_when_scan_runs_then_validation_is_exact,
    "local/empty_table_name.json"
);

// DDB-PAR-4: `#l` -> `l[0]` reads the top-level `l[0]`; unaliased `l[0]` reads
// the first list element.
scan_case!(
    given_bracket_alias_filter_when_scan_runs_then_top_level_attribute_matches,
    "local/ddb-par-4/bracket_alias_filter_scan.json"
);
// token/near rule (dynamo-conformance cases).
scan_case!(
    given_unclosed_function_filter_when_scan_runs_then_eof_syntax_error,
    "local/ddb-par-1/given_unclosed_function_filter_when_scan_runs_then_eof_syntax_error.json"
);
// AWS probe P1.
scan_case!(
    given_invalid_filter_text_when_scan_runs_then_syntax_error,
    "local/ddb-par-1/given_invalid_filter_text_when_scan_runs_then_syntax_error.json"
);
// AWS probe P3.
scan_case!(
    given_unknown_character_filter_when_scan_runs_then_character_is_the_token,
    "local/ddb-par-1/given_unknown_character_filter_when_scan_runs_then_character_is_the_token.\
     json"
);
// AWS probe P9.
scan_case!(
    given_values_without_expression_when_scan_runs_then_only_with_expressions_error,
    "local/ddb-par-1/\
     given_values_without_expression_when_scan_runs_then_only_with_expressions_error.json"
);
scan_case!(
    given_parenthesized_operand_comparison_when_scan_runs_then_filter_is_accepted,
    "local/ddb-par-1/\
     given_parenthesized_operand_comparison_when_scan_runs_then_filter_is_accepted.json"
);
// local text: AWS applies ScanFilter (probe P11); aux does not implement it.
scan_case!(
    given_scan_filter_when_scan_runs_then_legacy_parameter_is_rejected,
    "local/ddb-par-2/given_scan_filter_when_scan_runs_then_legacy_parameter_is_rejected.json"
);
// AWS probe P11.
scan_case!(
    given_scan_filter_and_filter_expression_when_scan_runs_then_mixed_parameters_are_rejected,
    "local/ddb-par-2/\
     given_scan_filter_and_filter_expression_when_scan_runs_then_mixed_parameters_are_rejected.\
     json"
);

// AWS behavioural suite
// AWS behavioural:
// check_ddb_frc_cross_type_string_operand_eq_empty_ne_everything.py
scan_case!(
    given_ddb_frc_cross_type_string_operand_eq_empty_ne_everything_same_type_n_eq_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_ddb_frc_cross_type_string_operand_eq_empty_ne_everything.same_type_n_eq.json"
);

// AWS behavioural:
// check_ddb_frc_cross_type_string_operand_eq_empty_ne_everything.py
scan_case!(
    given_ddb_frc_cross_type_string_operand_eq_empty_ne_everything_cross_type_s_eq_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_ddb_frc_cross_type_string_operand_eq_empty_ne_everything.cross_type_s_eq.json"
);

// AWS behavioural:
// check_ddb_frc_cross_type_string_operand_eq_empty_ne_everything.py
scan_case!(
    given_ddb_frc_cross_type_string_operand_eq_empty_ne_everything_cross_type_s_ne_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_ddb_frc_cross_type_string_operand_eq_empty_ne_everything.cross_type_s_ne.json"
);

// AWS behavioural: check_legacy_attributes_to_get_duplicate_name_rejected.py
scan_case!(
    given_legacy_attributes_to_get_duplicate_name_rejected_scan_duplicate_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_legacy_attributes_to_get_duplicate_name_rejected.scan_duplicate.json"
);

// AWS behavioural: check_legacy_attributes_to_get_duplicate_name_rejected.py
scan_case!(
    given_legacy_attributes_to_get_duplicate_name_rejected_scan_single_control_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_legacy_attributes_to_get_duplicate_name_rejected.scan_single_control.json"
);

// AWS behavioural:
// check_scan_and_query_filter_ordering_comparison_on_a_set_attribute_rejected.
// py
scan_case!(
    given_scan_and_query_filter_ordering_comparison_on_a_set_attribute_rejected_scan_ssattr_lt_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_and_query_filter_ordering_comparison_on_a_set_attribute_rejected.scan_ssattr_lt.json"
);

// AWS behavioural:
// check_scan_and_query_filter_ordering_comparison_on_a_set_attribute_rejected.
// py
scan_case!(
    given_scan_and_query_filter_ordering_comparison_on_a_set_attribute_rejected_scan_ssattr_gt_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_and_query_filter_ordering_comparison_on_a_set_attribute_rejected.scan_ssattr_gt.json"
);

// AWS behavioural:
// check_scan_and_query_filter_ordering_comparison_on_a_set_attribute_rejected.
// py
scan_case!(
    given_scan_and_query_filter_ordering_comparison_on_a_set_attribute_rejected_scan_nsattr_lt_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_and_query_filter_ordering_comparison_on_a_set_attribute_rejected.scan_nsattr_lt.json"
);

// AWS behavioural:
// check_scan_and_query_filter_ordering_comparison_on_a_set_attribute_rejected.
// py
scan_case!(
    given_scan_and_query_filter_ordering_comparison_on_a_set_attribute_rejected_scan_nsattr_gt_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_and_query_filter_ordering_comparison_on_a_set_attribute_rejected.scan_nsattr_gt.json"
);

// AWS behavioural:
// check_scan_and_query_filter_ordering_comparison_on_a_set_attribute_rejected.
// py
scan_case!(
    given_scan_and_query_filter_ordering_comparison_on_a_set_attribute_rejected_scan_bsattr_lt_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_and_query_filter_ordering_comparison_on_a_set_attribute_rejected.scan_bsattr_lt.json"
);

// AWS behavioural:
// check_scan_and_query_filter_ordering_comparison_on_a_set_attribute_rejected.
// py
scan_case!(
    given_scan_and_query_filter_ordering_comparison_on_a_set_attribute_rejected_scan_bsattr_gt_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_and_query_filter_ordering_comparison_on_a_set_attribute_rejected.scan_bsattr_gt.json"
);

// AWS behavioural:
// check_scan_combined_filter_and_projection_does_not_reduce_consumed_capacity.
// py
scan_case!(
    given_scan_combined_filter_and_projection_does_not_reduce_consumed_capacity_filter_and_projection_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_combined_filter_and_projection_does_not_reduce_consumed_capacity.filter_and_projection.json"
);

// AWS behavioural:
// check_scan_exclusive_start_key_naming_an_attribute_outside_the_key_schema_rejected.
// py
scan_case!(
    given_scan_exclusive_start_key_naming_an_attribute_outside_the_key_schema_rejected_extra_key_attribute_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_exclusive_start_key_naming_an_attribute_outside_the_key_schema_rejected.extra_key_attribute.json"
);

// AWS behavioural: check_scan_filter_begins_with_non_string_operand_rejected.py
scan_case!(
    given_scan_filter_begins_with_non_string_operand_rejected_empty_n_operand_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_begins_with_non_string_operand_rejected.empty_n_operand.json"
);

// AWS behavioural: check_scan_filter_begins_with_non_string_operand_rejected.py
scan_case!(
    given_scan_filter_begins_with_non_string_operand_rejected_empty_bool_operand_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_begins_with_non_string_operand_rejected.empty_bool_operand.json"
);

// AWS behavioural: check_scan_filter_begins_with_non_string_operand_rejected.py
scan_case!(
    given_scan_filter_begins_with_non_string_operand_rejected_populated_n_operand_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_begins_with_non_string_operand_rejected.populated_n_operand.json"
);

// AWS behavioural: check_scan_filter_begins_with_non_string_operand_rejected.py
scan_case!(
    given_scan_filter_begins_with_non_string_operand_rejected_populated_bool_operand_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_begins_with_non_string_operand_rejected.populated_bool_operand.json"
);

// AWS behavioural: check_scan_filter_begins_with_non_string_operand_rejected.py
scan_case!(
    given_scan_filter_begins_with_non_string_operand_rejected_populated_s_control_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_begins_with_non_string_operand_rejected.populated_s_control.json"
);

// AWS behavioural:
// check_scan_filter_between_binary_attribute_reversed_bounds_rejected.py
scan_case!(
    given_scan_filter_between_binary_attribute_reversed_bounds_rejected_reversed_bounds_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_between_binary_attribute_reversed_bounds_rejected.reversed_bounds.json"
);

// AWS behavioural:
// check_scan_filter_between_binary_attribute_reversed_bounds_rejected.py
scan_case!(
    given_scan_filter_between_binary_attribute_reversed_bounds_rejected_ascending_control_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_between_binary_attribute_reversed_bounds_rejected.ascending_control.json"
);

// AWS behavioural:
// check_scan_filter_between_binary_attribute_unsigned_byte_order_inclusive.py
scan_case!(
    given_scan_filter_between_binary_attribute_unsigned_byte_order_inclusive_01_to_80_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_between_binary_attribute_unsigned_byte_order_inclusive.01_to_80.json"
);

// AWS behavioural:
// check_scan_filter_between_binary_attribute_unsigned_byte_order_inclusive.py
scan_case!(
    given_scan_filter_between_binary_attribute_unsigned_byte_order_inclusive_80_to_ff_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_between_binary_attribute_unsigned_byte_order_inclusive.80_to_ff.json"
);

// AWS behavioural:
// check_scan_filter_between_binary_attribute_unsigned_byte_order_inclusive.py
scan_case!(
    given_scan_filter_between_binary_attribute_unsigned_byte_order_inclusive_00_to_ff_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_between_binary_attribute_unsigned_byte_order_inclusive.00_to_ff.json"
);

// AWS behavioural:
// check_scan_filter_between_binary_attribute_unsigned_byte_order_inclusive.py
scan_case!(
    given_scan_filter_between_binary_attribute_unsigned_byte_order_inclusive_01_to_7f_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_between_binary_attribute_unsigned_byte_order_inclusive.01_to_7f.json"
);

// AWS behavioural: check_scan_filter_binary_attribute_unsigned_byte_order.py
scan_case!(
    given_scan_filter_binary_attribute_unsigned_byte_order_greater_than_7f_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_binary_attribute_unsigned_byte_order.greater_than_7f.json"
);

// AWS behavioural: check_scan_filter_binary_attribute_unsigned_byte_order.py
scan_case!(
    given_scan_filter_binary_attribute_unsigned_byte_order_contains_00_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_binary_attribute_unsigned_byte_order.contains_00.json"
);

// AWS behavioural: check_scan_filter_binary_attribute_unsigned_byte_order.py
scan_case!(
    given_scan_filter_binary_attribute_unsigned_byte_order_begins_with_80_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_binary_attribute_unsigned_byte_order.begins_with_80.json"
);

// AWS behavioural:
// check_scan_filter_eq_and_contains_type_strict_across_s_n_b.py
scan_case!(
    given_scan_filter_eq_and_contains_type_strict_across_s_n_b_eq_n_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_eq_and_contains_type_strict_across_s_n_b.eq_n.json"
);

// AWS behavioural:
// check_scan_filter_eq_and_contains_type_strict_across_s_n_b.py
scan_case!(
    given_scan_filter_eq_and_contains_type_strict_across_s_n_b_eq_s_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_eq_and_contains_type_strict_across_s_n_b.eq_s.json"
);

// AWS behavioural:
// check_scan_filter_eq_and_contains_type_strict_across_s_n_b.py
scan_case!(
    given_scan_filter_eq_and_contains_type_strict_across_s_n_b_eq_b_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_eq_and_contains_type_strict_across_s_n_b.eq_b.json"
);

// AWS behavioural:
// check_scan_filter_eq_and_contains_type_strict_across_s_n_b.py
scan_case!(
    given_scan_filter_eq_and_contains_type_strict_across_s_n_b_contains_n_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_eq_and_contains_type_strict_across_s_n_b.contains_n.json"
);

// AWS behavioural:
// check_scan_filter_eq_and_contains_type_strict_across_s_n_b.py
scan_case!(
    given_scan_filter_eq_and_contains_type_strict_across_s_n_b_contains_s_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_eq_and_contains_type_strict_across_s_n_b.contains_s.json"
);

// AWS behavioural:
// check_scan_filter_eq_and_contains_type_strict_across_s_n_b.py
scan_case!(
    given_scan_filter_eq_and_contains_type_strict_across_s_n_b_contains_b_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_eq_and_contains_type_strict_across_s_n_b.contains_b.json"
);

// AWS behavioural: check_scan_filter_expression_le_ge_and_binary_begins_with.py
scan_case!(
    given_scan_filter_expression_le_ge_and_binary_begins_with_name_le_string_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_expression_le_ge_and_binary_begins_with.name_le_string.json"
);

// AWS behavioural: check_scan_filter_expression_le_ge_and_binary_begins_with.py
scan_case!(
    given_scan_filter_expression_le_ge_and_binary_begins_with_score_le_number_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_expression_le_ge_and_binary_begins_with.score_le_number.json"
);

// AWS behavioural: check_scan_filter_expression_le_ge_and_binary_begins_with.py
scan_case!(
    given_scan_filter_expression_le_ge_and_binary_begins_with_score_ge_number_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_expression_le_ge_and_binary_begins_with.score_ge_number.json"
);

// AWS behavioural: check_scan_filter_expression_le_ge_and_binary_begins_with.py
scan_case!(
    given_scan_filter_expression_le_ge_and_binary_begins_with_blob_begins_with_01_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_expression_le_ge_and_binary_begins_with.blob_begins_with_01.json"
);

// AWS behavioural:
// check_scan_filter_expression_not_and_per_comparison_parens.py
scan_case!(
    given_scan_filter_expression_not_and_per_comparison_parens_not_grouped_bare_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_expression_not_and_per_comparison_parens.not_grouped_bare.json"
);

// AWS behavioural:
// check_scan_filter_expression_not_and_per_comparison_parens.py
scan_case!(
    given_scan_filter_expression_not_and_per_comparison_parens_not_grouped_both_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_expression_not_and_per_comparison_parens.not_grouped_both.json"
);

// AWS behavioural:
// check_scan_filter_expression_not_and_per_comparison_parens.py
scan_case!(
    given_scan_filter_expression_not_and_per_comparison_parens_per_comparison_and_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_expression_not_and_per_comparison_parens.per_comparison_and.json"
);

// AWS behavioural:
// check_scan_filter_expression_ordering_comparison_non_orderable_operand_rejected.
// py
scan_case!(
    given_scan_filter_expression_ordering_comparison_non_orderable_operand_rejected_le_bool_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_expression_ordering_comparison_non_orderable_operand_rejected.le_bool.json"
);

// AWS behavioural:
// check_scan_filter_expression_ordering_comparison_non_orderable_operand_rejected.
// py
scan_case!(
    given_scan_filter_expression_ordering_comparison_non_orderable_operand_rejected_le_null_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_expression_ordering_comparison_non_orderable_operand_rejected.le_null.json"
);

// AWS behavioural:
// check_scan_filter_expression_ordering_comparison_non_orderable_operand_rejected.
// py
scan_case!(
    given_scan_filter_expression_ordering_comparison_non_orderable_operand_rejected_ge_bool_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_expression_ordering_comparison_non_orderable_operand_rejected.ge_bool.json"
);

// AWS behavioural:
// check_scan_filter_expression_ordering_comparison_non_orderable_operand_rejected.
// py
scan_case!(
    given_scan_filter_expression_ordering_comparison_non_orderable_operand_rejected_ge_null_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_expression_ordering_comparison_non_orderable_operand_rejected.ge_null.json"
);

// AWS behavioural:
// check_scan_filter_expression_ordering_comparison_non_orderable_operand_rejected.
// py
scan_case!(
    given_scan_filter_expression_ordering_comparison_non_orderable_operand_rejected_le_n_control_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_expression_ordering_comparison_non_orderable_operand_rejected.le_n_control.json"
);

// AWS behavioural:
// check_scan_filter_expression_redundant_nested_parens_rejected.py
scan_case!(
    given_scan_filter_expression_redundant_nested_parens_rejected_double_comparison_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_expression_redundant_nested_parens_rejected.double_comparison.json"
);

// AWS behavioural:
// check_scan_filter_expression_redundant_nested_parens_rejected.py
scan_case!(
    given_scan_filter_expression_redundant_nested_parens_rejected_double_not_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_expression_redundant_nested_parens_rejected.double_not.json"
);

// AWS behavioural:
// check_scan_filter_expression_redundant_nested_parens_rejected.py
scan_case!(
    given_scan_filter_expression_redundant_nested_parens_rejected_double_then_and_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_expression_redundant_nested_parens_rejected.double_then_and.json"
);

// AWS behavioural:
// check_scan_filter_expression_redundant_nested_parens_rejected.py
scan_case!(
    given_scan_filter_expression_redundant_nested_parens_rejected_single_pair_control_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_expression_redundant_nested_parens_rejected.single_pair_control.json"
);

// AWS behavioural: check_scan_filter_legacy_in_string_and_numeric.py
scan_case!(
    given_scan_filter_legacy_in_string_and_numeric_status_in_strings_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_legacy_in_string_and_numeric.status_in_strings.json"
);

// AWS behavioural: check_scan_filter_legacy_in_string_and_numeric.py
scan_case!(
    given_scan_filter_legacy_in_string_and_numeric_score_in_numbers_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_legacy_in_string_and_numeric.score_in_numbers.json"
);

// AWS behavioural: check_scan_filter_legacy_numeric_decimal_negative.py
scan_case!(
    given_scan_filter_legacy_numeric_decimal_negative_le_025_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_legacy_numeric_decimal_negative.le_025.json"
);

// AWS behavioural: check_scan_filter_legacy_numeric_decimal_negative.py
scan_case!(
    given_scan_filter_legacy_numeric_decimal_negative_lt_025_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_legacy_numeric_decimal_negative.lt_025.json"
);

// AWS behavioural: check_scan_filter_legacy_numeric_decimal_negative.py
scan_case!(
    given_scan_filter_legacy_numeric_decimal_negative_ge_375_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_legacy_numeric_decimal_negative.ge_375.json"
);

// AWS behavioural: check_scan_filter_legacy_numeric_decimal_negative.py
scan_case!(
    given_scan_filter_legacy_numeric_decimal_negative_gt_025_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_legacy_numeric_decimal_negative.gt_025.json"
);

// AWS behavioural: check_scan_filter_legacy_numeric_decimal_negative.py
scan_case!(
    given_scan_filter_legacy_numeric_decimal_negative_between_negative_to_375_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_legacy_numeric_decimal_negative.between_negative_to_375.json"
);

// AWS behavioural: check_scan_filter_legacy_string_byte_order.py
scan_case!(
    given_scan_filter_legacy_string_byte_order_le_b_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_legacy_string_byte_order.le_B.json"
);

// AWS behavioural: check_scan_filter_legacy_string_byte_order.py
scan_case!(
    given_scan_filter_legacy_string_byte_order_lt_b_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_legacy_string_byte_order.lt_B.json"
);

// AWS behavioural: check_scan_filter_legacy_string_byte_order.py
scan_case!(
    given_scan_filter_legacy_string_byte_order_ge_z_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_legacy_string_byte_order.ge_Z.json"
);

// AWS behavioural: check_scan_filter_legacy_string_byte_order.py
scan_case!(
    given_scan_filter_legacy_string_byte_order_gt_b_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_legacy_string_byte_order.gt_B.json"
);

// AWS behavioural: check_scan_filter_legacy_string_byte_order.py
scan_case!(
    given_scan_filter_legacy_string_byte_order_between_b_and_z_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_legacy_string_byte_order.between_B_and_Z.json"
);

// AWS behavioural: check_scan_filter_legacy_string_byte_order.py
scan_case!(
    given_scan_filter_legacy_string_byte_order_contains_an_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_legacy_string_byte_order.contains_an.json"
);

// AWS behavioural:
// check_scan_filter_ne_and_not_contains_on_missing_attribute.py
scan_case!(
    given_scan_filter_ne_and_not_contains_on_missing_attribute_ne_hello_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_ne_and_not_contains_on_missing_attribute.ne_hello.json"
);

// AWS behavioural:
// check_scan_filter_ne_and_not_contains_on_missing_attribute.py
scan_case!(
    given_scan_filter_ne_and_not_contains_on_missing_attribute_not_contains_hel_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_ne_and_not_contains_on_missing_attribute.not_contains_hel.json"
);

// AWS behavioural:
// check_scan_filter_ne_and_not_contains_on_missing_attribute.py
scan_case!(
    given_scan_filter_ne_and_not_contains_on_missing_attribute_attribute_exists_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_ne_and_not_contains_on_missing_attribute.attribute_exists.json"
);

// AWS behavioural:
// check_scan_filter_ne_and_not_contains_on_missing_attribute.py
scan_case!(
    given_scan_filter_ne_and_not_contains_on_missing_attribute_attribute_not_exists_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_ne_and_not_contains_on_missing_attribute.attribute_not_exists.json"
);

// AWS behavioural: check_scan_filter_size_covers_s_b_ss_l_m_not_n.py
scan_case!(
    given_scan_filter_size_covers_s_b_ss_l_m_not_n_size_s_eq_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_size_covers_s_b_ss_l_m_not_n.size_s_eq.json"
);

// AWS behavioural: check_scan_filter_size_covers_s_b_ss_l_m_not_n.py
scan_case!(
    given_scan_filter_size_covers_s_b_ss_l_m_not_n_parenthesized_size_s_eq_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_size_covers_s_b_ss_l_m_not_n.parenthesized_size_s_eq.json"
);

// AWS behavioural: check_scan_filter_size_covers_s_b_ss_l_m_not_n.py
scan_case!(
    given_scan_filter_size_covers_s_b_ss_l_m_not_n_size_b_eq_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_size_covers_s_b_ss_l_m_not_n.size_b_eq.json"
);

// AWS behavioural: check_scan_filter_size_covers_s_b_ss_l_m_not_n.py
scan_case!(
    given_scan_filter_size_covers_s_b_ss_l_m_not_n_parenthesized_size_b_eq_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_size_covers_s_b_ss_l_m_not_n.parenthesized_size_b_eq.json"
);

// AWS behavioural: check_scan_filter_size_covers_s_b_ss_l_m_not_n.py
scan_case!(
    given_scan_filter_size_covers_s_b_ss_l_m_not_n_size_ss_eq_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_size_covers_s_b_ss_l_m_not_n.size_ss_eq.json"
);

// AWS behavioural: check_scan_filter_size_covers_s_b_ss_l_m_not_n.py
scan_case!(
    given_scan_filter_size_covers_s_b_ss_l_m_not_n_parenthesized_size_ss_eq_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_size_covers_s_b_ss_l_m_not_n.parenthesized_size_ss_eq.json"
);

// AWS behavioural: check_scan_filter_size_covers_s_b_ss_l_m_not_n.py
scan_case!(
    given_scan_filter_size_covers_s_b_ss_l_m_not_n_size_l_eq_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_size_covers_s_b_ss_l_m_not_n.size_l_eq.json"
);

// AWS behavioural: check_scan_filter_size_covers_s_b_ss_l_m_not_n.py
scan_case!(
    given_scan_filter_size_covers_s_b_ss_l_m_not_n_parenthesized_size_l_eq_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_size_covers_s_b_ss_l_m_not_n.parenthesized_size_l_eq.json"
);

// AWS behavioural: check_scan_filter_size_covers_s_b_ss_l_m_not_n.py
scan_case!(
    given_scan_filter_size_covers_s_b_ss_l_m_not_n_size_m_eq_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_size_covers_s_b_ss_l_m_not_n.size_m_eq.json"
);

// AWS behavioural: check_scan_filter_size_covers_s_b_ss_l_m_not_n.py
scan_case!(
    given_scan_filter_size_covers_s_b_ss_l_m_not_n_parenthesized_size_m_eq_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_size_covers_s_b_ss_l_m_not_n.parenthesized_size_m_eq.json"
);

// AWS behavioural: check_scan_filter_size_covers_s_b_ss_l_m_not_n.py
scan_case!(
    given_scan_filter_size_covers_s_b_ss_l_m_not_n_size_of_number_greater_than_zero_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_size_covers_s_b_ss_l_m_not_n.size_of_number_greater_than_zero.json"
);

// AWS behavioural: check_scan_filter_size_covers_s_b_ss_l_m_not_n.py
scan_case!(
    given_scan_filter_size_covers_s_b_ss_l_m_not_n_size_of_number_equals_seven_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_size_covers_s_b_ss_l_m_not_n.size_of_number_equals_seven.json"
);

// AWS behavioural: check_scan_filter_size_covers_s_b_ss_l_m_not_n.py
scan_case!(
    given_scan_filter_size_covers_s_b_ss_l_m_not_n_number_equals_seven_control_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_scan_filter_size_covers_s_b_ss_l_m_not_n.number_equals_seven_control.json"
);

// AWS behavioural:
// check_select_non_specific_values_reject_projection_expression.py
scan_case!(
    given_select_non_specific_values_reject_projection_expression_scan_all_attributes_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_select_non_specific_values_reject_projection_expression.scan_all_attributes.json"
);

// AWS behavioural:
// check_select_non_specific_values_reject_projection_expression.py
scan_case!(
    given_select_non_specific_values_reject_projection_expression_scan_count_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_select_non_specific_values_reject_projection_expression.scan_count.json"
);

// AWS behavioural:
// check_select_non_specific_values_reject_projection_expression.py
scan_case!(
    given_select_non_specific_values_reject_projection_expression_scan_all_projected_attributes_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_select_non_specific_values_reject_projection_expression.scan_all_projected_attributes.json"
);

// AWS behavioural:
// check_select_non_specific_values_reject_projection_expression.py
scan_case!(
    given_select_non_specific_values_reject_projection_expression_scan_specific_control_when_scan_runs_then_aws_response_is_exact,
    "local/aws-behav/check_select_non_specific_values_reject_projection_expression.scan_specific_control.json"
);
