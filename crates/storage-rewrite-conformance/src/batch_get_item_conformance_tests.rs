//! BatchGetItem pathfinding cases from the authoritative DynamoDB fixture
//! suite.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::{Value, json};
use storage_observability_test_support::observation::observe;
use storage_types::{
    AttributeMap, BatchGetItemRequest, DynamoRequestValidate, GetItemRequest, KeyAttributes,
    KeysAndAttributes, PutItemRequest, TableName,
};

use super::test_support::{
    ConformanceFixtures, TestStore, assert_conformance_body, normalize_attribute_sets,
};

static FIXTURES: ConformanceFixtures = ConformanceFixtures::new();

fn fixture(source: &str) -> Value {
    FIXTURES.case(
        include_str!("../fixtures/batch_get_item_cases.jsonl"),
        source,
    )
}

async fn assert_batch_get_case(store: &TestStore, case: &Value) {
    assert_eq!(case["operation"], "BatchGetItem", "fixture operation");
    let request_items = case["request"]["RequestItems"]
        .as_object()
        .expect("fixture RequestItems");
    assert!(request_items.len() <= case["tables"].as_array().expect("fixture tables").len());
    for name in request_items.keys() {
        let kind = if name.starts_with("dynamo-conformance-basic-") {
            "basic_pk_sk"
        } else if name.starts_with("dynamo-conformance-pk-") {
            "basic_pk"
        } else {
            panic!("unexpected conformance table name: {name}");
        };
        store
            .create_conformance_table(kind, TableName::new(name))
            .await;
    }

    let preconditions = case["preconditions"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    for precondition in &preconditions {
        assert_eq!(
            precondition["operation"], "PutItem",
            "precondition operation"
        );
        let request = PutItemRequest::try_from(precondition["request"].clone())
            .expect("parse fixture precondition");
        store
            .operations
            .put_item(request)
            .await
            .expect("seed fixture item");
    }

    let parsed = observe(async { BatchGetItemRequest::try_from(case["request"].clone()) }).await;
    parsed.assert_no_warning_or_error();
    parsed.assert_database_calls_exact(&[]);
    let (status, body) = match parsed.result {
        Ok(request) => {
            let observed = observe(store.operations.batch_get_item(request)).await;
            observed.assert_no_warning_or_error();
            match observed.result {
                Ok(response) => (
                    200,
                    serde_json::to_value(response).expect("serialize BatchGetItem"),
                ),
                Err(error) => {
                    let (status, axum::Json(body)): (_, axum::Json<http_error::ErrorResponse>) =
                        http_error::HttpApiError::from(error).into();
                    (
                        status.as_u16(),
                        serde_json::to_value(body).expect("serialize BatchGetItem error"),
                    )
                }
            }
        }
        Err(message) => (
            400,
            json!({
                "__type": "com.amazon.coral.validate#ValidationException",
                "message": message
            }),
        ),
    };
    let expected_status = case["expected"]["status"]
        .as_u64()
        .expect("expected status") as u16;
    assert_eq!(
        status, expected_status,
        "{} returned {body}",
        case["source"]
    );
    let mut expected_body =
        serde_json::to_string(&case["expected"]["body"]).expect("serialize authoritative response");
    for name in request_items.keys() {
        let token = if name.starts_with("dynamo-conformance-basic-") {
            "<<table:basic_pk_sk>>"
        } else {
            "<<table:basic_pk>>"
        };
        expected_body = expected_body.replace(token, name);
    }
    assert!(
        !expected_body.contains("<<table:"),
        "unresolved table placeholder"
    );
    let mut expected: Value =
        serde_json::from_str(&expected_body).expect("parse authoritative response");
    let mut actual = body;
    normalize_attribute_sets(&mut expected);
    normalize_attribute_sets(&mut actual);
    normalize_batch_response_order(&mut expected);
    normalize_batch_response_order(&mut actual);
    assert_conformance_body(
        &actual,
        &expected,
        case["source"].as_str().expect("fixture source"),
    );

    for precondition in &preconditions {
        let item = &precondition["request"]["Item"];
        let mut key = json!({"pk": item["pk"]});
        if let Some(sort_key) = item.get("sk") {
            key["sk"] = sort_key.clone();
        }
        let table = TableName::new(precondition["request"]["TableName"].as_str().unwrap());
        let read = store
            .operations
            .get_item(GetItemRequest::new(
                table,
                serde_json::from_value::<KeyAttributes>(key).expect("parse seeded key"),
            ))
            .await
            .expect("read seeded item after BatchGetItem");
        let mut expected_item: AttributeMap =
            serde_json::from_value(item.clone()).expect("parse seeded item");
        expected_item.normalize_numbers_for_write();
        assert_eq!(
            serde_json::to_value(read).expect("serialize seeded item")["Item"],
            serde_json::to_value(expected_item).expect("serialize expected item"),
            "{} changed a seeded item",
            case["source"],
        );
    }
}

// DynamoDB does not promise the order of items returned by BatchGetItem.
fn normalize_batch_response_order(body: &mut Value) {
    let Some(responses) = body.get_mut("Responses").and_then(Value::as_object_mut) else {
        return;
    };
    for items in responses.values_mut() {
        if let Value::Array(items) = items {
            items.sort_by_key(|item| serde_json::to_string(item).expect("serialize batch item"));
        }
    }
}

macro_rules! batch_get_case {
    ($name:ident, $source:literal) => {
        #[tokio::test]
        async fn $name() {
            let case = fixture($source);
            let store = TestStore::new().await;
            let outcome = AssertUnwindSafe(assert_batch_get_case(&store, &case))
                .catch_unwind()
                .await;
            store.finish().await;
            if let Err(panic) = outcome {
                resume_unwind(panic);
            }
        }
    };
}

// Business rule: an existing key returns its complete item.
batch_get_case!(
    given_one_existing_key_when_batch_get_item_runs_then_full_item_matches_aws,
    "field-request_items/200/parity_request_1_existing_keys.json"
);

// Business rule: a missing key yields an empty table response.
batch_get_case!(
    given_one_missing_key_when_batch_get_item_runs_then_empty_response_matches_aws,
    "field-request_items/200/parity_request_all_1_keys_missing.json"
);

// Boundary rule: one request may read two tables in a single operation.
batch_get_case!(
    given_keys_in_two_tables_when_batch_get_item_runs_then_both_responses_match_aws,
    "field-request_items/200/parity_request_multi_table_1_and_1.json"
);

// Format rule: nested projection retains only the requested child.
batch_get_case!(
    given_nested_projection_when_batch_get_item_runs_then_shape_matches_aws,
    "field-projection_expression/200/parity_project_nested_child_1_keys.json"
);

// Negative rule: duplicate keys return the exact validation response.
batch_get_case!(
    given_duplicate_keys_when_batch_get_item_runs_then_validation_error_matches_aws,
    "field-request_items/400/parity_duplicate_keys.json"
);

// Boundary and validation rules: key limits and an empty request.
batch_get_case!(
    given_one_hundred_missing_keys_when_batch_get_item_runs_then_empty_response_matches_aws,
    "field-request_items/200/docs_limit_100_missing_keys.json"
);
batch_get_case!(
    given_maximum_partition_key_when_batch_get_item_runs_then_response_matches_aws,
    "field-request_items/200/docs_limit_partition_key_2048_bytes_valid.json"
);
batch_get_case!(
    given_oversized_partition_key_when_batch_get_item_runs_then_validation_matches_aws,
    "field-request_items/400/docs_limit_partition_key_2049_bytes_rejected.json"
);
batch_get_case!(
    given_empty_request_items_when_batch_get_item_runs_then_validation_matches_aws,
    "field-request_items/400/parity_empty_request_items.json"
);

// Business and format rules: multi-table reads and unordered response items.
batch_get_case!(
    given_two_keys_in_two_tables_when_batch_get_item_runs_then_response_matches_aws,
    "field-request_items/200/parity_request_multi_table_2_and_2.json"
);
batch_get_case!(
    given_unordered_keys_when_batch_get_item_runs_then_complete_response_matches_aws,
    "field-request_items/200/parity_request_unordered_response_order_3.json"
);
batch_get_case!(
    given_aliased_nested_reserved_word_when_batch_get_item_runs_then_projection_matches_aws,
    "field-projection_expression/200/aliased_nested_reserved_word.json"
);
batch_get_case!(
    given_raw_reserved_child_when_batch_get_item_runs_then_validation_matches_aws,
    "field-projection_expression/400/alias_parent_raw_reserved_child.json"
);

// Business, boundary, and format rules: field-projection_expression/200.
batch_get_case!(given_field_projection_expression_200_docs_limit_expression_attribute_name_token_255_bytes_valid_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/docs_limit_expression_attribute_name_token_255_bytes_valid.json");
batch_get_case!(given_field_projection_expression_200_docs_limit_expression_attribute_name_value_255_bytes_valid_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/docs_limit_expression_attribute_name_value_255_bytes_valid.json");
batch_get_case!(given_field_projection_expression_200_docs_limit_expression_attribute_name_value_256_bytes_valid_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/docs_limit_expression_attribute_name_value_256_bytes_valid.json");
batch_get_case!(given_field_projection_expression_200_docs_limit_projection_expression_4096_bytes_valid_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/docs_limit_projection_expression_4096_bytes_valid.json");
batch_get_case!(given_field_projection_expression_200_parity_project_absent_and_present_1_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_absent_and_present_1_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_absent_and_present_3_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_absent_and_present_3_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_absent_only_1_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_absent_only_1_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_absent_only_3_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_absent_only_3_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_alias_nested_1_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_alias_nested_1_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_alias_nested_3_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_alias_nested_3_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_alias_note_1_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_alias_note_1_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_alias_note_3_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_alias_note_3_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_alias_reserved_child_after_list_1_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_alias_reserved_child_after_list_1_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_alias_reserved_child_after_list_3_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_alias_reserved_child_after_list_3_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_all_top_level_selected_1_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_all_top_level_selected_1_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_all_top_level_selected_3_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_all_top_level_selected_3_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_deep_absent_1_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_deep_absent_1_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_deep_absent_3_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_deep_absent_3_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_keys_and_nested_1_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_keys_and_nested_1_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_keys_and_nested_3_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_keys_and_nested_3_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_keys_only_1_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_keys_only_1_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_keys_only_3_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_keys_only_3_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_list_absent_index_1_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_list_absent_index_1_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_list_absent_index_3_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_list_absent_index_3_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_list_first_1_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_list_first_1_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_list_first_3_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_list_first_3_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_list_second_1_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_list_second_1_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_list_second_3_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_list_second_3_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_map_parent_1_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_map_parent_1_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_map_parent_3_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_map_parent_3_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_multiple_scalars_1_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_multiple_scalars_1_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_multiple_scalars_3_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_multiple_scalars_3_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_nested_child_3_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_nested_child_3_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_nested_number_1_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_nested_number_1_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_nested_number_3_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_nested_number_3_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_note_only_1_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_note_only_1_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_note_only_3_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_note_only_3_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_number_only_1_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_number_only_1_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_number_only_3_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_number_only_3_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_set_number_1_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_set_number_1_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_set_number_3_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_set_number_3_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_set_string_1_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_set_string_1_keys.json");
batch_get_case!(given_field_projection_expression_200_parity_project_set_string_3_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/200/parity_project_set_string_3_keys.json");

// Validation and negative rules: field-projection_expression/400.
batch_get_case!(given_field_projection_expression_400_docs_limit_expression_attribute_name_token_256_bytes_rejected_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/400/docs_limit_expression_attribute_name_token_256_bytes_rejected.json");
batch_get_case!(given_field_projection_expression_400_docs_limit_projection_expression_4097_bytes_rejected_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/400/docs_limit_projection_expression_4097_bytes_rejected.json");
batch_get_case!(given_field_projection_expression_400_invalid_expression_attribute_name_key_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/400/invalid_expression_attribute_name_key.json");
batch_get_case!(given_field_projection_expression_400_missing_expression_attribute_name_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/400/missing_expression_attribute_name.json");
batch_get_case!(given_field_projection_expression_400_nested_reserved_word_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/400/nested_reserved_word.json");
batch_get_case!(given_field_projection_expression_400_parity_alpha_list_index_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/400/parity_alpha_list_index.json");
batch_get_case!(given_field_projection_expression_400_parity_empty_list_index_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/400/parity_empty_list_index.json");
batch_get_case!(given_field_projection_expression_400_parity_invalid_expression_attribute_name_key_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/400/parity_invalid_expression_attribute_name_key.json");
batch_get_case!(given_field_projection_expression_400_parity_list_raw_reserved_word_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/400/parity_list_raw_reserved_word.json");
batch_get_case!(given_field_projection_expression_400_parity_missing_expression_attribute_name_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/400/parity_missing_expression_attribute_name.json");
batch_get_case!(given_field_projection_expression_400_parity_negative_list_index_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/400/parity_negative_list_index.json");
batch_get_case!(given_field_projection_expression_400_parity_nested_reserved_word_comment_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/400/parity_nested_reserved_word_comment.json");
batch_get_case!(given_field_projection_expression_400_parity_overlapping_parent_and_child_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/400/parity_overlapping_parent_and_child.json");
batch_get_case!(given_field_projection_expression_400_parity_reserved_word_comment_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/400/parity_reserved_word_comment.json");
batch_get_case!(given_field_projection_expression_400_parity_unused_expression_attribute_name_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/400/parity_unused_expression_attribute_name.json");
batch_get_case!(given_field_projection_expression_400_reserved_word_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/400/reserved_word.json");
batch_get_case!(given_field_projection_expression_400_unused_expression_attribute_name_when_batch_get_item_runs_then_conformance_response_matches, "field-projection_expression/400/unused_expression_attribute_name.json");

// Business, boundary, and format rules: field-request_items/200.
batch_get_case!(given_field_request_items_200_docs_limit_99_missing_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/docs_limit_99_missing_keys.json");
batch_get_case!(given_field_request_items_200_docs_limit_non_key_empty_string_and_binary_values_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/docs_limit_non_key_empty_string_and_binary_values.json");
batch_get_case!(given_field_request_items_200_docs_limit_sort_key_1024_bytes_valid_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/docs_limit_sort_key_1024_bytes_valid.json");
batch_get_case!(given_field_request_items_200_parity_request_2_existing_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_2_existing_keys.json");
batch_get_case!(given_field_request_items_200_parity_request_3_existing_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_3_existing_keys.json");
batch_get_case!(given_field_request_items_200_parity_request_4_existing_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_4_existing_keys.json");
batch_get_case!(given_field_request_items_200_parity_request_5_existing_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_5_existing_keys.json");
batch_get_case!(given_field_request_items_200_parity_request_6_existing_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_6_existing_keys.json");
batch_get_case!(given_field_request_items_200_parity_request_all_2_keys_missing_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_all_2_keys_missing.json");
batch_get_case!(given_field_request_items_200_parity_request_all_3_keys_missing_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_all_3_keys_missing.json");
batch_get_case!(given_field_request_items_200_parity_request_all_4_keys_missing_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_all_4_keys_missing.json");
batch_get_case!(given_field_request_items_200_parity_request_all_5_keys_missing_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_all_5_keys_missing.json");
batch_get_case!(given_field_request_items_200_parity_request_all_6_keys_missing_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_all_6_keys_missing.json");
batch_get_case!(given_field_request_items_200_parity_request_existing_2_plus_tail_missing_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_existing_2_plus_tail_missing.json");
batch_get_case!(given_field_request_items_200_parity_request_existing_3_plus_tail_missing_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_existing_3_plus_tail_missing.json");
batch_get_case!(given_field_request_items_200_parity_request_existing_4_plus_tail_missing_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_existing_4_plus_tail_missing.json");
batch_get_case!(given_field_request_items_200_parity_request_existing_5_plus_tail_missing_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_existing_5_plus_tail_missing.json");
batch_get_case!(given_field_request_items_200_parity_request_existing_6_plus_tail_missing_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_existing_6_plus_tail_missing.json");
batch_get_case!(given_field_request_items_200_parity_request_mixed_existing_missing_order_1_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_mixed_existing_missing_order_1.json");
batch_get_case!(given_field_request_items_200_parity_request_mixed_existing_missing_order_2_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_mixed_existing_missing_order_2.json");
batch_get_case!(given_field_request_items_200_parity_request_mixed_existing_missing_order_3_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_mixed_existing_missing_order_3.json");
batch_get_case!(given_field_request_items_200_parity_request_multi_table_3_and_3_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_multi_table_3_and_3.json");
batch_get_case!(given_field_request_items_200_parity_request_multi_table_4_and_4_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_multi_table_4_and_4.json");
batch_get_case!(given_field_request_items_200_parity_request_multi_table_5_and_5_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_multi_table_5_and_5.json");
batch_get_case!(given_field_request_items_200_parity_request_multi_table_6_and_6_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_multi_table_6_and_6.json");
batch_get_case!(given_field_request_items_200_parity_request_one_existing_1_missing_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_one_existing_1_missing_keys.json");
batch_get_case!(given_field_request_items_200_parity_request_one_existing_2_missing_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_one_existing_2_missing_keys.json");
batch_get_case!(given_field_request_items_200_parity_request_one_existing_3_missing_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_one_existing_3_missing_keys.json");
batch_get_case!(given_field_request_items_200_parity_request_one_existing_4_missing_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_one_existing_4_missing_keys.json");
batch_get_case!(given_field_request_items_200_parity_request_one_existing_5_missing_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_one_existing_5_missing_keys.json");
batch_get_case!(given_field_request_items_200_parity_request_one_existing_6_missing_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_one_existing_6_missing_keys.json");
batch_get_case!(given_field_request_items_200_parity_request_unordered_response_order_1_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_unordered_response_order_1.json");
batch_get_case!(given_field_request_items_200_parity_request_unordered_response_order_2_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_unordered_response_order_2.json");
batch_get_case!(given_field_request_items_200_parity_request_unordered_response_order_4_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/200/parity_request_unordered_response_order_4.json");

// Validation and negative rules: field-request_items/400.
batch_get_case!(given_field_request_items_400_docs_limit_partition_key_empty_rejected_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_partition_key_empty_rejected.json");
batch_get_case!(given_field_request_items_400_docs_limit_sort_key_1025_bytes_rejected_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_sort_key_1025_bytes_rejected.json");
batch_get_case!(given_field_request_items_400_docs_limit_sort_key_empty_rejected_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_sort_key_empty_rejected.json");
batch_get_case!(given_field_request_items_400_parity_empty_key_object_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/400/parity_empty_key_object.json");
batch_get_case!(given_field_request_items_400_parity_empty_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/400/parity_empty_keys.json");
batch_get_case!(given_field_request_items_400_parity_extra_key_attribute_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/400/parity_extra_key_attribute.json");
batch_get_case!(given_field_request_items_400_parity_missing_sort_key_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/400/parity_missing_sort_key.json");
batch_get_case!(given_field_request_items_400_parity_too_many_keys_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/400/parity_too_many_keys.json");
batch_get_case!(given_field_request_items_400_parity_wrong_partition_key_type_when_batch_get_item_runs_then_conformance_response_matches, "field-request_items/400/parity_wrong_partition_key_type.json");

fn sized_batch_keys(count: usize) -> Vec<storage_types::KeyAttributes> {
    (0..count)
        .map(|index| {
            KeyAttributes::from_iter([(
                "pk".to_owned(),
                storage_types::AttributeValue::S(format!("over-{index:03}")),
            )])
        })
        .collect()
}

#[test]
fn given_hundred_keys_when_validated_then_accepted() {
    let table = TableName::new("point-read-boundary");
    let request = BatchGetItemRequest {
        request_items: storage_types::VecMap::from_iter([(
            table,
            KeysAndAttributes {
                keys: sized_batch_keys(100).into_iter().collect(),
                attributes_to_get: None,
                projection_expression: None,
                expression_attribute_names: None,
                consistent_read: None,
            },
        )]),
        return_consumed_capacity: None,
    };
    request
        .validate_for_dynamodb()
        .expect("exactly 100 keys is the public boundary");
}

#[test]
fn given_keys_over_public_limit_when_validated_then_rejected() {
    let table = TableName::new("point-read-boundary");
    let over_single_table = BatchGetItemRequest {
        request_items: storage_types::VecMap::from_iter([(
            table.clone(),
            KeysAndAttributes {
                keys: sized_batch_keys(101).into_iter().collect(),
                attributes_to_get: None,
                projection_expression: None,
                expression_attribute_names: None,
                consistent_read: None,
            },
        )]),
        return_consumed_capacity: None,
    };
    let error = over_single_table
        .validate_for_dynamodb()
        .expect_err("101 keys in one table must be rejected");
    assert!(
        error.contains("less than or equal to 100"),
        "unexpected per-table limit message: {error}"
    );

    let over_total = BatchGetItemRequest {
        request_items: storage_types::VecMap::from_iter([
            (
                table.clone(),
                KeysAndAttributes {
                    keys: sized_batch_keys(60).into_iter().collect(),
                    attributes_to_get: None,
                    projection_expression: None,
                    expression_attribute_names: None,
                    consistent_read: None,
                },
            ),
            (
                TableName::new("point-read-boundary-b"),
                KeysAndAttributes {
                    keys: sized_batch_keys(41).into_iter().collect(),
                    attributes_to_get: None,
                    projection_expression: None,
                    expression_attribute_names: None,
                    consistent_read: None,
                },
            ),
        ]),
        return_consumed_capacity: None,
    };
    let error = over_total
        .validate_for_dynamodb()
        .expect_err("101 keys across tables must be rejected");
    assert!(
        error.contains("Maximum allowed: 100"),
        "unexpected total limit message: {error}"
    );

    let empty_keys = BatchGetItemRequest {
        request_items: storage_types::VecMap::from_iter([(
            table,
            KeysAndAttributes {
                keys: Default::default(),
                attributes_to_get: None,
                projection_expression: None,
                expression_attribute_names: None,
                consistent_read: None,
            },
        )]),
        return_consumed_capacity: None,
    };
    assert!(
        empty_keys.validate_for_dynamodb().is_err(),
        "empty key list must be rejected"
    );
}

#[tokio::test]
async fn given_seeded_batches_when_batch_get_runs_then_every_item_and_payload_is_exact() {
    for count in [10, 100] {
        let name = format!("dynamo-conformance-pk-seeded-{}", uuid::Uuid::new_v4());
        let items: Vec<_> = (0..count)
            .map(|index| {
                json!({
                    "pk": {"S": format!("k{index:03}")},
                    "value": {"S": format!("v{index:03}")}
                })
            })
            .collect();
        let mut keys: Vec<_> = items.iter().map(|item| json!({"pk": item["pk"]})).collect();
        if count == 10 {
            keys.extend([
                json!({"pk": {"S": "absent-a"}}),
                json!({"pk": {"S": "absent-b"}}),
            ]);
        }
        let preconditions: Vec<_> = items
            .iter()
            .map(|item| {
                json!({
                    "operation": "PutItem", "request": {"TableName": name, "Item": item}
                })
            })
            .collect();
        let case = json!({
            "operation": "BatchGetItem", "source": "seeded batch exact coverage",
            "tables": [name], "preconditions": preconditions,
            "request": {"RequestItems": {name.clone(): {"Keys": keys}}},
            "expected": {"status": 200, "body": {
                "Responses": {"<<table:basic_pk>>": items}, "UnprocessedKeys": {}
            }}
        });
        let store = TestStore::new().await;
        let outcome = AssertUnwindSafe(assert_batch_get_case(&store, &case))
            .catch_unwind()
            .await;
        store.finish().await;
        if let Err(panic) = outcome {
            resume_unwind(panic);
        }
    }
}

#[tokio::test]
async fn given_missing_catalog_table_when_batch_get_runs_then_request_fails_and_existing_item_survives()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = super::test_support::unique_table_name();
        let missing = super::test_support::unique_table_name();
        store
            .create_conformance_table("basic_pk", table.clone())
            .await;
        let item = json!({"pk": {"S": "seed"}, "value": {"S": "kept"}});
        store
            .operations
            .put_item(
                PutItemRequest::try_from(json!({
                    "TableName": table, "Item": item
                }))
                .expect("valid put"),
            )
            .await
            .expect("seed item");
        for names in [
            [table.clone(), missing.clone()],
            [missing.clone(), table.clone()],
        ] {
            let request = BatchGetItemRequest::try_from(json!({"RequestItems": {
                names[0].as_ref(): {"Keys": [{"pk": {"S": "seed"}}]},
                names[1].as_ref(): {"Keys": [{"pk": {"S": "seed"}}]}
            }}))
            .expect("valid mixed-table request");
            let error = store
                .operations
                .batch_get_item(request)
                .await
                .expect_err("missing table is fatal");
            assert!(
                matches!(error.as_ref(), storage_types::StorageEnum::TableNotFound { name, .. }
                if name == missing.as_ref()),
                "wrong error: {error}"
            );
        }
        let response = store
            .operations
            .get_item(
                GetItemRequest::try_from(json!({
                    "TableName": table, "Key": {"pk": {"S": "seed"}}, "ConsistentRead": true
                }))
                .expect("valid get"),
            )
            .await
            .expect("existing item survives");
        assert_eq!(
            serde_json::to_value(response).expect("serialize item"),
            json!({"Item": item})
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

#[tokio::test]
async fn given_mixed_consistency_batch_when_batch_get_runs_then_present_and_missing_items_are_exact()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let mut request_items = serde_json::Map::new();
        let mut responses = serde_json::Map::new();
        let mut preconditions = Vec::new();
        for (consistent, value) in [(true, "strong"), (false, "eventual")] {
            let name = format!("dynamo-conformance-pk-mixed-{}", uuid::Uuid::new_v4());
            let item = json!({"pk": {"S": "present"}, "value": {"S": value}});
            request_items.insert(
                name.clone(),
                json!({
                    "Keys": [{"pk": {"S": "present"}}, {"pk": {"S": "missing"}}],
                    "ConsistentRead": consistent
                }),
            );
            responses.insert(name.clone(), json!([item]));
            preconditions.push(json!({"operation": "PutItem", "request": {
                "TableName": name, "Item": item
            }}));
        }
        let case = json!({
            "operation": "BatchGetItem", "source": "mixed consistency exact coverage",
            "tables": request_items.keys().collect::<Vec<_>>(), "preconditions": preconditions,
            "request": {"RequestItems": request_items},
            "expected": {"status": 200, "body": {"Responses": responses, "UnprocessedKeys": {}}}
        });
        assert_batch_get_case(&store, &case).await;
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
