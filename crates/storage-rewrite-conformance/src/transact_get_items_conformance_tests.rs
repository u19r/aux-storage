//! TransactGetItems public responses from the authoritative DynamoDB fixture
//! suite.
//!
//! The fixture snapshot is from ../dynamo-conformance at
//! 2158288d7883de4be6c17da00226d8398faa723a.

use std::{
    collections::BTreeSet,
    panic::{AssertUnwindSafe, resume_unwind},
};

use futures::FutureExt as _;
use serde_json::{Value, json};
use storage_observability_test_support::observation::observe;
use storage_types::{
    AttributeMap, GetItemRequest, KeyAttributes, PutItemRequest, TableName, TransactGetItemsRequest,
};

use super::test_support::{ConformanceFixtures, TestStore, assert_conformance_body};

static FIXTURES: ConformanceFixtures = ConformanceFixtures::new();

fn fixture(source: &str) -> Value {
    FIXTURES.case(
        include_str!("../fixtures/transact_get_items_cases.jsonl"),
        source,
    )
}

fn conformance_table_kind(name: &str) -> Option<&'static str> {
    if name.starts_with("dynamo-conformance-basic-") {
        Some("basic_pk_sk")
    } else if name.starts_with("dynamo-conformance-pk-") {
        Some("basic_pk")
    } else {
        None
    }
}

async fn assert_transact_get_case(store: &TestStore, case: &Value) {
    assert_eq!(case["operation"], "TransactGetItems", "fixture operation");
    let table_kinds = case["tables"].as_array().expect("fixture table kinds");
    let preconditions = case["preconditions"]
        .as_array()
        .expect("fixture preconditions");
    let gets = case["request"]["TransactItems"]
        .as_array()
        .expect("fixture TransactItems");
    let mut table_names = BTreeSet::new();
    for get in gets {
        let name = get["Get"]["TableName"].as_str().expect("Get table name");
        table_names.insert(name);
    }
    for precondition in preconditions {
        assert_eq!(
            precondition["operation"], "PutItem",
            "precondition operation"
        );
        table_names.insert(
            precondition["request"]["TableName"]
                .as_str()
                .expect("seed table name"),
        );
    }
    for name in table_names {
        if let Some(kind) = conformance_table_kind(name) {
            assert!(
                table_kinds.iter().any(|table| table == kind),
                "undeclared fixture table kind"
            );
            store
                .create_conformance_table(kind, TableName::new(name))
                .await;
        }
    }
    for precondition in preconditions {
        let request = PutItemRequest::try_from(precondition["request"].clone())
            .expect("parse fixture precondition");
        store
            .operations
            .put_item(request)
            .await
            .expect("seed fixture item");
    }

    let parsed =
        observe(async { TransactGetItemsRequest::try_from(case["request"].clone()) }).await;
    parsed.assert_no_warning_or_error();
    parsed.assert_database_calls_exact(&[]);
    let (status, body) = match parsed.result {
        Ok(request) => {
            let observed = observe(store.operations.transact_get_items(request)).await;
            observed.assert_no_warning_or_error();
            match observed.result {
                Ok(response) => (
                    200,
                    serde_json::to_value(response).expect("serialize TransactGetItems"),
                ),
                Err(error) => {
                    let (status, axum::Json(body)): (_, axum::Json<http_error::ErrorResponse>) =
                        http_error::HttpApiError::from(error).into();
                    (
                        status.as_u16(),
                        serde_json::to_value(body).expect("serialize TransactGetItems error"),
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
    assert_conformance_body(
        &body,
        &case["expected"]["body"],
        case["source"].as_str().expect("fixture source"),
    );

    for precondition in preconditions {
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
            .expect("read seeded item after TransactGetItems");
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

macro_rules! transact_get_case {
    ($name:ident, $source:literal) => {
        #[tokio::test]
        async fn $name() {
            let case = fixture($source);
            let store = TestStore::new().await;
            let outcome = AssertUnwindSafe(assert_transact_get_case(&store, &case))
                .catch_unwind()
                .await;
            store.finish().await;
            if let Err(panic) = outcome {
                resume_unwind(panic);
            }
        }
    };
}

// Business rule: existing and missing members retain their request order.
transact_get_case!(
    given_existing_and_missing_keys_when_transact_get_runs_then_ordered_responses_match_aws,
    "200/existing_and_missing_item_responses.json"
);

// Business rule: one transaction can interleave reads from two tables.
transact_get_case!(
    given_two_tables_when_transact_get_runs_then_interleaved_responses_match_aws,
    "field-transact_items/200/parity_multi_table_interleaved_1.json"
);

// Format rule: nested projection retains only the requested child.
transact_get_case!(
    given_nested_projection_when_transact_get_runs_then_response_matches_aws,
    "field-projection_expression/200/parity_projection_nested_child.json"
);

// Negative rule: duplicate keys fail with the exact public response.
transact_get_case!(
    given_duplicate_keys_when_transact_get_runs_then_validation_matches_aws,
    "400/duplicate_keys.json"
);

// Validation rule: a missing table takes precedence over a later duplicate.
transact_get_case!(
    given_missing_table_before_duplicate_when_transact_get_runs_then_error_matches_aws,
    "400/parity_missing_table_before_duplicate_keys.json"
);

// Boundary rule: the maximum supported member count retains all response slots.
transact_get_case!(
    given_one_hundred_missing_keys_when_transact_get_runs_then_all_response_slots_match_aws,
    "field-transact_items/200/docs_limit_100_missing_items.json"
);

// Negative rule: key-type mismatch returns the exact cancellation response.
transact_get_case!(
    given_mismatched_key_type_when_transact_get_runs_then_cancellation_matches_aws,
    "400/key_type_mismatch_cancellation.json"
);

// Additional authoritative cases, each independently runnable.
// Negative and validation rule:
// 400/parity_duplicate_keys_later_key_type_mismatch.json.
transact_get_case!(given_parity_duplicate_keys_later_key_type_mismatch_when_transact_get_items_runs_then_error_matches_aws, "400/parity_duplicate_keys_later_key_type_mismatch.json");

// Negative and validation rule:
// 400/parity_duplicate_keys_later_missing_table.json.
transact_get_case!(given_parity_duplicate_keys_later_missing_table_when_transact_get_items_runs_then_error_matches_aws, "400/parity_duplicate_keys_later_missing_table.json");

// Negative and validation rule:
// 400/parity_invalid_projection_alias_before_key_mismatch.json.
transact_get_case!(given_parity_invalid_projection_alias_before_key_mismatch_when_transact_get_items_runs_then_error_matches_aws, "400/parity_invalid_projection_alias_before_key_mismatch.json");

// Negative and validation rule:
// 400/parity_key_type_mismatch_before_duplicate_keys.json.
transact_get_case!(given_parity_key_type_mismatch_before_duplicate_keys_when_transact_get_items_runs_then_error_matches_aws, "400/parity_key_type_mismatch_before_duplicate_keys.json");

// Negative and validation rule:
// 400/parity_malformed_projection_before_missing_table.json.
transact_get_case!(given_parity_malformed_projection_before_missing_table_when_transact_get_items_runs_then_error_matches_aws, "400/parity_malformed_projection_before_missing_table.json");

// Negative and validation rule:
// 400/parity_missing_projection_alias_before_key_mismatch.json.
transact_get_case!(given_parity_missing_projection_alias_before_key_mismatch_when_transact_get_items_runs_then_error_matches_aws, "400/parity_missing_projection_alias_before_key_mismatch.json");

// Negative and validation rule: 400/parity_missing_table.json.
transact_get_case!(
    given_parity_missing_table_when_transact_get_items_runs_then_error_matches_aws,
    "400/parity_missing_table.json"
);

// Negative and validation rule:
// 400/parity_unused_projection_alias_before_key_mismatch.json.
transact_get_case!(given_parity_unused_projection_alias_before_key_mismatch_when_transact_get_items_runs_then_error_matches_aws, "400/parity_unused_projection_alias_before_key_mismatch.json");

// Boundary rule:
// field-projection_expression/200/
// docs_limit_expression_attribute_name_token_255_bytes_valid.json.
transact_get_case!(given_docs_limit_expression_attribute_name_token_255_bytes_valid_when_transact_get_items_runs_then_response_matches_aws, "field-projection_expression/200/docs_limit_expression_attribute_name_token_255_bytes_valid.json");

// Boundary rule:
// field-projection_expression/200/
// docs_limit_expression_attribute_name_value_255_bytes_valid.json.
transact_get_case!(given_docs_limit_expression_attribute_name_value_255_bytes_valid_when_transact_get_items_runs_then_response_matches_aws, "field-projection_expression/200/docs_limit_expression_attribute_name_value_255_bytes_valid.json");

// Boundary rule:
// field-projection_expression/200/
// docs_limit_expression_attribute_name_value_256_bytes_valid.json.
transact_get_case!(given_docs_limit_expression_attribute_name_value_256_bytes_valid_when_transact_get_items_runs_then_response_matches_aws, "field-projection_expression/200/docs_limit_expression_attribute_name_value_256_bytes_valid.json");

// Boundary rule:
// field-projection_expression/200/
// docs_limit_projection_expression_4096_bytes_valid.json.
transact_get_case!(given_docs_limit_projection_expression_4096_bytes_valid_when_transact_get_items_runs_then_response_matches_aws, "field-projection_expression/200/docs_limit_projection_expression_4096_bytes_valid.json");

// Format rule:
// field-projection_expression/200/
// missing_projected_attribute_returns_empty_response.json.
transact_get_case!(given_missing_projected_attribute_returns_empty_response_when_transact_get_items_runs_then_response_matches_aws, "field-projection_expression/200/missing_projected_attribute_returns_empty_response.json");

// Format rule:
// field-projection_expression/200/parity_projection_absent_and_present.json.
transact_get_case!(given_parity_projection_absent_and_present_when_transact_get_items_runs_then_response_matches_aws, "field-projection_expression/200/parity_projection_absent_and_present.json");

// Format rule:
// field-projection_expression/200/parity_projection_absent_only.json.
transact_get_case!(
    given_parity_projection_absent_only_when_transact_get_items_runs_then_response_matches_aws,
    "field-projection_expression/200/parity_projection_absent_only.json"
);

// Format rule:
// field-projection_expression/200/parity_projection_alias_nested.json.
transact_get_case!(
    given_parity_projection_alias_nested_when_transact_get_items_runs_then_response_matches_aws,
    "field-projection_expression/200/parity_projection_alias_nested.json"
);

// Format rule:
// field-projection_expression/200/parity_projection_alias_note.json.
transact_get_case!(
    given_parity_projection_alias_note_when_transact_get_items_runs_then_response_matches_aws,
    "field-projection_expression/200/parity_projection_alias_note.json"
);

// Format rule:
// field-projection_expression/200/
// parity_projection_alias_reserved_child_after_list.json.
transact_get_case!(given_parity_projection_alias_reserved_child_after_list_when_transact_get_items_runs_then_response_matches_aws, "field-projection_expression/200/parity_projection_alias_reserved_child_after_list.json");

// Format rule:
// field-projection_expression/200/parity_projection_all_top_level_selected.
// json.
transact_get_case!(given_parity_projection_all_top_level_selected_when_transact_get_items_runs_then_response_matches_aws, "field-projection_expression/200/parity_projection_all_top_level_selected.json");

// Format rule:
// field-projection_expression/200/parity_projection_deep_absent.json.
transact_get_case!(
    given_parity_projection_deep_absent_when_transact_get_items_runs_then_response_matches_aws,
    "field-projection_expression/200/parity_projection_deep_absent.json"
);

// Format rule:
// field-projection_expression/200/parity_projection_keys_and_nested.json.
transact_get_case!(
    given_parity_projection_keys_and_nested_when_transact_get_items_runs_then_response_matches_aws,
    "field-projection_expression/200/parity_projection_keys_and_nested.json"
);

// Format rule:
// field-projection_expression/200/parity_projection_keys_only.json.
transact_get_case!(
    given_parity_projection_keys_only_when_transact_get_items_runs_then_response_matches_aws,
    "field-projection_expression/200/parity_projection_keys_only.json"
);

// Format rule:
// field-projection_expression/200/parity_projection_list_absent_index.json.
transact_get_case!(given_parity_projection_list_absent_index_when_transact_get_items_runs_then_response_matches_aws, "field-projection_expression/200/parity_projection_list_absent_index.json");

// Format rule:
// field-projection_expression/200/parity_projection_list_first.json.
transact_get_case!(
    given_parity_projection_list_first_when_transact_get_items_runs_then_response_matches_aws,
    "field-projection_expression/200/parity_projection_list_first.json"
);

// Format rule:
// field-projection_expression/200/parity_projection_list_second.json.
transact_get_case!(
    given_parity_projection_list_second_when_transact_get_items_runs_then_response_matches_aws,
    "field-projection_expression/200/parity_projection_list_second.json"
);

// Format rule:
// field-projection_expression/200/parity_projection_map_parent.json.
transact_get_case!(
    given_parity_projection_map_parent_when_transact_get_items_runs_then_response_matches_aws,
    "field-projection_expression/200/parity_projection_map_parent.json"
);

// Format rule:
// field-projection_expression/200/parity_projection_multiple_scalars.json.
transact_get_case!(
    given_parity_projection_multiple_scalars_when_transact_get_items_runs_then_response_matches_aws,
    "field-projection_expression/200/parity_projection_multiple_scalars.json"
);

// Format rule:
// field-projection_expression/200/parity_projection_nested_number.json.
transact_get_case!(
    given_parity_projection_nested_number_when_transact_get_items_runs_then_response_matches_aws,
    "field-projection_expression/200/parity_projection_nested_number.json"
);

// Format rule:
// field-projection_expression/200/parity_projection_note_only.json.
transact_get_case!(
    given_parity_projection_note_only_when_transact_get_items_runs_then_response_matches_aws,
    "field-projection_expression/200/parity_projection_note_only.json"
);

// Format rule:
// field-projection_expression/200/parity_projection_number_only.json.
transact_get_case!(
    given_parity_projection_number_only_when_transact_get_items_runs_then_response_matches_aws,
    "field-projection_expression/200/parity_projection_number_only.json"
);

// Format rule:
// field-projection_expression/200/parity_projection_set_number.json.
transact_get_case!(
    given_parity_projection_set_number_when_transact_get_items_runs_then_response_matches_aws,
    "field-projection_expression/200/parity_projection_set_number.json"
);

// Format rule:
// field-projection_expression/200/parity_projection_set_string.json.
transact_get_case!(
    given_parity_projection_set_string_when_transact_get_items_runs_then_response_matches_aws,
    "field-projection_expression/200/parity_projection_set_string.json"
);

// Format rule: field-projection_expression/200/project_nested_attribute.json.
transact_get_case!(
    given_project_nested_attribute_when_transact_get_items_runs_then_response_matches_aws,
    "field-projection_expression/200/project_nested_attribute.json"
);

// Negative and validation rule:
// field-projection_expression/400/
// docs_limit_expression_attribute_name_token_256_bytes_rejected.json.
transact_get_case!(given_docs_limit_expression_attribute_name_token_256_bytes_rejected_when_transact_get_items_runs_then_error_matches_aws, "field-projection_expression/400/docs_limit_expression_attribute_name_token_256_bytes_rejected.json");

// Negative and validation rule:
// field-projection_expression/400/
// docs_limit_projection_expression_4097_bytes_rejected.json.
transact_get_case!(given_docs_limit_projection_expression_4097_bytes_rejected_when_transact_get_items_runs_then_error_matches_aws, "field-projection_expression/400/docs_limit_projection_expression_4097_bytes_rejected.json");

// Negative and validation rule:
// field-projection_expression/400/invalid_expression_attribute_name_key.json.
transact_get_case!(
    given_invalid_expression_attribute_name_key_when_transact_get_items_runs_then_error_matches_aws,
    "field-projection_expression/400/invalid_expression_attribute_name_key.json"
);

// Negative and validation rule:
// field-projection_expression/400/missing_expression_attribute_name.json.
transact_get_case!(
    given_missing_expression_attribute_name_when_transact_get_items_runs_then_error_matches_aws,
    "field-projection_expression/400/missing_expression_attribute_name.json"
);

// Negative and validation rule:
// field-projection_expression/400/parity_alpha_list_index.json.
transact_get_case!(
    given_parity_alpha_list_index_when_transact_get_items_runs_then_error_matches_aws,
    "field-projection_expression/400/parity_alpha_list_index.json"
);

// Negative and validation rule:
// field-projection_expression/400/parity_empty_list_index.json.
transact_get_case!(
    given_parity_empty_list_index_when_transact_get_items_runs_then_error_matches_aws,
    "field-projection_expression/400/parity_empty_list_index.json"
);

// Negative and validation rule:
// field-projection_expression/400/parity_invalid_expression_attribute_name_key.
// json.
transact_get_case!(given_parity_invalid_expression_attribute_name_key_when_transact_get_items_runs_then_error_matches_aws, "field-projection_expression/400/parity_invalid_expression_attribute_name_key.json");

// Negative and validation rule:
// field-projection_expression/400/parity_list_raw_reserved_word.json.
transact_get_case!(
    given_parity_list_raw_reserved_word_when_transact_get_items_runs_then_error_matches_aws,
    "field-projection_expression/400/parity_list_raw_reserved_word.json"
);

// Negative and validation rule:
// field-projection_expression/400/parity_missing_expression_attribute_name.
// json.
transact_get_case!(given_parity_missing_expression_attribute_name_when_transact_get_items_runs_then_error_matches_aws, "field-projection_expression/400/parity_missing_expression_attribute_name.json");

// Negative and validation rule:
// field-projection_expression/400/parity_negative_list_index.json.
transact_get_case!(
    given_parity_negative_list_index_when_transact_get_items_runs_then_error_matches_aws,
    "field-projection_expression/400/parity_negative_list_index.json"
);

// Negative and validation rule:
// field-projection_expression/400/parity_nested_reserved_word_comment.json.
transact_get_case!(
    given_parity_nested_reserved_word_comment_when_transact_get_items_runs_then_error_matches_aws,
    "field-projection_expression/400/parity_nested_reserved_word_comment.json"
);

// Negative and validation rule:
// field-projection_expression/400/parity_overlapping_parent_and_child.json.
transact_get_case!(
    given_parity_overlapping_parent_and_child_when_transact_get_items_runs_then_error_matches_aws,
    "field-projection_expression/400/parity_overlapping_parent_and_child.json"
);

// Negative and validation rule:
// field-projection_expression/400/parity_reserved_word_comment.json.
transact_get_case!(
    given_parity_reserved_word_comment_when_transact_get_items_runs_then_error_matches_aws,
    "field-projection_expression/400/parity_reserved_word_comment.json"
);

// Negative and validation rule:
// field-projection_expression/400/parity_unused_expression_attribute_name.json.
transact_get_case!(given_parity_unused_expression_attribute_name_when_transact_get_items_runs_then_error_matches_aws, "field-projection_expression/400/parity_unused_expression_attribute_name.json");

// Negative and validation rule:
// field-projection_expression/400/reserved_word.json.
transact_get_case!(
    given_reserved_word_when_transact_get_items_runs_then_error_matches_aws,
    "field-projection_expression/400/reserved_word.json"
);

// Boundary rule: field-transact_items/200/docs_limit_99_missing_items.json.
transact_get_case!(
    given_docs_limit_99_missing_items_when_transact_get_items_runs_then_response_matches_aws,
    "field-transact_items/200/docs_limit_99_missing_items.json"
);

// Boundary rule:
// field-transact_items/200/docs_limit_non_key_empty_string_and_binary_values.
// json.
transact_get_case!(given_docs_limit_non_key_empty_string_and_binary_values_when_transact_get_items_runs_then_response_matches_aws, "field-transact_items/200/docs_limit_non_key_empty_string_and_binary_values.json");

// Boundary rule:
// field-transact_items/200/docs_limit_partition_key_2048_bytes_valid.json.
transact_get_case!(given_docs_limit_partition_key_2048_bytes_valid_when_transact_get_items_runs_then_response_matches_aws, "field-transact_items/200/docs_limit_partition_key_2048_bytes_valid.json");

// Boundary rule:
// field-transact_items/200/docs_limit_sort_key_1024_bytes_valid.json.
transact_get_case!(given_docs_limit_sort_key_1024_bytes_valid_when_transact_get_items_runs_then_response_matches_aws, "field-transact_items/200/docs_limit_sort_key_1024_bytes_valid.json");

// Business rule: field-transact_items/200/parity_10_ordered_gets.json.
transact_get_case!(
    given_parity_10_ordered_gets_when_transact_get_items_runs_then_response_matches_aws,
    "field-transact_items/200/parity_10_ordered_gets.json"
);

// Business rule: field-transact_items/200/parity_1_ordered_gets.json.
transact_get_case!(
    given_parity_1_ordered_gets_when_transact_get_items_runs_then_response_matches_aws,
    "field-transact_items/200/parity_1_ordered_gets.json"
);

// Business rule: field-transact_items/200/parity_2_ordered_gets.json.
transact_get_case!(
    given_parity_2_ordered_gets_when_transact_get_items_runs_then_response_matches_aws,
    "field-transact_items/200/parity_2_ordered_gets.json"
);

// Business rule: field-transact_items/200/parity_3_ordered_gets.json.
transact_get_case!(
    given_parity_3_ordered_gets_when_transact_get_items_runs_then_response_matches_aws,
    "field-transact_items/200/parity_3_ordered_gets.json"
);

// Business rule: field-transact_items/200/parity_4_ordered_gets.json.
transact_get_case!(
    given_parity_4_ordered_gets_when_transact_get_items_runs_then_response_matches_aws,
    "field-transact_items/200/parity_4_ordered_gets.json"
);

// Business rule: field-transact_items/200/parity_5_ordered_gets.json.
transact_get_case!(
    given_parity_5_ordered_gets_when_transact_get_items_runs_then_response_matches_aws,
    "field-transact_items/200/parity_5_ordered_gets.json"
);

// Business rule: field-transact_items/200/parity_6_ordered_gets.json.
transact_get_case!(
    given_parity_6_ordered_gets_when_transact_get_items_runs_then_response_matches_aws,
    "field-transact_items/200/parity_6_ordered_gets.json"
);

// Business rule: field-transact_items/200/parity_7_ordered_gets.json.
transact_get_case!(
    given_parity_7_ordered_gets_when_transact_get_items_runs_then_response_matches_aws,
    "field-transact_items/200/parity_7_ordered_gets.json"
);

// Business rule: field-transact_items/200/parity_8_ordered_gets.json.
transact_get_case!(
    given_parity_8_ordered_gets_when_transact_get_items_runs_then_response_matches_aws,
    "field-transact_items/200/parity_8_ordered_gets.json"
);

// Business rule: field-transact_items/200/parity_9_ordered_gets.json.
transact_get_case!(
    given_parity_9_ordered_gets_when_transact_get_items_runs_then_response_matches_aws,
    "field-transact_items/200/parity_9_ordered_gets.json"
);

// Business rule:
// field-transact_items/200/parity_multi_table_interleaved_2.json.
transact_get_case!(
    given_parity_multi_table_interleaved_2_when_transact_get_items_runs_then_response_matches_aws,
    "field-transact_items/200/parity_multi_table_interleaved_2.json"
);

// Business rule:
// field-transact_items/200/parity_multi_table_interleaved_3.json.
transact_get_case!(
    given_parity_multi_table_interleaved_3_when_transact_get_items_runs_then_response_matches_aws,
    "field-transact_items/200/parity_multi_table_interleaved_3.json"
);

// Business rule:
// field-transact_items/200/parity_multi_table_interleaved_4.json.
transact_get_case!(
    given_parity_multi_table_interleaved_4_when_transact_get_items_runs_then_response_matches_aws,
    "field-transact_items/200/parity_multi_table_interleaved_4.json"
);

// Business rule:
// field-transact_items/200/parity_multi_table_interleaved_5.json.
transact_get_case!(
    given_parity_multi_table_interleaved_5_when_transact_get_items_runs_then_response_matches_aws,
    "field-transact_items/200/parity_multi_table_interleaved_5.json"
);

// Business rule:
// field-transact_items/200/parity_multi_table_interleaved_6.json.
transact_get_case!(
    given_parity_multi_table_interleaved_6_when_transact_get_items_runs_then_response_matches_aws,
    "field-transact_items/200/parity_multi_table_interleaved_6.json"
);

// Business rule: field-transact_items/200/parity_preserves_order_1.json.
transact_get_case!(
    given_parity_preserves_order_1_when_transact_get_items_runs_then_response_matches_aws,
    "field-transact_items/200/parity_preserves_order_1.json"
);

// Business rule: field-transact_items/200/parity_preserves_order_2.json.
transact_get_case!(
    given_parity_preserves_order_2_when_transact_get_items_runs_then_response_matches_aws,
    "field-transact_items/200/parity_preserves_order_2.json"
);

// Business rule: field-transact_items/200/parity_preserves_order_3.json.
transact_get_case!(
    given_parity_preserves_order_3_when_transact_get_items_runs_then_response_matches_aws,
    "field-transact_items/200/parity_preserves_order_3.json"
);

// Business rule: field-transact_items/200/parity_preserves_order_4.json.
transact_get_case!(
    given_parity_preserves_order_4_when_transact_get_items_runs_then_response_matches_aws,
    "field-transact_items/200/parity_preserves_order_4.json"
);

// Negative and validation rule:
// field-transact_items/400/docs_limit_partition_key_2049_bytes_rejected.json.
transact_get_case!(given_docs_limit_partition_key_2049_bytes_rejected_when_transact_get_items_runs_then_error_matches_aws, "field-transact_items/400/docs_limit_partition_key_2049_bytes_rejected.json");

// Negative and validation rule:
// field-transact_items/400/docs_limit_partition_key_empty_rejected.json.
transact_get_case!(given_docs_limit_partition_key_empty_rejected_when_transact_get_items_runs_then_error_matches_aws, "field-transact_items/400/docs_limit_partition_key_empty_rejected.json");

// Negative and validation rule:
// field-transact_items/400/docs_limit_sort_key_1025_bytes_rejected.json.
transact_get_case!(given_docs_limit_sort_key_1025_bytes_rejected_when_transact_get_items_runs_then_error_matches_aws, "field-transact_items/400/docs_limit_sort_key_1025_bytes_rejected.json");

// Negative and validation rule:
// field-transact_items/400/docs_limit_sort_key_empty_rejected.json.
transact_get_case!(
    given_docs_limit_sort_key_empty_rejected_when_transact_get_items_runs_then_error_matches_aws,
    "field-transact_items/400/docs_limit_sort_key_empty_rejected.json"
);
