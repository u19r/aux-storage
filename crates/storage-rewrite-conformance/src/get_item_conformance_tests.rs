//! Independently runnable GetItem cases from dynamo-conformance and local
//! rules.
//!
//! The fixture snapshot is from ../dynamo-conformance at
//! 2158288d7883de4be6c17da00226d8398faa723a. Each case retains its source
//! path and compares the entire Dynamo response through the replacement store.
//! Local cases use a `local/` source path.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::{Value, json};
use storage_observability_test_support::observation::observe;
use storage_types::{AttributeMap, GetItemRequest, KeyAttributes, PutItemRequest, TableName};

use crate::test_support::{ConformanceFixtures, TestStore};

static FIXTURES: ConformanceFixtures = ConformanceFixtures::new();

fn fixture(source: &str) -> Value {
    FIXTURES.case(include_str!("../fixtures/get_item_cases.jsonl"), source)
}

async fn run_case(store: &TestStore, case: &Value) {
    assert_eq!(case["operation"], "GetItem", "fixture operation");
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

    let parsed = observe(async { GetItemRequest::try_from(case["request"].clone()) }).await;
    parsed.assert_no_warning_or_error();
    parsed.assert_database_calls_exact(&[]);
    let (status, body) = match parsed.result {
        Ok(request) => {
            let observed = observe(store.operations.get_item(request)).await;
            observed.assert_no_warning_or_error();
            let operation = observed.one_span_with("dynamo_operation", &[("operation", "GetItem")]);
            assert_eq!(
                operation.fields.len(),
                1,
                "GetItem span must contain only safe context"
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

    // Rejected reads and AttributesToGet projections must leave the complete
    // seeded item intact.
    if (status == 400 || case["request"].get("AttributesToGet").is_some())
        && let Some(precondition) = preconditions.last()
    {
        let seeded_item = &precondition["request"]["Item"];
        let key = case["request"]["Key"].clone();
        let mut expected_item: AttributeMap = serde_json::from_value(seeded_item.clone()).unwrap();
        expected_item.normalize_numbers_for_write();
        let read = store
            .operations
            .get_item(GetItemRequest::new(
                table,
                serde_json::from_value::<KeyAttributes>(key).unwrap(),
            ))
            .await
            .unwrap();
        let read = serde_json::to_value(read).unwrap();
        assert_eq!(
            read["Item"],
            serde_json::to_value(expected_item).unwrap(),
            "{} complete item readback",
            case["source"],
        );
    }
}

macro_rules! get_item_case {
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

// Format rule: AttributesToGet projects the response without changing the item.
get_item_case!(
    given_stored_item_when_attributes_to_get_selects_one_field_then_full_item_remains_readable,
    "local/attributes_to_get_preserves_item.json"
);

// Format rule: unknown request fields leave the successful response unchanged.
get_item_case!(
    given_unknown_top_level_field_is_ignored_when_get_item_runs_then_conformance_response_matches,
    "200/unknown_top_level_field_is_ignored.json"
);

// Validation rule: an empty key has DynamoDB's exact error shape.
get_item_case!(
    given_missing_key_shape_validation_when_get_item_runs_then_conformance_response_matches,
    "400/missing_key_shape_validation.json"
);

// Boundary rules: valid binary and numeric key limits remain readable.
get_item_case!(given_docs_limit_binary_partition_key_1_byte_valid_when_get_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_binary_partition_key_1_byte_valid.json");
get_item_case!(given_docs_limit_binary_partition_key_2048_bytes_valid_when_get_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_binary_partition_key_2048_bytes_valid.json");
get_item_case!(given_docs_limit_binary_sort_key_1024_bytes_valid_when_get_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_binary_sort_key_1024_bytes_valid.json");
get_item_case!(given_docs_limit_binary_sort_key_1_byte_valid_when_get_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_binary_sort_key_1_byte_valid.json");
get_item_case!(given_docs_limit_number_partition_key_38_digits_valid_when_get_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_number_partition_key_38_digits_valid.json");
get_item_case!(given_docs_limit_number_partition_key_positive_min_exponent_valid_when_get_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_number_partition_key_positive_min_exponent_valid.json");
get_item_case!(given_docs_limit_number_partition_key_zero_valid_when_get_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_number_partition_key_zero_valid.json");
get_item_case!(given_docs_limit_number_sort_key_38_digits_valid_when_get_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_number_sort_key_38_digits_valid.json");
get_item_case!(given_docs_limit_number_sort_key_positive_min_exponent_valid_when_get_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_number_sort_key_positive_min_exponent_valid.json");
get_item_case!(given_docs_limit_number_sort_key_zero_valid_when_get_item_runs_then_conformance_response_matches, "field-key/200/docs_limit_number_sort_key_zero_valid.json");

// Negative boundary rules: invalid binary and numeric keys return exact errors.
get_item_case!(given_docs_limit_binary_partition_key_2049_bytes_rejected_when_get_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_binary_partition_key_2049_bytes_rejected.json");
get_item_case!(given_docs_limit_binary_partition_key_empty_rejected_when_get_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_binary_partition_key_empty_rejected.json");
get_item_case!(given_docs_limit_binary_sort_key_1025_bytes_rejected_when_get_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_binary_sort_key_1025_bytes_rejected.json");
get_item_case!(given_docs_limit_binary_sort_key_empty_rejected_when_get_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_binary_sort_key_empty_rejected.json");
get_item_case!(given_docs_limit_number_partition_key_39_digits_rejected_when_get_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_number_partition_key_39_digits_rejected.json");
get_item_case!(given_docs_limit_number_partition_key_empty_rejected_when_get_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_number_partition_key_empty_rejected.json");
get_item_case!(given_docs_limit_number_partition_key_under_min_exponent_rejected_when_get_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_number_partition_key_under_min_exponent_rejected.json");
get_item_case!(given_docs_limit_number_sort_key_39_digits_rejected_when_get_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_number_sort_key_39_digits_rejected.json");
get_item_case!(given_docs_limit_number_sort_key_empty_rejected_when_get_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_number_sort_key_empty_rejected.json");
get_item_case!(given_docs_limit_number_sort_key_under_min_exponent_rejected_when_get_item_runs_then_conformance_response_matches, "field-key/400/docs_limit_number_sort_key_under_min_exponent_rejected.json");

// Business and format rules: projections return exactly the requested item
// shape.
get_item_case!(
    given_aliased_list_child_reserved_word_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/200/aliased_list_child_reserved_word.json"
);
get_item_case!(
    given_aliased_reserved_word_comment_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/200/aliased_reserved_word_comment.json"
);
get_item_case!(
    given_bulk_project_absent_nested_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/200/bulk_project_absent_nested.json"
);
get_item_case!(
    given_bulk_project_absent_only_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/200/bulk_project_absent_only.json"
);
get_item_case!(
    given_bulk_project_absent_with_present_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/200/bulk_project_absent_with_present.json"
);
get_item_case!(
    given_bulk_project_alias_deep_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/200/bulk_project_alias_deep.json"
);
get_item_case!(
    given_bulk_project_alias_list_child_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/200/bulk_project_alias_list_child.json"
);
get_item_case!(
    given_bulk_project_alias_nested_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/200/bulk_project_alias_nested.json"
);
get_item_case!(
    given_bulk_project_alias_note_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/200/bulk_project_alias_note.json"
);
get_item_case!(
    given_bulk_project_bool_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/200/bulk_project_bool.json"
);
get_item_case!(
    given_bulk_project_deep_leaf_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/200/bulk_project_deep_leaf.json"
);
get_item_case!(
    given_bulk_project_key_aliases_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/200/bulk_project_key_aliases.json"
);
get_item_case!(
    given_bulk_project_key_and_nested_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/200/bulk_project_key_and_nested.json"
);
get_item_case!(
    given_bulk_project_keys_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/200/bulk_project_keys.json"
);
get_item_case!(
    given_bulk_project_list_first_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/200/bulk_project_list_first.json"
);
get_item_case!(
    given_bulk_project_list_map_child_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/200/bulk_project_list_map_child.json"
);
get_item_case!(
    given_bulk_project_multiple_scalars_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/200/bulk_project_multiple_scalars.json"
);
get_item_case!(
    given_bulk_project_nested_child_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/200/bulk_project_nested_child.json"
);
get_item_case!(
    given_bulk_project_nested_number_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/200/bulk_project_nested_number.json"
);
get_item_case!(
    given_bulk_project_nested_with_key_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/200/bulk_project_nested_with_key.json"
);
get_item_case!(
    given_bulk_project_note_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/200/bulk_project_note.json"
);
get_item_case!(
    given_bulk_project_null_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/200/bulk_project_null.json"
);
get_item_case!(
    given_bulk_project_number_set_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/200/bulk_project_number_set.json"
);
get_item_case!(
    given_bulk_project_string_set_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/200/bulk_project_string_set.json"
);

// Negative validation rules: malformed projections return exact errors.
get_item_case!(
    given_bulk_project_list_and_index_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/400/bulk_project_list_and_index.json"
);
get_item_case!(
    given_bulk_project_nested_and_parent_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/400/bulk_project_nested_and_parent.json"
);
get_item_case!(
    given_bulk_project_repeated_path_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/400/bulk_project_repeated_path.json"
);
get_item_case!(
    given_empty_list_index_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/400/empty_list_index.json"
);
get_item_case!(
    given_list_reserved_word_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/400/list_reserved_word.json"
);
get_item_case!(
    given_negative_list_index_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/400/negative_list_index.json"
);
get_item_case!(
    given_nested_reserved_word_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/400/nested_reserved_word.json"
);
get_item_case!(
    given_reserved_word_comment_lowercase_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/400/reserved_word_comment_lowercase.json"
);
get_item_case!(
    given_reserved_word_count_when_get_item_runs_then_conformance_response_matches,
    "field-projection_expression/400/reserved_word_count.json"
);
