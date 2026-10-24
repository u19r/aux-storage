//! TransactWriteItems public responses from the authoritative DynamoDB fixture
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
    AttributeMap, GetItemRequest, KeyAttributes, PutItemRequest, TableName,
    TransactWriteItemsRequest,
};

use super::test_support::{ConformanceFixtures, TestStore, assert_conformance_body};

static FIXTURES: ConformanceFixtures = ConformanceFixtures::new();

fn fixture(source: &str) -> Value {
    FIXTURES.case(
        include_str!("../fixtures/transact_write_items_cases.jsonl"),
        source,
    )
}

fn assert_fixture_body(body: &Value, case: &Value, source: &str) {
    let ignores = case["ignores"].as_array();
    if ignores.is_none_or(Vec::is_empty) {
        assert_conformance_body(body, &case["expected"]["body"], source);
        return;
    }
    let unordered_type_set = source
        == "field-condition_expression/400/\
            txpathfunc_attribute_type_literal_type_top_level_validation.json";
    assert!(
        unordered_type_set
            || source == "field-transact_items/400/docs_limit_101_transaction_items_rejected.json",
        "unexpected authoritative message waiver: {source}"
    );
    assert_eq!(
        ignores.unwrap(),
        &["message"],
        "unsupported authoritative ignore"
    );
    let actual_message = body["message"].as_str().unwrap();
    assert!(
        !actual_message.is_empty(),
        "empty validation message: {source}"
    );
    if unordered_type_set {
        let prefix = "Invalid ConditionExpression: Incorrect operand type for operator or \
                      function; operator or function: attribute_type, operand type: {";
        let expected_message = case["expected"]["body"]["message"].as_str().unwrap();
        let actual_names = actual_message
            .strip_prefix(prefix)
            .unwrap()
            .strip_suffix('}')
            .unwrap();
        let expected_names = expected_message
            .strip_prefix(prefix)
            .unwrap()
            .strip_suffix('}')
            .unwrap();
        let names = |text: &str| {
            text.split(',')
                .map(|name| name.trim().to_owned())
                .collect::<BTreeSet<_>>()
        };
        assert_eq!(names(actual_names), names(expected_names));
    }
    let mut actual = body.clone();
    let mut expected = case["expected"]["body"].clone();
    actual.as_object_mut().unwrap().remove("message");
    expected.as_object_mut().unwrap().remove("message");
    assert_conformance_body(&actual, &expected, source);
}

fn table_kind(name: &str) -> Option<&'static str> {
    if name.starts_with("dynamo-conformance-basic-") {
        Some("basic_pk_sk")
    } else if name.starts_with("dynamo-conformance-number-") {
        Some("basic_number_pk_sk")
    } else if name.starts_with("dynamo-conformance-binary-") {
        Some("basic_binary_pk_sk")
    } else {
        None
    }
}

fn action(item: &Value) -> (&str, &Value) {
    let members = item.as_object().unwrap();
    assert_eq!(members.len(), 1, "one action per transaction member");
    members
        .iter()
        .next()
        .map(|(name, value)| (name.as_str(), value))
        .unwrap()
}

fn action_key(name: &str, value: &Value) -> KeyAttributes {
    let key = if name == "Put" {
        let item = &value["Item"];
        json!({"pk": item["pk"], "sk": item["sk"]})
    } else {
        value["Key"].clone()
    };
    serde_json::from_value(key).unwrap()
}

async fn read_item(store: &TestStore, table: TableName, key: KeyAttributes) -> Value {
    let response = store
        .operations
        .get_item(GetItemRequest::new(table, key))
        .await
        .unwrap();
    serde_json::to_value(response).unwrap()["Item"].clone()
}

fn expected_updated_item(source: &str, request: &Value, before: &Value) -> Value {
    let expression = request["UpdateExpression"].as_str().unwrap();
    if source.starts_with("field-transact_items/200/docs_limit_update_") {
        assert_eq!(expression, "SET touched = :value");
        assert_eq!(
            request["ExpressionAttributeValues"][":value"],
            json!({"S": "yes"})
        );
        let mut item: AttributeMap = serde_json::from_value(request["Key"].clone()).unwrap();
        item.insert(
            "touched",
            serde_json::from_value(json!({"S": "yes"})).unwrap(),
        );
        item.normalize_numbers_for_write();
        return serde_json::to_value(item).unwrap();
    }

    let mut item = if before.is_null() {
        request["Key"].clone()
    } else {
        before.clone()
    };
    match expression {
        "SET n = n + :one" => {
            let expected =
                if source.ends_with("semantic_mixed_operations_condition_functions_success.json") {
                    "3"
                } else if source.ends_with(
                    "semantic_update_condition_true_with_complex_functions_allows_update.json",
                ) {
                    "8"
                } else {
                    "5"
                };
            item["n"] = json!({"N": expected});
        }
        "SET touched = :yes" => item["touched"] = json!({"S": "yes"}),
        "ADD n :inc" => {
            let sort_key = request["Key"]["sk"]["S"].as_str().unwrap();
            item["n"] = json!({"N": if sort_key == "sk-update-num" { "2" } else { "5" }});
        }
        "ADD n :inc SET snapx = n" => {
            item["n"] = json!({"N": "5"});
            item["snapx"] = json!({"N": "3"});
        }
        "DELETE tags :gone REMOVE #old" => {
            item["tags"] = json!({"SS": ["green"]});
            item.as_object_mut().unwrap().remove("old");
        }
        "ADD tags :add" => item["tags"] = json!({"SS": ["blue", "green", "red"]}),
        "DELETE bins :gone" => item["bins"] = json!({"BS": ["Yg=="]}),
        "SET sumx = n + :inc" => item["sumx"] = json!({"N": "10"}),
        "SET #list[0].#comment = :v" => {
            item["list"]["L"][0]["M"]["COMMENT"] = json!({"S": "changed"});
        }
        _ if source.starts_with("field-update_expression/200/") => {
            let (name, token) = expression
                .strip_prefix("SET ")
                .unwrap()
                .split_once(" = ")
                .unwrap();
            let name = request["ExpressionAttributeNames"]
                .get(name)
                .and_then(Value::as_str)
                .unwrap_or(name);
            item[name] = request["ExpressionAttributeValues"][token].clone();
        }
        _ => panic!("no independent readback oracle for {source}: {expression}"),
    }
    item
}

async fn assert_transact_write_case(store: &TestStore, case: &Value) {
    assert_eq!(case["operation"], "TransactWriteItems", "fixture operation");
    let source = case["source"].as_str().unwrap();
    let invalid_key_boundary = source.starts_with("field-transact_items/400/docs_limit_");
    let invalid_member_shape = case["request"]["TransactItems"]
        .as_array()
        .unwrap()
        .iter()
        .any(|member| member.as_object().is_none_or(|actions| actions.len() != 1));
    let items = case["request"]["TransactItems"].as_array().unwrap();
    let preconditions = case["preconditions"].as_array().unwrap();
    let declared_tables = case["tables"].as_array().unwrap();
    let mut table_names = BTreeSet::new();
    for member in items {
        for value in member.as_object().unwrap().values() {
            table_names.insert(value["TableName"].as_str().unwrap());
        }
    }
    for precondition in preconditions {
        assert_eq!(
            precondition["operation"], "PutItem",
            "precondition operation"
        );
        table_names.insert(precondition["request"]["TableName"].as_str().unwrap());
    }
    for name in table_names {
        if let Some(kind) = table_kind(name) {
            assert!(
                declared_tables.iter().any(|table| table == kind),
                "undeclared fixture table kind"
            );
            store
                .create_conformance_table(kind, TableName::new(name))
                .await;
        }
    }
    for precondition in preconditions {
        let request = PutItemRequest::try_from(precondition["request"].clone()).unwrap();
        store.operations.put_item(request).await.unwrap();
    }

    // The readback also catches a partial transaction when the public response
    // is correct.
    let mut before = Vec::with_capacity(items.len());
    if !invalid_key_boundary && !invalid_member_shape {
        for member in items {
            let (name, value) = action(member);
            let invalid_schema_key = source.starts_with("400/parity_")
                && source.contains("key_schema_mismatch")
                && value["Key"]["pk"].get("N").is_some();
            if invalid_schema_key {
                before.push(Value::Null);
            } else {
                let table = TableName::new(value["TableName"].as_str().unwrap());
                let key = action_key(name, value);
                before.push(read_item(store, table, key).await);
            }
        }
    }

    let parsed =
        observe(async { TransactWriteItemsRequest::try_from(case["request"].clone()) }).await;
    parsed.assert_no_warning_or_error();
    parsed.assert_database_calls_exact(&[]);
    let (status, body) = match parsed.result {
        Ok(request) => {
            let observed = observe(store.operations.transact_write_items(request)).await;
            observed.assert_no_warning_or_error();
            if case["source"] == "400/duplicate_put_same_key.json" {
                // Shape validation rejects duplicate targets before opening a
                // native attempt.
                observed.assert_database_calls_exact(&[]);
            }
            match observed.result {
                Ok(response) => (200, serde_json::to_value(response).unwrap()),
                Err(error) => {
                    let (status, axum::Json(body)): (_, axum::Json<http_error::ErrorResponse>) =
                        http_error::HttpApiError::from(error).into();
                    (status.as_u16(), serde_json::to_value(body).unwrap())
                }
            }
        }
        Err(message) => (
            400,
            json!({
                "__type": "com.amazon.coral.validate#ValidationException",
                "message": message,
            }),
        ),
    };
    let expected_status = case["expected"]["status"].as_u64().unwrap() as u16;
    assert_eq!(
        status, expected_status,
        "{} returned {body}",
        case["source"]
    );
    assert_fixture_body(&body, case, source);

    if invalid_key_boundary || invalid_member_shape {
        assert_eq!(
            status, 400,
            "invalid key, cardinality, or member shape must fail"
        );
        for precondition in preconditions {
            let item = &precondition["request"]["Item"];
            let key = json!({"pk": item["pk"], "sk": item["sk"]});
            let table = TableName::new(precondition["request"]["TableName"].as_str().unwrap());
            let after = read_item(store, table, serde_json::from_value(key).unwrap()).await;
            let mut expected: AttributeMap = serde_json::from_value(item.clone()).unwrap();
            expected.normalize_numbers_for_write();
            assert_conformance_body(&after, &serde_json::to_value(expected).unwrap(), source);
        }
        return;
    }

    for (index, member) in items.iter().enumerate() {
        let (name, value) = action(member);
        if source.starts_with("400/parity_")
            && source.contains("key_schema_mismatch")
            && value["Key"]["pk"].get("N").is_some()
        {
            assert_eq!(
                status, 400,
                "key schema mismatch must reject the transaction"
            );
            continue;
        }
        let table = TableName::new(value["TableName"].as_str().unwrap());
        let after = read_item(store, table, action_key(name, value)).await;
        let expected = if status == 400 || name == "ConditionCheck" {
            before[index].clone()
        } else if name == "Delete" {
            Value::Null
        } else if name == "Put" {
            let mut item: AttributeMap = serde_json::from_value(value["Item"].clone()).unwrap();
            item.normalize_numbers_for_write();
            serde_json::to_value(item).unwrap()
        } else if name == "Update" {
            expected_updated_item(source, value, &before[index])
        } else {
            panic!("unsupported transaction action {name}");
        };
        assert_conformance_body(&after, &expected, source);
    }
}

macro_rules! transact_write_case {
    ($name:ident, $source:literal) => {
        #[tokio::test]
        async fn $name() {
            let case = fixture($source);
            let store = TestStore::new().await;
            let outcome = AssertUnwindSafe(assert_transact_write_case(&store, &case))
                .catch_unwind()
                .await;
            store.finish().await;
            if let Err(panic) = outcome {
                resume_unwind(panic);
            }
        }
    };
}

// Business rule: a condition on a missing item succeeds without creating it.
transact_write_case!(
    given_missing_item_when_condition_check_runs_then_response_and_state_match_aws,
    "200/condition_check_attribute_not_exists_on_missing_item_succeeds.json"
);

// Business rule: deleting a missing item is idempotent.
transact_write_case!(
    given_missing_item_when_transaction_deletes_it_then_response_and_state_match_aws,
    "200/delete_missing_item_succeeds.json"
);

// Validation rule: two actions on the same item are rejected without a write.
transact_write_case!(
    given_duplicate_put_keys_when_transaction_runs_then_validation_and_state_match_aws,
    "400/duplicate_put_same_key.json"
);

// Negative rule: a later failed condition rolls back an earlier update.
transact_write_case!(given_later_failed_condition_when_transaction_updates_first_then_cancellation_and_rollback_match_aws, "400/parity_first_update_succeeds_second_condition_fails.json");

// Negative and format rule: a failed condition returns the exact old item and
// cancellation reason.
transact_write_case!(
    given_failed_put_condition_when_all_old_is_requested_then_cancellation_and_state_match_aws,
    "400/put_condition_failure_returns_all_old.json"
);

// Business rule: four action kinds apply atomically through one transaction.
transact_write_case!(
    given_mixed_actions_when_transaction_runs_then_response_and_all_items_match_aws,
    "field-transact_items/200/semantic_mixed_condition_check_put_delete_update_success.json"
);

// Boundary rule: the documented maximum of 100 transaction members succeeds.
transact_write_case!(
    given_one_hundred_deletes_when_transaction_runs_then_response_and_state_match_aws,
    "field-transact_items/200/docs_limit_100_transaction_items_valid.json"
);

// Boundary rule: a 38-digit numeric partition key is accepted and persisted.
transact_write_case!(
    given_38_digit_number_key_when_transaction_puts_item_then_response_and_state_match_aws,
    "field-transact_items/200/docs_limit_put_number_partition_key_38_digits_valid.json"
);

// Boundary, validation, and business rules: successful condition checks and key
// limits.
transact_write_case!(given_docs_limit_condition_expression_4096_bytes_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-condition_expression/200/docs_limit_condition_expression_4096_bytes_valid.json");
transact_write_case!(given_docs_limit_expression_attribute_name_token_255_bytes_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-condition_expression/200/docs_limit_expression_attribute_name_token_255_bytes_valid.json");
transact_write_case!(given_docs_limit_expression_attribute_value_token_255_bytes_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-condition_expression/200/docs_limit_expression_attribute_value_token_255_bytes_valid.json");
transact_write_case!(given_docs_limit_in_100_operands_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-condition_expression/200/docs_limit_in_100_operands_valid.json");
transact_write_case!(given_parser_condition_newlines_tabs_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-condition_expression/200/parser_condition_newlines_tabs_valid.json");
transact_write_case!(given_single_parenthesized_condition_check_when_transact_write_runs_then_response_and_state_match_aws, "field-condition_expression/200/single_parenthesized_condition_check.json");
transact_write_case!(given_docs_limit_delete_binary_partition_key_1_byte_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-transact_items/200/docs_limit_delete_binary_partition_key_1_byte_valid.json");
transact_write_case!(given_docs_limit_delete_binary_partition_key_2048_bytes_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-transact_items/200/docs_limit_delete_binary_partition_key_2048_bytes_valid.json");
transact_write_case!(given_docs_limit_delete_binary_sort_key_1024_bytes_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-transact_items/200/docs_limit_delete_binary_sort_key_1024_bytes_valid.json");
transact_write_case!(given_docs_limit_delete_number_partition_key_38_digits_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-transact_items/200/docs_limit_delete_number_partition_key_38_digits_valid.json");
transact_write_case!(given_docs_limit_delete_number_partition_key_positive_min_exponent_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-transact_items/200/docs_limit_delete_number_partition_key_positive_min_exponent_valid.json");
transact_write_case!(given_docs_limit_delete_number_partition_key_zero_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-transact_items/200/docs_limit_delete_number_partition_key_zero_valid.json");
transact_write_case!(given_docs_limit_delete_number_sort_key_38_digits_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-transact_items/200/docs_limit_delete_number_sort_key_38_digits_valid.json");
transact_write_case!(given_docs_limit_delete_number_sort_key_positive_min_exponent_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-transact_items/200/docs_limit_delete_number_sort_key_positive_min_exponent_valid.json");
transact_write_case!(given_docs_limit_delete_number_sort_key_zero_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-transact_items/200/docs_limit_delete_number_sort_key_zero_valid.json");
transact_write_case!(given_docs_limit_delete_string_partition_key_2048_bytes_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-transact_items/200/docs_limit_delete_string_partition_key_2048_bytes_valid.json");
transact_write_case!(given_docs_limit_delete_string_sort_key_1024_bytes_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-transact_items/200/docs_limit_delete_string_sort_key_1024_bytes_valid.json");
transact_write_case!(given_docs_limit_put_binary_partition_key_1_byte_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-transact_items/200/docs_limit_put_binary_partition_key_1_byte_valid.json");
transact_write_case!(given_docs_limit_put_binary_partition_key_2048_bytes_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-transact_items/200/docs_limit_put_binary_partition_key_2048_bytes_valid.json");
transact_write_case!(given_docs_limit_put_binary_sort_key_1024_bytes_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-transact_items/200/docs_limit_put_binary_sort_key_1024_bytes_valid.json");
transact_write_case!(given_docs_limit_put_number_partition_key_positive_min_exponent_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-transact_items/200/docs_limit_put_number_partition_key_positive_min_exponent_valid.json");
transact_write_case!(given_docs_limit_put_number_partition_key_zero_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-transact_items/200/docs_limit_put_number_partition_key_zero_valid.json");
transact_write_case!(given_docs_limit_put_number_sort_key_38_digits_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-transact_items/200/docs_limit_put_number_sort_key_38_digits_valid.json");
transact_write_case!(given_docs_limit_put_number_sort_key_positive_min_exponent_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-transact_items/200/docs_limit_put_number_sort_key_positive_min_exponent_valid.json");
transact_write_case!(given_docs_limit_put_number_sort_key_zero_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-transact_items/200/docs_limit_put_number_sort_key_zero_valid.json");
transact_write_case!(given_docs_limit_put_string_partition_key_2048_bytes_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-transact_items/200/docs_limit_put_string_partition_key_2048_bytes_valid.json");
transact_write_case!(given_docs_limit_put_string_sort_key_1024_bytes_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-transact_items/200/docs_limit_put_string_sort_key_1024_bytes_valid.json");

// Negative and validation rules: expression failures preserve exact AWS errors
// and atomic state.
transact_write_case!(given_condition_docs_limit_condition_expression_4097_bytes_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/docs_limit_condition_expression_4097_bytes_rejected.json");
transact_write_case!(given_condition_docs_limit_expression_attribute_name_token_256_bytes_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/docs_limit_expression_attribute_name_token_256_bytes_rejected.json");
transact_write_case!(given_condition_docs_limit_expression_attribute_value_token_256_bytes_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/docs_limit_expression_attribute_value_token_256_bytes_rejected.json");
transact_write_case!(given_condition_docs_limit_expression_substitution_values_over_2mb_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/docs_limit_expression_substitution_values_over_2mb_rejected.json");
transact_write_case!(given_condition_docs_limit_in_101_operands_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/docs_limit_in_101_operands_rejected.json");
transact_write_case!(given_condition_if_not_exists_function_not_allowed_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/if_not_exists_function_not_allowed.json");
transact_write_case!(given_condition_invalid_expression_attribute_name_key_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/invalid_expression_attribute_name_key.json");
transact_write_case!(given_condition_invalid_expression_attribute_value_key_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/invalid_expression_attribute_value_key.json");
transact_write_case!(
    given_condition_missing_value_when_transact_write_runs_then_error_and_state_match_aws,
    "field-condition_expression/400/missing_value.json"
);
transact_write_case!(given_condition_parser_condition_double_parenthesized_comparison_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/parser_condition_double_parenthesized_comparison_rejected.json");
transact_write_case!(given_condition_parser_condition_unused_value_precedes_cancellation_failure_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/parser_condition_unused_value_precedes_cancellation_failure.json");
transact_write_case!(given_condition_redundant_parenthesized_condition_check_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/redundant_parenthesized_condition_check.json");
transact_write_case!(
    given_condition_reserved_word_when_transact_write_runs_then_error_and_state_match_aws,
    "field-condition_expression/400/reserved_word.json"
);
transact_write_case!(given_condition_semantic_case_sensitive_function_name_top_level_validation_before_cancellation_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/semantic_case_sensitive_function_name_top_level_validation_before_cancellation.json");
transact_write_case!(given_condition_semantic_condition_check_invalid_function_precedes_later_invalid_update_expression_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/semantic_condition_check_invalid_function_precedes_later_invalid_update_expression.json");
transact_write_case!(given_condition_semantic_condition_unused_expression_name_top_level_validation_precedes_cancellations_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/semantic_condition_unused_expression_name_top_level_validation_precedes_cancellations.json");
transact_write_case!(given_condition_semantic_delete_condition_true_then_later_update_validation_order_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/semantic_delete_condition_true_then_later_update_validation_order.json");
transact_write_case!(given_condition_semantic_delete_condition_validation_precedes_put_condition_validation_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/semantic_delete_condition_validation_precedes_put_condition_validation.json");
transact_write_case!(given_condition_semantic_put_condition_invalid_type_name_top_level_validation_precedes_update_validation_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/semantic_put_condition_invalid_type_name_top_level_validation_precedes_update_validation.json");
transact_write_case!(given_condition_semantic_put_condition_true_then_later_condition_failure_order_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/semantic_put_condition_true_then_later_condition_failure_order.json");
transact_write_case!(given_condition_semantic_put_condition_validation_precedes_delete_condition_validation_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/semantic_put_condition_validation_precedes_delete_condition_validation.json");
transact_write_case!(given_condition_semantic_update_condition_false_suppresses_update_runtime_validation_same_item_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/semantic_update_condition_false_suppresses_update_runtime_validation_same_item.json");
transact_write_case!(given_condition_semantic_update_condition_true_exposes_update_runtime_validation_same_item_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/semantic_update_condition_true_exposes_update_runtime_validation_same_item.json");
transact_write_case!(given_condition_txpathfunc_attribute_type_invalid_type_name_top_level_validation_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/txpathfunc_attribute_type_invalid_type_name_top_level_validation.json");
transact_write_case!(given_condition_txpathfunc_begins_with_wrong_second_operand_top_level_validation_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/txpathfunc_begins_with_wrong_second_operand_top_level_validation.json");
transact_write_case!(given_condition_txpathfunc_contains_same_operand_top_level_validation_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/txpathfunc_contains_same_operand_top_level_validation.json");
transact_write_case!(given_condition_txpathfunc_list_alpha_index_top_level_validation_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/txpathfunc_list_alpha_index_top_level_validation.json");
transact_write_case!(given_condition_txpathfunc_list_negative_index_top_level_validation_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/txpathfunc_list_negative_index_top_level_validation.json");
transact_write_case!(given_condition_txpathfunc_missing_alias_in_nested_path_top_level_validation_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/txpathfunc_missing_alias_in_nested_path_top_level_validation.json");
transact_write_case!(given_condition_txpathfunc_unaliased_reserved_nested_path_top_level_validation_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/txpathfunc_unaliased_reserved_nested_path_top_level_validation.json");
transact_write_case!(given_condition_txpathfunc_unused_alias_with_reserved_path_precedes_cancellation_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/txpathfunc_unused_alias_with_reserved_path_precedes_cancellation.json");
transact_write_case!(given_condition_txprecedence_condition_false_later_condition_syntax_validation_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/txprecedence_condition_false_later_condition_syntax_validation.json");
transact_write_case!(given_condition_txprecedence_condition_false_later_missing_value_validation_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/txprecedence_condition_false_later_missing_value_validation.json");
transact_write_case!(given_condition_txprecedence_condition_false_same_item_missing_name_validation_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/txprecedence_condition_false_same_item_missing_name_validation.json");
transact_write_case!(given_condition_txprecedence_condition_false_same_item_missing_value_validation_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/txprecedence_condition_false_same_item_missing_value_validation.json");
transact_write_case!(given_condition_txprecedence_condition_false_same_item_unused_name_validation_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/txprecedence_condition_false_same_item_unused_name_validation.json");
transact_write_case!(given_condition_txprecedence_condition_false_same_item_unused_value_validation_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/txprecedence_condition_false_same_item_unused_value_validation.json");

// Negative and format rules: cancellation reasons retain member order and
// old-item shape.
transact_write_case!(given_parser_runtime_condition_failure_with_valid_aliases_has_cancellation_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/parser_runtime_condition_failure_with_valid_aliases_has_cancellation.json");
transact_write_case!(given_semantic_condition_check_failure_without_all_old_omits_item_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/semantic_condition_check_failure_without_all_old_omits_item.json");
transact_write_case!(given_semantic_condition_check_missing_item_all_old_has_no_item_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/semantic_condition_check_missing_item_all_old_has_no_item.json");
transact_write_case!(given_semantic_condition_failure_item_contains_complex_document_shapes_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/semantic_condition_failure_item_contains_complex_document_shapes.json");
transact_write_case!(given_semantic_delete_condition_failure_without_all_old_omits_item_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/semantic_delete_condition_failure_without_all_old_omits_item.json");
transact_write_case!(given_semantic_delete_missing_item_condition_failure_has_no_item_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/semantic_delete_missing_item_condition_failure_has_no_item.json");
transact_write_case!(given_semantic_delete_runtime_condition_check_put_order_with_middle_none_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/semantic_delete_runtime_condition_check_put_order_with_middle_none.json");
transact_write_case!(given_semantic_multiple_update_validation_reasons_preserve_order_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/semantic_multiple_update_validation_reasons_preserve_order.json");
transact_write_case!(given_semantic_none_between_condition_failures_reason_order_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/semantic_none_between_condition_failures_reason_order.json");
transact_write_case!(given_semantic_put_condition_failure_without_all_old_omits_item_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/semantic_put_condition_failure_without_all_old_omits_item.json");
transact_write_case!(given_semantic_put_delete_update_condition_failures_all_old_order_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/semantic_put_delete_update_condition_failures_all_old_order.json");
transact_write_case!(given_semantic_put_delete_update_condition_failures_none_all_old_mixed_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/semantic_put_delete_update_condition_failures_none_all_old_mixed.json");
transact_write_case!(given_semantic_put_update_delete_condition_check_reason_order_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/semantic_put_update_delete_condition_check_reason_order.json");
transact_write_case!(given_semantic_successful_put_delete_update_are_none_before_later_condition_failure_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/semantic_successful_put_delete_update_are_none_before_later_condition_failure.json");
transact_write_case!(given_semantic_update_condition_failed_then_update_runtime_validation_order_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/semantic_update_condition_failed_then_update_runtime_validation_order.json");
transact_write_case!(given_semantic_update_condition_failure_without_all_old_omits_item_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/semantic_update_condition_failure_without_all_old_omits_item.json");
transact_write_case!(given_semantic_update_missing_addend_validation_order_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/semantic_update_missing_addend_validation_order.json");
transact_write_case!(given_semantic_update_nested_scalar_parent_validation_order_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/semantic_update_nested_scalar_parent_validation_order.json");
transact_write_case!(given_semantic_update_runtime_validation_then_delete_condition_failed_order_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/semantic_update_runtime_validation_then_delete_condition_failed_order.json");
transact_write_case!(given_semantic_validation_condition_validation_interleaved_order_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/semantic_validation_condition_validation_interleaved_order.json");
transact_write_case!(given_txarith_add_set_to_scalar_validation_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/txarith_add_set_to_scalar_validation.json");
transact_write_case!(given_txarith_delete_wrong_set_then_condition_failure_order_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/txarith_delete_wrong_set_then_condition_failure_order.json");
transact_write_case!(given_txarith_runtime_add_number_to_string_then_set_type_order_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/txarith_runtime_add_number_to_string_then_set_type_order.json");
transact_write_case!(given_txarith_set_number_plus_string_path_validation_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/txarith_set_number_plus_string_path_validation.json");
transact_write_case!(given_txpathfunc_begins_with_wrong_first_operand_is_condition_failure_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/txpathfunc_begins_with_wrong_first_operand_is_condition_failure.json");
transact_write_case!(given_txpathfunc_contains_list_substring_is_condition_failure_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/txpathfunc_contains_list_substring_is_condition_failure.json");
transact_write_case!(given_txpathfunc_contains_set_substring_is_condition_failure_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/txpathfunc_contains_set_substring_is_condition_failure.json");
transact_write_case!(given_txpathfunc_list_index_out_of_range_is_condition_failure_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/txpathfunc_list_index_out_of_range_is_condition_failure.json");
transact_write_case!(given_txpathfunc_missing_nested_path_is_condition_failure_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/txpathfunc_missing_nested_path_is_condition_failure.json");
transact_write_case!(given_txpathfunc_put_delete_update_condition_mixed_function_failures_order_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/txpathfunc_put_delete_update_condition_mixed_function_failures_order.json");
transact_write_case!(given_txprecedence_condition_failure_then_later_runtime_arithmetic_validation_then_none_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/txprecedence_condition_failure_then_later_runtime_arithmetic_validation_then_none.json");
transact_write_case!(given_txprecedence_condition_failure_then_later_runtime_list_path_validation_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/txprecedence_condition_failure_then_later_runtime_list_path_validation.json");
transact_write_case!(given_txprecedence_update_condition_false_suppresses_runtime_type_validation_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/txprecedence_update_condition_false_suppresses_runtime_type_validation.json");
transact_write_case!(given_txprecedence_update_condition_true_exposes_runtime_type_validation_when_transact_write_runs_then_cancellation_and_state_match_aws, "field-cancellation_reasons/400/txprecedence_update_condition_true_exposes_runtime_type_validation.json");

// Validation and negative rules: update expression rejection precedes all
// writes.
transact_write_case!(given_update_condition_syntax_is_reported_when_transact_write_runs_then_error_and_state_match_aws, "field-update_expression/400/condition_syntax_is_reported.json");
transact_write_case!(given_update_contains_function_not_allowed_when_transact_write_runs_then_error_and_state_match_aws, "field-update_expression/400/contains_function_not_allowed.json");
transact_write_case!(given_update_docs_limit_expression_attribute_name_token_256_bytes_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-update_expression/400/docs_limit_expression_attribute_name_token_256_bytes_rejected.json");
transact_write_case!(given_update_docs_limit_expression_attribute_value_token_256_bytes_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-update_expression/400/docs_limit_expression_attribute_value_token_256_bytes_rejected.json");
transact_write_case!(given_update_docs_limit_update_expression_4097_bytes_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-update_expression/400/docs_limit_update_expression_4097_bytes_rejected.json");
transact_write_case!(
    given_update_reserved_word_when_transact_write_runs_then_error_and_state_match_aws,
    "field-update_expression/400/reserved_word.json"
);
transact_write_case!(given_update_semantic_invalid_update_expression_precedes_later_condition_check_validation_when_transact_write_runs_then_error_and_state_match_aws, "field-update_expression/400/semantic_invalid_update_expression_precedes_later_condition_check_validation.json");
transact_write_case!(given_update_semantic_update_reserved_word_top_level_validation_precedes_cancellations_when_transact_write_runs_then_error_and_state_match_aws, "field-update_expression/400/semantic_update_reserved_word_top_level_validation_precedes_cancellations.json");
transact_write_case!(given_update_semantic_update_unused_expression_value_top_level_validation_precedes_cancellations_when_transact_write_runs_then_error_and_state_match_aws, "field-update_expression/400/semantic_update_unused_expression_value_top_level_validation_precedes_cancellations.json");
transact_write_case!(given_update_txarith_duplicate_number_set_operand_top_level_validation_when_transact_write_runs_then_error_and_state_match_aws, "field-update_expression/400/txarith_duplicate_number_set_operand_top_level_validation.json");
transact_write_case!(given_update_txarith_duplicate_string_set_operand_top_level_validation_when_transact_write_runs_then_error_and_state_match_aws, "field-update_expression/400/txarith_duplicate_string_set_operand_top_level_validation.json");
transact_write_case!(given_update_txarith_empty_binary_set_top_level_validation_when_transact_write_runs_then_error_and_state_match_aws, "field-update_expression/400/txarith_empty_binary_set_top_level_validation.json");
transact_write_case!(given_update_txarith_empty_number_set_top_level_validation_when_transact_write_runs_then_error_and_state_match_aws, "field-update_expression/400/txarith_empty_number_set_top_level_validation.json");
transact_write_case!(given_update_txarith_empty_string_set_top_level_validation_when_transact_write_runs_then_error_and_state_match_aws, "field-update_expression/400/txarith_empty_string_set_top_level_validation.json");
transact_write_case!(given_update_txprecedence_condition_false_later_update_missing_name_validation_when_transact_write_runs_then_error_and_state_match_aws, "field-update_expression/400/txprecedence_condition_false_later_update_missing_name_validation.json");
transact_write_case!(given_update_txprecedence_condition_false_later_update_missing_value_validation_when_transact_write_runs_then_error_and_state_match_aws, "field-update_expression/400/txprecedence_condition_false_later_update_missing_value_validation.json");
transact_write_case!(given_update_txprecedence_condition_false_later_update_syntax_validation_when_transact_write_runs_then_error_and_state_match_aws, "field-update_expression/400/txprecedence_condition_false_later_update_syntax_validation.json");
transact_write_case!(given_update_txprecedence_update_condition_false_same_item_update_missing_value_validation_when_transact_write_runs_then_error_and_state_match_aws, "field-update_expression/400/txprecedence_update_condition_false_same_item_update_missing_value_validation.json");
transact_write_case!(given_update_txprecedence_update_condition_false_same_item_update_syntax_validation_when_transact_write_runs_then_error_and_state_match_aws, "field-update_expression/400/txprecedence_update_condition_false_same_item_update_syntax_validation.json");
transact_write_case!(given_update_update_error_precedes_condition_error_when_transact_write_runs_then_error_and_state_match_aws, "field-update_expression/400/update_error_precedes_condition_error.json");

// Boundary and validation rules: transaction size, key shapes, and duplicate
// actions.
transact_write_case!(given_docs_limit_delete_binary_partition_key_2049_bytes_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_delete_binary_partition_key_2049_bytes_rejected.json");
transact_write_case!(given_docs_limit_delete_binary_partition_key_empty_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_delete_binary_partition_key_empty_rejected.json");
transact_write_case!(given_docs_limit_delete_binary_sort_key_1025_bytes_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_delete_binary_sort_key_1025_bytes_rejected.json");
transact_write_case!(given_docs_limit_delete_binary_sort_key_empty_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_delete_binary_sort_key_empty_rejected.json");
transact_write_case!(given_docs_limit_delete_number_partition_key_39_digits_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_delete_number_partition_key_39_digits_rejected.json");
transact_write_case!(given_docs_limit_delete_number_partition_key_empty_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_delete_number_partition_key_empty_rejected.json");
transact_write_case!(given_docs_limit_delete_number_partition_key_under_min_exponent_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_delete_number_partition_key_under_min_exponent_rejected.json");
transact_write_case!(given_docs_limit_delete_number_sort_key_39_digits_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_delete_number_sort_key_39_digits_rejected.json");
transact_write_case!(given_docs_limit_delete_number_sort_key_empty_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_delete_number_sort_key_empty_rejected.json");
transact_write_case!(given_docs_limit_delete_number_sort_key_under_min_exponent_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_delete_number_sort_key_under_min_exponent_rejected.json");
transact_write_case!(given_docs_limit_delete_string_partition_key_2049_bytes_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_delete_string_partition_key_2049_bytes_rejected.json");
transact_write_case!(given_docs_limit_delete_string_partition_key_empty_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_delete_string_partition_key_empty_rejected.json");
transact_write_case!(given_docs_limit_delete_string_sort_key_1025_bytes_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_delete_string_sort_key_1025_bytes_rejected.json");
transact_write_case!(given_docs_limit_delete_string_sort_key_empty_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_delete_string_sort_key_empty_rejected.json");
transact_write_case!(given_docs_limit_put_binary_partition_key_2049_bytes_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_put_binary_partition_key_2049_bytes_rejected.json");
transact_write_case!(given_docs_limit_put_binary_partition_key_empty_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_put_binary_partition_key_empty_rejected.json");
transact_write_case!(given_docs_limit_put_binary_sort_key_1025_bytes_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_put_binary_sort_key_1025_bytes_rejected.json");
transact_write_case!(given_docs_limit_put_binary_sort_key_empty_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_put_binary_sort_key_empty_rejected.json");
transact_write_case!(given_docs_limit_put_number_partition_key_39_digits_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_put_number_partition_key_39_digits_rejected.json");
transact_write_case!(given_docs_limit_put_number_partition_key_empty_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_put_number_partition_key_empty_rejected.json");
transact_write_case!(given_docs_limit_put_number_partition_key_under_min_exponent_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_put_number_partition_key_under_min_exponent_rejected.json");
transact_write_case!(given_docs_limit_put_number_sort_key_39_digits_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_put_number_sort_key_39_digits_rejected.json");
transact_write_case!(given_docs_limit_put_number_sort_key_empty_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_put_number_sort_key_empty_rejected.json");
transact_write_case!(given_docs_limit_put_number_sort_key_under_min_exponent_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_put_number_sort_key_under_min_exponent_rejected.json");
transact_write_case!(given_docs_limit_put_string_partition_key_2049_bytes_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_put_string_partition_key_2049_bytes_rejected.json");
transact_write_case!(given_docs_limit_put_string_partition_key_empty_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_put_string_partition_key_empty_rejected.json");
transact_write_case!(given_docs_limit_put_string_sort_key_1025_bytes_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_put_string_sort_key_1025_bytes_rejected.json");
transact_write_case!(given_docs_limit_put_string_sort_key_empty_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_put_string_sort_key_empty_rejected.json");
transact_write_case!(given_docs_limit_update_binary_partition_key_2049_bytes_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_update_binary_partition_key_2049_bytes_rejected.json");
transact_write_case!(given_docs_limit_update_binary_partition_key_empty_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_update_binary_partition_key_empty_rejected.json");
transact_write_case!(given_docs_limit_update_binary_sort_key_1025_bytes_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_update_binary_sort_key_1025_bytes_rejected.json");
transact_write_case!(given_docs_limit_update_binary_sort_key_empty_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_update_binary_sort_key_empty_rejected.json");
transact_write_case!(given_docs_limit_update_number_partition_key_39_digits_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_update_number_partition_key_39_digits_rejected.json");
transact_write_case!(given_docs_limit_update_number_partition_key_empty_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_update_number_partition_key_empty_rejected.json");
transact_write_case!(given_docs_limit_update_number_partition_key_under_min_exponent_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_update_number_partition_key_under_min_exponent_rejected.json");
transact_write_case!(given_docs_limit_update_number_sort_key_39_digits_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_update_number_sort_key_39_digits_rejected.json");
transact_write_case!(given_docs_limit_update_number_sort_key_empty_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_update_number_sort_key_empty_rejected.json");
transact_write_case!(given_docs_limit_update_number_sort_key_under_min_exponent_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_update_number_sort_key_under_min_exponent_rejected.json");
transact_write_case!(given_docs_limit_update_string_partition_key_2049_bytes_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_update_string_partition_key_2049_bytes_rejected.json");
transact_write_case!(given_docs_limit_update_string_partition_key_empty_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_update_string_partition_key_empty_rejected.json");
transact_write_case!(given_docs_limit_update_string_sort_key_1025_bytes_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_update_string_sort_key_1025_bytes_rejected.json");
transact_write_case!(given_docs_limit_update_string_sort_key_empty_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_update_string_sort_key_empty_rejected.json");
transact_write_case!(given_txrule_condition_check_then_update_same_item_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/txrule_condition_check_then_update_same_item_rejected.json");
transact_write_case!(given_txrule_delete_then_condition_check_same_item_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/txrule_delete_then_condition_check_same_item_rejected.json");
transact_write_case!(given_txrule_delete_then_update_same_item_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/txrule_delete_then_update_same_item_rejected.json");
transact_write_case!(given_txrule_put_then_delete_same_item_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/txrule_put_then_delete_same_item_rejected.json");
transact_write_case!(given_txrule_put_then_update_same_item_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/txrule_put_then_update_same_item_rejected.json");
transact_write_case!(given_txrule_update_then_condition_check_same_item_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/txrule_update_then_condition_check_same_item_rejected.json");
transact_write_case!(given_txrule_update_then_delete_same_item_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/txrule_update_then_delete_same_item_rejected.json");
transact_write_case!(given_txrule_update_then_put_same_item_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/txrule_update_then_put_same_item_rejected.json");
transact_write_case!(given_txrule_update_then_update_same_item_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/txrule_update_then_update_same_item_rejected.json");

// Negative rules: runtime cancellation preserves all items and exact ordered
// reasons.
transact_write_case!(given_parity_condition_check_all_old_includes_item_omits_metrics_when_transact_write_runs_then_cancellation_and_state_match_aws, "400/parity_condition_check_all_old_includes_item_omits_metrics.json");
transact_write_case!(given_parity_condition_check_false_when_transact_write_runs_then_cancellation_and_state_match_aws, "400/parity_condition_check_false.json");
transact_write_case!(given_parity_condition_failed_then_runtime_validation_then_none_when_transact_write_runs_then_cancellation_and_state_match_aws, "400/parity_condition_failed_then_runtime_validation_then_none.json");
transact_write_case!(given_parity_condition_false_with_metrics_omits_metrics_when_transact_write_runs_then_cancellation_and_state_match_aws, "400/parity_condition_false_with_metrics_omits_metrics.json");
transact_write_case!(given_parity_failing_put_missing_delete_failing_condition_all_old_when_transact_write_runs_then_cancellation_and_state_match_aws, "400/parity_failing_put_missing_delete_failing_condition_all_old.json");
transact_write_case!(given_parity_first_condition_fails_second_update_would_succeed_when_transact_write_runs_then_cancellation_and_state_match_aws, "400/parity_first_condition_fails_second_update_would_succeed.json");
transact_write_case!(given_parity_runtime_validation_then_condition_failed_then_none_when_transact_write_runs_then_cancellation_and_state_match_aws, "400/parity_runtime_validation_then_condition_failed_then_none.json");
transact_write_case!(given_parity_two_condition_checks_false_when_transact_write_runs_then_cancellation_and_state_match_aws, "400/parity_two_condition_checks_false.json");
transact_write_case!(given_parity_two_condition_checks_false_later_update_none_when_transact_write_runs_then_cancellation_and_state_match_aws, "400/parity_two_condition_checks_false_later_update_none.json");
transact_write_case!(given_parity_update_list_scalar_child_path_invalid_when_transact_write_runs_then_cancellation_and_state_match_aws, "400/parity_update_list_scalar_child_path_invalid.json");
transact_write_case!(given_parity_update_missing_addend_left_when_transact_write_runs_then_cancellation_and_state_match_aws, "400/parity_update_missing_addend_left.json");
transact_write_case!(given_parity_update_missing_addend_right_when_transact_write_runs_then_cancellation_and_state_match_aws, "400/parity_update_missing_addend_right.json");
transact_write_case!(given_parity_update_scalar_parent_path_invalid_when_transact_write_runs_then_cancellation_and_state_match_aws, "400/parity_update_scalar_parent_path_invalid.json");
transact_write_case!(given_parity_update_type_error_second_item_when_transact_write_runs_then_cancellation_and_state_match_aws, "400/parity_update_type_error_second_item.json");
transact_write_case!(given_put_condition_failure_all_old_missing_item_has_no_item_when_transact_write_runs_then_cancellation_and_state_match_aws, "400/put_condition_failure_all_old_missing_item_has_no_item.json");
transact_write_case!(given_update_document_path_missing_parent_cancellation_when_transact_write_runs_then_cancellation_and_state_match_aws, "400/update_document_path_missing_parent_cancellation.json");
transact_write_case!(given_update_runtime_type_error_cancellation_when_transact_write_runs_then_cancellation_and_state_match_aws, "400/update_runtime_type_error_cancellation.json");

// Boundary rules: Update accepts maximum-length and numeric key values and
// persists the item.
transact_write_case!(given_docs_limit_update_binary_partition_key_1_byte_valid_when_transact_write_runs_then_response_and_updated_item_match_aws, "field-transact_items/200/docs_limit_update_binary_partition_key_1_byte_valid.json");
transact_write_case!(given_docs_limit_update_binary_partition_key_2048_bytes_valid_when_transact_write_runs_then_response_and_updated_item_match_aws, "field-transact_items/200/docs_limit_update_binary_partition_key_2048_bytes_valid.json");
transact_write_case!(given_docs_limit_update_binary_sort_key_1024_bytes_valid_when_transact_write_runs_then_response_and_updated_item_match_aws, "field-transact_items/200/docs_limit_update_binary_sort_key_1024_bytes_valid.json");
transact_write_case!(given_docs_limit_update_number_partition_key_38_digits_valid_when_transact_write_runs_then_response_and_updated_item_match_aws, "field-transact_items/200/docs_limit_update_number_partition_key_38_digits_valid.json");
transact_write_case!(given_docs_limit_update_number_partition_key_positive_min_exponent_valid_when_transact_write_runs_then_response_and_updated_item_match_aws, "field-transact_items/200/docs_limit_update_number_partition_key_positive_min_exponent_valid.json");
transact_write_case!(given_docs_limit_update_number_partition_key_zero_valid_when_transact_write_runs_then_response_and_updated_item_match_aws, "field-transact_items/200/docs_limit_update_number_partition_key_zero_valid.json");
transact_write_case!(given_docs_limit_update_number_sort_key_38_digits_valid_when_transact_write_runs_then_response_and_updated_item_match_aws, "field-transact_items/200/docs_limit_update_number_sort_key_38_digits_valid.json");
transact_write_case!(given_docs_limit_update_number_sort_key_positive_min_exponent_valid_when_transact_write_runs_then_response_and_updated_item_match_aws, "field-transact_items/200/docs_limit_update_number_sort_key_positive_min_exponent_valid.json");
transact_write_case!(given_docs_limit_update_number_sort_key_zero_valid_when_transact_write_runs_then_response_and_updated_item_match_aws, "field-transact_items/200/docs_limit_update_number_sort_key_zero_valid.json");
transact_write_case!(given_docs_limit_update_string_partition_key_2048_bytes_valid_when_transact_write_runs_then_response_and_updated_item_match_aws, "field-transact_items/200/docs_limit_update_string_partition_key_2048_bytes_valid.json");
transact_write_case!(given_docs_limit_update_string_sort_key_1024_bytes_valid_when_transact_write_runs_then_response_and_updated_item_match_aws, "field-transact_items/200/docs_limit_update_string_sort_key_1024_bytes_valid.json");

// Validation and negative rules: remaining schema, member-shape, and
// cardinality precedence.
transact_write_case!(given_remaining_parity_condition_false_later_key_schema_mismatch_when_transact_write_runs_then_error_and_state_match_aws, "400/parity_condition_false_later_key_schema_mismatch.json");
transact_write_case!(given_remaining_parity_key_schema_mismatch_later_condition_false_when_transact_write_runs_then_error_and_state_match_aws, "400/parity_key_schema_mismatch_later_condition_false.json");
transact_write_case!(given_remaining_parity_key_schema_mismatch_suppresses_runtime_validation_when_transact_write_runs_then_error_and_state_match_aws, "400/parity_key_schema_mismatch_suppresses_runtime_validation.json");
transact_write_case!(given_remaining_txpathfunc_attribute_type_literal_type_top_level_validation_when_transact_write_runs_then_error_and_state_match_aws, "field-condition_expression/400/txpathfunc_attribute_type_literal_type_top_level_validation.json");
transact_write_case!(given_remaining_docs_limit_101_transaction_items_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/docs_limit_101_transaction_items_rejected.json");
transact_write_case!(given_remaining_txrule_single_transact_item_with_update_and_delete_members_rejected_when_transact_write_runs_then_error_and_state_match_aws, "field-transact_items/400/txrule_single_transact_item_with_update_and_delete_members_rejected.json");

// Business, format, and boundary rules: successful mixed and expression writes.
transact_write_case!(given_remaining_semantic_mixed_operations_condition_functions_success_when_transact_write_runs_then_response_and_state_match_aws, "field-condition_expression/200/semantic_mixed_operations_condition_functions_success.json");
transact_write_case!(given_remaining_semantic_update_condition_true_with_complex_functions_allows_update_when_transact_write_runs_then_response_and_state_match_aws, "field-condition_expression/200/semantic_update_condition_true_with_complex_functions_allows_update.json");
transact_write_case!(given_remaining_txpathfunc_aliased_reserved_nested_path_success_when_transact_write_runs_then_response_and_state_match_aws, "field-condition_expression/200/txpathfunc_aliased_reserved_nested_path_success.json");
transact_write_case!(given_remaining_txpathfunc_mixed_operations_functions_paths_success_when_transact_write_runs_then_response_and_state_match_aws, "field-condition_expression/200/txpathfunc_mixed_operations_functions_paths_success.json");
transact_write_case!(given_remaining_txarith_distinct_put_delete_update_condition_success_when_transact_write_runs_then_response_and_state_match_aws, "field-transact_items/200/txarith_distinct_put_delete_update_condition_success.json");
transact_write_case!(given_remaining_txarith_mixed_action_sections_success_when_transact_write_runs_then_response_and_state_match_aws, "field-transact_items/200/txarith_mixed_action_sections_success.json");
transact_write_case!(given_remaining_txarith_mixed_add_delete_set_updates_success_when_transact_write_runs_then_response_and_state_match_aws, "field-transact_items/200/txarith_mixed_add_delete_set_updates_success.json");
transact_write_case!(given_remaining_docs_limit_expression_attribute_name_token_255_bytes_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-update_expression/200/docs_limit_expression_attribute_name_token_255_bytes_valid.json");
transact_write_case!(given_remaining_docs_limit_expression_attribute_value_token_255_bytes_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-update_expression/200/docs_limit_expression_attribute_value_token_255_bytes_valid.json");
transact_write_case!(given_remaining_docs_limit_update_expression_4096_bytes_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-update_expression/200/docs_limit_update_expression_4096_bytes_valid.json");
transact_write_case!(given_remaining_parser_update_alias_reserved_child_after_list_valid_when_transact_write_runs_then_response_and_state_match_aws, "field-update_expression/200/parser_update_alias_reserved_child_after_list_valid.json");
