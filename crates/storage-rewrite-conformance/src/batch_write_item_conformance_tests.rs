//! BatchWriteItem behavior through the replacement Dynamo operation boundary.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::{Value, json};
use storage_observability_test_support::observation::observe;
use storage_types::{
    AttributeMap, BatchWriteItemRequest, GetItemRequest, KeyAttributes, ScanRequest, TableName,
};
use tracing::Level;

use super::test_support::{ConformanceFixtures, TestStore};

static FIXTURES: ConformanceFixtures = ConformanceFixtures::new();

fn fixture(source: &str) -> Value {
    FIXTURES.case(
        include_str!("../fixtures/batch_write_item_cases.jsonl"),
        source,
    )
}

fn assert_expected_response(case: &Value, status: u16, body: &Value) {
    assert_eq!(
        status, case["expected"]["status"],
        "{} returned {body}",
        case["source"]
    );
    let mut actual_body = body.clone();
    let mut expected_body = case["expected"]["body"].clone();
    if let Some(ignores) = case["ignores"].as_array() {
        assert_eq!(ignores, &["message"], "unsupported authoritative ignore");
        assert!(!actual_body["message"].as_str().unwrap().is_empty());
        actual_body.as_object_mut().unwrap().remove("message");
        expected_body.as_object_mut().unwrap().remove("message");
    }
    assert_eq!(
        serde_json::to_string(&actual_body).unwrap(),
        serde_json::to_string(&expected_body).unwrap(),
        "{}",
        case["source"],
    );
}

async fn assert_batch_write_case(
    store: &TestStore,
    case: &Value,
    request: BatchWriteItemRequest,
    rejection_reason: Option<&str>,
) {
    let table_kind = case["tables"][0].as_str().unwrap();
    let (name, writes) = case["request"]["RequestItems"]
        .as_object()
        .unwrap()
        .iter()
        .next()
        .unwrap();
    let table = TableName::new(name);
    store
        .create_conformance_table(table_kind, table.clone())
        .await;

    let observed = observe(store.operations.batch_write_item(request)).await;
    if let Some(reason) = rejection_reason {
        let rejection = observed.one_event_with(
            Level::INFO,
            &[
                ("operation", "BatchWriteItem"),
                ("error_code", "ValidationException"),
                ("reason", reason),
                ("message", "batch write request rejected"),
            ],
        );
        assert_eq!(
            rejection.fields.len(),
            4,
            "rejection trace has only safe context"
        );
        observed.assert_no_warning_or_error();
    } else {
        observed.assert_no_warning_or_error();
    }
    let (status, body) = match observed.result {
        Ok(response) => (200, serde_json::to_value(response).unwrap()),
        Err(error) => {
            let (status, axum::Json(body)): (_, axum::Json<http_error::ErrorResponse>) =
                http_error::HttpApiError::from(error).into();
            (status.as_u16(), serde_json::to_value(body).unwrap())
        }
    };
    assert_expected_response(case, status, &body);

    if status != 200 {
        let scan = ScanRequest::try_from(json!({"TableName": table.as_ref()})).unwrap();
        let scanned = store.operations.scan(scan).await.unwrap();
        assert!(
            scanned.items.as_deref().unwrap_or_default().is_empty(),
            "rejected batch must write no items"
        );
        return;
    }
    for write in writes.as_array().unwrap() {
        let (key, expected_item) = if let Some(put) = write.get("PutRequest") {
            let item = &put["Item"];
            (json!({"pk": item["pk"], "sk": item["sk"]}), Some(item))
        } else {
            (write["DeleteRequest"]["Key"].clone(), None)
        };
        let read = store
            .operations
            .get_item(GetItemRequest::new(
                table.clone(),
                serde_json::from_value::<KeyAttributes>(key).unwrap(),
            ))
            .await
            .unwrap();
        let actual_item = serde_json::to_value(read).unwrap()["Item"].clone();
        if let Some(expected_item) = expected_item {
            let mut expected_item: AttributeMap =
                serde_json::from_value(expected_item.clone()).unwrap();
            expected_item.normalize_numbers_for_write();
            assert_eq!(actual_item, serde_json::to_value(expected_item).unwrap());
        } else {
            assert_eq!(actual_item, Value::Null, "deleted key must remain absent");
        }
    }
}

macro_rules! batch_write_case {
    ($name:ident, $source:literal, $rejection_reason:expr) => {
        #[tokio::test]
        async fn $name() {
            let case = fixture($source);
            let rejection_reason: Option<&str> = $rejection_reason;
            assert_eq!(case["operation"], "BatchWriteItem", "fixture operation");
            let parsed =
                observe(async { BatchWriteItemRequest::try_from(case["request"].clone()) }).await;
            parsed.assert_no_warning_or_error();
            parsed.assert_database_calls_exact(&[]);
            let request = match parsed.result {
                Ok(request) => request,
                Err(message) => {
                    assert!(
                        rejection_reason.is_none(),
                        "provider rejection requires a parsed request"
                    );
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
            let outcome = AssertUnwindSafe(assert_batch_write_case(
                &store,
                &case,
                request,
                rejection_reason,
            ))
            .catch_unwind()
            .await;
            store.finish().await;
            if let Err(panic) = outcome {
                resume_unwind(panic);
            }
        }
    };
}

// Boundary rule: a one-byte binary partition key is written and returned
// exactly.
batch_write_case!(
    given_one_byte_binary_key_when_batch_write_item_runs_then_aws_response_and_item_match,
    "field-request_items/200/docs_limit_put_binary_partition_key_1_byte_valid.json",
    None
);

// Negative rule: a put and delete for the same key reject the whole batch.
batch_write_case!(
    given_put_and_delete_of_one_key_when_batch_write_item_runs_then_aws_validation_and_no_write,
    "400/put_and_delete_same_key_validation.json",
    Some("duplicate_item_key")
);

// Validation and negative rules: 400.
batch_write_case!(given_400_duplicate_put_keys_validation_when_batch_write_item_runs_then_conformance_response_matches, "400/duplicate_put_keys_validation.json", Some("duplicate_item_key"));

// Business and boundary rules: field-request_items/200.
batch_write_case!(given_field_request_items_200_docs_limit_25_write_requests_valid_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/200/docs_limit_25_write_requests_valid.json", None);
batch_write_case!(given_field_request_items_200_docs_limit_delete_binary_partition_key_1_byte_valid_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/200/docs_limit_delete_binary_partition_key_1_byte_valid.json", None);
batch_write_case!(given_field_request_items_200_docs_limit_delete_binary_partition_key_2048_bytes_valid_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/200/docs_limit_delete_binary_partition_key_2048_bytes_valid.json", None);
batch_write_case!(given_field_request_items_200_docs_limit_delete_binary_sort_key_1024_bytes_valid_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/200/docs_limit_delete_binary_sort_key_1024_bytes_valid.json", None);
batch_write_case!(given_field_request_items_200_docs_limit_delete_number_partition_key_38_digits_valid_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/200/docs_limit_delete_number_partition_key_38_digits_valid.json", None);
batch_write_case!(given_field_request_items_200_docs_limit_delete_number_partition_key_positive_min_exponent_valid_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/200/docs_limit_delete_number_partition_key_positive_min_exponent_valid.json", None);
batch_write_case!(given_field_request_items_200_docs_limit_delete_number_partition_key_zero_valid_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/200/docs_limit_delete_number_partition_key_zero_valid.json", None);
batch_write_case!(given_field_request_items_200_docs_limit_delete_number_sort_key_38_digits_valid_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/200/docs_limit_delete_number_sort_key_38_digits_valid.json", None);
batch_write_case!(given_field_request_items_200_docs_limit_delete_number_sort_key_positive_min_exponent_valid_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/200/docs_limit_delete_number_sort_key_positive_min_exponent_valid.json", None);
batch_write_case!(given_field_request_items_200_docs_limit_delete_number_sort_key_zero_valid_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/200/docs_limit_delete_number_sort_key_zero_valid.json", None);
batch_write_case!(given_field_request_items_200_docs_limit_delete_string_partition_key_2048_bytes_valid_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/200/docs_limit_delete_string_partition_key_2048_bytes_valid.json", None);
batch_write_case!(given_field_request_items_200_docs_limit_delete_string_sort_key_1024_bytes_valid_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/200/docs_limit_delete_string_sort_key_1024_bytes_valid.json", None);
batch_write_case!(given_field_request_items_200_docs_limit_put_binary_partition_key_2048_bytes_valid_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/200/docs_limit_put_binary_partition_key_2048_bytes_valid.json", None);
batch_write_case!(given_field_request_items_200_docs_limit_put_binary_sort_key_1024_bytes_valid_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/200/docs_limit_put_binary_sort_key_1024_bytes_valid.json", None);
batch_write_case!(given_field_request_items_200_docs_limit_put_number_partition_key_38_digits_valid_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/200/docs_limit_put_number_partition_key_38_digits_valid.json", None);
batch_write_case!(given_field_request_items_200_docs_limit_put_number_partition_key_positive_min_exponent_valid_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/200/docs_limit_put_number_partition_key_positive_min_exponent_valid.json", None);
batch_write_case!(given_field_request_items_200_docs_limit_put_number_partition_key_zero_valid_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/200/docs_limit_put_number_partition_key_zero_valid.json", None);
batch_write_case!(given_field_request_items_200_docs_limit_put_number_sort_key_38_digits_valid_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/200/docs_limit_put_number_sort_key_38_digits_valid.json", None);
batch_write_case!(given_field_request_items_200_docs_limit_put_number_sort_key_positive_min_exponent_valid_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/200/docs_limit_put_number_sort_key_positive_min_exponent_valid.json", None);
batch_write_case!(given_field_request_items_200_docs_limit_put_number_sort_key_zero_valid_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/200/docs_limit_put_number_sort_key_zero_valid.json", None);
batch_write_case!(given_field_request_items_200_docs_limit_put_string_partition_key_2048_bytes_valid_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/200/docs_limit_put_string_partition_key_2048_bytes_valid.json", None);
batch_write_case!(given_field_request_items_200_docs_limit_put_string_sort_key_1024_bytes_valid_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/200/docs_limit_put_string_sort_key_1024_bytes_valid.json", None);

// Validation and negative rules: field-request_items/400.
batch_write_case!(given_field_request_items_400_docs_limit_26_write_requests_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_26_write_requests_rejected.json", None);
batch_write_case!(given_field_request_items_400_docs_limit_delete_binary_partition_key_2049_bytes_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_delete_binary_partition_key_2049_bytes_rejected.json", None);
batch_write_case!(given_field_request_items_400_docs_limit_delete_binary_partition_key_empty_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_delete_binary_partition_key_empty_rejected.json", None);
batch_write_case!(given_field_request_items_400_docs_limit_delete_binary_sort_key_1025_bytes_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_delete_binary_sort_key_1025_bytes_rejected.json", None);
batch_write_case!(given_field_request_items_400_docs_limit_delete_binary_sort_key_empty_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_delete_binary_sort_key_empty_rejected.json", None);
batch_write_case!(given_field_request_items_400_docs_limit_delete_number_partition_key_39_digits_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_delete_number_partition_key_39_digits_rejected.json", None);
batch_write_case!(given_field_request_items_400_docs_limit_delete_number_partition_key_empty_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_delete_number_partition_key_empty_rejected.json", None);
batch_write_case!(given_field_request_items_400_docs_limit_delete_number_partition_key_under_min_exponent_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_delete_number_partition_key_under_min_exponent_rejected.json", None);
batch_write_case!(given_field_request_items_400_docs_limit_delete_number_sort_key_39_digits_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_delete_number_sort_key_39_digits_rejected.json", None);
batch_write_case!(given_field_request_items_400_docs_limit_delete_number_sort_key_empty_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_delete_number_sort_key_empty_rejected.json", None);
batch_write_case!(given_field_request_items_400_docs_limit_delete_number_sort_key_under_min_exponent_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_delete_number_sort_key_under_min_exponent_rejected.json", None);
batch_write_case!(given_field_request_items_400_docs_limit_delete_string_partition_key_2049_bytes_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_delete_string_partition_key_2049_bytes_rejected.json", None);
batch_write_case!(given_field_request_items_400_docs_limit_delete_string_partition_key_empty_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_delete_string_partition_key_empty_rejected.json", None);
batch_write_case!(given_field_request_items_400_docs_limit_delete_string_sort_key_1025_bytes_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_delete_string_sort_key_1025_bytes_rejected.json", None);
batch_write_case!(given_field_request_items_400_docs_limit_delete_string_sort_key_empty_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_delete_string_sort_key_empty_rejected.json", None);
batch_write_case!(given_field_request_items_400_docs_limit_put_binary_partition_key_2049_bytes_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_put_binary_partition_key_2049_bytes_rejected.json", None);
batch_write_case!(given_field_request_items_400_docs_limit_put_binary_partition_key_empty_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_put_binary_partition_key_empty_rejected.json", None);
batch_write_case!(given_field_request_items_400_docs_limit_put_binary_sort_key_1025_bytes_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_put_binary_sort_key_1025_bytes_rejected.json", None);
batch_write_case!(given_field_request_items_400_docs_limit_put_binary_sort_key_empty_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_put_binary_sort_key_empty_rejected.json", None);
batch_write_case!(given_field_request_items_400_docs_limit_put_number_partition_key_39_digits_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_put_number_partition_key_39_digits_rejected.json", None);
batch_write_case!(given_field_request_items_400_docs_limit_put_number_partition_key_empty_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_put_number_partition_key_empty_rejected.json", None);
batch_write_case!(given_field_request_items_400_docs_limit_put_number_partition_key_under_min_exponent_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_put_number_partition_key_under_min_exponent_rejected.json", None);
batch_write_case!(given_field_request_items_400_docs_limit_put_number_sort_key_39_digits_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_put_number_sort_key_39_digits_rejected.json", None);
batch_write_case!(given_field_request_items_400_docs_limit_put_number_sort_key_empty_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_put_number_sort_key_empty_rejected.json", None);
batch_write_case!(given_field_request_items_400_docs_limit_put_number_sort_key_under_min_exponent_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_put_number_sort_key_under_min_exponent_rejected.json", None);
batch_write_case!(given_field_request_items_400_docs_limit_put_string_partition_key_2049_bytes_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_put_string_partition_key_2049_bytes_rejected.json", None);
batch_write_case!(given_field_request_items_400_docs_limit_put_string_partition_key_empty_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_put_string_partition_key_empty_rejected.json", None);
batch_write_case!(given_field_request_items_400_docs_limit_put_string_sort_key_1025_bytes_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_put_string_sort_key_1025_bytes_rejected.json", None);
batch_write_case!(given_field_request_items_400_docs_limit_put_string_sort_key_empty_rejected_when_batch_write_item_runs_then_conformance_response_matches, "field-request_items/400/docs_limit_put_string_sort_key_empty_rejected.json", None);
