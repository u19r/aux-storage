//! GetItem visibility after a real write and update through the replacement
//! store.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{GetItemRequest, KeyAttributeType, PutItemRequest, UpdateItemRequest};

use super::test_support::{TestStore, assert_dynamo_error_response_exact, unique_table_name};

const SCHEMA_MISMATCH_BODY: &str = concat!(
    r#"{"__type":"com.amazon.coral.validate#ValidationException","#,
    r#""message":"The provided key element does not match the schema"}"#,
);

// Business and format rules from Cucumber: special keys read the latest full
// image.
#[tokio::test]
async fn given_special_character_key_when_item_is_updated_then_get_item_returns_exact_latest_image()
{
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        let private_key = "key-with.special_chars@domain.com";
        let seed = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": {
                "pk": {"S": private_key},
                "name": {"S": "Original Name"},
                "obsolete": {"S": "private-old-value"}
            }
        }))
        .unwrap();
        store.operations.put_item(seed).await.unwrap();

        let update = UpdateItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": private_key}},
            "UpdateExpression": "SET #name = :name, #new = :new REMOVE obsolete",
            "ExpressionAttributeNames": {"#name": "name", "#new": "new"},
            "ExpressionAttributeValues": {
                ":name": {"S": "Updated Name"}, ":new": {"S": "New Attribute"}
            }
        }))
        .unwrap();
        store.operations.update_item(update).await.unwrap();

        let parsed = observe(async {
            GetItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Key": {"pk": {"S": private_key}},
                "ConsistentRead": true
            }))
        })
        .await;
        parsed.assert_database_calls_exact(&[]);
        parsed.assert_no_warning_or_error();
        let observed = observe(store.operations.get_item(parsed.result.unwrap())).await;
        observed.assert_no_warning_or_error();
        let span = observed.one_span_with("dynamo_operation", &[("operation", "GetItem")]);
        assert_eq!(span.fields.len(), 1);
        let trace = format!("{:?} {:?}", observed.spans, observed.events);
        assert!(!trace.contains(private_key));
        assert!(!trace.contains("private-old-value"));
        assert!(!trace.contains("Updated Name"));
        let actual = serde_json::to_value(observed.result.unwrap()).unwrap();
        let expected = json!({"Item": {
            "pk": {"S": private_key},
            "name": {"S": "Updated Name"},
            "new": {"S": "New Attribute"}
        }});
        assert_eq!(
            serde_json::to_string(&actual).unwrap(),
            serde_json::to_string(&expected).unwrap(),
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Negative and format rule from Cucumber: a missing table has the Dynamo error
// body.
#[tokio::test]
async fn given_missing_table_when_get_item_runs_then_resource_not_found_response_is_exact() {
    let store = TestStore::new().await;
    let table = unique_table_name();
    let request = GetItemRequest::try_from(json!({
        "TableName": table.as_ref(),
        "Key": {"pk": {"S": "private-customer-key"}}
    }))
    .unwrap();

    let observed = observe(store.operations.get_item(request)).await;
    observed.assert_no_warning_or_error();
    observed.one_span_with("dynamo_operation", &[("operation", "GetItem")]);
    let trace = format!("{:?} {:?}", observed.spans, observed.events);
    assert!(!trace.contains("private-customer-key"));
    let expected = json!({
        "__type": "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException",
        "message": format!("Requested resource not found: Table: {} not found", table.as_ref())
    });
    assert_dynamo_error_response_exact(
        observed.result.unwrap_err(),
        axum::http::StatusCode::BAD_REQUEST,
        &expected.to_string(),
    );
    store.finish().await;
}

// Validation and performance rules: an empty name is rejected before storage
// I/O.
#[tokio::test]
async fn given_empty_table_name_when_get_item_is_parsed_then_exact_error_requires_no_database_calls()
 {
    let parsed = observe(async {
        GetItemRequest::try_from(json!({
            "TableName": "", "Key": {"pk": {"S": "private-customer-key"}}
        }))
    })
    .await;
    parsed.assert_database_calls_exact(&[]);
    parsed.assert_no_warning_or_error();
    assert_eq!(parsed.result.unwrap_err(), "TableName cannot be empty");
}

// Negative and format rule from Cucumber: a partial compound key is invalid.
#[tokio::test]
async fn given_missing_sort_key_when_get_item_runs_then_exact_validation_response_is_returned() {
    let store = TestStore::new().await;
    let table = unique_table_name();
    store
        .create_hash_range_table(table.clone(), KeyAttributeType::S)
        .await;
    let request = GetItemRequest::try_from(json!({
        "TableName": table.as_ref(), "Key": {"pk": {"S": "private-customer-key"}}
    }))
    .unwrap();

    let observed = observe(store.operations.get_item(request)).await;
    observed.assert_no_warning_or_error();
    observed.one_span_with("dynamo_operation", &[("operation", "GetItem")]);
    let trace = format!("{:?} {:?}", observed.spans, observed.events);
    assert!(!trace.contains("private-customer-key"));
    assert_dynamo_error_response_exact(
        observed.result.unwrap_err(),
        axum::http::StatusCode::BAD_REQUEST,
        SCHEMA_MISMATCH_BODY,
    );
    store.finish().await;
}

// Negative rule: an extra key attribute cannot silently select an existing
// item.
#[tokio::test]
async fn given_extra_key_attribute_when_get_item_runs_then_exact_validation_response_is_returned() {
    let store = TestStore::new().await;
    let table = store.create_standard_table().await;
    let request = GetItemRequest::try_from(json!({
        "TableName": table.as_ref(),
        "Key": {"pk": {"S": "private-customer-key"}, "extra": {"S": "private-extra"}}
    }))
    .unwrap();

    let observed = observe(store.operations.get_item(request)).await;
    observed.assert_no_warning_or_error();
    assert_dynamo_error_response_exact(
        observed.result.unwrap_err(),
        axum::http::StatusCode::BAD_REQUEST,
        SCHEMA_MISMATCH_BODY,
    );
    let trace = format!("{:?} {:?}", observed.spans, observed.events);
    assert!(!trace.contains("private-customer-key"));
    assert!(!trace.contains("private-extra"));
    store.finish().await;
}

// Negative rule: the key value type must match the table's declared schema.
#[tokio::test]
async fn given_wrong_key_type_when_get_item_runs_then_exact_validation_response_is_returned() {
    let store = TestStore::new().await;
    let table = store.create_standard_table().await;
    let request = GetItemRequest::try_from(json!({
        "TableName": table.as_ref(), "Key": {"pk": {"N": "42"}}
    }))
    .unwrap();

    let observed = observe(store.operations.get_item(request)).await;
    observed.assert_no_warning_or_error();
    assert_dynamo_error_response_exact(
        observed.result.unwrap_err(),
        axum::http::StatusCode::BAD_REQUEST,
        SCHEMA_MISMATCH_BODY,
    );
    store.finish().await;
}
