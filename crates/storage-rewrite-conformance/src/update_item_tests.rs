//! UpdateItem readback, conditional atomicity, and CDC effects.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{
    AttributeValue, CreateTableRequest, GetItemRequest, KeyAttributeType, PutItemRequest,
    UpdateItemRequest,
};

use crate::test_support::{TestStore, assert_dynamo_error_response_exact, unique_table_name};

/// Business rule: an empty UpdateItem response still persists the complete
/// edit.
#[tokio::test]
async fn given_existing_item_when_update_returns_no_attributes_then_get_sees_set_and_remove() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        store
            .create_hash_range_table(table.clone(), KeyAttributeType::S)
            .await;

        let seeded = PutItemRequest::try_from(json!({
            "TableName": table.to_string(),
            "Item": {
                "pk": {"S": "partition"},
                "sk": {"S": "sort"},
                "count": {"N": "1"},
                "note": {"S": "before"}
            }
        }))
        .expect("valid seed request");
        store.operations.put_item(seeded).await.expect("seed item");

        let updated = UpdateItemRequest::try_from(json!({
            "TableName": table.to_string(),
            "Key": {"pk": {"S": "partition"}, "sk": {"S": "sort"}},
            "UpdateExpression": "SET #count = :next, added = :yes REMOVE note",
            "ExpressionAttributeNames": {"#count": "count"},
            "ExpressionAttributeValues": {":next": {"N": "2"}, ":yes": {"S": "yes"}}
        }))
        .expect("valid update request");
        let observed = observe(store.operations.update_item(updated)).await;
        observed.assert_no_warning_or_error();
        let response = observed.result.expect("update item");
        assert_eq!(
            serde_json::to_value(response).expect("serialize response"),
            json!({})
        );

        let read = GetItemRequest::try_from(json!({
            "TableName": table.to_string(),
            "Key": {"pk": {"S": "partition"}, "sk": {"S": "sort"}}
        }))
        .expect("valid readback request");
        let response = store
            .operations
            .get_item(read)
            .await
            .expect("read updated item");
        assert_eq!(
            serde_json::to_value(response).expect("serialize readback"),
            json!({"Item": {
                "pk": {"S": "partition"},
                "sk": {"S": "sort"},
                "count": {"N": "2"},
                "added": {"S": "yes"}
            }})
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

/// Business and negative rules from legacy SQL/KV: a version guard makes a
/// multi-field edit atomic. Dynamo conformance supplies the exact error body.
#[tokio::test]
async fn given_versioned_item_when_conditional_updates_run_then_only_matching_edit_commits() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let create = CreateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "StreamSpecification": {
                "StreamEnabled": true,
                "StreamViewType": "NEW_AND_OLD_IMAGES"
            }
        }))
        .expect("valid stream-enabled table");
        store.operations.create_table(create).await.expect("create table");

        let original = json!({
            "pk": {"S": "AUTHN_TOTP"},
            "sk": {"S": "U#u1#DEV#d1"},
            "_v": {"N": "0"}
        });
        let put = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": original
        }))
        .expect("valid seed PutItem");
        store.operations.put_item(put).await.expect("seed versioned item");

        let before = super::cdc_stream_tests::system_records(&store, 10)
            .await.expect("read CDC before update");
        assert_eq!(before.len(), 1);

        let update_request = |expected_version: &str| {
            UpdateItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Key": {"pk": {"S": "AUTHN_TOTP"}, "sk": {"S": "U#u1#DEV#d1"}},
                "UpdateExpression": "SET active = :active, activated_at = :at, backup_code_hashes = :codes, #v = :next",
                "ConditionExpression": "#v = :expected",
                "ExpressionAttributeNames": {"#v": "_v"},
                "ExpressionAttributeValues": {
                    ":active": {"BOOL": true},
                    ":at": {"S": "2025-01-01T00:00:00Z"},
                    ":codes": {"L": []},
                    ":next": {"N": "1"},
                    ":expected": {"N": expected_version}
                },
                "ReturnValues": "ALL_NEW"
            }))
            .expect("valid conditional UpdateItem")
        };

        let rejected = observe(store.operations.update_item(update_request("1"))).await;
        rejected.assert_no_warning_or_error();
        assert_dynamo_error_response_exact(
            rejected.result.expect_err("wrong version must reject edit"),
            axum::http::StatusCode::BAD_REQUEST,
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"The conditional request failed"}"#,
        );
        let unchanged = GetItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": "AUTHN_TOTP"}, "sk": {"S": "U#u1#DEV#d1"}}
        }))
        .expect("valid GetItem");
        let unchanged = store.operations.get_item(unchanged).await.expect("read unchanged item");
        assert_eq!(serde_json::to_value(unchanged).expect("serialize unchanged item"),
            json!({"Item": original}));
        let after_rejection = super::cdc_stream_tests::system_records(&store, 10)
            .await.expect("read CDC after rejected update");
        assert_eq!(after_rejection.len(), 1);
        assert_eq!(after_rejection[0].sequence_number, before[0].sequence_number);

        let accepted = observe(store.operations.update_item(update_request("0"))).await;
        accepted.assert_no_warning_or_error();
        let updated = json!({
            "pk": {"S": "AUTHN_TOTP"},
            "sk": {"S": "U#u1#DEV#d1"},
            "_v": {"N": "1"},
            "active": {"BOOL": true},
            "activated_at": {"S": "2025-01-01T00:00:00Z"},
            "backup_code_hashes": {"L": []}
        });
        assert_eq!(serde_json::to_value(accepted.result.expect("matching update commits"))
            .expect("serialize UpdateItem response"), json!({"Attributes": updated}));
        let read = GetItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": "AUTHN_TOTP"}, "sk": {"S": "U#u1#DEV#d1"}}
        }))
        .expect("valid updated GetItem");
        let read = store.operations.get_item(read).await.expect("read updated item");
        assert_eq!(serde_json::to_value(read).expect("serialize updated item"),
            json!({"Item": updated}));
        let after_commit = super::cdc_stream_tests::system_records(&store, 10)
            .await.expect("read CDC after accepted update");
        assert_eq!(after_commit.len(), 2);
        assert_eq!(after_commit[0].sequence_number, before[0].sequence_number);
        assert_ne!(after_commit[1].sequence_number, before[0].sequence_number);
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

/// Negative rule from legacy SQL and Cucumber. Dynamo conformance's missing
/// table cases for other operations supply this ResourceNotFound body; the
/// authoritative suite has no UpdateItem missing-table case yet.
#[tokio::test]
async fn given_missing_table_when_update_item_runs_then_resource_not_found_response_is_exact() {
    let store = TestStore::new().await;
    let table = unique_table_name();
    let request = UpdateItemRequest::try_from(json!({
        "TableName": table.as_ref(),
        "Key": {"pk": {"S": "item"}},
        "UpdateExpression": "SET #name = :new",
        "ExpressionAttributeNames": {"#name": "name"},
        "ExpressionAttributeValues": {":new": {"S": "updated"}}
    }))
    .expect("valid UpdateItem request");

    let observed = observe(store.operations.update_item(request)).await;
    observed.assert_no_warning_or_error();
    assert_dynamo_error_response_exact(
        observed
            .result
            .expect_err("missing table rejects UpdateItem"),
        axum::http::StatusCode::BAD_REQUEST,
        &format!(
            r#"{{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found: Table: {} not found"}}"#,
            table.as_ref()
        ),
    );
    store.finish().await;
}

#[tokio::test]
async fn given_concurrent_updates_when_outcomes_vary_then_acknowledged_images_are_retained() {
    let store = TestStore::new().await;
    let table = unique_table_name();
    let mut create = crate::test_support::hash_table_request(table.clone());
    create.stream_specification = Some(storage_types::StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(storage_types::StreamViewType::NewAndOldImages),
    });
    store.operations.create_table(create).await.unwrap();
    let writes = (0..32).map(|operation| {
        let request = UpdateItemRequest::try_from(json!({
            "TableName": table.to_string(), "Key": {"pk": {"S": "one"}},
            "UpdateExpression": "SET #operation = :operation ADD #count :one",
            "ExpressionAttributeNames": {"#operation": "operation", "#count": "count"},
            "ExpressionAttributeValues": {":operation": {"N": operation.to_string()}, ":one": {"N": "1"}},
            "ReturnValues": "ALL_NEW"
        })).unwrap();
        let operations = &store.operations;
        async move { (operation, operations.update_item(request).await) }
    });
    let mut acknowledged = std::collections::HashMap::new();
    let mut unknown = std::collections::HashSet::new();
    let mut rejected = 0;
    for (operation, result) in futures::future::join_all(writes).await {
        match result {
            Ok(response) => {
                let attributes = response.attributes.expect("ALL_NEW attributes");
                assert_eq!(
                    attributes.get("operation"),
                    Some(&AttributeValue::N(operation.to_string())),
                    "response must belong to its caller"
                );
                let Some(AttributeValue::N(count)) = attributes.get("count") else {
                    panic!("acknowledged counter must be numeric");
                };
                let count = count.parse::<usize>().unwrap();
                assert!(count > 0, "acknowledged increments are positive");
                assert!(
                    !acknowledged.values().any(|previous| *previous == count),
                    "each acknowledgment advances the counter"
                );
                acknowledged.insert(operation, count);
            }
            Err(error)
                if error.native_attempt_rejection()
                    == Some(storage_types::NativeAttemptRejection::Conflict) =>
            {
                rejected += 1
            }
            Err(error) if error.is_commit_unknown() => {
                unknown.insert(operation);
            }
            Err(error) => panic!("unexpected concurrent update failure: {:?}", error.as_ref()),
        }
    }
    // Read primary state before history: an unknown commit can settle between
    // reads. Its possible effect must remain bounded, never retried blindly.
    let mut request = GetItemRequest::new(
        table.clone(),
        [("pk".to_owned(), AttributeValue::S("one".to_owned()))],
    );
    request.consistent_read = Some(true);
    let item = serde_json::to_value(store.operations.get_item(request).await.unwrap()).unwrap();
    let current = item
        .get("Item")
        .filter(|item| !item.is_null())
        .map_or(0, |item| {
            item["count"]["N"]
                .as_str()
                .unwrap()
                .parse::<usize>()
                .unwrap()
        });
    assert!(
        current >= acknowledged.len(),
        "acknowledged increments cannot be lost"
    );
    assert!(
        acknowledged.values().all(|count| *count <= current),
        "acknowledged images cannot exceed current state"
    );
    let records = crate::image_retention_tests::table_records(&store, &table)
        .await
        .unwrap();
    assert!(
        records.len() >= current,
        "primary writes must have their history images"
    );
    assert!(
        records.len() <= acknowledged.len() + unknown.len(),
        "known-aborted writes cannot publish images"
    );
    eprintln!(
        "concurrent_updates: acknowledged={} rejected={rejected} unknown={} primary={current} \
         images={}",
        acknowledged.len(),
        unknown.len(),
        records.len()
    );
    let mut observed_operations = std::collections::HashSet::new();
    for record in &records {
        let image = record.item.as_ref().unwrap();
        let Some(AttributeValue::N(operation)) = image.get("operation") else {
            panic!("each image must identify its operation");
        };
        let operation = operation.parse::<usize>().unwrap();
        assert!(
            acknowledged.contains_key(&operation) || unknown.contains(&operation),
            "known-aborted operations cannot publish"
        );
        assert!(
            observed_operations.insert(operation),
            "a request cannot increment twice"
        );
        if let Some(count) = acknowledged.get(&operation) {
            assert_eq!(
                image.get("count"),
                Some(&AttributeValue::N(count.to_string())),
                "acknowledged image must match its response"
            );
        }
    }
    assert!(
        acknowledged
            .keys()
            .all(|operation| observed_operations.contains(operation)),
        "every acknowledgment must have its own image"
    );
    for count in 1..=records.len() {
        let matches = records
            .iter()
            .filter(|record| {
                record.item.as_ref().unwrap().get("count")
                    == Some(&AttributeValue::N(count.to_string()))
            })
            .count();
        assert_eq!(
            matches, 1,
            "every increment must appear once in stream history"
        );
    }
    store.finish().await;
}
