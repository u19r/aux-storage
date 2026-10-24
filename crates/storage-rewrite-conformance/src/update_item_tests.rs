//! UpdateItem readback, conditional atomicity, and CDC effects.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{
    AttributeValue, CreateTableRequest, GetItemRequest, KeyAttributeType, PutItemRequest,
    StreamName, UpdateItemRequest,
};
use stream_provider::StreamRowKind;

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

        let system_stream = StreamRowKind::System(StreamName::system_table_stream());
        let before = store.stream_operations.read_forward(system_stream.clone(), None, 10)
            .await.expect("read CDC before update");
        assert_eq!(before.items.len(), 1);

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
        let after_rejection = store.stream_operations.read_forward(system_stream.clone(), None, 10)
            .await.expect("read CDC after rejected update");
        assert_eq!(after_rejection.items.len(), 1);
        assert_eq!(after_rejection.items[0].id, before.items[0].id);

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
        let after_commit = store.stream_operations.read_forward(system_stream, None, 10)
            .await.expect("read CDC after accepted update");
        assert_eq!(after_commit.items.len(), 2);
        assert_eq!(after_commit.items[0].id, before.items[0].id);
        assert_ne!(after_commit.items[1].id, before.items[0].id);
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
async fn given_concurrent_updates_when_key_overlaps_then_every_increment_and_image_is_retained() {
    let store = TestStore::new().await;
    let table = unique_table_name();
    let mut create = crate::test_support::hash_table_request(table.clone());
    create.stream_specification = Some(storage_types::StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(storage_types::StreamViewType::NewAndOldImages),
    });
    store.operations.create_table(create).await.unwrap();
    let writes = (0..32).map(|_| {
        let request = UpdateItemRequest::try_from(json!({
            "TableName": table.to_string(),
            "Key": {"pk": {"S": "one"}},
            "UpdateExpression": "ADD #count :one",
            "ExpressionAttributeNames": {"#count": "count"},
            "ExpressionAttributeValues": {":one": {"N": "1"}},
            "ReturnValues": "ALL_NEW"
        }))
        .unwrap();
        store.operations.update_item(request)
    });
    for result in futures::future::join_all(writes).await {
        result.expect("ordinary concurrent update must commit");
    }
    let records = crate::image_retention_tests::table_records(&store, &table)
        .await
        .unwrap();
    assert_eq!(records.len(), 32);
    for count in 1..=32 {
        let matches = records
            .iter()
            .filter(|record| {
                record.new_image.as_ref().unwrap().get("count")
                    == Some(&AttributeValue::N(count.to_string()))
            })
            .count();
        assert_eq!(
            matches, 1,
            "every increment must appear once in stream history"
        );
    }
    let item = store
        .operations
        .get_item(storage_types::GetItemRequest::new(
            table,
            [("pk".to_owned(), AttributeValue::S("one".to_owned()))],
        ))
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(item).unwrap()["Item"]["count"],
        json!({"N": "32"})
    );
    store.finish().await;
}
