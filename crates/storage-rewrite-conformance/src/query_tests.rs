//! Query pagination rules at the replacement Dynamo operation boundary.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{KeyAttributeType, PutItemRequest, QueryRequest};

use super::test_support::{TestStore, unique_table_name};

// Business and boundary rules from legacy KV and Cucumber: Limit counts
// evaluated rows before FilterExpression, and an empty filtered page keeps its
// continuation.
#[tokio::test]
async fn given_filtered_query_when_pages_are_followed_then_every_match_appears_after_empty_page() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        store
            .create_hash_range_table(table.clone(), KeyAttributeType::S)
            .await;

        for (sk, status) in [
            ("item#001", "inactive"),
            ("item#002", "active"),
            ("item#003", "active"),
        ] {
            let put = PutItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Item": {
                    "pk": {"S": "user#1"},
                    "sk": {"S": sk},
                    "status": {"S": status}
                }
            }))
            .expect("valid page fixture PutItem");
            store
                .operations
                .put_item(put)
                .await
                .expect("seed query row");
        }

        let request = QueryRequest::try_from(json!({
            "TableName": table.as_ref(),
            "KeyConditionExpression": "pk = :pk",
            "FilterExpression": "#status = :active",
            "ExpressionAttributeNames": {"#status": "status"},
            "ExpressionAttributeValues": {
                ":pk": {"S": "user#1"},
                ":active": {"S": "active"}
            },
            "Limit": 1,
            "ConsistentRead": true
        }))
        .expect("valid filtered Query");

        let first = observe(store.operations.query(request.clone())).await;
        first.assert_no_warning_or_error();
        let first = first.result.expect("read filtered first page");
        assert_eq!(
            serde_json::to_value(&first).expect("serialize first page"),
            json!({
                "Items": [],
                "Count": 0,
                "ScannedCount": 1,
                "LastEvaluatedKey": {"pk": {"S": "user#1"}, "sk": {"S": "item#001"}}
            })
        );

        let mut next = request.clone();
        next.exclusive_start_key = Some(
            first
                .last_evaluated_key
                .expect("empty filtered page must be resumable")
                .into(),
        );
        let second = observe(store.operations.query(next)).await;
        second.assert_no_warning_or_error();
        let second = second.result.expect("read second page");
        assert_eq!(
            serde_json::to_value(&second).expect("serialize second page"),
            json!({
                "Items": [{
                    "pk": {"S": "user#1"},
                    "sk": {"S": "item#002"},
                    "status": {"S": "active"}
                }],
                "Count": 1,
                "ScannedCount": 1,
                "LastEvaluatedKey": {"pk": {"S": "user#1"}, "sk": {"S": "item#002"}}
            })
        );

        let mut last = request;
        last.exclusive_start_key = Some(
            second
                .last_evaluated_key
                .expect("second page has another match")
                .into(),
        );
        let third = observe(store.operations.query(last)).await;
        third.assert_no_warning_or_error();
        let third = third.result.expect("read final page");
        assert_eq!(
            serde_json::to_value(third).expect("serialize final page"),
            json!({
                "Items": [{
                    "pk": {"S": "user#1"},
                    "sk": {"S": "item#003"},
                    "status": {"S": "active"}
                }],
                "Count": 1,
                "ScannedCount": 1
            })
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
async fn given_projected_internal_query_when_following_pages_then_source_accounting_survives() {
    let store = TestStore::new().await;
    let table = unique_table_name();
    store
        .create_hash_range_table(table.clone(), KeyAttributeType::S)
        .await;
    let mut source_bytes = Vec::new();
    for sk in ["first", "second"] {
        let put = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": {
                "pk": {"S": "tenant"}, "sk": {"S": sk},
                "payload": {"S": "value"}, "unprojected": {"S": "x".repeat(1024)}
            }
        }))
        .expect("valid item");
        source_bytes.push(u64::from(
            put.item.logical_size().expect("source item size"),
        ));
        store.operations.put_item(put).await.expect("seed query");
    }
    let mut request = storage_types::QueryTableRequest {
        table_name: table,
        index_name: None,
        key_condition_expression: "pk = :pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(
            [(
                ":pk".to_string(),
                storage_types::AttributeValue::S("tenant".to_string()),
            )]
            .into_iter()
            .collect(),
        ),
        projection_expression: Some("payload".to_string()),
        limit: Some(1),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: true,
    };
    let first = store
        .operations
        .query_page(request.clone())
        .await
        .expect("first page");
    assert_eq!(first.progress.evaluated_count, 1);
    assert_eq!(first.progress.evaluated_bytes, source_bytes[0]);
    assert_eq!(
        first.items[0].to_attribute_map().expect("projected item"),
        [(
            "payload".to_string(),
            storage_types::AttributeValue::S("value".to_string())
        )]
        .into_iter()
        .collect()
    );
    request.exclusive_start_key = Some(first.continuation.expect("continuation"));
    let second = store
        .operations
        .query_page(request.clone())
        .await
        .expect("second page");
    assert_eq!(second.progress.evaluated_count, 1);
    assert_eq!(second.progress.evaluated_bytes, source_bytes[1]);
    assert_ne!(
        first
            .progress
            .last_evaluated_key
            .expect("first key")
            .next_page_token()
            .expect("token"),
        second
            .progress
            .last_evaluated_key
            .expect("second key")
            .next_page_token()
            .expect("token"),
    );
    assert!(second.continuation.is_none());
    request.index_name = Some(storage_types::IndexName::new("missing"));
    assert!(store.operations.query_page(request).await.is_err());
    store.finish().await;
}
