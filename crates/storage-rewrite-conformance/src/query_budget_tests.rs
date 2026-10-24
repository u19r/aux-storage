//! Customer-visible Query pages use logical source-item bytes.

use std::panic::{AssertUnwindSafe, resume_unwind};

use base64::{Engine as _, engine::general_purpose};
use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{KeyAttributeType, PutItemRequest, QueryRequest};

use super::test_support::{TestStore, unique_table_name};

/// Boundary rule from Cucumber: filtering, projection, and COUNT do not shrink
/// the source page.
#[tokio::test]
async fn given_large_filtered_rows_when_query_pages_then_source_bytes_set_the_cursor_for_projection_and_count()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        store
            .create_hash_range_table(table.clone(), KeyAttributeType::S)
            .await;
        let payload = "x".repeat(380_000);
        for (index, state) in ["inactive", "inactive", "active", "active"]
            .iter()
            .enumerate()
        {
            let put = PutItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Item": {
                    "pk": {"S": "range-budget"},
                    "sk": {"S": format!("item#{:03}", index + 1)},
                    "state": {"S": state},
                    "payload": {"S": payload}
                }
            }))
            .unwrap();
            store.operations.put_item(put).await.unwrap();
        }

        let query = QueryRequest::try_from(json!({
            "TableName": table.as_ref(),
            "KeyConditionExpression": "pk = :pk",
            "FilterExpression": "#state = :active",
            "ProjectionExpression": "pk, sk",
            "ExpressionAttributeNames": {"#state": "state"},
            "ExpressionAttributeValues": {
                ":pk": {"S": "range-budget"}, ":active": {"S": "active"}
            }
        }))
        .unwrap();
        let first = observe(store.operations.query(query.clone())).await;
        first.assert_no_warning_or_error();
        let span = first.one_span_with("dynamo_operation", &[("operation", "Query")]);
        assert_eq!(span.fields.len(), 1);
        let first = first.result.unwrap();
        let cursor = json!({"pk": {"S": "range-budget"}, "sk": {"S": "item#002"}});
        assert_eq!(
            serde_json::to_value(&first).unwrap(),
            json!({
                "Items": [], "Count": 0, "ScannedCount": 2, "LastEvaluatedKey": cursor
            })
        );

        let mut next = query;
        next.exclusive_start_key = Some(first.last_evaluated_key.unwrap().into());
        let second = observe(store.operations.query(next)).await;
        second.assert_no_warning_or_error();
        assert_eq!(
            serde_json::to_value(second.result.unwrap()).unwrap(),
            json!({
                "Items": [
                    {"pk": {"S": "range-budget"}, "sk": {"S": "item#003"}},
                    {"pk": {"S": "range-budget"}, "sk": {"S": "item#004"}}
                ],
                "Count": 2,
                "ScannedCount": 2
            })
        );

        let count = QueryRequest::try_from(json!({
            "TableName": table.as_ref(),
            "KeyConditionExpression": "pk = :pk",
            "FilterExpression": "#state = :active",
            "ExpressionAttributeNames": {"#state": "state"},
            "ExpressionAttributeValues": {
                ":pk": {"S": "range-budget"}, ":active": {"S": "active"}
            },
            "Select": "COUNT"
        }))
        .unwrap();
        let count = observe(store.operations.query(count)).await;
        count.assert_no_warning_or_error();
        assert_eq!(
            serde_json::to_value(count.result.unwrap()).unwrap(),
            json!({
                "Count": 0, "ScannedCount": 2, "LastEvaluatedKey": cursor
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

/// Format rules from Cucumber: escaped JSON and base64 expansion do not consume
/// source bytes.
#[tokio::test]
async fn given_escaped_and_binary_rows_when_query_runs_then_logical_pages_remain_complete() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        store
            .create_hash_range_table(table.clone(), KeyAttributeType::S)
            .await;
        let escaped = json!({"S": "\"".repeat(300_000)});
        let binary = json!({"B": general_purpose::STANDARD.encode(vec![0_u8; 300_000])});

        for (partition, count, payload) in [("escaped", 2, escaped), ("binary", 3, binary)] {
            let mut expected_items = Vec::new();
            for index in 1..=count {
                let item = json!({
                    "pk": {"S": partition},
                    "sk": {"S": format!("item#{index:03}")},
                    "state": {"S": "active"},
                    "payload": payload
                });
                let put = PutItemRequest::try_from(json!({
                    "TableName": table.as_ref(), "Item": item
                }))
                .unwrap();
                store.operations.put_item(put).await.unwrap();
                expected_items.push(item);
            }
            let query = QueryRequest::try_from(json!({
                "TableName": table.as_ref(),
                "KeyConditionExpression": "pk = :pk",
                "ExpressionAttributeValues": {":pk": {"S": partition}}
            }))
            .unwrap();
            let observed = observe(store.operations.query(query)).await;
            observed.assert_no_warning_or_error();
            let actual = serde_json::to_value(observed.result.unwrap()).unwrap();
            let expected = json!({
                "Items": expected_items, "Count": count, "ScannedCount": count
            });
            let item_keys = actual["Items"]
                .as_array()
                .unwrap()
                .iter()
                .map(|item| item["sk"]["S"].as_str().unwrap())
                .collect::<Vec<_>>();
            assert!(
                actual == expected,
                "{partition}: count={}, scanned={}, keys={item_keys:?}, cursor={}",
                actual["Count"],
                actual["ScannedCount"],
                actual["LastEvaluatedKey"]
            );
        }
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
