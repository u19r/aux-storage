//! Scan filter and pagination rules at the replacement Dynamo operation
//! boundary.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{KeyAttributeType, PutItemRequest, ScanRequest};

use super::test_support::{TestStore, unique_table_name};

// Business and boundary rules from legacy Scan pagination and Cucumber: Limit
// counts evaluated items, and a filtered empty page retains its continuation.
#[tokio::test]
async fn given_filtered_scan_when_pages_are_followed_then_matches_survive_an_empty_page() {
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
            store.operations.put_item(put).await.expect("seed scan row");
        }

        let request = ScanRequest::try_from(json!({
            "TableName": table.as_ref(),
            "FilterExpression": "#status = :active",
            "ExpressionAttributeNames": {"#status": "status"},
            "ExpressionAttributeValues": {":active": {"S": "active"}},
            "Limit": 1,
            "ConsistentRead": true
        }))
        .expect("valid filtered Scan");

        let first = observe(store.operations.scan(request.clone())).await;
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
        let second = observe(store.operations.scan(next)).await;
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
        let third = observe(store.operations.scan(last)).await;
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
