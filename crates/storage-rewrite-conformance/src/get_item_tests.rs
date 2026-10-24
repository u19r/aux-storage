//! GetItem business rules that span writes, key normalization, and item
//! decoding.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{CreateTableRequest, DeleteItemRequest, GetItemRequest, PutItemRequest};

use super::test_support::{TestStore, unique_table_name};

// Business rule from legacy SQL: equivalent numeric key forms have one
// customer-visible identity.
#[tokio::test]
async fn given_equivalent_numeric_keys_when_get_item_runs_then_latest_item_or_absence_is_exact() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let create = CreateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "N"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .expect("valid numeric-key table");
        store
            .operations
            .create_table(create)
            .await
            .expect("create numeric-key table");

        for (number, payload) in [("1", "first"), ("1.0", "second")] {
            let put = PutItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Item": {"pk": {"N": number}, "payload": {"S": payload}}
            }))
            .expect("valid numeric-key PutItem");
            store
                .operations
                .put_item(put)
                .await
                .unwrap_or_else(|error| panic!("write numeric-key item with {number}: {error}"));
        }

        let equivalent = GetItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"N": "1E0"}},
            "ConsistentRead": true
        }))
        .expect("valid equivalent-key GetItem");
        let read = observe(store.operations.get_item(equivalent)).await;
        read.assert_no_warning_or_error();
        assert_eq!(
            serde_json::to_value(read.result.expect("read equivalent key"))
                .expect("serialize GetItem"),
            json!({
                "Item": {"pk": {"N": "1.0"}, "payload": {"S": "second"}}
            })
        );

        let delete = DeleteItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"N": "1E0"}}
        }))
        .expect("valid equivalent-key DeleteItem");
        store
            .operations
            .delete_item(delete)
            .await
            .expect("delete equivalent key");

        let original = GetItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"N": "1"}},
            "ConsistentRead": true
        }))
        .expect("valid original-key GetItem");
        let absent = observe(store.operations.get_item(original)).await;
        absent.assert_no_warning_or_error();
        assert_eq!(
            serde_json::to_value(absent.result.expect("read deleted key"))
                .expect("serialize missing GetItem"),
            json!({})
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Boundary and format rules from Cucumber: large indexed values reconstruct
// without metadata.
#[tokio::test]
async fn given_large_indexed_item_when_get_item_runs_then_full_and_projected_images_are_exact() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let create = CreateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "MaxIndexers": 1
        }))
        .expect("valid indexed table");
        store
            .operations
            .create_table(create)
            .await
            .expect("create indexed table");

        let payload = "x".repeat(120_000);
        let item = json!({
            "pk": {"S": "large"},
            "status": {"S": "active"},
            "payload": {"S": payload}
        });
        let put = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": item,
            "Indexers": ["status"]
        }))
        .expect("valid large indexed PutItem");
        store
            .operations
            .put_item(put)
            .await
            .expect("write large indexed item");

        let full = GetItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": "large"}}
        }))
        .expect("valid full GetItem");
        let full = observe(store.operations.get_item(full)).await;
        full.assert_no_warning_or_error();
        assert_eq!(
            serde_json::to_value(full.result.expect("read full item"))
                .expect("serialize full GetItem"),
            json!({"Item": item})
        );

        let projected = GetItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": "large"}},
            "ProjectionExpression": "pk, #status",
            "ExpressionAttributeNames": {"#status": "status"}
        }))
        .expect("valid projected GetItem");
        let projected = observe(store.operations.get_item(projected)).await;
        projected.assert_no_warning_or_error();
        assert_eq!(
            serde_json::to_value(projected.result.expect("read projected item"))
                .expect("serialize projected GetItem"),
            json!({
                "Item": {"pk": {"S": "large"}, "status": {"S": "active"}}
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
