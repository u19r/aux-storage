//! Customer-visible UpdateItem results across indexer declaration transitions.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{
    CreateTableRequest, GetItemRequest, PutItemRequest, QueryRequest, UpdateItemRequest,
};

use super::test_support::{TestStore, unique_table_name};

// Business rule from Cucumber: declaration changes do not alter GSI membership;
// GSI membership follows the item's key attributes.
#[tokio::test]
async fn given_indexed_item_when_update_item_changes_declarations_then_queries_follow_each_transition()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let create = CreateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "status", "AttributeType": "S"},
                {"AttributeName": "region", "AttributeType": "S"}
            ],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "MaxIndexers": 2,
            "GlobalSecondaryIndexes": [
                {
                    "IndexName": "ByStatus",
                    "KeySchema": [{"AttributeName": "status", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"}
                },
                {
                    "IndexName": "ByRegion",
                    "KeySchema": [{"AttributeName": "region", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"}
                }
            ]
        }))
        .expect("valid indexed table");
        store
            .operations
            .create_table(create)
            .await
            .expect("create indexed table");

        let put = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": {"pk": {"S": "two"}, "status": {"S": "pending"}},
            "Indexers": ["status"]
        }))
        .expect("valid indexed PutItem");
        store
            .operations
            .put_item(put)
            .await
            .expect("seed indexed item");

        let preserve = UpdateItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": "two"}},
            "UpdateExpression": "SET #status = :status",
            "ExpressionAttributeNames": {"#status": "status"},
            "ExpressionAttributeValues": {":status": {"S": "complete"}},
            "ReturnValues": "ALL_NEW"
        }))
        .expect("valid preserving UpdateItem");
        let preserve = observe(store.operations.update_item(preserve)).await;
        preserve.assert_no_warning_or_error();
        assert_eq!(
            serde_json::to_value(preserve.result.expect("preserve indexers"))
                .expect("serialize preserving response"),
            json!({"Attributes": {
                "pk": {"S": "two"}, "status": {"S": "complete"}
            }})
        );
        let by_status = QueryRequest::try_from(json!({
            "TableName": table.as_ref(),
            "IndexName": "ByStatus",
            "KeyConditionExpression": "#status = :status",
            "ExpressionAttributeNames": {"#status": "status"},
            "ExpressionAttributeValues": {":status": {"S": "complete"}}
        }))
        .expect("valid status query");
        let by_status = store
            .operations
            .query(by_status)
            .await
            .expect("query preserved indexer");
        assert_eq!(
            serde_json::to_value(by_status).expect("serialize status query"),
            json!({
                "Items": [{"pk": {"S": "two"}, "status": {"S": "complete"}}],
                "Count": 1,
                "ScannedCount": 1
            })
        );

        let replace = UpdateItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": "two"}},
            "UpdateExpression": "SET #region = :region",
            "ExpressionAttributeNames": {"#region": "region"},
            "ExpressionAttributeValues": {":region": {"S": "eu"}},
            "Indexers": ["region", "status"],
            "ReturnValues": "ALL_NEW"
        }))
        .expect("valid replacing UpdateItem");
        let replace = observe(store.operations.update_item(replace)).await;
        replace.assert_no_warning_or_error();
        assert_eq!(
            serde_json::to_value(replace.result.expect("replace indexers"))
                .expect("serialize replacing response"),
            json!({"Attributes": {
                "pk": {"S": "two"}, "status": {"S": "complete"}, "region": {"S": "eu"}
            }})
        );
        let by_region = QueryRequest::try_from(json!({
            "TableName": table.as_ref(),
            "IndexName": "ByRegion",
            "KeyConditionExpression": "#region = :region",
            "ExpressionAttributeNames": {"#region": "region"},
            "ExpressionAttributeValues": {":region": {"S": "eu"}}
        }))
        .expect("valid region query");
        let by_region = store
            .operations
            .query(by_region)
            .await
            .expect("query new indexer");
        assert_eq!(
            serde_json::to_value(by_region).expect("serialize region query"),
            json!({
                "Items": [{"pk": {"S": "two"}, "status": {"S": "complete"}, "region": {"S": "eu"}}],
                "Count": 1,
                "ScannedCount": 1
            })
        );

        let clear = UpdateItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": "two"}},
            "UpdateExpression": "REMOVE #status",
            "ExpressionAttributeNames": {"#status": "status"},
            "Indexers": [],
            "ReturnValues": "ALL_NEW"
        }))
        .expect("valid clearing UpdateItem");
        let clear = observe(store.operations.update_item(clear)).await;
        clear.assert_no_warning_or_error();
        assert_eq!(
            serde_json::to_value(clear.result.expect("clear indexers"))
                .expect("serialize clearing response"),
            json!({"Attributes": {
                "pk": {"S": "two"}, "region": {"S": "eu"}
            }})
        );
        let get = GetItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": "two"}}
        }))
        .expect("valid GetItem");
        let get = store
            .operations
            .get_item(get)
            .await
            .expect("read cleared item");
        assert_eq!(
            serde_json::to_value(get).expect("serialize cleared item"),
            json!({
                "Item": {"pk": {"S": "two"}, "region": {"S": "eu"}}
            })
        );
        let by_region = QueryRequest::try_from(json!({
            "TableName": table.as_ref(),
            "IndexName": "ByRegion",
            "KeyConditionExpression": "#region = :region",
            "ExpressionAttributeNames": {"#region": "region"},
            "ExpressionAttributeValues": {":region": {"S": "eu"}}
        }))
        .expect("valid cleared region query");
        let by_region = store
            .operations
            .query(by_region)
            .await
            .expect("query after clearing declarations");
        assert_eq!(
            serde_json::to_value(by_region).expect("serialize region query"),
            json!({
                "Items": [{"pk": {"S": "two"}, "region": {"S": "eu"}}],
                "Count": 1,
                "ScannedCount": 1
            })
        );
        let by_status = QueryRequest::try_from(json!({
            "TableName": table.as_ref(),
            "IndexName": "ByStatus",
            "KeyConditionExpression": "#status = :status",
            "ExpressionAttributeNames": {"#status": "status"},
            "ExpressionAttributeValues": {":status": {"S": "complete"}}
        }))
        .expect("valid removed status query");
        let by_status = store
            .operations
            .query(by_status)
            .await
            .expect("query removed status");
        assert_eq!(
            serde_json::to_value(by_status).expect("serialize empty status query"),
            json!({
                "Items": [], "Count": 0, "ScannedCount": 0
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
