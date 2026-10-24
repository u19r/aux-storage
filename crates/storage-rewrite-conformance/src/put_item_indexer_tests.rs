//! Ordered indexer declarations at the replacement PutItem boundary.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{CreateTableRequest, GetItemRequest, PutItemRequest, QueryRequest, StreamName};
use stream_provider::StreamRowKind;

use super::test_support::{TestStore, assert_dynamo_error_response_exact, unique_table_name};

#[tokio::test]
async fn given_lookup_attributes_when_put_and_get_run_then_saved_wire_order_is_retained()
-> anyhow::Result<()> {
    let store = TestStore::new().await;
    let outcome = async {
        let table = unique_table_name();
        let mut create = super::test_support::hash_table_request(table.clone());
        create.max_indexers = storage_types::MaxIndexers::try_new(2)?;
        store.operations.create_table(create).await?;
        let bytes = br#"{"pk":{"S":"one"},"sk":{"S":"sort"},"gsi1pk":{"S":"group"},"gsi1sk":{"S":"rank"},"e_at":{"N":"4102444800"},"z_first":{"S":"first"},"a_second":{"S":"second"},"payload":{"S":"value"}}"#;
        let mut put = PutItemRequest::new(table.clone(), storage_types::WireItem::dynamo_json(bytes.to_vec()));
        put.indexers = Some(vec!["z_first".to_owned(), "a_second".to_owned()]);
        store.operations.put_item(put).await?;
        let get = GetItemRequest::try_from(json!({
            "TableName": table.as_ref(), "Key": {"pk": {"S": "one"}}, "ConsistentRead": true
        })).map_err(anyhow::Error::msg)?;
        let item = store.operations.get_item(get).await?.item
            .ok_or_else(|| anyhow::anyhow!("saved item missing"))?;
        let attributes = storage_types::AttributeMap::from_dynamo_json(&item.dynamo_json_bytes()?)?;
        anyhow::ensure!(attributes.keys().collect::<Vec<_>>() ==
            ["pk", "sk", "gsi1pk", "gsi1sk", "e_at", "z_first", "a_second", "payload"],
            "saved lookup order changed");
        anyhow::ensure!(item.dynamo_json_bytes()?.as_ref() == bytes, "saved values changed");
        Ok(())
    }.await;
    store.finish().await;
    outcome
}

// Business and format rules: an absent optional indexer slot does not change
// the item image.
#[tokio::test]
async fn given_missing_optional_indexer_when_put_item_runs_then_item_is_readable_without_metadata()
{
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let create = CreateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "status", "AttributeType": "S"}
            ],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "MaxIndexers": 2,
            "GlobalSecondaryIndexes": [{
                "IndexName": "ByStatus",
                "KeySchema": [{"AttributeName": "status", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }]
        }))
        .unwrap();
        store.operations.create_table(create).await.unwrap();

        let item = json!({"pk": {"S": "one"}, "status": {"S": "active"}, "value": {"N": "7"}});
        let request = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": item,
            "Indexers": ["status", "optional"]
        }))
        .unwrap();
        let written = observe(store.operations.put_item(request)).await;
        written.assert_no_warning_or_error();
        assert_eq!(
            serde_json::to_value(written.result.unwrap()).unwrap(),
            json!({})
        );

        let get = GetItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": "one"}}
        }))
        .unwrap();
        let read = store.operations.get_item(get).await.unwrap();
        assert_eq!(
            serde_json::to_value(read).unwrap(),
            json!({"Item": {
                "pk": {"S": "one"}, "status": {"S": "active"}, "value": {"N": "7"}
            }})
        );

        let query = QueryRequest::try_from(json!({
            "TableName": table.as_ref(),
            "IndexName": "ByStatus",
            "KeyConditionExpression": "#status = :status",
            "ExpressionAttributeNames": {"#status": "status"},
            "ExpressionAttributeValues": {":status": {"S": "active"}}
        }))
        .unwrap();
        let indexed = store.operations.query(query).await.unwrap();
        assert_eq!(
            serde_json::to_value(indexed).unwrap(),
            json!({
                "Items": [{"pk": {"S": "one"}, "status": {"S": "active"}, "value": {"N": "7"}}],
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

// Negative rules from the canary: every invalid declaration rejects atomically.
#[tokio::test]
async fn given_invalid_indexers_when_put_item_runs_then_exact_errors_and_no_items_or_cdc_events() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let mut create = super::test_support::hash_table_request(table.clone());
        create.max_indexers = storage_types::MaxIndexers::try_from(2).unwrap();
        create.stream_specification = Some(storage_types::StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(storage_types::StreamViewType::NewAndOldImages),
        });
        store.operations.create_table(create).await.unwrap();

        for line in include_str!("../fixtures/indexer_invalid_cases.jsonl").lines() {
            let case: serde_json::Value = serde_json::from_str(line).unwrap();
            let name = case["name"].as_str().unwrap();
            let key = format!("rejected-{name}");
            let mut item = json!({"pk": {"S": key}});
            if !case["indexed_value"].is_null() {
                item.as_object_mut()
                    .unwrap()
                    .insert("indexed".to_owned(), case["indexed_value"].clone());
            }
            let request = PutItemRequest::try_from(json!({
                "TableName": table.as_ref(), "Item": item, "Indexers": case["indexers"]
            }))
            .unwrap();
            let rejected = observe(store.operations.put_item(request)).await;
            rejected.assert_no_warning_or_error();
            let message = case["message"].as_str().unwrap();
            let expected = format!(
                "{{\"__type\":\"com.amazon.coral.validate#ValidationException\",\"message\":\"\
                 {message}\"}}"
            );
            assert_dynamo_error_response_exact(
                rejected.result.unwrap_err(),
                axum::http::StatusCode::BAD_REQUEST,
                &expected,
            );
            let get = GetItemRequest::try_from(json!({
                "TableName": table.as_ref(), "Key": {"pk": {"S": key}}
            }))
            .unwrap();
            let read = store.operations.get_item(get).await.unwrap();
            assert_eq!(serde_json::to_value(read).unwrap(), json!({}), "{name}");
        }

        let cdc = store
            .stream_operations
            .read_forward(
                StreamRowKind::System(StreamName::system_table_stream()),
                None,
                10,
            )
            .await
            .unwrap();
        assert!(cdc.items.is_empty(), "rejected PutItem must not append CDC");
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
