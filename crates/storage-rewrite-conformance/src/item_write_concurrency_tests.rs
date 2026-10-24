//! Mixed ordinary and transactional writes preserve one item history.

use std::sync::Arc;

use serde_json::json;
use storage_types::{AttributeValue, GetItemRequest, TransactWriteItemsRequest, UpdateItemRequest};
use tokio::sync::Barrier;

use crate::test_support::{TestStore, hash_table_request, unique_table_name};

#[tokio::test]
async fn given_mixed_item_writers_when_targets_overlap_then_increments_and_image_chain_are_complete()
 {
    let store = TestStore::new().await;
    let table = unique_table_name();
    let mut create = hash_table_request(table.clone());
    create.stream_specification = Some(storage_types::StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(storage_types::StreamViewType::NewAndOldImages),
    });
    store.operations.create_table(create).await.unwrap();
    let barrier = Arc::new(Barrier::new(24));
    let mut writers = Vec::new();
    for writer in 0..24 {
        let operations = Arc::clone(&store.operations);
        let barrier = Arc::clone(&barrier);
        let table = table.clone();
        writers.push(tokio::spawn(async move {
            let update = |key| {
                json!({
                    "TableName": table.as_ref(), "Key": {"pk": {"S": key}},
                    "UpdateExpression": "ADD #count :one",
                    "ExpressionAttributeNames": {"#count": "count"},
                    "ExpressionAttributeValues": {":one": {"N": "1"}}
                })
            };
            barrier.wait().await;
            if writer % 2 == 0 {
                operations
                    .update_item(UpdateItemRequest::try_from(update("shared")).unwrap())
                    .await
                    .map(|_| ())
            } else {
                let targets = if writer % 4 == 1 {
                    ["shared", "peer"]
                } else {
                    ["peer", "shared"]
                };
                let request = TransactWriteItemsRequest::try_from(json!({
                    "TransactItems": targets.map(|key| json!({"Update": update(key)}))
                }))
                .unwrap();
                operations.transact_write_items(request).await.map(|_| ())
            }
        }));
    }
    for writer in writers {
        writer.await.unwrap().expect("every mixed writer commits");
    }
    let records = crate::image_retention_tests::table_records(&store, &table)
        .await
        .unwrap();
    assert_eq!(
        records.len(),
        36,
        "every committed item change has a stream record"
    );
    for (key, total) in [("shared", 24), ("peer", 12)] {
        let item = store
            .operations
            .get_item(GetItemRequest::new(
                table.clone(),
                [("pk".to_owned(), AttributeValue::S(key.to_owned()))],
            ))
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(item).unwrap()["Item"]["count"],
            json!({"N": total.to_string()})
        );
        for count in 1..=total {
            let matching = records
                .iter()
                .filter(|record| {
                    let image = record.new_image.as_ref().unwrap();
                    image.get("pk") == Some(&AttributeValue::S(key.to_owned()))
                        && image.get("count") == Some(&AttributeValue::N(count.to_string()))
                })
                .collect::<Vec<_>>();
            assert_eq!(matching.len(), 1, "one record for {key} increment {count}");
            if count == 1 {
                assert!(matching[0].old_image.is_none());
            } else {
                assert_eq!(
                    matching[0].old_image.as_ref().unwrap().get("count"),
                    Some(&AttributeValue::N((count - 1).to_string()))
                );
            }
        }
    }
    store.finish().await;
}

#[tokio::test]
async fn given_missing_item_when_ordinary_and_transactional_conditions_race_then_one_write_commits()
{
    let store = TestStore::new().await;
    let table = unique_table_name();
    let mut create = hash_table_request(table.clone());
    create.stream_specification = Some(storage_types::StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(storage_types::StreamViewType::NewAndOldImages),
    });
    store.operations.create_table(create).await.unwrap();
    let put = json!({
        "TableName": table.as_ref(), "Item": {"pk": {"S": "missing"}},
        "ConditionExpression": "attribute_not_exists(pk)"
    });
    let ordinary = store
        .operations
        .put_item(storage_types::PutItemRequest::try_from(put.clone()).unwrap());
    let transaction = store.operations.transact_write_items(
        TransactWriteItemsRequest::try_from(json!({"TransactItems": [{"Put": put}]})).unwrap(),
    );
    let (ordinary, transaction) = tokio::join!(ordinary, transaction);
    assert_eq!(
        usize::from(ordinary.is_ok()) + usize::from(transaction.is_ok()),
        1
    );
    if let Err(error) = ordinary {
        crate::test_support::assert_dynamo_error_response_exact(
            error,
            axum::http::StatusCode::BAD_REQUEST,
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"The conditional request failed"}"#,
        );
    }
    if let Err(error) = transaction {
        use storage_types::context::WrappedError as _;
        assert!(matches!(
            error.to_enum(),
            storage_types::StorageEnum::TransactionCanceled { .. }
        ));
    }
    let records = crate::image_retention_tests::table_records(&store, &table)
        .await
        .unwrap();
    assert_eq!(records.len(), 1, "failed condition has no stream effects");
    assert!(records[0].old_image.is_none());
    let item = store
        .operations
        .get_item(GetItemRequest::new(
            table,
            [("pk".to_owned(), AttributeValue::S("missing".to_owned()))],
        ))
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(item).unwrap(),
        json!({"Item": {"pk": {"S": "missing"}}})
    );
    store.finish().await;
}
