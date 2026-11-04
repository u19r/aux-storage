//! DynamoDB event identity is independent of the configured image projection.

use std::sync::Arc;

use serde_json::{Value, json};

use super::dynamodb_tests::execute_dynamodb_json_body;
use crate::{routes::routes_tests::default_conformance_backends, types::AppState};

#[tokio::test]
async fn given_all_stream_views_when_item_changes_then_event_names_keys_and_images_are_exact() {
    for backend in default_conformance_backends() {
        for view in ["KEYS_ONLY", "NEW_IMAGE", "OLD_IMAGE", "NEW_AND_OLD_IMAGES"] {
            let state = Arc::new(AppState::new(backend.create_db().await));
            let table = execute_dynamodb_json_body(
                state.clone(),
                "DynamoDB_20120810.CreateTable",
                json!({
                    "TableName": "StreamViews",
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "StreamSpecification": {"StreamEnabled": true, "StreamViewType": view}
                }),
            )
            .await;
            let arn = table["TableDescription"]["LatestStreamArn"]
                .as_str()
                .unwrap();
            write_mutations(state.clone()).await;
            verify_snapshot_extension(state.clone()).await;
            let records = read_records(state, arn).await;
            assert_eq!(records.len(), 3, "{view}");
            for (ordinal, name) in ["INSERT", "MODIFY", "REMOVE"].into_iter().enumerate() {
                let record = &records[ordinal];
                assert_eq!(record["eventName"], name, "{view} event {ordinal}");
                assert_eq!(record["dynamodb"]["Keys"], json!({"pk": {"S": "key"}}));
                assert_eq!(record["dynamodb"]["StreamViewType"], view);
                verify_images(&record["dynamodb"], view, ordinal);
            }
        }
    }
}

async fn verify_snapshot_extension(state: Arc<AppState>) {
    let response = execute_dynamodb_json_body(
        state,
        "DynamoDB_20120810.GetStreamRecords",
        json!({"TableName": "StreamViews"}),
    )
    .await;
    let records = response["Records"].as_array().unwrap();
    assert_eq!(records.len(), 3);
    for (record, value) in records.iter().zip([Some("a"), Some("b"), None]) {
        let expected = value.map(|value| json!({"pk": {"S": "key"}, "value": {"S": value}}));
        assert_eq!(record["Item"], serde_json::to_value(expected).unwrap());
        assert_eq!(record["Keys"], json!({"pk": {"S": "key"}}));
        assert!(record.get("OldImage").is_none());
        assert!(record.get("NewImage").is_none());
    }
}

async fn write_mutations(state: Arc<AppState>) {
    execute_dynamodb_json_body(
        state.clone(),
        "DynamoDB_20120810.PutItem",
        json!({
            "TableName": "StreamViews", "Item": {"pk": {"S": "key"}, "value": {"S": "a"}}
        }),
    )
    .await;
    execute_dynamodb_json_body(
        state.clone(),
        "DynamoDB_20120810.UpdateItem",
        json!({
            "TableName": "StreamViews", "Key": {"pk": {"S": "key"}},
            "UpdateExpression": "SET #value = :new",
            "ExpressionAttributeNames": {"#value": "value"},
            "ExpressionAttributeValues": {":new": {"S": "b"}}
        }),
    )
    .await;
    execute_dynamodb_json_body(
        state,
        "DynamoDB_20120810.DeleteItem",
        json!({
            "TableName": "StreamViews", "Key": {"pk": {"S": "key"}}
        }),
    )
    .await;
}

async fn read_records(state: Arc<AppState>, arn: &str) -> Vec<Value> {
    let described = execute_dynamodb_json_body(
        state.clone(),
        "DynamoDBStreams_20120810.DescribeStream",
        json!({"StreamArn": arn}),
    )
    .await;
    let mut records = Vec::new();
    for shard in described["StreamDescription"]["Shards"].as_array().unwrap() {
        let iterator = execute_dynamodb_json_body(
            state.clone(), "DynamoDBStreams_20120810.GetShardIterator",
            json!({"StreamArn": arn, "ShardId": shard["ShardId"], "ShardIteratorType": "TRIM_HORIZON"}),
        ).await;
        let mut next = iterator["ShardIterator"].clone();
        for page_number in 0..5 {
            let page = execute_dynamodb_json_body(
                state.clone(),
                "DynamoDBStreams_20120810.GetRecords",
                json!({"ShardIterator": next, "Limit": 1}),
            )
            .await;
            let rows = page["Records"].as_array().unwrap();
            assert!(rows.len() <= 1);
            assert!(
                page["NextShardIterator"].is_string(),
                "active shard must remain pollable"
            );
            next = page["NextShardIterator"].clone();
            records.extend(rows.iter().cloned());
            if page_number >= 3 {
                assert!(rows.is_empty(), "no duplicate stream events");
            }
        }
    }
    records
}

fn verify_images(record: &Value, view: &str, ordinal: usize) {
    let old = (view == "OLD_IMAGE" || view == "NEW_AND_OLD_IMAGES")
        .then(|| [None, Some("a"), Some("b")][ordinal])
        .flatten();
    let new = (view == "NEW_IMAGE" || view == "NEW_AND_OLD_IMAGES")
        .then(|| [Some("a"), Some("b"), None][ordinal])
        .flatten();
    for (field, value) in [("OldImage", old), ("NewImage", new)] {
        let expected = value.map(|value| json!({"pk": {"S": "key"}, "value": {"S": value}}));
        assert_eq!(
            record.get(field),
            expected.as_ref(),
            "{view} {field} event {ordinal}"
        );
    }
}
