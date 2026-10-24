mod support_tests;

use http_request::reqwest::Client;
use serde_json::{Value, json};
use storage_provider::StorageBackend;
use tempfile::tempdir;

async fn dynamo_request(client: &Client, endpoint: &str, action: &str, body: Value) -> Value {
    let response = client
        .post(endpoint)
        .header("content-type", "application/x-amz-json-1.0")
        .header("x-amz-target", format!("DynamoDB_20120810.{action}"))
        .json(&body)
        .send()
        .await
        .expect("send DynamoDB request");
    let status = response.status();
    let body = response
        .json::<Value>()
        .await
        .expect("decode DynamoDB response");
    assert!(status.is_success(), "{action} failed with {status}: {body}");
    body
}

async fn queue_request(client: &Client, endpoint: &str, action: &str, body: Value) -> Value {
    let response = client
        .post(endpoint)
        .header("content-type", "application/x-amz-json-1.0")
        .header("x-amz-target", format!("AmazonSQS.{action}"))
        .json(&body)
        .send()
        .await
        .expect("send SQS request");
    let status = response.status();
    let body = response.json::<Value>().await.expect("decode SQS response");
    assert!(status.is_success(), "{action} failed with {status}: {body}");
    body
}

#[tokio::test]
async fn given_sqlite_origin_when_remote_fixture_starts_then_ready_and_shutdown_are_bounded() {
    let temp_dir = tempdir().expect("create remote canary fixture directory");
    let database_path = temp_dir.path().join("origin.sqlite");
    let fixture = support_tests::start_fixture(&database_path)
        .await
        .expect("start SQLite origin and Remote provider APIs");
    let remote_config = support_tests::remote_config(&fixture.origin.base_url);
    let remote_registry = support_tests::remote_connection_registry(&fixture.origin.base_url);

    assert!(matches!(remote_config.backend_type, StorageBackend::Remote));
    assert_eq!(
        remote_config
            .remote
            .as_ref()
            .expect("remote settings")
            .endpoint_urls,
        vec![fixture.origin.base_url.clone()]
    );
    assert_eq!(fixture.front_root, fixture.remote.base_url);
    assert_eq!(
        fixture.origin_queue_endpoint,
        format!("{}/queue", fixture.origin.base_url)
    );
    assert_eq!(remote_registry.default_connection_id, "primary");
    assert!(remote_registry.connections.contains_key("primary"));
    assert!(remote_registry.connections.contains_key("secondary"));

    let client = Client::new();
    let response = client
        .get(format!("{}/ready", fixture.remote.storage_url))
        .send()
        .await
        .expect("remote readiness request");
    assert!(response.status().is_success());

    let remote_url = fixture.remote.storage_url.clone();
    drop(fixture);
    assert!(
        client
            .get(format!("{remote_url}/ready"))
            .send()
            .await
            .is_err()
    );
}

#[tokio::test]
async fn remote_fixture_exposes_sqlite_queue_and_rejects_front_queue() {
    let temp_dir = tempdir().expect("create remote canary fixture directory");
    let database_path = temp_dir.path().join("origin.sqlite");
    let fixture = support_tests::start_fixture(&database_path)
        .await
        .expect("start SQLite origin and Remote provider APIs");
    let client = Client::new();

    let queue = queue_request(
        &client,
        &fixture.origin_queue_endpoint,
        "CreateQueue",
        json!({"QueueName": "remote-fixture-queue"}),
    )
    .await;
    let queue_url = queue["QueueUrl"].as_str().expect("queue URL").to_string();
    assert!(queue_url.starts_with(&fixture.origin.base_url));

    queue_request(
        &client,
        &fixture.origin_queue_endpoint,
        "SendMessage",
        json!({"QueueUrl": queue_url, "MessageBody": "origin-message"}),
    )
    .await;
    let received = queue_request(
        &client,
        &fixture.origin_queue_endpoint,
        "ReceiveMessage",
        json!({"QueueUrl": queue_url, "MaxNumberOfMessages": 1}),
    )
    .await;
    assert_eq!(received["Messages"][0]["Body"], "origin-message");

    let front_queue = client
        .post(format!("{}/queue", fixture.front_root))
        .header("content-type", "application/x-amz-json-1.0")
        .header("x-amz-target", "AmazonSQS.ListQueues")
        .json(&json!({}))
        .send()
        .await
        .expect("front queue request");
    assert_eq!(front_queue.status().as_u16(), 404);
}

#[tokio::test]
async fn remote_scan_and_query_resume_with_exact_dynamodb_key_maps() {
    let temp_dir = tempdir().expect("create remote canary fixture directory");
    let database_path = temp_dir.path().join("origin.sqlite");
    let fixture = support_tests::start_fixture(&database_path)
        .await
        .expect("start SQLite origin and Remote provider APIs");
    let client = Client::new();
    let endpoint = format!("{}/storage", fixture.remote.base_url);
    let table_name = "remote-pagination-table";

    dynamo_request(
        &client,
        &endpoint,
        "CreateTable",
        json!({
            "TableName": table_name,
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "BillingMode": "PAY_PER_REQUEST"
        }),
    )
    .await;
    for sort_key in ["001", "002", "003"] {
        dynamo_request(
            &client,
            &endpoint,
            "PutItem",
            json!({
                "TableName": table_name,
                "Item": {
                    "pk": {"S": "tenant#1"},
                    "sk": {"S": sort_key},
                    "payload": {"S": sort_key}
                }
            }),
        )
        .await;
    }

    let origin_item = dynamo_request(
        &client,
        &fixture.origin.storage_url,
        "GetItem",
        json!({
            "TableName": table_name,
            "Key": {"pk": {"S": "tenant#1"}, "sk": {"S": "001"}}
        }),
    )
    .await;
    assert_eq!(
        origin_item["Item"],
        json!({
            "pk": {"S": "tenant#1"},
            "sk": {"S": "001"},
            "payload": {"S": "001"}
        })
    );

    let first = dynamo_request(
        &client,
        &endpoint,
        "Scan",
        json!({"TableName": table_name, "Limit": 1}),
    )
    .await;
    assert_eq!(first["Items"][0]["sk"], json!({"S": "001"}));
    let first_key = first["LastEvaluatedKey"].clone();
    assert_eq!(
        first_key,
        json!({"pk": {"S": "tenant#1"}, "sk": {"S": "001"}})
    );

    let second = dynamo_request(
        &client,
        &endpoint,
        "Scan",
        json!({
            "TableName": table_name,
            "Limit": 1,
            "ExclusiveStartKey": first_key
        }),
    )
    .await;
    assert_eq!(second["Items"][0]["sk"], json!({"S": "002"}));
    assert_eq!(
        second["LastEvaluatedKey"],
        json!({"pk": {"S": "tenant#1"}, "sk": {"S": "002"}})
    );

    let query = dynamo_request(
        &client,
        &endpoint,
        "Query",
        json!({
            "TableName": table_name,
            "KeyConditionExpression": "pk = :pk",
            "ExpressionAttributeValues": {":pk": {"S": "tenant#1"}},
            "Limit": 1
        }),
    )
    .await;
    assert_eq!(query["Items"][0]["sk"], json!({"S": "001"}));
    assert_eq!(
        query["LastEvaluatedKey"],
        json!({"pk": {"S": "tenant#1"}, "sk": {"S": "001"}})
    );
}

async fn exercise_delete_return_values(client: &Client, endpoint: &str, table_name: &str) {
    dynamo_request(
        client,
        endpoint,
        "CreateTable",
        json!({
            "TableName": table_name,
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST"
        }),
    )
    .await;
    let item = json!({"id": {"S": "item#1"}, "status": {"S": "open"}});
    let key = json!({"id": {"S": "item#1"}});
    dynamo_request(
        client,
        endpoint,
        "PutItem",
        json!({"TableName": table_name, "Item": item}),
    )
    .await;

    let omitted = dynamo_request(
        client,
        endpoint,
        "DeleteItem",
        json!({"TableName": table_name, "Key": key}),
    )
    .await;
    assert!(!omitted.get("Attributes").is_some_and(Value::is_object));

    dynamo_request(
        client,
        endpoint,
        "PutItem",
        json!({"TableName": table_name, "Item": item}),
    )
    .await;
    let none = dynamo_request(
        client,
        endpoint,
        "DeleteItem",
        json!({"TableName": table_name, "Key": key, "ReturnValues": "NONE"}),
    )
    .await;
    assert!(!none.get("Attributes").is_some_and(Value::is_object));

    dynamo_request(
        client,
        endpoint,
        "PutItem",
        json!({"TableName": table_name, "Item": item}),
    )
    .await;
    let all_old = dynamo_request(
        client,
        endpoint,
        "DeleteItem",
        json!({"TableName": table_name, "Key": key, "ReturnValues": "ALL_OLD"}),
    )
    .await;
    assert_eq!(all_old["Attributes"], item);
}

#[tokio::test]
async fn remote_delete_return_values_match_sqlite_origin_control() {
    let temp_dir = tempdir().expect("create remote canary fixture directory");
    let database_path = temp_dir.path().join("origin.sqlite");
    let fixture = support_tests::start_fixture(&database_path)
        .await
        .expect("start SQLite origin and Remote provider APIs");
    let client = Client::new();
    exercise_delete_return_values(
        &client,
        &fixture.origin.storage_url,
        "sqlite-delete-return-values",
    )
    .await;
    exercise_delete_return_values(
        &client,
        &fixture.remote.storage_url,
        "remote-delete-return-values",
    )
    .await;
}
