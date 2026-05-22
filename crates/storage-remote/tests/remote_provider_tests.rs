use std::{collections::HashMap, sync::atomic::Ordering};

use httpmock::prelude::*;
use storage_provider::{RemoteCredentialStrategy, RemoteStorageSettings, StorageProvider};
use storage_remote::{MAX_ENDPOINT_RETRIES, RemoteStorageProvider};
use storage_sync::{SYNC_LEADER_HINT_HEADER, SYNC_NOT_LEADER_ERROR_TYPE};
use storage_types::{
    AttributeDefinition, AttributeValue, CreateTableRequest, KeyAttributeType, KeySchemaElement,
    KeyType, StorageError, TableName,
};

fn table_def() -> (TableName, Vec<AttributeDefinition>, Vec<KeySchemaElement>) {
    table_def_with_name("remote-test-table")
}

fn table_def_with_name(name: &str) -> (TableName, Vec<AttributeDefinition>, Vec<KeySchemaElement>) {
    let name = TableName::new(name);
    let attributes = vec![
        AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        },
        AttributeDefinition {
            attribute_name: "sk".to_string(),
            attribute_type: KeyAttributeType::S,
        },
    ];
    let keys = vec![
        KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        },
        KeySchemaElement {
            attribute_name: "sk".to_string(),
            key_type: KeyType::Range,
        },
    ];
    (name, attributes, keys)
}

async fn provider_for(server: &MockServer) -> RemoteStorageProvider {
    provider_with_endpoints(vec![server.url("/")]).await
}

async fn provider_with_endpoints(endpoints: Vec<String>) -> RemoteStorageProvider {
    RemoteStorageProvider::new(RemoteStorageSettings {
        endpoint_urls: endpoints,
        region: None,
        tls: false,
        credentials: RemoteCredentialStrategy::DefaultChain,
        timeouts: None,
    })
    .await
    .expect("remote provider")
}

#[tokio::test]
async fn create_table_issues_remote_call() {
    let server = MockServer::start_async().await;
    let provider = provider_for(&server).await;

    let (table_name, attributes, key_schema) = table_def();

    server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.CreateTable")
                .path("/");
            then.status(200).json_body(serde_json::json!({
                "TableDescription": {
                    "TableName": table_name.as_ref(),
                    "TableStatus": "ACTIVE",
                    "CreationDateTime": 0,
                    "AttributeDefinitions": attributes,
                    "KeySchema": key_schema,
                    "TableArn": "arn:aws:dynamodb:local:table/remote-test-table",
                    "BillingModeSummary": {
                        "BillingMode": "PAY_PER_REQUEST"
                    },
                    "ItemCount": 0,
                    "TableSizeBytes": 0
                }
            }));
        })
        .await;

    let request = CreateTableRequest::new(
        table_name,
        attributes,
        key_schema,
        storage_types::BillingMode::PayPerRequest,
    );

    provider
        .create_table(&request)
        .await
        .expect("table created");
}

#[tokio::test]
async fn create_table_enables_controls_for_managed_tables() {
    let server = MockServer::start_async().await;
    let provider = provider_for(&server).await;

    let (table_name, attributes, key_schema) = table_def_with_name("sys");
    let table_arn = format!("arn:aws:dynamodb:local:table/{}", table_name.as_ref());

    let create_attributes = attributes.clone();
    let create_key_schema = key_schema.clone();
    let create_mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.CreateTable")
                .path("/");
            then.status(200).json_body(serde_json::json!({
                "TableDescription": {
                    "TableName": table_name.as_ref(),
                    "TableStatus": "ACTIVE",
                    "CreationDateTime": 0,
                    "AttributeDefinitions": create_attributes,
                    "KeySchema": create_key_schema,
                    "TableArn": table_arn,
                    "BillingModeSummary": {
                        "BillingMode": "PAY_PER_REQUEST"
                    },
                    "ItemCount": 0,
                    "TableSizeBytes": 0
                }
            }));
        })
        .await;

    let describe_mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.DescribeTable")
                .body_includes("\"TableName\":\"sys\"")
                .path("/");
            then.status(200).json_body(serde_json::json!({
                "Table": {
                    "TableName": "sys",
                    "TableStatus": "ACTIVE",
                    "CreationDateTime": 0,
                    "AttributeDefinitions": [],
                    "KeySchema": [],
                    "TableArn": "arn:aws:dynamodb:local:table/sys",
                    "BillingModeSummary": {
                        "BillingMode": "PAY_PER_REQUEST"
                    },
                    "ItemCount": 0,
                    "TableSizeBytes": 0
                }
            }));
        })
        .await;

    let pitr_mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.UpdateContinuousBackups")
                .body_includes("\"TableName\":\"sys\"")
                .body_includes("\"PointInTimeRecoveryEnabled\":true")
                .path("/");
            then.status(200).json_body(serde_json::json!({}));
        })
        .await;

    let deletion_protection_mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.UpdateTable")
                .body_includes("\"TableName\":\"sys\"")
                .body_includes("\"DeletionProtectionEnabled\":true")
                .path("/");
            then.status(200).json_body(serde_json::json!({
                "TableDescription": {
                    "TableName": "sys",
                    "TableStatus": "ACTIVE",
                    "CreationDateTime": 0,
                    "AttributeDefinitions": [],
                    "KeySchema": [],
                    "TableArn": "arn:aws:dynamodb:local:table/sys",
                    "BillingModeSummary": {
                        "BillingMode": "PAY_PER_REQUEST"
                    },
                    "ItemCount": 0,
                    "TableSizeBytes": 0
                }
            }));
        })
        .await;

    let ttl_mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.UpdateTimeToLive")
                .body_includes("\"TableName\":\"sys\"")
                .body_includes("\"AttributeName\":\"ttl\"")
                .body_includes("\"Enabled\":true")
                .path("/");
            then.status(200).json_body(serde_json::json!({
                "TimeToLiveSpecification": {
                    "AttributeName": "ttl",
                    "Enabled": true
                }
            }));
        })
        .await;

    let request = CreateTableRequest::new(
        table_name,
        attributes,
        key_schema,
        storage_types::BillingMode::PayPerRequest,
    );

    provider
        .create_table(&request)
        .await
        .expect("table created");

    create_mock.assert_calls_async(1).await;
    describe_mock.assert_calls_async(2).await;
    pitr_mock.assert_calls_async(1).await;
    deletion_protection_mock.assert_calls_async(1).await;
    ttl_mock.assert_calls_async(1).await;
}

#[tokio::test]
async fn create_table_skips_managed_controls_when_auxfn_storage_surface_does_not_support_them() {
    let server = MockServer::start_async().await;
    let provider = provider_for(&server).await;

    let (table_name, attributes, key_schema) = table_def_with_name("sys");
    let table_arn = format!("arn:aws:dynamodb:local:table/{}", table_name.as_ref());

    let create_mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.CreateTable")
                .path("/");
            then.status(200).json_body(serde_json::json!({
                "TableDescription": {
                    "TableName": "sys",
                    "TableStatus": "ACTIVE",
                    "CreationDateTime": 0,
                    "AttributeDefinitions": [],
                    "KeySchema": [],
                    "TableArn": table_arn,
                    "BillingModeSummary": {
                        "BillingMode": "PAY_PER_REQUEST"
                    },
                    "ItemCount": 0,
                    "TableSizeBytes": 0
                }
            }));
        })
        .await;

    let describe_mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.DescribeTable")
                .path("/");
            then.status(200).json_body(serde_json::json!({
                "Table": {
                    "TableName": "sys",
                    "TableStatus": "ACTIVE",
                    "CreationDateTime": 0,
                    "AttributeDefinitions": [],
                    "KeySchema": [],
                    "TableArn": "arn:aws:dynamodb:local:table/sys",
                    "BillingModeSummary": {
                        "BillingMode": "PAY_PER_REQUEST"
                    },
                    "ItemCount": 0,
                    "TableSizeBytes": 0
                }
            }));
        })
        .await;

    let pitr_mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.UpdateContinuousBackups")
                .path("/");
            then.status(400).json_body(serde_json::json!({
                "__type": "ValidationException",
                "message": "UpdateContinuousBackups is not yet supported on the AuxFn storage compatibility surface"
            }));
        })
        .await;

    provider
        .create_table(&CreateTableRequest::new(
            table_name,
            attributes,
            key_schema,
            storage_types::BillingMode::PayPerRequest,
        ))
        .await
        .expect("table created with controls skipped");

    create_mock.assert_calls_async(1).await;
    describe_mock.assert_calls_async(1).await;
    pitr_mock.assert_calls_async(1).await;
}

#[tokio::test]
async fn put_and_get_round_trip() {
    let server = MockServer::start_async().await;
    let provider = provider_for(&server).await;

    server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.PutItem")
                .path("/");
            then.status(200).json_body(serde_json::json!({}));
        })
        .await;

    server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.GetItem")
                .path("/");
            then.status(200).json_body(serde_json::json!({
                "Item": {
                    "pk": {"S": "T#1"},
                    "sk": {"S": "U#1"}
                }
            }));
        })
        .await;

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("T#1".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("U#1".to_string()));

    provider
        .put_item(
            TableName::new("remote"),
            item.clone(),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("put");

    let mut key = HashMap::new();
    key.insert("pk".to_string(), AttributeValue::S("T#1".to_string()));
    key.insert("sk".to_string(), AttributeValue::S("U#1".to_string()));

    let fetched = provider
        .get_item(TableName::new("remote"), key.into(), true)
        .await
        .expect("get")
        .expect("present")
        .into_attribute_map()
        .expect("decode wire item");
    assert_eq!(fetched, item);
}

#[tokio::test]
async fn failover_retries_next_endpoint_on_server_error() {
    let failing = MockServer::start_async().await;
    let succeeding = MockServer::start_async().await;
    let provider = provider_with_endpoints(vec![failing.url("/"), succeeding.url("/")]).await;

    let table_name = TableName::new("remote-test-table");

    let failing_mock = failing
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.DescribeTable")
                .path("/");
            then.status(500).json_body(serde_json::json!({
                "__type": "InternalServerError",
                "message": "simulated failure"
            }));
        })
        .await;

    let succeeding_mock = succeeding
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.DescribeTable")
                .path("/");
            then.status(200).json_body(serde_json::json!({
                "Table": {
                    "TableName": table_name.as_ref(),
                    "TableStatus": "ACTIVE",
                    "CreationDateTime": 0,
                    "AttributeDefinitions": [],
                    "KeySchema": [],
                    "TableArn": "arn:aws:dynamodb:local:table/remote-test-table",
                    "BillingModeSummary": {
                        "BillingMode": "PAY_PER_REQUEST"
                    },
                    "ItemCount": 0,
                    "TableSizeBytes": 0
                }
            }));
        })
        .await;

    let exists = provider
        .table_exists(&table_name)
        .await
        .expect("table exists");
    assert!(exists);

    failing_mock.assert_calls_async(3).await;
    succeeding_mock.assert_calls_async(1).await;
}

#[tokio::test]
async fn validation_error_does_not_trigger_failover() {
    let first = MockServer::start_async().await;
    let second = MockServer::start_async().await;
    let provider = provider_with_endpoints(vec![first.url("/"), second.url("/")]).await;

    let table_name = TableName::new("remote-test-table");

    let validation_mock = first
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.DescribeTable")
                .path("/");
            then.status(400).json_body(serde_json::json!({
                "__type": "ValidationException",
                "message": "bad request"
            }));
        })
        .await;

    let succeeding_mock = second
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.DescribeTable")
                .path("/");
            then.status(200).json_body(serde_json::json!({
                "Table": {
                    "TableName": table_name.as_ref(),
                    "TableStatus": "ACTIVE",
                    "CreationDateTime": 0,
                    "AttributeDefinitions": [],
                    "KeySchema": [],
                    "TableArn": "arn:aws:dynamodb:local:table/remote-test-table",
                    "BillingModeSummary": {
                        "BillingMode": "PAY_PER_REQUEST"
                    },
                    "ItemCount": 0,
                    "TableSizeBytes": 0
                }
            }));
        })
        .await;

    let error = provider
        .table_exists(&table_name)
        .await
        .expect_err("validation failure");
    assert!(matches!(error, StorageError::Base(_)));

    validation_mock.assert_calls_async(1).await;
    succeeding_mock.assert_calls_async(0).await;
}

#[tokio::test]
async fn not_leader_hint_promotes_leader_endpoint_and_retries() {
    let follower = MockServer::start_async().await;
    let leader = MockServer::start_async().await;
    let leader_endpoint = leader.url("/");
    let leader_hint = leader_endpoint.trim_end_matches('/').to_string();
    let provider = provider_with_endpoints(vec![follower.url("/"), leader_endpoint]).await;

    let table_name = TableName::new("remote-test-table");

    let follower_mock = follower
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.DescribeTable")
                .path("/");
            then.status(500)
                .header(SYNC_LEADER_HINT_HEADER, leader_hint.as_str())
                .json_body(serde_json::json!({
                    "__type": SYNC_NOT_LEADER_ERROR_TYPE,
                    "message": "retry against the current leader"
                }));
        })
        .await;

    let leader_mock = leader
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.DescribeTable")
                .path("/");
            then.status(200).json_body(serde_json::json!({
                "Table": {
                    "TableName": table_name.as_ref(),
                    "TableStatus": "ACTIVE",
                    "CreationDateTime": 0,
                    "AttributeDefinitions": [],
                    "KeySchema": [],
                    "TableArn": "arn:aws:dynamodb:local:table/remote-test-table",
                    "BillingModeSummary": {
                        "BillingMode": "PAY_PER_REQUEST"
                    },
                    "ItemCount": 0,
                    "TableSizeBytes": 0
                }
            }));
        })
        .await;

    let exists = provider
        .table_exists(&table_name)
        .await
        .expect("table exists");

    assert!(exists);
    assert_eq!(provider.primary_endpoint.load(Ordering::Relaxed), 1);

    let cached_exists = provider
        .table_exists(&table_name)
        .await
        .expect("table exists through cached leader");

    assert!(cached_exists);
    follower_mock.assert_calls_async(1).await;
    leader_mock.assert_calls_async(2).await;
}

#[tokio::test]
async fn http_redirects_are_not_followed_implicitly() {
    let follower = MockServer::start_async().await;
    let leader = MockServer::start_async().await;
    let provider = provider_with_endpoints(vec![follower.url("/")]).await;

    let table_name = TableName::new("remote-test-table");

    let redirect_mock = follower
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.DescribeTable")
                .path("/");
            then.status(307).header("location", leader.url("/"));
        })
        .await;

    let leader_mock = leader
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.DescribeTable")
                .path("/");
            then.status(200).json_body(serde_json::json!({
                "Table": {
                    "TableName": table_name.as_ref(),
                    "TableStatus": "ACTIVE",
                    "CreationDateTime": 0,
                    "AttributeDefinitions": [],
                    "KeySchema": [],
                    "TableArn": "arn:aws:dynamodb:local:table/remote-test-table",
                    "BillingModeSummary": {
                        "BillingMode": "PAY_PER_REQUEST"
                    },
                    "ItemCount": 0,
                    "TableSizeBytes": 0
                }
            }));
        })
        .await;

    let error = provider
        .table_exists(&table_name)
        .await
        .expect_err("redirect should not be followed");

    assert!(matches!(error, StorageError::Base(_)));
    redirect_mock.assert_calls_async(1).await;
    leader_mock.assert_calls_async(0).await;
}

#[tokio::test]
async fn single_endpoint_exhausts_retries() {
    let server = MockServer::start_async().await;
    let provider = provider_for(&server).await;

    let table_name = TableName::new("remote-test-table");

    let failing_mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.DescribeTable")
                .path("/");
            then.status(500).json_body(serde_json::json!({
                "__type": "InternalServerError",
                "message": "simulated failure"
            }));
        })
        .await;

    let err = provider
        .table_exists(&table_name)
        .await
        .expect_err("should fail");
    assert!(matches!(err, StorageError::Base(_)));

    let hits = failing_mock.calls_async().await;
    assert_eq!(hits, MAX_ENDPOINT_RETRIES);
}
