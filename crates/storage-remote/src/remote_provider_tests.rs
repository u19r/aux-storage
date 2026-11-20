use std::{collections::HashMap, sync::atomic::Ordering};

use httpmock::prelude::*;
use storage_observability_test_support::observation::observe;
use storage_provider::{DynamoOperations as _, RemoteCredentialStrategy, RemoteStorageSettings};
use storage_types::{
    AttributeDefinition, AttributeValue, CreateTableRequest, DescribeTableRequest, GetItemRequest,
    KeyAttributeType, KeySchemaElement, KeyType, PutItemRequest, QueryTableRequest,
    STORAGE_LEADER_HINT_HEADER, STORAGE_NOT_LEADER_ERROR_TYPE, StorageEnum, StreamItemId,
    TableName, TransactWriteItemsRequest, context::WrappedError as _,
};

use crate::{RemoteStorageProvider, constants::MAX_ENDPOINT_RETRIES};

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

fn describe_table_body(
    table_name: &TableName,
    attributes: &[AttributeDefinition],
    key_schema: &[KeySchemaElement],
) -> serde_json::Value {
    serde_json::json!({
        "Table": {
            "TableName": table_name.as_ref(),
            "TableStatus": "ACTIVE",
            "TableId": uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, (table_name.as_ref()).as_bytes()).to_string(),
            "CreationDateTime": 0,
            "AttributeDefinitions": attributes,
            "KeySchema": key_schema,
            "TableArn": format!("arn:aws:dynamodb:local:table/{table_name}"),
            "ItemCount": 0,
            "TableSizeBytes": 0
        }
    })
}

#[tokio::test]
async fn query_converts_remote_dynamodb_last_key_to_provider_page_token() {
    let server = MockServer::start_async().await;
    let provider = provider_for(&server).await;
    let (table_name, attributes, key_schema) = table_def();
    let describe_mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.DescribeTable")
                .path("/");
            then.status(200)
                .json_body(describe_table_body(&table_name, &attributes, &key_schema));
        })
        .await;
    let query_mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.Query")
                .path("/");
            then.status(200).json_body(serde_json::json!({
                "Items": [{"pk": {"S": "tenant#1"}, "sk": {"S": "7"}}],
                "Count": 1,
                "ScannedCount": 1,
                "LastEvaluatedKey": {"pk": {"S": "tenant#1"}, "sk": {"S": "7"}}
            }));
        })
        .await;

    let page = provider
        .query_page(QueryTableRequest {
            table_name,
            index_name: None,
            key_condition_expression: "pk = :pk".to_string(),
            expression_attribute_names: None,
            expression_attribute_values: None,
            projection_expression: None,
            limit: Some(1),
            exclusive_start_key: None,
            scan_index_forward: Some(true),
            consistent_read: false,
        })
        .await
        .expect("remote query");

    assert!(
        page.continuation.is_some(),
        "query must expose a continuation"
    );
    assert_eq!(page.progress.evaluated_count, 1);
    assert_eq!(describe_mock.calls_async().await, 1);
    query_mock.assert_calls_async(1).await;
}

#[tokio::test]
async fn query_preserves_full_source_for_tiny_projection_when_upstream_cursor_is_at_end() {
    let server = MockServer::start_async().await;
    let provider = provider_for(&server).await;
    let (table_name, attributes, key_schema) = table_def();
    let describe_mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.DescribeTable")
                .path("/");
            then.status(200)
                .json_body(describe_table_body(&table_name, &attributes, &key_schema));
        })
        .await;
    let payload = "source-value".repeat(2_000);
    let query_mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.Query")
                .path("/")
                .body_excludes("ProjectionExpression")
                .body_excludes("Select");
            then.status(200).json_body(serde_json::json!({
                "Items": [{
                    "pk": {"S": "tenant#1"},
                    "sk": {"S": "7"},
                    "payload": {"S": payload}
                }],
                "Count": 1,
                "ScannedCount": 1
            }));
        })
        .await;

    let page = provider
        .query_page(QueryTableRequest {
            table_name: table_name.clone(),
            index_name: None,
            key_condition_expression: "pk = :pk".to_string(),
            expression_attribute_names: None,
            expression_attribute_values: None,
            projection_expression: Some("pk".to_string()),
            limit: Some(1),
            exclusive_start_key: None,
            scan_index_forward: Some(true),
            consistent_read: false,
        })
        .await
        .expect("remote query");

    assert!(page.continuation.is_none(), "end cursor must remain absent");
    assert_eq!(page.progress.evaluated_count, 1);
    assert!(page.progress.evaluated_bytes > 20_000);
    assert!(
        page.items[0]
            .attribute_value("payload")
            .expect("payload lookup")
            .is_some()
    );
    describe_mock.assert_calls_async(1).await;
    query_mock.assert_calls_async(1).await;
}

fn describe_request(table_name: &TableName) -> DescribeTableRequest {
    DescribeTableRequest {
        table_name: table_name.clone(),
    }
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
async fn transaction_cancellation_is_returned_once_with_complete_reasons() {
    let server = MockServer::start_async().await;
    let provider = provider_for(&server).await;
    let transaction_mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.TransactWriteItems")
                .path("/");
            then.status(400).json_body(serde_json::json!({
                "__type": "com.amazonaws.dynamodb.v20120810#TransactionCanceledException",
                "message": "Transaction cancelled",
                "CancellationReasons": [{
                    "Code": "ConditionalCheckFailed",
                    "Message": "The conditional request failed.",
                    "Item": {"pk": {"S": "item-1"}}
                }]
            }));
        })
        .await;

    let observed =
        observe(provider.transact_write_items(TransactWriteItemsRequest::default())).await;
    let traces = format!("{:?}{:?}", observed.events, observed.spans);
    assert!(
        !traces.contains("item-1"),
        "cancellation item entered tracing"
    );
    assert!(
        !traces.contains(&server.base_url()),
        "endpoint URL entered tracing"
    );
    let error = observed.result.expect_err("transaction cancellation");
    let expected_item: storage_types::AttributeMap =
        serde_json::from_value(serde_json::json!({"pk": {"S": "item-1"}})).unwrap();

    assert!(matches!(
        error.to_enum(),
        StorageEnum::TransactionCanceled { reasons }
            if reasons.as_slice() == [storage_types::CancellationReason::from_parts(
                storage_types::CancellationCode::ConditionalCheckFailed,
                Some("The conditional request failed.".into()),
                Some(expected_item),
            )]
    ));
    transaction_mock.assert_calls_async(1).await;
}

#[tokio::test]
async fn given_bound_table_stream_when_read_remotely_then_identity_and_cursor_are_forwarded() {
    use storage_types::{GetStreamRecordsRequest, TableHistoryTarget, TableStreamCheckpoint};
    let server = MockServer::start_async().await;
    let provider = provider_for(&server).await;
    let target = TableHistoryTarget {
        table_name: TableName::new("tenant_a"),
        history_id: 1_u32.into(),
    };
    let starting_pointer = StreamItemId::from([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    let next_pointer = StreamItemId::from([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]);
    let scope = storage_types::SystemStreamScope::try_new("source", 1).unwrap();
    let checkpoint = |offset: &str| {
        TableStreamCheckpoint::try_new(
            target.history_id,
            scope.clone(),
            vec![storage_types::SystemStreamPosition::try_new("shard", "lineage", offset).unwrap()],
            storage_types::TableStreamReadProgress {
                next_lane: 0,
                initial_visibility_cutoff: None,
            },
        )
        .unwrap()
        .to_string()
    };
    let starting_cursor = checkpoint("1");
    let next_cursor = checkpoint("2");
    let stream_mock = server.mock_async(|when, then| {
        when.method(POST)
            .header("x-amz-target", "DynamoDB_20120810.GetStreamRecords")
            .path("/")
            .body_includes("\"TableName\":\"tenant_a\"")
            .body_includes(format!("\"LastEvaluatedKey\":\"{starting_cursor}\""))
            .body_includes("\"TableHistoryId\":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1]")
            .body_includes("\"Limit\":25");
        then.status(200).json_body(serde_json::json!({
            "TableName": "tenant_a",
            "Records": [{ "Keys": {"pk": {"S": "metric_batch_1"}}, "SequenceNumber": "sequence_1", "Item": {"value": {"N": "7"}} }],
            "LastEvaluatedKey": next_cursor,
            "TableFrontier": storage_types::TableStreamFrontier { table_history_id: target.history_id, source_scope: scope.clone(), lanes: vec![storage_types::TableStreamLaneFrontier {
                shard_id: storage_types::SystemStreamShardId::try_new("shard").unwrap(), lineage_id: storage_types::SystemStreamLineageId::try_new("lineage").unwrap(),
                oldest: Some(starting_pointer.into()), head: Some(next_pointer.into()),
            }] }
        }));
    }).await;
    let response = storage_provider::DynamoOperations::get_stream_records(
        &provider,
        GetStreamRecordsRequest::table_at_incarnation(target, Some(starting_cursor), Some(25)),
    )
    .await
    .unwrap();
    assert_eq!(response.records().len(), 1);
    assert_eq!(response.records()[0].sequence_number, "sequence_1");
    assert_eq!(response.last_evaluated_key(), Some(next_cursor.as_str()));
    stream_mock.assert_calls_async(1).await;
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
                .body_excludes("MaxIndexers")
                .path("/");
            then.status(200).json_body(serde_json::json!({
                "TableDescription": {
                    "TableName": table_name.as_ref(),
                    "TableStatus": "ACTIVE",
                    "TableId": uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, (table_name.as_ref()).as_bytes()).to_string(),
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

    provider.create_table(request).await.expect("table created");
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
                    "TableId": uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, (table_name.as_ref()).as_bytes()).to_string(),
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
                    "TableId": uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, ("sys").as_bytes()).to_string(),
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
                    "TableId": uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, ("sys").as_bytes()).to_string(),
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

    provider.create_table(request).await.expect("table created");

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
                    "TableId": uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, ("sys").as_bytes()).to_string(),
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
                    "TableId": uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, ("sys").as_bytes()).to_string(),
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
        .create_table(CreateTableRequest::new(
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

    let request = PutItemRequest::new(
        TableName::new("remote"),
        storage_types::WireItem::try_from(&(item.clone())).unwrap(),
    );
    provider.put_item(request).await.expect("put");

    let mut key = HashMap::new();
    key.insert("pk".to_string(), AttributeValue::S("T#1".to_string()));
    key.insert("sk".to_string(), AttributeValue::S("U#1".to_string()));

    let mut get = GetItemRequest::new(
        TableName::new("remote"),
        key.into_iter().collect::<storage_types::KeyAttributes>(),
    );
    get.consistent_read = Some(true);
    let fetched = provider
        .get_item(get)
        .await
        .expect("get")
        .item
        .expect("present")
        .into_attribute_map()
        .expect("decode wire item");
    assert_eq!(fetched, item.into());
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
                    "TableId": uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, (table_name.as_ref()).as_bytes()).to_string(),
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

    let observed = observe(provider.describe_table(describe_request(&table_name))).await;
    let traces = format!("{:?}{:?}", observed.events, observed.spans);
    for private in [
        table_name.as_ref(),
        &failing.base_url(),
        &succeeding.base_url(),
        "simulated failure",
    ] {
        assert!(
            !traces.contains(private),
            "remote failure data entered tracing"
        );
    }
    observed.result.expect("table exists");

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
                    "TableId": uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, (table_name.as_ref()).as_bytes()).to_string(),
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
        .describe_table(describe_request(&table_name))
        .await
        .expect_err("validation failure");
    assert!(error.base_error().is_some());

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
                .header(STORAGE_LEADER_HINT_HEADER, leader_hint.as_str())
                .json_body(serde_json::json!({
                    "__type": STORAGE_NOT_LEADER_ERROR_TYPE,
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
                    "TableId": uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, (table_name.as_ref()).as_bytes()).to_string(),
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

    provider
        .describe_table(describe_request(&table_name))
        .await
        .expect("table exists");

    assert_eq!(provider.primary_endpoint.load(Ordering::Relaxed), 1);

    // Keep this assertion about the cached primary, not probation sampling.
    provider
        .probation_endpoint
        .store(provider.endpoints.len(), Ordering::Relaxed);

    provider
        .describe_table(describe_request(&table_name))
        .await
        .expect("table exists through cached leader");

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
                    "TableId": uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, (table_name.as_ref()).as_bytes()).to_string(),
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
        .describe_table(describe_request(&table_name))
        .await
        .expect_err("redirect should not be followed");

    assert!(error.base_error().is_some());
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
        .describe_table(describe_request(&table_name))
        .await
        .expect_err("should fail");
    assert!(err.base_error().is_some());

    let hits = failing_mock.calls_async().await;
    assert_eq!(hits, MAX_ENDPOINT_RETRIES);
}

#[tokio::test]
async fn given_dynamodb_stream_requests_when_forwarded_then_standard_actions_preserve_images() {
    let server = MockServer::start_async().await;
    let provider = provider_for(&server).await;
    let description = server.mock_async(|when, then| {
        when.method(POST).path("/").header("x-amz-target", "DynamoDBStreams_20120810.DescribeStream")
            .json_body(serde_json::json!({"StreamArn": "stream-arn"}));
        then.status(200).json_body(serde_json::json!({"StreamDescription": {
            "StreamArn": "stream-arn", "StreamLabel": "label", "StreamStatus": "ENABLED",
            "StreamViewType": "NEW_AND_OLD_IMAGES", "CreationRequestDateTime": 1,
            "TableName": "tenant_a", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "Shards": [{"ShardId": "shard-1", "SequenceNumberRange": {"StartingSequenceNumber": "1"}}]
        }}));
    }).await;
    let iterator = server.mock_async(|when, then| {
        when.method(POST).path("/").header("x-amz-target", "DynamoDBStreams_20120810.GetShardIterator")
            .json_body(serde_json::json!({"StreamArn": "stream-arn", "ShardId": "shard-1", "ShardIteratorType": "LATEST"}));
        then.status(200).json_body(serde_json::json!({"ShardIterator": "iterator-1"}));
    }).await;
    let records = server.mock_async(|when, then| {
        when.method(POST).path("/").header("x-amz-target", "DynamoDBStreams_20120810.GetRecords")
            .json_body(serde_json::json!({"ShardIterator": "iterator-1", "Limit": 1}));
        then.status(200).json_body(serde_json::json!({"NextShardIterator": "iterator-2", "Records": [{
            "eventID": "event-1", "eventName": "MODIFY", "eventVersion": "1.0",
            "eventSource": "aws:dynamodb", "awsRegion": "eu-north-1",
            "eventSourceARN": "arn:aws:dynamodb:eu-north-1:000000000000:table/tenant_a/stream/label",
            "dynamodb": {
                "Keys": {"pk": {"S": "key"}}, "SequenceNumber": "1", "StreamViewType": "NEW_AND_OLD_IMAGES",
                "SizeBytes": 100, "OldImage": {"pk": {"S": "key"}, "value": {"S": "old"}},
                "NewImage": {"pk": {"S": "key"}, "value": {"S": "new"}}
            }
        }]}));
    }).await;
    let described = provider
        .describe_stream(
            serde_json::from_value(serde_json::json!({"StreamArn": "stream-arn"})).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(described.stream_description.shards[0].shard_id, "shard-1");
    let issued = provider.get_shard_iterator(serde_json::from_value(serde_json::json!({"StreamArn": "stream-arn", "ShardId": "shard-1", "ShardIteratorType": "LATEST"})).unwrap()).await.unwrap();
    let page = provider
        .get_records(storage_types::GetRecordsRequest {
            shard_iterator: issued.shard_iterator,
            limit: Some(1),
        })
        .await
        .unwrap();
    assert_eq!(page.next_shard_iterator.as_deref(), Some("iterator-2"));
    assert_eq!(page.records[0].event_name, "MODIFY");
    assert_eq!(
        page.records[0]
            .dynamodb
            .old_image
            .as_ref()
            .unwrap()
            .get("value"),
        Some(&AttributeValue::S("old".into()))
    );
    assert_eq!(
        page.records[0]
            .dynamodb
            .new_image
            .as_ref()
            .unwrap()
            .get("value"),
        Some(&AttributeValue::S("new".into()))
    );
    for mock in [description, iterator, records] {
        mock.assert_calls_async(1).await;
    }
}
