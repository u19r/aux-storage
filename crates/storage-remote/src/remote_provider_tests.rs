use std::{
    collections::HashMap,
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

use httpmock::prelude::*;
use storage_observability_test_support::observation::observe;
use storage_provider::{
    OrderedOperationContexts, PhysicalWrite, RemoteCredentialStrategy, RemoteStorageSettings,
    StorageProvider, TtlWriteContext,
};
use storage_types::{
    AttributeDefinition, AttributeValue, CreateTableRequest, DeleteItemRequest, ItemKey,
    KeyAttributeType, KeySchemaElement, KeyType, PutItemRequest, QueryTableRequest,
    STORAGE_LEADER_HINT_HEADER, STORAGE_NOT_LEADER_ERROR_TYPE, ScanTableRequest, StorageEnum,
    StreamItemId, StreamName, TableName, TransactWriteItemsRequest, context::WrappedError as _,
};
use stream::StreamProvider;

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
async fn scan_converts_remote_dynamodb_last_key_to_provider_page_token() {
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
    let scan_mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.Scan")
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
        .scan_table(&ScanTableRequest {
            table_name: table_name.clone(),
            index_name: None,
            limit: Some(1),
            exclusive_start_key: None,
            consistent_read: false,
        })
        .await
        .expect("remote scan");

    let expected = ItemKey::table_key(
        table_name,
        AttributeValue::S("tenant#1".to_string()),
        Some(AttributeValue::S("7".to_string())),
    )
    .next_page_token()
    .expect("page token");
    assert_eq!(page.continuation.as_deref(), Some(expected.as_str()));
    assert_eq!(page.progress.evaluated_count, 1);
    assert!(page.progress.evaluated_bytes > 0);
    assert!(page.progress.last_evaluated_key.is_some());
    assert_eq!(describe_mock.calls_async().await, 1);
    scan_mock.assert_calls_async(1).await;
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

    let page = storage_provider::DynamoOperations::query_page(
        &provider,
        QueryTableRequest {
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
        },
    )
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
    let table_info = provider
        .get_table_info(&table_name)
        .await
        .expect("remote table metadata");
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
        .query_table_with_table_info(
            &QueryTableRequest {
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
            },
            &table_info,
        )
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

#[tokio::test]
async fn remote_scan_rejects_opaque_last_key_payloads() {
    let server = MockServer::start_async().await;
    let provider = provider_for(&server).await;
    let table_name = TableName::new("remote-test-table");
    let scan_mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.Scan")
                .path("/");
            then.status(200).json_body(serde_json::json!({
                "Items": [],
                "Count": 0,
                "ScannedCount": 0,
                "LastEvaluatedKey": "opaque-cursor"
            }));
        })
        .await;

    let error = provider
        .scan_table(&ScanTableRequest {
            table_name,
            index_name: None,
            limit: Some(1),
            exclusive_start_key: None,
            consistent_read: false,
        })
        .await
        .expect_err("opaque key must not cross the DynamoDB continuation boundary");

    assert!(matches!(
        error.to_enum(),
        StorageEnum::InternalServerError { message } if message.contains("LastEvaluatedKey")
    ));
    scan_mock.assert_calls_async(1).await;
}

#[tokio::test]
async fn delete_item_fetches_preimage_for_storage_manager_contract() {
    let server = MockServer::start_async().await;
    let provider = provider_for(&server).await;
    let table_name = TableName::new("remote-delete-table");
    let key = HashMap::from([("id".to_string(), AttributeValue::S("item#1".to_string()))]);
    let delete_mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.DeleteItem")
                .path("/")
                .body_includes("\"ReturnValues\":\"ALL_OLD\"");
            then.status(200).json_body(serde_json::json!({
                "Attributes": {
                    "id": {"S": "item#1"},
                    "status": {"S": "open"}
                }
            }));
        })
        .await;
    let request = DeleteItemRequest::new(table_name.clone(), key);
    let write = PhysicalWrite::try_new(
        0,
        table_name.clone(),
        request,
        TtlWriteContext::direct(table_name).expect("ttl context"),
    )
    .expect("physical write");

    let attributes = provider
        .delete_item_request(write)
        .await
        .expect("remote delete")
        .expect("deleted preimage")
        .into_iter()
        .collect::<HashMap<_, _>>();
    assert_eq!(attributes["status"], AttributeValue::S("open".to_string()));
    delete_mock.assert_calls_async(1).await;
}

#[tokio::test]
async fn list_tables_paginates_remote_wire_limit() {
    let server = MockServer::start_async().await;
    let provider = provider_for(&server).await;
    let (_, attributes, key_schema) = table_def();
    for table_name in ["table-a", "table-b"] {
        server
            .mock_async(|when, then| {
                when.method(POST)
                    .header("x-amz-target", "DynamoDB_20120810.DescribeTable")
                    .path("/")
                    .body_includes(format!("\"TableName\":\"{table_name}\""));
                then.status(200).json_body(describe_table_body(
                    &TableName::new(table_name),
                    &attributes,
                    &key_schema,
                ));
            })
            .await;
    }
    let first_page = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.ListTables")
                .path("/")
                .body_includes("\"Limit\":100")
                .body_excludes("ExclusiveStartTableName");
            then.status(200).json_body(serde_json::json!({
                "TableNames": ["table-a"],
                "LastEvaluatedTableName": "table-a"
            }));
        })
        .await;
    let second_page = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.ListTables")
                .path("/")
                .body_includes("\"Limit\":100")
                .body_includes("\"ExclusiveStartTableName\":\"table-a\"");
            then.status(200).json_body(serde_json::json!({
                "TableNames": ["table-b"]
            }));
        })
        .await;

    let tables = provider
        .list_tables(10_000, None)
        .await
        .expect("remote list tables");

    assert_eq!(
        tables
            .into_iter()
            .map(|table| table.table_name.to_string())
            .collect::<Vec<_>>(),
        ["table-a", "table-b"]
    );
    first_page.assert_calls_async(1).await;
    second_page.assert_calls_async(1).await;
}

#[tokio::test]
async fn list_tables_overlaps_delayed_describes_with_a_bounded_fanout() {
    let server = MockServer::start_async().await;
    let provider = provider_for(&server).await;
    let (_, attributes, key_schema) = table_def();
    let table_names = (0..8)
        .map(|index| format!("delayed-table-{index}"))
        .collect::<Vec<_>>();
    let list_mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.ListTables")
                .path("/");
            then.status(200).json_body(serde_json::json!({
                "TableNames": table_names,
            }));
        })
        .await;
    let describe_mocks = table_names
        .iter()
        .map(|table_name| {
            let response =
                describe_table_body(&TableName::new(table_name), &attributes, &key_schema);
            server.mock(|when, then| {
                when.method(POST)
                    .header("x-amz-target", "DynamoDB_20120810.DescribeTable")
                    .body_includes(format!("\"TableName\":\"{table_name}\""));
                then.status(200)
                    .json_body(response)
                    .delay(Duration::from_millis(60));
            })
        })
        .collect::<Vec<_>>();

    let started = Instant::now();
    let tables = provider
        .list_tables(8, None)
        .await
        .expect("delayed remote list tables");
    let elapsed = started.elapsed();

    assert_eq!(tables.len(), table_names.len());
    assert!(
        elapsed < Duration::from_millis(400),
        "bounded describe fanout should beat eight serial 60ms calls, elapsed={elapsed:?}"
    );
    println!(
        "LIST_TABLES_DELAYED_FANOUT names={} list_calls={} describe_calls={} elapsed_ms={}",
        tables.len(),
        list_mock.calls_async().await,
        describe_mocks.len(),
        elapsed.as_millis()
    );
    list_mock.assert_calls_async(1).await;
    for describe_mock in describe_mocks {
        describe_mock.assert_calls_async(1).await;
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

    let observed = observe(provider.transact_write_items(
        TransactWriteItemsRequest::default(),
        OrderedOperationContexts::try_new(Vec::new()).expect("empty contexts"),
    ))
    .await;
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

    assert!(matches!(
        error.to_enum(),
        StorageEnum::TransactionCanceled { reasons }
            if reasons.iter().map(String::as_str).eq([
                "ConditionalCheckFailed\tThe conditional request failed.\t{\"pk\":{\"S\":\"item-1\"}}"
            ])
    ));
    transaction_mock.assert_calls_async(1).await;
}

#[tokio::test]
async fn table_stream_records_are_forwarded_to_the_remote_storage_api() {
    let server = MockServer::start_async().await;
    let provider = provider_for(&server).await;
    let table_name = TableName::new("tenant_a");
    let starting_cursor = "000000000000000000000001"
        .parse::<StreamItemId>()
        .expect("starting cursor");
    let next_cursor = "000000000000000000000002";

    let stream_mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.GetStreamRecords")
                .path("/")
                .body_includes("\"TableName\":\"tenant_a\"")
                .body_includes("\"LastEvaluatedKey\":\"000000000000000000000001\"")
                .body_includes("\"Limit\":25");
            then.status(200).json_body(serde_json::json!({
                "TableName": "tenant_a",
                "Records": [{
                    "Keys": {"pk": {"S": "metric_batch_1"}},
                    "SequenceNumber": "sequence_1",
                    "NewImage": {"value": {"N": "7"}}
                }],
                "LastEvaluatedKey": next_cursor,
                "TableFrontier": storage_types::TableStreamFrontier {
                    table_history_id: 1u32.into(),
                    oldest: Some(starting_cursor),
                    head: Some(next_cursor.parse().expect("frontier head")),
                }
            }));
        })
        .await;

    let (records, cursor) = provider
        .get_stream_records_from_pointer_stream(
            StreamName::table_stream(&table_name),
            &[],
            Some(starting_cursor),
            Some(25),
        )
        .await
        .expect("remote table stream records");

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].sequence_number, "sequence_1");
    assert_eq!(cursor.expect("next cursor").to_string(), next_cursor);
    stream_mock.assert_calls_async(1).await;
}

#[tokio::test]
async fn given_system_stream_when_remote_records_are_read_then_operation_is_rejected() {
    let server = MockServer::start_async().await;
    let provider = provider_for(&server).await;
    let error = provider
        .get_stream_records_from_pointer_stream(
            StreamName::system_table_stream(),
            &[],
            None,
            Some(25),
        )
        .await
        .expect_err("remote system stream lineage is unsupported");
    assert!(
        error
            .to_string()
            .contains("Unsupported storage provider operation")
    );
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
async fn list_tables_describes_in_order_and_ignores_deleted_names() {
    let server = MockServer::start_async().await;
    let provider = provider_for(&server).await;

    let list_mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.ListTables")
                .path("/");
            then.status(200).json_body(serde_json::json!({
                "TableNames": ["remote-a", "remote-deleted", "remote-c"]
            }));
        })
        .await;

    let first_mock = describe_table_mock(&server, "remote-a", 200);
    let deleted_mock = describe_table_mock(&server, "remote-deleted", 400);
    let third_mock = describe_table_mock(&server, "remote-c", 200);

    let tables = provider.list_tables(100, None).await.expect("list tables");

    assert_eq!(
        tables
            .iter()
            .map(|table| table.table_name.as_ref())
            .collect::<Vec<_>>(),
        ["remote-a", "remote-c"]
    );
    list_mock.assert_calls_async(1).await;
    first_mock.assert_calls_async(1).await;
    deleted_mock.assert_calls_async(1).await;
    third_mock.assert_calls_async(1).await;
}

fn describe_table_mock<'a>(
    server: &'a MockServer,
    table_name: &str,
    status: u16,
) -> httpmock::Mock<'a> {
    server.mock(|when, then| {
        when.method(POST)
            .header("x-amz-target", "DynamoDB_20120810.DescribeTable")
            .body_includes(format!("\"TableName\":\"{table_name}\""));
        if status == 200 {
            then.status(status).json_body(serde_json::json!({
                "Table": {
                    "TableName": table_name,
                    "TableStatus": "ACTIVE",
                    "CreationDateTime": 0,
                    "AttributeDefinitions": [],
                    "KeySchema": [],
                    "TableArn": "arn:aws:dynamodb:local:table/remote-table",
                    "BillingModeSummary": {
                        "BillingMode": "PAY_PER_REQUEST"
                    },
                    "ItemCount": 0,
                    "TableSizeBytes": 0
                }
            }));
        } else {
            then.status(status).json_body(serde_json::json!({
                "__type": "ResourceNotFoundException",
                "message": "table disappeared"
            }));
        }
    })
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
    provider
        .put_item_request(
            PhysicalWrite::try_new(
                0,
                TableName::new("remote"),
                request,
                TtlWriteContext::direct(TableName::new("remote")).expect("ttl context"),
            )
            .expect("physical write"),
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

    let observed = observe(provider.table_exists(&table_name)).await;
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
    assert!(observed.result.expect("table exists"));

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

    // Keep this assertion about the cached primary, not probation sampling.
    provider
        .probation_endpoint
        .store(provider.endpoints.len(), Ordering::Relaxed);

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
        .table_exists(&table_name)
        .await
        .expect_err("should fail");
    assert!(err.base_error().is_some());

    let hits = failing_mock.calls_async().await;
    assert_eq!(hits, MAX_ENDPOINT_RETRIES);
}
