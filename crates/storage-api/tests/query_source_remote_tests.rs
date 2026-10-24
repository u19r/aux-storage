use std::sync::Arc;

use storage::{
    DatabaseManager, DatabaseManagerRuntimeOptions, PutItemInput, QueryTableInput,
    RemoteStorageProvider,
};
use storage_api::{AppState, server_router};
use storage_provider::{
    DynamoOperations, RemoteCredentialStrategy, RemoteStaticCredentials, RemoteStorageSettings,
    StorageBackend, StorageConfig,
};
use storage_types::{
    AttributeDefinition, AttributeMap, AttributeValue, BillingMode, CreateTableRequest,
    KeyAttributeType, KeySchemaElement, KeyType, QueryTableRequest, TableName,
    context::WrappedError as _,
};
use tokio::net::TcpListener;

fn remote_config(endpoint_url: String) -> StorageConfig {
    StorageConfig {
        backend_type: StorageBackend::Remote,
        connection_string: None,
        file_path: None,
        sqlite: None,
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: Some(RemoteStorageSettings {
            endpoint_urls: vec![endpoint_url],
            region: None,
            tls: false,
            credentials: RemoteCredentialStrategy::Static(RemoteStaticCredentials {
                access_key_id: "test-access".into(),
                secret_access_key: "test-secret".into(),
                session_token: None,
            }),
            timeouts: None,
        }),
    }
}

async fn seed_database() -> (Arc<DatabaseManager>, TableName) {
    let database = Arc::new(DatabaseManager::new_for_test().await.unwrap());
    let table = TableName::new("source-page-remote");
    let schema = [("pk", KeyType::Hash), ("sk", KeyType::Range)];
    database
        .create_table(&CreateTableRequest::new(
            table.clone(),
            schema
                .iter()
                .map(|(name, _)| AttributeDefinition {
                    attribute_name: (*name).into(),
                    attribute_type: KeyAttributeType::S,
                })
                .collect(),
            schema
                .iter()
                .map(|(name, key_type)| KeySchemaElement {
                    attribute_name: (*name).into(),
                    key_type: key_type.clone(),
                })
                .collect(),
            BillingMode::PayPerRequest,
        ))
        .await
        .unwrap();
    for id in ["a", "b", "c"] {
        database
            .put_item(
                PutItemInput::builder()
                    .table_name(table.clone())
                    .item(AttributeMap::from_iter([
                        ("pk".into(), AttributeValue::S("roles".into())),
                        ("sk".into(), AttributeValue::S(id.into())),
                    ]))
                    .build(),
            )
            .await
            .unwrap();
    }
    (database, table)
}

fn query(table: &TableName) -> QueryTableRequest {
    QueryTableInput::builder()
        .table_name(table.clone())
        .key_condition_expression("pk = :pk")
        .expression_attribute_values(AttributeMap::from_iter([(
            ":pk".into(),
            AttributeValue::S("roles".into()),
        )]))
        .consistent_read(true)
        .limit(1_u32)
        .build()
        .into()
}

#[tokio::test]
async fn given_remote_source_query_when_paginated_then_native_addresses_and_progress_are_preserved()
{
    let (database, table) = seed_database().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            server_router(Arc::new(AppState::new(database)), false),
        )
        .await
        .unwrap();
    });
    let remote = DatabaseManager::new_with_config_and_runtime_options(
        remote_config(endpoint),
        DatabaseManagerRuntimeOptions::builder()
            .enable_database_jobs(false)
            .build(),
    )
    .await
    .unwrap();
    let mut request = query(&table);
    let mut ids = Vec::new();
    loop {
        let page = remote.query_source_page(request.clone()).await.unwrap();
        assert_eq!(page.progress.evaluated_count as usize, page.items.len());
        assert!(page.progress.evaluated_bytes > 0);
        for source in &page.items {
            let attributes = source.item.to_attribute_map().unwrap();
            let expected = storage_types::TableKey::new(
                table.clone(),
                attributes.get("pk").unwrap().clone(),
                Some(attributes.get("sk").unwrap().clone()),
            );
            assert_eq!(
                serde_json::to_value(&source.key).unwrap(),
                serde_json::to_value(storage_types::ItemKey::Table(expected)).unwrap()
            );
            ids.push(attributes.get("sk").unwrap().clone());
        }
        let Some(continuation) = page.continuation else {
            break;
        };
        assert_ne!(request.exclusive_start_key.as_ref(), Some(&continuation));
        request.exclusive_start_key = Some(continuation);
    }
    assert_eq!(ids, ["a", "b", "c"].map(|id| AttributeValue::S(id.into())));
    let mut invalid = query(&table);
    invalid.limit = Some(0);
    assert!(remote.query_source_page(invalid).await.is_err());
    server.abort();
}

#[tokio::test]
async fn given_remote_source_page_when_progress_is_invalid_then_the_response_is_rejected() {
    let server = httpmock::MockServer::start_async().await;
    let response = server
        .mock_async(|when, then| {
            when.method(httpmock::Method::POST)
                .header("x-amz-target", "DynamoDB_20120810.QuerySourcePage");
            then.status(200).json_body(serde_json::json!({
                "items": [],
                "progress": {
                    "evaluated_count": 1,
                    "evaluated_bytes": 1,
                    "last_evaluated_key": null
                },
                "continuation": null
            }));
        })
        .await;
    let remote = RemoteStorageProvider::new(remote_config(server.base_url()).remote.unwrap())
        .await
        .unwrap();
    let error = remote
        .query_source_page(query(&TableName::new("source-page-remote")))
        .await
        .expect_err("invalid source progress must fail closed");
    assert!(
        matches!(error.to_enum(), storage_types::StorageEnum::InternalServerError { message } if message == "range source item count must equal evaluated count")
    );
    response.assert_calls_async(1).await;
}

#[tokio::test]
async fn given_remote_source_address_when_image_disagrees_then_transport_preserves_the_address() {
    let table = TableName::new("source-page-remote");
    let address = storage_types::ItemKey::Table(storage_types::TableKey::new(
        table.clone(),
        AttributeValue::S("roles".into()),
        Some(AttributeValue::S("physical-id".into())),
    ));
    let page = storage_provider::RangeReadPage {
        items: vec![storage_provider::RangeReadItem {
            key: address.clone(),
            item: storage_types::WireItem::try_from(AttributeMap::from_iter([
                ("pk".into(), AttributeValue::S("roles".into())),
                ("sk".into(), AttributeValue::S("different-image-id".into())),
            ]))
            .unwrap(),
        }],
        progress: storage_provider::RangeReadProgress {
            evaluated_count: 1,
            evaluated_bytes: 64,
            last_evaluated_key: Some(address.clone()),
        },
        continuation: None,
    };
    let server = httpmock::MockServer::start_async().await;
    let response = server
        .mock_async(|when, then| {
            when.method(httpmock::Method::POST)
                .header("x-amz-target", "DynamoDB_20120810.QuerySourcePage");
            then.status(200)
                .json_body(serde_json::to_value(&page).unwrap());
        })
        .await;
    let remote = RemoteStorageProvider::new(remote_config(server.base_url()).remote.unwrap())
        .await
        .unwrap();
    let result = remote.query_source_page(query(&table)).await.unwrap();
    assert_eq!(
        serde_json::to_value(&result.items[0].key).unwrap(),
        serde_json::to_value(&address).unwrap()
    );
    assert_eq!(
        result.items[0].item.to_attribute_map().unwrap().get("sk"),
        Some(&AttributeValue::S("different-image-id".into()))
    );
    response.assert_calls_async(1).await;
}

#[tokio::test]
async fn given_remote_transactional_graph_when_reading_index_then_service_owns_one_native_snapshot()
{
    let database = Arc::new(DatabaseManager::new_for_test().await.unwrap());
    let table = TableName::new("remote-transactional-graph");
    let request: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "TableName": table,
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "catalog", "AttributeType": "S"}
        ],
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "BillingMode": "PAY_PER_REQUEST",
        "GlobalSecondaryIndexes": [{
            "IndexName": "catalog", "KeySchema": [{"AttributeName": "catalog", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"}
        }]
    }))
    .unwrap();
    database.create_table(&request).await.unwrap();
    for id in ["a", "b"] {
        database
            .put_item(
                PutItemInput::builder()
                    .table_name(table.clone())
                    .item(AttributeMap::from_iter([
                        ("pk".into(), AttributeValue::S("product".into())),
                        ("sk".into(), AttributeValue::S(id.into())),
                        ("catalog".into(), AttributeValue::S("products".into())),
                    ]))
                    .build(),
            )
            .await
            .unwrap();
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let router =
        server_router(Arc::new(AppState::new(database)), false).layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let observed = Arc::clone(&observed);
                async move {
                    assert_eq!(
                        request.headers().get("x-amz-target").unwrap(),
                        "DynamoDB_20120810.ReadSequence"
                    );
                    observed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    next.run(request).await
                }
            },
        ));
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let remote = RemoteStorageProvider::new(remote_config(endpoint).remote.unwrap())
        .await
        .unwrap();
    let mut graph: storage_types::ReadSequenceRequest = serde_json::from_value(serde_json::json!({
        "ReadConsistency": "TRANSACTIONAL",
        "Nodes": [{"Name": "products", "Operation": {"Query": {
            "TableName": table, "IndexName": "catalog", "KeyConditionExpression": "#catalog = :catalog",
            "ExpressionAttributeNames": {"#catalog": "catalog"},
            "ExpressionAttributeValues": {":catalog": {"S": "products"}}, "Limit": 50
        }}}]
    })).unwrap();
    let response = remote
        .read_sequence(graph.clone())
        .await
        .expect("remote graph must run on the service's native attempt");
    assert_eq!(
        response.read_consistency,
        storage_types::ReadSequenceConsistency::Transactional
    );
    let storage_types::ReadSequenceInvocationPayload::Query(page) =
        &response.nodes[0].invocations[0].result
    else {
        panic!("query result required")
    };
    assert_eq!(page.count, 2);
    assert!(!response.partial);
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    graph.read_consistency = storage_types::ReadSequenceConsistency::Strong;
    let error = remote
        .read_sequence(graph)
        .await
        .expect_err("ordinary strong GSI reads must still reject");
    assert!(matches!(
        error.to_enum(),
        storage_types::StorageEnum::Validation { .. }
            | storage_types::StorageEnum::RawValidation { .. }
    ));
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 2);
    server.abort();
}
