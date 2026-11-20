#![allow(dead_code)]

use std::{collections::HashMap, future::Future, sync::Arc};

use http_error::HttpApiError;
use serde_json::Value;
use storage::{AdmissionClass, DatabaseManager, DatabaseManagerRuntimeOptions, PutItemInput};
use storage_provider::{StorageBackend, StorageConfig};
use storage_types::{
    AttributeDefinition, AttributeMap, AttributeValue, BatchGetItemRequest, BatchWriteItemRequest,
    CreateTableRequest, DeleteItemRequest, DescribeTableRequest, DescribeTimeToLiveRequest,
    GetItemRequest, KeyAttributeType, KeySchemaElement, KeyType, ListTablesRequest, PutItemRequest,
    PutItemResponse, QueryRequest, ScanRequest, StorageResult, TableName, TransactGetItemsRequest,
    TransactWriteItemsRequest, UpdateItemRequest, UpdateTableRequest,
};

use crate::{
    storage_operations::{clear_all_tables, dynamo_admitted},
    types::Response,
};

#[test]
fn given_request_path_sources_when_scanned_then_provider_calls_are_admitted() {
    let source_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut violations = Vec::new();
    for directory in ["storage_operations", "routes"] {
        let mut directories = vec![source_root.join(directory)];
        while let Some(directory) = directories.pop() {
            let entries = std::fs::read_dir(&directory).expect("request-path source directory");
            for entry in entries {
                let path = entry.expect("request-path source entry").path();
                if path.is_dir() {
                    directories.push(path);
                    continue;
                }
                if path.extension().and_then(std::ffi::OsStr::to_str) != Some("rs") {
                    continue;
                }
                let file_name = path
                    .file_name()
                    .and_then(std::ffi::OsStr::to_str)
                    .unwrap_or_default();
                if file_name.ends_with("_tests.rs")
                    || file_name == "test_support.rs"
                    || path.components().any(|component| {
                        component.as_os_str().to_string_lossy().ends_with("_tests")
                    })
                {
                    continue;
                }
                let source = std::fs::read_to_string(&path).expect("request-path source file");
                for accessor in ["stream_provider()", "queue_provider()", "pubsub_provider()"] {
                    if source.contains(accessor) {
                        violations.push(format!("{} contains {accessor}", path.display()));
                    }
                }
            }
        }
    }
    assert!(
        violations.is_empty(),
        "request paths must use an admitted provider handle:\n{}",
        violations.join("\n")
    );
}

pub async fn create_sqlite_test_db() -> Arc<DatabaseManager> {
    Arc::new(
        DatabaseManager::new_for_test()
            .await
            .expect("create test database manager"),
    )
}

pub async fn seed_item(
    db: &DatabaseManager,
    input: PutItemInput,
) -> StorageResult<PutItemResponse> {
    let item = storage_types::WireItem::try_from(input.item)?;
    let request = PutItemRequest {
        table_name: input.table_name,
        item,
        indexers: input.indexers,
        condition_expression: input.condition_expression,
        expression_attribute_names: input.expression_attribute_names,
        expression_attribute_values: input.expression_attribute_values,
        expected: None,
        conditional_operator: None,
        return_values: input.return_values,
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
        return_values_on_condition_check_failure: input
            .return_old_on_condition_failure
            .then(|| "ALL_OLD".to_string()),
    };
    db.run_dynamo_admitted("default", AdmissionClass::Write, |dynamo| async move {
        dynamo.put_item(request).await
    })
    .await
}

/// Creates a table whose key attributes are strings.
pub async fn create_string_key_table(
    db: &DatabaseManager,
    table_name: &str,
    hash_key: &str,
    range_key: Option<&str>,
) {
    let mut definitions = vec![AttributeDefinition {
        attribute_name: hash_key.to_string(),
        attribute_type: KeyAttributeType::S,
    }];
    let mut key_schema = vec![KeySchemaElement {
        attribute_name: hash_key.to_string(),
        key_type: KeyType::Hash,
    }];
    if let Some(range_key) = range_key {
        definitions.push(AttributeDefinition {
            attribute_name: range_key.to_string(),
            attribute_type: KeyAttributeType::S,
        });
        key_schema.push(KeySchemaElement {
            attribute_name: range_key.to_string(),
            key_type: KeyType::Range,
        });
    }
    let request = CreateTableRequest::new(
        TableName::new(table_name),
        definitions,
        key_schema,
        storage_types::BillingMode::PayPerRequest,
    );
    db.create_table(&request).await.unwrap();
}

/// Writes one item built from attribute pairs.
pub async fn seed_attributes(
    db: &DatabaseManager,
    table_name: &str,
    attributes: Vec<(&str, AttributeValue)>,
) {
    let item = attributes
        .into_iter()
        .map(|(name, value)| (name.to_string(), value))
        .collect::<HashMap<_, _>>();
    seed_item(
        db,
        PutItemInput::builder()
            .table_name(TableName::new(table_name))
            .item(item)
            .build(),
    )
    .await
    .unwrap();
}

/// Writes the nested item used by the projection tests, keyed by
/// `key_attribute` = "item-1".
pub async fn seed_nested_projection_item(
    db: &DatabaseManager,
    table_name: &str,
    key_attribute: &str,
) {
    let item = HashMap::from([
        (
            key_attribute.to_string(),
            AttributeValue::S("item-1".to_string()),
        ),
        (
            "status".to_string(),
            AttributeValue::S("active".to_string()),
        ),
        (
            "tags".to_string(),
            AttributeValue::L(vec![
                AttributeValue::S("red".to_string()),
                AttributeValue::S("blue".to_string()),
            ]),
        ),
        (
            "profile".to_string(),
            AttributeValue::M(AttributeMap::from(HashMap::from([
                ("name".to_string(), AttributeValue::S("Ada".to_string())),
                ("age".to_string(), AttributeValue::N("37".to_string())),
                ("city".to_string(), AttributeValue::S("London".to_string())),
            ]))),
        ),
        (
            "nested".to_string(),
            AttributeValue::M(AttributeMap::from(HashMap::from([(
                "list".to_string(),
                AttributeValue::L(vec![
                    AttributeValue::M(AttributeMap::from(HashMap::from([
                        ("v".to_string(), AttributeValue::S("first".to_string())),
                        ("x".to_string(), AttributeValue::S("drop".to_string())),
                    ]))),
                    AttributeValue::M(AttributeMap::from(HashMap::from([(
                        "v".to_string(),
                        AttributeValue::S("second".to_string()),
                    )]))),
                ]),
            )]))),
        ),
    ]);

    seed_item(
        db,
        PutItemInput::builder()
            .table_name(TableName::new(table_name))
            .item(item)
            .build(),
    )
    .await
    .unwrap();
}

#[derive(Clone, Debug)]
pub struct ConformanceTestBackend {
    pub name: &'static str,
    config: StorageConfig,
}

impl ConformanceTestBackend {
    pub async fn create_db(&self) -> Arc<DatabaseManager> {
        let config = self.isolated_config();
        Arc::new(
            DatabaseManager::new_with_config_and_runtime_options(
                config,
                conformance_runtime_options(),
            )
            .await
            .unwrap_or_else(|err| panic!("create {} conformance database: {err}", self.name)),
        )
    }

    pub async fn create_transactional_db(&self) -> Arc<DatabaseManager> {
        let mut config = self.isolated_config();
        if matches!(config.backend_type, StorageBackend::SQLite) {
            config.connection_string = Some(
                crate::storage_api_test_support_tests::unique_path("sqlite-transactional")
                    .to_string_lossy()
                    .into_owned(),
            );
        }
        Arc::new(
            DatabaseManager::new_with_config_and_runtime_options(
                config,
                conformance_runtime_options(),
            )
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "create transactional {} conformance database: {err}",
                    self.name
                )
            }),
        )
    }

    fn isolated_config(&self) -> StorageConfig {
        self.config.clone()
    }
}

pub async fn for_each_conformance_backend<F, Fut>(mut test: F)
where
    F: FnMut(&'static str, Arc<DatabaseManager>) -> Fut,
    Fut: Future<Output = ()>,
{
    for backend in default_conformance_backends() {
        let name = backend.name;
        let db = backend.create_db().await;
        test(name, db).await;
    }
}

pub async fn for_each_transactional_conformance_backend<F, Fut>(mut test: F)
where
    F: FnMut(&'static str, Arc<DatabaseManager>) -> Fut,
    Fut: Future<Output = ()>,
{
    for backend in default_conformance_backends() {
        let name = backend.name;
        let db = backend.create_transactional_db().await;
        test(name, db).await;
    }
}

fn conformance_runtime_options() -> DatabaseManagerRuntimeOptions {
    DatabaseManagerRuntimeOptions::builder()
        .enable_database_jobs(false)
        .build()
}

pub async fn create_sqlite_transactional_test_db() -> Arc<DatabaseManager> {
    let config = StorageConfig {
        initial_stream_lineages: None,
        backend_type: StorageBackend::SQLite,
        connection_string: Some(
            crate::storage_api_test_support_tests::unique_path("sqlite-transactional")
                .to_string_lossy()
                .into_owned(),
        ),
        file_path: None,
        sqlite: Some(storage_provider::SqliteSettings {}),
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    };
    Arc::new(
        DatabaseManager::new_for_test_with_config(config)
            .await
            .expect("create transactional test database manager"),
    )
}

pub fn default_conformance_backends() -> Vec<ConformanceTestBackend> {
    vec![ConformanceTestBackend {
        name: "sqlite",
        config: StorageConfig {
            initial_stream_lineages: None,
            backend_type: StorageBackend::SQLite,
            connection_string: Some(":memory:".to_string()),
            file_path: None,
            sqlite: Some(storage_provider::SqliteSettings::default()),
            postgres: None,
            turso: None,
            rocksdb: None,
            foundationdb: None,
            remote: None,
        },
    }]
}

pub async fn handle_create_table(
    db: Arc<DatabaseManager>,
    request: CreateTableRequest,
) -> Result<Response, HttpApiError> {
    dynamo_admitted(
        &db,
        AdmissionClass::Write,
        Response::CreateTable,
        move |d| async move { d.create_table(request).await },
    )
    .await
}

pub async fn handle_list_tables(
    db: Arc<DatabaseManager>,
    request: ListTablesRequest,
) -> Result<Response, HttpApiError> {
    dynamo_admitted(
        &db,
        AdmissionClass::RangeRead,
        Response::ListTables,
        move |d| async move { d.list_tables(request).await },
    )
    .await
}

pub async fn handle_put_item(
    db: Arc<DatabaseManager>,
    request: PutItemRequest,
) -> Result<Response, HttpApiError> {
    dynamo_admitted(
        &db,
        AdmissionClass::Write,
        Response::PutItem,
        move |d| async move { d.put_item(request).await },
    )
    .await
}

pub async fn handle_get_item(
    db: Arc<DatabaseManager>,
    request: GetItemRequest,
) -> Result<Response, HttpApiError> {
    dynamo_admitted(
        &db,
        AdmissionClass::PointRead,
        Response::GetItem,
        move |d| async move { d.get_item(request).await },
    )
    .await
}

pub async fn handle_delete_item(
    db: Arc<DatabaseManager>,
    request: DeleteItemRequest,
) -> Result<Response, HttpApiError> {
    dynamo_admitted(
        &db,
        AdmissionClass::Write,
        Response::DeleteItem,
        move |d| async move { d.delete_item(request).await },
    )
    .await
}

pub async fn handle_update_item(
    db: Arc<DatabaseManager>,
    request: UpdateItemRequest,
) -> Result<Response, HttpApiError> {
    dynamo_admitted(
        &db,
        AdmissionClass::Write,
        Response::UpdateItem,
        move |d| async move { d.update_item(request).await },
    )
    .await
}

pub async fn handle_describe_table(
    db: Arc<DatabaseManager>,
    request: DescribeTableRequest,
) -> Result<Response, HttpApiError> {
    dynamo_admitted(
        &db,
        AdmissionClass::PointRead,
        Response::DescribeTable,
        move |d| async move { d.describe_table(request).await },
    )
    .await
}

pub async fn handle_update_table(
    db: Arc<DatabaseManager>,
    request: UpdateTableRequest,
) -> Result<Response, HttpApiError> {
    dynamo_admitted(
        &db,
        AdmissionClass::Write,
        Response::UpdateTable,
        move |d| async move { d.update_table(request).await },
    )
    .await
}

pub async fn handle_batch_write_item(
    db: Arc<DatabaseManager>,
    request: BatchWriteItemRequest,
) -> Result<Response, HttpApiError> {
    dynamo_admitted(
        &db,
        AdmissionClass::Write,
        Response::BatchWriteItem,
        move |d| async move { d.batch_write_item(request).await },
    )
    .await
}

pub async fn handle_batch_get_item(
    db: Arc<DatabaseManager>,
    request: BatchGetItemRequest,
) -> Result<Response, HttpApiError> {
    dynamo_admitted(
        &db,
        AdmissionClass::RangeRead,
        Response::BatchGetItem,
        move |d| async move { d.batch_get_item(request).await },
    )
    .await
}

pub async fn handle_transact_write_items(
    db: Arc<DatabaseManager>,
    request: TransactWriteItemsRequest,
) -> Result<Response, HttpApiError> {
    dynamo_admitted(
        &db,
        AdmissionClass::Write,
        Response::TransactWriteItems,
        move |d| async move { d.transact_write_items(request).await },
    )
    .await
}

pub async fn handle_transact_get_items(
    db: Arc<DatabaseManager>,
    request: TransactGetItemsRequest,
) -> Result<Response, HttpApiError> {
    db.transact_get_items(request)
        .await
        .map(Response::TransactGetItems)
        .map_err(HttpApiError::from)
}

pub async fn handle_scan(
    db: Arc<DatabaseManager>,
    request: ScanRequest,
) -> Result<Response, HttpApiError> {
    dynamo_admitted(
        &db,
        AdmissionClass::RangeRead,
        Response::Scan,
        move |d| async move { d.scan(request).await },
    )
    .await
}

pub async fn handle_query(
    db: Arc<DatabaseManager>,
    request: QueryRequest,
) -> Result<Response, HttpApiError> {
    dynamo_admitted(
        &db,
        AdmissionClass::RangeRead,
        Response::Query,
        move |d| async move { d.query(request).await },
    )
    .await
}

pub async fn handle_describe_time_to_live(
    db: Arc<DatabaseManager>,
    request: DescribeTimeToLiveRequest,
) -> Result<Response, HttpApiError> {
    dynamo_admitted(
        &db,
        AdmissionClass::PointRead,
        Response::DescribeTimeToLive,
        move |d| async move { d.describe_time_to_live(request).await },
    )
    .await
}

pub async fn handle_clear_all_tables(
    db: Arc<DatabaseManager>,
    _payload: Value,
) -> Result<Response, HttpApiError> {
    clear_all_tables(&db).await
}
