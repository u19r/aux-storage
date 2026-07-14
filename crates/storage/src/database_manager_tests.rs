use std::{collections::HashMap, sync::Arc};

use serde::Serialize;
use storage_provider::StorageProvider;
#[cfg(feature = "foundationdb")]
use storage_provider::{FoundationDbSettings, StorageBackend, StorageConfig};
use storage_types::{
    AttributeDefinition, AttributeValue, BatchWriteItemEncodeRequest, BatchWriteItemRequest,
    CreateGlobalSecondaryIndex, CreateTableRequest, EncodePutRequest, EncodeWriteRequest,
    IndexName, KeyAttributeType, KeySchemaElement, KeyType, Projection, ProjectionType, PutRequest,
    StreamSpecification, StreamViewType, TableName, TableNamespace, TimestampMillis,
    TransactEncodeItem, TransactEncodePutRequest, TransactPutRequest, TransactUpdateRequest,
    TransactWriteItem, TransactWriteItemsEncodeRequest, TransactWriteItemsRequest, TryIntoWireItem,
    WireItem, WriteRequest, single_table_entity::SingleTableEntity,
};

use crate::{
    CappedStorageError, CreateCappedEntityInput, DatabaseManager, DatabaseManagerRuntimeOptions,
    DeleteCappedEntityInput, QueryIndexInput, QueryTableInput, Tables,
};

#[cfg(feature = "foundationdb")]
const LOCAL_FDB_CLUSTER_FILE: &str = "/usr/local/etc/foundationdb/fdb.cluster";

async fn create_hash_table(db: &DatabaseManager, table_name: &TableName) {
    let request = CreateTableRequest::new(
        table_name.clone(),
        vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        storage_types::BillingMode::PayPerRequest,
    );
    db.create_table(&request).await.expect("create table");
}

async fn create_hash_table_with_stream(db: &DatabaseManager, table_name: &TableName) {
    let request = CreateTableRequest::new(
        table_name.clone(),
        vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        storage_types::BillingMode::PayPerRequest,
    )
    .with_stream_specification(Some(StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(StreamViewType::NewAndOldImages),
    }));
    db.create_table(&request).await.expect("create table");
}

async fn create_pk_sk_table(db: &DatabaseManager, table_name: &TableName) {
    let request = CreateTableRequest::new(
        table_name.clone(),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        storage_types::BillingMode::PayPerRequest,
    );
    db.create_table(&request).await.expect("create table");
}

async fn create_single_table_mode_db() -> DatabaseManager {
    DatabaseManager::new_for_test_with_runtime_options(
        DatabaseManagerRuntimeOptions::builder()
            .enable_single_table_mode(true)
            .build(),
    )
    .await
    .expect("create single-table mode database manager")
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore = "requires a local FoundationDB cluster on 127.0.0.1:4689"]
async fn foundationdb_database_manager_bootstraps_system_tables() {
    if !std::path::Path::new(LOCAL_FDB_CLUSTER_FILE).is_file() {
        eprintln!("Skipping FoundationDB manager bootstrap test: cluster file missing");
        return;
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos();
    let config = StorageConfig {
        backend_type: StorageBackend::FoundationDb,
        connection_string: None,
        file_path: None,
        sqlite: None,
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: Some(FoundationDbSettings {
            cluster_file: Some(LOCAL_FDB_CLUSTER_FILE.to_string()),
            tenant_name: None,
            subspace_prefix: Some(format!("tests/storage-manager-bootstrap/{nanos}/")),
            cache_read_version_ms: 0,
            immediate_gsi_consistency: true,
        }),
        remote: None,
    };

    let db = DatabaseManager::new_with_config_and_runtime_options(
        config,
        DatabaseManagerRuntimeOptions::builder()
            .enable_database_jobs(false)
            .enable_background_refresh(false)
            .enable_background_watchers(false)
            .run_gsi_maintenance_after_write(None)
            .build(),
    )
    .await
    .expect("foundationdb manager should bootstrap system tables");

    assert!(
        db.table_exists(&Tables::sys_namespaces())
            .await
            .expect("query system namespaces table"),
        "system namespaces table should exist after manager construction"
    );
}

#[derive(Debug, Clone, Serialize)]
struct TestCappedEntity {
    entity_id: String,
    payload: String,
}

#[derive(Debug, Clone, Serialize)]
struct TestTimestampEntity {
    entity_id: String,
    updated_at: i64,
}

impl storage_types::single_table_entity::SingleTableEntity for TestCappedEntity {
    const STORAGE_ENTITY_TYPE: &'static str = "PLATFORM_BILLING_CATALOG_PRODUCT";
    const ENTITY_TYPE: &'static str = "PLATFORM_BILLING_CATALOG_PRODUCT";

    fn pk(&self) -> String {
        "TEST".to_string()
    }

    fn sk(&self) -> String {
        format!("ENTITY#{}", self.entity_id)
    }
}

impl TryIntoWireItem for TestCappedEntity {
    fn try_into_wire_item(&self) -> storage_types::StorageResult<WireItem> {
        storage_types::single_table_entity::to_wire_item(self)
            .map_err(|err| storage_types::StorageError::internal(&err.to_string()))
    }
}

impl storage_types::single_table_entity::SingleTableEntity for TestTimestampEntity {
    const STORAGE_ENTITY_TYPE: &'static str = "TEST_TIMESTAMP_ENTITY";
    const ENTITY_TYPE: &'static str = "TEST_TIMESTAMP_ENTITY";

    fn pk(&self) -> String {
        "TEST".to_string()
    }

    fn sk(&self) -> String {
        format!("TIMESTAMP#{}", self.entity_id)
    }
}

impl TryIntoWireItem for TestTimestampEntity {
    fn try_into_wire_item(&self) -> storage_types::StorageResult<WireItem> {
        storage_types::single_table_entity::to_wire_item(self)
            .map_err(|err| storage_types::StorageError::internal(&err.to_string()))
    }
}

fn read_updated_at_ms(item: &HashMap<String, AttributeValue>) -> i64 {
    match item.get("u_at").or_else(|| item.get("updated_at")) {
        Some(AttributeValue::N(value)) => value.parse::<i64>().expect("updated_at should parse"),
        other => panic!("expected numeric updated_at, got: {other:?}"),
    }
}

fn read_count_value(item: &HashMap<String, AttributeValue>) -> u64 {
    match item.get("value") {
        Some(AttributeValue::N(value)) => value.parse::<u64>().expect("count should parse"),
        other => panic!("expected numeric count value, got: {other:?}"),
    }
}

fn assert_no_single_table_metadata(item: &HashMap<String, AttributeValue>) {
    for attr in [
        "created_at",
        "updated_at",
        "c_at",
        "u_at",
        "entity_type",
        "et",
    ] {
        assert!(
            !item.contains_key(attr),
            "default DynamoDB mode must not add reserved single-table attribute {attr}"
        );
    }
}

#[tokio::test]
async fn clear_all_tables_preserves_internal_system_tables() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");

    let user_table = TableName::new("user_data");
    let create_user_table = CreateTableRequest::new(
        user_table.clone(),
        vec![AttributeDefinition {
            attribute_name: "id".to_string(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "id".to_string(),
            key_type: KeyType::Hash,
        }],
        storage_types::BillingMode::PayPerRequest,
    );
    db.create_table(&create_user_table)
        .await
        .expect("create user table");

    Tables::create_sys_jobs_table(&db)
        .await
        .expect("create system jobs table");

    db.clear_all_tables().await.expect("clear all tables");

    let has_user_table = db
        .table_exists(&user_table)
        .await
        .expect("query user table existence");
    assert!(
        !has_user_table,
        "clear_all_tables should remove user tables"
    );

    let has_j_table = db
        .table_exists(&Tables::sys_jobs())
        .await
        .expect("query j table existence");
    assert!(has_j_table, "clear_all_tables should recreate j table");

    let has_r_table = db
        .table_exists(&Tables::sys_storage_replication())
        .await
        .expect("query r table existence");
    assert!(has_r_table, "clear_all_tables should recreate r table");
}

#[tokio::test]
async fn sys_namespaces_bootstrap_enables_streams_for_analytics_polling() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");

    Tables::create_sys_namespaces_table(&db)
        .await
        .expect("create system namespace table");

    let table_info = db
        .get_table_info(&Tables::sys_namespaces())
        .await
        .expect("load system namespace table");
    let stream = table_info
        .stream_specification
        .expect("system namespace table should have stream config");
    assert!(stream.stream_enabled);
    assert_eq!(
        stream.stream_view_type,
        Some(storage_types::StreamViewType::NewAndOldImages)
    );
}

#[tokio::test]
async fn sys_namespaces_bootstrap_upgrades_existing_table_without_streams() {
    let provider = Arc::new(
        sql::SQLiteStorageProvider::new(":memory:")
            .await
            .expect("create sqlite provider"),
    );
    provider
        .initialize_storage()
        .await
        .expect("initialize sqlite provider");
    let db = DatabaseManager::new_with_mocks(provider);

    db.create_table(&CreateTableRequest::new(
        Tables::sys_namespaces(),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        storage_types::BillingMode::PayPerRequest,
    ))
    .await
    .expect("create legacy system namespace table");
    Tables::create_sys_jobs_table(&db)
        .await
        .expect("create system jobs table");
    Tables::create_sys_storage_replication_table(&db)
        .await
        .expect("create system replication table");

    Tables::create_sys_namespaces_table(&db)
        .await
        .expect("upgrade system namespace table");

    let table_info = db
        .get_table_info(&Tables::sys_namespaces())
        .await
        .expect("load upgraded system namespace table");
    assert!(
        table_info
            .stream_specification
            .as_ref()
            .is_some_and(|stream| stream.stream_enabled)
    );
}

#[tokio::test]
async fn namespace_table_bootstrap_enables_streams_for_analytics_polling() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let namespace = TableNamespace::system();

    Tables::create_namespace_table(&db, &namespace)
        .await
        .expect("create namespace table");

    let table_info = db
        .get_table_info(&Tables::namespace(&namespace))
        .await
        .expect("load namespace table");
    let stream = table_info
        .stream_specification
        .expect("namespace table should have stream config");
    assert!(stream.stream_enabled);
    assert_eq!(
        stream.stream_view_type,
        Some(storage_types::StreamViewType::NewAndOldImages)
    );
}

#[tokio::test]
async fn namespace_table_bootstrap_upgrades_existing_table_without_streams() {
    let provider = Arc::new(
        sql::SQLiteStorageProvider::new(":memory:")
            .await
            .expect("create sqlite provider"),
    );
    provider
        .initialize_storage()
        .await
        .expect("initialize sqlite provider");
    let db = DatabaseManager::new_with_mocks(provider);
    let namespace = TableNamespace::system();
    let table_name = Tables::namespace(&namespace);

    create_pk_sk_table(&db, &table_name).await;
    Tables::create_sys_jobs_table(&db)
        .await
        .expect("create system jobs table");
    Tables::create_sys_storage_replication_table(&db)
        .await
        .expect("create system replication table");

    Tables::create_namespace_table(&db, &namespace)
        .await
        .expect("upgrade namespace table");

    let table_info = db
        .get_table_info(&table_name)
        .await
        .expect("load upgraded namespace table");
    assert!(
        table_info
            .stream_specification
            .as_ref()
            .is_some_and(|stream| stream.stream_enabled)
    );
}

#[tokio::test]
async fn shared_namespace_table_bootstrap_enables_streams_for_analytics_polling() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");

    Tables::create_shared_namespace_table(&db, 0)
        .await
        .expect("create shared namespace table");

    let table_info = db
        .storage_provider()
        .get_table_info(&Tables::shared_namespace(0))
        .await
        .expect("load shared namespace table");
    let stream = table_info
        .stream_specification
        .expect("shared namespace table should have stream config");
    assert!(stream.stream_enabled);
    assert_eq!(
        stream.stream_view_type,
        Some(storage_types::StreamViewType::NewAndOldImages)
    );
}

#[tokio::test]
async fn default_put_item_persists_exact_customer_attributes() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("default_put_exact_attrs");
    create_hash_table(&db, &table_name).await;

    db.put_item(
        crate::PutItemInput::builder()
            .table_name(table_name.clone())
            .item(HashMap::from([
                ("pk".to_string(), AttributeValue::S("item#1".to_string())),
                (
                    "payload".to_string(),
                    AttributeValue::S("customer-value".to_string()),
                ),
            ]))
            .build(),
    )
    .await
    .expect("put item");

    let stored = db
        .get_item_map(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))]),
        )
        .await
        .expect("get item")
        .expect("item should exist");
    assert_eq!(stored.len(), 2);
    assert_no_single_table_metadata(&stored);
    assert_eq!(
        stored.get("payload"),
        Some(&AttributeValue::S("customer-value".to_string()))
    );
}

#[tokio::test]
async fn get_stream_records_for_table_name_uses_stream_record_sequence() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("default_stream_records_target_sequence");
    create_hash_table_with_stream(&db, &table_name).await;

    db.put_item(
        crate::PutItemInput::builder()
            .table_name(table_name.clone())
            .item(HashMap::from([
                ("pk".to_string(), AttributeValue::S("item#1".to_string())),
                (
                    "payload".to_string(),
                    AttributeValue::S("customer-value".to_string()),
                ),
            ]))
            .build(),
    )
    .await
    .expect("put item");

    let response = db
        .get_stream_records_for_table_name(&table_name, None, Some(10))
        .await
        .expect("read stream records");

    assert_eq!(response.records.len(), 1);
    assert_eq!(
        response.records[0].cursor.as_deref(),
        Some(response.records[0].sequence_number.as_str())
    );
    assert_eq!(
        response.records[0].keys.get("pk"),
        Some(&AttributeValue::S("item#1".to_string()))
    );
    assert_eq!(
        response.records[0]
            .new_image
            .as_ref()
            .and_then(|item| item.get("payload")),
        Some(&AttributeValue::S("customer-value".to_string()))
    );
}

#[tokio::test]
async fn get_stream_records_after_empty_page_does_not_skip_future_writes() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("default_stream_records_empty_then_future");
    create_hash_table_with_stream(&db, &table_name).await;

    let empty_response = db
        .get_stream_records_for_table_name(&table_name, None, Some(10))
        .await
        .expect("read empty stream records");
    assert!(empty_response.records.is_empty());

    db.put_item(
        crate::PutItemInput::builder()
            .table_name(table_name.clone())
            .item(HashMap::from([
                (
                    "pk".to_string(),
                    AttributeValue::S("item#future".to_string()),
                ),
                (
                    "payload".to_string(),
                    AttributeValue::S("future-value".to_string()),
                ),
            ]))
            .build(),
    )
    .await
    .expect("put future item");

    let future_response = db
        .get_stream_records_for_table_name(
            &table_name,
            empty_response.last_evaluated_key.as_deref(),
            Some(10),
        )
        .await
        .expect("read future stream records");

    assert_eq!(future_response.records.len(), 1);
    assert_eq!(
        future_response.records[0].keys.get("pk"),
        Some(&AttributeValue::S("item#future".to_string()))
    );
}

#[tokio::test]
async fn default_batch_write_item_persists_exact_customer_attributes() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("default_batch_exact_attrs");
    create_hash_table(&db, &table_name).await;

    db.batch_write_item(BatchWriteItemRequest {
        request_items: HashMap::from([(
            table_name.clone(),
            vec![WriteRequest {
                put_request: Some(PutRequest {
                    item: HashMap::from([
                        ("pk".to_string(), AttributeValue::S("item#1".to_string())),
                        (
                            "payload".to_string(),
                            AttributeValue::S("customer-value".to_string()),
                        ),
                    ]),
                    aux_item_stream_ttl_hours: None,
                }),
                delete_request: None,
            }],
        )]),
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    })
    .await
    .expect("batch write should succeed");

    let stored = db
        .get_item_map(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))]),
        )
        .await
        .expect("get item")
        .expect("item should exist");
    assert_eq!(stored.len(), 2);
    assert_no_single_table_metadata(&stored);
}

#[tokio::test]
async fn default_put_item_refreshes_existing_updated_at_metadata() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("default_put_existing_timestamp");
    create_hash_table(&db, &table_name).await;

    db.put_item(
        crate::PutItemInput::builder()
            .table_name(table_name.clone())
            .item(HashMap::from([
                ("pk".to_string(), AttributeValue::S("item#1".to_string())),
                (
                    storage_types::single_table_entity::UPDATED_AT_ATTR.to_string(),
                    AttributeValue::N("1".to_string()),
                ),
            ]))
            .build(),
    )
    .await
    .expect("put item");

    let stored = db
        .get_item_map(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))]),
        )
        .await
        .expect("get item")
        .expect("item should exist");
    assert!(read_updated_at_ms(&stored) > 1);
    assert!(
        !stored.contains_key(storage_types::single_table_entity::UPDATED_AT_ATTR),
        "storage manager should normalize updated_at to the compact storage alias"
    );
}

#[tokio::test]
async fn default_batch_write_item_refreshes_entity_timestamp_payloads() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("default_batch_entity_timestamp");
    create_pk_sk_table(&db, &table_name).await;
    let entity = TestTimestampEntity {
        entity_id: "one".to_string(),
        updated_at: 1,
    };

    db.batch_write_item(BatchWriteItemRequest {
        request_items: HashMap::from([(
            table_name.clone(),
            vec![WriteRequest {
                put_request: Some(PutRequest {
                    item: storage_types::single_table_entity::to_item_map(&entity)
                        .expect("encode entity"),
                    aux_item_stream_ttl_hours: None,
                }),
                delete_request: None,
            }],
        )]),
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    })
    .await
    .expect("batch write should succeed");

    let stored = db
        .get_item_map(table_name, entity.table_key_map())
        .await
        .expect("get item")
        .expect("item should exist");
    assert!(read_updated_at_ms(&stored) > 1);
}

#[tokio::test]
async fn default_update_item_does_not_inject_updated_at() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("default_update_no_stamp");
    create_hash_table(&db, &table_name).await;

    db.put_item(
        crate::PutItemInput::builder()
            .table_name(table_name.clone())
            .item(HashMap::from([(
                "pk".to_string(),
                AttributeValue::S("item#1".to_string()),
            )]))
            .build(),
    )
    .await
    .expect("put item");

    db.update_item(
        crate::UpdateItemInput::builder()
            .table_name(table_name.clone())
            .key(HashMap::from([(
                "pk".to_string(),
                AttributeValue::S("item#1".to_string()),
            )]))
            .update_expression("SET payload = :payload".to_string())
            .expression_attribute_values(HashMap::from([(
                ":payload".to_string(),
                AttributeValue::S("customer-value".to_string()),
            )]))
            .build(),
    )
    .await
    .expect("update item");

    let stored = db
        .get_item_map(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))]),
        )
        .await
        .expect("get item")
        .expect("item should exist");
    assert_eq!(stored.len(), 2);
    assert_no_single_table_metadata(&stored);
}

#[tokio::test]
async fn default_update_item_refreshes_existing_updated_at_metadata() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("default_update_existing_timestamp");
    create_hash_table(&db, &table_name).await;

    db.put_item(
        crate::PutItemInput::builder()
            .table_name(table_name.clone())
            .item(HashMap::from([
                ("pk".to_string(), AttributeValue::S("item#1".to_string())),
                (
                    storage_types::single_table_entity::UPDATED_AT_ALIAS_ATTR.to_string(),
                    AttributeValue::N("1".to_string()),
                ),
            ]))
            .build(),
    )
    .await
    .expect("put item");

    db.update_item(
        crate::UpdateItemInput::builder()
            .table_name(table_name.clone())
            .key(HashMap::from([(
                "pk".to_string(),
                AttributeValue::S("item#1".to_string()),
            )]))
            .update_expression("SET payload = :payload".to_string())
            .expression_attribute_values(HashMap::from([(
                ":payload".to_string(),
                AttributeValue::S("customer-value".to_string()),
            )]))
            .build(),
    )
    .await
    .expect("update item");

    let stored = db
        .get_item_map(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))]),
        )
        .await
        .expect("get item")
        .expect("item should exist");
    assert!(read_updated_at_ms(&stored) > 1);
}

#[tokio::test]
async fn default_mode_allows_custom_gsi_name_and_numeric_sort_key() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("default_custom_gsi_numeric_sort");
    let index_name = IndexName::new("by_score");
    let request = CreateTableRequest::new(
        table_name.clone(),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "category".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "score".to_string(),
                attribute_type: KeyAttributeType::N,
            },
        ],
        vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        storage_types::BillingMode::PayPerRequest,
    )
    .with_global_secondary_indexes(Some(vec![CreateGlobalSecondaryIndex {
        index_name: index_name.clone(),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "category".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "score".to_string(),
                key_type: KeyType::Range,
            },
        ],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }]));
    db.create_table(&request).await.expect("create table");

    db.batch_write_item(BatchWriteItemRequest {
        request_items: HashMap::from([(
            table_name.clone(),
            vec![WriteRequest {
                put_request: Some(PutRequest {
                    item: HashMap::from([
                        ("pk".to_string(), AttributeValue::S("item#1".to_string())),
                        (
                            "category".to_string(),
                            AttributeValue::S("games".to_string()),
                        ),
                        ("score".to_string(), AttributeValue::N("42".to_string())),
                    ]),
                    aux_item_stream_ttl_hours: None,
                }),
                delete_request: None,
            }],
        )]),
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    })
    .await
    .expect("batch write should succeed");

    let (items, _) = db
        .query_index_map(
            QueryIndexInput::builder()
                .table_name(table_name)
                .index_name(index_name)
                .key_condition_expression("category = :category AND score = :score".to_string())
                .expression_attribute_values(HashMap::from([
                    (
                        ":category".to_string(),
                        AttributeValue::S("games".to_string()),
                    ),
                    (":score".to_string(), AttributeValue::N("42".to_string())),
                ]))
                .build(),
        )
        .await
        .expect("query custom index");

    assert_eq!(items.len(), 1);
    assert_no_single_table_metadata(&items[0]);
    assert_eq!(
        items[0].get("score"),
        Some(&AttributeValue::N("42".to_string()))
    );
}

#[tokio::test]
async fn default_mode_allows_explicit_table_entity_put_helper() {
    let db = DatabaseManager::new_for_test_with_runtime_options(
        DatabaseManagerRuntimeOptions::default(),
    )
    .await
    .expect("create test database manager");
    let table_name = TableName::new("default_reject_single_table_helpers");
    create_pk_sk_table(&db, &table_name).await;

    let entity = TestCappedEntity {
        entity_id: "one".to_string(),
        payload: "payload-one".to_string(),
    };
    db.put_item_entity_encode(
        crate::PutItemEntityEncodeInput::builder()
            .table_name(table_name.clone())
            .item(&entity)
            .build(),
    )
    .await
    .expect("explicit table entity put helper should work outside single-table mode");

    let item = db
        .get_item(table_name, entity.table_key())
        .await
        .expect("read entity")
        .expect("entity item")
        .to_attribute_map()
        .expect("decode entity item");
    assert_eq!(
        item.get("payload"),
        Some(&AttributeValue::S("payload-one".to_string()))
    );
    assert!(
        read_updated_at_ms(&item) > 0,
        "entity write helpers should own metadata stamping without single-table mode"
    );
}

#[tokio::test]
async fn query_table_rejects_unused_expression_attribute_values() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");

    let table_name = TableName::new("query_unused_values");
    let request = CreateTableRequest::new(
        table_name.clone(),
        vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        storage_types::BillingMode::PayPerRequest,
    );
    db.create_table(&request).await.expect("create table");

    let query = QueryTableInput::builder()
        .table_name(table_name)
        .key_condition_expression("pk = :pk_val".to_string())
        .expression_attribute_values(HashMap::from([
            (
                ":pk_val".to_string(),
                AttributeValue::S("tenant#1".to_string()),
            ),
            (":pk".to_string(), AttributeValue::S("unused".to_string())),
        ]))
        .build();

    let err = db
        .query_table_map(query)
        .await
        .expect_err("query should fail for unused expression values");
    assert_eq!(
        err.to_string(),
        "Value provided in ExpressionAttributeValues unused in expressions: keys: {:pk}"
    );
}

#[tokio::test]
async fn query_table_rejects_unused_expression_attribute_names() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");

    let table_name = TableName::new("query_unused_names");
    let request = CreateTableRequest::new(
        table_name.clone(),
        vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        storage_types::BillingMode::PayPerRequest,
    );
    db.create_table(&request).await.expect("create table");

    let query = QueryTableInput::builder()
        .table_name(table_name)
        .key_condition_expression("pk = :pk_val".to_string())
        .expression_attribute_names(HashMap::from([("#pk".to_string(), "pk".to_string())]))
        .expression_attribute_values(HashMap::from([(
            ":pk_val".to_string(),
            AttributeValue::S("tenant#1".to_string()),
        )]))
        .build();

    let err = db
        .query_table_map(query)
        .await
        .expect_err("query should fail for unused expression names");
    assert_eq!(
        err.to_string(),
        "Value provided in ExpressionAttributeNames unused in expressions: keys: {#pk}"
    );
}

#[tokio::test]
async fn put_item_stamps_updated_at_millis() {
    let db = create_single_table_mode_db().await;
    let table_name = TableName::new("put_item_updated_at");
    create_hash_table(&db, &table_name).await;

    db.put_item(
        crate::PutItemInput::builder()
            .table_name(table_name.clone())
            .item(HashMap::from([
                ("pk".to_string(), AttributeValue::S("item#1".to_string())),
                (
                    "payload".to_string(),
                    AttributeValue::S("value".to_string()),
                ),
            ]))
            .build(),
    )
    .await
    .expect("put item");

    let stored = db
        .get_item_map(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))]),
        )
        .await
        .expect("get item")
        .expect("item should exist");
    assert!(
        read_updated_at_ms(&stored) > 0,
        "updated_at should be stamped by DatabaseManager"
    );
}

#[tokio::test]
async fn put_item_never_stamps_updated_at_before_created_at() {
    let db = create_single_table_mode_db().await;
    let table_name = TableName::new("put_item_created_at_floor");
    create_hash_table(&db, &table_name).await;
    let created_at_ms = TimestampMillis::now().timestamp_millis() + 60_000;

    db.put_item(
        crate::PutItemInput::builder()
            .table_name(table_name.clone())
            .item(HashMap::from([
                ("pk".to_string(), AttributeValue::S("item#1".to_string())),
                (
                    "c_at".to_string(),
                    AttributeValue::N(created_at_ms.to_string()),
                ),
            ]))
            .build(),
    )
    .await
    .expect("put item");

    let stored = db
        .get_item_map(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))]),
        )
        .await
        .expect("get item")
        .expect("item should exist");
    assert_eq!(read_updated_at_ms(&stored), created_at_ms);
}

#[tokio::test]
async fn update_item_injects_updated_at_when_expression_has_no_set_clause() {
    let db = create_single_table_mode_db().await;
    let table_name = TableName::new("update_item_updated_at");
    create_hash_table(&db, &table_name).await;

    db.put_item(
        crate::PutItemInput::builder()
            .table_name(table_name.clone())
            .item(HashMap::from([
                ("pk".to_string(), AttributeValue::S("item#1".to_string())),
                (
                    "payload".to_string(),
                    AttributeValue::S("value".to_string()),
                ),
            ]))
            .build(),
    )
    .await
    .expect("put item");
    let before = db
        .get_item_map(
            table_name.clone(),
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))]),
        )
        .await
        .expect("get before")
        .expect("before exists");
    let before_updated_at = read_updated_at_ms(&before);

    db.update_item(
        crate::UpdateItemInput::builder()
            .table_name(table_name.clone())
            .key(HashMap::from([(
                "pk".to_string(),
                AttributeValue::S("item#1".to_string()),
            )]))
            .update_expression("REMOVE payload".to_string())
            .build(),
    )
    .await
    .expect("update item");

    let after = db
        .get_item_map(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))]),
        )
        .await
        .expect("get after")
        .expect("after exists");
    let after_updated_at = read_updated_at_ms(&after);
    assert!(
        after_updated_at >= before_updated_at,
        "updated_at should be refreshed on update path"
    );
}

#[tokio::test]
async fn update_item_preserves_add_clause_before_set_when_stamping_updated_at() {
    let db = create_single_table_mode_db().await;
    let table_name = TableName::new("update_item_preserve_add_clause");
    create_hash_table(&db, &table_name).await;

    db.put_item(
        crate::PutItemInput::builder()
            .table_name(table_name.clone())
            .item(HashMap::from([(
                "pk".to_string(),
                AttributeValue::S("metric#count".to_string()),
            )]))
            .build(),
    )
    .await
    .expect("seed counter item");

    db.update_item(
        crate::UpdateItemInput::builder()
            .table_name(table_name.clone())
            .key(HashMap::from([(
                "pk".to_string(),
                AttributeValue::S("metric#count".to_string()),
            )]))
            .update_expression("ADD #count :one SET #hour = :hour".to_string())
            .expression_attribute_names(HashMap::from([
                ("#count".to_string(), "count".to_string()),
                ("#hour".to_string(), "hour".to_string()),
            ]))
            .expression_attribute_values(HashMap::from([
                (":one".to_string(), AttributeValue::N("1".to_string())),
                (
                    ":hour".to_string(),
                    AttributeValue::N("1700000000".to_string()),
                ),
            ]))
            .build(),
    )
    .await
    .expect("update should preserve ADD clause and succeed");

    let stored = db
        .get_item_map(
            table_name,
            HashMap::from([(
                "pk".to_string(),
                AttributeValue::S("metric#count".to_string()),
            )]),
        )
        .await
        .expect("get counter")
        .expect("counter item should exist");

    assert_eq!(
        stored.get("count"),
        Some(&AttributeValue::N("1".to_string()))
    );
    assert_eq!(
        stored.get("hour"),
        Some(&AttributeValue::N("1700000000".to_string()))
    );
    assert!(read_updated_at_ms(&stored) > 0);
}

#[tokio::test]
async fn batch_write_item_stamps_updated_at() {
    let db = create_single_table_mode_db().await;
    let table_name = TableName::new("batch_write_updated_at");
    create_hash_table(&db, &table_name).await;

    let request = BatchWriteItemRequest {
        request_items: HashMap::from([(
            table_name.clone(),
            vec![WriteRequest {
                put_request: Some(PutRequest {
                    item: HashMap::from([
                        ("pk".to_string(), AttributeValue::S("item#1".to_string())),
                        (
                            "payload".to_string(),
                            AttributeValue::S("value".to_string()),
                        ),
                    ]),
                    aux_item_stream_ttl_hours: None,
                }),
                delete_request: None,
            }],
        )]),
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    };
    db.batch_write_item(request)
        .await
        .expect("batch write should succeed");

    let stored = db
        .get_item_map(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))]),
        )
        .await
        .expect("get item")
        .expect("item should exist");
    assert!(read_updated_at_ms(&stored) > 0);
}

#[tokio::test]
async fn transact_write_items_stamps_updated_at_for_put_and_update() {
    let db = create_single_table_mode_db().await;
    let table_name = TableName::new("transact_write_updated_at");
    create_hash_table(&db, &table_name).await;

    db.put_item(
        crate::PutItemInput::builder()
            .table_name(table_name.clone())
            .item(HashMap::from([(
                "pk".to_string(),
                AttributeValue::S("item#existing".to_string()),
            )]))
            .build(),
    )
    .await
    .expect("seed existing item");
    let existing_before = db
        .get_item_map(
            table_name.clone(),
            HashMap::from([(
                "pk".to_string(),
                AttributeValue::S("item#existing".to_string()),
            )]),
        )
        .await
        .expect("get existing before")
        .expect("existing before should exist");
    let before_updated_at = read_updated_at_ms(&existing_before);

    db.transact_write_items(TransactWriteItemsRequest {
        transact_items: vec![
            TransactWriteItem {
                put: Some(TransactPutRequest {
                    table_name: table_name.clone(),
                    item: HashMap::from([
                        ("pk".to_string(), AttributeValue::S("item#new".to_string())),
                        (
                            "payload".to_string(),
                            AttributeValue::S("value".to_string()),
                        ),
                    ]),
                    condition_expression: None,
                    expression_attribute_names: None,
                    expression_attribute_values: None,
                    return_values_on_condition_check_failure: None,
                    aux_item_stream_ttl_hours: None,
                }),
                update: None,
                delete: None,
                condition_check: None,
            },
            TransactWriteItem {
                put: None,
                update: Some(TransactUpdateRequest {
                    table_name: table_name.clone(),
                    key: HashMap::from([(
                        "pk".to_string(),
                        AttributeValue::S("item#existing".to_string()),
                    )])
                    .into(),
                    update_expression: "REMOVE payload".to_string(),
                    condition_expression: None,
                    expression_attribute_names: None,
                    expression_attribute_values: None,
                    return_values_on_condition_check_failure: None,
                    aux_item_stream_ttl_hours: None,
                }),
                delete: None,
                condition_check: None,
            },
        ],
        client_request_token: None,
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    })
    .await
    .expect("transact write should succeed");

    let inserted = db
        .get_item_map(
            table_name.clone(),
            HashMap::from([("pk".to_string(), AttributeValue::S("item#new".to_string()))]),
        )
        .await
        .expect("get inserted")
        .expect("inserted should exist");
    assert!(read_updated_at_ms(&inserted) > 0);

    let existing_after = db
        .get_item_map(
            table_name,
            HashMap::from([(
                "pk".to_string(),
                AttributeValue::S("item#existing".to_string()),
            )]),
        )
        .await
        .expect("get existing after")
        .expect("existing after should exist");
    assert!(
        read_updated_at_ms(&existing_after) >= before_updated_at,
        "transact update should refresh updated_at"
    );
}

#[tokio::test]
async fn transact_write_items_encode_stamps_updated_at_for_put_and_update() {
    let db = create_single_table_mode_db().await;
    let table_name = TableName::new("transact_write_encode_updated_at");
    create_hash_table(&db, &table_name).await;

    db.put_item(
        crate::PutItemInput::builder()
            .table_name(table_name.clone())
            .item(HashMap::from([(
                "pk".to_string(),
                AttributeValue::S("item#existing".to_string()),
            )]))
            .build(),
    )
    .await
    .expect("seed existing item");

    let encoded_put = WireItem::from_attribute_map(&HashMap::from([
        ("pk".to_string(), AttributeValue::S("item#new".to_string())),
        (
            "payload".to_string(),
            AttributeValue::S("value".to_string()),
        ),
    ]))
    .expect("encode put item");
    db.transact_write_items_encode(TransactWriteItemsEncodeRequest {
        transact_items: vec![
            TransactEncodeItem {
                put: Some(TransactEncodePutRequest {
                    table_name: table_name.clone(),
                    item: encoded_put,
                    condition_expression: None,
                    expression_attribute_names: None,
                    expression_attribute_values: None,
                    return_values_on_condition_check_failure: None,
                    aux_item_stream_ttl_hours: None,
                }),
                update: None,
                delete: None,
                condition_check: None,
            },
            TransactEncodeItem {
                put: None,
                update: Some(TransactUpdateRequest {
                    table_name: table_name.clone(),
                    key: HashMap::from([(
                        "pk".to_string(),
                        AttributeValue::S("item#existing".to_string()),
                    )])
                    .into(),
                    update_expression: "REMOVE payload".to_string(),
                    condition_expression: None,
                    expression_attribute_names: None,
                    expression_attribute_values: None,
                    return_values_on_condition_check_failure: None,
                    aux_item_stream_ttl_hours: None,
                }),
                delete: None,
                condition_check: None,
            },
        ],
        client_request_token: None,
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    })
    .await
    .expect("transact write encode should succeed");

    let inserted = db
        .get_item_map(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#new".to_string()))]),
        )
        .await
        .expect("get inserted")
        .expect("inserted should exist");
    assert!(read_updated_at_ms(&inserted) > 0);
}

#[tokio::test]
async fn default_transact_write_items_encode_refreshes_entity_timestamp_payloads() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("default_transact_entity_timestamp");
    create_pk_sk_table(&db, &table_name).await;
    let entity = TestTimestampEntity {
        entity_id: "one".to_string(),
        updated_at: 1,
    };

    db.transact_write_items_encode(TransactWriteItemsEncodeRequest {
        transact_items: vec![TransactEncodeItem {
            put: Some(TransactEncodePutRequest {
                table_name: table_name.clone(),
                item: storage_types::single_table_entity::to_wire_item_fast(&entity)
                    .expect("encode entity"),
                condition_expression: None,
                expression_attribute_names: None,
                expression_attribute_values: None,
                return_values_on_condition_check_failure: None,
                aux_item_stream_ttl_hours: None,
            }),
            update: None,
            delete: None,
            condition_check: None,
        }],
        client_request_token: None,
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    })
    .await
    .expect("transaction write should succeed");

    let stored = db
        .get_item_map(table_name, entity.table_key_map())
        .await
        .expect("get item")
        .expect("item should exist");
    assert!(read_updated_at_ms(&stored) > 1);
}

#[tokio::test]
async fn batch_write_item_encode_stamps_updated_at() {
    let db = create_single_table_mode_db().await;
    let table_name = TableName::new("batch_write_encode_updated_at");
    create_hash_table(&db, &table_name).await;

    let put_item = WireItem::from_attribute_map(&HashMap::from([
        ("pk".to_string(), AttributeValue::S("item#1".to_string())),
        (
            "payload".to_string(),
            AttributeValue::S("value".to_string()),
        ),
    ]))
    .expect("encode put request");
    db.batch_write_item_encode(BatchWriteItemEncodeRequest {
        request_items: HashMap::from([(
            table_name.clone(),
            vec![EncodeWriteRequest {
                put_request: Some(EncodePutRequest {
                    item: put_item,
                    aux_item_stream_ttl_hours: None,
                }),
                delete_request: None,
            }],
        )]),
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    })
    .await
    .expect("batch write encode should succeed");

    let stored = db
        .get_item_map(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))]),
        )
        .await
        .expect("get item")
        .expect("item should exist");
    assert!(read_updated_at_ms(&stored) > 0);
}

#[tokio::test]
async fn transact_write_items_encode_preserves_add_clause_before_set_when_stamping_updated_at() {
    let db = create_single_table_mode_db().await;
    let table_name = TableName::new("transact_encode_preserve_add_clause");
    create_hash_table(&db, &table_name).await;

    db.put_item(
        crate::PutItemInput::builder()
            .table_name(table_name.clone())
            .item(HashMap::from([(
                "pk".to_string(),
                AttributeValue::S("metric#count".to_string()),
            )]))
            .build(),
    )
    .await
    .expect("seed counter item");

    db.transact_write_items_encode(TransactWriteItemsEncodeRequest {
        transact_items: vec![TransactEncodeItem {
            put: None,
            update: Some(TransactUpdateRequest {
                table_name: table_name.clone(),
                key: HashMap::from([(
                    "pk".to_string(),
                    AttributeValue::S("metric#count".to_string()),
                )])
                .into(),
                update_expression: "ADD #count :one SET #hour = :hour".to_string(),
                condition_expression: None,
                expression_attribute_names: Some(HashMap::from([
                    ("#count".to_string(), "count".to_string()),
                    ("#hour".to_string(), "hour".to_string()),
                ])),
                expression_attribute_values: Some(HashMap::from([
                    (":one".to_string(), AttributeValue::N("1".to_string())),
                    (
                        ":hour".to_string(),
                        AttributeValue::N("1700000000".to_string()),
                    ),
                ])),
                return_values_on_condition_check_failure: None,
                aux_item_stream_ttl_hours: None,
            }),
            delete: None,
            condition_check: None,
        }],
        client_request_token: None,
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    })
    .await
    .expect("transact write encode should keep ADD clause and succeed");

    let stored = db
        .get_item_map(
            table_name,
            HashMap::from([(
                "pk".to_string(),
                AttributeValue::S("metric#count".to_string()),
            )]),
        )
        .await
        .expect("get counter")
        .expect("counter item should exist");

    assert_eq!(
        stored.get("count"),
        Some(&AttributeValue::N("1".to_string())),
        "ADD clause must be preserved when updated_at is injected"
    );
    assert_eq!(
        stored.get("hour"),
        Some(&AttributeValue::N("1700000000".to_string()))
    );
    assert!(read_updated_at_ms(&stored) > 0);
}

#[tokio::test]
async fn capped_entity_helpers_work_in_default_manager_mode() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("default_capped_entity");
    create_pk_sk_table(&db, &table_name).await;
    let entity = TestCappedEntity {
        entity_id: "one".to_string(),
        payload: "payload-one".to_string(),
    };

    db.create_capped_entity(
        CreateCappedEntityInput::builder()
            .table_name(table_name.clone())
            .item(&entity)
            .counted_entity_type("PLATFORM_BILLING_CATALOG_PRODUCT".to_string())
            .max_value(2_u64)
            .build(),
    )
    .await
    .expect("create capped entity");

    db.delete_capped_entity(
        DeleteCappedEntityInput::builder()
            .table_name(table_name)
            .key(entity.table_key_map())
            .counted_entity_type("PLATFORM_BILLING_CATALOG_PRODUCT".to_string())
            .build(),
    )
    .await
    .expect("delete capped entity");
}

#[tokio::test]
async fn create_capped_entity_enforces_capacity_and_tracks_counter() {
    let db = create_single_table_mode_db().await;
    let table_name = TableName::new("create_capped_entity");
    create_pk_sk_table(&db, &table_name).await;

    for entity_id in ["one", "two"] {
        let entity = TestCappedEntity {
            entity_id: entity_id.to_string(),
            payload: format!("payload-{entity_id}"),
        };
        db.create_capped_entity(
            CreateCappedEntityInput::builder()
                .table_name(table_name.clone())
                .item(&entity)
                .counted_entity_type("PLATFORM_BILLING_CATALOG_PRODUCT".to_string())
                .max_value(2_u64)
                .build(),
        )
        .await
        .expect("create capped entity");
    }

    let third = TestCappedEntity {
        entity_id: "three".to_string(),
        payload: "payload-three".to_string(),
    };
    let err = db
        .create_capped_entity(
            CreateCappedEntityInput::builder()
                .table_name(table_name.clone())
                .item(&third)
                .counted_entity_type("PLATFORM_BILLING_CATALOG_PRODUCT".to_string())
                .max_value(2_u64)
                .build(),
        )
        .await
        .expect_err("third create should exceed capacity");
    assert!(matches!(err, CappedStorageError::CapacityExceededError));

    let counter = db
        .get_item_map(
            table_name,
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("COUNT".to_string())),
                (
                    "sk".to_string(),
                    AttributeValue::S("PLATFORM_BILLING_CATALOG_PRODUCT".to_string()),
                ),
            ]),
        )
        .await
        .expect("get counter")
        .expect("counter should exist");
    assert_eq!(read_count_value(&counter), 2);
}

#[tokio::test]
async fn create_capped_entity_reports_existing_item_before_capacity() {
    let db = create_single_table_mode_db().await;
    let table_name = TableName::new("create_capped_existing");
    create_pk_sk_table(&db, &table_name).await;

    let entity = TestCappedEntity {
        entity_id: "one".to_string(),
        payload: "payload-one".to_string(),
    };
    db.create_capped_entity(
        CreateCappedEntityInput::builder()
            .table_name(table_name.clone())
            .item(&entity)
            .counted_entity_type("PLATFORM_BILLING_CATALOG_PRODUCT".to_string())
            .max_value(2_u64)
            .build(),
    )
    .await
    .expect("seed capped entity");

    let err = db
        .create_capped_entity(
            CreateCappedEntityInput::builder()
                .table_name(table_name)
                .item(&entity)
                .counted_entity_type("PLATFORM_BILLING_CATALOG_PRODUCT".to_string())
                .max_value(2_u64)
                .build(),
        )
        .await
        .expect_err("duplicate create should fail");
    assert!(matches!(err, CappedStorageError::ItemExistError));
}

#[tokio::test]
async fn delete_capped_entity_decrements_counter_and_is_missing_aware() {
    let db = create_single_table_mode_db().await;
    let table_name = TableName::new("delete_capped_entity");
    create_pk_sk_table(&db, &table_name).await;

    let entity = TestCappedEntity {
        entity_id: "one".to_string(),
        payload: "payload-one".to_string(),
    };
    db.create_capped_entity(
        CreateCappedEntityInput::builder()
            .table_name(table_name.clone())
            .item(&entity)
            .counted_entity_type("PLATFORM_BILLING_CATALOG_PRODUCT".to_string())
            .max_value(2_u64)
            .build(),
    )
    .await
    .expect("seed capped entity");

    db.delete_capped_entity(
        DeleteCappedEntityInput::builder()
            .table_name(table_name.clone())
            .key(entity.table_key_map())
            .counted_entity_type("PLATFORM_BILLING_CATALOG_PRODUCT".to_string())
            .build(),
    )
    .await
    .expect("delete capped entity");

    let counter = db
        .get_item_map(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("COUNT".to_string())),
                (
                    "sk".to_string(),
                    AttributeValue::S("PLATFORM_BILLING_CATALOG_PRODUCT".to_string()),
                ),
            ]),
        )
        .await
        .expect("get counter")
        .expect("counter should exist");
    assert_eq!(read_count_value(&counter), 0);

    let err = db
        .delete_capped_entity(
            DeleteCappedEntityInput::builder()
                .table_name(table_name)
                .key(entity.table_key_map())
                .counted_entity_type("PLATFORM_BILLING_CATALOG_PRODUCT".to_string())
                .build(),
        )
        .await
        .expect_err("second delete should report missing item");
    assert!(matches!(err, CappedStorageError::ItemNotExistsError));
}
