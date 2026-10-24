use std::collections::HashMap;

use serde::Serialize;
use storage_derive::{SingleTableKeys, WireItemEncode};
#[cfg(feature = "foundationdb")]
use storage_provider::FoundationDbSettings;
use storage_provider::{SqliteSettings, StorageBackend, StorageConfig};
use storage_types::{
    AttributeDefinition, AttributeValue, BatchWriteItemEncodeRequest, BatchWriteItemRequest,
    CreateGlobalSecondaryIndex, CreateTableRequest, EncodePutRequest, EncodeWriteRequest,
    IndexName, KeyAttributeType, KeySchemaElement, KeyType, Projection, ProjectionType, PutRequest,
    StreamSpecification, StreamViewType, TableName, TableNamespace, TimestampMillis,
    TransactEncodeItem, TransactEncodePutRequest, TransactPutRequest, TransactUpdateRequest,
    TransactWriteItem, TransactWriteItemsEncodeRequest, TransactWriteItemsRequest, WireItem,
    WriteRequest, context::WrappedError as _, project_attribute_map,
    single_table_entity::SingleTableEntity,
};

use crate::{
    CappedStorageError, CreateCappedEntityInput, DatabaseManager, DatabaseManagerRuntimeOptions,
    QueryIndexInput, QueryTableInput, Tables, database_manager::DeleteCappedEntityInput,
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
    let mut request = CreateTableRequest::new(
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
    request.stream_specification = Some(StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(StreamViewType::NewAndOldImages),
    });
    db.create_table(&request).await.expect("create table");
}

#[tokio::test]
async fn given_valid_system_and_namespace_like_table_names_when_crud_runs_then_names_are_not_routed()
 {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");

    for table_name in [
        TableName::new("system"),
        TableName::new("ns_1BCDEFGHJKMNPQRSTVWXYZ0"),
    ] {
        create_hash_table(&db, &table_name).await;
        let key = HashMap::from([("pk".to_string(), AttributeValue::S("item".to_string()))]);
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(HashMap::from([(
                    "pk".to_string(),
                    AttributeValue::S("item".to_string()),
                )]))
                .build(),
        )
        .await
        .expect("valid table name should use the logical table directly");
        assert!(
            db.get_item_decode::<_, storage_types::AttributeMap>(table_name, key)
                .await
                .expect("read direct logical table")
                .is_some()
        );
    }
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

async fn create_indexed_pk_sk_table(db: &DatabaseManager, table_name: &TableName) {
    let mut request = CreateTableRequest::new(
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
    request.max_indexers = storage_types::MaxIndexers::try_new(1).expect("valid capacity");
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
async fn foundationdb_database_manager_bootstraps_system_tables() {
    let cluster_file = std::env::var("TEST_FDB_CLUSTER_FILE")
        .unwrap_or_else(|_| LOCAL_FDB_CLUSTER_FILE.to_string());
    assert!(
        std::path::Path::new(&cluster_file).is_file(),
        "FoundationDB manager test requires {cluster_file}"
    );
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
            cluster_file: Some(cluster_file),
            tenant_name: None,
            subspace_prefix: Some(format!("tests/storage-manager-bootstrap/{nanos}/")),
            cache_read_version_ms: 0,
            immediate_gsi_consistency: true,
            transaction_byte_limit: None,
        }),
        remote: None,
    };

    let db = DatabaseManager::new_with_config_and_runtime_options(
        config,
        DatabaseManagerRuntimeOptions::builder()
            .enable_database_jobs(false)
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

#[derive(Debug, Clone, Serialize, WireItemEncode)]
struct TestCappedEntity {
    entity_id: String,
    payload: String,
}

#[derive(Debug, Clone, Serialize, WireItemEncode)]
struct TestTimestampEntity {
    entity_id: String,
    updated_at: i64,
}

#[derive(Debug, Serialize, SingleTableKeys, WireItemEncode)]
#[single_table(
    entity_type = "INDEXED_ORDER",
    pk_lit = "ORDER",
    sk_expr = "format!(\"ORDER#{}\", self.order_id)"
)]
struct IndexedOrder {
    order_id: String,
    #[single_table(indexer = 0)]
    customer_id: String,
    updated_at: TimestampMillis,
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

fn read_updated_at_ms(item: &storage_types::AttributeMap) -> i64 {
    match item.get("u_at").or_else(|| item.get("updated_at")) {
        Some(AttributeValue::N(value)) => value.parse::<i64>().expect("updated_at should parse"),
        other => panic!("expected numeric updated_at, got: {other:?}"),
    }
}

fn read_count_value(item: &storage_types::AttributeMap) -> u64 {
    match item.get("value") {
        Some(AttributeValue::N(value)) => value.parse::<u64>().expect("count should parse"),
        other => panic!("expected numeric count value, got: {other:?}"),
    }
}

#[test]
fn query_index_input_conversion_preserves_projection_expression() {
    let request = storage_types::QueryTableRequest::from(
        QueryIndexInput::builder()
            .table_name(TableName::new("tenant_data"))
            .index_name(IndexName::new("gsi1"))
            .key_condition_expression("#pk = :pk".to_string())
            .expression_attribute_names(HashMap::from([("#pk".to_string(), "gsi1pk".to_string())]))
            .expression_attribute_values(HashMap::from([(
                ":pk".to_string(),
                AttributeValue::S("tenant#1".to_string()),
            )]))
            .projection_expression("pk, sk, emails".to_string())
            .build(),
    );

    assert_eq!(
        request.projection_expression.as_deref(),
        Some("pk, sk, emails")
    );
}

fn assert_no_single_table_metadata(item: &storage_types::AttributeMap) {
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

    db.clear_all_tables().await.expect("clear all tables");

    let has_user_table = db
        .table_exists(&user_table)
        .await
        .expect("query user table existence");
    assert!(
        !has_user_table,
        "clear_all_tables should remove user tables"
    );

    let has_r_table = db
        .table_exists(&TableName::new("rep"))
        .await
        .expect("query r table existence");
    assert!(
        !has_r_table,
        "legacy replication tables must not be recreated"
    );

    for table in [Tables::sys_namespaces(), Tables::sys_analytics()] {
        assert!(
            db.table_exists(&table)
                .await
                .expect("query bootstrap table existence"),
            "clear_all_tables should recreate {table} table"
        );
    }
}

#[tokio::test]
async fn given_fresh_sqlite_manager_when_reopened_then_system_gsi_done_state_is_persisted() {
    let database_path =
        crate::storage_test_support_tests::unique_path("sqlite-system-gsi-bootstrap");
    let config = StorageConfig {
        backend_type: StorageBackend::SQLite,
        connection_string: Some(database_path.to_string_lossy().into_owned()),
        file_path: None,
        sqlite: Some(SqliteSettings {
            immediate_gsi_consistency: true,
            force_file_backed_database: true,
        }),
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    };
    let runtime_options = DatabaseManagerRuntimeOptions::builder()
        .enable_database_jobs(false)
        .build();

    let db = DatabaseManager::new_with_config_and_runtime_options(
        config.clone(),
        runtime_options.clone(),
    )
    .await
    .expect("fresh sqlite database manager should bootstrap system GSIs");
    assert!(
        db.table_exists(&Tables::sys_namespaces())
            .await
            .expect("query fresh system namespaces table")
    );
    drop(db);

    let reopened = DatabaseManager::new_with_config_and_runtime_options(config, runtime_options)
        .await
        .expect("reopened sqlite database manager should preload system GSI");
    assert!(
        reopened
            .table_exists(&Tables::sys_namespaces())
            .await
            .expect("query reopened system namespaces table")
    );
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

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sys_namespaces_bootstrap_upgrades_existing_table_without_streams() {
    let db = crate::database_manager::construction_tests::sqlite_manager_without_system_tables()
        .await
        .expect("create initialized SQLite manager");

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

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn namespace_table_bootstrap_enables_streams_for_analytics_polling() {
    let db = crate::database_manager::construction_tests::sqlite_manager_without_system_tables()
        .await
        .expect("create initialized SQLite manager");
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
    assert_eq!(table_info.max_indexers.get(), 2);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn given_created_namespace_when_bootstrapped_again_then_existing_table_is_preserved() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let namespace = TableNamespace::from_seed("repeated-namespace-bootstrap");
    Tables::create_namespace_table(&db, &namespace)
        .await
        .expect("create namespace table");
    let before = db
        .get_table_info(&Tables::namespace(&namespace))
        .await
        .expect("read existing namespace table");

    Tables::create_namespace_table(&db, &namespace)
        .await
        .expect("repeated namespace bootstrap must succeed");

    let after = db
        .get_table_info(&Tables::namespace(&namespace))
        .await
        .expect("read preserved namespace table");
    assert_eq!(
        serde_json::to_value((
            after.key_schema,
            after.max_indexers,
            after.stream_specification,
        ))
        .expect("serialize preserved schema"),
        serde_json::to_value((
            before.key_schema,
            before.max_indexers,
            before.stream_specification,
        ))
        .expect("serialize original schema")
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn namespace_table_bootstrap_upgrades_existing_table_without_streams() {
    let db = crate::database_manager::construction_tests::sqlite_manager_without_system_tables()
        .await
        .expect("create initialized SQLite manager");
    let namespace = TableNamespace::system();
    let table_name = Tables::namespace(&namespace);

    create_pk_sk_table(&db, &table_name).await;

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
    assert_eq!(table_info.max_indexers.get(), 2);
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
        .get_item_decode::<_, storage_types::AttributeMap>(
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

    assert_eq!(response.records().len(), 1);
    assert_eq!(
        response.records()[0].cursor.as_deref(),
        Some(response.records()[0].sequence_number.as_str())
    );
    assert_eq!(
        response.records()[0].keys.get("pk"),
        Some(&AttributeValue::S("item#1".to_string()))
    );
    assert_eq!(
        response.records()[0]
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
    assert!(empty_response.records().is_empty());

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
            empty_response.last_evaluated_key(),
            Some(10),
        )
        .await
        .expect("read future stream records");

    assert_eq!(future_response.records().len(), 1);
    assert_eq!(
        future_response.records()[0].keys.get("pk"),
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
        request_items: storage_types::VecMap::from_iter([(
            table_name.clone(),
            vec![WriteRequest {
                put_request: Some(PutRequest {
                    item: WireItem::try_from(&HashMap::from([
                        ("pk".to_string(), AttributeValue::S("item#1".to_string())),
                        (
                            "payload".to_string(),
                            AttributeValue::S("customer-value".to_string()),
                        ),
                    ]))
                    .expect("encode item"),
                    indexers: None,
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
        .get_item_decode::<_, storage_types::AttributeMap>(
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
async fn batch_write_rejects_invalid_indexer_declaration_before_writing() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("batch_invalid_indexer");
    let mut create = CreateTableRequest::new(
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
    create.max_indexers = storage_types::MaxIndexers::try_new(1).expect("valid capacity");
    db.create_table(&create).await.expect("create table");

    let error = db
        .batch_write_item(BatchWriteItemRequest {
            request_items: storage_types::VecMap::from_iter([(
                table_name.clone(),
                vec![WriteRequest {
                    put_request: Some(PutRequest {
                        item: WireItem::try_from(&HashMap::from([
                            ("pk".to_string(), AttributeValue::S("item#1".to_string())),
                            (
                                "customer_id".to_string(),
                                AttributeValue::N("7".to_string()),
                            ),
                        ]))
                        .expect("encode item"),
                        indexers: Some(vec!["customer_id".to_string()]),
                        aux_item_stream_ttl_hours: None,
                    }),
                    delete_request: None,
                }],
            )]),
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
        })
        .await
        .expect_err("non-string indexer must fail the request");

    assert!(
        error
            .to_string()
            .contains("Indexers:attribute_must_be_string")
    );
    assert!(
        db.get_item_decode::<_, storage_types::AttributeMap>(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))]),
        )
        .await
        .expect("get item")
        .is_none(),
        "validation must happen before any write",
    );
}

#[tokio::test]
async fn default_put_item_preserves_customer_timestamp_attributes() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("default_put_existing_timestamp");
    create_hash_table(&db, &table_name).await;
    let item = HashMap::from([
        ("pk".to_string(), AttributeValue::S("item#1".to_string())),
        (
            storage_types::single_table_entity::UPDATED_AT_ATTR.to_string(),
            AttributeValue::N("1".to_string()),
        ),
        (
            storage_types::single_table_entity::UPDATED_AT_ALIAS_ATTR.to_string(),
            AttributeValue::N("2".to_string()),
        ),
        (
            storage_types::single_table_entity::CREATED_AT_ATTR.to_string(),
            AttributeValue::N("3".to_string()),
        ),
        (
            storage_types::single_table_entity::CREATED_AT_ALIAS_ATTR.to_string(),
            AttributeValue::N("4".to_string()),
        ),
    ]);

    db.put_item(
        crate::PutItemInput::builder()
            .table_name(table_name.clone())
            .item(item.clone())
            .build(),
    )
    .await
    .expect("put item");

    let stored = db
        .get_item_decode::<_, storage_types::AttributeMap>(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))]),
        )
        .await
        .expect("get item")
        .expect("item should exist");
    assert_eq!(stored, storage_types::AttributeMap::from(item));
}

#[tokio::test]
async fn default_batch_write_item_preserves_timestamp_payloads() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("default_batch_entity_timestamp");
    create_pk_sk_table(&db, &table_name).await;
    let entity = TestTimestampEntity {
        entity_id: "one".to_string(),
        updated_at: 1,
    };
    let item = storage_types::single_table_entity::to_wire_entity(&entity)
        .expect("encode entity")
        .into_write_parts()
        .0
        .to_attribute_map()
        .expect("decode entity");
    let expected = item.clone();

    db.batch_write_item(BatchWriteItemRequest {
        request_items: storage_types::VecMap::from_iter([(
            table_name.clone(),
            vec![WriteRequest {
                put_request: Some(PutRequest {
                    item: WireItem::try_from(item).expect("encode item"),
                    indexers: None,
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
        .get_item_decode::<_, storage_types::AttributeMap>(table_name, entity.table_key_map())
        .await
        .expect("get item")
        .expect("item should exist");
    assert_eq!(stored, expected);
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
        .get_item_decode::<_, storage_types::AttributeMap>(
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
async fn default_update_item_preserves_existing_timestamp_attributes() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("default_update_existing_timestamp");
    create_hash_table(&db, &table_name).await;
    let item = HashMap::from([
        ("pk".to_string(), AttributeValue::S("item#1".to_string())),
        (
            storage_types::single_table_entity::UPDATED_AT_ATTR.to_string(),
            AttributeValue::N("1".to_string()),
        ),
        (
            storage_types::single_table_entity::UPDATED_AT_ALIAS_ATTR.to_string(),
            AttributeValue::N("2".to_string()),
        ),
        (
            storage_types::single_table_entity::CREATED_AT_ATTR.to_string(),
            AttributeValue::N("3".to_string()),
        ),
        (
            storage_types::single_table_entity::CREATED_AT_ALIAS_ATTR.to_string(),
            AttributeValue::N("4".to_string()),
        ),
    ]);

    db.put_item(
        crate::PutItemInput::builder()
            .table_name(table_name.clone())
            .item(item.clone())
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
        .get_item_decode::<_, storage_types::AttributeMap>(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))]),
        )
        .await
        .expect("get item")
        .expect("item should exist");
    let mut expected = item;
    expected.insert(
        "payload".to_string(),
        AttributeValue::S("customer-value".to_string()),
    );
    assert_eq!(stored, storage_types::AttributeMap::from(expected));
}

#[tokio::test]
async fn custom_gsi_query_projection_returns_only_projected_selected_attributes() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("default_custom_gsi_numeric_sort");
    let index_name = IndexName::new("by_score");
    let mut request = CreateTableRequest::new(
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
    );
    request.global_secondary_indexes = Some(vec![CreateGlobalSecondaryIndex {
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
            projection_type: Some(ProjectionType::KeysOnly),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }]);
    db.create_table(&request).await.expect("create table");

    db.batch_write_item(BatchWriteItemRequest {
        request_items: storage_types::VecMap::from_iter([(
            table_name.clone(),
            vec![WriteRequest {
                put_request: Some(PutRequest {
                    item: WireItem::try_from(&HashMap::from([
                        ("pk".to_string(), AttributeValue::S("item#1".to_string())),
                        (
                            "category".to_string(),
                            AttributeValue::S("games".to_string()),
                        ),
                        ("score".to_string(), AttributeValue::N("42".to_string())),
                    ]))
                    .expect("encode item"),
                    indexers: None,
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

    let expression_attribute_names =
        storage_types::VecMap::from_iter([("#selected".to_string(), "score".to_string())]);
    let page = db
        .query_index_map(
            QueryIndexInput::builder()
                .table_name(table_name.clone())
                .index_name(index_name.clone())
                .key_condition_expression("category = :category AND score = :score".to_string())
                .expression_attribute_names(expression_attribute_names.clone())
                .expression_attribute_values(HashMap::from([
                    (
                        ":category".to_string(),
                        AttributeValue::S("games".to_string()),
                    ),
                    (":score".to_string(), AttributeValue::N("42".to_string())),
                ]))
                .projection_expression("#selected".to_string())
                .build(),
        )
        .await
        .expect("query custom index");

    assert_eq!(page.items.len(), 1);
    assert_no_single_table_metadata(&page.items[0]);
    assert_eq!(page.items[0].len(), 1);
    assert_eq!(page.progress.evaluated_count, 1);
    assert!(page.progress.evaluated_bytes > 0);
    assert!(page.progress.last_evaluated_key.is_some());
    assert!(!page.items[0].contains_key("pk"));
    assert!(!page.items[0].contains_key("category"));
    assert_eq!(
        project_attribute_map(
            page.items[0].clone(),
            Some("#selected"),
            None,
            Some(&expression_attribute_names),
        )
        .get("score"),
        Some(&AttributeValue::N("42".to_string()))
    );

    let error = db
        .query_index_map(
            QueryIndexInput::builder()
                .table_name(table_name)
                .index_name(index_name.clone())
                .key_condition_expression("category = :category".to_string())
                .expression_attribute_values(HashMap::from([(
                    ":category".to_string(),
                    AttributeValue::S("games".to_string()),
                )]))
                .projection_expression("private_note".to_string())
                .build(),
        )
        .await
        .expect_err("KEYS_ONLY index must reject non-projected attributes");
    assert_eq!(
        error.to_string(),
        format!(
            "One or more parameter values were invalid: Global secondary index {index_name} does \
             not project [private_note]"
        )
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
async fn typed_entity_writes_apply_the_generated_indexer_declaration() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("typed_entity_indexers");
    create_indexed_pk_sk_table(&db, &table_name).await;
    let entity = IndexedOrder {
        order_id: "one".to_string(),
        customer_id: "customer#one".to_string(),
        updated_at: TimestampMillis::from_timestamp(1),
    };

    db.put_item_entity_encode(
        crate::PutItemEntityEncodeInput::builder()
            .table_name(table_name.clone())
            .item(&entity)
            .build(),
    )
    .await
    .expect("typed put");

    db.update_item_entity::<IndexedOrder>(
        crate::UpdateItemInput::builder()
            .table_name(table_name.clone())
            .key(entity.table_key_map())
            .update_expression("SET customer_id = :customer_id".to_string())
            .expression_attribute_values(HashMap::from([(
                ":customer_id".to_string(),
                AttributeValue::S("customer#two".to_string()),
            )]))
            .build(),
    )
    .await
    .expect("typed update");

    let error = db
        .update_item(
            crate::UpdateItemInput::builder()
                .table_name(table_name)
                .key(entity.table_key_map())
                .update_expression("SET customer_id = :customer_id".to_string())
                .expression_attribute_values(HashMap::from([(
                    ":customer_id".to_string(),
                    AttributeValue::N("7".to_string()),
                )]))
                .build(),
        )
        .await
        .expect_err("stored declaration must reject a non-string value");

    assert!(
        error
            .to_string()
            .contains("Indexers:attribute_must_be_string")
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
        .get_item_decode::<_, storage_types::AttributeMap>(
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
        .get_item_decode::<_, storage_types::AttributeMap>(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))]),
        )
        .await
        .expect("get item")
        .expect("item should exist");
    assert_eq!(read_updated_at_ms(&stored), created_at_ms);
}

#[tokio::test]
async fn given_explicit_timestamp_when_put_and_conditional_update_then_caller_clock_is_preserved() {
    let db = create_single_table_mode_db().await;
    let table = TableName::new("explicit_timestamp");
    create_hash_table(&db, &table).await;
    let key = storage_types::KeyAttributes::from_iter([(
        "pk".to_string(),
        AttributeValue::S("one".to_string()),
    )]);
    for field in ["updated_at", "u_at"] {
        let item = WireItem::dynamo_json(format!(r#"{{"pk":{{"S":"one"}},"c_at":{{"N":"1900000000000"}},"{field}":{{"N":"1900000000001"}}}}"#).into_bytes());
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table.clone())
                .item(item)
                .build(),
        )
        .await
        .expect("put explicit clock");
        let stored = db
            .get_item(table.clone(), key.clone())
            .await
            .expect("get item")
            .expect("stored item");
        assert_eq!(
            stored.number_attribute_i64(field).expect("read timestamp"),
            Some(1_900_000_000_001)
        );
    }
    let update = |expected: i64| {
        crate::UpdateItemInput::builder()
            .table_name(table.clone())
            .key(key.clone())
            .update_expression("SET u_at = :next".to_string())
            .condition_expression("u_at = :expected".to_string())
            .expression_attribute_values(storage_types::AttributeMap::from_iter([
                (
                    ":next".to_string(),
                    AttributeValue::N("1900000000002".to_string()),
                ),
                (
                    ":expected".to_string(),
                    AttributeValue::N(expected.to_string()),
                ),
            ]))
            .build()
    };
    db.update_item(update(1_900_000_000_001i64))
        .await
        .expect("conditional update");
    let error = db
        .update_item(update(1_900_000_000_001i64))
        .await
        .expect_err("stale clock must fail");
    assert!(error.is_conditional_write_contention());
    let stored = db
        .get_item(table, key)
        .await
        .expect("get updated item")
        .expect("stored item");
    assert_eq!(
        stored.number_attribute_i64("u_at").expect("read timestamp"),
        Some(1_900_000_000_002)
    );
}

#[tokio::test]
async fn given_explicit_timestamp_when_transaction_put_and_update_then_caller_clock_is_preserved() {
    let db = create_single_table_mode_db().await;
    let table = TableName::new("explicit_transaction_timestamp");
    create_hash_table(&db, &table).await;
    let key = storage_types::KeyAttributes::from_iter([(
        "pk".to_string(),
        AttributeValue::S("one".to_string()),
    )]);
    db.transact_write_items(TransactWriteItemsRequest {
        transact_items: vec![TransactWriteItem {
            put: Some(TransactPutRequest {
                table_name: table.clone(),
                item: WireItem::dynamo_json(
                    br#"{"pk":{"S":"one"},"u_at":{"N":"1900000000001"}}"#.to_vec(),
                ),
                indexers: None,
                condition_expression: Some("attribute_not_exists(pk)".to_string()),
                expression_attribute_names: None,
                expression_attribute_values: None,
                return_values_on_condition_check_failure: None,
                aux_item_stream_ttl_hours: None,
            }),
            ..Default::default()
        }],
        ..Default::default()
    })
    .await
    .expect("transaction put");
    let stored = db
        .get_item(table.clone(), key.clone())
        .await
        .expect("get inserted item")
        .expect("stored item");
    assert_eq!(
        stored.number_attribute_i64("u_at").expect("read timestamp"),
        Some(1_900_000_000_001)
    );
    db.transact_write_items(TransactWriteItemsRequest {
        transact_items: vec![TransactWriteItem {
            update: Some(TransactUpdateRequest {
                table_name: table.clone(),
                key: key.clone(),
                update_expression: "SET updated_at = :next".to_string(),
                condition_expression: Some("u_at = :expected".to_string()),
                indexers: None,
                expression_attribute_names: None,
                expression_attribute_values: Some(storage_types::AttributeMap::from_iter([
                    (
                        ":next".to_string(),
                        AttributeValue::N("1900000000002".to_string()),
                    ),
                    (
                        ":expected".to_string(),
                        AttributeValue::N("1900000000001".to_string()),
                    ),
                ])),
                return_values_on_condition_check_failure: None,
                aux_item_stream_ttl_hours: None,
            }),
            ..Default::default()
        }],
        ..Default::default()
    })
    .await
    .expect("transaction update");
    let stored = db
        .get_item(table, key)
        .await
        .expect("get updated item")
        .expect("stored item");
    assert_eq!(
        stored.number_attribute_i64("u_at").expect("read timestamp"),
        Some(1_900_000_000_002)
    );
}

#[tokio::test]
async fn given_missing_timestamp_value_when_update_then_request_is_rejected() {
    let db = create_single_table_mode_db().await;
    let table = TableName::new("missing_timestamp_value");
    create_hash_table(&db, &table).await;
    let key = storage_types::KeyAttributes::from_iter([(
        "pk".to_string(),
        AttributeValue::S("one".to_string()),
    )]);
    db.put_item(
        crate::PutItemInput::builder()
            .table_name(table.clone())
            .item(WireItem::dynamo_json(
                br#"{"pk":{"S":"one"},"u_at":{"N":"123"}}"#.to_vec(),
            ))
            .build(),
    )
    .await
    .expect("seed item");
    let error = db
        .update_item(
            crate::UpdateItemInput::builder()
                .table_name(table.clone())
                .key(key.clone())
                .update_expression("SET u_at = :missing".to_string())
                .build(),
        )
        .await
        .expect_err("missing value must fail");
    assert!(
        matches!(error.to_enum(), storage_types::StorageEnum::RawValidation { message }
        if message.contains("attribute value: :missing"))
    );
    let stored = db
        .get_item(table, key)
        .await
        .expect("get unchanged item")
        .expect("stored item");
    assert_eq!(
        stored.number_attribute_i64("u_at").expect("read timestamp"),
        Some(123)
    );
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
        .get_item_decode::<_, storage_types::AttributeMap>(
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
        .get_item_decode::<_, storage_types::AttributeMap>(
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
        .get_item_decode::<_, storage_types::AttributeMap>(
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
        request_items: storage_types::VecMap::from_iter([(
            table_name.clone(),
            vec![WriteRequest {
                put_request: Some(PutRequest {
                    item: WireItem::try_from(&HashMap::from([
                        ("pk".to_string(), AttributeValue::S("item#1".to_string())),
                        (
                            "payload".to_string(),
                            AttributeValue::S("value".to_string()),
                        ),
                    ]))
                    .expect("encode item"),
                    indexers: None,
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
        .get_item_decode::<_, storage_types::AttributeMap>(
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
        .get_item_decode::<_, storage_types::AttributeMap>(
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
                    item: WireItem::try_from(&HashMap::from([
                        ("pk".to_string(), AttributeValue::S("item#new".to_string())),
                        (
                            "payload".to_string(),
                            AttributeValue::S("value".to_string()),
                        ),
                    ]))
                    .expect("encode item"),
                    indexers: None,
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
                    indexers: None,
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
        .get_item_decode::<_, storage_types::AttributeMap>(
            table_name.clone(),
            HashMap::from([("pk".to_string(), AttributeValue::S("item#new".to_string()))]),
        )
        .await
        .expect("get inserted")
        .expect("inserted should exist");
    assert!(read_updated_at_ms(&inserted) > 0);

    let existing_after = db
        .get_item_decode::<_, storage_types::AttributeMap>(
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

    let encoded_put = WireItem::try_from(&HashMap::from([
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
                    item: storage_types::WireEntity::unindexed(encoded_put),
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
                    indexers: None,
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
        .get_item_decode::<_, storage_types::AttributeMap>(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#new".to_string()))]),
        )
        .await
        .expect("get inserted")
        .expect("inserted should exist");
    assert!(read_updated_at_ms(&inserted) > 0);
}

#[tokio::test]
async fn default_transact_write_items_encode_preserves_timestamp_payloads() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("default_transact_entity_timestamp");
    create_pk_sk_table(&db, &table_name).await;
    let entity = TestTimestampEntity {
        entity_id: "one".to_string(),
        updated_at: 1,
    };
    let encoded =
        storage_types::single_table_entity::to_wire_entity(&entity).expect("encode entity");
    let expected = encoded
        .clone()
        .into_write_parts()
        .0
        .to_attribute_map()
        .expect("decode entity");

    db.transact_write_items_encode(TransactWriteItemsEncodeRequest {
        transact_items: vec![TransactEncodeItem {
            put: Some(TransactEncodePutRequest {
                table_name: table_name.clone(),
                item: encoded,
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
        .get_item_decode::<_, storage_types::AttributeMap>(table_name, entity.table_key_map())
        .await
        .expect("get item")
        .expect("item should exist");
    assert_eq!(stored, expected);
}

#[tokio::test]
async fn batch_write_item_encode_stamps_updated_at() {
    let db = create_single_table_mode_db().await;
    let table_name = TableName::new("batch_write_encode_updated_at");
    create_hash_table(&db, &table_name).await;

    let put_item = WireItem::try_from(&HashMap::from([
        ("pk".to_string(), AttributeValue::S("item#1".to_string())),
        (
            "payload".to_string(),
            AttributeValue::S("value".to_string()),
        ),
    ]))
    .expect("encode put request");
    db.batch_write_item_encode(BatchWriteItemEncodeRequest {
        request_items: storage_types::VecMap::from_iter([(
            table_name.clone(),
            vec![EncodeWriteRequest {
                put_request: Some(EncodePutRequest {
                    item: storage_types::WireEntity::unindexed(put_item),
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
        .get_item_decode::<_, storage_types::AttributeMap>(
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
                indexers: None,
                condition_expression: None,
                expression_attribute_names: Some(storage_types::VecMap::from_iter([
                    ("#count".to_string(), "count".to_string()),
                    ("#hour".to_string(), "hour".to_string()),
                ])),
                expression_attribute_values: Some(storage_types::AttributeMap::from_iter([
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
        .get_item_decode::<_, storage_types::AttributeMap>(
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

#[cfg(feature = "sqlite")]
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

#[cfg(feature = "sqlite")]
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
        .get_item_decode::<_, storage_types::AttributeMap>(
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

#[cfg(feature = "sqlite")]
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

#[cfg(feature = "sqlite")]
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
        .get_item_decode::<_, storage_types::AttributeMap>(
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

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn given_prepared_capped_create_when_leading_guard_and_capacity_reject_then_original_guard_error_and_state_are_preserved()
 {
    let db = create_single_table_mode_db().await;
    let table = TableName::new("prepared_capped_guard");
    create_pk_sk_table(&db, &table).await;
    let first = TestCappedEntity {
        entity_id: "first".into(),
        payload: "first".into(),
    };
    let second = TestCappedEntity {
        entity_id: "second".into(),
        payload: "second".into(),
    };
    let counted_type = "PLATFORM_BILLING_CATALOG_PRODUCT";
    db.create_capped_entity(
        CreateCappedEntityInput::builder()
            .table_name(table.clone())
            .item(&first)
            .counted_entity_type(counted_type)
            .max_value(1_u64)
            .build(),
    )
    .await
    .unwrap();
    for candidate in [&first, &second] {
        let leading = storage_types::TransactEncodeItem {
            condition_check: Some(storage_types::TransactConditionCheckRequest {
                table_name: table.clone(),
                key: storage_types::KeyOwned::pk_sk("required", "charge").into(),
                condition_expression: "attribute_exists(pk)".into(),
                expression_attribute_names: None,
                expression_attribute_values: None,
                return_values_on_condition_check_failure: None,
            }),
            ..Default::default()
        };
        let (request, outcome) = crate::PreparedCappedEntityCreate::try_new(
            table.clone(),
            candidate,
            counted_type.into(),
            1,
        )
        .unwrap()
        .into_transaction(vec![leading]);
        let error = db.transact_write_items_encode(request).await.unwrap_err();
        assert!(error.is_canceled_condition_check_failed(0));
        assert!(error.is_canceled_condition_check_failed(2));
        assert!(outcome.classify_error(&db, &error).await.unwrap().is_none());
        assert!(error.is_canceled_condition_check_failed(0));
    }
    assert!(
        db.get_item_decode::<_, storage_types::AttributeMap>(table.clone(), second.table_key_map())
            .await
            .unwrap()
            .is_none()
    );
    let capacity = db
        .create_capped_entity(
            CreateCappedEntityInput::builder()
                .table_name(table.clone())
                .item(&second)
                .counted_entity_type(counted_type)
                .max_value(1_u64)
                .build(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        capacity,
        CappedStorageError::CapacityExceededError
    ));
    let first_saved = db
        .get_item_decode::<_, storage_types::AttributeMap>(table, first.table_key_map())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        first_saved.get("payload"),
        Some(&AttributeValue::S("first".into()))
    );
}
