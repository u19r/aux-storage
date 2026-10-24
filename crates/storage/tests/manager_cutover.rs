use std::sync::atomic::{AtomicU64, Ordering};

use storage::{
    DatabaseManager, DatabaseManagerRuntimeOptions, DeleteItemInput, PutItemInput, QueryTableInput,
    UpdateItemInput,
};
use storage_provider::{StorageBackend, StorageConfig};
use storage_types::{
    AttributeDefinition, AttributeValue, BatchWriteItemRequest, BillingMode, CreateTableRequest,
    KeyAttributeType, KeySchemaElement, KeyType, PutRequest, TableName, TransactPutRequest,
    TransactWriteItem, TransactWriteItemsRequest, WireItem, WriteRequest,
};

fn item(key: &str, value: &str) -> WireItem {
    WireItem::try_from(
        &[
            ("pk".to_string(), AttributeValue::S(key.to_string())),
            ("value".to_string(), AttributeValue::S(value.to_string())),
        ]
        .into_iter()
        .collect::<storage_types::AttributeMap>(),
    )
    .expect("valid item")
}

#[tokio::test]
async fn manager_operations_share_the_initialized_store_and_rejected_transaction_has_no_effect() {
    for backend in [
        #[cfg(feature = "sqlite")]
        StorageBackend::SQLite,
        #[cfg(feature = "rocksdb")]
        StorageBackend::RocksDB,
    ] {
        static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(1);
        let directory = std::env::temp_dir().join(format!(
            "aux-manager-cutover-{}-{}",
            std::process::id(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir(&directory).expect("isolated fixture");
        let path = directory.join("database");
        let manager = DatabaseManager::new_with_config_and_runtime_options(
            StorageConfig {
                backend_type: backend.clone(),
                connection_string: Some(path.to_string_lossy().into_owned()),
                file_path: None,
                sqlite: None,
                turso: None,
                postgres: None,
                rocksdb: None,
                foundationdb: None,
                remote: None,
            },
            DatabaseManagerRuntimeOptions::builder()
                .enable_database_jobs(false)
                .build(),
        )
        .await
        .expect("initialize manager");
        let namespace = storage_types::TableNamespace::system();
        let namespace_table = storage::Tables::namespace(&namespace);
        manager
            .create_table(&CreateTableRequest::new(
                namespace_table.clone(),
                ["pk", "sk"]
                    .into_iter()
                    .map(|name| AttributeDefinition {
                        attribute_name: name.to_owned(),
                        attribute_type: KeyAttributeType::S,
                    })
                    .collect(),
                vec![
                    KeySchemaElement {
                        attribute_name: "pk".to_owned(),
                        key_type: KeyType::Hash,
                    },
                    KeySchemaElement {
                        attribute_name: "sk".to_owned(),
                        key_type: KeyType::Range,
                    },
                ],
                BillingMode::PayPerRequest,
            ))
            .await
            .expect("existing namespace table without stream or indexer capacity");
        for _ in 0..2 {
            storage::Tables::create_namespace_table(&manager, &namespace)
                .await
                .expect("idempotent namespace bootstrap");
            let info = manager
                .get_table_info(&namespace_table)
                .await
                .expect("namespace metadata");
            assert!(
                info.stream_specification
                    .is_some_and(|stream| stream.stream_enabled
                        && stream.stream_view_type
                            == Some(storage_types::StreamViewType::NewAndOldImages))
            );
            assert_eq!(info.max_indexers.get(), 2);
        }
        let table = TableName::new("manager_cutover");
        manager
            .create_table(&CreateTableRequest::new(
                table.clone(),
                vec![AttributeDefinition {
                    attribute_name: "pk".to_string(),
                    attribute_type: KeyAttributeType::S,
                }],
                vec![KeySchemaElement {
                    attribute_name: "pk".to_string(),
                    key_type: KeyType::Hash,
                }],
                BillingMode::PayPerRequest,
            ))
            .await
            .expect("create table");
        manager
            .put_item(
                PutItemInput::builder()
                    .table_name(table.clone())
                    .item(item("one", "initial"))
                    .build(),
            )
            .await
            .expect("put through shared owner");
        manager
            .update_item(
                UpdateItemInput::builder()
                    .table_name(table.clone())
                    .key(storage_types::KeyAttributes::from([(
                        "pk".to_string(),
                        AttributeValue::S("one".to_string()),
                    )]))
                    .update_expression("SET #value = :value")
                    .expression_attribute_names(
                        [("#value".to_string(), "value".to_string())]
                            .into_iter()
                            .collect::<storage_types::VecMap<_, _>>(),
                    )
                    .expression_attribute_values(
                        [(
                            ":value".to_string(),
                            AttributeValue::S("updated".to_string()),
                        )]
                        .into_iter()
                        .collect::<storage_types::AttributeMap>(),
                    )
                    .build(),
            )
            .await
            .expect("update through shared owner");
        let write = WriteRequest {
            put_request: Some(PutRequest {
                item: item("two", "batch"),
                indexers: None,
                aux_item_stream_ttl_hours: None,
            }),
            delete_request: None,
        };
        manager
            .batch_write_item(BatchWriteItemRequest {
                request_items: [(table.clone(), vec![write])].into_iter().collect(),
                return_consumed_capacity: None,
                return_item_collection_metrics: None,
            })
            .await
            .expect("batch through shared owner");
        let transaction = |key, condition| TransactWriteItemsRequest {
            transact_items: vec![TransactWriteItem {
                put: Some(TransactPutRequest {
                    table_name: table.clone(),
                    item: item(key, "transaction"),
                    indexers: None,
                    condition_expression: condition,
                    expression_attribute_names: None,
                    expression_attribute_values: None,
                    return_values_on_condition_check_failure: None,
                    aux_item_stream_ttl_hours: None,
                }),
                ..TransactWriteItem::default()
            }],
            ..TransactWriteItemsRequest::default()
        };
        manager
            .transact_write_items(transaction("three", None))
            .await
            .expect("transaction through shared owner");
        manager
            .batch_write_item_encode(storage_types::BatchWriteItemEncodeRequest {
                request_items: [(
                    table.clone(),
                    vec![storage_types::EncodeWriteRequest {
                        put_request: Some(storage_types::EncodePutRequest {
                            item: storage_types::WireEntity::unindexed(item(
                                "four",
                                "encoded batch",
                            )),
                            aux_item_stream_ttl_hours: None,
                        }),
                        delete_request: None,
                    }],
                )]
                .into_iter()
                .collect(),
                ..storage_types::BatchWriteItemEncodeRequest::default()
            })
            .await
            .expect("encoded batch through shared owner");
        manager
            .transact_write_items_encode(storage_types::TransactWriteItemsEncodeRequest {
                transact_items: vec![storage_types::TransactEncodeItem {
                    put: Some(storage_types::TransactEncodePutRequest {
                        table_name: table.clone(),
                        item: storage_types::WireEntity::unindexed(item(
                            "five",
                            "encoded transaction",
                        )),
                        condition_expression: None,
                        expression_attribute_names: None,
                        expression_attribute_values: None,
                        return_values_on_condition_check_failure: None,
                        aux_item_stream_ttl_hours: None,
                    }),
                    ..storage_types::TransactEncodeItem::default()
                }],
                ..storage_types::TransactWriteItemsEncodeRequest::default()
            })
            .await
            .expect("encoded transaction through shared owner");
        for (pk, expected) in [("four", "encoded batch"), ("five", "encoded transaction")] {
            let key = storage_types::KeyAttributes::from([(
                "pk".to_string(),
                AttributeValue::S(pk.to_string()),
            )]);
            let found = manager
                .get_item(table.clone(), key)
                .await
                .expect("read encoded item")
                .expect("encoded item exists");
            assert_eq!(
                found.required_string_attribute("value").expect("value"),
                expected
            );
        }

        manager
            .transact_write_items(transaction(
                "one",
                Some("attribute_not_exists(pk)".to_string()),
            ))
            .await
            .expect_err("rejected transaction must not replace the item");
        let key = storage_types::KeyAttributes::from([(
            "pk".to_string(),
            AttributeValue::S("one".to_string()),
        )]);
        let found = manager
            .get_item(table.clone(), key.clone())
            .await
            .expect("get")
            .expect("item exists");
        assert_eq!(
            found.required_string_attribute("value").expect("value"),
            "updated"
        );
        let page = manager
            .query_table(
                QueryTableInput::builder()
                    .table_name(table.clone())
                    .key_condition_expression("pk = :pk")
                    .expression_attribute_values(
                        [(":pk".to_string(), AttributeValue::S("one".to_string()))]
                            .into_iter()
                            .collect::<storage_types::AttributeMap>(),
                    )
                    .limit(1_u32)
                    .consistent_read(true)
                    .build(),
            )
            .await
            .expect("internal query");
        assert_eq!(page.progress.evaluated_count, 1);
        assert_eq!(page.items.len(), 1);
        manager
            .delete_item(
                DeleteItemInput::builder()
                    .table_name(table.clone())
                    .key(key.clone())
                    .build(),
            )
            .await
            .expect("delete through shared owner");
        assert!(
            manager
                .get_item(table.clone(), key)
                .await
                .expect("get deleted item")
                .is_none()
        );
        drop(manager);
        let bundle = match backend {
            #[cfg(feature = "sqlite")]
            StorageBackend::SQLite => {
                let driver = sql_backend::sqlite::SqliteBackend::initialize(&path)
                    .await
                    .expect("open existing SQLite");
                let store = std::sync::Arc::new(
                    sql_backend::SqlStore::initialize_schema(driver)
                        .await
                        .expect("initialized SQL owner"),
                );
                storage::StorageProviderBundle::try_from_initialized_store(
                    store,
                    Default::default(),
                )
                .expect("same-owner bundle")
            }
            #[cfg(feature = "rocksdb")]
            StorageBackend::RocksDB => {
                let driver = kv_backend::rocksdb::RocksDbBackend::initialize(path.clone())
                    .expect("open existing RocksDB");
                let store = std::sync::Arc::new(
                    kv_backend::KvStore::initialize_format(driver)
                        .await
                        .expect("initialized KV owner"),
                );
                storage::StorageProviderBundle::try_from_initialized_store(
                    store,
                    Default::default(),
                )
                .expect("same-owner bundle")
            }
            _ => unreachable!("embedded fixture backends"),
        };
        let manager = DatabaseManager::from_initialized_bundle(
            StorageConfig {
                backend_type: backend,
                connection_string: Some(path.to_string_lossy().into_owned()),
                file_path: None,
                sqlite: None,
                turso: None,
                postgres: None,
                rocksdb: None,
                foundationdb: None,
                remote: None,
            },
            bundle,
            DatabaseManagerRuntimeOptions::builder()
                .enable_database_jobs(false)
                .build(),
        )
        .await
        .expect("manager over caller-initialized owner");
        let retained = manager
            .get_item(
                table.clone(),
                storage_types::KeyAttributes::from([(
                    "pk".to_owned(),
                    AttributeValue::S("two".to_owned()),
                )]),
            )
            .await
            .expect("restart read")
            .expect("batch row survives restart");
        assert_eq!(
            retained
                .to_attribute_map()
                .expect("decode row")
                .get("value"),
            Some(&AttributeValue::S("batch".to_owned()))
        );
        let info = manager
            .get_table_info(&namespace_table)
            .await
            .expect("restarted namespace");
        assert_eq!(info.max_indexers.get(), 2);
        assert!(
            info.stream_specification
                .is_some_and(|stream| stream.stream_enabled)
        );
        drop(manager);
        std::fs::remove_dir_all(directory).expect("remove owned fixture");
    }
}
