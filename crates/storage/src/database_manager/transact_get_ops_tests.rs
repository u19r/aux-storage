use std::collections::HashMap;

use storage_provider::{SqliteSettings, StorageBackend, StorageConfig};
use storage_types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableRequest, KeyAttributeType,
    KeyAttributes, KeySchemaElement, KeyType, TableName, TransactGetItem, TransactGetItemsRequest,
    TransactGetRequest,
};

use crate::{AdmissionConfig, DatabaseManager, DatabaseManagerRuntimeOptions, PutItemInput};

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn transact_get_same_name_replacement_uses_cold_native_metadata() {
    let database_path = crate::storage_test_support_tests::unique_path("transact-get-replacement");
    let config = StorageConfig {
        backend_type: StorageBackend::SQLite,
        connection_string: Some(database_path.to_string_lossy().into_owned()),
        file_path: None,
        sqlite: Some(SqliteSettings {
            force_file_backed_database: true,
            ..SqliteSettings::default()
        }),
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    };
    let table = TableName::new("transact_get_same_name_replacement");
    let create_request = CreateTableRequest::new(
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
    );
    let first = DatabaseManager::new_for_test_with_config(config.clone())
        .await
        .expect("create first manager");
    first
        .create_table(&create_request)
        .await
        .expect("create first table");
    first
        .put_item(
            PutItemInput::builder()
                .table_name(table.clone())
                .item(HashMap::from([
                    ("pk".to_string(), AttributeValue::S("row".to_string())),
                    ("value".to_string(), AttributeValue::S("old".to_string())),
                ]))
                .build(),
        )
        .await
        .expect("write first table");
    first
        .delete_table(&table)
        .await
        .expect("delete first table");
    first
        .clear_all_tables()
        .await
        .expect("finish deleted table reclamation");
    first
        .create_table(&create_request)
        .await
        .expect("create replacement table");
    first
        .put_item(
            PutItemInput::builder()
                .table_name(table.clone())
                .item(HashMap::from([
                    ("pk".to_string(), AttributeValue::S("row".to_string())),
                    ("value".to_string(), AttributeValue::S("new".to_string())),
                ]))
                .build(),
        )
        .await
        .expect("write replacement table");
    drop(first);

    let cold = DatabaseManager::new_for_test_with_config(config)
        .await
        .expect("create cold replacement manager");
    let response = cold
        .transact_get_items(TransactGetItemsRequest {
            transact_items: vec![TransactGetItem {
                get: TransactGetRequest {
                    table_name: table,
                    key: KeyAttributes::from([(
                        "pk".to_string(),
                        AttributeValue::S("row".to_string()),
                    )]),
                    projection_expression: None,
                    expression_attribute_names: None,
                },
            }],
            return_consumed_capacity: None,
        })
        .await
        .expect("cold native TransactGet must bind the replacement metadata");
    let item = response.responses[0]
        .item
        .as_ref()
        .expect("replacement row");
    assert_eq!(
        item.to_attribute_map()
            .expect("decode replacement row")
            .get("value"),
        Some(&AttributeValue::S("new".to_string()))
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn transact_get_uses_one_admitted_snapshot_for_its_read_wave() {
    let database_path =
        crate::storage_test_support_tests::unique_path("transact-get-admission-wave");
    let database = DatabaseManager::new_with_config_and_runtime_options(
        StorageConfig {
            backend_type: StorageBackend::SQLite,
            connection_string: Some(database_path.to_string_lossy().into_owned()),
            file_path: None,
            sqlite: Some(SqliteSettings {
                force_file_backed_database: true,
                ..SqliteSettings::default()
            }),
            postgres: None,
            turso: None,
            rocksdb: None,
            foundationdb: None,
            remote: None,
        },
        DatabaseManagerRuntimeOptions::builder()
            .enable_database_jobs(false)
            .admission_config(AdmissionConfig {
                enabled: false,
                initial_sustainable_throughput_rps: 1,
                initial_latency_estimate_ms: 1,
                minimum_concurrency: 1,
                maximum_concurrency: 2,
                control_reserve_concurrency: 1,
                queue_capacity: 128,
                max_queue_wait_ms: 1,
            })
            .build(),
    )
    .await
    .expect("create test database manager");
    let table_name = TableName::new("transact_get_admission_wave");
    database
        .create_table(&CreateTableRequest::new(
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
            BillingMode::PayPerRequest,
        ))
        .await
        .expect("create transact-get table");

    let before = database.default_admission_controller().snapshot();
    assert!(!before.enabled);
    assert_eq!(before.desired_limit, before.effective_maximum);
    assert_eq!(before.effective_maximum, 1);
    let request = TransactGetItemsRequest {
        transact_items: (0..100)
            .map(|index| TransactGetItem {
                get: TransactGetRequest {
                    table_name: table_name.clone(),
                    key: KeyAttributes::from([
                        ("pk".to_string(), AttributeValue::S("missing".to_string())),
                        ("sk".to_string(), AttributeValue::S(format!("item-{index}"))),
                    ]),
                    projection_expression: None,
                    expression_attribute_names: None,
                },
            })
            .collect(),
        return_consumed_capacity: None,
    };

    let response = database
        .transact_get_items(request)
        .await
        .expect("100 missing items should stay within the transaction boundary");
    assert_eq!(response.responses.len(), 100);

    let after = database.default_admission_controller().snapshot();
    assert_eq!(after.in_flight, before.in_flight);
    assert_eq!(after.queue_depth, before.queue_depth);
}
