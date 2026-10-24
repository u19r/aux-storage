#[cfg(any(feature = "postgres", feature = "turso"))]
use std::{
    collections::HashMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(any(feature = "postgres", feature = "turso"))]
use storage_common::JobIntervalMillis;
#[cfg(any(feature = "postgres", feature = "turso"))]
use storage_types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateGlobalSecondaryIndex,
    CreateTableRequest, IndexName, KeyAttributeType, KeySchemaElement, KeyType, MaxIndexers,
    Projection, ProjectionType, TableName, UpdateTableRequest,
};

#[cfg(feature = "postgres")]
use crate::provider::PostgresSettings;
#[cfg(feature = "turso")]
use crate::provider::TursoSettings;
#[cfg(any(feature = "postgres", feature = "turso"))]
use crate::{
    DatabaseManager, DatabaseManagerRuntimeOptions, PutItemInput, QueryIndexInput, QueryTableInput,
    provider::{StorageBackend, StorageConfig},
};

#[cfg(feature = "postgres")]
async fn isolated_postgres_dsn() -> String {
    let base_dsn = std::env::var("TEST_POSTGRES_DSN").expect("TEST_POSTGRES_DSN required");
    let schema = format!("aux_gsi_{}_{}", std::process::id(), unique_suffix());
    let (client, connection) = tokio_postgres::connect(&base_dsn, tokio_postgres::NoTls)
        .await
        .expect("connect to PostgreSQL to create an isolated deferred-GSI schema");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
        .await
        .expect("create isolated deferred-GSI PostgreSQL schema");

    let separator = if base_dsn.contains('?') { '&' } else { '?' };
    format!("{base_dsn}{separator}options=-csearch_path%3D{schema}")
}

#[cfg(any(feature = "postgres", feature = "turso"))]
fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_nanos()
}

#[cfg(feature = "postgres")]
async fn postgres_config() -> StorageConfig {
    let dsn = isolated_postgres_dsn().await;
    StorageConfig {
        backend_type: StorageBackend::Postgres,
        connection_string: Some(dsn.clone()),
        file_path: None,
        sqlite: None,
        postgres: Some(PostgresSettings {
            dsn,
            tls: false,
            ..PostgresSettings::default()
        }),
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    }
}

#[cfg(feature = "turso")]
fn turso_config() -> (StorageConfig, String) {
    let path = std::env::temp_dir()
        .join(format!("aux-storage-deferred-gsi-{}.db", unique_suffix()))
        .to_string_lossy()
        .into_owned();
    let config = StorageConfig {
        backend_type: StorageBackend::Turso,
        connection_string: Some(path.clone()),
        file_path: None,
        sqlite: None,
        postgres: None,
        turso: Some(TursoSettings::default()),
        rocksdb: None,
        foundationdb: None,
        remote: None,
    };
    (config, path)
}

#[cfg(any(feature = "postgres", feature = "turso"))]
fn table_request(table_name: &TableName, index_name: &IndexName) -> CreateTableRequest {
    let mut request = CreateTableRequest::new(
        table_name.clone(),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_owned(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_owned(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_pk".to_owned(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_sk".to_owned(),
                attribute_type: KeyAttributeType::S,
            },
        ],
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
    );
    request.global_secondary_indexes = Some(vec![CreateGlobalSecondaryIndex {
        index_name: index_name.clone(),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "gsi_pk".to_owned(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "gsi_sk".to_owned(),
                key_type: KeyType::Range,
            },
        ],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }]);
    request
}

#[cfg(any(feature = "postgres", feature = "turso"))]
async fn assert_automatic_deferred_gsi_maintenance(config: StorageConfig, backend: &'static str) {
    let mut runtime_options = DatabaseManagerRuntimeOptions::default();
    runtime_options
        .database_job_intervals
        .gsi_backfill_interval_ms = JobIntervalMillis(1);
    let manager = DatabaseManager::new_with_config_and_runtime_options(config, runtime_options)
        .await
        .unwrap_or_else(|error| panic!("construct default {backend} manager: {error}"));
    let table_name = TableName::new(&format!("deferred_gsi_{}_{}", backend, unique_suffix()));
    let index_name = IndexName::new("by_gsi_key");

    manager
        .create_table(&table_request(&table_name, &index_name))
        .await
        .unwrap_or_else(|error| panic!("create {backend} deferred-GSI table: {error}"));
    manager
        .update_table(UpdateTableRequest {
            max_indexers: Some(MaxIndexers::try_new(6).expect("six indexers are supported")),
            ..UpdateTableRequest::new(table_name.clone())
        })
        .await
        .unwrap_or_else(|error| panic!("increase {backend} deferred-GSI capacity: {error}"));

    let mut item = HashMap::from([
        ("pk".to_owned(), AttributeValue::S("partition".to_owned())),
        ("sk".to_owned(), AttributeValue::S("item".to_owned())),
        (
            "gsi_pk".to_owned(),
            AttributeValue::S("deferred".to_owned()),
        ),
        ("gsi_sk".to_owned(), AttributeValue::S("item".to_owned())),
    ]);
    let indexers: Vec<String> = (0..6)
        .map(|ordinal| {
            let name = format!("indexer_{ordinal}");
            item.insert(name.clone(), AttributeValue::S(format!("value_{ordinal}")));
            name
        })
        .collect();

    manager
        .put_item(
            PutItemInput::builder()
                .table_name(table_name.clone())
                .item(item)
                .indexers(indexers)
                .build(),
        )
        .await
        .unwrap_or_else(|error| panic!("put six-indexer item on {backend}: {error}"));

    let query = || {
        manager.query_index_map(
            QueryIndexInput::builder()
                .table_name(table_name.clone())
                .index_name(index_name.clone())
                .key_condition_expression("gsi_pk = :gsi_pk AND gsi_sk = :gsi_sk".to_owned())
                .expression_attribute_values(HashMap::from([
                    (
                        ":gsi_pk".to_owned(),
                        AttributeValue::S("deferred".to_owned()),
                    ),
                    (":gsi_sk".to_owned(), AttributeValue::S("item".to_owned())),
                ]))
                .build(),
        )
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut last_observation: Option<String>;
    loop {
        match query().await {
            Ok(page) if page.items.len() == 1 => return,
            Ok(page) => {
                last_observation =
                    Some(format!("query returned {} indexed items", page.items.len()));
            }
            Err(error) => {
                last_observation = Some(format!("query failed: {error}"));
            }
        }
        if tokio::time::Instant::now() >= deadline {
            let readiness = manager
                .get_gsi_index_readiness(&table_name, std::slice::from_ref(&index_name))
                .await;
            let table_info = manager.get_table_info(&table_name).await;
            let base_item = manager
                .query_table_map(
                    QueryTableInput::builder()
                        .table_name(table_name.clone())
                        .key_condition_expression("pk = :pk AND sk = :sk".to_owned())
                        .expression_attribute_values(HashMap::from([
                            (":pk".to_owned(), AttributeValue::S("partition".to_owned())),
                            (":sk".to_owned(), AttributeValue::S("item".to_owned())),
                        ]))
                        .build(),
                )
                .await;
            panic!(
                "automatic deferred-GSI maintenance did not publish the item on {backend}; \
                 last_observation={:?}; readiness_count={}; table_info={table_info:?}; \
                 base_item={base_item:?}",
                last_observation.as_deref().unwrap_or("no query completed"),
                readiness.as_ref().map_or(0, Vec::len),
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires a configured PostgreSQL service"]
async fn given_postgres_default_manager_when_six_indexers_are_written_then_gsi_catches_up() {
    assert_automatic_deferred_gsi_maintenance(postgres_config().await, "postgres").await;
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn given_turso_default_manager_when_six_indexers_are_written_then_gsi_catches_up() {
    let (config, path) = turso_config();
    assert_automatic_deferred_gsi_maintenance(config, "turso").await;
    let _ = std::fs::remove_file(path);
}
