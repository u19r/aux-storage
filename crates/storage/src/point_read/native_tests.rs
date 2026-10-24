use std::{collections::HashMap, sync::Arc};

use super::PointReadPlan;
use crate::{
    DatabaseManager, DatabaseManagerRuntimeOptions, DeleteItemInput, PutItemInput,
    types::{
        AttributeDefinition, AttributeMap, AttributeValue, BillingMode, CreateTableRequest,
        KeyAttributeType, KeyAttributes, KeySchemaElement, KeyType, TableName,
    },
};

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn given_native_sqlite_tables_when_point_source_reads_then_isolated_hits_absence_and_new_writes_are_visible()
 {
    let db = DatabaseManager::new_for_test_with_runtime_options(
        DatabaseManagerRuntimeOptions::builder()
            .enable_database_jobs(false)
            .build(),
    )
    .await
    .expect("SQLite database");
    verify_point_reads(db).await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN for an owned Postgres fixture"]
async fn given_native_postgres_tables_when_point_source_reads_then_isolated_hits_absence_and_new_writes_are_visible()
 {
    let mut config = remote_config(crate::provider::StorageBackend::Postgres);
    config.postgres = Some(crate::provider::PostgresSettings {
        dsn: std::env::var("TEST_POSTGRES_DSN").expect("owned Postgres DSN"),
        tls: false,
        ..Default::default()
    });
    verify_point_reads(open_database(config).await).await;
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore = "requires TEST_FDB_CLUSTER_FILE for an owned FoundationDB fixture"]
async fn given_native_foundationdb_tables_when_point_source_reads_then_isolated_hits_absence_and_new_writes_are_visible()
 {
    let mut config = remote_config(crate::provider::StorageBackend::FoundationDb);
    config.foundationdb = Some(crate::provider::FoundationDbSettings {
        cluster_file: Some(std::env::var("TEST_FDB_CLUSTER_FILE").expect("owned FDB cluster")),
        subspace_prefix: Some(format!("tests/point-source/{}/", unique_suffix())),
        cache_read_version_ms: 0,
        immediate_gsi_consistency: true,
        ..Default::default()
    });
    verify_point_reads(open_database(config).await).await;
}

#[cfg(any(feature = "postgres", feature = "foundationdb"))]
fn remote_config(backend_type: crate::provider::StorageBackend) -> crate::provider::StorageConfig {
    crate::provider::StorageConfig {
        backend_type,
        connection_string: None,
        file_path: None,
        sqlite: None,
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    }
}

#[cfg(any(feature = "postgres", feature = "foundationdb"))]
async fn open_database(config: crate::provider::StorageConfig) -> DatabaseManager {
    DatabaseManager::new_with_config_and_runtime_options(
        config,
        DatabaseManagerRuntimeOptions::builder()
            .enable_database_jobs(false)
            .build(),
    )
    .await
    .expect("native database")
}

fn unique_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock")
        .as_nanos()
}

fn key(value: &str) -> KeyAttributes {
    KeyAttributes::from([("pk".to_owned(), AttributeValue::S(value.to_owned()))])
}

async fn create_table(db: &DatabaseManager, table: &TableName) {
    db.create_table(&CreateTableRequest::new(
        table.clone(),
        vec![AttributeDefinition {
            attribute_name: "pk".into(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".into(),
            key_type: KeyType::Hash,
        }],
        BillingMode::PayPerRequest,
    ))
    .await
    .expect("create owned table");
}

async fn put_value(db: &DatabaseManager, table: &TableName, value: &str) {
    db.put_item(
        PutItemInput::builder()
            .table_name(table.clone())
            .item(HashMap::from([
                ("pk".to_owned(), AttributeValue::S("shared".into())),
                ("value".to_owned(), AttributeValue::S(value.to_owned())),
            ]))
            .build(),
    )
    .await
    .expect("put owned item");
}

async fn verify_point_reads(db: DatabaseManager) {
    let db = Arc::new(db);
    let source = db.point_reads();
    assert!(Arc::ptr_eq(&source, &db.point_reads()));
    let suffix = unique_suffix();
    let tables = [
        TableName::new(&format!("point_source_a_{suffix}")),
        TableName::new(&format!("point_source_b_{suffix}")),
    ];
    for (table, value) in tables.iter().zip(["first", "second"]) {
        create_table(&db, table).await;
        put_value(&db, table, value).await;
    }
    let mut plan = PointReadPlan::default();
    for table in &tables {
        plan.add_item(table.clone(), key("shared")).unwrap();
        plan.add_item(table.clone(), key("shared")).unwrap();
        plan.add_item(table.clone(), key("missing")).unwrap();
    }
    let retained = source.fetch_store(plan).await.expect("native point batch");
    for (table, expected) in tables.iter().zip(["first", "second"]) {
        let item = retained
            .get_decoded::<AttributeMap>(table, &key("shared"))
            .unwrap()
            .expect("isolated hit");
        assert_eq!(item.get("value"), Some(&AttributeValue::S(expected.into())));
        assert!(
            retained
                .get_decoded::<AttributeMap>(table, &key("missing"))
                .unwrap()
                .is_none()
        );
    }
    put_value(&db, &tables[0], "replacement").await;
    let fresh = source
        .fetch_decoded::<AttributeMap>(tables[0].clone(), key("shared"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        fresh.get("value"),
        Some(&AttributeValue::S("replacement".into()))
    );
    assert_eq!(
        retained
            .get_decoded::<AttributeMap>(&tables[0], &key("shared"))
            .unwrap()
            .unwrap()
            .get("value"),
        Some(&AttributeValue::S("first".into()))
    );
    db.delete_item(
        DeleteItemInput::builder()
            .table_name(tables[0].clone())
            .key(HashMap::from([(
                "pk".to_owned(),
                AttributeValue::S("shared".to_owned()),
            )]))
            .build(),
    )
    .await
    .unwrap();
    assert!(
        source
            .fetch_decoded::<AttributeMap>(tables[0].clone(), key("shared"))
            .await
            .unwrap()
            .is_none()
    );
    for table in tables {
        db.delete_table(&table).await.expect("delete owned table");
        source
            .fetch_decoded::<AttributeMap>(table, key("shared"))
            .await
            .expect_err("a deleted table must not become an absent item");
    }
}
