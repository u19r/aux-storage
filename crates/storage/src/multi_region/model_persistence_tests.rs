use std::collections::HashMap;

use storage_backfill::{
    LogicalBackfillChunk, LogicalBackfillChunkId, LogicalBackfillDomain, LogicalBackfillId,
    LogicalBackfillManifest, LogicalBackfillResult, LogicalExportPage, LogicalExportRequest,
    MultiRegionBootstrapPolicy,
};
use storage_provider::{StorageBackend, StorageConfig};
use storage_types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableRequest, KeyAttributeType,
    KeySchemaElement, KeyType, StreamItemId, StreamSpecification, StreamViewType, TableName,
    TimestampMillis,
};

use crate::{DatabaseManager, PutItemInput, TableBootstrapCursorRecord};

#[tokio::test]
async fn imported_logical_snapshot_and_bootstrap_activation_state_survives_reopen() {
    let source = DatabaseManager::new_for_test()
        .await
        .expect("create source database manager");
    let destination_config = file_backed_sqlite_config("multi-region-logical-reopen");
    let destination = DatabaseManager::new_with_config(destination_config.clone())
        .await
        .expect("create destination database manager");
    let table_name = TableName::new("tenant_bootstrap_reopen");
    create_test_table_with_stream(&source, &table_name).await;
    create_test_table(&destination, &table_name).await;
    source
        .put_item(
            PutItemInput::builder()
                .table_name(table_name.clone())
                .item(item("pk-import", "sk-import", "logical-snapshot"))
                .build(),
        )
        .await
        .expect("write source item");

    let manifest_id =
        LogicalBackfillId::new("manifest-bootstrap-reopen").expect("manifest id should be valid");
    let manifest = LogicalBackfillManifest::for_policy(
        manifest_id.clone(),
        &MultiRegionBootstrapPolicy,
        "sqlite",
        "sqlite",
        vec![
            LogicalBackfillDomain::TableMetadata,
            LogicalBackfillDomain::ItemRecords,
        ],
    );
    import_exported_page(
        &source,
        &destination,
        &manifest,
        "table-metadata",
        LogicalBackfillDomain::TableMetadata,
        Some(table_name.as_ref().to_string()),
    )
    .await;
    import_exported_page(
        &source,
        &destination,
        &manifest,
        "item-records",
        LogicalBackfillDomain::ItemRecords,
        Some(table_name.as_ref().to_string()),
    )
    .await;

    let cursor = TableBootstrapCursorRecord {
        table_name: table_name.clone(),
        peer_region: "eu-west-1".to_string(),
        protected_stream_cursor: Some(StreamItemId::from([11; 12])),
        last_system_stream_cursor: Some(StreamItemId::from([12; 12])),
        activation_cursor: Some(StreamItemId::from([13; 12])),
        session_started_at: Some(TimestampMillis::from_timestamp(1_700_010_000_000)),
        logical_backfill_manifest_id: Some(manifest_id.as_str().to_string()),
        logical_backfill_domain: Some("item_records".to_string()),
        logical_backfill_cursor: Some("cursor-after-items".to_string()),
        updated_at: TimestampMillis::from_timestamp(1_700_010_000_100),
    };
    destination
        .put_table_bootstrap_cursor(&cursor)
        .await
        .expect("persist bootstrap activation cursor");

    drop(destination);

    let reopened = DatabaseManager::new_with_config(destination_config)
        .await
        .expect("reopen destination database manager");
    let table_info = reopened
        .get_table_info(&table_name)
        .await
        .expect("imported table metadata should survive reopen");
    assert!(
        table_info
            .stream_specification
            .as_ref()
            .is_some_and(|stream| stream.stream_enabled),
        "imported stream metadata should survive reopen"
    );
    let stored = reopened
        .get_item_map(table_name.clone(), key("pk-import", "sk-import"))
        .await
        .expect("read imported item after reopen")
        .expect("imported item should survive reopen");
    assert_eq!(
        stored.get("data"),
        Some(&AttributeValue::S("logical-snapshot".to_string()))
    );
    let loaded_cursor = reopened
        .get_table_bootstrap_cursor(&table_name, "eu-west-1")
        .await
        .expect("read bootstrap activation cursor after reopen");
    assert_eq!(loaded_cursor, Some(cursor));
}

async fn import_exported_page(
    source: &DatabaseManager,
    destination: &DatabaseManager,
    manifest: &LogicalBackfillManifest,
    chunk_id: &str,
    domain: LogicalBackfillDomain,
    table_name: Option<String>,
) {
    let page = source
        .export_logical_backfill_page(LogicalExportRequest {
            manifest_id: manifest.id.clone(),
            domain,
            table_name,
            cursor: None,
            limit: 100,
        })
        .await
        .expect("export logical page");
    let result = destination
        .import_logical_backfill_chunk(manifest, chunk_from_page(chunk_id, page))
        .await
        .unwrap_or_else(|error| panic!("import logical page for {domain:?}: {error:?}"));
    assert_eq!(result, LogicalBackfillResult::ChunkImported);
}

fn chunk_from_page(chunk_id: &str, page: LogicalExportPage) -> LogicalBackfillChunk {
    LogicalBackfillChunk {
        summary: storage_backfill::LogicalBackfillChunkSummary {
            id: LogicalBackfillChunkId::new(chunk_id).expect("chunk id should be valid"),
            domain: page.domain,
            record_count: page.records.len() as u64,
            checksum: page.checksum,
        },
        records: page.records,
    }
}

async fn create_test_table(db: &DatabaseManager, table_name: &TableName) {
    create_test_table_with_request(
        db,
        CreateTableRequest::new(
            table_name.clone(),
            table_attributes(),
            table_key_schema(),
            BillingMode::PayPerRequest,
        ),
    )
    .await;
}

async fn create_test_table_with_stream(db: &DatabaseManager, table_name: &TableName) {
    create_test_table_with_request(
        db,
        CreateTableRequest::new(
            table_name.clone(),
            table_attributes(),
            table_key_schema(),
            BillingMode::PayPerRequest,
        )
        .with_stream_specification(Some(StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewAndOldImages),
        })),
    )
    .await;
}

async fn create_test_table_with_request(db: &DatabaseManager, request: CreateTableRequest) {
    db.create_table(&request).await.expect("create table");
}

fn table_attributes() -> Vec<AttributeDefinition> {
    vec![
        AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        },
        AttributeDefinition {
            attribute_name: "sk".to_string(),
            attribute_type: KeyAttributeType::S,
        },
    ]
}

fn table_key_schema() -> Vec<KeySchemaElement> {
    vec![
        KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        },
        KeySchemaElement {
            attribute_name: "sk".to_string(),
            key_type: KeyType::Range,
        },
    ]
}

fn file_backed_sqlite_config(label: &str) -> StorageConfig {
    let path = std::env::temp_dir().join(format!(
        "aux-storage-{label}-{}.db",
        TimestampMillis::now().timestamp_millis()
    ));
    StorageConfig {
        backend_type: StorageBackend::SQLite,
        connection_string: Some(path.to_string_lossy().to_string()),
        file_path: None,
        sqlite: None,
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    }
}

fn key(pk: &str, sk: &str) -> storage_types::KeyAttributes {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S(pk.to_string())),
        ("sk".to_string(), AttributeValue::S(sk.to_string())),
    ])
    .into()
}

fn item(pk: &str, sk: &str, data: &str) -> HashMap<String, AttributeValue> {
    let mut item = key(pk, sk).to_attribute_map();
    item.insert("data".to_string(), AttributeValue::S(data.to_string()));
    item
}
