use std::collections::HashMap;

use storage_backfill::{
    LogicalBackfillChecksum, LogicalBackfillChunk, LogicalBackfillChunkId,
    LogicalBackfillChunkSummary, LogicalBackfillDomain, LogicalBackfillId, LogicalBackfillManifest,
    LogicalBackfillRecord, LogicalExportRequest, SyncLearnerCatchupPolicy,
};
use storage_provider::StorageProvider;
use storage_sync::{
    ResolvedSyncMutation, ResolvedSyncMutationBatch, SyncCommitMetadata, SyncLogId, SyncMutationId,
    SyncMutationResponse, SyncPutMutation,
};
use storage_types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateGlobalSecondaryIndex,
    CreateTableRequest, DurablePointReadProof, DurablePointReadRequest, IndexName,
    ItemStreamVersion, KeyAttributeType, KeyAttributes, KeySchemaElement, KeyType, Projection,
    ProjectionType, QueryTableRequest, StreamName, StreamSpecification, StreamViewType, TableName,
    TimeToLiveSpecification, TimeToLiveStatus, TimestampMillis, UpdateTimeToLiveRequest,
};
use stream_provider::StreamProvider;

use crate::{SortedKvDbStorageProvider, kv_support_tests::create_test_store};

#[tokio::test]
async fn rocksdb_logical_item_export_import_preserves_target_revision() {
    let source = initialized_provider().await;
    let destination = initialized_provider().await;
    let table_name = TableName::new("SyncKvLogicalItems");
    source
        .create_table(&basic_create_table_request(&table_name))
        .await
        .expect("create source table");
    destination
        .create_table(&basic_create_table_request(&table_name))
        .await
        .expect("create destination table");
    apply_sync_put(&source, &table_name, "item#1", "open", 7).await;

    let page = source
        .export_logical_backfill_page(LogicalExportRequest {
            manifest_id: LogicalBackfillId::new("manifest").expect("manifest id"),
            domain: LogicalBackfillDomain::ItemRecords,
            table_name: Some(table_name.as_ref().to_string()),
            cursor: None,
            limit: 10,
        })
        .await
        .expect("export item records");
    assert_eq!(page.records.len(), 1);

    let manifest = logical_manifest(vec![LogicalBackfillDomain::ItemRecords]);
    import_records(
        &destination,
        &manifest,
        LogicalBackfillDomain::ItemRecords,
        page.records,
    )
    .await;

    assert_present_revision(&destination, &table_name, "item#1", 7).await;

    let stale = LogicalBackfillRecord::PresentItem {
        table_name: table_name.as_ref().to_string(),
        key_json: r#"{"pk":{"S":"item#1"}}"#.to_string(),
        item_json: r#"{"pk":{"S":"item#1"},"status":{"S":"stale"}}"#.to_string(),
        item_stream_version: ItemStreamVersion::new(6),
    };
    import_records(
        &destination,
        &manifest,
        LogicalBackfillDomain::ItemRecords,
        vec![stale],
    )
    .await;

    assert_present_revision(&destination, &table_name, "item#1", 7).await;
    let stored = destination
        .get_item(
            table_name.clone(),
            KeyAttributes::from(key_map("item#1")),
            true,
        )
        .await
        .expect("read destination item")
        .expect("destination item should exist")
        .to_attribute_map()
        .expect("wire item to map");
    assert_eq!(
        stored.get("status"),
        Some(&AttributeValue::S("open".to_string()))
    );
}

#[tokio::test]
async fn rocksdb_logical_table_metadata_import_creates_usable_table() {
    let source = initialized_provider().await;
    let destination = initialized_provider().await;
    let table_name = TableName::new("SyncKvLogicalMetadata");
    source
        .create_table(&basic_create_table_request(&table_name))
        .await
        .expect("create source table");

    let page = source
        .export_logical_backfill_page(LogicalExportRequest {
            manifest_id: LogicalBackfillId::new("manifest").expect("manifest id"),
            domain: LogicalBackfillDomain::TableMetadata,
            table_name: Some(table_name.as_ref().to_string()),
            cursor: None,
            limit: 10,
        })
        .await
        .expect("export table metadata");
    assert_eq!(page.records.len(), 1);

    import_records(
        &destination,
        &logical_manifest(vec![LogicalBackfillDomain::TableMetadata]),
        LogicalBackfillDomain::TableMetadata,
        page.records,
    )
    .await;

    destination
        .put_item(
            table_name,
            item_map("item#1", "created"),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("imported metadata should create usable table");
}

#[tokio::test]
async fn rocksdb_logical_ttl_export_import_preserves_config() {
    let source = initialized_provider().await;
    let destination = initialized_provider().await;
    let table_name = TableName::new("SyncKvLogicalTtl");
    source
        .create_table(&basic_create_table_request(&table_name))
        .await
        .expect("create source table");
    destination
        .create_table(&basic_create_table_request(&table_name))
        .await
        .expect("create destination table");
    source
        .update_time_to_live(UpdateTimeToLiveRequest {
            table_name: table_name.clone(),
            time_to_live_specification: TimeToLiveSpecification {
                attribute_name: "ttl".to_string(),
                enabled: true,
            },
        })
        .await
        .expect("enable ttl");

    let page = source
        .export_logical_backfill_page(export_request(
            LogicalBackfillDomain::TtlRecords,
            Some(&table_name),
        ))
        .await
        .expect("export ttl records");
    assert_eq!(page.records.len(), 1);

    import_records(
        &destination,
        &logical_manifest(vec![LogicalBackfillDomain::TtlRecords]),
        LogicalBackfillDomain::TtlRecords,
        page.records,
    )
    .await;

    let config = destination
        .load_ttl_config(&table_name)
        .await
        .expect("load imported ttl config")
        .expect("ttl config should exist");
    assert_eq!(config.attribute_name, "ttl");
    assert_eq!(config.status, TimeToLiveStatus::Enabling);
}

#[tokio::test]
async fn rocksdb_logical_gsi_export_import_preserves_query_rows() {
    let source = initialized_provider_with_immediate_gsi().await;
    let destination = initialized_provider_with_immediate_gsi().await;
    let table_name = TableName::new("SyncKvLogicalGsi");
    source
        .create_table(&gsi_create_table_request(&table_name))
        .await
        .expect("create source table");
    destination
        .create_table(&gsi_create_table_request(&table_name))
        .await
        .expect("create destination table");
    source
        .put_item(
            table_name.clone(),
            gsi_item("item#1", "open"),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("put source item");

    let page = source
        .export_logical_backfill_page(export_request(
            LogicalBackfillDomain::GsiRecords,
            Some(&table_name),
        ))
        .await
        .expect("export gsi records");
    assert!(
        !page.records.is_empty(),
        "gsi export should include physical index rows"
    );

    import_records(
        &destination,
        &logical_manifest(vec![LogicalBackfillDomain::GsiRecords]),
        LogicalBackfillDomain::GsiRecords,
        page.records,
    )
    .await;

    let (items, _) = destination
        .query_table(&gsi_query(table_name))
        .await
        .expect("query imported gsi rows");
    assert_eq!(items.len(), 1);
}

#[tokio::test]
async fn rocksdb_logical_stream_export_import_preserves_system_stream_rows() {
    let source = initialized_provider().await;
    let destination = initialized_provider().await;
    source
        .initialize_stream()
        .await
        .expect("initialize source stream");
    destination
        .initialize_stream()
        .await
        .expect("initialize destination stream");
    let table_name = TableName::new("SyncKvLogicalStream");
    source
        .create_table(&stream_create_table_request(&table_name))
        .await
        .expect("create source table");
    apply_sync_put(&source, &table_name, "item#1", "open", 7).await;

    let page = source
        .export_logical_backfill_page(export_request(
            LogicalBackfillDomain::StreamRecords,
            Some(&table_name),
        ))
        .await
        .expect("export stream records");
    assert!(
        !page.records.is_empty(),
        "stream export should include table/system stream rows"
    );

    import_records(
        &destination,
        &logical_manifest(vec![LogicalBackfillDomain::StreamRecords]),
        LogicalBackfillDomain::StreamRecords,
        page.records,
    )
    .await;

    let page = destination
        .read_forward(StreamName::system_table_stream(), None, 10)
        .await
        .expect("read imported system stream");
    assert!(
        !page.items.is_empty(),
        "system stream should contain imported stream rows"
    );
}

async fn initialized_provider() -> SortedKvDbStorageProvider<crate::kv_support_tests::TestStore> {
    let provider = SortedKvDbStorageProvider::new(create_test_store());
    provider
        .initialize_storage()
        .await
        .expect("initialize storage");
    provider
}

async fn initialized_provider_with_immediate_gsi()
-> SortedKvDbStorageProvider<crate::kv_support_tests::TestStore> {
    let provider =
        SortedKvDbStorageProvider::new(create_test_store()).with_immediate_gsi_consistency(true);
    provider
        .initialize_storage()
        .await
        .expect("initialize storage");
    provider
}

async fn apply_sync_put(
    provider: &SortedKvDbStorageProvider<crate::kv_support_tests::TestStore>,
    table_name: &TableName,
    pk: &str,
    status: &str,
    version: u64,
) {
    let mutation = ResolvedSyncMutation::Put(SyncPutMutation {
        mutation_id: SyncMutationId::new(format!("mutation-{version}")).expect("mutation id"),
        table_name: table_name.clone(),
        key_json: serde_json::to_string(&key_map(pk)).expect("encode key"),
        item_json: serde_json::to_string(&item_map(pk, status)).expect("encode item"),
        old_item_json: None,
        target_item_stream_version: ItemStreamVersion::new(version),
        response: SyncMutationResponse {
            response_json: Some(r#"{"ok":true}"#.to_string()),
        },
    });
    provider
        .apply_resolved_sync_mutations(
            SyncCommitMetadata {
                log_id: SyncLogId::new(3, version),
                committed_at: TimestampMillis::from_timestamp(1_700_000_000_000),
                leader_node_id: "node-1".to_string(),
            },
            ResolvedSyncMutationBatch::new(vec![mutation]),
        )
        .await
        .expect("apply sync put");
}

async fn assert_present_revision(
    provider: &SortedKvDbStorageProvider<crate::kv_support_tests::TestStore>,
    table_name: &TableName,
    pk: &str,
    version: u64,
) {
    let proof = provider
        .get_item_with_durable_proof(DurablePointReadRequest {
            table_name: table_name.clone(),
            key: KeyAttributes::from(key_map(pk)),
            consistent_read: true,
        })
        .await
        .expect("durable proof");
    let DurablePointReadProof::Present { revision, .. } = proof else {
        panic!("item should be present");
    };
    assert_eq!(
        revision.as_bytes(),
        &ItemStreamVersion::new(version).to_be_bytes()
    );
}

async fn import_records(
    provider: &SortedKvDbStorageProvider<crate::kv_support_tests::TestStore>,
    manifest: &LogicalBackfillManifest,
    domain: LogicalBackfillDomain,
    records: Vec<LogicalBackfillRecord>,
) {
    provider
        .import_logical_backfill_chunk(
            manifest,
            LogicalBackfillChunk {
                summary: LogicalBackfillChunkSummary {
                    id: LogicalBackfillChunkId::new("chunk-1").expect("chunk id"),
                    domain,
                    record_count: records.len() as u64,
                    checksum: LogicalBackfillChecksum::new("unchecked").expect("checksum"),
                },
                records,
            },
        )
        .await
        .expect("import logical records");
}

fn logical_manifest(domains: Vec<LogicalBackfillDomain>) -> LogicalBackfillManifest {
    LogicalBackfillManifest::for_policy(
        LogicalBackfillId::new("manifest").expect("manifest id"),
        &SyncLearnerCatchupPolicy,
        "rocksdb",
        "rocksdb",
        domains,
    )
}

fn basic_create_table_request(table_name: &TableName) -> CreateTableRequest {
    CreateTableRequest::new(
        table_name.clone(),
        vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        BillingMode::PayPerRequest,
    )
}

fn stream_create_table_request(table_name: &TableName) -> CreateTableRequest {
    basic_create_table_request(table_name).with_stream_specification(Some(StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(StreamViewType::NewAndOldImages),
    }))
}

fn gsi_create_table_request(table_name: &TableName) -> CreateTableRequest {
    CreateTableRequest::new(
        table_name.clone(),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        BillingMode::PayPerRequest,
    )
    .with_global_secondary_indexes(Some(vec![CreateGlobalSecondaryIndex {
        index_name: IndexName::new("by_status"),
        key_schema: vec![KeySchemaElement {
            attribute_name: "gsi_pk".to_string(),
            key_type: KeyType::Hash,
        }],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }]))
}

fn export_request(
    domain: LogicalBackfillDomain,
    table_name: Option<&TableName>,
) -> LogicalExportRequest {
    LogicalExportRequest {
        manifest_id: LogicalBackfillId::new("manifest").expect("manifest id"),
        domain,
        table_name: table_name.map(|table| table.as_ref().to_string()),
        cursor: None,
        limit: 50,
    }
}

fn key_map(pk: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([("pk".to_string(), AttributeValue::S(pk.to_string()))])
}

fn item_map(pk: &str, status: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S(pk.to_string())),
        ("status".to_string(), AttributeValue::S(status.to_string())),
    ])
}

fn gsi_item(pk: &str, status: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S(pk.to_string())),
        ("gsi_pk".to_string(), AttributeValue::S(status.to_string())),
        ("status".to_string(), AttributeValue::S(status.to_string())),
    ])
}

fn gsi_query(table_name: TableName) -> QueryTableRequest {
    QueryTableRequest {
        table_name,
        index_name: Some(IndexName::new("by_status")),
        key_condition_expression: "gsi_pk = :status".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(HashMap::from([(
            ":status".to_string(),
            AttributeValue::S("open".to_string()),
        )])),
        limit: Some(10),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    }
}
