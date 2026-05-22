#[cfg(test)]
use std::{collections::HashMap, convert::TryFrom};

use chrono::Utc;
use storage_backfill::{
    LogicalBackfillChecksum, LogicalBackfillChunk, LogicalBackfillChunkId,
    LogicalBackfillChunkSummary, LogicalBackfillDomain, LogicalBackfillExport,
    LogicalBackfillImport, LogicalBackfillManifest, LogicalBackfillRecord,
    LogicalBackfillTombstone, LogicalExportRequest, SyncLearnerCatchupPolicy,
};
use storage_common::{GSI_UPDATE_JOB, TTL_SWEEP_JOB};
use storage_provider::StorageProvider;
use storage_types::{
    AllOld, AttributeDefinition, AttributeValue, CreateGlobalSecondaryIndex, CreateTableRequest,
    DurablePointReadGuard, DurablePointReadProof, DurablePointReadRequest,
    GuardedDeleteItemRequest, GuardedPutItemRequest, GuardedUpdateItemRequest, IndexName, ItemKey,
    KeyAttributeType, KeyAttributes, KeySchemaElement, KeyType, Projection, ProjectionType,
    ReplicationEventMetadata, ReplicationHybridLogicalClock, ReplicationMutation,
    ReplicationWriteSource, ReturnValuesOldNewUpdated, ScanTableRequest, StorageEnum, StorageError,
    StorageResult, StreamItemId, StreamName, StreamSpecification, StreamViewType, TableName,
    TimeToLiveSpecification, TimestampMillis, UpdateItemRequest, UpdateTimeToLiveRequest,
    UserStreamName, WireItem,
};
use stream_provider::{
    CursorName, CursorPosition, StoredStreamPointer, StreamDataType, StreamItem, StreamProvider,
};
use tracing_test::traced_test;

use crate::{SQLiteStorageProvider, constants, naming, sql_statements, utils::call_sqlite};

async fn create_test_table() -> SQLiteStorageProvider {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let create_request = CreateTableRequest::new(
        TableName::new("PaginatedTable"),
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

    provider.create_table(&create_request).await.unwrap();

    // Add test items
    let items = vec![
        ("item1", "val1"),
        ("item2", "val2"),
        ("item3", "val3"),
        ("item4", "val4"),
    ];

    for (id, value) in items {
        let mut item = HashMap::new();
        item.insert("id".to_string(), AttributeValue::S(id.to_string()));
        item.insert("value".to_string(), AttributeValue::S(value.to_string()));
        provider
            .put_item(
                TableName::new("PaginatedTable"),
                item,
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
    }

    provider
}

async fn create_revision_test_table(table_name: &str) -> SQLiteStorageProvider {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    provider
        .create_table(&CreateTableRequest::new(
            TableName::new(table_name),
            vec![AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            }],
            vec![KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            }],
            storage_types::BillingMode::PayPerRequest,
        ))
        .await
        .unwrap();

    provider
}

async fn create_limit_boundary_table(table_name: &str) -> SQLiteStorageProvider {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let table_name = TableName::new(table_name);
    provider
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
            storage_types::BillingMode::PayPerRequest,
        ))
        .await
        .unwrap();

    for index in 1..=15 {
        provider
            .put_item(
                table_name.clone(),
                HashMap::from([
                    ("pk".to_string(), AttributeValue::S("user#1".to_string())),
                    (
                        "sk".to_string(),
                        AttributeValue::S(format!("item#{index:03}")),
                    ),
                    ("value".to_string(), AttributeValue::N(index.to_string())),
                ]),
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
    }

    provider
}

#[tokio::test]
async fn sqlite_capabilities_advertise_durable_guards_and_transactions() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    assert!(provider.supports_guarded_writes());
    assert!(provider.supports_guarded_transaction_writes());
}

fn revision_test_key(pk: &str) -> storage_types::KeyAttributes {
    storage_types::KeyAttributes::from([("pk".to_string(), AttributeValue::S(pk.to_string()))])
}

fn revision_test_item(pk: &str, value: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S(pk.to_string())),
        ("value".to_string(), AttributeValue::S(value.to_string())),
    ])
}

fn proof_revision(proof: &DurablePointReadProof) -> Vec<u8> {
    match proof {
        DurablePointReadProof::Present { revision, .. } => revision.as_bytes().to_vec(),
        DurablePointReadProof::Absent { proof } => proof.as_bytes().to_vec(),
    }
}

async fn durable_proof(
    provider: &SQLiteStorageProvider,
    table_name: &str,
    pk: &str,
) -> DurablePointReadProof {
    provider
        .get_item_with_durable_proof(DurablePointReadRequest {
            table_name: TableName::new(table_name),
            key: revision_test_key(pk),
            consistent_read: true,
        })
        .await
        .unwrap()
}

fn proof_guard(proof: &DurablePointReadProof) -> DurablePointReadGuard {
    match proof {
        DurablePointReadProof::Present { revision, .. } => {
            DurablePointReadGuard::Present(revision.clone())
        }
        DurablePointReadProof::Absent { proof } => DurablePointReadGuard::Absent(proof.clone()),
    }
}

#[tokio::test]
async fn sqlite_durable_proof_tracks_put_update_and_delete_revisions() {
    let table_name = "revision_proofs";
    let provider = create_revision_test_table(table_name).await;

    let initial = durable_proof(&provider, table_name, "item#1").await;
    assert!(matches!(initial, DurablePointReadProof::Absent { .. }));

    provider
        .put_item(
            TableName::new(table_name),
            revision_test_item("item#1", "alpha"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let after_put = durable_proof(&provider, table_name, "item#1").await;
    assert!(matches!(after_put, DurablePointReadProof::Present { .. }));
    assert_ne!(proof_revision(&initial), proof_revision(&after_put));

    provider
        .update_item(UpdateItemRequest {
            table_name: TableName::new(table_name),
            key: revision_test_key("item#1"),
            update_expression: "SET #value = :value".to_string(),
            attribute_updates: None,
            condition_expression: None,
            expression_attribute_names: Some(HashMap::from([(
                "#value".to_string(),
                "value".to_string(),
            )])),
            expression_attribute_values: Some(HashMap::from([(
                ":value".to_string(),
                AttributeValue::S("bravo".to_string()),
            )])),
            expected: None,
            conditional_operator: None,
            return_values: None,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
            return_values_on_condition_check_failure: None,
        })
        .await
        .unwrap_or_else(|err| panic!("export gsi records: {err:?}"));
    let after_update = durable_proof(&provider, table_name, "item#1").await;
    assert!(matches!(
        after_update,
        DurablePointReadProof::Present { .. }
    ));
    assert_ne!(proof_revision(&after_put), proof_revision(&after_update));

    provider
        .delete_item(
            TableName::new(table_name),
            revision_test_key("item#1"),
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let after_delete = durable_proof(&provider, table_name, "item#1").await;
    assert!(matches!(after_delete, DurablePointReadProof::Absent { .. }));
    assert_ne!(proof_revision(&after_update), proof_revision(&after_delete));
}

#[tokio::test]
async fn sqlite_versioned_internal_scan_returns_item_stream_versions() {
    let table_name = TableName::new("versioned_scan");
    let provider = create_revision_test_table(table_name.as_ref()).await;

    provider
        .put_item(
            table_name.clone(),
            revision_test_item("item#1", "alpha"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    provider
        .put_item(
            table_name.clone(),
            revision_test_item("item#1", "bravo"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let (items, last_evaluated_key) =
        <SQLiteStorageProvider as StorageProvider>::scan_table_with_item_stream_versions(
            &provider,
            &ScanTableRequest {
                table_name,
                index_name: None,
                limit: Some(10),
                exclusive_start_key: None,
                consistent_read: true,
            },
        )
        .await
        .unwrap();

    assert!(last_evaluated_key.is_none());
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].item_stream_version,
        storage_types::ItemStreamVersion::new(2)
    );
    assert_eq!(
        items[0].item.to_attribute_map().unwrap().get("value"),
        Some(&AttributeValue::S("bravo".to_string()))
    );
}

#[tokio::test]
async fn sqlite_versioned_internal_scan_rejects_gsi_reads() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();
    let table_name = TableName::new("versioned_scan_gsi");
    provider
        .create_table(
            &CreateTableRequest::new(
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
                storage_types::BillingMode::PayPerRequest,
            )
            .with_global_secondary_indexes(Some(vec![CreateGlobalSecondaryIndex {
                index_name: IndexName::new("gsi"),
                key_schema: vec![KeySchemaElement {
                    attribute_name: "gsi_pk".to_string(),
                    key_type: KeyType::Hash,
                }],
                projection: Projection {
                    projection_type: Some(ProjectionType::All),
                    non_key_attributes: None,
                },
                provisioned_throughput: None,
            }])),
        )
        .await
        .unwrap();

    let err = <SQLiteStorageProvider as StorageProvider>::scan_table_with_item_stream_versions(
        &provider,
        &ScanTableRequest {
            table_name,
            index_name: Some(IndexName::new("gsi")),
            limit: Some(10),
            exclusive_start_key: None,
            consistent_read: false,
        },
    )
    .await
    .unwrap_err();

    assert!(err.to_string().contains("base tables"));
}

#[tokio::test]
async fn sqlite_logical_item_export_import_preserves_newer_versions() {
    let table_name = TableName::new("logical_item_import");
    let source = create_revision_test_table(table_name.as_ref()).await;
    let destination = create_revision_test_table(table_name.as_ref()).await;

    source
        .put_item(
            table_name.clone(),
            revision_test_item("item#1", "alpha"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    source
        .put_item(
            table_name.clone(),
            revision_test_item("item#1", "bravo"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let page = source
        .export_logical_page(LogicalExportRequest {
            manifest_id: storage_backfill::LogicalBackfillId::new("manifest").unwrap(),
            domain: LogicalBackfillDomain::ItemRecords,
            table_name: Some(table_name.as_ref().to_string()),
            cursor: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(page.records.len(), 1);
    let records = page.records.clone();
    let manifest = LogicalBackfillManifest::for_policy(
        storage_backfill::LogicalBackfillId::new("manifest").unwrap(),
        &SyncLearnerCatchupPolicy,
        "sqlite",
        "sqlite",
        vec![LogicalBackfillDomain::ItemRecords],
    );

    destination
        .import_logical_chunk(
            &manifest,
            LogicalBackfillChunk {
                summary: LogicalBackfillChunkSummary {
                    id: LogicalBackfillChunkId::new("chunk-1").unwrap(),
                    domain: LogicalBackfillDomain::ItemRecords,
                    record_count: records.len() as u64,
                    checksum: LogicalBackfillChecksum::new("unchecked").unwrap(),
                },
                records: records.clone(),
            },
        )
        .await
        .unwrap();
    destination
        .import_logical_chunk(
            &manifest,
            LogicalBackfillChunk {
                summary: LogicalBackfillChunkSummary {
                    id: LogicalBackfillChunkId::new("chunk-1").unwrap(),
                    domain: LogicalBackfillDomain::ItemRecords,
                    record_count: records.len() as u64,
                    checksum: LogicalBackfillChecksum::new("unchecked").unwrap(),
                },
                records,
            },
        )
        .await
        .unwrap();

    let (items, _) =
        <SQLiteStorageProvider as StorageProvider>::scan_table_with_item_stream_versions(
            &destination,
            &ScanTableRequest {
                table_name: table_name.clone(),
                index_name: None,
                limit: Some(10),
                exclusive_start_key: None,
                consistent_read: true,
            },
        )
        .await
        .unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].item_stream_version,
        storage_types::ItemStreamVersion::new(2)
    );
    assert_eq!(
        items[0].item.to_attribute_map().unwrap().get("value"),
        Some(&AttributeValue::S("bravo".to_string()))
    );
}

#[tokio::test]
async fn sqlite_logical_tombstone_import_blocks_older_scan_image() {
    let table_name = TableName::new("logical_tombstone_import");
    let destination = create_revision_test_table(table_name.as_ref()).await;
    let key = revision_test_key("item#1");
    let key_json = key.canonical_dynamo_json().unwrap();

    destination
        .import_logical_chunk(
            &LogicalBackfillManifest::for_policy(
                storage_backfill::LogicalBackfillId::new("manifest").unwrap(),
                &SyncLearnerCatchupPolicy,
                "sqlite",
                "sqlite",
                vec![
                    LogicalBackfillDomain::ItemRecords,
                    LogicalBackfillDomain::Tombstones,
                ],
            ),
            LogicalBackfillChunk {
                summary: LogicalBackfillChunkSummary {
                    id: LogicalBackfillChunkId::new("chunk-1").unwrap(),
                    domain: LogicalBackfillDomain::Tombstones,
                    record_count: 2,
                    checksum: LogicalBackfillChecksum::new("unchecked").unwrap(),
                },
                records: vec![
                    LogicalBackfillRecord::Tombstone(LogicalBackfillTombstone {
                        table_name: table_name.as_ref().to_string(),
                        key_json: key_json.clone(),
                        item_stream_version: storage_types::ItemStreamVersion::new(3),
                    }),
                    LogicalBackfillRecord::PresentItem {
                        table_name: table_name.as_ref().to_string(),
                        key_json,
                        item_json: serde_json::to_string(&revision_test_item("item#1", "stale"))
                            .unwrap(),
                        item_stream_version: storage_types::ItemStreamVersion::new(2),
                    },
                ],
            },
        )
        .await
        .unwrap();

    let item = destination.get_item(table_name, key, true).await.unwrap();
    assert!(item.is_none());
}

#[tokio::test]
async fn sqlite_logical_import_keeps_newer_stream_image_over_older_scan_image() {
    let table_name = TableName::new("logical_concurrent_import");
    let destination = create_revision_test_table(table_name.as_ref()).await;
    let key = revision_test_key("item#1");
    let key_json = key.canonical_dynamo_json().unwrap();

    destination
        .import_logical_chunk(
            &LogicalBackfillManifest::for_policy(
                storage_backfill::LogicalBackfillId::new("manifest").unwrap(),
                &SyncLearnerCatchupPolicy,
                "sqlite",
                "sqlite",
                vec![LogicalBackfillDomain::ItemRecords],
            ),
            LogicalBackfillChunk {
                summary: LogicalBackfillChunkSummary {
                    id: LogicalBackfillChunkId::new("chunk-1").unwrap(),
                    domain: LogicalBackfillDomain::ItemRecords,
                    record_count: 2,
                    checksum: LogicalBackfillChecksum::new("unchecked").unwrap(),
                },
                records: vec![
                    LogicalBackfillRecord::PresentItem {
                        table_name: table_name.as_ref().to_string(),
                        key_json: key_json.clone(),
                        item_json: serde_json::to_string(&revision_test_item("item#1", "stream"))
                            .unwrap(),
                        item_stream_version: storage_types::ItemStreamVersion::new(3),
                    },
                    LogicalBackfillRecord::PresentItem {
                        table_name: table_name.as_ref().to_string(),
                        key_json,
                        item_json: serde_json::to_string(&revision_test_item("item#1", "scan"))
                            .unwrap(),
                        item_stream_version: storage_types::ItemStreamVersion::new(2),
                    },
                ],
            },
        )
        .await
        .unwrap();

    let item = destination
        .get_item(table_name, key, true)
        .await
        .unwrap()
        .expect("newer stream item")
        .to_attribute_map()
        .unwrap();
    assert_eq!(
        item.get("value"),
        Some(&AttributeValue::S("stream".to_string()))
    );
}

#[tokio::test]
async fn sqlite_logical_import_rejects_chunk_domain_missing_from_manifest() {
    let destination = create_revision_test_table("logical_stale_manifest").await;
    let err = destination
        .import_logical_chunk(
            &LogicalBackfillManifest::for_policy(
                storage_backfill::LogicalBackfillId::new("manifest").unwrap(),
                &SyncLearnerCatchupPolicy,
                "sqlite",
                "sqlite",
                vec![LogicalBackfillDomain::TtlRecords],
            ),
            LogicalBackfillChunk {
                summary: LogicalBackfillChunkSummary {
                    id: LogicalBackfillChunkId::new("chunk-1").unwrap(),
                    domain: LogicalBackfillDomain::ItemRecords,
                    record_count: 0,
                    checksum: LogicalBackfillChecksum::new("unchecked").unwrap(),
                },
                records: Vec::new(),
            },
        )
        .await
        .expect_err("manifest without item records domain should be rejected");

    assert!(err.to_string().contains("not in manifest"));
}

#[tokio::test]
async fn sqlite_logical_durable_revision_export_import_preserves_revision_rows() {
    let table_name = TableName::new("logical_revision_import");
    let source = create_revision_test_table(table_name.as_ref()).await;
    let destination = create_revision_test_table(table_name.as_ref()).await;

    source
        .put_item(
            table_name.clone(),
            revision_test_item("item#1", "alpha"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    source
        .put_item(
            table_name.clone(),
            revision_test_item("item#1", "bravo"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let page = source
        .export_logical_page(LogicalExportRequest {
            manifest_id: storage_backfill::LogicalBackfillId::new("manifest").unwrap(),
            domain: LogicalBackfillDomain::DurableRevisions,
            table_name: Some(table_name.as_ref().to_string()),
            cursor: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(page.records.len(), 1);

    destination
        .import_logical_chunk(
            &LogicalBackfillManifest::for_policy(
                storage_backfill::LogicalBackfillId::new("manifest").unwrap(),
                &SyncLearnerCatchupPolicy,
                "sqlite",
                "sqlite",
                vec![LogicalBackfillDomain::DurableRevisions],
            ),
            LogicalBackfillChunk {
                summary: LogicalBackfillChunkSummary {
                    id: LogicalBackfillChunkId::new("chunk-1").unwrap(),
                    domain: LogicalBackfillDomain::DurableRevisions,
                    record_count: page.records.len() as u64,
                    checksum: LogicalBackfillChecksum::new("unchecked").unwrap(),
                },
                records: page.records,
            },
        )
        .await
        .unwrap();

    let revision = call_sqlite(&destination.connection, move |conn| {
        conn.query_row(
            "SELECT revision FROM item_revisions WHERE table_name = ?1",
            [table_name.as_ref()],
            |row| row.get::<_, i64>(0),
        )
        .map_err(crate::error_handler::map_sqlite_error)
    })
    .await
    .unwrap();
    assert_eq!(revision, 2);
}

#[tokio::test]
async fn sqlite_logical_ttl_export_import_preserves_config_blob() {
    let table_name = TableName::new("logical_ttl_import");
    let source = create_revision_test_table(table_name.as_ref()).await;
    let destination = create_revision_test_table(table_name.as_ref()).await;
    call_sqlite(&source.connection, {
        let table_name = table_name.clone();
        move |conn| {
            conn.execute(
                "INSERT INTO sys_ttl_configs (table_name, config_blob) VALUES (?1, ?2)",
                (table_name.as_ref(), vec![1_u8, 2, 3, 4]),
            )
            .map_err(crate::error_handler::map_sqlite_error)?;
            Ok(())
        }
    })
    .await
    .unwrap();

    let page = source
        .export_logical_page(LogicalExportRequest {
            manifest_id: storage_backfill::LogicalBackfillId::new("manifest").unwrap(),
            domain: LogicalBackfillDomain::TtlRecords,
            table_name: Some(table_name.as_ref().to_string()),
            cursor: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(page.records.len(), 1);

    destination
        .import_logical_chunk(
            &LogicalBackfillManifest::for_policy(
                storage_backfill::LogicalBackfillId::new("manifest").unwrap(),
                &SyncLearnerCatchupPolicy,
                "sqlite",
                "sqlite",
                vec![LogicalBackfillDomain::TtlRecords],
            ),
            LogicalBackfillChunk {
                summary: LogicalBackfillChunkSummary {
                    id: LogicalBackfillChunkId::new("chunk-1").unwrap(),
                    domain: LogicalBackfillDomain::TtlRecords,
                    record_count: page.records.len() as u64,
                    checksum: LogicalBackfillChecksum::new("unchecked").unwrap(),
                },
                records: page.records,
            },
        )
        .await
        .unwrap();

    let config_blob = call_sqlite(&destination.connection, move |conn| {
        conn.query_row(
            "SELECT config_blob FROM sys_ttl_configs WHERE table_name = ?1",
            [table_name.as_ref()],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .map_err(crate::error_handler::map_sqlite_error)
    })
    .await
    .unwrap();
    assert_eq!(config_blob, vec![1_u8, 2, 3, 4]);
}

#[tokio::test]
async fn sqlite_logical_table_metadata_export_import_preserves_table_row() {
    let table_name = TableName::new("logical_metadata_import");
    let source = create_revision_test_table(table_name.as_ref()).await;
    let destination = create_revision_test_table(table_name.as_ref()).await;

    let page = source
        .export_logical_page(LogicalExportRequest {
            manifest_id: storage_backfill::LogicalBackfillId::new("manifest").unwrap(),
            domain: LogicalBackfillDomain::TableMetadata,
            table_name: Some(table_name.as_ref().to_string()),
            cursor: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(page.records.len(), 1);

    destination
        .import_logical_chunk(
            &LogicalBackfillManifest::for_policy(
                storage_backfill::LogicalBackfillId::new("manifest").unwrap(),
                &SyncLearnerCatchupPolicy,
                "sqlite",
                "sqlite",
                vec![LogicalBackfillDomain::TableMetadata],
            ),
            LogicalBackfillChunk {
                summary: LogicalBackfillChunkSummary {
                    id: LogicalBackfillChunkId::new("chunk-1").unwrap(),
                    domain: LogicalBackfillDomain::TableMetadata,
                    record_count: page.records.len() as u64,
                    checksum: LogicalBackfillChecksum::new("unchecked").unwrap(),
                },
                records: page.records,
            },
        )
        .await
        .unwrap();

    let source_info = source.get_table_info(&table_name).await.unwrap();
    let destination_info = destination.get_table_info(&table_name).await.unwrap();
    assert_eq!(destination_info.table_name, source_info.table_name);
    assert_eq!(
        destination_info
            .key_schema
            .iter()
            .map(|key| (&key.attribute_name, format!("{:?}", key.key_type)))
            .collect::<Vec<_>>(),
        source_info
            .key_schema
            .iter()
            .map(|key| (&key.attribute_name, format!("{:?}", key.key_type)))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        destination_info
            .attribute_definitions
            .iter()
            .map(|attribute| {
                (
                    &attribute.attribute_name,
                    format!("{:?}", attribute.attribute_type),
                )
            })
            .collect::<Vec<_>>(),
        source_info
            .attribute_definitions
            .iter()
            .map(|attribute| {
                (
                    &attribute.attribute_name,
                    format!("{:?}", attribute.attribute_type),
                )
            })
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn sqlite_logical_job_table_item_export_import_preserves_lock_rows() {
    let table_name = TableName::new("job");
    let source = create_job_lock_test_table().await;
    let destination = create_job_lock_test_table().await;
    source
        .put_item(
            table_name.clone(),
            HashMap::from([
                (
                    "pk".to_string(),
                    AttributeValue::S("JOB#ttl-sweep".to_string()),
                ),
                ("sk".to_string(), AttributeValue::S("LOCK".to_string())),
                (
                    "leased_by".to_string(),
                    AttributeValue::S("worker-a".to_string()),
                ),
                (
                    "lease_until_ms".to_string(),
                    AttributeValue::N("12345".to_string()),
                ),
                (
                    "job_id".to_string(),
                    AttributeValue::S("ttl-sweep".to_string()),
                ),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let page = source
        .export_logical_page(LogicalExportRequest {
            manifest_id: storage_backfill::LogicalBackfillId::new("manifest").unwrap(),
            domain: LogicalBackfillDomain::ItemRecords,
            table_name: Some(table_name.as_ref().to_string()),
            cursor: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(page.records.len(), 1);

    destination
        .import_logical_chunk(
            &LogicalBackfillManifest::for_policy(
                storage_backfill::LogicalBackfillId::new("manifest").unwrap(),
                &SyncLearnerCatchupPolicy,
                "sqlite",
                "sqlite",
                vec![LogicalBackfillDomain::ItemRecords],
            ),
            LogicalBackfillChunk {
                summary: LogicalBackfillChunkSummary {
                    id: LogicalBackfillChunkId::new("chunk-1").unwrap(),
                    domain: LogicalBackfillDomain::ItemRecords,
                    record_count: page.records.len() as u64,
                    checksum: LogicalBackfillChecksum::new("unchecked").unwrap(),
                },
                records: page.records,
            },
        )
        .await
        .unwrap();

    let row = destination
        .get_item(
            table_name,
            KeyAttributes::from([
                (
                    "pk".to_string(),
                    AttributeValue::S("JOB#ttl-sweep".to_string()),
                ),
                ("sk".to_string(), AttributeValue::S("LOCK".to_string())),
            ]),
            true,
        )
        .await
        .unwrap()
        .expect("job lock row")
        .to_attribute_map()
        .unwrap();
    assert_eq!(
        row.get("leased_by"),
        Some(&AttributeValue::S("worker-a".to_string()))
    );
    assert_eq!(
        row.get("lease_until_ms"),
        Some(&AttributeValue::N("12345".to_string()))
    );
}

#[tokio::test]
async fn sqlite_logical_stream_export_import_preserves_metadata_items_and_cursors() {
    let source = SQLiteStorageProvider::new(":memory:").await.unwrap();
    source.initialize_storage().await.unwrap();
    source.initialize_stream().await.unwrap();
    let destination = SQLiteStorageProvider::new(":memory:").await.unwrap();
    destination.initialize_storage().await.unwrap();
    destination.initialize_stream().await.unwrap();

    let user_stream_name = UserStreamName::new("logical-stream-source");
    let stream_name = source
        .create_stream(user_stream_name.clone(), None, Default::default())
        .await
        .unwrap();
    let first_item = source
        .append_item(stream_name.clone(), b"first", None)
        .await
        .unwrap();
    source
        .append_item(stream_name.clone(), b"second", None)
        .await
        .unwrap();
    let cursor_name = CursorName::new("logical-consumer");
    source
        .create_cursor(
            stream_name.clone(),
            cursor_name.clone(),
            CursorPosition::Head,
        )
        .await
        .unwrap();
    source
        .advance_cursor(stream_name.clone(), cursor_name.clone(), first_item)
        .await
        .unwrap();

    let page = source
        .export_logical_page(LogicalExportRequest {
            manifest_id: storage_backfill::LogicalBackfillId::new("manifest").unwrap(),
            domain: LogicalBackfillDomain::StreamRecords,
            table_name: Some(user_stream_name.to_string()),
            cursor: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(page.domain, LogicalBackfillDomain::StreamRecords);
    assert_eq!(page.records.len(), 5);

    destination
        .import_logical_chunk(
            &LogicalBackfillManifest::for_policy(
                storage_backfill::LogicalBackfillId::new("manifest").unwrap(),
                &SyncLearnerCatchupPolicy,
                "sqlite",
                "sqlite",
                vec![LogicalBackfillDomain::StreamRecords],
            ),
            LogicalBackfillChunk {
                summary: LogicalBackfillChunkSummary {
                    id: LogicalBackfillChunkId::new("chunk-1").unwrap(),
                    domain: LogicalBackfillDomain::StreamRecords,
                    record_count: page.records.len() as u64,
                    checksum: LogicalBackfillChecksum::new("unchecked").unwrap(),
                },
                records: page.records,
            },
        )
        .await
        .unwrap();

    let imported_stream = destination
        .get_stream(user_stream_name.clone())
        .await
        .unwrap()
        .expect("user stream");
    assert_eq!(imported_stream.internal_id, stream_name);
    let imported_items = destination
        .read_forward(stream_name.clone(), None, 10)
        .await
        .unwrap();
    assert_eq!(imported_items.items.len(), 2);
    assert_eq!(imported_items.items[0].data, b"first");
    assert_eq!(imported_items.items[1].data, b"second");

    let imported_cursor = destination
        .get_cursor(stream_name, cursor_name)
        .await
        .unwrap()
        .expect("cursor");
    assert_eq!(imported_cursor.position, first_item);

    let format_version = call_sqlite(&destination.connection, move |conn| {
        conn.query_row(
            "SELECT format_version FROM sys_stream_format_metadata WHERE format_key = ?1",
            ["item_versioned_stream"],
            |row| row.get::<_, i64>(0),
        )
        .map_err(crate::error_handler::map_sqlite_error)
    })
    .await
    .unwrap();
    assert_eq!(format_version, 1);
}

#[tokio::test]
async fn sqlite_logical_gsi_export_import_preserves_physical_rows_and_backfill_state() {
    let table_name = TableName::new("logical_gsi_import");
    let index_name = IndexName::new("TestGSI");
    let source = create_empty_gsi_test_table_with_settings(
        table_name.as_ref(),
        storage_provider::SqliteSettings {
            immediate_gsi_consistency: true,
            ..Default::default()
        },
    )
    .await;
    let destination = SQLiteStorageProvider::new(":memory:").await.unwrap();
    destination.initialize_storage().await.unwrap();
    destination.initialize_stream().await.unwrap();

    source
        .put_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("user1".to_string())),
                ("sk".to_string(), AttributeValue::S("item1".to_string())),
                ("gsi_pk".to_string(), AttributeValue::S("grp".to_string())),
                ("gsi_sk".to_string(), AttributeValue::S("001".to_string())),
                ("value".to_string(), AttributeValue::S("data".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    call_sqlite(&source.connection, {
        let table_name = table_name.clone();
        let index_name = index_name.clone();
        move |conn| {
            let now = TimestampMillis::now();
            let (sql, params) = sql_statements::upsert_gsi_backfill_start(
                &table_name,
                &index_name,
                "Done",
                Some("scan-complete"),
                Some("stream-tail"),
                &now,
                &now,
            );
            conn.execute(sql, params)
                .map_err(crate::error_handler::map_sqlite_error)?;
            Ok(())
        }
    })
    .await
    .unwrap();

    let table_page = source
        .export_logical_page(LogicalExportRequest {
            manifest_id: storage_backfill::LogicalBackfillId::new("manifest").unwrap(),
            domain: LogicalBackfillDomain::TableMetadata,
            table_name: Some(table_name.as_ref().to_string()),
            cursor: None,
            limit: 10,
        })
        .await
        .unwrap();
    let gsi_page = source
        .export_logical_page(LogicalExportRequest {
            manifest_id: storage_backfill::LogicalBackfillId::new("manifest").unwrap(),
            domain: LogicalBackfillDomain::GsiRecords,
            table_name: Some(table_name.as_ref().to_string()),
            cursor: None,
            limit: 10,
        })
        .await
        .unwrap_or_else(|err| panic!("export gsi records: {err:?}"));
    assert!(
        gsi_page.records.len() >= 2,
        "expected backfill state and at least one physical gsi row"
    );

    let manifest = LogicalBackfillManifest::for_policy(
        storage_backfill::LogicalBackfillId::new("manifest").unwrap(),
        &SyncLearnerCatchupPolicy,
        "sqlite",
        "sqlite",
        vec![
            LogicalBackfillDomain::TableMetadata,
            LogicalBackfillDomain::GsiRecords,
        ],
    );
    destination
        .import_logical_chunk(
            &manifest,
            LogicalBackfillChunk {
                summary: LogicalBackfillChunkSummary {
                    id: LogicalBackfillChunkId::new("table-chunk").unwrap(),
                    domain: LogicalBackfillDomain::TableMetadata,
                    record_count: table_page.records.len() as u64,
                    checksum: LogicalBackfillChecksum::new("unchecked").unwrap(),
                },
                records: table_page.records,
            },
        )
        .await
        .unwrap();
    destination
        .import_logical_chunk(
            &manifest,
            LogicalBackfillChunk {
                summary: LogicalBackfillChunkSummary {
                    id: LogicalBackfillChunkId::new("gsi-chunk").unwrap(),
                    domain: LogicalBackfillDomain::GsiRecords,
                    record_count: gsi_page.records.len() as u64,
                    checksum: LogicalBackfillChecksum::new("unchecked").unwrap(),
                },
                records: gsi_page.records,
            },
        )
        .await
        .unwrap();

    let gsi_items = destination
        .query_table(&gsi_query_request(table_name.as_ref(), "grp"))
        .await
        .unwrap();
    assert_eq!(gsi_items.0.len(), 1);
    assert_eq!(
        gsi_items.0[0].get("value"),
        Some(&AttributeValue::S("data".to_string()))
    );

    let backfill_status = call_sqlite(&destination.connection, move |conn| {
        let (sql, params) = sql_statements::get_gsi_backfill(&table_name, &index_name);
        conn.query_row(sql, params, |row| row.get::<_, String>(0))
            .map_err(crate::error_handler::map_sqlite_error)
    })
    .await
    .unwrap();
    assert_eq!(backfill_status, "Done");
}

#[tokio::test]
async fn sqlite_logical_storage_control_plane_export_import_uses_replication_table() {
    let source = create_storage_control_plane_test_table().await;
    let destination = create_storage_control_plane_test_table().await;
    let table_name = TableName::new("sys_storage_replication");

    source
        .put_item(
            table_name.clone(),
            HashMap::from([
                (
                    "pk".to_string(),
                    AttributeValue::S("TABLE#orders".to_string()),
                ),
                ("sk".to_string(), AttributeValue::S("CONFIG".to_string())),
                (
                    "replica_epoch".to_string(),
                    AttributeValue::N("7".to_string()),
                ),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let page = source
        .export_logical_page(LogicalExportRequest {
            manifest_id: storage_backfill::LogicalBackfillId::new("manifest").unwrap(),
            domain: LogicalBackfillDomain::StorageControlPlane,
            table_name: None,
            cursor: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(page.domain, LogicalBackfillDomain::StorageControlPlane);
    assert_eq!(page.records.len(), 1);

    destination
        .import_logical_chunk(
            &LogicalBackfillManifest::for_policy(
                storage_backfill::LogicalBackfillId::new("manifest").unwrap(),
                &SyncLearnerCatchupPolicy,
                "sqlite",
                "sqlite",
                vec![LogicalBackfillDomain::StorageControlPlane],
            ),
            LogicalBackfillChunk {
                summary: LogicalBackfillChunkSummary {
                    id: LogicalBackfillChunkId::new("control-plane-chunk").unwrap(),
                    domain: LogicalBackfillDomain::StorageControlPlane,
                    record_count: page.records.len() as u64,
                    checksum: LogicalBackfillChecksum::new("unchecked").unwrap(),
                },
                records: page.records,
            },
        )
        .await
        .unwrap();

    let imported = destination
        .get_item(
            table_name,
            KeyAttributes::from([
                (
                    "pk".to_string(),
                    AttributeValue::S("TABLE#orders".to_string()),
                ),
                ("sk".to_string(), AttributeValue::S("CONFIG".to_string())),
            ]),
            true,
        )
        .await
        .unwrap()
        .expect("control-plane record")
        .to_attribute_map()
        .unwrap();
    assert_eq!(
        imported.get("replica_epoch"),
        Some(&AttributeValue::N("7".to_string()))
    );
}

async fn create_job_lock_test_table() -> SQLiteStorageProvider {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();
    provider
        .create_table(&CreateTableRequest::new(
            TableName::new("job"),
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
        .unwrap();
    provider
}

async fn create_storage_control_plane_test_table() -> SQLiteStorageProvider {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();
    provider
        .create_table(&CreateTableRequest::new(
            TableName::new("sys_storage_replication"),
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
        .unwrap();
    provider
}

#[tokio::test]
async fn sqlite_rolled_back_item_stream_allocation_is_not_visible() {
    let table_name = TableName::new("revision_rollback");
    let provider = create_revision_test_table(table_name.as_ref()).await;
    let key = revision_test_key("item#rollback");
    let rollback_table = table_name.clone();
    let rollback_key = key.clone();

    let result: StorageResult<()> =
        crate::transaction_manager::with_transaction(&provider.connection, move |sqlite| {
            let revision = SQLiteStorageProvider::do_bump_item_revision(
                &rollback_table,
                &rollback_key,
                sqlite,
            )?;
            assert_eq!(revision, 1);
            Err(StorageError::internal(
                "force rollback after revision allocation",
            ))
        })
        .await;

    assert!(result.is_err());
    let proof = provider
        .get_item_with_durable_proof(DurablePointReadRequest {
            table_name: table_name.clone(),
            key,
            consistent_read: true,
        })
        .await
        .unwrap();
    let DurablePointReadProof::Absent { proof } = proof else {
        panic!("rolled-back write should not create an item");
    };
    assert_eq!(proof.as_bytes(), 0_i64.to_be_bytes());

    let page = StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
        .await
        .unwrap();
    assert!(
        page.items.is_empty(),
        "rolled-back allocation should not leave visible stream records"
    );
}

#[tokio::test]
async fn sqlite_guarded_put_commits_when_absence_proof_matches() {
    let table_name = "guarded_put_success";
    let provider = create_revision_test_table(table_name).await;
    let absence = durable_proof(&provider, table_name, "item#1").await;

    provider
        .guarded_put_item(GuardedPutItemRequest {
            table_name: TableName::new(table_name),
            item: revision_test_item("item#1", "alpha"),
            guard: proof_guard(&absence),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values: Some(AllOld::AllOld),
        })
        .await
        .expect("matching absence proof should commit");

    let present = durable_proof(&provider, table_name, "item#1").await;
    assert!(matches!(present, DurablePointReadProof::Present { .. }));
    assert_ne!(proof_revision(&absence), proof_revision(&present));
}

#[tokio::test]
async fn sqlite_guarded_put_returns_guard_conflict_when_proof_is_stale() {
    let table_name = "guarded_put_conflict";
    let provider = create_revision_test_table(table_name).await;
    let stale_absence = durable_proof(&provider, table_name, "item#1").await;
    provider
        .put_item(
            TableName::new(table_name),
            revision_test_item("item#1", "alpha"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let error = provider
        .guarded_put_item(GuardedPutItemRequest {
            table_name: TableName::new(table_name),
            item: revision_test_item("item#1", "bravo"),
            guard: proof_guard(&stale_absence),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values: None,
        })
        .await
        .expect_err("stale proof should conflict");

    assert!(matches!(error.as_ref(), StorageEnum::GuardConflict { .. }));
}

#[tokio::test]
async fn sqlite_guarded_delete_and_update_validate_present_revision() {
    let table_name = "guarded_delete_update";
    let provider = create_revision_test_table(table_name).await;
    provider
        .put_item(
            TableName::new(table_name),
            revision_test_item("item#1", "alpha"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let present = durable_proof(&provider, table_name, "item#1").await;
    provider
        .guarded_update_item(GuardedUpdateItemRequest {
            request: UpdateItemRequest {
                table_name: TableName::new(table_name),
                key: revision_test_key("item#1"),
                update_expression: "SET #value = :value".to_string(),
                attribute_updates: None,
                condition_expression: None,
                expression_attribute_names: Some(HashMap::from([(
                    "#value".to_string(),
                    "value".to_string(),
                )])),
                expression_attribute_values: Some(HashMap::from([(
                    ":value".to_string(),
                    AttributeValue::S("bravo".to_string()),
                )])),
                expected: None,
                conditional_operator: None,
                return_values: Some(ReturnValuesOldNewUpdated::AllNew),
                return_consumed_capacity: None,
                return_item_collection_metrics: None,
                return_values_on_condition_check_failure: None,
            },
            guard: proof_guard(&present),
        })
        .await
        .expect("matching present revision should update");

    let stale_error = provider
        .guarded_delete_item(GuardedDeleteItemRequest {
            table_name: TableName::new(table_name),
            key: revision_test_key("item#1"),
            guard: proof_guard(&present),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
        })
        .await
        .expect_err("stale present revision should conflict");
    assert!(matches!(
        stale_error.as_ref(),
        StorageEnum::GuardConflict { .. }
    ));

    let current = durable_proof(&provider, table_name, "item#1").await;
    provider
        .guarded_delete_item(GuardedDeleteItemRequest {
            table_name: TableName::new(table_name),
            key: revision_test_key("item#1"),
            guard: proof_guard(&current),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
        })
        .await
        .expect("matching present revision should delete");
    let absent = durable_proof(&provider, table_name, "item#1").await;
    assert!(matches!(absent, DurablePointReadProof::Absent { .. }));
}

async fn create_stream_replication_table() -> SQLiteStorageProvider {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let create_request = CreateTableRequest::new(
        TableName::new("ReplicationStreamTable"),
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
    )
    .with_stream_specification(Some(StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(StreamViewType::NewAndOldImages),
    }));

    provider.create_table(&create_request).await.unwrap();
    provider
}

#[tokio::test]
async fn list_tables_respects_exclusive_start_key() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();

    let table_a = TableName::new("list_tables_a");
    let table_b = TableName::new("list_tables_b");
    let table_c = TableName::new("list_tables_c");

    for table_name in [&table_a, &table_b, &table_c] {
        provider
            .create_table(&CreateTableRequest::new(
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
            ))
            .await
            .unwrap();
    }

    let tables = provider
        .list_tables(10, Some(table_a.clone()))
        .await
        .unwrap();
    let names: Vec<_> = tables
        .into_iter()
        .map(|table| table.table_name.to_string())
        .filter(|name| name.starts_with("list_tables_"))
        .collect();

    assert_eq!(names, vec![table_b.to_string(), table_c.to_string()]);
}

fn sqlite_sample_replication_metadata(
    region_name: &str,
    sequence_suffix: u64,
) -> ReplicationEventMetadata {
    let mut bytes = [0_u8; 12];
    bytes[4..].copy_from_slice(&sequence_suffix.to_be_bytes());
    let physical_ms = 1_700_000_000_000_i64 + sequence_suffix as i64;

    ReplicationEventMetadata {
        origin_region: region_name.to_string(),
        origin_sequence: StreamItemId::from(bytes),
        origin_hlc: ReplicationHybridLogicalClock {
            physical_ms: TimestampMillis::from_timestamp(physical_ms),
            logical: sequence_suffix as u32,
        },
        origin_commit_ts: TimestampMillis::from_timestamp(physical_ms),
        table_replica_epoch: 2,
        write_source: ReplicationWriteSource::Replicated,
    }
}

async fn create_empty_gsi_test_table_with_settings(
    table_name: &str,
    settings: storage_provider::SqliteSettings,
) -> SQLiteStorageProvider {
    let provider = SQLiteStorageProvider::new_with_settings(":memory:", settings)
        .await
        .unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let create_request = CreateTableRequest::new(
        TableName::new(table_name),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_sk".to_string(),
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
    )
    .with_global_secondary_indexes(Some(vec![CreateGlobalSecondaryIndex {
        index_name: IndexName::new("TestGSI"),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "gsi_pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "gsi_sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }]));

    provider.create_table(&create_request).await.unwrap();

    provider
}

async fn create_gsi_test_table(table_name: &str) -> SQLiteStorageProvider {
    let provider = create_empty_gsi_test_table_with_settings(
        table_name,
        storage_provider::SqliteSettings::default(),
    )
    .await;

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("user1".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("item1".to_string()));
    item.insert("gsi_pk".to_string(), AttributeValue::S("grp".to_string()));
    item.insert("gsi_sk".to_string(), AttributeValue::S("001".to_string()));
    item.insert("value".to_string(), AttributeValue::S("data".to_string()));
    provider
        .put_item(TableName::new(table_name), item, None, None, None, None)
        .await
        .unwrap();

    provider.process_gsi_updates().await.unwrap();

    provider
}

fn gsi_query_request(table_name: &str, gsi_pk: &str) -> storage_types::QueryTableRequest {
    storage_types::QueryTableRequest {
        table_name: TableName::new(table_name),
        index_name: Some(IndexName::new("TestGSI")),
        key_condition_expression: "gsi_pk = :p".to_string(),
        expression_attribute_values: Some(HashMap::from([(
            ":p".to_string(),
            AttributeValue::S(gsi_pk.to_string()),
        )])),
        expression_attribute_names: None,
        limit: Some(100),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    }
}

async fn latest_system_stream_tail(provider: &SQLiteStorageProvider) -> Option<StreamItemId> {
    StreamProvider::read_forward(provider, StreamName::system_table_stream(), None, 100)
        .await
        .unwrap()
        .items
        .last()
        .map(|item| item.id)
}

async fn start_test_gsi_backfill(
    provider: &SQLiteStorageProvider,
    table_name: &TableName,
    captured_stream_tail: Option<StreamItemId>,
) {
    let now = TimestampMillis::now();
    call_sqlite(&provider.connection, {
        let table_name = table_name.clone();
        let index_name = IndexName::new("TestGSI");
        let captured_stream_tail = captured_stream_tail.map(|tail| tail.to_string());
        move |conn| {
            let (sql, params) = sql_statements::upsert_gsi_backfill_start(
                &table_name,
                &index_name,
                "Backfilling",
                None,
                captured_stream_tail.as_deref(),
                &now,
                &now,
            );
            conn.execute(sql, params)
                .map_err(crate::error_handler::map_sqlite_error)
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn sqlite_gsi_visibility_is_delayed_by_default() {
    let provider = create_empty_gsi_test_table_with_settings(
        "sqlite_gsi_default_delayed",
        storage_provider::SqliteSettings::default(),
    )
    .await;

    provider
        .put_item(
            TableName::new("sqlite_gsi_default_delayed"),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("user1".to_string())),
                ("sk".to_string(), AttributeValue::S("item1".to_string())),
                ("gsi_pk".to_string(), AttributeValue::S("grp".to_string())),
                ("gsi_sk".to_string(), AttributeValue::S("001".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let query = storage_types::QueryTableRequest {
        table_name: TableName::new("sqlite_gsi_default_delayed"),
        index_name: Some(IndexName::new("TestGSI")),
        key_condition_expression: "gsi_pk = :p".to_string(),
        expression_attribute_values: Some(HashMap::from([(
            ":p".to_string(),
            AttributeValue::S("grp".to_string()),
        )])),
        expression_attribute_names: None,
        limit: Some(100),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };

    let (before, _) = provider.query_table(&query).await.unwrap();
    assert!(
        before.is_empty(),
        "default mode should delay GSI visibility"
    );

    provider.run_job(GSI_UPDATE_JOB).await.unwrap();

    let (after, _) = provider.query_table(&query).await.unwrap();
    assert_eq!(after.len(), 1, "gsi-update should publish the pending row");
}

#[tokio::test]
async fn sqlite_immediate_gsi_consistency_updates_indexes_inline() {
    let provider = create_empty_gsi_test_table_with_settings(
        "sqlite_gsi_immediate",
        storage_provider::SqliteSettings {
            immediate_gsi_consistency: true,
            ..storage_provider::SqliteSettings::default()
        },
    )
    .await;

    provider
        .put_item(
            TableName::new("sqlite_gsi_immediate"),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("user1".to_string())),
                ("sk".to_string(), AttributeValue::S("item1".to_string())),
                ("gsi_pk".to_string(), AttributeValue::S("grp".to_string())),
                ("gsi_sk".to_string(), AttributeValue::S("001".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let query = storage_types::QueryTableRequest {
        table_name: TableName::new("sqlite_gsi_immediate"),
        index_name: Some(IndexName::new("TestGSI")),
        key_condition_expression: "gsi_pk = :p".to_string(),
        expression_attribute_values: Some(HashMap::from([(
            ":p".to_string(),
            AttributeValue::S("grp".to_string()),
        )])),
        expression_attribute_names: None,
        limit: Some(100),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };

    let (before_job, _) = provider.query_table(&query).await.unwrap();
    assert_eq!(
        before_job.len(),
        1,
        "immediate mode should publish the GSI row in the main write transaction"
    );

    provider.run_job(GSI_UPDATE_JOB).await.unwrap();

    let (after_job, _) = provider.query_table(&query).await.unwrap();
    assert_eq!(
        after_job.len(),
        1,
        "no-op job should not duplicate index rows"
    );
}

#[tokio::test]
async fn sqlite_immediate_gsi_consistency_moves_index_entries_inline() {
    let provider = create_empty_gsi_test_table_with_settings(
        "sqlite_gsi_immediate_move",
        storage_provider::SqliteSettings {
            immediate_gsi_consistency: true,
            ..storage_provider::SqliteSettings::default()
        },
    )
    .await;

    provider
        .put_item(
            TableName::new("sqlite_gsi_immediate_move"),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("user1".to_string())),
                ("sk".to_string(), AttributeValue::S("item1".to_string())),
                ("gsi_pk".to_string(), AttributeValue::S("grp".to_string())),
                ("gsi_sk".to_string(), AttributeValue::S("001".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    provider
        .put_item(
            TableName::new("sqlite_gsi_immediate_move"),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("user1".to_string())),
                ("sk".to_string(), AttributeValue::S("item1".to_string())),
                ("gsi_pk".to_string(), AttributeValue::S("grp-2".to_string())),
                ("gsi_sk".to_string(), AttributeValue::S("002".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let old_query = storage_types::QueryTableRequest {
        table_name: TableName::new("sqlite_gsi_immediate_move"),
        index_name: Some(IndexName::new("TestGSI")),
        key_condition_expression: "gsi_pk = :p".to_string(),
        expression_attribute_values: Some(HashMap::from([(
            ":p".to_string(),
            AttributeValue::S("grp".to_string()),
        )])),
        expression_attribute_names: None,
        limit: Some(100),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };
    let (old_items, _) = provider.query_table(&old_query).await.unwrap();
    assert!(
        old_items.is_empty(),
        "immediate mode should remove the old GSI row in the same write transaction"
    );

    let new_query = storage_types::QueryTableRequest {
        table_name: TableName::new("sqlite_gsi_immediate_move"),
        index_name: Some(IndexName::new("TestGSI")),
        key_condition_expression: "gsi_pk = :p".to_string(),
        expression_attribute_values: Some(HashMap::from([(
            ":p".to_string(),
            AttributeValue::S("grp-2".to_string()),
        )])),
        expression_attribute_names: None,
        limit: Some(100),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };
    let (new_items, _) = provider.query_table(&new_query).await.unwrap();
    assert_eq!(
        new_items.len(),
        1,
        "immediate mode should insert the new GSI row in the same write transaction"
    );
}

#[tokio::test]
async fn gsi_updates_add_and_ignore_missing_sqlite() {
    // Create an empty table with a GSI
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let create_request = CreateTableRequest::new(
        TableName::new("GSIFieldLifecycleTest"),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_sk".to_string(),
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
    )
    .with_global_secondary_indexes(Some(vec![CreateGlobalSecondaryIndex {
        index_name: IndexName::new("TestGSI"),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "gsi_pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "gsi_sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }]));

    provider.create_table(&create_request).await.unwrap();

    // A: initially missing GSI fields
    let mut item_a = std::collections::HashMap::new();
    item_a.insert("pk".to_string(), AttributeValue::S("user1".to_string()));
    item_a.insert("sk".to_string(), AttributeValue::S("a".to_string()));

    // B: present in GSI
    let mut item_b = std::collections::HashMap::new();
    item_b.insert("pk".to_string(), AttributeValue::S("user1".to_string()));
    item_b.insert("sk".to_string(), AttributeValue::S("b".to_string()));
    item_b.insert("gsi_pk".to_string(), AttributeValue::S("grp1".to_string()));
    item_b.insert("gsi_sk".to_string(), AttributeValue::S("001".to_string()));

    // C: always missing GSI fields (ignored)
    let mut item_c = std::collections::HashMap::new();
    item_c.insert("pk".to_string(), AttributeValue::S("user1".to_string()));
    item_c.insert("sk".to_string(), AttributeValue::S("c".to_string()));

    provider
        .put_item(
            TableName::new("GSIFieldLifecycleTest"),
            item_a,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    provider
        .put_item(
            TableName::new("GSIFieldLifecycleTest"),
            item_b.clone(),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    provider
        .put_item(
            TableName::new("GSIFieldLifecycleTest"),
            item_c,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    // Process GSI updates: only B should appear
    provider.process_gsi_updates().await.unwrap();

    let q1 = storage_types::QueryTableRequest {
        table_name: TableName::new("GSIFieldLifecycleTest"),
        index_name: Some(IndexName::new("TestGSI")),
        key_condition_expression: "gsi_pk = :p".to_string(),
        expression_attribute_values: Some({
            let mut m = std::collections::HashMap::new();
            m.insert(":p".to_string(), AttributeValue::S("grp1".to_string()));
            m
        }),
        expression_attribute_names: None,
        limit: Some(100),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };
    let (items1, _lek1) = provider.query_table(&q1).await.unwrap();
    assert_eq!(items1.len(), 1, "Only item B should be indexed initially");

    // Update A to add GSI fields → should be added to index
    let mut item_a_updated = std::collections::HashMap::new();
    item_a_updated.insert("pk".to_string(), AttributeValue::S("user1".to_string()));
    item_a_updated.insert("sk".to_string(), AttributeValue::S("a".to_string()));
    item_a_updated.insert("gsi_pk".to_string(), AttributeValue::S("grp1".to_string()));
    item_a_updated.insert("gsi_sk".to_string(), AttributeValue::S("002".to_string()));

    provider
        .put_item(
            TableName::new("GSIFieldLifecycleTest"),
            item_a_updated,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    provider.process_gsi_updates().await.unwrap();

    let (items2, _lek2) = provider.query_table(&q1).await.unwrap();
    assert_eq!(
        items2.len(),
        2,
        "Items A and B should both be indexed after update"
    );
}

#[tokio::test]
async fn gsi_backfill_missing_required_stream_history_fails_closed_sqlite() {
    let table_name = TableName::new("GsiBackfillMissingHistory");
    let index_name = IndexName::new("TestGSI");
    let provider =
        create_empty_gsi_test_table_with_settings(table_name.as_ref(), Default::default()).await;

    let now = TimestampMillis::now();
    let missing_tail = stream_id_from_u64(42).to_string();
    let item_key = ItemKey::table_key(
        table_name.clone(),
        AttributeValue::S("user1".to_string()),
        Some(AttributeValue::S("item1".to_string())),
    );
    let item_stream = StreamName::table_item_stream(&table_name, &item_key).expect("item stream");
    let newer_pointer = build_pointer_stream_item(
        stream_id_from_u64(100),
        now,
        &table_name,
        item_stream.clone(),
    );
    insert_stream_item(
        &provider,
        &StreamName::table_stream(&table_name),
        &newer_pointer,
    )
    .await;

    call_sqlite(&provider.connection, {
        let table_name = table_name.clone();
        let index_name = index_name.clone();
        let missing_tail = missing_tail.clone();
        move |conn| {
            let (sql, params) = sql_statements::upsert_gsi_backfill_start(
                &table_name,
                &index_name,
                "Backfilling",
                None,
                Some(&missing_tail),
                &now,
                &now,
            );
            conn.execute(sql, params)
                .map_err(crate::error_handler::map_sqlite_error)
        }
    })
    .await
    .unwrap();

    provider
        .process_gsi_backfills()
        .await
        .expect_err("missing stream history must fail closed");

    let status = call_sqlite(&provider.connection, {
        let table_name = table_name.clone();
        let index_name = index_name.clone();
        move |conn| {
            let (sql, params) = sql_statements::get_gsi_backfill(&table_name, &index_name);
            conn.query_row(sql, params, |row| row.get::<_, String>(0))
                .map_err(crate::error_handler::map_sqlite_error)
        }
    })
    .await
    .unwrap();
    assert_eq!(status, "Backfilling");
}

#[tokio::test]
async fn gsi_backfill_fails_closed_after_required_stream_history_is_trimmed_sqlite() {
    let table_name = TableName::new("GsiBackfillTrimmedHistory");
    let provider =
        create_empty_gsi_test_table_with_settings(table_name.as_ref(), Default::default()).await;

    provider
        .put_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("user1".to_string())),
                ("sk".to_string(), AttributeValue::S("item1".to_string())),
                ("gsi_pk".to_string(), AttributeValue::S("grp".to_string())),
                ("gsi_sk".to_string(), AttributeValue::S("001".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let captured_tail = latest_system_stream_tail(&provider)
        .await
        .expect("initial write should create stream tail");
    start_test_gsi_backfill(&provider, &table_name, Some(captured_tail)).await;

    let cutoff = TimestampMillis::now()
        - (constants::STREAM_TRIM_RETENTION_HOURS * constants::MILLIS_PER_HOUR);
    force_all_stream_items_created_at(&provider, cutoff - 1_000).await;
    provider
        .run_job(storage_common::STREAM_TRIM_JOB)
        .await
        .unwrap();

    provider
        .process_gsi_backfills()
        .await
        .expect_err("trimmed required stream history must fail closed");

    let status = call_sqlite(&provider.connection, {
        let table_name = table_name.clone();
        let index_name = IndexName::new("TestGSI");
        move |conn| {
            let (sql, params) = sql_statements::get_gsi_backfill(&table_name, &index_name);
            conn.query_row(sql, params, |row| row.get::<_, String>(0))
                .map_err(crate::error_handler::map_sqlite_error)
        }
    })
    .await
    .unwrap();
    assert_eq!(status, "Backfilling");
}

#[tokio::test]
async fn gsi_backfill_drains_stream_updates_that_move_index_keys_sqlite() {
    let table_name = TableName::new("GsiBackfillMoveKey");
    let provider =
        create_empty_gsi_test_table_with_settings(table_name.as_ref(), Default::default()).await;

    provider
        .put_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("user1".to_string())),
                ("sk".to_string(), AttributeValue::S("item1".to_string())),
                ("gsi_pk".to_string(), AttributeValue::S("old".to_string())),
                ("gsi_sk".to_string(), AttributeValue::S("001".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    provider.process_gsi_updates().await.unwrap();
    let captured_tail = latest_system_stream_tail(&provider).await;

    provider
        .put_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("user1".to_string())),
                ("sk".to_string(), AttributeValue::S("item1".to_string())),
                ("gsi_pk".to_string(), AttributeValue::S("new".to_string())),
                ("gsi_sk".to_string(), AttributeValue::S("002".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    start_test_gsi_backfill(&provider, &table_name, captured_tail).await;

    provider.process_gsi_backfills().await.unwrap();

    let (old_items, _) = provider
        .query_table(&gsi_query_request(table_name.as_ref(), "old"))
        .await
        .unwrap();
    assert!(
        old_items.is_empty(),
        "stream catch-up must remove the stale old GSI key"
    );
    let (new_items, _) = provider
        .query_table(&gsi_query_request(table_name.as_ref(), "new"))
        .await
        .unwrap();
    assert_eq!(new_items.len(), 1);
}

#[tokio::test]
async fn gsi_backfill_drains_projected_attribute_updates_sqlite() {
    let table_name = TableName::new("GsiBackfillProjectedUpdate");
    let provider =
        create_empty_gsi_test_table_with_settings(table_name.as_ref(), Default::default()).await;

    provider
        .put_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("user1".to_string())),
                ("sk".to_string(), AttributeValue::S("item1".to_string())),
                ("gsi_pk".to_string(), AttributeValue::S("grp".to_string())),
                ("gsi_sk".to_string(), AttributeValue::S("001".to_string())),
                ("value".to_string(), AttributeValue::S("before".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    provider.process_gsi_updates().await.unwrap();
    let captured_tail = latest_system_stream_tail(&provider).await;

    provider
        .put_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("user1".to_string())),
                ("sk".to_string(), AttributeValue::S("item1".to_string())),
                ("gsi_pk".to_string(), AttributeValue::S("grp".to_string())),
                ("gsi_sk".to_string(), AttributeValue::S("001".to_string())),
                ("value".to_string(), AttributeValue::S("after".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    start_test_gsi_backfill(&provider, &table_name, captured_tail).await;

    provider.process_gsi_backfills().await.unwrap();

    let (items, _) = provider
        .query_table(&gsi_query_request(table_name.as_ref(), "grp"))
        .await
        .unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("value"),
        Some(&AttributeValue::S("after".to_string()))
    );
}

#[tokio::test]
async fn gsi_backfill_drains_stream_deletes_before_marking_done_sqlite() {
    let table_name = TableName::new("GsiBackfillDelete");
    let provider =
        create_empty_gsi_test_table_with_settings(table_name.as_ref(), Default::default()).await;

    provider
        .put_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("user1".to_string())),
                ("sk".to_string(), AttributeValue::S("item1".to_string())),
                ("gsi_pk".to_string(), AttributeValue::S("grp".to_string())),
                ("gsi_sk".to_string(), AttributeValue::S("001".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    provider.process_gsi_updates().await.unwrap();
    let captured_tail = latest_system_stream_tail(&provider).await;

    provider
        .delete_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("user1".to_string())),
                ("sk".to_string(), AttributeValue::S("item1".to_string())),
            ])
            .into(),
            None,
            None,
            None,
        )
        .await
        .unwrap();
    start_test_gsi_backfill(&provider, &table_name, captured_tail).await;

    provider.process_gsi_backfills().await.unwrap();

    let (items, _) = provider
        .query_table(&gsi_query_request(table_name.as_ref(), "grp"))
        .await
        .unwrap();
    assert!(
        items.is_empty(),
        "stream catch-up must remove a stale GSI row for a deleted item"
    );
}

#[tokio::test]
async fn gsi_backfill_tombstones_are_hidden_and_do_not_consume_query_limit_sqlite() {
    let table_name = TableName::new("GsiBackfillHiddenTombstone");
    let provider =
        create_empty_gsi_test_table_with_settings(table_name.as_ref(), Default::default()).await;

    for (sk, gsi_sk) in [("item1", "001"), ("item2", "002")] {
        provider
            .put_item(
                table_name.clone(),
                HashMap::from([
                    ("pk".to_string(), AttributeValue::S("user1".to_string())),
                    ("sk".to_string(), AttributeValue::S(sk.to_string())),
                    ("gsi_pk".to_string(), AttributeValue::S("grp".to_string())),
                    ("gsi_sk".to_string(), AttributeValue::S(gsi_sk.to_string())),
                ]),
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
    }
    provider.process_gsi_updates().await.unwrap();
    let captured_tail = latest_system_stream_tail(&provider).await;

    provider
        .delete_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("user1".to_string())),
                ("sk".to_string(), AttributeValue::S("item1".to_string())),
            ])
            .into(),
            None,
            None,
            None,
        )
        .await
        .unwrap();
    start_test_gsi_backfill(&provider, &table_name, captured_tail).await;

    provider.process_gsi_backfills().await.unwrap();

    let tombstone_count: i64 = call_sqlite(&provider.connection, {
        let physical_gsi = naming::physical_gsi_table_name(&table_name, &IndexName::new("TestGSI"));
        move |conn| {
            conn.query_row(
                &format!("SELECT COUNT(*) FROM \"{physical_gsi}\" WHERE __aux_tombstone = 1"),
                [],
                |row| row.get(0),
            )
            .map_err(crate::error_handler::map_sqlite_error)
        }
    })
    .await
    .unwrap();
    assert_eq!(tombstone_count, 1);

    let mut query = gsi_query_request(table_name.as_ref(), "grp");
    query.limit = Some(1);
    let (items, last_evaluated_key) = provider.query_table(&query).await.unwrap();
    assert_eq!(
        items.len(),
        1,
        "hidden tombstone must not consume the visible item limit"
    );
    assert_eq!(
        items[0].get("sk"),
        Some(&AttributeValue::S("item2".to_string()))
    );
    assert!(
        last_evaluated_key.is_none(),
        "filtered tombstone should not force pagination"
    );
}

#[tokio::test]
async fn gsi_backfill_scan_cannot_overwrite_newer_tombstone_sqlite() {
    let table_name = TableName::new("GsiBackfillVersionedTombstone");
    let index_name = IndexName::new("TestGSI");
    let provider =
        create_empty_gsi_test_table_with_settings(table_name.as_ref(), Default::default()).await;

    provider
        .put_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("user1".to_string())),
                ("sk".to_string(), AttributeValue::S("item1".to_string())),
                ("gsi_pk".to_string(), AttributeValue::S("grp".to_string())),
                ("gsi_sk".to_string(), AttributeValue::S("001".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    provider.process_gsi_updates().await.unwrap();

    let physical_gsi = naming::physical_gsi_table_name(&table_name, &index_name);
    call_sqlite(&provider.connection, {
        let physical_gsi = physical_gsi.clone();
        move |conn| {
            conn.execute(
                &format!(
                    "UPDATE \"{physical_gsi}\" SET __aux_tombstone = 1, __aux_item_version = 999"
                ),
                [],
            )
            .map_err(crate::error_handler::map_sqlite_error)
        }
    })
    .await
    .unwrap();

    let captured_tail = latest_system_stream_tail(&provider).await;
    start_test_gsi_backfill(&provider, &table_name, captured_tail).await;
    provider.process_gsi_backfills().await.unwrap();

    let items = provider
        .query_table(&gsi_query_request(table_name.as_ref(), "grp"))
        .await
        .unwrap()
        .0;
    assert!(
        items.is_empty(),
        "older scan observations must not overwrite newer tombstone evidence"
    );

    let (is_tombstone, item_version): (i64, i64) = call_sqlite(&provider.connection, {
        let physical_gsi = physical_gsi.clone();
        move |conn| {
            conn.query_row(
                &format!("SELECT __aux_tombstone, __aux_item_version FROM \"{physical_gsi}\""),
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(crate::error_handler::map_sqlite_error)
        }
    })
    .await
    .unwrap();
    assert_eq!(is_tombstone, 1);
    assert_eq!(item_version, 999);
}

#[tokio::test]
async fn gsi_backfill_tombstone_cleanup_removes_hidden_rows_after_completion_sqlite() {
    let table_name = TableName::new("GsiBackfillTombstoneCleanup");
    let index_name = IndexName::new("TestGSI");
    let provider =
        create_empty_gsi_test_table_with_settings(table_name.as_ref(), Default::default()).await;

    provider
        .put_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("user1".to_string())),
                ("sk".to_string(), AttributeValue::S("item1".to_string())),
                ("gsi_pk".to_string(), AttributeValue::S("grp".to_string())),
                ("gsi_sk".to_string(), AttributeValue::S("001".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    provider.process_gsi_updates().await.unwrap();
    let captured_tail = latest_system_stream_tail(&provider).await;
    provider
        .delete_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("user1".to_string())),
                ("sk".to_string(), AttributeValue::S("item1".to_string())),
            ])
            .into(),
            None,
            None,
            None,
        )
        .await
        .unwrap();
    start_test_gsi_backfill(&provider, &table_name, captured_tail).await;
    provider.process_gsi_backfills().await.unwrap();

    let deleted = provider
        .cleanup_gsi_backfill_tombstones(&table_name, &index_name)
        .await
        .unwrap();
    assert_eq!(deleted, 1);

    let tombstone_count: i64 = call_sqlite(&provider.connection, {
        let physical_gsi = naming::physical_gsi_table_name(&table_name, &index_name);
        move |conn| {
            conn.query_row(
                &format!("SELECT COUNT(*) FROM \"{physical_gsi}\" WHERE __aux_tombstone = 1"),
                [],
                |row| row.get(0),
            )
            .map_err(crate::error_handler::map_sqlite_error)
        }
    })
    .await
    .unwrap();
    assert_eq!(tombstone_count, 0);
}

#[tokio::test]
#[ignore = "GSI removal behavior regressed; tracked for follow-up"]
async fn gsi_updates_remove_field_sqlite() {
    // Create an empty table with a GSI
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let create_request = CreateTableRequest::new(
        TableName::new("GSIFieldRemovalTest"),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_sk".to_string(),
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
    )
    .with_global_secondary_indexes(Some(vec![CreateGlobalSecondaryIndex {
        index_name: IndexName::new("TestGSI"),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "gsi_pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "gsi_sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }]));

    provider.create_table(&create_request).await.unwrap();

    // D: initially has GSI fields
    let mut item_d = std::collections::HashMap::new();
    item_d.insert("pk".to_string(), AttributeValue::S("user2".to_string()));
    item_d.insert("sk".to_string(), AttributeValue::S("d".to_string()));
    item_d.insert("gsi_pk".to_string(), AttributeValue::S("grp2".to_string()));
    item_d.insert("gsi_sk".to_string(), AttributeValue::S("001".to_string()));

    provider
        .put_item(
            TableName::new("GSIFieldRemovalTest"),
            item_d,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    provider.process_gsi_updates().await.unwrap();

    let q = storage_types::QueryTableRequest {
        table_name: TableName::new("GSIFieldRemovalTest"),
        index_name: Some(IndexName::new("TestGSI")),
        key_condition_expression: "gsi_pk = :p".to_string(),
        expression_attribute_values: Some({
            let mut m = std::collections::HashMap::new();
            m.insert(":p".to_string(), AttributeValue::S("grp2".to_string()));
            m
        }),
        expression_attribute_names: None,
        limit: Some(100),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };

    let (items_before, _) = provider.query_table(&q).await.unwrap();
    assert_eq!(
        items_before.len(),
        1,
        "Item D should be indexed before removal"
    );

    // Replace D without GSI fields → should be removed from index
    let mut item_d_removed = std::collections::HashMap::new();
    item_d_removed.insert("pk".to_string(), AttributeValue::S("user2".to_string()));
    item_d_removed.insert("sk".to_string(), AttributeValue::S("d".to_string()));

    provider
        .put_item(
            TableName::new("GSIFieldRemovalTest"),
            item_d_removed,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    provider.process_gsi_updates().await.unwrap();

    let (items_after, _) = provider.query_table(&q).await.unwrap();
    assert_eq!(
        items_after.len(),
        0,
        "Item D should be removed from the index after update"
    );
}

#[tokio::test]
async fn query_gsi_consistent_read_rejected_sqlite() {
    let provider = create_gsi_test_table("consistent_read_gsi_query_sqlite").await;

    let mut values = HashMap::new();
    values.insert(":gsi_pk".to_string(), AttributeValue::S("grp".to_string()));
    let request = storage_types::QueryTableRequest {
        table_name: TableName::new("consistent_read_gsi_query_sqlite"),
        index_name: Some(IndexName::new("TestGSI")),
        key_condition_expression: "gsi_pk = :gsi_pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(values),
        limit: None,
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: true,
    };

    let err = provider.query_table(&request).await.unwrap_err();
    assert!(matches!(err.as_ref(), StorageEnum::Validation { .. }));
    assert_eq!(
        err.to_string(),
        "Consistent reads are not supported on global secondary indexes"
    );
}

#[tokio::test]
async fn scan_gsi_consistent_read_rejected_sqlite() {
    let provider = create_gsi_test_table("consistent_read_gsi_scan_sqlite").await;

    let request = ScanTableRequest {
        table_name: TableName::new("consistent_read_gsi_scan_sqlite"),
        index_name: Some(IndexName::new("TestGSI")),
        limit: Some(10),
        exclusive_start_key: None,
        consistent_read: true,
    };

    let err = provider.scan_table(&request).await.unwrap_err();
    assert!(matches!(err.as_ref(), StorageEnum::Validation { .. }));
    assert_eq!(
        err.to_string(),
        "Consistent reads are not supported on global secondary indexes"
    );
}

#[tokio::test]
async fn scan_base_allows_consistent_read_sqlite() {
    let provider = create_test_table().await;

    let scan_request = ScanTableRequest {
        table_name: TableName::new("PaginatedTable"),
        index_name: None,
        limit: Some(1),
        exclusive_start_key: None,
        consistent_read: true,
    };
    let (items, _) = provider.scan_table(&scan_request).await.unwrap();

    assert_eq!(items.len(), 1);
}

#[tokio::test]
async fn scan_with_pagination_first_page() {
    let provider = create_test_table().await;

    // First scan with limit 2
    let scan_request = ScanTableRequest {
        table_name: TableName::new("PaginatedTable"),
        index_name: None,
        limit: Some(2),
        exclusive_start_key: None,
        consistent_read: false,
    };
    let (items, last_evaluated_key) = provider.scan_table(&scan_request).await.unwrap();

    assert_eq!(items.len(), 2, "First page should have 2 items");
    assert!(
        last_evaluated_key.is_some(),
        "Should have LastEvaluatedKey for pagination"
    );
}

#[tokio::test]
async fn scan_with_pagination_second_page() {
    let provider = create_test_table().await;

    // First scan to get LastEvaluatedKey
    let scan_request = ScanTableRequest {
        table_name: TableName::new("PaginatedTable"),
        index_name: None,
        limit: Some(2),
        exclusive_start_key: None,
        consistent_read: false,
    };
    let (first_items, last_evaluated_key) = provider.scan_table(&scan_request).await.unwrap();

    assert_eq!(first_items.len(), 2);
    assert!(last_evaluated_key.is_some());

    // Second scan using LastEvaluatedKey
    let scan_request = ScanTableRequest {
        table_name: TableName::new("PaginatedTable"),
        index_name: None,
        limit: Some(2),
        exclusive_start_key: last_evaluated_key,
        consistent_read: false,
    };
    let (second_items, _) = provider.scan_table(&scan_request).await.unwrap();

    // This should fail with the current bug - we expect 2 items but get 0
    assert_eq!(second_items.len(), 2, "Second page should have 2 items");
}

#[tokio::test]
async fn scan_pagination_complete_flow() {
    let provider = create_test_table().await;

    let mut all_items = Vec::new();
    let mut exclusive_start_key = None;

    // Scan in pages of 2 until we get all items
    loop {
        let scan_request = ScanTableRequest {
            table_name: TableName::new("PaginatedTable"),
            index_name: None,
            limit: Some(2),
            exclusive_start_key: exclusive_start_key.clone(),
            consistent_read: false,
        };
        let (items, last_key) = provider.scan_table(&scan_request).await.unwrap();

        if items.is_empty() {
            break;
        }

        all_items.extend(items);

        if last_key.is_none() {
            break;
        }

        exclusive_start_key = last_key;
    }

    assert_eq!(all_items.len(), 4, "Should collect all 4 items");
}

#[tokio::test]
async fn sqlite_query_and_scan_only_return_last_evaluated_key_when_more_items_remain() {
    let table_name = TableName::new("LimitBoundarySqlite");
    let provider = create_limit_boundary_table(table_name.as_ref()).await;

    let query_values =
        HashMap::from([(":pk".to_string(), AttributeValue::S("user#1".to_string()))]);
    let query_request = storage_types::QueryTableRequest {
        table_name: table_name.clone(),
        index_name: None,
        key_condition_expression: "pk = :pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(query_values.clone()),
        limit: None,
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };
    let (items, last_evaluated_key) = provider.query_table(&query_request).await.unwrap();
    assert_eq!(items.len(), 15);
    assert!(last_evaluated_key.is_none());

    let query_request = storage_types::QueryTableRequest {
        limit: Some(10),
        expression_attribute_values: Some(query_values.clone()),
        ..query_request.clone()
    };
    let (items, last_evaluated_key) = provider.query_table(&query_request).await.unwrap();
    assert_eq!(items.len(), 10);
    assert!(last_evaluated_key.is_some());

    let query_request = storage_types::QueryTableRequest {
        limit: Some(15),
        expression_attribute_values: Some(query_values),
        ..query_request
    };
    let (items, last_evaluated_key) = provider.query_table(&query_request).await.unwrap();
    assert_eq!(items.len(), 15);
    assert!(last_evaluated_key.is_none());

    let scan_request = ScanTableRequest {
        table_name,
        index_name: None,
        limit: Some(10),
        exclusive_start_key: None,
        consistent_read: false,
    };
    let (items, last_evaluated_key) = provider.scan_table(&scan_request).await.unwrap();
    assert_eq!(items.len(), 10);
    assert!(last_evaluated_key.is_some());

    let scan_request = ScanTableRequest {
        limit: Some(15),
        ..scan_request
    };
    let (items, last_evaluated_key) = provider.scan_table(&scan_request).await.unwrap();
    assert_eq!(items.len(), 15);
    assert!(last_evaluated_key.is_none());
}

async fn create_stream_test_table() -> (SQLiteStorageProvider, TableName) {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let create_request = CreateTableRequest::new(
        TableName::new("StreamTestTable"),
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
    )
    .with_stream_specification(Some(StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(StreamViewType::NewAndOldImages),
    }));

    let () = provider.create_table(&create_request).await.unwrap();

    (provider, TableName::new("StreamTestTable"))
}

async fn create_stream_test_table_named(provider: &SQLiteStorageProvider, table_name: &TableName) {
    let create_request = CreateTableRequest::new(
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
    )
    .with_stream_specification(Some(StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(StreamViewType::NewAndOldImages),
    }));

    provider.create_table(&create_request).await.unwrap();
}

async fn create_multi_region_control_table(provider: &SQLiteStorageProvider) {
    let create_request = CreateTableRequest::new(
        TableName::new("sys_storage_replication"),
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

    provider.create_table(&create_request).await.unwrap();
}

async fn create_no_stream_test_table() -> (SQLiteStorageProvider, TableName) {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let create_request = CreateTableRequest::new(
        TableName::new("NoStreamTestTable"),
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

    provider.create_table(&create_request).await.unwrap();

    (provider, TableName::new("NoStreamTestTable"))
}

async fn create_gsi_stream_test_table() -> (SQLiteStorageProvider, TableName) {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let create_request = CreateTableRequest::new(
        TableName::new("GsiStreamTestTable"),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_sk".to_string(),
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
    )
    .with_global_secondary_indexes(Some(vec![CreateGlobalSecondaryIndex {
        index_name: IndexName::new("StreamTestGSI"),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "gsi_pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "gsi_sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }]));

    provider.create_table(&create_request).await.unwrap();

    (provider, TableName::new("GsiStreamTestTable"))
}

#[tokio::test]
async fn put_item_without_stream_gsi_ttl_skips_stream_entries() {
    let (provider, table_name) = create_no_stream_test_table().await;

    let mut item = HashMap::new();
    item.insert(
        "pk".to_string(),
        AttributeValue::S("partition1".to_string()),
    );
    item.insert("sk".to_string(), AttributeValue::S("sort1".to_string()));
    item.insert(
        "data".to_string(),
        AttributeValue::S("test_data".to_string()),
    );

    provider
        .put_item(table_name.clone(), item, None, None, None, None)
        .await
        .unwrap();

    let page = StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
        .await
        .unwrap();
    assert!(
        page.items.is_empty(),
        "no stream entries expected when streams, GSI, and TTL are disabled"
    );
}

#[tokio::test]
async fn put_item_with_gsi_creates_stream_entries_without_stream_spec() {
    let (provider, table_name) = create_gsi_stream_test_table().await;

    let mut item = HashMap::new();
    item.insert(
        "pk".to_string(),
        AttributeValue::S("partition1".to_string()),
    );
    item.insert("sk".to_string(), AttributeValue::S("sort1".to_string()));
    item.insert(
        "gsi_pk".to_string(),
        AttributeValue::S("gsi_partition1".to_string()),
    );
    item.insert(
        "gsi_sk".to_string(),
        AttributeValue::S("gsi_sort1".to_string()),
    );

    provider
        .put_item(table_name.clone(), item, None, None, None, None)
        .await
        .unwrap();

    let table_info = provider.get_table_info(&table_name).await.unwrap();
    let item_key = ItemKey::table_key(
        table_info.table_name.clone(),
        AttributeValue::S("partition1".to_string()),
        Some(AttributeValue::S("sort1".to_string())),
    );

    let page = StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1, "expected stream entry for GSI table");
    assert_eq!(page.items[0].data_type, StreamDataType::StreamPointer);

    let stored_pointer: StoredStreamPointer =
        storage_types::storage_serde::from_bytes(&page.items[0].data).unwrap();
    assert_eq!(
        stored_pointer.target_item_stream_version(),
        storage_types::ItemStreamVersion::new(1)
    );
    let pointer_data = stored_pointer.into_stream_pointer(page.items[0].id);
    assert_eq!(
        pointer_data.stream_name,
        StreamName::table_item_stream(&table_info.table_name, &item_key).expect("item stream")
    );
}

#[tokio::test]
async fn put_item_with_ttl_skips_stream_entries_without_stream_spec() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();
    let table_name = TableName::new("TtlStreamTestTable");
    create_ttl_enabled_table(&provider, &table_name).await;

    let mut item = HashMap::new();
    item.insert(
        "pk".to_string(),
        AttributeValue::S("partition1".to_string()),
    );
    item.insert("sk".to_string(), AttributeValue::S("sort1".to_string()));
    item.insert(
        "ttl".to_string(),
        AttributeValue::N("1700000000".to_string()),
    );

    provider
        .put_item(table_name.clone(), item, None, None, None, None)
        .await
        .unwrap();

    let page = StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
        .await
        .unwrap();
    assert!(
        page.items.is_empty(),
        "expected no stream entries when only TTL is enabled"
    );
}

#[tokio::test]
async fn repeated_puts_to_same_item_store_increasing_pointer_target_versions() {
    let (provider, table_name) = create_stream_test_table().await;

    let mut first = HashMap::new();
    first.insert(
        "pk".to_string(),
        AttributeValue::S("partition1".to_string()),
    );
    first.insert("sk".to_string(), AttributeValue::S("sort1".to_string()));
    first.insert("data".to_string(), AttributeValue::S("first".to_string()));
    provider
        .put_item(table_name.clone(), first, None, None, None, None)
        .await
        .unwrap();

    let mut second = HashMap::new();
    second.insert(
        "pk".to_string(),
        AttributeValue::S("partition1".to_string()),
    );
    second.insert("sk".to_string(), AttributeValue::S("sort1".to_string()));
    second.insert("data".to_string(), AttributeValue::S("second".to_string()));
    provider
        .put_item(table_name, second, None, None, None, None)
        .await
        .unwrap();

    let page = StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
        .await
        .unwrap();
    let versions = page
        .items
        .iter()
        .map(|item| {
            let pointer: StoredStreamPointer =
                storage_types::storage_serde::from_bytes(&item.data).unwrap();
            pointer.target_item_stream_version()
        })
        .collect::<Vec<_>>();

    assert_eq!(
        versions,
        vec![
            storage_types::ItemStreamVersion::new(1),
            storage_types::ItemStreamVersion::new(2),
        ]
    );
}

#[tokio::test]
async fn failed_conditional_put_does_not_allocate_item_stream_version() {
    let (provider, table_name) = create_stream_test_table().await;

    let mut first = HashMap::new();
    first.insert(
        "pk".to_string(),
        AttributeValue::S("partition1".to_string()),
    );
    first.insert("sk".to_string(), AttributeValue::S("sort1".to_string()));
    first.insert("data".to_string(), AttributeValue::S("first".to_string()));
    provider
        .put_item(table_name.clone(), first, None, None, None, None)
        .await
        .unwrap();

    let mut failed = HashMap::new();
    failed.insert(
        "pk".to_string(),
        AttributeValue::S("partition1".to_string()),
    );
    failed.insert("sk".to_string(), AttributeValue::S("sort1".to_string()));
    failed.insert("data".to_string(), AttributeValue::S("failed".to_string()));

    let err = provider
        .put_item(
            table_name.clone(),
            failed,
            Some("#data = :expected".to_string()),
            Some(HashMap::from([("#data".to_string(), "data".to_string())])),
            Some(HashMap::from([(
                ":expected".to_string(),
                AttributeValue::S("not-first".to_string()),
            )])),
            None,
        )
        .await
        .expect_err("conditional put should fail");
    assert!(matches!(
        err,
        StorageError::Base(StorageEnum::ConditionalCheckFailed)
    ));

    let mut second = HashMap::new();
    second.insert(
        "pk".to_string(),
        AttributeValue::S("partition1".to_string()),
    );
    second.insert("sk".to_string(), AttributeValue::S("sort1".to_string()));
    second.insert("data".to_string(), AttributeValue::S("second".to_string()));
    provider
        .put_item(table_name, second, None, None, None, None)
        .await
        .unwrap();

    let page = StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
        .await
        .unwrap();
    let versions = page
        .items
        .iter()
        .map(|item| {
            let pointer: StoredStreamPointer =
                storage_types::storage_serde::from_bytes(&item.data).unwrap();
            pointer.target_item_stream_version()
        })
        .collect::<Vec<_>>();

    assert_eq!(
        versions,
        vec![
            storage_types::ItemStreamVersion::new(1),
            storage_types::ItemStreamVersion::new(2),
        ]
    );
}

#[tokio::test]
async fn delete_writes_next_pointer_target_version_and_missing_delete_is_stream_noop() {
    let (provider, table_name) = create_stream_test_table().await;

    let mut item = HashMap::new();
    item.insert(
        "pk".to_string(),
        AttributeValue::S("partition1".to_string()),
    );
    item.insert("sk".to_string(), AttributeValue::S("sort1".to_string()));
    item.insert("data".to_string(), AttributeValue::S("first".to_string()));
    provider
        .put_item(table_name.clone(), item, None, None, None, None)
        .await
        .unwrap();

    let key = storage_types::KeyAttributes::from([
        (
            "pk".to_string(),
            AttributeValue::S("partition1".to_string()),
        ),
        ("sk".to_string(), AttributeValue::S("sort1".to_string())),
    ]);
    provider
        .delete_item(table_name.clone(), key.clone(), None, None, None)
        .await
        .unwrap();
    provider
        .delete_item(table_name, key, None, None, None)
        .await
        .unwrap();

    let page = StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
        .await
        .unwrap();
    let versions = page
        .items
        .iter()
        .map(|item| {
            let pointer: StoredStreamPointer =
                storage_types::storage_serde::from_bytes(&item.data).unwrap();
            pointer.target_item_stream_version()
        })
        .collect::<Vec<_>>();

    assert_eq!(
        versions,
        vec![
            storage_types::ItemStreamVersion::new(1),
            storage_types::ItemStreamVersion::new(2),
        ]
    );
}

#[tokio::test]
async fn different_items_can_share_target_version_one_without_pointer_conflict() {
    let (provider, table_name) = create_stream_test_table().await;

    for pk in ["partition1", "partition2"] {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S(pk.to_string()));
        item.insert("sk".to_string(), AttributeValue::S("sort1".to_string()));
        item.insert("data".to_string(), AttributeValue::S("first".to_string()));
        provider
            .put_item(table_name.clone(), item, None, None, None, None)
            .await
            .unwrap();
    }

    let system_page =
        StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
            .await
            .unwrap();
    let mut pointer_ids = system_page
        .items
        .iter()
        .map(|item| item.id)
        .collect::<Vec<_>>();
    pointer_ids.sort();
    pointer_ids.dedup();

    let versions = system_page
        .items
        .iter()
        .map(|item| {
            let pointer: StoredStreamPointer =
                storage_types::storage_serde::from_bytes(&item.data).unwrap();
            pointer.target_item_stream_version()
        })
        .collect::<Vec<_>>();

    assert_eq!(pointer_ids.len(), 2);
    assert_eq!(
        versions,
        vec![
            storage_types::ItemStreamVersion::new(1),
            storage_types::ItemStreamVersion::new(1),
        ]
    );
}

#[tokio::test]
async fn pointer_stream_dereferences_item_version_when_pointer_id_differs() {
    let (provider, table_name) = create_stream_test_table().await;

    let mut item = HashMap::new();
    item.insert(
        "pk".to_string(),
        AttributeValue::S("partition1".to_string()),
    );
    item.insert("sk".to_string(), AttributeValue::S("sort1".to_string()));
    item.insert("data".to_string(), AttributeValue::S("first".to_string()));
    provider
        .put_item(table_name.clone(), item, None, None, None, None)
        .await
        .unwrap();

    let table_info = provider.get_table_info(&table_name).await.unwrap();
    let (records, _) = StreamProvider::get_stream_records_from_pointer_stream(
        &provider,
        StreamName::system_table_stream(),
        &table_info.key_schema,
        None,
        Some(10),
    )
    .await
    .unwrap();

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].sequence_number, "1");
    assert_eq!(
        records[0].keys.get("pk"),
        Some(&AttributeValue::S("partition1".to_string()))
    );
}

#[tokio::test]
async fn put_item_encode_updates_stream_and_ttl_side_effects_sqlite() {
    let (provider, table_name) = create_stream_test_table().await;

    provider
        .update_time_to_live(UpdateTimeToLiveRequest {
            table_name: table_name.clone(),
            time_to_live_specification: TimeToLiveSpecification {
                attribute_name: "ttl".to_string(),
                enabled: true,
            },
        })
        .await
        .unwrap();

    let baseline =
        StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
            .await
            .unwrap()
            .items
            .len();

    let future_at = (Utc::now().timestamp() + 3_600).to_string();
    let mut item = HashMap::new();
    item.insert(
        "pk".to_string(),
        AttributeValue::S("encode_partition".to_string()),
    );
    item.insert(
        "sk".to_string(),
        AttributeValue::S("encode_sort".to_string()),
    );
    item.insert("ttl".to_string(), AttributeValue::N(future_at.clone()));
    item.insert(
        "data".to_string(),
        AttributeValue::S("encode_payload".to_string()),
    );
    let wire_item = WireItem::from_attribute_map(&item).expect("wire item");

    provider
        .put_item_encode(table_name.clone(), wire_item, None, None, None, None)
        .await
        .unwrap();

    let after =
        StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
            .await
            .unwrap();
    assert!(after.items.len() > baseline);
    assert_eq!(
        after.items.last().expect("stream entry").data_type,
        StreamDataType::StreamPointer
    );

    let table_info = provider.get_table_info(&table_name).await.unwrap();
    let token = storage_common::ttl::ttl_index_key_token_for_item(&table_info, &item).unwrap();
    let ttl_value = i64::try_from(storage_common::ttl::normalize_ttl_seconds(
        future_at.parse::<i64>().unwrap(),
    ))
    .expect("normalized ttl should fit i64");
    let ttl_table = naming::physical_ttl_index_table_name(&table_name);
    let has_row: bool = provider
        .connection
        .call_unwrap({
            let ttl_table = ttl_table.clone();
            let token = token.clone();
            move |conn| -> Result<bool, rusqlite::Error> {
                let sql = format!(
                    "SELECT 1 FROM \"{ttl_table}\" WHERE ttl_value = ?1 AND key_token = ?2"
                );
                let mut stmt = conn.prepare(&sql)?;
                let mut rows = stmt.query(rusqlite::params![ttl_value, token])?;
                Ok(rows.next()?.is_some())
            }
        })
        .await
        .unwrap();
    assert!(has_row, "ttl index row should exist for encode write");
}

#[tokio::test]
async fn put_item_creates_stream_entries() {
    let (provider, table_name) = create_stream_test_table().await;

    // Put an item
    let mut item = HashMap::new();
    item.insert(
        "pk".to_string(),
        AttributeValue::S("partition1".to_string()),
    );
    item.insert("sk".to_string(), AttributeValue::S("sort1".to_string()));
    item.insert(
        "data".to_string(),
        AttributeValue::S("test_data".to_string()),
    );

    provider
        .put_item(
            TableName::new("StreamTestTable"),
            item.clone(),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let table_info = provider.get_table_info(&table_name).await.unwrap();
    let item_key = ItemKey::table_key(
        table_info.table_name.clone(),
        AttributeValue::S("partition1".to_string()),
        Some(AttributeValue::S("sort1".to_string())),
    );

    // Check that stream entries were created
    let expected_streams: [StreamName; 3] = [
        StreamName::system_table_stream(),
        StreamName::table_stream(&table_info.table_name),
        StreamName::table_item_stream(&table_info.table_name, &item_key)
            .expect("table item stream"),
    ];

    for stream_name in expected_streams {
        // Verify stream has data
        let page = StreamProvider::read_forward(&provider, stream_name.clone(), None, 10)
            .await
            .unwrap();
        assert!(
            !page.items.is_empty(),
            "Stream {} should have items",
            Into::<String>::into(&stream_name)
        );

        let stream_item_data = &page.items[0].data;

        if page.items[0].data_type == StreamDataType::DynamoDbJson {
            // Item stream should contain full data
            assert_eq!(
                page.items[0].id,
                StreamItemId::from(storage_types::ItemStreamVersion::new(1))
            );
            let deserialized: HashMap<String, AttributeValue> =
                storage_types::storage_serde::from_bytes(stream_item_data).unwrap();
            assert_eq!(
                deserialized.get("pk").unwrap(),
                &AttributeValue::S("partition1".to_string())
            );
        } else {
            // System and table streams should contain pointer data
            let stored_pointer: StoredStreamPointer =
                storage_types::storage_serde::from_bytes(stream_item_data).unwrap();
            assert_eq!(
                stored_pointer.target_item_stream_version(),
                storage_types::ItemStreamVersion::new(1)
            );
            assert_ne!(
                page.items[0].id,
                StreamItemId::from(stored_pointer.target_item_stream_version())
            );
            let pointer_data = stored_pointer.into_stream_pointer(page.items[0].id);
            assert_eq!(
                pointer_data.stream_name,
                StreamName::table_item_stream(&table_info.table_name, &item_key)
                    .expect("table item stream")
            );
        }
    }
}

#[tokio::test]
async fn delete_item_creates_stream_entries() {
    let (provider, table_name) = create_stream_test_table().await;

    // Put an item first
    let mut item = HashMap::new();
    item.insert(
        "pk".to_string(),
        AttributeValue::S("partition2".to_string()),
    );
    item.insert("sk".to_string(), AttributeValue::S("sort2".to_string()));
    item.insert(
        "data".to_string(),
        AttributeValue::S("delete_test".to_string()),
    );

    provider
        .put_item(
            TableName::new("StreamTestTable"),
            item.clone(),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    // Delete the item
    let mut key = HashMap::new();
    key.insert(
        "pk".to_string(),
        AttributeValue::S("partition2".to_string()),
    );
    key.insert("sk".to_string(), AttributeValue::S("sort2".to_string()));

    let deleted_item = provider
        .delete_item(
            TableName::new("StreamTestTable"),
            key.clone().into(),
            None,
            None,
            None,
        )
        .await
        .unwrap();

    assert!(deleted_item.is_some(), "Item should have been deleted");

    let table_info = provider.get_table_info(&table_name).await.unwrap();
    let item_key =
        ItemKey::from_key_schema(table_info.table_name.clone(), &table_info.key_schema, &key)
            .unwrap();

    // Check that delete stream entries were created
    let expected_streams: [StreamName; 3] = [
        StreamName::system_table_stream(),
        StreamName::table_stream(&table_info.table_name),
        StreamName::table_item_stream(&table_info.table_name, &item_key)
            .expect("table item stream"),
    ];

    for stream_name in expected_streams {
        // Verify stream has data
        let page = StreamProvider::read_forward(&provider, stream_name.clone(), None, 10)
            .await
            .unwrap();

        // Should have 2 entries: one for put, one for delete
        assert_eq!(
            page.items.len(),
            2,
            "Stream {} should have 2 items (put + delete)",
            Into::<String>::into(&stream_name)
        );

        if page.items[0].data_type == StreamDataType::StreamPointer {
            // System and table streams should contain pointer data for delete
            let delete_stream_item_data = &page.items[1].data;
            let stored_pointer: StoredStreamPointer =
                storage_types::storage_serde::from_bytes(delete_stream_item_data).unwrap();
            let pointer_data = stored_pointer.into_stream_pointer(page.items[1].id);

            assert_eq!(
                pointer_data.stream_name,
                StreamName::table_item_stream(&table_info.table_name, &item_key)
                    .expect("table item stream")
            );
        } else {
            // Item stream should contain full data with delete flag
            let put_stream_item_data = &page.items[0].data;
            let deserialized_put: HashMap<String, AttributeValue> =
                storage_types::storage_serde::from_bytes(put_stream_item_data).unwrap();
            assert_eq!(
                deserialized_put.get("pk").unwrap(),
                &AttributeValue::S("partition2".to_string())
            );
            let delete_stream_item_data_type = &page.items[1].data_type;
            assert!(matches!(
                delete_stream_item_data_type,
                stream_provider::StreamDataType::DeleteMarker
            ));
        }
    }
}

#[tokio::test]
async fn multiple_operations_create_ordered_stream_entries() {
    let (provider, table_name) = create_stream_test_table().await;

    // Perform multiple operations
    let operations = [
        ("put", "operation1"),
        ("put", "operation2"),
        ("delete", "operation1"),
        ("put", "operation3"),
    ];

    for (i, (op, key_suffix)) in operations.iter().enumerate() {
        let mut item = HashMap::new();
        item.insert(
            "pk".to_string(),
            AttributeValue::S("multi_test".to_string()),
        );
        item.insert(
            "sk".to_string(),
            AttributeValue::S((*key_suffix).to_string()),
        );
        item.insert("data".to_string(), AttributeValue::S(format!("data_{i}")));

        match *op {
            "put" => {
                provider
                    .put_item(
                        TableName::new("StreamTestTable"),
                        item,
                        None,
                        None,
                        None,
                        None,
                    )
                    .await
                    .unwrap();
            }
            "delete" => {
                let mut key = HashMap::new();
                key.insert(
                    "pk".to_string(),
                    AttributeValue::S("multi_test".to_string()),
                );
                key.insert(
                    "sk".to_string(),
                    AttributeValue::S((*key_suffix).to_string()),
                );
                provider
                    .delete_item(
                        TableName::new("StreamTestTable"),
                        key.into(),
                        None,
                        None,
                        None,
                    )
                    .await
                    .unwrap();
            }
            _ => panic!("Unknown operation"),
        }
    }

    let table_info = provider.get_table_info(&table_name).await.unwrap();

    // Check the table-level stream has all operations (should contain pointers)
    let table_stream_name: StreamName = StreamName::table_stream(&table_info.table_name);
    let page = StreamProvider::read_forward(&provider, table_stream_name.clone(), None, 10)
        .await
        .unwrap();

    assert_eq!(page.items.len(), 4, "Table stream should have 4 operations");

    // Verify operations are in chronological order (UUIDv7 is time-ordered)
    for i in 1..page.items.len() {
        assert!(
            page.items[i].id > page.items[i - 1].id,
            "Stream items should be in chronological order"
        );
    }

    // Verify all entries are pointer entries in the table stream
    for stream_item in page.items {
        assert!(
            stream_item.data_type == stream_provider::StreamDataType::StreamPointer,
            "Stream item  should be a pointer entry",
        );
    }
}

#[tokio::test]
async fn can_follow_pointers_from_system_stream_to_item_data() {
    let (provider, table_name) = create_stream_test_table().await;

    // Put an item
    let mut item = HashMap::new();
    item.insert(
        "pk".to_string(),
        AttributeValue::S("test_pointer".to_string()),
    );
    item.insert("sk".to_string(), AttributeValue::S("follow".to_string()));
    item.insert(
        "data".to_string(),
        AttributeValue::S("pointer_test_data".to_string()),
    );

    provider
        .put_item(
            TableName::new("StreamTestTable"),
            item.clone(),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let table_info = provider.get_table_info(&table_name).await.unwrap();
    let item_key =
        ItemKey::from_key_schema(table_info.table_name.clone(), &table_info.key_schema, &item)
            .unwrap();

    // Read from system stream to get pointer
    let system_page =
        StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
            .await
            .unwrap();

    assert!(
        !system_page.items.is_empty(),
        "System stream should have items"
    );

    assert!(matches!(
        system_page.items[0].data_type,
        stream_provider::StreamDataType::StreamPointer
    ));

    let stored_pointer: StoredStreamPointer =
        storage_types::storage_serde::from_bytes(&system_page.items[0].data).unwrap();
    let pointer_data = stored_pointer.into_stream_pointer(system_page.items[0].id);

    assert_eq!(
        pointer_data.stream_name,
        StreamName::table_item_stream(&table_info.table_name, &item_key)
            .expect("table item stream")
    );

    // Follow the pointer to get the actual item data
    let item_page = StreamProvider::read_forward(&provider, pointer_data.stream_name, None, 10)
        .await
        .unwrap();

    assert!(!item_page.items.is_empty(), "Item stream should have data");

    // Verify we can get the original item data
    let item_data: HashMap<String, AttributeValue> =
        storage_types::storage_serde::from_bytes(&item_page.items[0].data).unwrap();

    assert!(matches!(
        item_page.items[0].data_type,
        stream_provider::StreamDataType::DynamoDbJson
    ));

    assert_eq!(
        item_data.get("pk").unwrap(),
        &AttributeValue::S("test_pointer".to_string())
    );
    assert_eq!(
        item_data.get("data").unwrap(),
        &AttributeValue::S("pointer_test_data".to_string())
    );
}

#[tokio::test]
async fn apply_replication_mutation_put_preserves_replication_metadata() {
    let provider = create_stream_replication_table().await;
    let table_name = TableName::new("ReplicationStreamTable");
    let new_image = HashMap::from([
        ("pk".to_string(), AttributeValue::S("user1".to_string())),
        ("sk".to_string(), AttributeValue::S("item1".to_string())),
        ("data".to_string(), AttributeValue::S("payload".to_string())),
    ]);
    let metadata = sqlite_sample_replication_metadata("eu-central-1", 5);

    provider
        .apply_replication_mutation(ReplicationMutation {
            table_name: table_name.clone(),
            key: HashMap::from([
                ("pk".to_string(), AttributeValue::S("user1".to_string())),
                ("sk".to_string(), AttributeValue::S("item1".to_string())),
            ])
            .into(),
            new_image: Some(new_image.clone()),
            old_image: None,
            metadata: metadata.clone(),
        })
        .await
        .unwrap();

    let system_page =
        StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
            .await
            .unwrap();
    let stored_pointer: StoredStreamPointer =
        storage_types::storage_serde::from_bytes(&system_page.items[0].data).unwrap();
    assert_eq!(stored_pointer.replication_metadata(), Some(&metadata));

    let stored = provider
        .get_item(
            table_name,
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("user1".to_string())),
                ("sk".to_string(), AttributeValue::S("item1".to_string())),
            ])
            .into(),
            true,
        )
        .await
        .unwrap()
        .unwrap()
        .to_attribute_map()
        .unwrap();
    assert_eq!(stored, new_image);
}

#[tokio::test]
async fn apply_replication_mutation_delete_writes_tombstone_for_missing_item() {
    let provider = create_stream_replication_table().await;
    let table_name = TableName::new("ReplicationStreamTable");
    let key = HashMap::from([
        ("pk".to_string(), AttributeValue::S("user9".to_string())),
        ("sk".to_string(), AttributeValue::S("missing".to_string())),
    ]);
    let metadata = sqlite_sample_replication_metadata("us-west-1", 9);

    provider
        .apply_replication_mutation(ReplicationMutation {
            table_name: table_name.clone(),
            key: key.clone().into(),
            new_image: None,
            old_image: None,
            metadata: metadata.clone(),
        })
        .await
        .unwrap();

    let system_page =
        StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
            .await
            .unwrap();
    let stored_pointer: StoredStreamPointer =
        storage_types::storage_serde::from_bytes(&system_page.items[0].data).unwrap();
    assert_eq!(stored_pointer.replication_metadata(), Some(&metadata));

    let item_key = ItemKey::from_key_schema(
        table_name.clone(),
        &[
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        &key,
    )
    .unwrap();
    let item_stream = StreamName::table_item_stream(&table_name, &item_key).unwrap();
    let item_page = StreamProvider::read_forward(&provider, item_stream, None, 10)
        .await
        .unwrap();
    assert_eq!(item_page.items.len(), 1);
    assert_eq!(item_page.items[0].data_type, StreamDataType::DeleteMarker);
}

#[tokio::test]
async fn batch_write_item_put_operations() {
    use storage_types::{BatchWriteItemRequest, PutRequest, WriteRequest};

    let provider = create_test_table().await;

    let mut request_items = HashMap::new();

    let write_requests = vec![
        WriteRequest {
            put_request: Some(PutRequest {
                item: {
                    let mut item = HashMap::new();
                    item.insert("id".to_string(), AttributeValue::S("batch1".to_string()));
                    item.insert(
                        "data".to_string(),
                        AttributeValue::S("batch_data1".to_string()),
                    );
                    item
                },
            }),
            delete_request: None,
        },
        WriteRequest {
            put_request: Some(PutRequest {
                item: {
                    let mut item = HashMap::new();
                    item.insert("id".to_string(), AttributeValue::S("batch2".to_string()));
                    item.insert(
                        "data".to_string(),
                        AttributeValue::S("batch_data2".to_string()),
                    );
                    item
                },
            }),
            delete_request: None,
        },
    ];

    request_items.insert(TableName::new("PaginatedTable"), write_requests);

    let batch_request = BatchWriteItemRequest {
        request_items,
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    };

    let response = provider
        .batch_write_item(batch_request, false)
        .await
        .unwrap();

    assert!(response.unprocessed_items.is_none());

    // Verify items were written
    let key1 = {
        let mut key = HashMap::new();
        key.insert("id".to_string(), AttributeValue::S("batch1".to_string()));
        key
    };
    let item1 = provider
        .get_item_map(TableName::new("PaginatedTable"), key1, true)
        .await
        .unwrap();
    assert!(item1.is_some());
    assert_eq!(
        item1.unwrap().get("data").unwrap(),
        &AttributeValue::S("batch_data1".to_string())
    );

    let key2 = {
        let mut key = HashMap::new();
        key.insert("id".to_string(), AttributeValue::S("batch2".to_string()));
        key
    };
    let item2 = provider
        .get_item_map(TableName::new("PaginatedTable"), key2, true)
        .await
        .unwrap();
    assert!(item2.is_some());
    assert_eq!(
        item2.unwrap().get("data").unwrap(),
        &AttributeValue::S("batch_data2".to_string())
    );
}

#[tokio::test]
async fn batch_write_item_delete_operations() {
    use storage_types::{BatchWriteItemRequest, DeleteRequest, WriteRequest};

    let provider = create_test_table().await;

    // Put some items first
    let key1 = {
        let mut key = HashMap::new();
        key.insert("id".to_string(), AttributeValue::S("delete1".to_string()));
        key
    };
    let key2 = {
        let mut key = HashMap::new();
        key.insert("id".to_string(), AttributeValue::S("delete2".to_string()));
        key
    };

    let item1 = {
        let mut item = HashMap::new();
        item.insert("id".to_string(), AttributeValue::S("delete1".to_string()));
        item.insert(
            "data".to_string(),
            AttributeValue::S("to_be_deleted1".to_string()),
        );
        item
    };
    let item2 = {
        let mut item = HashMap::new();
        item.insert("id".to_string(), AttributeValue::S("delete2".to_string()));
        item.insert(
            "data".to_string(),
            AttributeValue::S("to_be_deleted2".to_string()),
        );
        item
    };

    provider
        .put_item(
            TableName::new("PaginatedTable"),
            item1,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    provider
        .put_item(
            TableName::new("PaginatedTable"),
            item2,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    // Now delete them with batch operation
    let mut request_items = HashMap::new();

    let write_requests = vec![
        WriteRequest {
            put_request: None,
            delete_request: Some(DeleteRequest {
                key: key1.clone().into(),
            }),
        },
        WriteRequest {
            put_request: None,
            delete_request: Some(DeleteRequest {
                key: key2.clone().into(),
            }),
        },
    ];

    request_items.insert(TableName::new("PaginatedTable"), write_requests);

    let batch_request = BatchWriteItemRequest {
        request_items,
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    };

    let response = provider
        .batch_write_item(batch_request, false)
        .await
        .unwrap();

    assert!(response.unprocessed_items.is_none());

    // Verify items were deleted
    let retrieved_item1 = provider
        .get_item_map(TableName::new("PaginatedTable"), key1, true)
        .await
        .unwrap();
    assert!(retrieved_item1.is_none());

    let retrieved_item2 = provider
        .get_item_map(TableName::new("PaginatedTable"), key2, true)
        .await
        .unwrap();
    assert!(retrieved_item2.is_none());
}

#[tokio::test]
async fn batch_write_item_mixed_operations() {
    use storage_types::{BatchWriteItemRequest, DeleteRequest, PutRequest, WriteRequest};

    let provider = create_test_table().await;

    // Put an item first to delete later
    let delete_item = {
        let mut item = HashMap::new();
        item.insert("id".to_string(), AttributeValue::S("to_delete".to_string()));
        item.insert(
            "data".to_string(),
            AttributeValue::S("delete_me".to_string()),
        );
        item
    };
    provider
        .put_item(
            TableName::new("PaginatedTable"),
            delete_item,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    // Mixed operations: put and delete
    let mut request_items = HashMap::new();

    let write_requests = vec![
        WriteRequest {
            put_request: Some(PutRequest {
                item: {
                    let mut item = HashMap::new();
                    item.insert("id".to_string(), AttributeValue::S("new_item".to_string()));
                    item.insert(
                        "data".to_string(),
                        AttributeValue::S("new_data".to_string()),
                    );
                    item
                },
            }),
            delete_request: None,
        },
        WriteRequest {
            put_request: None,
            delete_request: Some(DeleteRequest {
                key: {
                    let mut key = HashMap::new();
                    key.insert("id".to_string(), AttributeValue::S("to_delete".to_string()));
                    key.into()
                },
            }),
        },
    ];

    request_items.insert(TableName::new("PaginatedTable"), write_requests);

    let batch_request = BatchWriteItemRequest {
        request_items,
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    };

    let response = provider
        .batch_write_item(batch_request, false)
        .await
        .unwrap();

    assert!(response.unprocessed_items.is_none());

    // Verify put operation succeeded
    let new_key = {
        let mut key = HashMap::new();
        key.insert("id".to_string(), AttributeValue::S("new_item".to_string()));
        key
    };
    let new_item = provider
        .get_item_map(TableName::new("PaginatedTable"), new_key, true)
        .await
        .unwrap();
    assert!(new_item.is_some());
    assert_eq!(
        new_item.unwrap().get("data").unwrap(),
        &AttributeValue::S("new_data".to_string())
    );

    // Verify delete operation succeeded
    let delete_key = {
        let mut key = HashMap::new();
        key.insert("id".to_string(), AttributeValue::S("to_delete".to_string()));
        key
    };
    let deleted_item = provider
        .get_item_map(TableName::new("PaginatedTable"), delete_key, true)
        .await
        .unwrap();
    assert!(deleted_item.is_none());
}

#[tokio::test]
async fn gsi_projection_limits_attributes() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    // Create a table with GSIs having different projection types
    let create_request = CreateTableRequest::new(
        TableName::new("ProjectionTestTable"),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_sk".to_string(),
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
    )
    .with_global_secondary_indexes(Some(vec![
        CreateGlobalSecondaryIndex {
            index_name: IndexName::new("KeysOnlyGSI"),
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: "gsi_pk".to_string(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "gsi_sk".to_string(),
                    key_type: KeyType::Range,
                },
            ],
            projection: Projection {
                projection_type: Some(ProjectionType::KeysOnly),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        },
        CreateGlobalSecondaryIndex {
            index_name: IndexName::new("IncludeGSI"),
            key_schema: vec![KeySchemaElement {
                attribute_name: "gsi_pk".to_string(),
                key_type: KeyType::Hash,
            }],
            projection: Projection {
                projection_type: Some(ProjectionType::Include),
                non_key_attributes: Some(vec!["included_attr".to_string()]),
            },
            provisioned_throughput: None,
        },
        CreateGlobalSecondaryIndex {
            index_name: IndexName::new("AllGSI"),
            key_schema: vec![KeySchemaElement {
                attribute_name: "gsi_pk".to_string(),
                key_type: KeyType::Hash,
            }],
            projection: Projection {
                projection_type: Some(ProjectionType::All),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        },
    ]));

    provider.create_table(&create_request).await.unwrap();

    // Put an item with many attributes
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("main_pk".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("main_sk".to_string()));
    item.insert(
        "gsi_pk".to_string(),
        AttributeValue::S("index_pk".to_string()),
    );
    item.insert(
        "gsi_sk".to_string(),
        AttributeValue::S("index_sk".to_string()),
    );
    item.insert(
        "included_attr".to_string(),
        AttributeValue::S("should_be_included".to_string()),
    );
    item.insert(
        "excluded_attr".to_string(),
        AttributeValue::S("should_be_excluded".to_string()),
    );
    item.insert(
        "another_attr".to_string(),
        AttributeValue::S("also_excluded".to_string()),
    );

    provider
        .put_item(
            TableName::new("ProjectionTestTable"),
            item.clone(),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    // Process GSI updates (this would normally be done by the background job)
    provider.process_gsi_updates().await.unwrap();

    // Query each GSI and verify the attributes

    // KeysOnly GSI should only have key attributes
    let keys_only_scan = ScanTableRequest {
        table_name: TableName::new("ProjectionTestTable"),
        index_name: Some(IndexName::new("KeysOnlyGSI")),
        limit: Some(10),
        exclusive_start_key: None,
        consistent_read: false,
    };
    let (keys_only_items, _) = provider.scan_table(&keys_only_scan).await.unwrap();
    assert_eq!(keys_only_items.len(), 1);

    let keys_only_item = &keys_only_items[0];
    assert_eq!(keys_only_item.len(), 4); // pk, sk, gsi_pk, gsi_sk
    assert!(keys_only_item.contains_key("pk"));
    assert!(keys_only_item.contains_key("sk"));
    assert!(keys_only_item.contains_key("gsi_pk"));
    assert!(keys_only_item.contains_key("gsi_sk"));
    assert!(!keys_only_item.contains_key("included_attr"));
    assert!(!keys_only_item.contains_key("excluded_attr"));
    assert!(!keys_only_item.contains_key("another_attr"));

    // Include GSI should have keys plus included attributes
    let include_scan = ScanTableRequest {
        table_name: TableName::new("ProjectionTestTable"),
        index_name: Some(IndexName::new("IncludeGSI")),
        limit: Some(10),
        exclusive_start_key: None,
        consistent_read: false,
    };
    let (include_items, _) = provider.scan_table(&include_scan).await.unwrap();
    assert_eq!(include_items.len(), 1);

    let include_item = &include_items[0];
    assert_eq!(include_item.len(), 4); // pk, sk, gsi_pk, included_attr
    assert!(include_item.contains_key("pk"));
    assert!(include_item.contains_key("sk"));
    assert!(include_item.contains_key("gsi_pk"));
    assert!(include_item.contains_key("included_attr"));
    assert!(!include_item.contains_key("gsi_sk")); // Not in this GSI's key schema
    assert!(!include_item.contains_key("excluded_attr"));
    assert!(!include_item.contains_key("another_attr"));

    // All GSI should have all attributes
    let all_scan = ScanTableRequest {
        table_name: TableName::new("ProjectionTestTable"),
        index_name: Some(IndexName::new("AllGSI")),
        limit: Some(10),
        exclusive_start_key: None,
        consistent_read: false,
    };
    let (all_items, _) = provider.scan_table(&all_scan).await.unwrap();
    assert_eq!(all_items.len(), 1);

    let all_item = &all_items[0];
    assert_eq!(all_item.len(), 7); // All attributes
    assert!(all_item.contains_key("pk"));
    assert!(all_item.contains_key("sk"));
    assert!(all_item.contains_key("gsi_pk"));
    assert!(all_item.contains_key("included_attr"));
    assert!(all_item.contains_key("excluded_attr"));
    assert!(all_item.contains_key("another_attr"));
}

#[tokio::test]
async fn batch_get_item_operations() {
    use storage_types::{BatchGetItemRequest, KeysAndAttributes};

    let provider = create_test_table().await;

    // Add some additional test items
    let items = vec![
        ("batch_get1", "get_data1"),
        ("batch_get2", "get_data2"),
        ("batch_get3", "get_data3"),
    ];

    for (id, data) in items {
        let mut item = HashMap::new();
        item.insert("id".to_string(), AttributeValue::S(id.to_string()));
        item.insert("data".to_string(), AttributeValue::S(data.to_string()));
        provider
            .put_item(
                TableName::new("PaginatedTable"),
                item,
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
    }

    // Batch get operation
    let mut request_items = HashMap::new();

    let keys = vec![
        {
            let mut key = HashMap::new();
            key.insert(
                "id".to_string(),
                AttributeValue::S("batch_get1".to_string()),
            );
            key
        },
        {
            let mut key = HashMap::new();
            key.insert(
                "id".to_string(),
                AttributeValue::S("batch_get2".to_string()),
            );
            key
        },
        {
            let mut key = HashMap::new();
            key.insert(
                "id".to_string(),
                AttributeValue::S("nonexistent".to_string()),
            );
            key
        },
    ];

    request_items.insert(
        TableName::new("PaginatedTable"),
        KeysAndAttributes {
            keys: keys.into_iter().map(Into::into).collect::<Vec<_>>().into(),
            attributes_to_get: None,
            projection_expression: None,
            expression_attribute_names: None,
            consistent_read: None,
        },
    );

    let batch_request = BatchGetItemRequest {
        request_items,
        return_consumed_capacity: None,
    };

    let response = provider.batch_get_item(batch_request).await.unwrap();

    assert!(response.unprocessed_keys.is_none());
    assert!(response.responses.is_some());

    let responses = response.responses.unwrap();
    let table_responses = responses.get(&TableName::new("PaginatedTable")).unwrap();

    // Should return 2 items (batch_get1 and batch_get2), nonexistent item won't be
    // returned
    assert_eq!(table_responses.len(), 2);

    let mut found_ids = Vec::new();
    for item in table_responses {
        let id = item.get("id").unwrap();
        found_ids.push(id.clone());
    }

    assert!(found_ids.contains(&AttributeValue::S("batch_get1".to_string())));
    assert!(found_ids.contains(&AttributeValue::S("batch_get2".to_string())));
}

#[tokio::test]
async fn batch_write_item_with_streams() {
    use storage_types::{BatchWriteItemRequest, PutRequest, WriteRequest};

    let (provider, _table_name) = create_stream_test_table().await;

    let mut request_items = HashMap::new();

    let write_requests = vec![
        WriteRequest {
            put_request: Some(PutRequest {
                item: {
                    let mut item = HashMap::new();
                    item.insert(
                        "pk".to_string(),
                        AttributeValue::S("batch_stream1".to_string()),
                    );
                    item.insert("sk".to_string(), AttributeValue::S("sort1".to_string()));
                    item.insert(
                        "data".to_string(),
                        AttributeValue::S("stream_data1".to_string()),
                    );
                    item
                },
            }),
            delete_request: None,
        },
        WriteRequest {
            put_request: Some(PutRequest {
                item: {
                    let mut item = HashMap::new();
                    item.insert(
                        "pk".to_string(),
                        AttributeValue::S("batch_stream2".to_string()),
                    );
                    item.insert("sk".to_string(), AttributeValue::S("sort2".to_string()));
                    item.insert(
                        "data".to_string(),
                        AttributeValue::S("stream_data2".to_string()),
                    );
                    item
                },
            }),
            delete_request: None,
        },
    ];

    request_items.insert(TableName::new("StreamTestTable"), write_requests);

    let batch_request = BatchWriteItemRequest {
        request_items,
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    };

    let response = provider
        .batch_write_item(batch_request, true)
        .await
        .unwrap();

    assert!(
        response.unprocessed_items.is_none(),
        "All items should be processed, but found: {:?}",
        response.unprocessed_items
    );

    // Verify stream entries were created - check system stream for activity
    let system_page =
        StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
            .await
            .unwrap();

    // Should have at least 2 entries from the batch operation
    assert!(
        system_page.items.len() >= 2,
        "System stream should have batch entries, found {}",
        system_page.items.len()
    );
    for item in system_page.items.iter().take(2) {
        let pointer: StoredStreamPointer =
            storage_types::storage_serde::from_bytes(&item.data).unwrap();
        assert_eq!(
            pointer.target_item_stream_version(),
            storage_types::ItemStreamVersion::new(1)
        );
    }
}

#[tokio::test]
async fn batch_write_item_reuses_ttl_config_within_transaction() {
    use storage_types::{BatchWriteItemRequest, PutRequest, WriteRequest};

    let (provider, table_name) = create_stream_test_table().await;
    provider
        .update_time_to_live(UpdateTimeToLiveRequest {
            table_name: table_name.clone(),
            time_to_live_specification: TimeToLiveSpecification {
                attribute_name: "ttl".to_string(),
                enabled: true,
            },
        })
        .await
        .unwrap();

    let mut request_items = HashMap::new();
    request_items.insert(
        table_name.clone(),
        (0..8)
            .map(|index| {
                let mut item = HashMap::new();
                item.insert(
                    "pk".to_string(),
                    AttributeValue::S(format!("batch_ttl_{index}")),
                );
                item.insert("sk".to_string(), AttributeValue::S("sort".to_string()));
                item.insert(
                    "ttl".to_string(),
                    AttributeValue::N((Utc::now().timestamp() + 3_600 + index).to_string()),
                );
                WriteRequest {
                    put_request: Some(PutRequest { item }),
                    delete_request: None,
                }
            })
            .collect(),
    );

    provider
        .batch_write_item(
            BatchWriteItemRequest {
                request_items,
                return_consumed_capacity: None,
                return_item_collection_metrics: None,
            },
            true,
        )
        .await
        .unwrap();

    let ttl_table = naming::physical_ttl_index_table_name(&table_name);
    let row_count = provider
        .connection
        .call_unwrap(move |conn| {
            let sql = format!("SELECT COUNT(*) FROM \"{ttl_table}\"");
            conn.query_row(&sql, [], |row| row.get::<_, i64>(0))
        })
        .await
        .unwrap();
    assert_eq!(row_count, 8);
}

#[tokio::test]
async fn batch_write_item_encode_missing_table_returns_not_found() {
    use storage_types::{
        BatchWriteItemEncodeRequest, BatchWriteItemRequest, PutRequest, WriteRequest,
    };

    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let mut item = HashMap::new();
    item.insert("id".to_string(), AttributeValue::S("missing".to_string()));

    let request = BatchWriteItemRequest {
        request_items: HashMap::from([(
            TableName::new("NonExistentTable"),
            vec![WriteRequest {
                put_request: Some(PutRequest { item }),
                delete_request: None,
            }],
        )]),
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    };

    let encoded = BatchWriteItemEncodeRequest::try_from(request).expect("encode request");
    let err = provider
        .batch_write_item_encode(encoded, true)
        .await
        .expect_err("missing table should fail");
    assert!(matches!(err.as_ref(), StorageEnum::TableNotFound { .. }));
}

#[tokio::test]
async fn batch_get_item_multiple_tables() {
    use storage_types::{BatchGetItemRequest, KeysAndAttributes};

    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    // Create two test tables
    let create_table1 = CreateTableRequest::new(
        TableName::new("Table1"),
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

    let create_table2 = CreateTableRequest::new(
        TableName::new("Table2"),
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

    provider.create_table(&create_table1).await.unwrap();
    provider.create_table(&create_table2).await.unwrap();

    // Add items to both tables
    let table1_items = vec![("item1", "data1"), ("item2", "data2")];
    for (id, data) in table1_items {
        let mut item = HashMap::new();
        item.insert("id".to_string(), AttributeValue::S(id.to_string()));
        item.insert("value".to_string(), AttributeValue::S(data.to_string()));
        provider
            .put_item(TableName::new("Table1"), item, None, None, None, None)
            .await
            .unwrap();
    }

    let table2_items = vec![("itemA", "dataA"), ("itemB", "dataB")];
    for (pk, data) in table2_items {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S(pk.to_string()));
        item.insert("info".to_string(), AttributeValue::S(data.to_string()));
        provider
            .put_item(TableName::new("Table2"), item, None, None, None, None)
            .await
            .unwrap();
    }

    // Perform batch get across multiple tables
    let mut request_items = HashMap::new();

    // Keys for Table1
    let table1_keys = vec![
        {
            let mut key = HashMap::new();
            key.insert("id".to_string(), AttributeValue::S("item1".to_string()));
            key
        },
        {
            let mut key = HashMap::new();
            key.insert(
                "id".to_string(),
                AttributeValue::S("nonexistent1".to_string()),
            );
            key
        },
    ];

    request_items.insert(
        TableName::new("Table1"),
        KeysAndAttributes {
            keys: table1_keys
                .into_iter()
                .map(Into::into)
                .collect::<Vec<_>>()
                .into(),
            attributes_to_get: None,
            projection_expression: None,
            expression_attribute_names: None,
            consistent_read: None,
        },
    );

    // Keys for Table2
    let table2_keys = vec![
        {
            let mut key = HashMap::new();
            key.insert("pk".to_string(), AttributeValue::S("itemA".to_string()));
            key
        },
        {
            let mut key = HashMap::new();
            key.insert("pk".to_string(), AttributeValue::S("itemB".to_string()));
            key
        },
    ];

    request_items.insert(
        TableName::new("Table2"),
        KeysAndAttributes {
            keys: table2_keys
                .into_iter()
                .map(Into::into)
                .collect::<Vec<_>>()
                .into(),
            attributes_to_get: None,
            projection_expression: None,
            expression_attribute_names: None,
            consistent_read: None,
        },
    );

    let batch_request = BatchGetItemRequest {
        request_items,
        return_consumed_capacity: None,
    };

    let response = provider.batch_get_item(batch_request).await.unwrap();

    assert!(response.unprocessed_keys.is_none());
    assert!(response.responses.is_some());

    let responses = response.responses.unwrap();

    // Verify Table1 responses
    let table1_responses = responses.get(&TableName::new("Table1")).unwrap();
    assert_eq!(table1_responses.len(), 1); // Only item1 exists, nonexistent1 won't be returned

    let table1_item = &table1_responses[0];
    assert_eq!(
        table1_item.get("id").unwrap(),
        &AttributeValue::S("item1".to_string())
    );
    assert_eq!(
        table1_item.get("value").unwrap(),
        &AttributeValue::S("data1".to_string())
    );

    // Verify Table2 responses
    let table2_responses = responses.get(&TableName::new("Table2")).unwrap();
    assert_eq!(table2_responses.len(), 2); // Both itemA and itemB exist

    let mut found_pks = Vec::new();
    for item in table2_responses {
        let pk = item.get("pk").unwrap();
        found_pks.push(pk.clone());
    }

    assert!(found_pks.contains(&AttributeValue::S("itemA".to_string())));
    assert!(found_pks.contains(&AttributeValue::S("itemB".to_string())));

    // Verify we got responses from exactly 2 tables
    assert_eq!(responses.len(), 2);
}

async fn create_table_with_gsi() -> SQLiteStorageProvider {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let create_request = CreateTableRequest::new(
        TableName::new("GSITestTable"),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_sk".to_string(),
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
    )
    .with_global_secondary_indexes(Some(vec![CreateGlobalSecondaryIndex {
        index_name: IndexName::new("TestGSI"),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "gsi_pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "gsi_sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }]));

    provider.create_table(&create_request).await.unwrap();

    // Add test items with both main table and GSI keys
    let test_items = vec![
        ("pk1", "sk1", "gsi_pk1", "gsi_sk1", "data1"),
        ("pk1", "sk2", "gsi_pk1", "gsi_sk2", "data2"),
        ("pk2", "sk1", "gsi_pk2", "gsi_sk1", "data3"),
        ("pk2", "sk2", "gsi_pk2", "gsi_sk2", "data4"),
        ("pk3", "sk1", "gsi_pk1", "gsi_sk3", "data5"), // Same GSI partition key as first two
    ];

    for (pk, sk, gsi_pk, gsi_sk, data) in test_items {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S(pk.to_string()));
        item.insert("sk".to_string(), AttributeValue::S(sk.to_string()));
        item.insert("gsi_pk".to_string(), AttributeValue::S(gsi_pk.to_string()));
        item.insert("gsi_sk".to_string(), AttributeValue::S(gsi_sk.to_string()));
        item.insert("data".to_string(), AttributeValue::S(data.to_string()));
        provider
            .put_item(TableName::new("GSITestTable"), item, None, None, None, None)
            .await
            .unwrap();
    }

    // Process GSI updates synchronously for testing
    provider.process_gsi_updates().await.unwrap();

    provider
}

#[tokio::test]
async fn scan_table_with_gsi() {
    let provider = create_table_with_gsi().await;

    // Scan using the GSI
    let scan_request = ScanTableRequest {
        table_name: TableName::new("GSITestTable"),
        index_name: Some(IndexName::new("TestGSI")),
        limit: Some(10),
        exclusive_start_key: None,
        consistent_read: false,
    };

    let (items, last_evaluated_key) = provider.scan_table(&scan_request).await.unwrap();

    // Should return all 5 items
    assert_eq!(items.len(), 5);
    assert!(last_evaluated_key.is_none()); // No pagination needed for 5 items

    // Verify all items have the expected GSI attributes
    for (i, item) in items.iter().enumerate() {
        println!("Item {} keys: {:?}", i, item.keys().collect::<Vec<_>>());
        assert!(item.contains_key("gsi_pk"), "Item {i} missing gsi_pk");
        assert!(item.contains_key("gsi_sk"), "Item {i} missing gsi_sk");
        assert!(item.contains_key("data"), "Item {i} missing data");
    }

    // Verify we can retrieve the same items via main table scan
    let main_scan_request = ScanTableRequest {
        table_name: TableName::new("GSITestTable"),
        index_name: None,
        limit: Some(10),
        exclusive_start_key: None,
        consistent_read: false,
    };

    let (main_items, _) = provider.scan_table(&main_scan_request).await.unwrap();
    assert_eq!(main_items.len(), 5);
}

#[tokio::test]
async fn scan_table_gsi_pagination() {
    let provider = create_table_with_gsi().await;

    // Scan GSI with small page size
    let scan_request = ScanTableRequest {
        table_name: TableName::new("GSITestTable"),
        index_name: Some(IndexName::new("TestGSI")),
        limit: Some(2),
        exclusive_start_key: None,
        consistent_read: false,
    };

    let (first_page, last_key) = provider.scan_table(&scan_request).await.unwrap();

    assert_eq!(first_page.len(), 2);
    assert!(last_key.is_some()); // Should have more items

    // Get second page
    let second_scan_request = ScanTableRequest {
        table_name: TableName::new("GSITestTable"),
        index_name: Some(IndexName::new("TestGSI")),
        limit: Some(2),
        exclusive_start_key: last_key,
        consistent_read: false,
    };

    let (second_page, second_last_key) = provider.scan_table(&second_scan_request).await.unwrap();

    assert_eq!(second_page.len(), 2);
    assert!(second_last_key.is_some());

    // Get third page (should have 1 item)
    let third_scan_request = ScanTableRequest {
        table_name: TableName::new("GSITestTable"),
        index_name: Some(IndexName::new("TestGSI")),
        limit: Some(2),
        exclusive_start_key: second_last_key,
        consistent_read: false,
    };

    let (third_page, third_last_key) = provider.scan_table(&third_scan_request).await.unwrap();

    assert_eq!(third_page.len(), 1);
    assert!(third_last_key.is_none()); // No more items

    // Verify all items are unique across pages
    let mut all_item_keys = std::collections::HashSet::new();
    for item in &first_page {
        let key = format!(
            "{}-{}",
            item.get("gsi_pk")
                .unwrap()
                .inner_string()
                .expect("gsi_pk scalar"),
            item.get("gsi_sk")
                .unwrap()
                .inner_string()
                .expect("gsi_sk scalar")
        );
        assert!(all_item_keys.insert(key));
    }
    for item in &second_page {
        let key = format!(
            "{}-{}",
            item.get("gsi_pk")
                .unwrap()
                .inner_string()
                .expect("gsi_pk scalar"),
            item.get("gsi_sk")
                .unwrap()
                .inner_string()
                .expect("gsi_sk scalar")
        );
        assert!(all_item_keys.insert(key));
    }
    for item in &third_page {
        let key = format!(
            "{}-{}",
            item.get("gsi_pk")
                .unwrap()
                .inner_string()
                .expect("gsi_pk scalar"),
            item.get("gsi_sk")
                .unwrap()
                .inner_string()
                .expect("gsi_sk scalar")
        );
        assert!(all_item_keys.insert(key));
    }

    assert_eq!(all_item_keys.len(), 5); // All items should be unique
}

#[tokio::test]
async fn query_table_with_gsi_hash_key_only() {
    let provider = create_table_with_gsi().await;

    // Query GSI by hash key only
    let query_request = storage_types::QueryTableRequest {
        table_name: TableName::new("GSITestTable"),
        index_name: Some(IndexName::new("TestGSI")),
        key_condition_expression: "gsi_pk = :pk".to_string(),
        expression_attribute_values: Some({
            let mut values = HashMap::new();
            values.insert(":pk".to_string(), AttributeValue::S("gsi_pk1".to_string()));
            values
        }),
        expression_attribute_names: None,
        limit: Some(10),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };

    let (items, last_evaluated_key) = provider.query_table(&query_request).await.unwrap();

    // Should return 3 items with gsi_pk1 (from our test data)
    assert_eq!(items.len(), 3);
    assert!(last_evaluated_key.is_none());

    // Verify all returned items have the correct GSI partition key
    for item in &items {
        assert_eq!(
            item.get("gsi_pk").unwrap(),
            &AttributeValue::S("gsi_pk1".to_string())
        );
    }

    // Verify we get the expected sort keys
    let mut found_sort_keys = std::collections::HashSet::new();
    for item in &items {
        found_sort_keys.insert(
            item.get("gsi_sk")
                .unwrap()
                .inner_string()
                .expect("gsi_sk scalar"),
        );
    }

    assert!(found_sort_keys.contains("gsi_sk1"));
    assert!(found_sort_keys.contains("gsi_sk2"));
    assert!(found_sort_keys.contains("gsi_sk3"));
}

#[tokio::test]
async fn query_table_with_gsi_hash_and_range_key() {
    let provider = create_table_with_gsi().await;

    // Query GSI by both hash and range key
    let query_request = storage_types::QueryTableRequest {
        table_name: TableName::new("GSITestTable"),
        index_name: Some(IndexName::new("TestGSI")),
        key_condition_expression: "gsi_pk = :pk AND gsi_sk = :sk".to_string(),
        expression_attribute_values: Some({
            let mut values = HashMap::new();
            values.insert(":pk".to_string(), AttributeValue::S("gsi_pk1".to_string()));
            values.insert(":sk".to_string(), AttributeValue::S("gsi_sk1".to_string()));
            values
        }),
        expression_attribute_names: None,
        limit: Some(10),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };

    let (items, last_evaluated_key) = provider.query_table(&query_request).await.unwrap();

    // Should return exactly 1 item
    assert_eq!(items.len(), 1);
    assert!(last_evaluated_key.is_none());

    let item = &items[0];
    assert_eq!(
        item.get("gsi_pk").unwrap(),
        &AttributeValue::S("gsi_pk1".to_string())
    );
    assert_eq!(
        item.get("gsi_sk").unwrap(),
        &AttributeValue::S("gsi_sk1".to_string())
    );
    assert_eq!(
        item.get("data").unwrap(),
        &AttributeValue::S("data1".to_string())
    );
}

#[tokio::test]
async fn query_table_gsi_with_range_key_condition() {
    let provider = create_table_with_gsi().await;

    // Query GSI with range key condition (begins_with)
    let query_request = storage_types::QueryTableRequest {
        table_name: TableName::new("GSITestTable"),
        index_name: Some(IndexName::new("TestGSI")),
        key_condition_expression: "gsi_pk = :pk AND begins_with(gsi_sk, :sk_prefix)".to_string(),
        expression_attribute_values: Some({
            let mut values = HashMap::new();
            values.insert(":pk".to_string(), AttributeValue::S("gsi_pk1".to_string()));
            values.insert(
                ":sk_prefix".to_string(),
                AttributeValue::S("gsi_sk".to_string()),
            );
            values
        }),
        expression_attribute_names: None,
        limit: Some(10),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };

    let (items, last_evaluated_key) = provider.query_table(&query_request).await.unwrap();

    // Should return 3 items (all start with "gsi_sk")
    assert_eq!(items.len(), 3);
    assert!(last_evaluated_key.is_none());

    // Verify all items have the correct partition key and sort key prefix
    for item in &items {
        assert_eq!(
            item.get("gsi_pk").unwrap(),
            &AttributeValue::S("gsi_pk1".to_string())
        );
        let sk = item
            .get("gsi_sk")
            .unwrap()
            .inner_string()
            .expect("gsi_sk scalar");
        assert!(sk.starts_with("gsi_sk"));
    }
}

#[tokio::test]
async fn query_table_gsi_pagination() {
    let provider = create_table_with_gsi().await;

    // Query GSI with pagination
    let query_request = storage_types::QueryTableRequest {
        table_name: TableName::new("GSITestTable"),
        index_name: Some(IndexName::new("TestGSI")),
        key_condition_expression: "gsi_pk = :pk".to_string(),
        expression_attribute_values: Some({
            let mut values = HashMap::new();
            values.insert(":pk".to_string(), AttributeValue::S("gsi_pk1".to_string()));
            values
        }),
        expression_attribute_names: None,
        limit: Some(2),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };

    let (first_page, last_key) = provider.query_table(&query_request).await.unwrap();

    assert_eq!(first_page.len(), 2);
    assert!(last_key.is_some());

    // Get second page
    let second_query_request = storage_types::QueryTableRequest {
        table_name: TableName::new("GSITestTable"),
        index_name: Some(IndexName::new("TestGSI")),
        key_condition_expression: "gsi_pk = :pk".to_string(),
        expression_attribute_values: Some({
            let mut values = HashMap::new();
            values.insert(":pk".to_string(), AttributeValue::S("gsi_pk1".to_string()));
            values
        }),
        expression_attribute_names: None,
        limit: Some(2),
        exclusive_start_key: last_key,
        scan_index_forward: Some(true),
        consistent_read: false,
    };

    let (second_page, second_last_key) = provider.query_table(&second_query_request).await.unwrap();

    assert_eq!(second_page.len(), 1); // Only 1 item left
    assert!(second_last_key.is_none());

    // Verify all items are unique across pages
    let mut all_item_keys = std::collections::HashSet::new();
    for item in &first_page {
        let key = format!(
            "{}-{}",
            item.get("gsi_pk")
                .unwrap()
                .inner_string()
                .expect("gsi_pk scalar"),
            item.get("gsi_sk")
                .unwrap()
                .inner_string()
                .expect("gsi_sk scalar")
        );
        assert!(all_item_keys.insert(key));
    }
    for item in &second_page {
        let key = format!(
            "{}-{}",
            item.get("gsi_pk")
                .unwrap()
                .inner_string()
                .expect("gsi_pk scalar"),
            item.get("gsi_sk")
                .unwrap()
                .inner_string()
                .expect("gsi_sk scalar")
        );
        assert!(all_item_keys.insert(key));
    }

    assert_eq!(all_item_keys.len(), 3); // All items from gsi_pk1 partition
}

#[tokio::test]
async fn query_table_gsi_pagination_includes_items_added_after_first_page() {
    let provider = create_table_with_gsi().await;

    let first_query_request = storage_types::QueryTableRequest {
        table_name: TableName::new("GSITestTable"),
        index_name: Some(IndexName::new("TestGSI")),
        key_condition_expression: "gsi_pk = :pk".to_string(),
        expression_attribute_values: Some({
            let mut values = HashMap::new();
            values.insert(":pk".to_string(), AttributeValue::S("gsi_pk1".to_string()));
            values
        }),
        expression_attribute_names: None,
        limit: Some(2),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };

    let (first_page, last_key) = provider.query_table(&first_query_request).await.unwrap();
    assert_eq!(first_page.len(), 2);
    assert!(last_key.is_some());

    let mut new_item = HashMap::new();
    new_item.insert("pk".to_string(), AttributeValue::S("pk4".to_string()));
    new_item.insert("sk".to_string(), AttributeValue::S("sk1".to_string()));
    new_item.insert(
        "gsi_pk".to_string(),
        AttributeValue::S("gsi_pk1".to_string()),
    );
    new_item.insert(
        "gsi_sk".to_string(),
        AttributeValue::S("gsi_sk4".to_string()),
    );
    new_item.insert("data".to_string(), AttributeValue::S("data6".to_string()));
    provider
        .put_item(
            TableName::new("GSITestTable"),
            new_item,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    provider.process_gsi_updates().await.unwrap();

    let second_query_request = storage_types::QueryTableRequest {
        table_name: TableName::new("GSITestTable"),
        index_name: Some(IndexName::new("TestGSI")),
        key_condition_expression: "gsi_pk = :pk".to_string(),
        expression_attribute_values: Some({
            let mut values = HashMap::new();
            values.insert(":pk".to_string(), AttributeValue::S("gsi_pk1".to_string()));
            values
        }),
        expression_attribute_names: None,
        limit: Some(10),
        exclusive_start_key: last_key,
        scan_index_forward: Some(true),
        consistent_read: false,
    };

    let (second_page, second_last_key) = provider.query_table(&second_query_request).await.unwrap();
    let second_page_gsi_sks = second_page
        .iter()
        .map(|item| {
            item.get("gsi_sk")
                .unwrap()
                .inner_string()
                .expect("gsi_sk scalar")
        })
        .collect::<Vec<_>>();

    assert_eq!(second_page_gsi_sks, vec!["gsi_sk3", "gsi_sk4"]);
    assert!(second_last_key.is_none());
}

#[tokio::test]
async fn query_table_gsi_reverse_order() {
    let provider = create_table_with_gsi().await;

    // Query GSI in reverse order
    let query_request = storage_types::QueryTableRequest {
        table_name: TableName::new("GSITestTable"),
        index_name: Some(IndexName::new("TestGSI")),
        key_condition_expression: "gsi_pk = :pk".to_string(),
        expression_attribute_values: Some({
            let mut values = HashMap::new();
            values.insert(":pk".to_string(), AttributeValue::S("gsi_pk1".to_string()));
            values
        }),
        expression_attribute_names: None,
        limit: Some(10),
        exclusive_start_key: None,
        scan_index_forward: Some(false), // Reverse order
        consistent_read: false,
    };

    let (items, _) = provider.query_table(&query_request).await.unwrap();

    assert_eq!(items.len(), 3);

    // Verify items are in reverse sort key order
    let sk1 = items[0]
        .get("gsi_sk")
        .unwrap()
        .inner_string()
        .expect("gsi_sk scalar");
    let sk2 = items[1]
        .get("gsi_sk")
        .unwrap()
        .inner_string()
        .expect("gsi_sk scalar");
    let sk3 = items[2]
        .get("gsi_sk")
        .unwrap()
        .inner_string()
        .expect("gsi_sk scalar");

    // Should be gsi_sk3, gsi_sk2, gsi_sk1 (reverse alphabetical)
    assert_eq!(sk1, "gsi_sk3");
    assert_eq!(sk2, "gsi_sk2");
    assert_eq!(sk3, "gsi_sk1");
}

#[tokio::test]
async fn scan_table_gsi_vs_main_table_consistency() {
    let provider = create_table_with_gsi().await;

    // Scan main table
    let main_scan_request = ScanTableRequest {
        table_name: TableName::new("GSITestTable"),
        index_name: None,
        limit: Some(10),
        exclusive_start_key: None,
        consistent_read: false,
    };

    let (main_items, _) = provider.scan_table(&main_scan_request).await.unwrap();

    // Scan GSI
    let gsi_scan_request = ScanTableRequest {
        table_name: TableName::new("GSITestTable"),
        index_name: Some(IndexName::new("TestGSI")),
        limit: Some(10),
        exclusive_start_key: None,
        consistent_read: false,
    };

    let (gsi_items, _) = provider.scan_table(&gsi_scan_request).await.unwrap();

    // Both should return the same number of items
    assert_eq!(main_items.len(), gsi_items.len());

    // All main table items should be present in GSI results (and vice versa)
    // We compare by data field since that's unique
    let main_data: std::collections::HashSet<_> = main_items
        .iter()
        .map(|item| {
            item.get("data")
                .unwrap()
                .inner_string()
                .expect("data scalar")
        })
        .collect();

    let gsi_data: std::collections::HashSet<_> = gsi_items
        .iter()
        .map(|item| {
            item.get("data")
                .unwrap()
                .inner_string()
                .expect("data scalar")
        })
        .collect();

    assert_eq!(main_data, gsi_data);
}

#[tokio::test]
async fn ttl_sweep_removes_expired_items_sqlite() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();
    let table = TableName::new("TtlSweepSqlite");
    create_ttl_enabled_table(&provider, &table).await;

    let expired_at = (Utc::now().timestamp() - 120).to_string();
    let mut expired_item = HashMap::new();
    expired_item.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    expired_item.insert("sk".to_string(), AttributeValue::S("expired".to_string()));
    expired_item.insert("ttl".to_string(), AttributeValue::N(expired_at.clone()));
    provider
        .put_item(table.clone(), expired_item, None, None, None, None)
        .await
        .unwrap();

    for _ in 0..200 {
        provider.run_ttl_sweep().await.unwrap_or_else(|err| {
            if let StorageError::Base(StorageEnum::InternalServerError { message }) = &err {
                panic!("ttl sweep job failed: {message}");
            }
            panic!("ttl sweep job failed: {err:?}");
        });
        let mut expired_key = HashMap::new();
        expired_key.insert("pk".to_string(), AttributeValue::S("user".to_string()));
        expired_key.insert("sk".to_string(), AttributeValue::S("expired".to_string()));
        if provider
            .get_item_map(table.clone(), expired_key.clone(), true)
            .await
            .unwrap_or_else(|err| panic!("get_item during sweep failed: {err:?}"))
            .is_none()
        {
            break;
        }
    }
    let mut expired_key = HashMap::new();
    expired_key.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    expired_key.insert("sk".to_string(), AttributeValue::S("expired".to_string()));
    assert!(
        provider
            .get_item_map(table.clone(), expired_key, true)
            .await
            .unwrap_or_else(|err| panic!("final get_item failed: {err:?}"))
            .is_none()
    );

    let config = provider.load_ttl_config(&table).await.unwrap().unwrap();
    let expected_ttl = expired_at.parse::<i64>().unwrap();
    assert_eq!(
        config.last_processed_watermark,
        Some(expected_ttl),
        "sweep should record last processed TTL watermark"
    );
}

#[tokio::test]
async fn ttl_index_removed_on_delete_sqlite() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();
    let table = TableName::new("TtlIndexDeleteSqlite");
    create_ttl_enabled_table(&provider, &table).await;

    let future_at = (Utc::now().timestamp() + 3_600).to_string();
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("session".to_string()));
    item.insert("ttl".to_string(), AttributeValue::N(future_at.clone()));
    provider
        .put_item(table.clone(), item.clone(), None, None, None, None)
        .await
        .unwrap();

    let table_info = provider.get_table_info(&table).await.unwrap();
    let token = storage_common::ttl::ttl_index_key_token_for_item(&table_info, &item).unwrap();
    let ttl_value = i64::try_from(storage_common::ttl::normalize_ttl_seconds(
        future_at.parse::<i64>().unwrap(),
    ))
    .expect("normalized ttl should fit i64");
    let ttl_table = naming::physical_ttl_index_table_name(&table);
    let has_row: bool = provider
        .connection
        .call_unwrap({
            let ttl_table = ttl_table.clone();
            let token = token.clone();
            move |conn| -> Result<bool, rusqlite::Error> {
                let sql = format!(
                    "SELECT 1 FROM \"{ttl_table}\" WHERE ttl_value = ?1 AND key_token = ?2"
                );
                let mut stmt = conn.prepare(&sql)?;
                let mut rows = stmt.query(rusqlite::params![ttl_value, token])?;
                Ok(rows.next()?.is_some())
            }
        })
        .await
        .unwrap();
    assert!(has_row, "ttl index row should exist before delete");

    let mut delete_key = HashMap::new();
    delete_key.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    delete_key.insert("sk".to_string(), AttributeValue::S("session".to_string()));
    provider
        .delete_item(table.clone(), delete_key.into(), None, None, None)
        .await
        .unwrap();

    let has_row_after: bool = provider
        .connection
        .call_unwrap({
            let ttl_table = ttl_table.clone();
            let token = token.clone();
            move |conn| -> Result<bool, rusqlite::Error> {
                let sql = format!(
                    "SELECT 1 FROM \"{ttl_table}\" WHERE ttl_value = ?1 AND key_token = ?2"
                );
                let mut stmt = conn.prepare(&sql)?;
                let mut rows = stmt.query(rusqlite::params![ttl_value, token])?;
                Ok(rows.next()?.is_some())
            }
        })
        .await
        .unwrap();
    assert!(
        !has_row_after,
        "ttl index row should be removed after delete"
    );
}

#[tokio::test]
async fn ttl_index_skips_invalid_ttl_value_sqlite() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();
    let table = TableName::new("TtlIndexInvalidSqlite");
    create_ttl_enabled_table(&provider, &table).await;

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("invalid".to_string()));
    item.insert(
        "ttl".to_string(),
        AttributeValue::S("not-a-number".to_string()),
    );
    provider
        .put_item(table.clone(), item.clone(), None, None, None, None)
        .await
        .unwrap();

    let ttl_table = naming::physical_ttl_index_table_name(&table);
    let has_row: bool = provider
        .connection
        .call_unwrap({
            let ttl_table = ttl_table.clone();
            move |conn| -> Result<bool, rusqlite::Error> {
                let sql = format!("SELECT 1 FROM \"{ttl_table}\" LIMIT 1");
                let mut stmt = conn.prepare(&sql)?;
                let mut rows = stmt.query([])?;
                Ok(rows.next()?.is_some())
            }
        })
        .await
        .unwrap();
    assert!(
        !has_row,
        "invalid ttl values should not write ttl index rows"
    );
}

#[tokio::test]
async fn ttl_index_skips_missing_ttl_attribute_sqlite() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();
    let table = TableName::new("TtlIndexMissingSqlite");
    create_ttl_enabled_table(&provider, &table).await;

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("missing".to_string()));
    provider
        .put_item(table.clone(), item, None, None, None, None)
        .await
        .unwrap();

    let ttl_table = naming::physical_ttl_index_table_name(&table);
    let has_row: bool = provider
        .connection
        .call_unwrap({
            let ttl_table = ttl_table.clone();
            move |conn| -> Result<bool, rusqlite::Error> {
                let sql = format!("SELECT 1 FROM \"{ttl_table}\" LIMIT 1");
                let mut stmt = conn.prepare(&sql)?;
                let mut rows = stmt.query([])?;
                Ok(rows.next()?.is_some())
            }
        })
        .await
        .unwrap();
    assert!(
        !has_row,
        "items without ttl should not write ttl index rows"
    );
}

#[tokio::test]
async fn ttl_sweep_skip_progression_sqlite() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();
    let table = TableName::new("TtlSkipSqlite");
    create_ttl_enabled_table(&provider, &table).await;

    let future_at = (Utc::now().timestamp() + 3_600).to_string();
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("future".to_string()));
    item.insert("ttl".to_string(), AttributeValue::N(future_at));
    provider
        .put_item(table.clone(), item.clone(), None, None, None, None)
        .await
        .unwrap();

    provider.run_job(TTL_SWEEP_JOB).await.unwrap();
    let mut config = provider.load_ttl_config(&table).await.unwrap().unwrap();
    assert_eq!(config.skip_streak, 1);
    assert_eq!(config.skip_runs_remaining, 1);

    provider.run_job(TTL_SWEEP_JOB).await.unwrap();
    config = provider.load_ttl_config(&table).await.unwrap().unwrap();
    assert_eq!(config.skip_streak, 1);
    assert_eq!(config.skip_runs_remaining, 0);

    provider.run_job(TTL_SWEEP_JOB).await.unwrap();
    config = provider.load_ttl_config(&table).await.unwrap().unwrap();
    assert_eq!(config.skip_streak, 2);
    assert_eq!(config.skip_runs_remaining, 2);
}

#[tokio::test]
async fn ttl_sweep_health_check_forces_run_sqlite() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();
    let table = TableName::new("TtlHealthCheckSqlite");
    create_ttl_enabled_table(&provider, &table).await;

    provider.run_ttl_sweep().await.unwrap();

    let mut config = provider.load_ttl_config(&table).await.unwrap().unwrap();
    config.skip_runs_remaining = 3;
    config.skip_streak = 3;
    let interval_ms =
        i64::try_from(constants::TTL_SWEEP_HEALTH_CHECK_INTERVAL_MINUTES).unwrap() * 60_000;
    let forced_past = TimestampMillis::from_timestamp(*TimestampMillis::now() - interval_ms - 1);
    config.last_sweep_started_at = Some(forced_past);
    provider.save_ttl_config(&table, &config).await.unwrap();

    provider.run_ttl_sweep().await.unwrap();

    let refreshed = provider.load_ttl_config(&table).await.unwrap().unwrap();
    let after = refreshed
        .last_sweep_started_at
        .expect("last sweep timestamp");
    assert!(
        after > forced_past,
        "health check should force sweep execution"
    );
}

#[traced_test]
#[tokio::test]
async fn ttl_sweep_emits_traces_sqlite() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();
    let table = TableName::new("TtlTraceSqlite");
    create_ttl_enabled_table(&provider, &table).await;

    let expired_at = (Utc::now().timestamp() - 120).to_string();
    let mut expired_item = HashMap::new();
    expired_item.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    expired_item.insert("sk".to_string(), AttributeValue::S("expired".to_string()));
    expired_item.insert("ttl".to_string(), AttributeValue::N(expired_at.clone()));
    provider
        .put_item(table.clone(), expired_item, None, None, None, None)
        .await
        .unwrap();

    provider.run_job(TTL_SWEEP_JOB).await.unwrap();

    logs_assert(|lines| {
        let table_line = lines
            .iter()
            .find(|line| line.contains("ttl.sweep.table_summary"))
            .ok_or_else(|| "missing ttl.sweep.table_summary trace".to_string())?;
        if !table_line.contains("retry_batches=") {
            return Err("table summary missing retry_batches field".to_string());
        }
        if !table_line.contains("retry_attempts=") {
            return Err("table summary missing retry_attempts field".to_string());
        }
        if !table_line.contains("retry_failures=") {
            return Err("table summary missing retry_failures field".to_string());
        }

        let job_line = lines
            .iter()
            .find(|line| line.contains("ttl.sweep.job_summary"))
            .ok_or_else(|| "missing ttl.sweep.job_summary trace".to_string())?;
        if !job_line.contains("retry_batches=") {
            return Err("job summary missing retry_batches field".to_string());
        }
        if !job_line.contains("retry_attempts=") {
            return Err("job summary missing retry_attempts field".to_string());
        }
        if !job_line.contains("retry_failures=") {
            return Err("job summary missing retry_failures field".to_string());
        }
        Ok(())
    });
}

#[tokio::test]
async fn ttl_disable_removes_ttl_index_sqlite() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();
    let table = TableName::new("TtlDisableSqlite");
    create_ttl_enabled_table(&provider, &table).await;

    let ttl_table = naming::physical_ttl_index_table_name(&table);

    // Disable TTL.
    let disable_request = UpdateTimeToLiveRequest {
        table_name: table.clone(),
        time_to_live_specification: TimeToLiveSpecification {
            attribute_name: "ttl".to_string(),
            enabled: false,
        },
    };
    provider.update_time_to_live(disable_request).await.unwrap();

    // TTL config removed.
    assert!(provider.load_ttl_config(&table).await.unwrap().is_none());

    // TTL index table dropped.
    let exists: bool = provider
        .connection
        .call_unwrap({
            let physical = ttl_table.clone();
            move |conn| -> Result<bool, rusqlite::Error> {
                let sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?1";
                let mut stmt = conn.prepare(sql)?;
                let mut rows = stmt.query(rusqlite::params![physical])?;
                Ok(rows.next()?.is_some())
            }
        })
        .await
        .unwrap();
    assert!(!exists, "ttl index table should be dropped");
}

#[tokio::test]
async fn ttl_sweep_skips_updated_item_sqlite() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();
    let table = TableName::new("TtlConditionalSqlite");
    create_ttl_enabled_table(&provider, &table).await;

    let expired_at = (Utc::now().timestamp() - 120).to_string();
    let future_at = (Utc::now().timestamp() + 3_600).to_string();

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("session".to_string()));
    item.insert("ttl".to_string(), AttributeValue::N(expired_at.clone()));
    provider
        .put_item(table.clone(), item.clone(), None, None, None, None)
        .await
        .unwrap();
    let table_info = provider.get_table_info(&table).await.unwrap();
    let expired_token =
        storage_common::ttl::ttl_index_key_token_for_item(&table_info, &item).unwrap();
    let expired_value = i64::try_from(storage_common::ttl::normalize_ttl_seconds(
        expired_at.parse::<i64>().unwrap(),
    ))
    .expect("normalized ttl should fit i64");
    let ttl_table = naming::physical_ttl_index_table_name(&table);
    let has_expired: bool = provider
        .connection
        .call_unwrap({
            let ttl_table = ttl_table.clone();
            let token = expired_token.clone();
            move |conn| -> Result<bool, rusqlite::Error> {
                let sql = format!(
                    "SELECT 1 FROM \"{ttl_table}\" WHERE ttl_value = ?1 AND key_token = ?2"
                );
                let mut stmt = conn.prepare(&sql)?;
                let mut rows = stmt.query(rusqlite::params![expired_value, token])?;
                Ok(rows.next()?.is_some())
            }
        })
        .await
        .unwrap();
    assert!(has_expired, "expired ttl index row should exist");

    let mut refreshed = HashMap::new();
    refreshed.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    refreshed.insert("sk".to_string(), AttributeValue::S("session".to_string()));
    refreshed.insert("ttl".to_string(), AttributeValue::N(future_at.clone()));
    let refreshed_item = refreshed.clone();
    provider
        .put_item(table.clone(), refreshed, None, None, None, None)
        .await
        .unwrap();
    let refreshed_token =
        storage_common::ttl::ttl_index_key_token_for_item(&table_info, &refreshed_item).unwrap();
    let refreshed_value = i64::try_from(storage_common::ttl::normalize_ttl_seconds(
        future_at.parse::<i64>().unwrap(),
    ))
    .expect("normalized ttl should fit i64");
    let has_refreshed: bool = provider
        .connection
        .call_unwrap({
            let ttl_table = ttl_table.clone();
            let token = refreshed_token.clone();
            move |conn| -> Result<bool, rusqlite::Error> {
                let sql = format!(
                    "SELECT 1 FROM \"{ttl_table}\" WHERE ttl_value = ?1 AND key_token = ?2"
                );
                let mut stmt = conn.prepare(&sql)?;
                let mut rows = stmt.query(rusqlite::params![refreshed_value, token])?;
                Ok(rows.next()?.is_some())
            }
        })
        .await
        .unwrap();
    assert!(has_refreshed, "future ttl index row should exist");
    let has_expired_after: bool = provider
        .connection
        .call_unwrap({
            let ttl_table = ttl_table.clone();
            let token = expired_token.clone();
            move |conn| -> Result<bool, rusqlite::Error> {
                let sql = format!(
                    "SELECT 1 FROM \"{ttl_table}\" WHERE ttl_value = ?1 AND key_token = ?2"
                );
                let mut stmt = conn.prepare(&sql)?;
                let mut rows = stmt.query(rusqlite::params![expired_value, token])?;
                Ok(rows.next()?.is_some())
            }
        })
        .await
        .unwrap();
    assert!(
        !has_expired_after,
        "expired ttl index row should be removed after refresh"
    );

    provider.run_job(TTL_SWEEP_JOB).await.unwrap();

    let mut key = HashMap::new();
    key.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    key.insert("sk".to_string(), AttributeValue::S("session".to_string()));
    let item = provider
        .get_item_map(table.clone(), key, true)
        .await
        .unwrap();
    assert!(
        item.is_some(),
        "item should remain because TTL was extended before sweep completed"
    );
}

async fn create_ttl_enabled_table(provider: &SQLiteStorageProvider, table_name: &TableName) {
    let create_request = CreateTableRequest::new(
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

    provider.create_table(&create_request).await.unwrap();

    let enable_request = UpdateTimeToLiveRequest {
        table_name: table_name.clone(),
        time_to_live_specification: TimeToLiveSpecification {
            attribute_name: "ttl".to_string(),
            enabled: true,
        },
    };
    provider.update_time_to_live(enable_request).await.unwrap();
}

fn stream_id_from_u64(value: u64) -> StreamItemId {
    let mut bytes = [0u8; 12];
    bytes[4..].copy_from_slice(&value.to_be_bytes());
    StreamItemId::from(bytes)
}

fn build_pointer_stream_item(
    stream_item_id: StreamItemId,
    created_at: TimestampMillis,
    table_name: &TableName,
    item_stream: StreamName,
) -> StreamItem {
    let stored_pointer =
        StoredStreamPointer::pointer(item_stream, table_name.clone(), stream_item_id.into());
    StreamItem {
        id: stream_item_id,
        stream_name: None,
        data: storage_types::storage_serde::to_bytes(&stored_pointer).expect("pointer bytes"),
        data_type: StreamDataType::StreamPointer,
        created_at,
    }
}

fn build_item_stream_item(
    stream_item_id: StreamItemId,
    created_at: TimestampMillis,
    stream_name: StreamName,
    item: &HashMap<String, AttributeValue>,
) -> StreamItem {
    StreamItem {
        id: stream_item_id,
        stream_name: Some(stream_name),
        data: storage_types::storage_serde::to_bytes(item).expect("item bytes"),
        data_type: StreamDataType::DynamoDbJson,
        created_at,
    }
}

async fn insert_stream_item(
    provider: &SQLiteStorageProvider,
    stream_name: &StreamName,
    stream_item: &StreamItem,
) {
    let stream_name = stream_name.clone();
    let stream_item_id = stream_item.id;
    let data = stream_item.data.clone();
    let created_at = stream_item.created_at;
    let data_type = stream_item.data_type;

    provider
        .connection
        .call_unwrap(move |conn| {
            let (sql, params) = sql_statements::insert_stream_entry(
                &stream_name,
                &stream_item_id,
                data.as_slice(),
                &created_at,
                data_type,
            );
            conn.execute(sql, params).map(|_| ())
        })
        .await
        .unwrap();
}

async fn force_all_stream_items_created_at(
    provider: &SQLiteStorageProvider,
    created_at: TimestampMillis,
) {
    provider
        .connection
        .call_unwrap(move |conn| {
            conn.execute("UPDATE sys_stream_items SET created_at = ?1", [*created_at])
                .map(|_| ())
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn stream_trim_removes_expired_entries_sqlite() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let table_name = TableName::new("StreamTrimSqlite");
    create_stream_test_table_named(&provider, &table_name).await;

    let item_key = ItemKey::table_key(
        table_name.clone(),
        AttributeValue::S("pk1".to_string()),
        Some(AttributeValue::S("sk1".to_string())),
    );
    let item_stream = StreamName::table_item_stream(&table_name, &item_key).expect("item stream");

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("pk1".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("sk1".to_string()));
    item.insert("data".to_string(), AttributeValue::S("payload".to_string()));

    let cutoff = TimestampMillis::now()
        - (constants::STREAM_TRIM_RETENTION_HOURS * constants::MILLIS_PER_HOUR);
    let old_created_at = cutoff - 1_000;
    let new_created_at = cutoff + 1_000;

    let old_id = stream_id_from_u64(1);
    let new_id = stream_id_from_u64(2);

    let old_pointer =
        build_pointer_stream_item(old_id, old_created_at, &table_name, item_stream.clone());
    let old_item = build_item_stream_item(old_id, old_created_at, item_stream.clone(), &item);
    let new_pointer =
        build_pointer_stream_item(new_id, new_created_at, &table_name, item_stream.clone());
    let new_item = build_item_stream_item(new_id, new_created_at, item_stream.clone(), &item);

    insert_stream_item(&provider, &StreamName::system_table_stream(), &old_pointer).await;
    insert_stream_item(
        &provider,
        &StreamName::table_stream(&table_name),
        &old_pointer,
    )
    .await;
    insert_stream_item(&provider, &item_stream, &old_item).await;

    insert_stream_item(&provider, &StreamName::system_table_stream(), &new_pointer).await;
    insert_stream_item(
        &provider,
        &StreamName::table_stream(&table_name),
        &new_pointer,
    )
    .await;
    insert_stream_item(&provider, &item_stream, &new_item).await;

    provider
        .run_job(storage_common::STREAM_TRIM_JOB)
        .await
        .unwrap();

    let sys_page =
        StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
            .await
            .unwrap();
    assert_eq!(sys_page.items.len(), 1);
    assert_eq!(sys_page.items[0].id, new_id);

    let table_page =
        StreamProvider::read_forward(&provider, StreamName::table_stream(&table_name), None, 10)
            .await
            .unwrap();
    assert_eq!(table_page.items.len(), 1);
    assert_eq!(table_page.items[0].id, new_id);

    let item_page = StreamProvider::read_forward(&provider, item_stream.clone(), None, 10)
        .await
        .unwrap();
    assert_eq!(item_page.items.len(), 1);
    assert_eq!(item_page.items[0].id, new_id);
}

#[tokio::test]
async fn stream_trim_does_not_reset_item_high_watermark_sqlite() {
    let (provider, table_name) = create_stream_test_table().await;

    let mut first = HashMap::new();
    first.insert("pk".to_string(), AttributeValue::S("pk1".to_string()));
    first.insert("sk".to_string(), AttributeValue::S("sk1".to_string()));
    first.insert("data".to_string(), AttributeValue::S("first".to_string()));
    provider
        .put_item(table_name.clone(), first, None, None, None, None)
        .await
        .unwrap();

    let mut second = HashMap::new();
    second.insert("pk".to_string(), AttributeValue::S("pk1".to_string()));
    second.insert("sk".to_string(), AttributeValue::S("sk1".to_string()));
    second.insert("data".to_string(), AttributeValue::S("second".to_string()));
    provider
        .put_item(table_name.clone(), second, None, None, None, None)
        .await
        .unwrap();

    let cutoff = TimestampMillis::now()
        - (constants::STREAM_TRIM_RETENTION_HOURS * constants::MILLIS_PER_HOUR);
    force_all_stream_items_created_at(&provider, cutoff - 1_000).await;

    provider
        .run_job(storage_common::STREAM_TRIM_JOB)
        .await
        .unwrap();

    let after_trim =
        StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
            .await
            .unwrap();
    assert!(after_trim.items.is_empty());

    let mut third = HashMap::new();
    third.insert("pk".to_string(), AttributeValue::S("pk1".to_string()));
    third.insert("sk".to_string(), AttributeValue::S("sk1".to_string()));
    third.insert("data".to_string(), AttributeValue::S("third".to_string()));
    provider
        .put_item(table_name, third, None, None, None, None)
        .await
        .unwrap();

    let after_write =
        StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
            .await
            .unwrap();
    assert_eq!(after_write.items.len(), 1);
    let pointer: StoredStreamPointer =
        storage_types::storage_serde::from_bytes(&after_write.items[0].data).unwrap();
    assert_eq!(
        pointer.target_item_stream_version(),
        storage_types::ItemStreamVersion::new(3)
    );
}

#[tokio::test]
async fn stream_trim_keeps_recent_entries_sqlite() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let table_name = TableName::new("StreamTrimRecentSqlite");
    create_stream_test_table_named(&provider, &table_name).await;

    let item_key = ItemKey::table_key(
        table_name.clone(),
        AttributeValue::S("pk1".to_string()),
        Some(AttributeValue::S("sk1".to_string())),
    );
    let item_stream = StreamName::table_item_stream(&table_name, &item_key).expect("item stream");

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("pk1".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("sk1".to_string()));
    item.insert("data".to_string(), AttributeValue::S("payload".to_string()));

    let cutoff = TimestampMillis::now()
        - (constants::STREAM_TRIM_RETENTION_HOURS * constants::MILLIS_PER_HOUR);
    let created_at = cutoff + 60_000;
    let stream_id = stream_id_from_u64(10);

    let pointer =
        build_pointer_stream_item(stream_id, created_at, &table_name, item_stream.clone());
    let item_entry = build_item_stream_item(stream_id, created_at, item_stream.clone(), &item);

    insert_stream_item(&provider, &StreamName::system_table_stream(), &pointer).await;
    insert_stream_item(&provider, &StreamName::table_stream(&table_name), &pointer).await;
    insert_stream_item(&provider, &item_stream, &item_entry).await;

    provider
        .run_job(storage_common::STREAM_TRIM_JOB)
        .await
        .unwrap();

    let sys_page =
        StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
            .await
            .unwrap();
    assert_eq!(sys_page.items.len(), 1);
    assert_eq!(sys_page.items[0].id, stream_id);
}

#[tokio::test]
async fn stream_trim_respects_replication_checkpoint_floor_sqlite() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let table_name = TableName::new("StreamTrimReplicationFloorSqlite");
    create_stream_test_table_named(&provider, &table_name).await;
    create_multi_region_control_table(&provider).await;

    let item_key = ItemKey::table_key(
        table_name.clone(),
        AttributeValue::S("pk1".to_string()),
        Some(AttributeValue::S("sk1".to_string())),
    );
    let item_stream = StreamName::table_item_stream(&table_name, &item_key).expect("item stream");

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("pk1".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("sk1".to_string()));
    item.insert("data".to_string(), AttributeValue::S("payload".to_string()));

    let cutoff = TimestampMillis::now()
        - (constants::STREAM_TRIM_RETENTION_HOURS * constants::MILLIS_PER_HOUR);
    let old_created_at = cutoff - 5_000;
    let protected_created_at = cutoff - 4_000;

    let old_id = stream_id_from_u64(20);
    let protected_id = stream_id_from_u64(21);

    let old_pointer =
        build_pointer_stream_item(old_id, old_created_at, &table_name, item_stream.clone());
    let old_item = build_item_stream_item(old_id, old_created_at, item_stream.clone(), &item);
    let protected_pointer = build_pointer_stream_item(
        protected_id,
        protected_created_at,
        &table_name,
        item_stream.clone(),
    );
    let protected_item = build_item_stream_item(
        protected_id,
        protected_created_at,
        item_stream.clone(),
        &item,
    );

    insert_stream_item(&provider, &StreamName::system_table_stream(), &old_pointer).await;
    insert_stream_item(
        &provider,
        &StreamName::table_stream(&table_name),
        &old_pointer,
    )
    .await;
    insert_stream_item(&provider, &item_stream, &old_item).await;

    insert_stream_item(
        &provider,
        &StreamName::system_table_stream(),
        &protected_pointer,
    )
    .await;
    insert_stream_item(
        &provider,
        &StreamName::table_stream(&table_name),
        &protected_pointer,
    )
    .await;
    insert_stream_item(&provider, &item_stream, &protected_item).await;

    provider
        .put_item(
            TableName::new("sys_storage_replication"),
            HashMap::from([
                (
                    "pk".to_string(),
                    AttributeValue::S("catchup#learner-1".to_string()),
                ),
                ("sk".to_string(), AttributeValue::S("session".to_string())),
                (
                    "payload".to_string(),
                    AttributeValue::S(
                        serde_json::json!({
                            "protected_stream_cursor": protected_id,
                            "updated_at": TimestampMillis::now(),
                        })
                        .to_string(),
                    ),
                ),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    provider
        .run_job(storage_common::STREAM_TRIM_JOB)
        .await
        .unwrap();

    let sys_page =
        StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
            .await
            .unwrap();
    assert_eq!(sys_page.items.len(), 1);
    assert_eq!(sys_page.items[0].id, protected_id);

    let table_page =
        StreamProvider::read_forward(&provider, StreamName::table_stream(&table_name), None, 10)
            .await
            .unwrap();
    assert_eq!(table_page.items.len(), 1);
    assert_eq!(table_page.items[0].id, protected_id);

    let item_page = StreamProvider::read_forward(&provider, item_stream.clone(), None, 10)
        .await
        .unwrap();
    assert_eq!(item_page.items.len(), 1);
    assert_eq!(item_page.items[0].id, protected_id);
}

#[tokio::test]
async fn stream_trim_fails_closed_for_malformed_active_session_sqlite() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let table_name = TableName::new("StreamTrimMalformedSessionSqlite");
    create_stream_test_table_named(&provider, &table_name).await;
    create_multi_region_control_table(&provider).await;

    let item_key = ItemKey::table_key(
        table_name.clone(),
        AttributeValue::S("pk1".to_string()),
        Some(AttributeValue::S("sk1".to_string())),
    );
    let item_stream = StreamName::table_item_stream(&table_name, &item_key).expect("item stream");

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("pk1".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("sk1".to_string()));
    item.insert("data".to_string(), AttributeValue::S("payload".to_string()));

    let cutoff = TimestampMillis::now()
        - (constants::STREAM_TRIM_RETENTION_HOURS * constants::MILLIS_PER_HOUR);
    let stream_id = stream_id_from_u64(22);
    let pointer =
        build_pointer_stream_item(stream_id, cutoff - 5_000, &table_name, item_stream.clone());
    let item_entry = build_item_stream_item(stream_id, cutoff - 5_000, item_stream.clone(), &item);

    insert_stream_item(&provider, &StreamName::system_table_stream(), &pointer).await;
    insert_stream_item(&provider, &StreamName::table_stream(&table_name), &pointer).await;
    insert_stream_item(&provider, &item_stream, &item_entry).await;

    provider
        .put_item(
            TableName::new("sys_storage_replication"),
            HashMap::from([
                (
                    "pk".to_string(),
                    AttributeValue::S("bootstrap#region-b".to_string()),
                ),
                ("sk".to_string(), AttributeValue::S("session".to_string())),
                (
                    "payload".to_string(),
                    AttributeValue::S(r#"{"updated_at": 1}"#.to_string()),
                ),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    provider
        .run_job(storage_common::STREAM_TRIM_JOB)
        .await
        .expect_err("malformed active session must fail closed");

    let sys_page =
        StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
            .await
            .unwrap();
    assert_eq!(sys_page.items.len(), 1);
    assert_eq!(sys_page.items[0].id, stream_id);
}

#[tokio::test]
async fn stream_trim_missing_table_stream_entry_sqlite() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let table_name = TableName::new("StreamTrimMissingTableStreamSqlite");
    create_stream_test_table_named(&provider, &table_name).await;

    let item_key = ItemKey::table_key(
        table_name.clone(),
        AttributeValue::S("pk1".to_string()),
        Some(AttributeValue::S("sk1".to_string())),
    );
    let item_stream = StreamName::table_item_stream(&table_name, &item_key).expect("item stream");

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("pk1".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("sk1".to_string()));
    item.insert("data".to_string(), AttributeValue::S("payload".to_string()));

    let cutoff = TimestampMillis::now()
        - (constants::STREAM_TRIM_RETENTION_HOURS * constants::MILLIS_PER_HOUR);
    let created_at = cutoff - 1_000;
    let stream_id = stream_id_from_u64(11);

    let pointer =
        build_pointer_stream_item(stream_id, created_at, &table_name, item_stream.clone());
    let item_entry = build_item_stream_item(stream_id, created_at, item_stream.clone(), &item);

    insert_stream_item(&provider, &StreamName::system_table_stream(), &pointer).await;
    insert_stream_item(&provider, &item_stream, &item_entry).await;

    provider
        .run_job(storage_common::STREAM_TRIM_JOB)
        .await
        .unwrap();

    let sys_page =
        StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
            .await
            .unwrap();
    assert!(sys_page.items.is_empty());

    let table_page =
        StreamProvider::read_forward(&provider, StreamName::table_stream(&table_name), None, 10)
            .await
            .unwrap();
    assert!(table_page.items.is_empty());

    let item_page = StreamProvider::read_forward(&provider, item_stream.clone(), None, 10)
        .await
        .unwrap();
    assert!(item_page.items.is_empty());
}

#[tokio::test]
async fn stream_trim_missing_item_stream_entry_sqlite() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let table_name = TableName::new("StreamTrimMissingItemStreamSqlite");
    create_stream_test_table_named(&provider, &table_name).await;

    let item_key = ItemKey::table_key(
        table_name.clone(),
        AttributeValue::S("pk1".to_string()),
        Some(AttributeValue::S("sk1".to_string())),
    );
    let item_stream = StreamName::table_item_stream(&table_name, &item_key).expect("item stream");

    let cutoff = TimestampMillis::now()
        - (constants::STREAM_TRIM_RETENTION_HOURS * constants::MILLIS_PER_HOUR);
    let created_at = cutoff - 1_000;
    let stream_id = stream_id_from_u64(12);

    let pointer =
        build_pointer_stream_item(stream_id, created_at, &table_name, item_stream.clone());

    insert_stream_item(&provider, &StreamName::system_table_stream(), &pointer).await;
    insert_stream_item(&provider, &StreamName::table_stream(&table_name), &pointer).await;

    provider
        .run_job(storage_common::STREAM_TRIM_JOB)
        .await
        .unwrap();

    let sys_page =
        StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
            .await
            .unwrap();
    assert!(sys_page.items.is_empty());

    let table_page =
        StreamProvider::read_forward(&provider, StreamName::table_stream(&table_name), None, 10)
            .await
            .unwrap();
    assert!(table_page.items.is_empty());

    let item_page = StreamProvider::read_forward(&provider, item_stream.clone(), None, 10)
        .await
        .unwrap();
    assert!(item_page.items.is_empty());
}

#[tokio::test]
async fn stream_trim_handles_multiple_batches_sqlite() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let table_name = TableName::new("StreamTrimMultiBatchSqlite");
    create_stream_test_table_named(&provider, &table_name).await;

    let item_key = ItemKey::table_key(
        table_name.clone(),
        AttributeValue::S("pk1".to_string()),
        Some(AttributeValue::S("sk1".to_string())),
    );
    let item_stream = StreamName::table_item_stream(&table_name, &item_key).expect("item stream");

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("pk1".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("sk1".to_string()));
    item.insert("data".to_string(), AttributeValue::S("payload".to_string()));

    let cutoff = TimestampMillis::now()
        - (constants::STREAM_TRIM_RETENTION_HOURS * constants::MILLIS_PER_HOUR);
    let total = constants::STREAM_TRIM_DELETE_BATCH_SIZE * 3 + 1;

    for idx in 0..total {
        let stream_id = stream_id_from_u64(u64::try_from(idx + 1).unwrap());
        let created_at = cutoff - 1_000 - i64::try_from(idx).unwrap();
        let pointer =
            build_pointer_stream_item(stream_id, created_at, &table_name, item_stream.clone());
        let item_entry = build_item_stream_item(stream_id, created_at, item_stream.clone(), &item);

        insert_stream_item(&provider, &StreamName::system_table_stream(), &pointer).await;
        insert_stream_item(&provider, &StreamName::table_stream(&table_name), &pointer).await;
        insert_stream_item(&provider, &item_stream, &item_entry).await;
    }

    provider
        .run_job(storage_common::STREAM_TRIM_JOB)
        .await
        .unwrap();

    let sys_page = StreamProvider::read_forward(
        &provider,
        StreamName::system_table_stream(),
        None,
        constants::STREAM_TRIM_READ_LIMIT,
    )
    .await
    .unwrap();
    assert!(sys_page.items.is_empty());

    let table_page = StreamProvider::read_forward(
        &provider,
        StreamName::table_stream(&table_name),
        None,
        constants::STREAM_TRIM_READ_LIMIT,
    )
    .await
    .unwrap();
    assert!(table_page.items.is_empty());

    let item_page = StreamProvider::read_forward(
        &provider,
        item_stream.clone(),
        None,
        constants::STREAM_TRIM_READ_LIMIT,
    )
    .await
    .unwrap();
    assert!(item_page.items.is_empty());
}

#[tokio::test]
async fn stream_trim_skips_invalid_pointer_sqlite() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let cutoff = TimestampMillis::now()
        - (constants::STREAM_TRIM_RETENTION_HOURS * constants::MILLIS_PER_HOUR);
    let created_at = cutoff - 1_000;
    let stream_id = stream_id_from_u64(3);

    let invalid_pointer = StreamItem {
        id: stream_id,
        stream_name: None,
        data: b"invalid".to_vec(),
        data_type: StreamDataType::StreamPointer,
        created_at,
    };

    insert_stream_item(
        &provider,
        &StreamName::system_table_stream(),
        &invalid_pointer,
    )
    .await;

    provider
        .run_job(storage_common::STREAM_TRIM_JOB)
        .await
        .unwrap();

    let sys_page =
        StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
            .await
            .unwrap();
    assert_eq!(sys_page.items.len(), 1);
    assert_eq!(sys_page.items[0].id, stream_id);
}
