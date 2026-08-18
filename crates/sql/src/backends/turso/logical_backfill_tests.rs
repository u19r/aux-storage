use std::{collections::HashMap, time::Instant};

use alloc_counter::AllocationGuard;
use storage_backfill::{
    LogicalBackfillChecksum, LogicalBackfillChunk, LogicalBackfillChunkId,
    LogicalBackfillChunkSummary, LogicalBackfillDomain, LogicalBackfillExport, LogicalBackfillId,
    LogicalBackfillImport, LogicalBackfillManifest, LogicalBackfillRecord, LogicalExportRequest,
    SyncLearnerCatchupPolicy,
};
use storage_common::provider_perf::emit_runtime_report;
use storage_provider::StorageProvider;
use storage_sync::{
    ResolvedSyncLogEntry, ResolvedSyncMutation, ResolvedSyncMutationBatch, SyncCommitMetadata,
    SyncDeleteMutation, SyncLogId, SyncMutationId, SyncMutationResponse, SyncPutMutation,
};
use storage_types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableRequest, DurablePointReadProof,
    DurablePointReadRequest, ItemStreamVersion, KeyAttributeType, KeyAttributes, KeySchemaElement,
    KeyType, ReplicationEventMetadata, ReplicationHybridLogicalClock, ReplicationMutation,
    ReplicationWriteSource, StreamItemId, StreamName, StreamSpecification, StreamViewType,
    TableName, TimestampMillis,
};
use stream_provider::{StoredStreamPointer, StreamProvider};

use super::TursoStorageProvider;

const SYNC_APPLY_ALLOC_BATCHES: usize = 8;
const SYNC_APPLY_ALLOC_BATCH_SIZE: usize = 8;

#[tokio::test]
async fn given_replication_put_when_streamed_then_indexer_declaration_is_preserved() {
    let provider = initialized_provider().await;
    let table_name = TableName::new("turso_replication_indexers");
    let mut create = basic_create_table_request(&table_name).with_stream_specification(Some(
        StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewAndOldImages),
        },
    ));
    create.max_indexers = storage_types::MaxIndexers::try_new(1).expect("valid capacity");
    provider.create_table(&create).await.expect("create table");

    let metadata = ReplicationEventMetadata {
        origin_region: "eu-west-1".to_string(),
        origin_sequence: StreamItemId::from([1_u8; 12]),
        origin_hlc: ReplicationHybridLogicalClock {
            physical_ms: TimestampMillis::from_timestamp(1_700_000_000_000),
            logical: 1,
        },
        origin_commit_ts: TimestampMillis::from_timestamp(1_700_000_000_000),
        table_replica_epoch: 1,
        write_source: ReplicationWriteSource::Replicated,
    };
    provider
        .apply_replication_mutation(ReplicationMutation {
            table_name: table_name.clone(),
            key: HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))])
                .into(),
            new_image: Some(HashMap::from([
                ("pk".to_string(), AttributeValue::S("item#1".to_string())),
                ("status".to_string(), AttributeValue::S("open".to_string())),
            ])),
            new_indexers: Some(vec!["status".to_string()]),
            old_image: None,
            old_indexers: None,
            metadata,
        })
        .await
        .expect("apply mutation");

    let page = provider
        .read_forward(StreamName::table_stream(&table_name), None, 1)
        .await
        .expect("read stream");
    let pointer: StoredStreamPointer =
        storage_types::storage_serde::from_bytes(&page.items[0].data).expect("decode pointer");
    assert_eq!(pointer.indexers(), ["status"]);
}

#[tokio::test]
async fn given_indexed_item_when_logically_copied_then_declaration_and_missing_slot_survive() {
    let source = initialized_provider().await;
    let destination = initialized_provider().await;
    let table_name = TableName::new("turso_logical_indexers");
    let mut create = basic_create_table_request(&table_name);
    create.max_indexers = storage_types::MaxIndexers::try_new(2).expect("valid capacity");
    source
        .create_table(&create)
        .await
        .expect("create source table");
    destination
        .create_table(&create)
        .await
        .expect("create destination table");

    let mut put =
        storage_types::PutItemRequest::new(table_name.clone(), item_map("item#1", "open"));
    put.indexers = Some(vec!["status".to_string(), "missing".to_string()]);
    source
        .put_item_request(put)
        .await
        .expect("put indexed item");
    let page = source
        .export_logical_page(export_request(
            LogicalBackfillDomain::ItemRecords,
            Some(&table_name),
        ))
        .await
        .expect("export item");
    import_records(
        &destination,
        &logical_manifest(vec![LogicalBackfillDomain::ItemRecords]),
        LogicalBackfillDomain::ItemRecords,
        page.records,
    )
    .await;

    let DurablePointReadProof::Present { item, indexers, .. } = destination
        .get_item_with_durable_proof(DurablePointReadRequest {
            table_name,
            key: KeyAttributes::from(key_map("item#1")),
            consistent_read: true,
        })
        .await
        .expect("read imported item")
    else {
        panic!("imported item should exist");
    };
    assert_eq!(indexers, ["status", "missing"]);
    assert_eq!(
        item.into_attribute_map().expect("decode item"),
        item_map("item#1", "open")
    );
}

#[tokio::test]
async fn turso_logical_empty_domains_export_without_unsupported_errors() {
    let provider = initialized_provider().await;

    for domain in [
        LogicalBackfillDomain::Tombstones,
        LogicalBackfillDomain::TtlRecords,
        LogicalBackfillDomain::StorageControlPlane,
        LogicalBackfillDomain::BackgroundJobs,
        LogicalBackfillDomain::SyncControlPlane,
    ] {
        let page = provider
            .export_logical_page(LogicalExportRequest {
                manifest_id: LogicalBackfillId::new("manifest").expect("manifest id"),
                domain,
                table_name: None,
                cursor: None,
                limit: 10,
            })
            .await
            .expect("empty domain export should not be unsupported");
        assert_eq!(page.domain, domain);
        assert!(page.records.is_empty());
    }
}

#[tokio::test]
async fn turso_resolved_sync_apply_is_idempotent_and_sets_target_revision() {
    let provider = TursoStorageProvider::new(":memory:")
        .await
        .expect("create turso provider");
    provider
        .initialize_storage()
        .await
        .expect("initialize turso storage");
    provider
        .initialize_stream()
        .await
        .expect("initialize turso streams");

    let table_name = TableName::new("sync_turso_items");
    let mut create = basic_create_table_request(&table_name);
    create.max_indexers = storage_types::MaxIndexers::try_new(1).expect("valid capacity");
    provider.create_table(&create).await.expect("create table");

    let key_json = r#"{"pk":{"S":"item#1"}}"#.to_string();
    let item_json = r#"{"pk":{"S":"item#1"},"status":{"S":"open"}}"#.to_string();
    let put = ResolvedSyncMutation::Put(SyncPutMutation {
        mutation_id: SyncMutationId::new("mutation-1").expect("mutation id"),
        table_name: table_name.clone(),
        key_json: key_json.clone(),
        item_json,
        indexers: vec!["status".to_string()],
        old_item_json: None,
        old_indexers: None,
        target_item_stream_version: ItemStreamVersion::new(7),
        response: SyncMutationResponse {
            response_json: Some(r#"{"ok":true}"#.to_string()),
        },
    });
    let delete_key_json = r#"{"pk":{"S":"item#2"}}"#.to_string();
    let delete = ResolvedSyncMutation::Delete(SyncDeleteMutation {
        mutation_id: SyncMutationId::new("mutation-2").expect("mutation id"),
        table_name: table_name.clone(),
        key_json: delete_key_json.clone(),
        old_item_json: Some(r#"{"pk":{"S":"item#2"},"status":{"S":"closed"}}"#.to_string()),
        old_indexers: None,
        target_item_stream_version: ItemStreamVersion::new(8),
        response: SyncMutationResponse {
            response_json: Some(r#"{"deleted":true}"#.to_string()),
        },
    });
    let metadata = SyncCommitMetadata {
        log_id: SyncLogId::new(3, 9),
        committed_at: TimestampMillis::from_timestamp(1_700_000_000_000),
        leader_node_id: "node-1".to_string(),
    };
    let batch = ResolvedSyncMutationBatch::new(vec![put, delete]);

    provider
        .persist_resolved_sync_log_entry(&metadata, &batch)
        .await
        .expect("persist sync log entry");
    let first_response = provider
        .apply_resolved_sync_mutations(metadata.clone(), batch.clone())
        .await
        .expect("apply first sync batch");
    let replay_response = provider
        .apply_resolved_sync_mutations(metadata.clone(), batch.clone())
        .await
        .expect("replay sync batch");

    assert_eq!(first_response, replay_response);
    assert_eq!(
        first_response[0].response_json.as_deref(),
        Some(r#"{"ok":true}"#)
    );
    assert_eq!(
        provider
            .last_resolved_sync_log_id()
            .await
            .expect("last sync log id"),
        Some(metadata.log_id)
    );
    assert_eq!(
        provider
            .get_resolved_sync_log_entry(metadata.log_id)
            .await
            .expect("lookup sync log")
            .expect("sync log entry should exist"),
        ResolvedSyncLogEntry::new(metadata.clone(), batch.clone())
    );
    assert_eq!(
        provider
            .resolved_sync_log_entries_after(Some(SyncLogId::new(3, 8)), 10)
            .await
            .expect("scan sync log"),
        vec![ResolvedSyncLogEntry::new(metadata.clone(), batch.clone())]
    );

    let key =
        serde_json::from_str::<HashMap<String, AttributeValue>>(&key_json).expect("decode key");
    let proof = provider
        .get_item_with_durable_proof(DurablePointReadRequest {
            table_name: table_name.clone(),
            key: KeyAttributes::from(key),
            consistent_read: true,
        })
        .await
        .expect("durable proof");
    let DurablePointReadProof::Present {
        revision, indexers, ..
    } = proof
    else {
        panic!("sync-applied item should be present");
    };
    assert_eq!(
        revision.as_bytes(),
        &ItemStreamVersion::new(7).to_be_bytes()
    );
    assert_eq!(indexers, ["status"]);
    let delete_key = serde_json::from_str::<HashMap<String, AttributeValue>>(&delete_key_json)
        .expect("decode delete key");
    let delete_proof = provider
        .get_item_with_durable_proof(DurablePointReadRequest {
            table_name,
            key: KeyAttributes::from(delete_key),
            consistent_read: true,
        })
        .await
        .expect("durable delete proof");
    let DurablePointReadProof::Absent { proof } = delete_proof else {
        panic!("sync-deleted item should be absent");
    };
    assert_eq!(proof.as_bytes(), &ItemStreamVersion::new(8).to_be_bytes());
}

#[tokio::test(flavor = "current_thread")]
async fn turso_resolved_sync_apply_allocation_baseline_tests() {
    let provider = initialized_provider().await;

    let table_name = TableName::new("sync_turso_apply_alloc");
    provider
        .create_table(&basic_create_table_request(&table_name))
        .await
        .expect("create table");
    let batches = sync_apply_allocation_batches(&table_name);

    let guard = AllocationGuard::start(
        module_path!(),
        "turso_resolved_sync_apply_allocation_baseline_tests",
        file!(),
        line!(),
        Some("put_batches_8x8"),
    );

    let started = Instant::now();
    let mut mutation_count = 0_usize;
    for (batch_index, batch) in batches.into_iter().enumerate() {
        mutation_count = mutation_count.saturating_add(batch.mutations.len());
        provider
            .apply_resolved_sync_mutations(
                SyncCommitMetadata {
                    log_id: SyncLogId::new(7, batch_index as u64 + 1),
                    committed_at: TimestampMillis::from_timestamp(1_700_000_000_000),
                    leader_node_id: "node-1".to_string(),
                },
                batch,
            )
            .await
            .expect("apply sync batch");
    }
    let elapsed = started.elapsed();

    let report = guard.finish();
    alloc_counter::emit_report(&report);
    emit_runtime_report(
        module_path!(),
        "turso_resolved_sync_apply_allocation_baseline_tests",
        "put_batches_8x8",
        mutation_count,
        elapsed,
    );
    assert!(report.allocation_count > 0);
    assert!(report.allocated_bytes > 0);
}

#[tokio::test]
async fn turso_logical_item_export_import_preserves_target_revision() {
    let source = initialized_provider().await;
    let destination = initialized_provider().await;
    let table_name = TableName::new("sync_turso_logical_items");
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
        .export_logical_page(LogicalExportRequest {
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
        page.records.clone(),
    )
    .await;

    assert_present_revision(&destination, &table_name, "item#1", 7).await;

    let stale = LogicalBackfillRecord::PresentItem {
        table_name: table_name.as_ref().to_string(),
        key_json: r#"{"pk":{"S":"item#1"}}"#.to_string(),
        item_json: r#"{"pk":{"S":"item#1"},"status":{"S":"stale"}}"#.to_string(),
        indexers: Vec::new(),
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
async fn turso_logical_durable_revision_export_import_preserves_revision() {
    let source = initialized_provider().await;
    let destination = initialized_provider().await;
    let table_name = TableName::new("sync_turso_logical_revisions");
    source
        .create_table(&basic_create_table_request(&table_name))
        .await
        .expect("create source table");
    destination
        .create_table(&basic_create_table_request(&table_name))
        .await
        .expect("create destination table");
    apply_sync_put(&source, &table_name, "item#1", "open", 19).await;
    destination
        .put_item(
            table_name.clone(),
            item_map("item#1", "open"),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("seed destination item");

    let page = source
        .export_logical_page(export_request(
            LogicalBackfillDomain::DurableRevisions,
            Some(&table_name),
        ))
        .await
        .expect("export durable revisions");
    assert_eq!(page.records.len(), 1);
    import_records(
        &destination,
        &logical_manifest(vec![LogicalBackfillDomain::DurableRevisions]),
        LogicalBackfillDomain::DurableRevisions,
        page.records,
    )
    .await;

    assert_present_revision(&destination, &table_name, "item#1", 19).await;
}

#[tokio::test]
async fn turso_logical_table_metadata_import_creates_physical_table() {
    let source = initialized_provider().await;
    let destination = initialized_provider().await;
    let table_name = TableName::new("sync_turso_logical_metadata");
    source
        .create_table(&basic_create_table_request(&table_name))
        .await
        .expect("create source table");

    let page = source
        .export_logical_page(LogicalExportRequest {
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

    assert!(
        destination
            .table_exists(&table_name)
            .await
            .expect("table exists query")
    );
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
        .expect("imported metadata should create physical table storage");
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

async fn initialized_provider() -> TursoStorageProvider {
    let provider = TursoStorageProvider::new(":memory:")
        .await
        .expect("create turso provider");
    provider
        .initialize_storage()
        .await
        .expect("initialize turso storage");
    provider
        .initialize_stream()
        .await
        .expect("initialize turso streams");
    provider
}

async fn apply_sync_put(
    provider: &TursoStorageProvider,
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
        indexers: Vec::new(),
        old_item_json: None,
        old_indexers: None,
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

fn sync_apply_allocation_batches(table_name: &TableName) -> Vec<ResolvedSyncMutationBatch> {
    (0..SYNC_APPLY_ALLOC_BATCHES)
        .map(|batch_index| {
            let mutations = (0..SYNC_APPLY_ALLOC_BATCH_SIZE)
                .map(|item_index| {
                    let absolute_index = batch_index * SYNC_APPLY_ALLOC_BATCH_SIZE + item_index;
                    ResolvedSyncMutation::Put(SyncPutMutation {
                        mutation_id: SyncMutationId::new(format!(
                            "alloc-mutation-{absolute_index:04}"
                        ))
                        .expect("mutation id"),
                        table_name: table_name.clone(),
                        key_json: serde_json::to_string(&key_map(&format!(
                            "alloc-item-{absolute_index:04}"
                        )))
                        .expect("encode key"),
                        item_json: serde_json::to_string(&item_map(
                            &format!("alloc-item-{absolute_index:04}"),
                            "open",
                        ))
                        .expect("encode item"),
                        indexers: Vec::new(),
                        old_item_json: None,
                        old_indexers: None,
                        target_item_stream_version: ItemStreamVersion::new(
                            absolute_index as u64 + 1,
                        ),
                        response: SyncMutationResponse {
                            response_json: Some(format!(
                                r#"{{"mutation":"alloc-mutation-{absolute_index:04}"}}"#
                            )),
                        },
                    })
                })
                .collect();
            ResolvedSyncMutationBatch::new(mutations)
        })
        .collect()
}

async fn assert_present_revision(
    provider: &TursoStorageProvider,
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
    provider: &TursoStorageProvider,
    manifest: &LogicalBackfillManifest,
    domain: LogicalBackfillDomain,
    records: Vec<LogicalBackfillRecord>,
) {
    provider
        .import_logical_chunk(
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
        "turso",
        "turso",
        domains,
    )
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
