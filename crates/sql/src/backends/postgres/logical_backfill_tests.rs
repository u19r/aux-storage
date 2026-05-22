use std::{collections::HashMap, time::Instant};

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
    KeyType, TableName, TimeToLiveSpecification, TimeToLiveStatus, TimestampMillis,
    UpdateTimeToLiveRequest,
};

use super::PostgresStorageProvider;

#[tokio::test]
async fn postgres_logical_item_export_import_preserves_target_revision() {
    let Some(source) = initialized_provider().await else {
        return;
    };
    let Some(destination) = initialized_provider().await else {
        return;
    };
    let table_name = unique_table_name("sync_pg_logical_items");
    source
        .create_table(&basic_create_table_request(&table_name))
        .await
        .expect("create source table");
    destination
        .create_table(&basic_create_table_request(&table_name))
        .await
        .expect("create destination table");
    apply_sync_put(&source, &table_name, "item#1", "open", 17).await;

    let page = source
        .export_logical_page(export_request(
            LogicalBackfillDomain::ItemRecords,
            Some(&table_name),
        ))
        .await
        .expect("export item records");
    assert_eq!(page.records.len(), 1);

    import_records(
        &destination,
        &logical_manifest(vec![LogicalBackfillDomain::ItemRecords]),
        LogicalBackfillDomain::ItemRecords,
        page.records,
    )
    .await;
    assert_present_revision(&destination, &table_name, "item#1", 17).await;

    source
        .delete_table(&table_name)
        .await
        .expect("drop source table");
    destination
        .delete_table(&table_name)
        .await
        .expect("drop destination table");
}

#[tokio::test]
async fn postgres_logical_durable_revision_export_import_preserves_revision() {
    let Some(source) = initialized_provider().await else {
        return;
    };
    let Some(destination) = initialized_provider().await else {
        return;
    };
    let table_name = unique_table_name("sync_pg_logical_revisions");
    source
        .create_table(&basic_create_table_request(&table_name))
        .await
        .expect("create source table");
    destination
        .create_table(&basic_create_table_request(&table_name))
        .await
        .expect("create destination table");
    apply_sync_put(&source, &table_name, "item#1", "open", 23).await;
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

    assert_present_revision(&destination, &table_name, "item#1", 23).await;

    source
        .delete_table(&table_name)
        .await
        .expect("drop source table");
    destination
        .delete_table(&table_name)
        .await
        .expect("drop destination table");
}

#[tokio::test]
async fn postgres_logical_metadata_and_ttl_import_create_usable_state() {
    let Some(source) = initialized_provider().await else {
        return;
    };
    let Some(destination) = initialized_provider().await else {
        return;
    };
    let table_name = unique_table_name("sync_pg_logical_metadata");
    source
        .create_table(&basic_create_table_request(&table_name))
        .await
        .expect("create source table");
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

    let metadata_page = source
        .export_logical_page(export_request(
            LogicalBackfillDomain::TableMetadata,
            Some(&table_name),
        ))
        .await
        .expect("export table metadata");
    assert_eq!(metadata_page.records.len(), 1);
    import_records(
        &destination,
        &logical_manifest(vec![LogicalBackfillDomain::TableMetadata]),
        LogicalBackfillDomain::TableMetadata,
        metadata_page.records,
    )
    .await;
    destination
        .put_item(
            table_name.clone(),
            item_map("item#1", "created"),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("imported metadata should create physical table storage");

    let ttl_page = source
        .export_logical_page(export_request(
            LogicalBackfillDomain::TtlRecords,
            Some(&table_name),
        ))
        .await
        .expect("export ttl records");
    assert_eq!(ttl_page.records.len(), 1);
    import_records(
        &destination,
        &logical_manifest(vec![LogicalBackfillDomain::TtlRecords]),
        LogicalBackfillDomain::TtlRecords,
        ttl_page.records,
    )
    .await;
    let ttl = destination
        .describe_time_to_live(&table_name)
        .await
        .expect("describe imported ttl")
        .time_to_live_description
        .expect("ttl description should exist");
    assert_eq!(ttl.attribute_name.as_deref(), Some("ttl"));
    assert_eq!(ttl.time_to_live_status, TimeToLiveStatus::Enabling);

    source
        .delete_table(&table_name)
        .await
        .expect("drop source table");
    destination
        .delete_table(&table_name)
        .await
        .expect("drop destination table");
}

#[tokio::test]
async fn postgres_logical_empty_domains_export_without_unsupported_errors() {
    let Some(provider) = initialized_provider().await else {
        return;
    };

    for domain in [
        LogicalBackfillDomain::Tombstones,
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
async fn postgres_resolved_sync_apply_is_idempotent_and_sets_target_revision() {
    let Some(dsn) = postgres_test_dsn() else {
        return;
    };
    let provider = PostgresStorageProvider::new(&dsn, 8)
        .await
        .expect("create postgres provider");
    provider
        .initialize_storage()
        .await
        .expect("initialize postgres storage");

    let table_name_value = format!("sync_pg_{}", TimestampMillis::now().timestamp_millis());
    let table_name = TableName::new(&table_name_value);
    provider
        .create_table(&basic_create_table_request(&table_name))
        .await
        .expect("create table");

    let key_json = r#"{"pk":{"S":"item#1"}}"#.to_string();
    let item_json = r#"{"pk":{"S":"item#1"},"status":{"S":"open"}}"#.to_string();
    let put = ResolvedSyncMutation::Put(SyncPutMutation {
        mutation_id: SyncMutationId::new("mutation-1").expect("mutation id"),
        table_name: table_name.clone(),
        key_json: key_json.clone(),
        item_json,
        old_item_json: None,
        target_item_stream_version: ItemStreamVersion::new(11),
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
        target_item_stream_version: ItemStreamVersion::new(12),
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
    let DurablePointReadProof::Present { revision, .. } = proof else {
        panic!("sync-applied item should be present");
    };
    assert_eq!(
        revision.as_bytes(),
        &ItemStreamVersion::new(11).to_be_bytes()
    );
    let delete_key = serde_json::from_str::<HashMap<String, AttributeValue>>(&delete_key_json)
        .expect("decode delete key");
    let delete_proof = provider
        .get_item_with_durable_proof(DurablePointReadRequest {
            table_name: table_name.clone(),
            key: KeyAttributes::from(delete_key),
            consistent_read: true,
        })
        .await
        .expect("durable delete proof");
    let DurablePointReadProof::Absent { proof } = delete_proof else {
        panic!("sync-deleted item should be absent");
    };
    assert_eq!(proof.as_bytes(), &ItemStreamVersion::new(12).to_be_bytes());

    provider
        .delete_table(&table_name)
        .await
        .expect("drop table");
}

#[tokio::test]
async fn postgres_resolved_sync_apply_grouped_vs_separate_runtime_tests() {
    let Some(provider) = initialized_provider().await else {
        return;
    };

    let separate_table = unique_table_name("sync_pg_apply_separate");
    let grouped_table = unique_table_name("sync_pg_apply_grouped");
    provider
        .create_table(&basic_create_table_request(&separate_table))
        .await
        .expect("create separate table");
    provider
        .create_table(&basic_create_table_request(&grouped_table))
        .await
        .expect("create grouped table");

    let separate_started = Instant::now();
    for index in 0..8 {
        provider
            .apply_resolved_sync_mutations(
                commit_metadata(11, index + 1),
                ResolvedSyncMutationBatch::new(vec![sync_put(
                    &separate_table,
                    &format!("separate-mutation-{index}"),
                    &format!("separate-item-{index}"),
                    "open",
                    index + 1,
                )]),
            )
            .await
            .expect("apply separate sync put");
    }
    let separate_elapsed = separate_started.elapsed();

    let grouped_started = Instant::now();
    provider
        .apply_resolved_sync_mutations(
            commit_metadata(12, 1),
            ResolvedSyncMutationBatch::new(
                (0..8)
                    .map(|index| {
                        sync_put(
                            &grouped_table,
                            &format!("grouped-mutation-{index}"),
                            &format!("grouped-item-{index}"),
                            "open",
                            index + 1,
                        )
                    })
                    .collect(),
            ),
        )
        .await
        .expect("apply grouped sync puts");
    let grouped_elapsed = grouped_started.elapsed();

    emit_runtime_report(
        module_path!(),
        "postgres_resolved_sync_apply_grouped_vs_separate_runtime_tests",
        "separate_puts_8x1",
        8,
        separate_elapsed,
    );
    emit_runtime_report(
        module_path!(),
        "postgres_resolved_sync_apply_grouped_vs_separate_runtime_tests",
        "grouped_puts_1x8",
        8,
        grouped_elapsed,
    );
    assert!(
        grouped_elapsed < separate_elapsed,
        "grouped apply should beat separate applies: grouped={grouped_elapsed:?} \
         separate={separate_elapsed:?}"
    );

    provider
        .delete_table(&separate_table)
        .await
        .expect("drop separate table");
    provider
        .delete_table(&grouped_table)
        .await
        .expect("drop grouped table");
}

fn postgres_test_dsn() -> Option<String> {
    std::env::var("TEST_POSTGRES_DSN")
        .ok()
        .or_else(|| std::env::var("CUCUMBER_POSTGRES_DSN").ok())
}

async fn initialized_provider() -> Option<PostgresStorageProvider> {
    let dsn = postgres_test_dsn()?;
    let provider = PostgresStorageProvider::new(&dsn, 8)
        .await
        .expect("create postgres provider");
    provider
        .initialize_storage()
        .await
        .expect("initialize postgres storage");
    Some(provider)
}

async fn apply_sync_put(
    provider: &PostgresStorageProvider,
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

fn sync_put(
    table_name: &TableName,
    mutation_id: &str,
    pk: &str,
    status: &str,
    version: u64,
) -> ResolvedSyncMutation {
    ResolvedSyncMutation::Put(SyncPutMutation {
        mutation_id: SyncMutationId::new(mutation_id).expect("mutation id"),
        table_name: table_name.clone(),
        key_json: serde_json::to_string(&key_map(pk)).expect("encode key"),
        item_json: serde_json::to_string(&item_map(pk, status)).expect("encode item"),
        old_item_json: None,
        target_item_stream_version: ItemStreamVersion::new(version),
        response: SyncMutationResponse {
            response_json: Some(format!(r#"{{"mutation":"{mutation_id}"}}"#)),
        },
    })
}

fn commit_metadata(term: u64, index: u64) -> SyncCommitMetadata {
    SyncCommitMetadata {
        log_id: SyncLogId::new(term, index),
        committed_at: TimestampMillis::from_timestamp(1_700_000_000_000),
        leader_node_id: "node-1".to_string(),
    }
}

async fn assert_present_revision(
    provider: &PostgresStorageProvider,
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
    provider: &PostgresStorageProvider,
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
        "postgres",
        "postgres",
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

fn unique_table_name(prefix: &str) -> TableName {
    TableName::new(&format!(
        "{prefix}_{}",
        TimestampMillis::now().timestamp_millis()
    ))
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
