use std::{collections::HashMap, time::Instant};

use alloc_counter::AllocationGuard;
use storage_backfill::{
    LogicalBackfillChunk, LogicalBackfillChunkId, LogicalBackfillChunkSummary,
    LogicalBackfillDomain, LogicalBackfillExport, LogicalBackfillId, LogicalBackfillImport,
    LogicalBackfillManifest, LogicalExportRequest, SyncLearnerCatchupPolicy,
};
use storage_common::provider_perf::emit_runtime_report;
use storage_provider::{SqliteSettings, StorageProvider};
use storage_sync::{
    ResolvedSyncMutation, ResolvedSyncMutationBatch, SyncCommitMetadata, SyncDeleteMutation,
    SyncLogId, SyncMutationId, SyncMutationResponse, SyncPutMutation,
};
use storage_types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableRequest, DurablePointReadProof,
    DurablePointReadRequest, ItemStreamVersion, KeyAttributeType, KeyAttributes, KeySchemaElement,
    KeyType, TableName, TimestampMillis,
};
use stream_provider::StreamProvider;

use super::SQLiteStorageProvider;

const SYNC_APPLY_ALLOC_BATCHES: usize = 8;
const SYNC_APPLY_ALLOC_BATCH_SIZE: usize = 8;
const LOGICAL_SNAPSHOT_PROFILE_ITEMS: usize = 64;

#[tokio::test]
async fn sqlite_logical_empty_domains_export_without_unsupported_errors() {
    let provider = SQLiteStorageProvider::new(":memory:")
        .await
        .expect("create sqlite provider");
    initialize_sqlite(&provider).await;

    for domain in [
        LogicalBackfillDomain::Tombstones,
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
async fn sqlite_resolved_sync_apply_is_idempotent_and_persists_sync_log() {
    let provider = SQLiteStorageProvider::new(":memory:")
        .await
        .expect("create sqlite provider");
    initialize_sqlite(&provider).await;

    let table_name = TableName::new("sync_sqlite_items");
    provider
        .create_table(&basic_create_table_request(&table_name))
        .await
        .expect("create table");

    let first = sync_put(&table_name, "mutation-1", "item#1", "open", 7);
    let second = sync_put(&table_name, "mutation-2", "item#2", "closed", 8);
    let delete = sync_delete(&table_name, "mutation-3", "item#2", "closed", 9);
    let batch = ResolvedSyncMutationBatch::new(vec![first, second, delete]);
    let metadata = SyncCommitMetadata {
        log_id: SyncLogId::new(3, 9),
        committed_at: TimestampMillis::from_timestamp(1_700_000_000_000),
        leader_node_id: "node-1".to_string(),
    };

    provider
        .persist_resolved_sync_log_entry(&metadata, &batch)
        .await
        .expect("persist sync log entry");
    let first_response = provider
        .apply_resolved_sync_mutations(metadata.clone(), batch.clone())
        .await
        .expect("apply sync batch");
    let replay_response = provider
        .apply_resolved_sync_mutations(metadata.clone(), batch.clone())
        .await
        .expect("replay sync batch");

    assert_eq!(first_response, replay_response);
    assert_eq!(first_response.len(), 3);
    assert_present_revision(&provider, &table_name, "item#1", 7).await;
    assert_absent_revision(&provider, &table_name, "item#2", 9).await;
    assert_eq!(
        provider
            .last_resolved_sync_log_id()
            .await
            .expect("read last applied"),
        Some(metadata.log_id)
    );
    assert_eq!(
        provider
            .get_resolved_sync_log_entry(metadata.log_id)
            .await
            .expect("lookup sync log")
            .expect("sync log entry should exist")
            .batch,
        batch
    );
    assert_eq!(
        provider
            .resolved_sync_log_entries_after(Some(SyncLogId::new(3, 8)), 10)
            .await
            .expect("scan sync log")
            .len(),
        1
    );
}

#[tokio::test]
async fn sqlite_resolved_sync_apply_state_persists_across_reopen() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let database_path = tempdir.path().join("resolved-sync.db");
    let database_path = database_path.to_string_lossy().to_string();
    let provider = file_backed_provider(&database_path)
        .await
        .expect("create sqlite provider");
    initialize_sqlite(&provider).await;

    let table_name = TableName::new("sync_sqlite_reopen");
    provider
        .create_table(&basic_create_table_request(&table_name))
        .await
        .expect("create table");
    let put = sync_put(&table_name, "mutation-reopen-put", "item#1", "open", 11);
    let delete = sync_delete(
        &table_name,
        "mutation-reopen-delete",
        "item#2",
        "closed",
        12,
    );
    let batch = ResolvedSyncMutationBatch::new(vec![put, delete]);
    let metadata = SyncCommitMetadata {
        log_id: SyncLogId::new(4, 12),
        committed_at: TimestampMillis::from_timestamp(1_700_000_000_000),
        leader_node_id: "node-1".to_string(),
    };

    provider
        .persist_resolved_sync_log_entry(&metadata, &batch)
        .await
        .expect("persist sync log entry");
    provider
        .apply_resolved_sync_mutations(metadata.clone(), batch.clone())
        .await
        .expect("apply sync batch");
    drop(provider);

    let reopened = file_backed_provider(&database_path)
        .await
        .expect("reopen sqlite provider");
    initialize_sqlite(&reopened).await;
    assert_eq!(
        reopened
            .last_resolved_sync_log_id()
            .await
            .expect("read last applied after reopen"),
        Some(metadata.log_id)
    );
    assert_eq!(
        reopened
            .get_resolved_sync_log_entry(metadata.log_id)
            .await
            .expect("lookup sync log after reopen")
            .expect("sync log entry should exist")
            .batch,
        batch
    );
    assert_present_revision(&reopened, &table_name, "item#1", 11).await;
    assert_absent_revision(&reopened, &table_name, "item#2", 12).await;
}

#[tokio::test(flavor = "current_thread")]
async fn sqlite_resolved_sync_apply_allocation_baseline_tests() {
    let provider = SQLiteStorageProvider::new(":memory:")
        .await
        .expect("create sqlite provider");
    initialize_sqlite(&provider).await;

    let table_name = TableName::new("sync_sqlite_apply_alloc");
    provider
        .create_table(&basic_create_table_request(&table_name))
        .await
        .expect("create table");
    let batches = sync_apply_allocation_batches(&table_name);

    let guard = AllocationGuard::start(
        module_path!(),
        "sqlite_resolved_sync_apply_allocation_baseline_tests",
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
        "sqlite_resolved_sync_apply_allocation_baseline_tests",
        "put_batches_8x8",
        mutation_count,
        elapsed,
    );
    assert!(report.allocation_count > 0);
    assert!(report.allocated_bytes > 0);
}

#[tokio::test(flavor = "current_thread")]
async fn sqlite_resolved_sync_apply_grouped_vs_separate_runtime_tests() {
    let provider = SQLiteStorageProvider::new(":memory:")
        .await
        .expect("create sqlite provider");
    initialize_sqlite(&provider).await;

    let separate_table = TableName::new("sync_sqlite_apply_separate");
    let grouped_table = TableName::new("sync_sqlite_apply_grouped");
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
        "sqlite_resolved_sync_apply_grouped_vs_separate_runtime_tests",
        "separate_puts_8x1",
        8,
        separate_elapsed,
    );
    emit_runtime_report(
        module_path!(),
        "sqlite_resolved_sync_apply_grouped_vs_separate_runtime_tests",
        "grouped_puts_1x8",
        8,
        grouped_elapsed,
    );
    assert!(
        grouped_elapsed < separate_elapsed,
        "grouped apply should beat separate applies: grouped={grouped_elapsed:?} \
         separate={separate_elapsed:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn sqlite_logical_item_snapshot_export_import_runtime_tests() {
    let source = SQLiteStorageProvider::new(":memory:")
        .await
        .expect("create source sqlite provider");
    initialize_sqlite(&source).await;
    let target = SQLiteStorageProvider::new(":memory:")
        .await
        .expect("create target sqlite provider");
    initialize_sqlite(&target).await;

    let table_name = TableName::new("logical_snapshot_profile_items");
    let create_table = basic_create_table_request(&table_name);
    source
        .create_table(&create_table)
        .await
        .expect("create source table");
    target
        .create_table(&create_table)
        .await
        .expect("create target table");

    for index in 0..LOGICAL_SNAPSHOT_PROFILE_ITEMS {
        source
            .apply_resolved_sync_mutations(
                commit_metadata(20, index as u64 + 1),
                ResolvedSyncMutationBatch::new(vec![sync_put(
                    &table_name,
                    &format!("snapshot-mutation-{index:04}"),
                    &format!("snapshot-item-{index:04}"),
                    "open",
                    index as u64 + 1,
                )]),
            )
            .await
            .expect("seed source item");
    }

    let export_guard = AllocationGuard::start(
        module_path!(),
        "sqlite_logical_item_snapshot_export_import_runtime_tests",
        file!(),
        line!(),
        Some("export_items_64"),
    );
    let export_started = Instant::now();
    let page = source
        .export_logical_page(LogicalExportRequest {
            manifest_id: LogicalBackfillId::new("snapshot-profile").expect("manifest id"),
            domain: LogicalBackfillDomain::ItemRecords,
            table_name: Some(table_name.as_ref().to_string()),
            cursor: None,
            limit: LOGICAL_SNAPSHOT_PROFILE_ITEMS as u32,
        })
        .await
        .expect("export logical item page");
    let export_elapsed = export_started.elapsed();
    let export_report = export_guard.finish();
    alloc_counter::emit_report(&export_report);
    emit_runtime_report(
        module_path!(),
        "sqlite_logical_item_snapshot_export_import_runtime_tests",
        "export_items_64",
        page.records.len(),
        export_elapsed,
    );

    let record_count = page.records.len();
    assert_eq!(record_count, LOGICAL_SNAPSHOT_PROFILE_ITEMS);
    let checksum = page.checksum.clone();
    let manifest = LogicalBackfillManifest::for_policy(
        LogicalBackfillId::new("snapshot-profile").expect("manifest id"),
        &SyncLearnerCatchupPolicy,
        "sqlite",
        "sqlite",
        vec![LogicalBackfillDomain::ItemRecords],
    );
    let chunk = LogicalBackfillChunk {
        summary: LogicalBackfillChunkSummary {
            id: LogicalBackfillChunkId::new("snapshot-profile-items").expect("chunk id"),
            domain: LogicalBackfillDomain::ItemRecords,
            record_count: record_count as u64,
            checksum,
        },
        records: page.records,
    };

    let import_guard = AllocationGuard::start(
        module_path!(),
        "sqlite_logical_item_snapshot_export_import_runtime_tests",
        file!(),
        line!(),
        Some("import_items_64"),
    );
    let import_started = Instant::now();
    target
        .import_logical_chunk(&manifest, chunk)
        .await
        .expect("import logical item chunk");
    let import_elapsed = import_started.elapsed();
    let import_report = import_guard.finish();
    alloc_counter::emit_report(&import_report);
    emit_runtime_report(
        module_path!(),
        "sqlite_logical_item_snapshot_export_import_runtime_tests",
        "import_items_64",
        record_count,
        import_elapsed,
    );

    assert_present_revision(&target, &table_name, "snapshot-item-0000", 1).await;
    assert_present_revision(
        &target,
        &table_name,
        &format!("snapshot-item-{:04}", LOGICAL_SNAPSHOT_PROFILE_ITEMS - 1),
        LOGICAL_SNAPSHOT_PROFILE_ITEMS as u64,
    )
    .await;
    assert!(export_report.allocation_count > 0);
    assert!(import_report.allocation_count > 0);
}

async fn initialize_sqlite(provider: &SQLiteStorageProvider) {
    provider
        .initialize_storage()
        .await
        .expect("initialize sqlite storage");
    provider
        .initialize_stream()
        .await
        .expect("initialize sqlite stream storage");
}

async fn file_backed_provider(
    database_path: &str,
) -> storage_types::StorageResult<SQLiteStorageProvider> {
    SQLiteStorageProvider::new_with_settings(
        database_path,
        SqliteSettings {
            force_file_backed_database: true,
            ..SqliteSettings::default()
        },
    )
    .await
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

fn sync_put(
    table_name: &TableName,
    mutation_id: &str,
    key: &str,
    status: &str,
    version: u64,
) -> ResolvedSyncMutation {
    ResolvedSyncMutation::Put(SyncPutMutation {
        mutation_id: SyncMutationId::new(mutation_id).expect("mutation id"),
        table_name: table_name.clone(),
        key_json: format!(r#"{{"pk":{{"S":"{key}"}}}}"#),
        item_json: format!(r#"{{"pk":{{"S":"{key}"}},"status":{{"S":"{status}"}}}}"#),
        old_item_json: None,
        target_item_stream_version: ItemStreamVersion::new(version),
        response: SyncMutationResponse {
            response_json: Some(format!(r#"{{"mutation":"{mutation_id}"}}"#)),
        },
    })
}

fn sync_apply_allocation_batches(table_name: &TableName) -> Vec<ResolvedSyncMutationBatch> {
    (0..SYNC_APPLY_ALLOC_BATCHES)
        .map(|batch_index| {
            let mutations = (0..SYNC_APPLY_ALLOC_BATCH_SIZE)
                .map(|item_index| {
                    let absolute_index = batch_index * SYNC_APPLY_ALLOC_BATCH_SIZE + item_index;
                    sync_put(
                        table_name,
                        &format!("alloc-mutation-{absolute_index:04}"),
                        &format!("alloc-item-{absolute_index:04}"),
                        "open",
                        absolute_index as u64 + 1,
                    )
                })
                .collect();
            ResolvedSyncMutationBatch::new(mutations)
        })
        .collect()
}

fn commit_metadata(term: u64, index: u64) -> SyncCommitMetadata {
    SyncCommitMetadata {
        log_id: SyncLogId::new(term, index),
        committed_at: TimestampMillis::from_timestamp(1_700_000_000_000),
        leader_node_id: "node-1".to_string(),
    }
}

fn sync_delete(
    table_name: &TableName,
    mutation_id: &str,
    key: &str,
    old_status: &str,
    version: u64,
) -> ResolvedSyncMutation {
    ResolvedSyncMutation::Delete(SyncDeleteMutation {
        mutation_id: SyncMutationId::new(mutation_id).expect("mutation id"),
        table_name: table_name.clone(),
        key_json: format!(r#"{{"pk":{{"S":"{key}"}}}}"#),
        old_item_json: Some(format!(
            r#"{{"pk":{{"S":"{key}"}},"status":{{"S":"{old_status}"}}}}"#
        )),
        target_item_stream_version: ItemStreamVersion::new(version),
        response: SyncMutationResponse {
            response_json: Some(format!(r#"{{"mutation":"{mutation_id}"}}"#)),
        },
    })
}

async fn assert_present_revision(
    provider: &SQLiteStorageProvider,
    table_name: &TableName,
    key: &str,
    version: u64,
) {
    let proof = provider
        .get_item_with_durable_proof(DurablePointReadRequest {
            table_name: table_name.clone(),
            key: KeyAttributes::from(key_map(key)),
            consistent_read: true,
        })
        .await
        .expect("durable proof");
    let DurablePointReadProof::Present { revision, .. } = proof else {
        panic!("sync-applied item should be present");
    };
    assert_eq!(
        revision.as_bytes(),
        &ItemStreamVersion::new(version).to_be_bytes()
    );
}

async fn assert_absent_revision(
    provider: &SQLiteStorageProvider,
    table_name: &TableName,
    key: &str,
    version: u64,
) {
    let proof = provider
        .get_item_with_durable_proof(DurablePointReadRequest {
            table_name: table_name.clone(),
            key: KeyAttributes::from(key_map(key)),
            consistent_read: true,
        })
        .await
        .expect("durable proof");
    let DurablePointReadProof::Absent { proof } = proof else {
        panic!("sync-deleted item should be absent");
    };
    assert_eq!(
        proof.as_bytes(),
        &ItemStreamVersion::new(version).to_be_bytes()
    );
}

fn key_map(key: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([("pk".to_string(), AttributeValue::S(key.to_string()))])
}
