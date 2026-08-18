use std::{collections::HashMap, time::Instant};

use alloc_counter::AllocationGuard;
use storage_common::provider_perf::emit_runtime_report;
use storage_provider::StorageProvider;
use storage_sync::{
    ResolvedSyncLogEntry, ResolvedSyncMutation, ResolvedSyncMutationBatch, SyncCommitMetadata,
    SyncDeleteMutation, SyncLogId, SyncMutationId, SyncMutationResponse, SyncPutMutation,
};
use storage_types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateGlobalSecondaryIndex,
    CreateTableRequest, DurablePointReadProof, DurablePointReadRequest, IndexName,
    ItemStreamVersion, KeyAttributeType, KeySchemaElement, KeyType, Projection, ProjectionType,
    QueryTableRequest, StreamItemId, StreamName, StreamSpecification, StreamViewType, TableName,
    TimeToLiveSpecification, TimestampMillis, UpdateTimeToLiveRequest,
};
use stream_provider::{StreamDataType, StreamProvider};

#[cfg(feature = "rocksdb-backend")]
use crate::kv_support_tests::rocksdb_test_path;
use crate::{
    SortedKvDbStorageProvider, keyspace::compact, kv_support_tests::create_test_store,
    sorted_kv_store::SortedKvStore as _, ttl,
};

const SYNC_APPLY_ALLOC_BATCHES: usize = 8;
const SYNC_APPLY_ALLOC_BATCH_SIZE: usize = 8;

async fn compact_ttl_key<S: crate::partition_family::PartitionFamilyKvStore + 'static>(
    provider: &SortedKvDbStorageProvider<S>,
    table: &TableName,
    table_info: &storage_types::StoredTableInfo,
    item: &HashMap<String, AttributeValue>,
) -> Vec<u8> {
    let metadata = provider
        .get_table_identity_from_name(table)
        .await
        .expect("table metadata result")
        .expect("table metadata");
    ttl::compact_ttl_index_key_for_item(&metadata.identity, table_info, "ttl", item)
        .expect("ttl key result")
        .expect("ttl key")
}

#[tokio::test]
#[cfg(feature = "rocksdb-backend")]
async fn rocksdb_resolved_sync_apply_is_crash_idempotent_and_preserves_side_effects() {
    let store = crate::RocksDbKvStore::new(rocksdb_test_path("sync-apply")).expect("rocksdb");
    let provider =
        SortedKvDbStorageProvider::new(store.clone()).with_immediate_gsi_consistency(true);
    provider
        .initialize_storage()
        .await
        .expect("initialize storage");
    provider
        .initialize_stream()
        .await
        .expect("initialize stream");

    let table = TableName::new("SyncApplyRocks");
    provider
        .create_table(&create_table_request(table.clone()))
        .await
        .expect("create table");
    provider
        .update_time_to_live(UpdateTimeToLiveRequest {
            table_name: table.clone(),
            time_to_live_specification: TimeToLiveSpecification {
                attribute_name: "ttl".to_string(),
                enabled: true,
            },
        })
        .await
        .expect("enable ttl");

    let item = item("user#1", "order#1", "1700000300", "open");
    let metadata = commit_metadata(1, 7);
    let batch = ResolvedSyncMutationBatch::new(vec![ResolvedSyncMutation::Put(SyncPutMutation {
        mutation_id: SyncMutationId::new("m-put-1").expect("mutation id"),
        table_name: table.clone(),
        key_json: serde_json::to_string(&key("user#1", "order#1")).expect("key json"),
        item_json: serde_json::to_string(&item).expect("item json"),
        indexers: Vec::new(),
        old_item_json: None,
        old_indexers: None,
        target_item_stream_version: ItemStreamVersion::new(42),
        response: SyncMutationResponse {
            response_json: Some("{\"ok\":true}".to_string()),
        },
    })]);

    provider
        .persist_resolved_sync_log_entry(&metadata, &batch)
        .await
        .expect("persist sync log");
    let responses = provider
        .apply_resolved_sync_mutations(metadata.clone(), batch.clone())
        .await
        .expect("apply sync mutation");
    assert_eq!(responses.len(), 1);
    assert_eq!(
        provider
            .last_resolved_sync_log_id()
            .await
            .expect("last log"),
        Some(metadata.log_id)
    );

    let restarted = SortedKvDbStorageProvider::new(store).with_immediate_gsi_consistency(true);
    restarted
        .initialize_stream()
        .await
        .expect("initialize restarted stream");
    restarted
        .apply_resolved_sync_mutations(metadata.clone(), batch.clone())
        .await
        .expect("replay duplicate");

    let stored = restarted
        .get_item(table.clone(), key("user#1", "order#1").into(), true)
        .await
        .expect("get item")
        .expect("item exists")
        .into_attribute_map()
        .expect("item map");
    assert_eq!(
        stored.get("status"),
        Some(&AttributeValue::S("open".to_string()))
    );
    let proof = restarted
        .get_item_with_durable_proof(DurablePointReadRequest {
            table_name: table.clone(),
            key: key("user#1", "order#1").into(),
            consistent_read: true,
        })
        .await
        .expect("durable point read proof");
    let DurablePointReadProof::Present { revision, .. } = proof else {
        panic!("sync-applied item should be present");
    };
    assert_eq!(
        revision.as_bytes(),
        &ItemStreamVersion::new(42).to_be_bytes()
    );

    let system_page =
        StreamProvider::read_forward(&restarted, StreamName::system_table_stream(), None, 10)
            .await
            .expect("system stream");
    assert_eq!(
        system_page.items.len(),
        1,
        "duplicate replay must not add stream rows"
    );
    assert_eq!(
        system_page.items[0].id,
        StreamItemId::from(ItemStreamVersion::new(42))
    );
    assert_eq!(
        system_page.items[0].data_type,
        StreamDataType::StreamPointer
    );

    let gsi_items = restarted
        .query_table(&gsi_query(table.clone()))
        .await
        .expect("query gsi")
        .0;
    assert_eq!(gsi_items.len(), 1);

    let table_info = restarted.get_table_info(&table).await.expect("table info");
    let ttl_key = compact_ttl_key(&restarted, &table, &table_info, &item).await;
    assert!(
        restarted
            .kv_store
            .get(&ttl_key, true)
            .await
            .expect("ttl lookup")
            .is_some(),
        "resolved apply must preserve TTL index side effect",
    );

    assert_eq!(
        restarted
            .get_resolved_sync_log_entry(metadata.log_id)
            .await
            .expect("get log entry"),
        Some(ResolvedSyncLogEntry::new(metadata.clone(), batch.clone()))
    );
    assert_eq!(
        restarted
            .resolved_sync_log_entries_after(Some(SyncLogId::new(1, 6)), 10)
            .await
            .expect("scan log entries"),
        vec![ResolvedSyncLogEntry::new(metadata, batch)]
    );
}

#[tokio::test]
async fn shared_kv_resolved_sync_apply_preserves_side_effects() {
    let provider =
        SortedKvDbStorageProvider::new(create_test_store()).with_immediate_gsi_consistency(true);
    provider
        .initialize_storage()
        .await
        .expect("initialize storage");
    provider
        .initialize_stream()
        .await
        .expect("initialize stream");

    let table = TableName::new("SyncApplySharedKv");
    provider
        .create_table(&create_table_request(table.clone()))
        .await
        .expect("create table");
    provider
        .update_time_to_live(UpdateTimeToLiveRequest {
            table_name: table.clone(),
            time_to_live_specification: TimeToLiveSpecification {
                attribute_name: "ttl".to_string(),
                enabled: true,
            },
        })
        .await
        .expect("enable ttl");

    let put_item = item("user#1", "order#1", "1700000300", "open");
    let deleted_item = item("user#2", "order#2", "1700000400", "closed");
    let metadata = commit_metadata(1, 8);
    let batch = ResolvedSyncMutationBatch::new(vec![
        ResolvedSyncMutation::Put(SyncPutMutation {
            mutation_id: SyncMutationId::new("m-put-shared").expect("mutation id"),
            table_name: table.clone(),
            key_json: serde_json::to_string(&key("user#1", "order#1")).expect("key json"),
            item_json: serde_json::to_string(&put_item).expect("item json"),
            indexers: Vec::new(),
            old_item_json: None,
            old_indexers: None,
            target_item_stream_version: ItemStreamVersion::new(43),
            response: SyncMutationResponse {
                response_json: Some("{\"ok\":true}".to_string()),
            },
        }),
        ResolvedSyncMutation::Delete(SyncDeleteMutation {
            mutation_id: SyncMutationId::new("m-delete-shared").expect("mutation id"),
            table_name: table.clone(),
            key_json: serde_json::to_string(&key("user#2", "order#2")).expect("key json"),
            old_item_json: Some(serde_json::to_string(&deleted_item).expect("old item json")),
            old_indexers: None,
            target_item_stream_version: ItemStreamVersion::new(44),
            response: SyncMutationResponse {
                response_json: Some("{\"deleted\":true}".to_string()),
            },
        }),
    ]);

    provider
        .persist_resolved_sync_log_entry(&metadata, &batch)
        .await
        .expect("persist sync log");
    provider
        .apply_resolved_sync_mutations(metadata.clone(), batch.clone())
        .await
        .expect("apply sync mutation");
    provider
        .apply_resolved_sync_mutations(metadata.clone(), batch.clone())
        .await
        .expect("replay duplicate");

    let proof = provider
        .get_item_with_durable_proof(DurablePointReadRequest {
            table_name: table.clone(),
            key: key("user#1", "order#1").into(),
            consistent_read: true,
        })
        .await
        .expect("durable point read proof");
    let DurablePointReadProof::Present { revision, .. } = proof else {
        panic!("sync-applied item should be present");
    };
    assert_eq!(
        revision.as_bytes(),
        &ItemStreamVersion::new(43).to_be_bytes()
    );
    let deleted_proof = provider
        .get_item_with_durable_proof(DurablePointReadRequest {
            table_name: table.clone(),
            key: key("user#2", "order#2").into(),
            consistent_read: true,
        })
        .await
        .expect("durable delete proof");
    let DurablePointReadProof::Absent { proof } = deleted_proof else {
        panic!("sync-deleted item should be absent");
    };
    assert_eq!(proof.as_bytes(), &ItemStreamVersion::new(44).to_be_bytes());

    let system_page =
        StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
            .await
            .expect("system stream");
    assert_eq!(
        system_page.items.len(),
        2,
        "duplicate replay must not add stream rows"
    );
    assert_eq!(
        system_page.items[0].id,
        StreamItemId::from(ItemStreamVersion::new(43))
    );
    assert_eq!(
        system_page.items[1].id,
        StreamItemId::from(ItemStreamVersion::new(44))
    );
    assert_eq!(
        system_page.items[0].data_type,
        StreamDataType::StreamPointer
    );

    let gsi_items = provider
        .query_table(&gsi_query(table.clone()))
        .await
        .expect("query gsi")
        .0;
    assert_eq!(gsi_items.len(), 1);

    let table_info = provider.get_table_info(&table).await.expect("table info");
    let ttl_key = compact_ttl_key(&provider, &table, &table_info, &put_item).await;
    assert!(
        provider
            .kv_store
            .get(&ttl_key, true)
            .await
            .expect("ttl lookup")
            .is_some(),
        "resolved apply must preserve TTL index side effect",
    );

    assert_eq!(
        provider
            .get_resolved_sync_log_entry(metadata.log_id)
            .await
            .expect("get log entry"),
        Some(ResolvedSyncLogEntry::new(metadata.clone(), batch.clone()))
    );
    assert!(
        provider
            .kv_store
            .get(
                &compact::sync_log_entry_key(metadata.log_id.term, metadata.log_id.index),
                true
            )
            .await
            .expect("compact sync log lookup")
            .is_some()
    );
    assert_eq!(
        provider
            .kv_store
            .get(
                b"sys/sync/log/00000000000000000001/00000000000000000007",
                true
            )
            .await
            .expect("legacy sync log lookup"),
        None
    );
    assert_eq!(
        provider
            .kv_store
            .get(b"sys/sync/apply/mutation/m-put-shared", true)
            .await
            .expect("legacy sync apply lookup"),
        None
    );
    assert_eq!(
        provider
            .resolved_sync_log_entries_after(Some(SyncLogId::new(1, 7)), 10)
            .await
            .expect("scan log entries"),
        vec![ResolvedSyncLogEntry::new(metadata, batch)]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn shared_kv_resolved_sync_apply_allocation_baseline_tests() {
    let provider =
        SortedKvDbStorageProvider::new(create_test_store()).with_immediate_gsi_consistency(true);
    provider
        .initialize_storage()
        .await
        .expect("initialize storage");
    provider
        .initialize_stream()
        .await
        .expect("initialize stream");

    let table = TableName::new("SyncApplyAllocSharedKv");
    provider
        .create_table(&create_table_request(table.clone()))
        .await
        .expect("create table");
    provider
        .update_time_to_live(UpdateTimeToLiveRequest {
            table_name: table.clone(),
            time_to_live_specification: TimeToLiveSpecification {
                attribute_name: "ttl".to_string(),
                enabled: true,
            },
        })
        .await
        .expect("enable ttl");
    let batches = sync_apply_allocation_batches(&table);

    let guard = AllocationGuard::start(
        module_path!(),
        "shared_kv_resolved_sync_apply_allocation_baseline_tests",
        file!(),
        line!(),
        Some("put_batches_8x8"),
    );

    let started = Instant::now();
    let mut mutation_count = 0_usize;
    for (batch_index, batch) in batches.into_iter().enumerate() {
        mutation_count = mutation_count.saturating_add(batch.mutations.len());
        provider
            .apply_resolved_sync_mutations(commit_metadata(7, batch_index as u64 + 1), batch)
            .await
            .expect("apply sync batch");
    }
    let elapsed = started.elapsed();

    let report = guard.finish();
    alloc_counter::emit_report(&report);
    emit_runtime_report(
        module_path!(),
        "shared_kv_resolved_sync_apply_allocation_baseline_tests",
        "put_batches_8x8",
        mutation_count,
        elapsed,
    );
    assert!(report.allocation_count > 0);
    assert!(report.allocated_bytes > 0);
}

#[tokio::test]
async fn shared_kv_resolved_sync_log_scan_respects_after_boundary() {
    let provider = SortedKvDbStorageProvider::new(create_test_store());
    provider
        .initialize_storage()
        .await
        .expect("initialize storage");

    let first = commit_metadata(1, 1);
    let second = commit_metadata(1, 2);
    let empty = ResolvedSyncMutationBatch::new(Vec::new());
    provider
        .persist_resolved_sync_log_entry(&first, &empty)
        .await
        .expect("persist first");
    provider
        .persist_resolved_sync_log_entry(&second, &empty)
        .await
        .expect("persist second");

    let entries = provider
        .resolved_sync_log_entries_after(Some(first.log_id), 10)
        .await
        .expect("scan after first");

    assert_eq!(entries, vec![ResolvedSyncLogEntry::new(second, empty)]);
}

fn create_table_request(table_name: TableName) -> CreateTableRequest {
    CreateTableRequest::new(
        table_name,
        vec![
            attr("pk", KeyAttributeType::S),
            attr("sk", KeyAttributeType::S),
            attr("gsi_pk", KeyAttributeType::S),
            attr("gsi_sk", KeyAttributeType::S),
        ],
        vec![hash_key("pk"), range_key("sk")],
        BillingMode::PayPerRequest,
    )
    .with_global_secondary_indexes(Some(vec![CreateGlobalSecondaryIndex {
        index_name: IndexName::new("by_status"),
        key_schema: vec![hash_key("gsi_pk"), range_key("gsi_sk")],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }]))
    .with_stream_specification(Some(StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(StreamViewType::NewAndOldImages),
    }))
}

fn attr(name: &str, attribute_type: KeyAttributeType) -> AttributeDefinition {
    AttributeDefinition {
        attribute_name: name.to_string(),
        attribute_type,
    }
}

fn hash_key(name: &str) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_string(),
        key_type: KeyType::Hash,
    }
}

fn range_key(name: &str) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_string(),
        key_type: KeyType::Range,
    }
}

fn key(pk: &str, sk: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S(pk.to_string())),
        ("sk".to_string(), AttributeValue::S(sk.to_string())),
    ])
}

fn item(pk: &str, sk: &str, ttl: &str, status: &str) -> HashMap<String, AttributeValue> {
    let mut item = key(pk, sk);
    item.insert("ttl".to_string(), AttributeValue::N(ttl.to_string()));
    item.insert("status".to_string(), AttributeValue::S(status.to_string()));
    item.insert("gsi_pk".to_string(), AttributeValue::S(status.to_string()));
    item.insert("gsi_sk".to_string(), AttributeValue::S(sk.to_string()));
    item
}

fn sync_apply_allocation_batches(table_name: &TableName) -> Vec<ResolvedSyncMutationBatch> {
    (0..SYNC_APPLY_ALLOC_BATCHES)
        .map(|batch_index| {
            let mutations = (0..SYNC_APPLY_ALLOC_BATCH_SIZE)
                .map(|item_index| {
                    let absolute_index = batch_index * SYNC_APPLY_ALLOC_BATCH_SIZE + item_index;
                    let pk = format!("alloc-user#{absolute_index:04}");
                    let sk = format!("alloc-order#{absolute_index:04}");
                    let item = item(
                        &pk,
                        &sk,
                        &(1_700_003_000_u64 + absolute_index as u64).to_string(),
                        "open",
                    );
                    ResolvedSyncMutation::Put(SyncPutMutation {
                        mutation_id: SyncMutationId::new(format!(
                            "alloc-mutation-{absolute_index:04}"
                        ))
                        .expect("mutation id"),
                        table_name: table_name.clone(),
                        key_json: serde_json::to_string(&key(&pk, &sk)).expect("key json"),
                        item_json: serde_json::to_string(&item).expect("item json"),
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
        projection_expression: None,
        limit: Some(10),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    }
}

fn commit_metadata(term: u64, index: u64) -> SyncCommitMetadata {
    SyncCommitMetadata {
        log_id: SyncLogId::new(term, index),
        committed_at: TimestampMillis::from_timestamp(1_700_000_000_000 + index as i64),
        leader_node_id: "leader-a".to_string(),
    }
}
