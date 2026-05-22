use std::collections::HashMap;

#[cfg(feature = "rocksdb")]
use storage_provider::{RocksdbSettings, StorageBackend, StorageConfig};
use storage_sync::{
    ResolvedSyncMutation, SyncApply, SyncMutationResolver, SyncProposalId,
    SyncWriteProposalRequest, SyncWriteRequest,
};
#[cfg(feature = "rocksdb")]
use storage_types::TimeToLiveStatus;
use storage_types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableRequest, IndexName,
    KeyAttributeType, KeySchemaElement, KeyType, PutItemRequest, TableName,
    TimeToLiveSpecification, UpdateTimeToLiveRequest,
};

use crate::{
    DatabaseManager, PutItemInput,
    database_manager::sync_resolver_ops_support_tests::{
        commit_metadata, create_gsi_table, create_hash_table, create_hash_table_with_stream,
        create_single_node_sync_db, create_single_node_sync_db_with_immediate_gsi,
        file_backed_sqlite_config, item,
    },
};

#[tokio::test]
async fn single_node_sync_put_writes_stream_entry_with_committed_item_version() {
    let db = create_single_node_sync_db().await;
    let table_name = TableName::new("single_node_stream_side_effect");
    create_hash_table_with_stream(&db, &table_name).await;

    db.put_item(
        PutItemInput::builder()
            .table_name(table_name.clone())
            .item(item("item#1", "open"))
            .build(),
    )
    .await
    .expect("put through sync mode");

    let response = db
        .get_stream_records_for_table_name(&table_name, None, Some(10))
        .await
        .expect("read stream records");
    assert_eq!(response.records.len(), 1);
    assert_eq!(response.records[0].sequence_number, "1");
    assert_eq!(
        response.records[0].keys.get("pk"),
        Some(&AttributeValue::S("item#1".to_string()))
    );
}

#[tokio::test]
async fn single_node_sync_update_preserves_old_and_new_stream_images() {
    let db = create_single_node_sync_db().await;
    let table_name = TableName::new("single_node_update_stream_images");
    create_hash_table_with_stream(&db, &table_name).await;

    db.put_item(
        PutItemInput::builder()
            .table_name(table_name.clone())
            .item(item("item#1", "open"))
            .build(),
    )
    .await
    .expect("put through sync mode");

    db.update_item(crate::UpdateItemInput {
        table_name: table_name.clone(),
        key: HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))]).into(),
        update_expression: "SET #value = :value".to_string(),
        condition_expression: Some("#value = :old".to_string()),
        expression_attribute_names: Some(HashMap::from([(
            "#value".to_string(),
            "value".to_string(),
        )])),
        expression_attribute_values: Some(HashMap::from([
            (
                ":value".to_string(),
                AttributeValue::S("closed".to_string()),
            ),
            (":old".to_string(), AttributeValue::S("open".to_string())),
        ])),
        return_values: Some(storage_types::ReturnValuesOldNewUpdated::AllNew),
    })
    .await
    .expect("update through sync mode");

    let response = db
        .get_stream_records_for_table_name(&table_name, None, Some(10))
        .await
        .expect("read stream records");
    assert_eq!(response.records.len(), 2);
    let update_record = &response.records[1];
    assert_eq!(update_record.sequence_number, "2");
    assert_eq!(
        update_record
            .old_image
            .as_ref()
            .and_then(|item| item.get("value")),
        Some(&AttributeValue::S("open".to_string()))
    );
    assert_eq!(
        update_record
            .new_image
            .as_ref()
            .and_then(|item| item.get("value")),
        Some(&AttributeValue::S("closed".to_string()))
    );
}

#[tokio::test]
async fn single_node_sync_put_updates_ttl_side_effects_for_sweeper() {
    let db = create_single_node_sync_db().await;
    let table_name = TableName::new("single_node_ttl_side_effect");
    create_hash_table(&db, &table_name).await;
    db.update_time_to_live(UpdateTimeToLiveRequest {
        table_name: table_name.clone(),
        time_to_live_specification: TimeToLiveSpecification {
            attribute_name: "ttl".to_string(),
            enabled: true,
        },
    })
    .await
    .expect("enable ttl");

    let mut expired_item = item("item#1", "expired");
    expired_item.insert("ttl".to_string(), AttributeValue::N("1".to_string()));
    db.put_item(
        PutItemInput::builder()
            .table_name(table_name.clone())
            .item(expired_item)
            .build(),
    )
    .await
    .expect("put expired item through sync mode");
    db.run_job(storage_common::TTL_SWEEP_JOB).await;

    assert!(
        db.get_item_map(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))])
        )
        .await
        .expect("read swept item")
        .is_none()
    );
}

#[tokio::test]
async fn single_node_sync_put_updates_immediate_gsi_side_effect() {
    let db = create_single_node_sync_db_with_immediate_gsi().await;
    let table_name = TableName::new("single_node_gsi_side_effect");
    create_gsi_table(&db, &table_name).await;
    let mut indexed_item = item("item#1", "indexed");
    indexed_item.insert("gsi_pk".to_string(), AttributeValue::S("grp".to_string()));
    db.put_item(
        PutItemInput::builder()
            .table_name(table_name.clone())
            .item(indexed_item)
            .build(),
    )
    .await
    .expect("put indexed item through sync mode");

    let (items, _) = db
        .query_index(crate::QueryIndexInput {
            table_name,
            index_name: IndexName::new("TestGSI"),
            key_condition_expression: "gsi_pk = :gsi_pk".to_string(),
            expression_attribute_names: None,
            expression_attribute_values: Some(HashMap::from([(
                ":gsi_pk".to_string(),
                AttributeValue::S("grp".to_string()),
            )])),
            limit: Some(10),
            exclusive_start_key: None,
            scan_index_forward: Some(true),
        })
        .await
        .expect("query gsi");
    assert_eq!(items.len(), 1);
}

#[tokio::test]
async fn single_node_sync_replays_persisted_log_after_restart_once() {
    let config = file_backed_sqlite_config("sync-replay");
    let db = DatabaseManager::new_with_config(config.clone())
        .await
        .expect("file-backed db");
    let table_name = TableName::new("single_node_replay_after_restart");
    create_hash_table_with_stream(&db, &table_name).await;
    let proposal = db
        .resolve_sync_mutation(SyncWriteProposalRequest::new(
            SyncProposalId::new("proposal-replay").unwrap(),
            SyncWriteRequest::PutItem(PutItemRequest {
                table_name: table_name.clone(),
                item: item("item#1", "replayed"),
                condition_expression: None,
                expression_attribute_names: None,
                expression_attribute_values: None,
                expected: None,
                conditional_operator: None,
                return_values: None,
                return_consumed_capacity: None,
                return_item_collection_metrics: None,
                return_values_on_condition_check_failure: None,
            }),
        ))
        .await
        .expect("resolve replay proposal");
    let metadata = commit_metadata(1);
    db.storage_provider()
        .persist_resolved_sync_log_entry(&metadata, &proposal.batch)
        .await
        .expect("persist unapplied sync log entry");
    drop(db);

    let reopened = DatabaseManager::new_with_config(config)
        .await
        .expect("reopen file-backed db");
    assert_eq!(
        reopened
            .replay_resolved_sync_log_entries(10)
            .await
            .expect("replay sync log"),
        1
    );
    assert_eq!(
        reopened
            .replay_resolved_sync_log_entries(10)
            .await
            .expect("replay sync log again"),
        0,
        "second replay should not re-apply already checkpointed entries"
    );
    let stored = reopened
        .get_item_map(
            table_name.clone(),
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))]),
        )
        .await
        .expect("read replayed item")
        .expect("replayed item");
    assert_eq!(
        stored.get("value"),
        Some(&AttributeValue::S("replayed".to_string()))
    );
    let stream_records = reopened
        .get_stream_records_for_table_name(&table_name, None, Some(10))
        .await
        .expect("read replayed stream records");
    assert_eq!(stream_records.records.len(), 1);
    assert_eq!(stream_records.records[0].sequence_number, "1");
}

#[tokio::test]
async fn sync_lifecycle_create_table_resolves_and_applies_after_commit() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("test database");
    let table_name = TableName::new("sync_lifecycle_create_table");
    let request = CreateTableRequest::new(
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
    );

    let proposal = db
        .resolve_sync_mutation(SyncWriteProposalRequest::new(
            SyncProposalId::new("proposal-create-table").unwrap(),
            SyncWriteRequest::CreateTable(request),
        ))
        .await
        .expect("resolve lifecycle proposal");

    assert_eq!(proposal.batch.mutations.len(), 1);
    assert!(matches!(
        proposal.batch.mutations[0],
        ResolvedSyncMutation::CreateTable(_)
    ));
    assert!(
        !db.storage_provider()
            .table_exists(&table_name)
            .await
            .expect("table exists check"),
        "resolution must not mutate table metadata before commit"
    );

    let responses = db
        .apply_resolved_sync_mutations(commit_metadata(1), proposal.batch)
        .await
        .expect("apply lifecycle mutation");

    assert_eq!(responses.len(), 1);
    assert!(
        db.storage_provider()
            .table_exists(&table_name)
            .await
            .expect("table exists check")
    );
    let response_json = responses[0]
        .response_json
        .as_deref()
        .expect("create table response");
    let response: storage_types::CreateTableResponse =
        serde_json::from_str(response_json).expect("create response json");
    assert_eq!(response.table_description.table_name, table_name);
}

#[tokio::test]
#[cfg(feature = "rocksdb")]
async fn sync_lifecycle_update_ttl_replay_treats_matching_in_progress_state_as_idempotent() {
    let db = create_rocksdb_single_node_sync_db("ttl-replay").await;
    let table_name = TableName::new("sync_lifecycle_ttl_replay");
    create_hash_table(&db, &table_name).await;
    let request = UpdateTimeToLiveRequest {
        table_name: table_name.clone(),
        time_to_live_specification: TimeToLiveSpecification {
            attribute_name: "ttl".to_string(),
            enabled: true,
        },
    };
    let proposal = db
        .resolve_sync_mutation(SyncWriteProposalRequest::new(
            SyncProposalId::new("proposal-update-ttl").unwrap(),
            SyncWriteRequest::UpdateTimeToLive(request),
        ))
        .await
        .expect("resolve ttl proposal");

    db.apply_resolved_sync_mutations(commit_metadata(2), proposal.batch.clone())
        .await
        .expect("first ttl apply");
    db.apply_resolved_sync_mutations(commit_metadata(2), proposal.batch)
        .await
        .expect("replay ttl apply");

    let description = db
        .describe_time_to_live(&table_name)
        .await
        .expect("describe ttl")
        .time_to_live_description
        .expect("ttl description");
    assert_eq!(description.attribute_name.as_deref(), Some("ttl"));
    assert!(matches!(
        description.time_to_live_status,
        TimeToLiveStatus::Enabling | TimeToLiveStatus::Enabled
    ));
}

#[cfg(feature = "rocksdb")]
async fn create_rocksdb_single_node_sync_db(label: &str) -> DatabaseManager {
    let path = std::env::temp_dir().join(format!(
        "aux-storage-{label}-{}",
        storage_types::TimestampMillis::now().timestamp_millis()
    ));
    DatabaseManager::new_with_config_and_runtime_options(
        StorageConfig {
            backend_type: StorageBackend::RocksDB,
            connection_string: Some(path.to_string_lossy().to_string()),
            file_path: None,
            sqlite: None,
            postgres: None,
            turso: None,
            rocksdb: Some(RocksdbSettings {
                immediate_gsi_consistency: true,
            }),
            foundationdb: None,
            remote: None,
        },
        crate::database_manager::DatabaseManagerRuntimeOptions::builder()
            .enable_single_node_sync_mode(true)
            .build(),
    )
    .await
    .expect("rocksdb sync db")
}
