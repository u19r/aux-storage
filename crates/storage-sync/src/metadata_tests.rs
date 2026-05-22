use storage_types::{ItemStreamVersion, TableName, TimestampMillis};

use crate::{
    ResolvedSyncLogEntry, ResolvedSyncMutationBatch, SyncCommitMetadata, SyncItemBaseVersion,
    SyncLogId, SyncReadSet,
};

#[test]
fn sync_log_id_orders_by_term_then_index() {
    assert!(SyncLogId::new(2, 1) > SyncLogId::new(1, 100));
    assert!(SyncLogId::new(2, 2) > SyncLogId::new(2, 1));
}

#[test]
fn sync_read_set_records_absent_and_present_base_versions() {
    let read_set = SyncReadSet::new(vec![
        SyncItemBaseVersion {
            table_name: TableName::new("orders"),
            key_json: r#"{"pk":{"S":"order#1"}}"#.to_string(),
            item_stream_version: Some(ItemStreamVersion::new(7)),
        },
        SyncItemBaseVersion {
            table_name: TableName::new("orders"),
            key_json: r#"{"pk":{"S":"order#2"}}"#.to_string(),
            item_stream_version: None,
        },
    ]);

    assert!(!read_set.is_empty());
    assert_eq!(read_set.items.len(), 2);
}

#[test]
fn sync_commit_metadata_carries_leader_assigned_commit_identity() {
    let metadata = SyncCommitMetadata {
        log_id: SyncLogId::new(3, 9),
        committed_at: TimestampMillis::from_timestamp(1_700_000_000_000),
        leader_node_id: "node-a".to_string(),
    };

    assert_eq!(metadata.log_id, SyncLogId::new(3, 9));
    assert_eq!(metadata.leader_node_id, "node-a");
}

#[test]
fn resolved_sync_log_entry_keeps_metadata_with_batch() {
    let metadata = SyncCommitMetadata {
        log_id: SyncLogId::new(3, 9),
        committed_at: TimestampMillis::from_timestamp(1_700_000_000_000),
        leader_node_id: "node-a".to_string(),
    };
    let entry =
        ResolvedSyncLogEntry::new(metadata.clone(), ResolvedSyncMutationBatch::new(Vec::new()));

    assert_eq!(entry.metadata, metadata);
    assert!(entry.batch.is_empty());
}
