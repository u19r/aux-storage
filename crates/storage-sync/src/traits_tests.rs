use storage_types::{ItemStreamVersion, TableName, TimestampMillis};

use crate::{
    ResolvedSyncMutation, ResolvedSyncMutationBatch, SyncApply, SyncCommitMetadata, SyncLogId,
    SyncMutationId, SyncMutationResponse, SyncPutMutation,
    sync_support_tests::ResolvedOnlyApplyAdapter,
};

#[tokio::test]
async fn sync_apply_adapter_is_typed_only_for_resolved_mutations() {
    let adapter = ResolvedOnlyApplyAdapter;
    let mutation = ResolvedSyncMutation::Put(SyncPutMutation {
        mutation_id: SyncMutationId::new("mutation-1").unwrap(),
        table_name: TableName::new("orders"),
        key_json: r#"{"pk":{"S":"order#1"}}"#.to_string(),
        item_json: r#"{"pk":{"S":"order#1"},"status":{"S":"open"}}"#.to_string(),
        indexers: Vec::new(),
        old_item_json: None,
        old_indexers: None,
        target_item_stream_version: ItemStreamVersion::new(1),
        response: SyncMutationResponse::default(),
    });

    let responses = adapter
        .apply_resolved_sync_mutations(
            SyncCommitMetadata {
                log_id: SyncLogId::new(1, 1),
                committed_at: TimestampMillis::from_timestamp(1_700_000_000_000),
                leader_node_id: "node-a".to_string(),
            },
            ResolvedSyncMutationBatch::new(vec![mutation]),
        )
        .await
        .unwrap();

    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].response_json.as_deref(), Some("mutation-1"));
}
