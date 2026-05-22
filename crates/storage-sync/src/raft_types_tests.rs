use crate::{ResolvedSyncMutationBatch, SyncRaftRequest, SyncRaftResponse};

#[test]
fn sync_raft_payload_wraps_resolved_batch_and_responses() {
    let request = SyncRaftRequest::new(ResolvedSyncMutationBatch::new(Vec::new()));
    let response = SyncRaftResponse::new(Vec::new());

    assert!(request.batch.is_empty());
    assert!(response.responses.is_empty());
}
