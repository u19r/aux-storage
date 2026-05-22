use crate::{SyncHealthResponse, SyncRaftRole};

#[test]
fn disabled_sync_health_snapshot_is_explicit() {
    let response = SyncHealthResponse::disabled();

    assert_eq!(response.role, SyncRaftRole::Disabled);
    assert!(response.local_node_id.is_none());
    assert!(response.voters.is_empty());
}
