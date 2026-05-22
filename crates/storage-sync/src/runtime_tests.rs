use crate::{SyncRaftRole, runtime::role_from_server_state};

#[test]
fn server_state_maps_to_public_sync_role() {
    assert_eq!(
        role_from_server_state(openraft::ServerState::Leader),
        SyncRaftRole::Leader
    );
    assert_eq!(
        role_from_server_state(openraft::ServerState::Shutdown),
        SyncRaftRole::Disabled
    );
}
