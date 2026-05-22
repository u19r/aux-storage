use crate::{SyncLeaderForward, SyncLeaderForwardDecision, plan_leader_forward};

#[test]
fn leader_forward_serves_only_on_local_leader() {
    assert_eq!(
        plan_leader_forward(SyncLeaderForward {
            local_is_leader: true,
            leader_hint: Some("http://leader.test/storage".to_string()),
        }),
        SyncLeaderForwardDecision::Serve
    );
    assert_eq!(
        plan_leader_forward(SyncLeaderForward {
            local_is_leader: false,
            leader_hint: Some("http://leader.test/storage".to_string()),
        }),
        SyncLeaderForwardDecision::NotLeader {
            leader_hint: Some("http://leader.test/storage".to_string()),
        }
    );
}
