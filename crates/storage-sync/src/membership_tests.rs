use crate::{SyncMembershipDecision, SyncMembershipGate, plan_membership_activation};

#[test]
fn membership_activation_requires_joint_and_new_config_with_quorum() {
    assert_eq!(
        plan_membership_activation(SyncMembershipGate {
            old_config_committed: true,
            joint_config_committed: false,
            new_config_committed: true,
            leader_has_quorum: true,
        }),
        SyncMembershipDecision::Block
    );
    assert_eq!(
        plan_membership_activation(SyncMembershipGate {
            old_config_committed: true,
            joint_config_committed: true,
            new_config_committed: true,
            leader_has_quorum: false,
        }),
        SyncMembershipDecision::Block
    );
    assert_eq!(
        plan_membership_activation(SyncMembershipGate {
            old_config_committed: true,
            joint_config_committed: true,
            new_config_committed: true,
            leader_has_quorum: true,
        }),
        SyncMembershipDecision::Activate
    );
}
