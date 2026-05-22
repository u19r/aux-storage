#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncMembershipGate {
    pub old_config_committed: bool,
    pub joint_config_committed: bool,
    pub new_config_committed: bool,
    pub leader_has_quorum: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMembershipDecision {
    Activate,
    Block,
}

#[must_use]
pub const fn plan_membership_activation(gate: SyncMembershipGate) -> SyncMembershipDecision {
    if gate.old_config_committed
        && gate.joint_config_committed
        && gate.new_config_committed
        && gate.leader_has_quorum
    {
        SyncMembershipDecision::Activate
    } else {
        SyncMembershipDecision::Block
    }
}
