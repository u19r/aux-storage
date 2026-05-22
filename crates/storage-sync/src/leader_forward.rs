#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncLeaderForward {
    pub local_is_leader: bool,
    pub leader_hint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncLeaderForwardDecision {
    Serve,
    NotLeader { leader_hint: Option<String> },
}

#[must_use]
pub fn plan_leader_forward(forward: SyncLeaderForward) -> SyncLeaderForwardDecision {
    if forward.local_is_leader {
        SyncLeaderForwardDecision::Serve
    } else {
        SyncLeaderForwardDecision::NotLeader {
            leader_hint: forward.leader_hint,
        }
    }
}
