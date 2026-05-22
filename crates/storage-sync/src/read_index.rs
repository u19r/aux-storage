#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncStrongReadGate {
    pub leader_has_quorum: bool,
    pub read_index_applied: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncStrongReadDecision {
    Serve,
    Block,
}

#[must_use]
pub const fn plan_strong_read_gate(gate: SyncStrongReadGate) -> SyncStrongReadDecision {
    if gate.leader_has_quorum && gate.read_index_applied {
        SyncStrongReadDecision::Serve
    } else {
        SyncStrongReadDecision::Block
    }
}
