use crate::{SyncStrongReadDecision, SyncStrongReadGate, plan_strong_read_gate};

#[test]
fn strong_read_gate_requires_quorum_and_applied_read_index() {
    assert_eq!(
        plan_strong_read_gate(SyncStrongReadGate {
            leader_has_quorum: false,
            read_index_applied: true,
        }),
        SyncStrongReadDecision::Block
    );
    assert_eq!(
        plan_strong_read_gate(SyncStrongReadGate {
            leader_has_quorum: true,
            read_index_applied: false,
        }),
        SyncStrongReadDecision::Block
    );
    assert_eq!(
        plan_strong_read_gate(SyncStrongReadGate {
            leader_has_quorum: true,
            read_index_applied: true,
        }),
        SyncStrongReadDecision::Serve
    );
}
