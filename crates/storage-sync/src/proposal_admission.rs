use crate::{SyncProposalPipelineLimits, SyncProposalShape};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncProposalAdmissionGate {
    pub shape: SyncProposalShape,
    pub in_flight: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncProposalAdmissionDecision {
    Admit,
    RejectOperationCount,
    RejectByteCount,
    RejectQueueFull,
}

#[must_use]
pub fn plan_proposal_admission(
    limits: SyncProposalPipelineLimits,
    gate: SyncProposalAdmissionGate,
) -> SyncProposalAdmissionDecision {
    if gate.shape.operation_count > limits.max_batch_operations {
        return SyncProposalAdmissionDecision::RejectOperationCount;
    }
    if gate.shape.byte_count > limits.max_batch_bytes {
        return SyncProposalAdmissionDecision::RejectByteCount;
    }
    if gate.in_flight >= limits.max_queue_depth {
        return SyncProposalAdmissionDecision::RejectQueueFull;
    }
    SyncProposalAdmissionDecision::Admit
}
