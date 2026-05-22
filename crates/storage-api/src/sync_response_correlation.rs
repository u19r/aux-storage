#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SyncResponseCorrelationGate {
    pub response_count: usize,
    pub index: usize,
    pub payload_present: bool,
    pub required: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SyncResponseCorrelationDecision {
    UseDefault,
    DecodePayload,
    MissingEntry,
    MissingPayload,
}

pub(crate) const fn plan_sync_response_correlation(
    gate: SyncResponseCorrelationGate,
) -> SyncResponseCorrelationDecision {
    if gate.index >= gate.response_count {
        return if gate.required {
            SyncResponseCorrelationDecision::MissingEntry
        } else {
            SyncResponseCorrelationDecision::UseDefault
        };
    }
    if gate.payload_present {
        SyncResponseCorrelationDecision::DecodePayload
    } else if gate.required {
        SyncResponseCorrelationDecision::MissingPayload
    } else {
        SyncResponseCorrelationDecision::UseDefault
    }
}
