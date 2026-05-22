#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncCompactionRetentionGate {
    pub requested_log_compaction_index: u64,
    pub requested_stream_trim_index: u64,
    pub oldest_active_learner_log_index: Option<u64>,
    pub oldest_active_stream_index: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncCompactionRetentionDecision {
    Allow,
    Block(SyncCompactionBlockReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncCompactionBlockReason {
    ActiveLearnerNeedsLogEntry,
    ActiveLearnerNeedsStreamRecord,
}

#[must_use]
pub const fn plan_sync_compaction_retention(
    gate: SyncCompactionRetentionGate,
) -> SyncCompactionRetentionDecision {
    if let Some(required_log_index) = gate.oldest_active_learner_log_index
        && gate.requested_log_compaction_index >= required_log_index
    {
        return SyncCompactionRetentionDecision::Block(
            SyncCompactionBlockReason::ActiveLearnerNeedsLogEntry,
        );
    }
    if let Some(required_stream_index) = gate.oldest_active_stream_index
        && gate.requested_stream_trim_index >= required_stream_index
    {
        return SyncCompactionRetentionDecision::Block(
            SyncCompactionBlockReason::ActiveLearnerNeedsStreamRecord,
        );
    }
    SyncCompactionRetentionDecision::Allow
}
