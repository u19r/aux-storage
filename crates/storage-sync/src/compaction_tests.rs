use crate::{
    SyncCompactionBlockReason, SyncCompactionRetentionDecision, SyncCompactionRetentionGate,
    plan_sync_compaction_retention,
};

#[test]
fn compaction_retention_blocks_purging_entries_or_stream_records_needed_by_learners() {
    assert_eq!(
        plan_sync_compaction_retention(SyncCompactionRetentionGate {
            requested_log_compaction_index: 50,
            requested_stream_trim_index: 10,
            oldest_active_learner_log_index: Some(50),
            oldest_active_stream_index: Some(100),
        }),
        SyncCompactionRetentionDecision::Block(
            SyncCompactionBlockReason::ActiveLearnerNeedsLogEntry
        )
    );
    assert_eq!(
        plan_sync_compaction_retention(SyncCompactionRetentionGate {
            requested_log_compaction_index: 49,
            requested_stream_trim_index: 100,
            oldest_active_learner_log_index: Some(50),
            oldest_active_stream_index: Some(100),
        }),
        SyncCompactionRetentionDecision::Block(
            SyncCompactionBlockReason::ActiveLearnerNeedsStreamRecord
        )
    );
    assert_eq!(
        plan_sync_compaction_retention(SyncCompactionRetentionGate {
            requested_log_compaction_index: 49,
            requested_stream_trim_index: 99,
            oldest_active_learner_log_index: Some(50),
            oldest_active_stream_index: Some(100),
        }),
        SyncCompactionRetentionDecision::Allow
    );
}
