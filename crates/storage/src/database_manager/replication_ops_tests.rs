use storage_types::{
    ReplicationEventMetadata, ReplicationHybridLogicalClock, ReplicationWriteSource, StreamItemId,
};

use crate::{
    ReplicationMutationApplyOutcome,
    database_manager::replication_ops::evaluate_replication_apply_outcome,
};

#[test]
fn given_fast_clock_current_when_slow_clock_causally_later_write_arrives_then_lww_skips_it() {
    let fast_clock_current = replication_metadata("region-fast", 1, 2_000);
    let slow_clock_later = replication_metadata("region-slow", 2, 1_000);

    let outcome = evaluate_replication_apply_outcome(Some(&fast_clock_current), &slow_clock_later);

    assert_eq!(outcome, ReplicationMutationApplyOutcome::SkippedStale);
}

#[test]
fn given_slow_clock_current_when_fast_clock_write_arrives_then_lww_applies_it() {
    let slow_clock_current = replication_metadata("region-slow", 1, 1_000);
    let fast_clock_incoming = replication_metadata("region-fast", 2, 2_000);

    let outcome =
        evaluate_replication_apply_outcome(Some(&slow_clock_current), &fast_clock_incoming);

    assert_eq!(outcome, ReplicationMutationApplyOutcome::Applied);
}

fn replication_metadata(
    region_name: &str,
    sequence_suffix: u64,
    physical_ms: i64,
) -> ReplicationEventMetadata {
    let mut sequence_bytes = [0_u8; 12];
    sequence_bytes[4..].copy_from_slice(&sequence_suffix.to_be_bytes());
    ReplicationEventMetadata {
        origin_region: region_name.to_string(),
        origin_sequence: StreamItemId::from(sequence_bytes),
        origin_hlc: ReplicationHybridLogicalClock {
            physical_ms: physical_ms.into(),
            logical: 0,
        },
        origin_commit_ts: physical_ms.into(),
        table_replica_epoch: 1,
        write_source: ReplicationWriteSource::Replicated,
    }
}
