use storage_types::{ItemStreamVersion, StreamRetentionDuration, TableName, TimestampMillis};

use crate::{
    StreamTrimDueMarker, StreamTrimMarkerOutcome, StreamTrimScope, StreamTrimState, due_bucket_for,
    next_due_from_first_remaining,
};

#[test]
fn due_bucket_rounds_down_to_bucket_boundary() {
    let due_at = TimestampMillis::from_timestamp(7_199_999);

    assert_eq!(
        due_bucket_for(due_at, 3_600_000),
        TimestampMillis::from_timestamp(3_600_000)
    );
}

#[test]
fn next_due_uses_first_remaining_timestamp_and_finite_retention() {
    let first_remaining = TimestampMillis::from_timestamp(10_000);

    assert_eq!(
        next_due_from_first_remaining(
            Some(first_remaining),
            StreamRetentionDuration::FiniteHours(2),
        ),
        Some(TimestampMillis::from_timestamp(7_210_000))
    );
    assert_eq!(
        next_due_from_first_remaining(Some(first_remaining), StreamRetentionDuration::Forever),
        None
    );
    assert_eq!(
        next_due_from_first_remaining(None, StreamRetentionDuration::FiniteHours(2)),
        None
    );
}

#[test]
fn marker_validation_distinguishes_current_stale_and_forever_markers() {
    let table_name = TableName::new("trim_state_table");
    let scope = StreamTrimScope::table("table-1", table_name);
    let state = StreamTrimState {
        scope: scope.clone(),
        policy_version: 3,
        retention: StreamRetentionDuration::FiniteHours(24),
        effective_retention: StreamRetentionDuration::FiniteHours(24),
        next_due_at: Some(TimestampMillis::from_timestamp(3_600_000)),
        oldest_retained_version: Some(ItemStreamVersion::new(1)),
        oldest_retained_timestamp: Some(TimestampMillis::from_timestamp(1)),
        latest_version: Some(ItemStreamVersion::new(2)),
        latest_timestamp: Some(TimestampMillis::from_timestamp(2)),
        updated_at: TimestampMillis::from_timestamp(3),
    };
    let current =
        StreamTrimDueMarker::new(TimestampMillis::from_timestamp(3_600_000), scope.clone(), 3);
    let stale = StreamTrimDueMarker::new(TimestampMillis::from_timestamp(3_600_000), scope, 2);

    assert_eq!(
        state.validated_marker_outcome(&current),
        StreamTrimMarkerOutcome::Current
    );
    assert_eq!(
        state.validated_marker_outcome(&stale),
        StreamTrimMarkerOutcome::Stale
    );

    let forever = StreamTrimState {
        effective_retention: StreamRetentionDuration::Forever,
        ..state
    };
    assert_eq!(
        forever.validated_marker_outcome(&current),
        StreamTrimMarkerOutcome::Forever
    );
}
