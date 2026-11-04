use storage_types::{StreamRetentionDuration, TimestampMillis, storage_serde};

use crate::plan_table_stream_duration;

#[test]
fn table_duration_plan_schedules_finite_and_skips_forever_due_marker() {
    let finite = plan_table_stream_duration(
        crate::retention_test_support::scope("orders"),
        crate::retention_test_support::frontier(7),
        StreamRetentionDuration::from_hours(24).unwrap(),
        TimestampMillis::from_timestamp(1_000),
    )
    .unwrap();

    assert_eq!(
        finite.state.next_due_at,
        Some(TimestampMillis::from_timestamp(86_401_000))
    );
    assert!(finite.next_marker.is_some());

    let forever = plan_table_stream_duration(
        crate::retention_test_support::scope("orders"),
        crate::retention_test_support::frontier(8),
        StreamRetentionDuration::Forever,
        TimestampMillis::from_timestamp(1_000),
    )
    .unwrap();
    assert_eq!(forever.state.next_due_at, None);
    assert!(forever.next_marker.is_none());
}

#[test]
fn table_duration_plan_schedules_default_shrink_and_expansion() {
    let default_plan = plan_table_stream_duration(
        crate::retention_test_support::scope("orders"),
        crate::retention_test_support::frontier(1),
        StreamRetentionDuration::default(),
        TimestampMillis::from_timestamp(1_000),
    )
    .unwrap();
    assert_eq!(
        default_plan.state.next_due_at,
        Some(TimestampMillis::from_timestamp(259_201_000))
    );

    let shrink = plan_table_stream_duration(
        crate::retention_test_support::scope("orders"),
        crate::retention_test_support::frontier(2),
        StreamRetentionDuration::from_hours(24).unwrap(),
        TimestampMillis::from_timestamp(1_000),
    )
    .unwrap();
    let expansion = plan_table_stream_duration(
        crate::retention_test_support::scope("orders"),
        crate::retention_test_support::frontier(3),
        StreamRetentionDuration::from_hours(168).unwrap(),
        TimestampMillis::from_timestamp(1_000),
    )
    .unwrap();

    assert!(shrink.state.next_due_at < default_plan.state.next_due_at);
    assert!(expansion.state.next_due_at > default_plan.state.next_due_at);
    assert_ne!(shrink.state.policy_version, expansion.state.policy_version);
}

#[test]
fn given_qualified_frontier_when_authority_is_corrupt_then_reject_state() {
    let write = plan_table_stream_duration(
        crate::retention_test_support::scope("orders"),
        crate::retention_test_support::frontier(7),
        StreamRetentionDuration::default(),
        TimestampMillis::from_timestamp(0),
    )
    .unwrap();
    let encoded = storage_serde::to_bytes(&write.state).unwrap();
    let state: crate::StreamTrimState = storage_serde::from_bytes(&encoded).unwrap();
    state.require_valid_frontier().unwrap();
    let mut wrong_generation = state.clone();
    wrong_generation.frontier.table_policy_version = std::num::NonZeroU64::MIN;
    assert!(wrong_generation.require_valid_frontier().is_err());
    let mut wrong_table = state.clone();
    wrong_table.frontier.table_history_id = 2_u32.into();
    assert!(wrong_table.require_valid_frontier().is_err());
    let mut duplicate_lane = state.clone();
    duplicate_lane
        .frontier
        .lineages
        .push(state.frontier.lineages[0].clone());
    assert!(duplicate_lane.require_valid_frontier().is_err());
    let mut cleaned_without_retirement = state;
    cleaned_without_retirement.frontier.lineages[0].cleaned_through =
        Some(storage_types::SystemStreamOffset::try_new("1").unwrap());
    assert!(cleaned_without_retirement.require_valid_frontier().is_err());
}
