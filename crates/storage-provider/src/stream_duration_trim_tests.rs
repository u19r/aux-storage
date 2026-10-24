use storage_types::{
    ItemStreamVersion, StreamItemId, StreamName, StreamRetentionDuration, TableName,
    TimestampMillis, maintenance::TargetIncarnation,
};

use crate::{
    DetachedItemStreamObservation, DetachedItemStreamRange, DetachedItemStreamRanges,
    StreamTrimBoundary, StreamTrimDeletionBound, StreamTrimDueMarker, StreamTrimMarkerOutcome,
    StreamTrimPageLimit, StreamTrimScope, StreamTrimScopeBoundaries, StreamTrimState,
};

#[test]
fn due_bucket_rounds_down_to_bucket_boundary() {
    let due_at = TimestampMillis::from_timestamp(7_199_999);

    let scope = StreamTrimScope::table("table-1", TableName::new("trim_state_table"));
    let marker = StreamTrimDueMarker::new(due_at, scope, 1);

    assert_eq!(
        marker.due_bucket,
        TimestampMillis::from_timestamp(3_600_000)
    );
}

#[test]
fn stream_trim_page_limit_rejects_invalid_bounds() {
    assert!(StreamTrimPageLimit::try_from(0usize).is_err());
    assert!(StreamTrimPageLimit::try_from(10_000usize).is_ok());
    assert!(StreamTrimPageLimit::try_from(10_001usize).is_err());
}

#[test]
fn rescheduled_marker_leaves_the_entire_current_due_bucket() {
    let scope = StreamTrimScope::table("table-1", TableName::new("trim_state_table"));
    let marker = StreamTrimDueMarker::new(TimestampMillis::from_timestamp(3_600_000), scope, 1);

    let next = marker
        .rescheduled_after(
            TimestampMillis::from_timestamp(7_200_001),
            TimestampMillis::from_timestamp(7_200_000),
        )
        .expect("rescheduling should not overflow");

    assert_eq!(next.due_bucket, TimestampMillis::from_timestamp(10_800_000));
    assert!(next.due_bucket > TimestampMillis::from_timestamp(7_200_001));
}

#[test]
fn rescheduled_marker_rejects_timestamp_overflow() {
    let scope = StreamTrimScope::table("table-1", TableName::new("trim_state_table"));
    let marker = StreamTrimDueMarker::new(TimestampMillis::from_timestamp(0), scope, 1);

    let error = marker
        .rescheduled_after(
            TimestampMillis::from_timestamp(i64::MAX),
            TimestampMillis::from_timestamp(i64::MAX),
        )
        .err()
        .expect("rescheduling at the timestamp limit must fail closed");
    assert!(error.to_string().contains("overflow"));
}

#[test]
fn next_due_uses_first_remaining_timestamp_and_finite_retention() {
    let first_remaining = TimestampMillis::from_timestamp(10_000);
    let scope = StreamTrimScope::table("table-1", TableName::new("trim_state_table"));
    let mut finite = StreamTrimState {
        scope: scope.clone(),
        policy_version: 1,
        retention: StreamRetentionDuration::FiniteHours(2),
        effective_retention: StreamRetentionDuration::FiniteHours(2),
        next_due_at: None,
        oldest_retained_version: None,
        oldest_retained_timestamp: None,
        latest_version: None,
        latest_timestamp: None,
        updated_at: TimestampMillis::from_timestamp(0),
    };

    finite.finish_page(None, Some(first_remaining), first_remaining);
    assert_eq!(
        finite.next_due_at,
        Some(TimestampMillis::from_timestamp(7_210_000))
    );

    let mut forever = StreamTrimState {
        retention: StreamRetentionDuration::Forever,
        effective_retention: StreamRetentionDuration::Forever,
        ..finite
    };
    forever.finish_page(None, Some(first_remaining), first_remaining);
    assert_eq!(forever.next_due_at, None);
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

    assert!(matches!(
        state.validated_marker_outcome(&current),
        StreamTrimMarkerOutcome::Current
    ));
    assert!(matches!(
        state.validated_marker_outcome(&stale),
        StreamTrimMarkerOutcome::Stale
    ));

    let forever = StreamTrimState {
        effective_retention: StreamRetentionDuration::Forever,
        ..state
    };
    assert!(matches!(
        forever.validated_marker_outcome(&current),
        StreamTrimMarkerOutcome::Forever
    ));
}

#[test]
fn final_trim_page_clears_due_marker_but_keeps_monotonic_floor() {
    let scope = StreamTrimScope::table("table-1", TableName::new("trim_state_table"));
    let mut state = StreamTrimState {
        scope,
        policy_version: 1,
        retention: StreamRetentionDuration::FiniteHours(24),
        effective_retention: StreamRetentionDuration::FiniteHours(24),
        next_due_at: Some(TimestampMillis::from_timestamp(100)),
        oldest_retained_version: Some(ItemStreamVersion::new(9)),
        oldest_retained_timestamp: Some(TimestampMillis::from_timestamp(10)),
        latest_version: Some(ItemStreamVersion::new(10)),
        latest_timestamp: Some(TimestampMillis::from_timestamp(20)),
        updated_at: TimestampMillis::from_timestamp(20),
    };

    let next = state.finish_page(None, None, TimestampMillis::from_timestamp(30));

    assert!(next.is_none());
    assert_eq!(state.next_due_at, None);
    assert_eq!(
        state.oldest_retained_version,
        Some(ItemStreamVersion::new(9))
    );
    assert_eq!(
        state.oldest_retained_timestamp,
        Some(TimestampMillis::from_timestamp(10))
    );
}

#[test]
fn trim_floor_does_not_regress_from_an_older_remaining_row() {
    let scope = StreamTrimScope::table("table-1", TableName::new("trim_state_table"));
    let mut state = StreamTrimState {
        scope,
        policy_version: 1,
        retention: StreamRetentionDuration::FiniteHours(1),
        effective_retention: StreamRetentionDuration::FiniteHours(1),
        next_due_at: None,
        oldest_retained_version: Some(ItemStreamVersion::new(9)),
        oldest_retained_timestamp: Some(TimestampMillis::from_timestamp(10)),
        latest_version: Some(ItemStreamVersion::new(12)),
        latest_timestamp: Some(TimestampMillis::from_timestamp(20)),
        updated_at: TimestampMillis::from_timestamp(20),
    };

    let next = state.finish_page(
        Some(ItemStreamVersion::new(8)),
        Some(TimestampMillis::from_timestamp(5)),
        TimestampMillis::from_timestamp(30),
    );

    assert_eq!(
        state.oldest_retained_version,
        Some(ItemStreamVersion::new(9))
    );
    assert_eq!(
        state.oldest_retained_timestamp,
        Some(TimestampMillis::from_timestamp(10))
    );
    assert_eq!(
        state.next_due_at,
        Some(TimestampMillis::from_timestamp(3_600_010))
    );
    assert!(next.map(|marker| marker.scope) == Some(state.scope));
}

#[test]
fn minimum_stream_boundary_means_delete_nothing() {
    let boundary = StreamItemId::from([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]);
    let predecessor = StreamItemId::from([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    assert!(matches!(
        StreamTrimDeletionBound::for_reclamation(None, Some(boundary), None, None),
        StreamTrimDeletionBound::Inclusive(value) if value == predecessor
    ));
    assert!(matches!(
        StreamTrimDeletionBound::for_reclamation(None, Some(StreamItemId::default()), None, None,),
        StreamTrimDeletionBound::DeleteNone
    ));
    assert!(matches!(
        StreamTrimDeletionBound::for_reclamation(None, None, None, None),
        StreamTrimDeletionBound::Unbounded
    ));
}

#[test]
fn zero_boundary_wins_before_predecessor_conversion() {
    assert!(matches!(
        StreamTrimDeletionBound::for_reclamation(
            None,
            Some(StreamItemId::from([0; 12])),
            Some(StreamItemId::from([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 100])),
            None,
        ),
        StreamTrimDeletionBound::DeleteNone
    ));
}

#[test]
fn requested_page_bound_cannot_exceed_a_live_boundary() {
    let requested = StreamItemId::from([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 90]);
    let retained = StreamItemId::from([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 50]);
    let predecessor = StreamItemId::from([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 49]);
    assert!(matches!(
        StreamTrimDeletionBound::for_reclamation(
            Some(requested),
            None,
            None,
            Some(retained),
        ),
        StreamTrimDeletionBound::Inclusive(value) if value == predecessor
    ));
    assert!(matches!(
        StreamTrimDeletionBound::for_reclamation(
            Some(predecessor),
            None,
            None,
            Some(retained),
        ),
        StreamTrimDeletionBound::Inclusive(value) if value == predecessor
    ));
}

#[test]
fn given_live_reference_when_trimming_then_preserve_its_immediate_predecessor() {
    let id = |value| StreamItemId::from([value; 12]);
    let observation = crate::ItemHistoryRetention {
        latest: Some(id(9)),
        referenced: Some(id(7)),
        at_reference: [Some(id(7)), Some(id(3))],
    };
    let bound = StreamTrimDeletionBound::for_item_history(None, observation).unwrap();
    assert!(matches!(bound, StreamTrimDeletionBound::Inclusive(maximum) if maximum < id(3)));
}

#[test]
fn given_invalid_reference_observation_when_trimming_then_reject_deletion() {
    let id = |value| StreamItemId::from([value; 12]);
    for (latest, images) in [
        (Some(id(9)), [Some(id(6)), Some(id(3))]),
        (Some(id(9)), [None, None]),
        (None, [Some(id(7)), Some(id(3))]),
        (Some(id(6)), [Some(id(7)), Some(id(3))]),
        (Some(id(9)), [Some(id(7)), Some(id(7))]),
        (Some(id(9)), [Some(id(7)), Some(id(8))]),
    ] {
        let observation = crate::ItemHistoryRetention {
            latest,
            referenced: Some(id(7)),
            at_reference: images,
        };
        assert!(StreamTrimDeletionBound::for_item_history(None, observation).is_err());
    }
}

#[test]
fn given_no_references_when_trimming_then_preserve_the_latest_image() {
    let latest = StreamItemId::from([9; 12]);
    let observation = crate::ItemHistoryRetention {
        latest: Some(latest),
        referenced: None,
        at_reference: [None; 2],
    };
    let bound = StreamTrimDeletionBound::for_item_history(None, observation).unwrap();
    assert!(matches!(bound, StreamTrimDeletionBound::Inclusive(maximum) if maximum < latest));
}

fn detached_range(name: &str, item_id: u8) -> DetachedItemStreamRange {
    DetachedItemStreamRange {
        table_name: TableName::new("orders"),
        table_incarnation: TargetIncarnation::from([1; 4]),
        item_stream: StreamName::new(name.as_bytes()),
        max_item_id: StreamItemId::from([item_id; 12]),
    }
}

#[test]
fn detached_item_ranges_preserve_first_seen_order_and_highest_version() {
    let ranges = [
        detached_range("orders/stream-item/a", 1),
        detached_range("orders/stream-item/b", 2),
        detached_range("orders/stream-item/a", 3),
    ]
    .into_iter()
    .collect::<DetachedItemStreamRanges>();

    assert_eq!(
        ranges
            .iter()
            .map(|range| range.item_stream.as_ref())
            .collect::<Vec<_>>(),
        [
            b"orders/stream-item/a".as_slice(),
            b"orders/stream-item/b".as_slice()
        ]
    );
    assert_eq!(ranges[0].max_item_id, StreamItemId::from([3; 12]));
}

#[test]
fn detached_item_reclaim_uses_current_table_policy_and_all_live_boundaries() {
    let range = detached_range("orders/stream-item/a", 9);
    let scope = StreamTrimScope::item("orders/stream-item/a", range.table_name.clone(), "item-a");
    let request = DetachedItemStreamObservation {
        range,
        scope: scope.clone(),
        custom_state: Some(StreamTrimState {
            scope,
            policy_version: 1,
            retention: StreamRetentionDuration::FiniteHours(2),
            effective_retention: StreamRetentionDuration::FiniteHours(2),
            next_due_at: None,
            oldest_retained_version: None,
            oldest_retained_timestamp: None,
            latest_version: None,
            latest_timestamp: None,
            updated_at: TimestampMillis::from_timestamp(0),
        }),
        table_retention: StreamRetentionDuration::FiniteHours(4),
        default_item_retention: StreamRetentionDuration::FiniteHours(3),
        boundaries: StreamTrimScopeBoundaries {
            latest_item_id: Some(StreamItemId::from([8; 12])),
            protected_boundary: Some(StreamTrimBoundary {
                item_id: StreamItemId::from([7; 12]),
            }),
            retained_table_pointer_boundary: Some(StreamTrimBoundary {
                item_id: StreamItemId::from([6; 12]),
            }),
        },
    }
    .reclaim_request(
        TimestampMillis::from_timestamp(20_000_000),
        StreamTrimPageLimit::try_from(100usize).expect("valid page limit"),
    )
    .expect("observation should be valid")
    .expect("finite retention should produce work");

    assert_eq!(
        request.request.cutoff_timestamp,
        TimestampMillis::from_timestamp(5_600_000)
    );
    assert_eq!(
        request.request.max_deleted_item_id,
        Some(StreamItemId::from([6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 5]))
    );
    assert!(request.matches_current_policy(
        &TargetIncarnation::from([1; 4]),
        request.expected_custom_state.as_ref(),
        StreamRetentionDuration::FiniteHours(4),
        StreamRetentionDuration::FiniteHours(3),
    ));
    assert!(!request.matches_current_policy(
        &TargetIncarnation::from([1; 4]),
        request.expected_custom_state.as_ref(),
        StreamRetentionDuration::FiniteHours(5),
        StreamRetentionDuration::FiniteHours(3),
    ));
    assert!(!request.matches_current_policy(
        &TargetIncarnation::from([2; 4]),
        request.expected_custom_state.as_ref(),
        StreamRetentionDuration::FiniteHours(4),
        StreamRetentionDuration::FiniteHours(3),
    ));
}

#[test]
fn detached_item_reclaim_skips_forever_and_zero_boundary() {
    let forever_range = detached_range("orders/stream-item/a", 9);
    let forever_scope = StreamTrimScope::item(
        "orders/stream-item/a",
        forever_range.table_name.clone(),
        "item-a",
    );
    let forever = DetachedItemStreamObservation {
        range: forever_range,
        scope: forever_scope,
        custom_state: None,
        table_retention: StreamRetentionDuration::FiniteHours(1),
        default_item_retention: StreamRetentionDuration::Forever,
        boundaries: StreamTrimScopeBoundaries::unbounded(),
    };
    assert!(
        forever
            .reclaim_request(
                TimestampMillis::from_timestamp(20_000_000),
                StreamTrimPageLimit::try_from(100usize).expect("valid page limit"),
            )
            .expect("observation should be valid")
            .is_none()
    );

    let protected_range = detached_range("orders/stream-item/b", 9);
    let protected_scope = StreamTrimScope::item(
        "orders/stream-item/b",
        protected_range.table_name.clone(),
        "item-b",
    );
    let protected = DetachedItemStreamObservation {
        range: protected_range,
        scope: protected_scope,
        custom_state: None,
        table_retention: StreamRetentionDuration::FiniteHours(1),
        default_item_retention: StreamRetentionDuration::FiniteHours(1),
        boundaries: StreamTrimScopeBoundaries {
            latest_item_id: Some(StreamItemId::default()),
            protected_boundary: None,
            retained_table_pointer_boundary: None,
        },
    };
    assert!(
        protected
            .reclaim_request(
                TimestampMillis::from_timestamp(20_000_000),
                StreamTrimPageLimit::try_from(100usize).expect("valid page limit"),
            )
            .expect("observation should be valid")
            .is_none()
    );
}
