use std::{collections::BTreeSet, num::NonZeroU64};

use storage_types::{
    ItemHistoryAddress, ItemHistoryKind, ItemHistoryMetadata, ItemHistoryPublication,
    ItemStreamVersion, StreamName, SystemStreamOffset, SystemStreamPosition, SystemStreamScope,
    TableHistoryId, TimestampMillis,
};

use super::history_cleanup::{
    HistoryCleanupContinuation, ItemHistoryCleanupObservation, ItemHistoryCleanupPlan,
    ItemHistoryMetadataPage, TableHistoryTrimFrontier, TableHistoryTrimLineage,
};

fn address() -> ItemHistoryAddress {
    ItemHistoryAddress {
        history_id: TableHistoryId::from([3; 16]),
        stream_name: StreamName::new(b"orders/stream-item/a"),
    }
}

fn scope() -> SystemStreamScope {
    SystemStreamScope::try_new("source", 1).unwrap()
}

fn event(version: u64, previous: Option<u64>, lane: usize, deleted: bool) -> ItemHistoryMetadata {
    ItemHistoryMetadata::try_new(
        address(),
        ItemStreamVersion::new(version),
        previous.map(ItemStreamVersion::new),
        ItemHistoryPublication::Event {
            kind: if deleted {
                ItemHistoryKind::Deleted
            } else {
                ItemHistoryKind::Image
            },
            source_scope: scope(),
            table_position: SystemStreamPosition::try_new(
                "shard",
                format!("lane{lane}"),
                version.to_string(),
            )
            .unwrap(),
            created_at: TimestampMillis::from_timestamp(0),
        },
    )
    .unwrap()
}

fn frontier(cuts: [u64; 2]) -> TableHistoryTrimFrontier {
    TableHistoryTrimFrontier {
        item_history_sweep: None,
        table_history_id: address().history_id,
        table_policy_version: NonZeroU64::new(7).unwrap(),
        source_scope: scope(),
        lineages: cuts
            .into_iter()
            .enumerate()
            .map(|(lane, through)| TableHistoryTrimLineage {
                shard_id: storage_types::SystemStreamShardId::try_new("shard").unwrap(),
                lineage_id: format!("lane{lane}").try_into().unwrap(),
                retired_through: (through > 0)
                    .then(|| SystemStreamOffset::try_new(through.to_string()).unwrap()),
                cleaned_through: None,
            })
            .collect(),
    }
}

fn observation(
    entries: Vec<ItemHistoryMetadata>,
    reached_tail: bool,
    cuts: [u64; 2],
    live: Option<u64>,
) -> ItemHistoryCleanupObservation {
    ItemHistoryCleanupObservation::try_new(
        address(),
        frontier(cuts),
        ItemHistoryMetadataPage {
            entries,
            reached_tail,
        },
        live.map(ItemStreamVersion::new),
    )
    .unwrap()
}

#[test]
fn given_independent_lineages_when_newer_event_retires_first_then_older_event_is_protected() {
    let result = observation(
        vec![event(1, None, 0, false), event(2, Some(1), 1, true)],
        true,
        [0, 2],
        None,
    )
    .plan_cleanup()
    .unwrap();
    assert!(matches!(result, ItemHistoryCleanupPlan::Complete));
}

#[test]
fn given_retired_partial_page_when_cleaned_then_boundary_and_durable_work_remain() {
    let result = observation(
        vec![event(1, None, 0, false), event(2, Some(1), 1, false)],
        false,
        [9, 9],
        Some(4),
    )
    .plan_cleanup()
    .unwrap();
    let ItemHistoryCleanupPlan::Delete {
        history,
        through,
        expected_table_policy_version,
        continuation,
    } = result
    else {
        panic!("oldest expired row must progress")
    };
    assert_eq!(history, address());
    assert_eq!(through.get(), 1);
    assert_eq!(expected_table_policy_version.get(), 7);
    assert_eq!(continuation, HistoryCleanupContinuation::Repeat);
    let protected = observation(
        vec![event(2, Some(1), 1, false), event(4, Some(2), 0, false)],
        true,
        [1, 9],
        Some(4),
    )
    .plan_cleanup()
    .unwrap();
    assert!(matches!(protected, ItemHistoryCleanupPlan::Complete));
}

#[test]
fn given_final_delete_when_all_events_retire_then_last_marker_can_be_deleted() {
    let result = observation(vec![event(9, Some(4), 0, true)], true, [9, 0], None)
        .plan_cleanup()
        .unwrap();
    assert!(
        matches!(result, ItemHistoryCleanupPlan::Delete { through, continuation: HistoryCleanupContinuation::Complete, .. } if through.get() == 9)
    );
    let live = observation(vec![event(9, Some(4), 0, false)], true, [9, 0], Some(9))
        .plan_cleanup()
        .unwrap();
    assert!(matches!(live, ItemHistoryCleanupPlan::Complete));
}

#[test]
fn given_unpublished_anchor_when_its_event_is_retained_then_exact_old_image_survives() {
    let anchor = ItemHistoryMetadata::try_new(
        address(),
        ItemStreamVersion::new(3),
        None,
        ItemHistoryPublication::PredecessorAnchor,
    )
    .unwrap();
    let result = observation(
        vec![anchor, event(10, Some(3), 0, false)],
        true,
        [0, 0],
        Some(10),
    )
    .plan_cleanup()
    .unwrap();
    assert!(matches!(result, ItemHistoryCleanupPlan::Complete));
}

#[test]
fn given_unretired_event_with_missing_exact_predecessor_when_cleaned_then_rejected() {
    let result = observation(
        vec![event(2, Some(1), 1, true), event(10, Some(3), 0, false)],
        true,
        [0, 2],
        Some(10),
    )
    .plan_cleanup();
    assert!(result.is_err());
}

#[test]
fn given_delete_marker_as_predecessor_when_cleaned_then_rejected() {
    let result = observation(
        vec![event(2, Some(1), 1, true), event(10, Some(2), 0, false)],
        true,
        [0, 2],
        Some(10),
    )
    .plan_cleanup();
    assert!(result.is_err());
}

#[test]
fn given_unknown_scope_or_lineage_when_cleaned_then_event_is_not_assumed_retired() {
    let input = || ItemHistoryMetadataPage {
        entries: vec![event(1, None, 0, false)],
        reached_tail: true,
    };
    let mut wrong_scope = frontier([9, 9]);
    wrong_scope.source_scope = SystemStreamScope::try_new("other", 1).unwrap();
    let mut unknown_lineage = frontier([9, 9]);
    unknown_lineage.lineages.clear();
    for frontier in [wrong_scope, unknown_lineage] {
        let result = ItemHistoryCleanupObservation::try_new(address(), frontier, input(), None)
            .unwrap()
            .plan_cleanup()
            .unwrap();
        assert!(matches!(result, ItemHistoryCleanupPlan::Complete));
    }
}

#[test]
fn given_invalid_prefix_or_frontier_when_observed_then_rejected() {
    for (entries, reached_tail) in [
        (vec![], false),
        (vec![event(1, None, 0, false)], false),
        (
            vec![event(2, Some(1), 0, false), event(1, None, 0, false)],
            true,
        ),
        (
            vec![event(1, None, 0, false), event(1, None, 0, false)],
            true,
        ),
        ((1..=3).map(|v| event(v, None, 0, false)).collect(), true),
    ] {
        assert!(
            ItemHistoryCleanupObservation::try_new(
                address(),
                frontier([9, 9]),
                ItemHistoryMetadataPage {
                    entries,
                    reached_tail
                },
                None
            )
            .is_err()
        );
    }
    for retired in [None, Some(SystemStreamOffset::try_new("1").unwrap())] {
        let mut invalid = frontier([1, 1]);
        invalid.lineages[0].retired_through = retired;
        invalid.lineages[0].cleaned_through = Some(SystemStreamOffset::try_new("2").unwrap());
        assert!(
            ItemHistoryCleanupObservation::try_new(
                address(),
                invalid,
                ItemHistoryMetadataPage {
                    entries: vec![],
                    reached_tail: true
                },
                None
            )
            .is_err()
        );
    }
}

// Independent required-set oracle: no planner helper chooses expected rows or
// continuation. Retiring two lanes separately includes non-prefix retirement.
#[test]
fn given_all_lane_assignments_and_retirement_prefixes_when_cleaned_then_required_versions_and_work_survive()
 {
    for lanes in 0..64 {
        let events = [
            event(1, None, lanes & 1, false),
            event(2, Some(1), lanes >> 1 & 1, false),
            event(3, Some(2), lanes >> 2 & 1, true),
            event(4, None, lanes >> 3 & 1, false),
            event(5, Some(4), lanes >> 4 & 1, false),
            event(6, Some(5), lanes >> 5 & 1, true),
        ];
        for first in 0..=6 {
            for second in 0..=6 {
                for length in 1..=events.len() {
                    verify_all_safe_prefixes(&events[..length], [first, second]);
                }
            }
        }
    }
}

fn verify_all_safe_prefixes(events: &[ItemHistoryMetadata], cuts: [u64; 2]) {
    let live = events
        .last()
        .filter(|entry| entry.kind() == ItemHistoryKind::Image)
        .map(|entry| entry.version().get());
    let mut required = live.into_iter().collect::<BTreeSet<_>>();
    for event in events {
        let ItemHistoryPublication::Event { table_position, .. } = event.publication() else {
            unreachable!()
        };
        let lane = usize::from(table_position.lineage_id().as_str() == "lane1");
        if event.version().get() > cuts[lane] {
            required.insert(event.version().get());
            required.extend(event.previous_version().map(ItemStreamVersion::get));
        }
    }
    for purged in 0..=events.len() {
        let stored = &events[purged..];
        if required
            .iter()
            .any(|version| !stored.iter().any(|entry| entry.version().get() == *version))
        {
            continue;
        }
        let result = observation(
            stored.iter().take(2).cloned().collect(),
            stored.len() <= 2,
            cuts,
            live,
        )
        .plan_cleanup()
        .unwrap();
        verify_deletion(result, stored, &required);
    }
}

fn verify_deletion(
    result: ItemHistoryCleanupPlan,
    stored: &[ItemHistoryMetadata],
    required: &BTreeSet<u64>,
) {
    let (remaining, complete) = match result {
        ItemHistoryCleanupPlan::Complete => (stored, true),
        ItemHistoryCleanupPlan::Delete {
            through,
            continuation,
            ..
        } => {
            assert_eq!(through, stored[0].version());
            assert!(!required.contains(&through.get()));
            (
                &stored[1..],
                continuation == HistoryCleanupContinuation::Complete,
            )
        }
    };
    if complete {
        assert!(
            remaining
                .first()
                .is_none_or(|entry| required.contains(&entry.version().get())),
            "discarding work would strand an expired prefix"
        );
    }
}
