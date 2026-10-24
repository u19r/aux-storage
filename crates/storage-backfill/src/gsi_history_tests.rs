use storage_types::{
    IndexName, ItemStreamVersion, StreamItemId, TableName, TimestampMillis,
    maintenance::TargetIncarnation,
};
use stream_provider::{StreamDataType, StreamItem};

use crate::gsi::{
    GsiBuild, GsiBuildTarget, GsiCatchingUp, GsiChange, GsiFailureReason, GsiObservation,
    GsiReferencedHistory, GsiReferencedHistoryItem, GsiState, GsiStreamObservation,
    GsiStreamPageEnd,
};

#[test]
fn referenced_history_items_are_grouped_by_candidate() {
    let histories =
        GsiReferencedHistory::group([history_item(0, 2), history_item(0, 1), history_item(2, 3)])
            .expect("ordered referenced history is valid");

    assert_eq!(histories.len(), 2);
    assert_eq!(histories[0].candidate_ordinal, 0);
    assert_eq!(histories[0].items.len(), 2);
    assert_eq!(histories[1].candidate_ordinal, 2);
    assert_eq!(histories[1].items.len(), 1);
}

#[test]
fn referenced_history_items_reject_out_of_order_candidates() {
    assert!(GsiReferencedHistory::group([history_item(2, 1), history_item(1, 2)]).is_err());
}

#[test]
fn given_malformed_history_images_when_grouped_then_corruption_is_rejected() {
    for versions in [vec![3, 2, 1], vec![2, 2], vec![1, 2]] {
        let result = GsiReferencedHistory::group(
            versions.into_iter().map(|version| history_item(4, version)),
        );
        assert!(
            result.is_err(),
            "history must be a descending current/predecessor pair"
        );
    }
}

#[test]
fn stream_origin_does_not_imply_a_history_gap() {
    let high = checkpoint(1);
    let build = catching_up(checkpoint(0), high.clone());
    let change = build
        .decide_next_change(GsiObservation::Stream(GsiStreamObservation {
            expected_cursor: checkpoint(0),
            through: high.clone(),
            end: GsiStreamPageEnd::ReachedThrough,
            projections: Vec::new(),
        }))
        .unwrap();
    assert!(
        matches!(change, GsiChange::Persist { next_state: GsiState::CatchingUp(state), .. } if state.cursor == high)
    );
}

#[test]
fn missing_required_history_fails_closed_at_the_last_valid_cursor() {
    let required = checkpoint(1);
    let build = catching_up(required.clone(), checkpoint(3));
    assert!(
        matches!(build.block_unavailable_history(), GsiChange::Persist { next_state: GsiState::Blocked(state), .. } if state.retained_from == required && state.reason == GsiFailureReason::HistoryUnavailable)
    );
}

fn checkpoint(offset: u64) -> storage_types::SystemStreamCheckpoint {
    storage_types::SystemStreamCheckpoint::try_new(
        storage_types::SystemStreamScope::try_new("gsi-history", 1).unwrap(),
        vec![
            storage_types::SystemStreamPosition::try_new("root", "root", format!("{offset:020}"))
                .unwrap(),
        ],
    )
    .unwrap()
}

fn catching_up(
    cursor: storage_types::SystemStreamCheckpoint,
    high: storage_types::SystemStreamCheckpoint,
) -> GsiBuild<String> {
    GsiBuild {
        target: GsiBuildTarget {
            table_name: TableName::new("gsi_history"),
            index_name: IndexName::new("by_group"),
            incarnation: TargetIncarnation::from([1; 16]),
        },
        state: GsiState::CatchingUp(GsiCatchingUp { cursor, high }),
    }
}

fn history_item(candidate_ordinal: usize, version: u64) -> GsiReferencedHistoryItem {
    let id = StreamItemId::from(ItemStreamVersion::new(version));
    GsiReferencedHistoryItem {
        candidate_ordinal,
        item: StreamItem {
            id,
            stream_name: None,
            data: Vec::new(),
            data_type: StreamDataType::DynamoDbJson,
            created_at: TimestampMillis::from_timestamp(i64::try_from(version).expect("test ID")),
        },
    }
}
