use storage_types::{IndexName, TableName, maintenance::TargetIncarnation};

use crate::gsi::{
    GsiBuild, GsiBuildTarget, GsiCatchingUp, GsiChange, GsiFailureReason, GsiObservation, GsiState,
    GsiStreamObservation, GsiStreamPageEnd,
};

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
            history_id: 1u32.into(),
            table_name: TableName::new("gsi_history"),
            index_name: IndexName::new("by_group"),
            incarnation: TargetIncarnation::from([1; 16]),
        },
        state: GsiState::CatchingUp(GsiCatchingUp { cursor, high }),
    }
}
