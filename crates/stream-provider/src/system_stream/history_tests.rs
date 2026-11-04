//! Pure fault controls for logical page decisions. Native parity is a separate
//! gate.

use std::num::NonZeroU64;

use storage_types::{SystemStreamCheckpoint, SystemStreamPosition, SystemStreamScope};

use super::{
    SystemStreamHistory, SystemStreamHistoryBounds, SystemStreamHistoryObservation,
    SystemStreamHistoryPage, SystemStreamHistoryRecord, SystemStreamLineage,
    SystemStreamLineageCatalog,
};

fn history() -> SystemStreamHistory {
    let scope = SystemStreamScope::try_new("history-test", 1).unwrap();
    let root = SystemStreamLineage::try_new("root-shard", "root", None, true).unwrap();
    let left =
        SystemStreamLineage::try_new("left-shard", "left", Some("root".into()), false).unwrap();
    let right =
        SystemStreamLineage::try_new("right-shard", "right", Some("root".into()), false).unwrap();
    let bounds = [
        (&root, 1, 3, Some(3)),
        (&left, 7, 9, None),
        (&right, 7, 8, None),
    ]
    .into_iter()
    .map(|(lineage, initial, next, seal)| SystemStreamHistoryBounds {
        lineage_id: lineage.lineage_id().clone(),
        initial_sequence: NonZeroU64::new(initial).unwrap(),
        next_sequence: NonZeroU64::new(next).unwrap(),
        sealed_after: seal.and_then(NonZeroU64::new),
    })
    .collect();
    SystemStreamHistory::try_new(
        SystemStreamLineageCatalog::try_new(scope, vec![root, left, right]).unwrap(),
        bounds,
    )
    .unwrap()
}

fn position(lineage: &str, offset: u64) -> SystemStreamPosition {
    SystemStreamPosition::try_new(format!("{lineage}-shard"), lineage, format!("{offset:020}"))
        .unwrap()
}

fn checkpoint(positions: Vec<SystemStreamPosition>) -> SystemStreamCheckpoint {
    SystemStreamCheckpoint::try_new(
        SystemStreamScope::try_new("history-test", 1).unwrap(),
        positions,
    )
    .unwrap()
}

#[test]
fn given_versionstamped_history_when_capturing_high_then_exact_commit_ordinals_survive() {
    let positions = [position("left", 8), position("right", 7)]
        .into_iter()
        .map(|position| {
            let version = position.offset().order().unwrap();
            let mut stamp = [0; 12];
            stamp[..8].copy_from_slice(&version.to_be_bytes());
            stamp[11] = 3;
            position.with_versionstamp(stamp).unwrap()
        })
        .collect::<Vec<_>>();
    let history = history().with_commit_ordering(positions.clone());
    assert!(history.uses_commit_versionstamps());
    assert_eq!(history.capture_high().unwrap().frontier(), positions);
}

#[test]
fn given_child_baseline_seven_when_parent_drains_then_children_start_at_seven() {
    let history = history();
    let high = history.capture_high().unwrap();
    let current = checkpoint(vec![position("root", 1)]);
    let parent = history.plan_page(&current, &high, 2).unwrap();
    assert_eq!(parent.as_ref().len(), 1);
    assert_eq!(parent.as_ref()[0].limit, 1);
    let current = finish(&parent, vec![vec![record("root", 2)]]).unwrap();
    let children = history.plan_page(&current, &high, 2).unwrap();
    assert_eq!(children.as_ref().len(), 1);
    assert_eq!(children.as_ref()[0].after, position("left", 6));
    let current = finish(&children, vec![vec![record("left", 7), record("left", 8)]]).unwrap();
    assert!(current.frontier().contains(&position("right", 6)));
    let right = history.plan_page(&current, &high, 2).unwrap();
    assert_eq!(right.as_ref()[0].after, position("right", 6));
    let current = finish(&right, vec![vec![record("right", 7)]]).unwrap();
    assert_eq!(current, high);
    assert!(
        history
            .plan_page(&current, &high, 2)
            .unwrap()
            .as_ref()
            .is_empty()
    );
}

#[test]
fn given_fixed_parent_high_when_children_exist_then_page_does_not_expand_past_high() {
    let history = history();
    let high = checkpoint(vec![position("root", 2)]);
    let plan = history.plan_page(&high, &high, 10).unwrap();
    assert!(plan.as_ref().is_empty());
    assert_eq!(plan.finish(&[]).unwrap(), high);
}

#[test]
fn given_missing_or_reordered_records_when_finishing_page_then_history_fails_closed() {
    let history = history();
    let high = history.capture_high().unwrap();
    let current = checkpoint(vec![position("root", 2)]);
    for rows in [
        vec![],
        vec![record("left", 8)],
        vec![record("left", 7)],
        vec![record("left", 8), record("left", 7)],
        vec![record("left", 7), record("left", 7)],
        vec![record("right", 7), record("right", 8)],
    ] {
        let plan = history.plan_page(&current, &high, 2).unwrap();
        assert!(finish(&plan, vec![rows]).is_err());
    }
}

#[test]
fn given_invalid_frontier_when_planning_then_no_read_is_issued() {
    let history = history();
    let high = history.capture_high().unwrap();
    for current in [
        checkpoint(vec![position("left", 6)]),
        checkpoint(vec![position("root", 2), position("left", 6)]),
        checkpoint(vec![position("left", 5), position("right", 6)]),
        checkpoint(vec![position("left", 9), position("right", 6)]),
    ] {
        assert!(history.plan_page(&current, &high, 10).is_err());
    }
    assert!(
        history
            .plan_page(&high, &checkpoint(vec![position("root", 2)]), 10)
            .is_err()
    );
}

fn record(lineage: &str, offset: u64) -> SystemStreamHistoryRecord {
    SystemStreamHistoryRecord {
        position: position(lineage, offset),
    }
}

#[test]
fn given_another_lineage_when_finishing_a_page_then_progress_is_rejected() {
    let history = history();
    let high = history.capture_high().unwrap();
    let current = checkpoint(vec![position("root", 1)]);
    let plan = history.plan_page(&current, &high, 2).unwrap();
    assert!(finish(&plan, vec![vec![record("other", 2)]]).is_err());
}

#[test]
fn given_empty_root_at_seven_when_capturing_then_initial_and_high_are_equal() {
    let scope = SystemStreamScope::try_new("history-test", 1).unwrap();
    let root = SystemStreamLineage::try_new("root-shard", "root", None, false).unwrap();
    let bounds = vec![SystemStreamHistoryBounds {
        lineage_id: root.lineage_id().clone(),
        initial_sequence: NonZeroU64::new(7).unwrap(),
        next_sequence: NonZeroU64::new(7).unwrap(),
        sealed_after: None,
    }];
    let history = SystemStreamHistory::try_new(
        SystemStreamLineageCatalog::try_new(scope, vec![root]).unwrap(),
        bounds,
    )
    .unwrap();
    let low = history.initial_checkpoint().unwrap();
    let high = history.capture_high().unwrap();
    assert_eq!(low, checkpoint(vec![position("root", 6)]));
    assert_eq!(low, high);
    assert_eq!(
        history
            .plan_page(&low, &high, 1)
            .unwrap()
            .finish(&[])
            .unwrap(),
        high
    );
}

#[test]
fn given_ordered_suffix_when_planning_then_existing_offset_contract_is_used() {
    let history = history();
    let current = checkpoint(vec![
        SystemStreamPosition::try_new("root-shard", "root", "00000000000000000001-a").unwrap(),
    ]);
    let high = checkpoint(vec![position("root", 2)]);
    let plan = history.plan_page(&current, &high, 1).unwrap();
    assert_eq!(finish(&plan, vec![vec![record("root", 2)]]).unwrap(), high);
}

#[test]
fn given_two_ready_children_when_budget_is_three_then_both_read_limits_are_bounded() {
    let history = history();
    let high = history.capture_high().unwrap();
    let current = checkpoint(vec![position("root", 2)]);
    let plan = history.plan_page(&current, &high, 3).unwrap();
    assert_eq!(plan.as_ref().len(), 2);
    assert_eq!(plan.as_ref()[0].limit, 2);
    assert_eq!(plan.as_ref()[1].limit, 1);
    assert_eq!(plan.as_ref()[0].initial_sequence.get(), 7);
    assert_eq!(
        finish(
            &plan,
            vec![
                vec![record("left", 7), record("left", 8)],
                vec![record("right", 7)]
            ]
        )
        .unwrap(),
        high
    );
}

fn finish(
    plan: &SystemStreamHistoryPage,
    rows: Vec<Vec<SystemStreamHistoryRecord>>,
) -> Result<SystemStreamCheckpoint, storage_types::SystemStreamContractError> {
    let observations = plan
        .as_ref()
        .iter()
        .zip(rows)
        .map(|(read, records)| SystemStreamHistoryObservation {
            read,
            records,
            retired_through: None,
        })
        .collect::<Vec<_>>();
    plan.finish(&observations)
}

#[test]
fn given_wrong_read_boundary_when_records_match_then_page_is_rejected() {
    let history = history();
    let current = checkpoint(vec![position("root", 1)]);
    let high = checkpoint(vec![position("root", 2)]);
    let plan = history.plan_page(&current, &high, 1).unwrap();
    let wrong_read = super::SystemStreamHistoryRead {
        ordering: super::SystemStreamHistoryOrdering::Contiguous,
        after: position("root", 1),
        through: 3,
        initial_sequence: NonZeroU64::MIN,
        limit: 1,
    };
    assert!(
        plan.finish(&[SystemStreamHistoryObservation {
            retired_through: None,
            read: &wrong_read,
            records: vec![record("root", 2)],
        }])
        .is_err()
    );
}

#[test]
fn given_suffixed_record_when_high_is_reached_then_private_progress_is_canonical() {
    let history = history();
    let current = checkpoint(vec![position("root", 1)]);
    let high = checkpoint(vec![position("root", 2)]);
    let plan = history.plan_page(&current, &high, 1).unwrap();
    let mut row = record("root", 2);
    row.position =
        SystemStreamPosition::try_new("root-shard", "root", "00000000000000000002-a").unwrap();
    assert_eq!(finish(&plan, vec![vec![row]]).unwrap(), high);
    let suffixed = checkpoint(vec![
        SystemStreamPosition::try_new("root-shard", "root", "00000000000000000002-a").unwrap(),
    ]);
    assert_eq!(
        history
            .plan_page(&suffixed, &high, 1)
            .unwrap()
            .finish(&[])
            .unwrap(),
        high
    );
}

#[test]
fn given_large_backlog_when_limit_is_unbounded_then_native_work_has_the_contract_cap() {
    let scope = SystemStreamScope::try_new("history-test", 1).unwrap();
    let root = SystemStreamLineage::try_new("root-shard", "root", None, false).unwrap();
    let history = SystemStreamHistory::try_new(
        SystemStreamLineageCatalog::try_new(scope, vec![root.clone()]).unwrap(),
        vec![SystemStreamHistoryBounds {
            lineage_id: root.lineage_id().clone(),
            initial_sequence: NonZeroU64::MIN,
            next_sequence: NonZeroU64::MAX,
            sealed_after: None,
        }],
    )
    .unwrap();
    let plan = history
        .plan_page(
            &history.initial_checkpoint().unwrap(),
            &history.capture_high().unwrap(),
            u32::MAX,
        )
        .unwrap();
    assert_eq!(plan.as_ref().len(), 1);
    assert_eq!(
        plan.as_ref()[0].limit,
        storage_types::SYSTEM_STREAM_RECORDS_LIMIT_MAX
    );
}

#[test]
fn given_retired_parent_and_children_when_a_fresh_checkpoint_is_issued_then_all_floors_survive() {
    let history = history();
    let retired = [
        position("root", 2),
        position("left", 7),
        position("right", 7),
    ];
    assert_eq!(
        history.retained_checkpoint(&retired).unwrap(),
        checkpoint(vec![position("left", 7), position("right", 7)])
    );
    for invalid in [
        position("unknown", 1),
        position("left", 99),
        SystemStreamPosition::try_new("other-shard", "left", "7").unwrap(),
    ] {
        assert!(history.retained_checkpoint(&[invalid]).is_err());
    }
}

fn sparse_history() -> SystemStreamHistory {
    let scope = SystemStreamScope::try_new("history-test", 1).unwrap();
    let root = SystemStreamLineage::try_new("root-shard", "root", None, false).unwrap();
    SystemStreamHistory::try_new(
        SystemStreamLineageCatalog::try_new(scope, vec![root]).unwrap(),
        vec![SystemStreamHistoryBounds {
            lineage_id: storage_types::SystemStreamLineageId::try_new("root").unwrap(),
            initial_sequence: NonZeroU64::MIN,
            next_sequence: NonZeroU64::new(101).unwrap(),
            sealed_after: None,
        }],
    )
    .unwrap()
    .with_sparse_numeric_ordering(vec![position("root", 90)])
}

#[test]
fn given_sparse_history_when_capturing_high_then_pending_versions_are_excluded() {
    let history = sparse_history();
    assert_eq!(
        history.capture_high().unwrap(),
        checkpoint(vec![position("root", 90)])
    );
}

#[test]
fn given_sparse_gaps_when_page_has_more_then_resume_at_exact_last_record() {
    let history = sparse_history();
    let high = history.capture_high().unwrap();
    let page = history
        .plan_page(&checkpoint(vec![position("root", 0)]), &high, 1)
        .unwrap();
    let next = page
        .finish_bounded(
            &[SystemStreamHistoryObservation {
                retired_through: None,
                read: &page.as_ref()[0],
                records: vec![record("root", 10)],
            }],
            &[true],
        )
        .unwrap();
    assert_eq!(next, checkpoint(vec![position("root", 10)]));
    let page = history.plan_page(&next, &high, 1).unwrap();
    let next = page
        .finish_bounded(
            &[SystemStreamHistoryObservation {
                retired_through: None,
                read: &page.as_ref()[0],
                records: vec![record("root", 80)],
            }],
            &[false],
        )
        .unwrap();
    assert_eq!(next, high);
}

#[test]
fn given_sparse_history_when_exhausted_without_records_then_advance_to_safe_high() {
    let history = sparse_history();
    let high = history.capture_high().unwrap();
    let page = history
        .plan_page(&checkpoint(vec![position("root", 0)]), &high, 8)
        .unwrap();
    assert_eq!(
        page.finish_bounded(
            &[SystemStreamHistoryObservation {
                retired_through: None,
                read: &page.as_ref()[0],
                records: vec![],
            }],
            &[false]
        )
        .unwrap(),
        high
    );
}

#[test]
fn given_sparse_history_when_rows_repeat_or_cross_high_then_reject_page() {
    let history = sparse_history();
    let high = history.capture_high().unwrap();
    let page = history
        .plan_page(&checkpoint(vec![position("root", 0)]), &high, 2)
        .unwrap();
    for rows in [
        vec![record("root", 10), record("root", 10)],
        vec![record("root", 91)],
    ] {
        assert!(
            page.finish_bounded(
                &[SystemStreamHistoryObservation {
                    retired_through: None,
                    read: &page.as_ref()[0],
                    records: rows,
                }],
                &[false]
            )
            .is_err()
        );
    }
}
