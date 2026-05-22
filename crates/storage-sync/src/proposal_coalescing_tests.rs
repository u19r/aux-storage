use storage_types::{ItemStreamVersion, TableName};

use crate::{
    ResolvedSyncMutation, ResolvedSyncMutationBatch, SyncCreateTableMutation, SyncItemBaseVersion,
    SyncMutationId, SyncMutationResponse, SyncProposalBatch, SyncProposalCoalescingDecision,
    SyncProposalCoalescingGate, SyncProposalId, SyncPutMutation, SyncReadSet,
    plan_proposal_coalescing,
};

#[test]
fn given_disjoint_item_batches_when_planning_coalescing_then_allows_grouping() {
    let left = item_proposal("left", "table", "item#1", 1);
    let right = item_proposal("right", "table", "item#2", 2);

    assert_eq!(
        plan_proposal_coalescing(SyncProposalCoalescingGate {
            left: &left,
            right: &right,
        }),
        SyncProposalCoalescingDecision::Coalesce
    );
}

#[test]
fn given_same_key_writes_when_planning_coalescing_then_rejects_write_conflict() {
    let left = item_proposal("left", "table", "item#1", 1);
    let right = item_proposal("right", "table", "item#1", 2);

    assert_eq!(
        plan_proposal_coalescing(SyncProposalCoalescingGate {
            left: &left,
            right: &right,
        }),
        SyncProposalCoalescingDecision::RejectWriteWriteConflict
    );
}

#[test]
fn given_right_read_depends_on_left_write_when_planning_coalescing_then_rejects_stale_read() {
    let left = item_proposal("left", "table", "item#1", 1);
    let right = item_proposal("right", "table", "item#2", 2).with_read_set(SyncReadSet::new(vec![
        SyncItemBaseVersion {
            table_name: TableName::new("table"),
            key_json: key_json("item#1"),
            item_stream_version: None,
        },
    ]));

    assert_eq!(
        plan_proposal_coalescing(SyncProposalCoalescingGate {
            left: &left,
            right: &right,
        }),
        SyncProposalCoalescingDecision::RejectStaleReadDependency
    );
}

#[test]
fn given_lifecycle_mutation_when_planning_coalescing_then_rejects_boundary() {
    let left = item_proposal("left", "table", "item#1", 1);
    let right = SyncProposalBatch::new(
        SyncProposalId::new("right").expect("proposal id"),
        ResolvedSyncMutationBatch::new(vec![ResolvedSyncMutation::CreateTable(
            SyncCreateTableMutation {
                mutation_id: SyncMutationId::new("right#create").expect("mutation id"),
                table_name: TableName::new("table"),
                request_json: "{}".to_string(),
            },
        )]),
    );

    assert_eq!(
        plan_proposal_coalescing(SyncProposalCoalescingGate {
            left: &left,
            right: &right,
        }),
        SyncProposalCoalescingDecision::RejectLifecycleBoundary
    );
}

fn item_proposal(id: &str, table: &str, key: &str, version: u64) -> SyncProposalBatch {
    SyncProposalBatch::new(
        SyncProposalId::new(id).expect("proposal id"),
        ResolvedSyncMutationBatch::new(vec![ResolvedSyncMutation::Put(SyncPutMutation {
            mutation_id: SyncMutationId::new(format!("{id}#put")).expect("mutation id"),
            table_name: TableName::new(table),
            key_json: key_json(key),
            item_json: format!(r#"{{"pk":{{"S":"{key}"}}}}"#),
            old_item_json: None,
            target_item_stream_version: ItemStreamVersion::new(version),
            response: SyncMutationResponse::default(),
        })]),
    )
}

fn key_json(key: &str) -> String {
    format!(r#"{{"pk":{{"S":"{key}"}}}}"#)
}
