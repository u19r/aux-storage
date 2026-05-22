use storage_types::TableName;

use crate::{ResolvedSyncMutation, SyncProposalBatch};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncProposalCoalescingGate<'a> {
    pub left: &'a SyncProposalBatch,
    pub right: &'a SyncProposalBatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncProposalCoalescingDecision {
    Coalesce,
    RejectLifecycleBoundary,
    RejectWriteWriteConflict,
    RejectStaleReadDependency,
}

#[must_use]
pub fn plan_proposal_coalescing(
    gate: SyncProposalCoalescingGate<'_>,
) -> SyncProposalCoalescingDecision {
    if has_lifecycle_mutation(gate.left) || has_lifecycle_mutation(gate.right) {
        return SyncProposalCoalescingDecision::RejectLifecycleBoundary;
    }
    if has_write_write_conflict(gate.left, gate.right) {
        return SyncProposalCoalescingDecision::RejectWriteWriteConflict;
    }
    if has_stale_read_dependency(gate.left, gate.right) {
        return SyncProposalCoalescingDecision::RejectStaleReadDependency;
    }
    SyncProposalCoalescingDecision::Coalesce
}

fn has_lifecycle_mutation(batch: &SyncProposalBatch) -> bool {
    batch
        .batch
        .mutations
        .iter()
        .any(|mutation| mutation_key(mutation).is_none())
}

fn has_write_write_conflict(left: &SyncProposalBatch, right: &SyncProposalBatch) -> bool {
    left.batch.mutations.iter().any(|left_mutation| {
        let Some(left_key) = mutation_key(left_mutation) else {
            return false;
        };
        right.batch.mutations.iter().any(|right_mutation| {
            mutation_key(right_mutation).is_some_and(|right_key| left_key == right_key)
        })
    })
}

fn has_stale_read_dependency(left: &SyncProposalBatch, right: &SyncProposalBatch) -> bool {
    left.batch.mutations.iter().any(|left_mutation| {
        let Some(left_key) = mutation_key(left_mutation) else {
            return false;
        };
        right.read_set.items.iter().any(|right_read| {
            left_key == SyncItemKey::new(&right_read.table_name, &right_read.key_json)
        })
    })
}

fn mutation_key(mutation: &ResolvedSyncMutation) -> Option<SyncItemKey<'_>> {
    match mutation {
        ResolvedSyncMutation::Put(mutation) => {
            Some(SyncItemKey::new(&mutation.table_name, &mutation.key_json))
        }
        ResolvedSyncMutation::Delete(mutation) => {
            Some(SyncItemKey::new(&mutation.table_name, &mutation.key_json))
        }
        ResolvedSyncMutation::CreateTable(_)
        | ResolvedSyncMutation::UpdateTable(_)
        | ResolvedSyncMutation::DeleteTable(_)
        | ResolvedSyncMutation::UpdateTimeToLive(_) => None,
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct SyncItemKey<'a> {
    table_name: &'a TableName,
    key_json: &'a str,
}

impl<'a> SyncItemKey<'a> {
    const fn new(table_name: &'a TableName, key_json: &'a str) -> Self {
        Self {
            table_name,
            key_json,
        }
    }
}
