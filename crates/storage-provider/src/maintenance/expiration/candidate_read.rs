//! Observe one bounded expiration page through the common Dynamo item read
//! contract.

use storage_types::{BoundTableInfo, StorageError, StorageResult, maintenance::TargetIncarnation};

use super::{
    ExpirationCandidateBatch, ExpirationCandidateObservation, ExpirationCurrentObservation,
    ExpirationCurrentRead, ExpirationObservedItem,
};
use crate::GetItemAttempt;

pub async fn read_expiration_candidates_in_attempt<A: GetItemAttempt>(
    attempt: &mut A,
    table: &BoundTableInfo,
    read: &ExpirationCurrentRead,
) -> StorageResult<ExpirationCurrentObservation> {
    if table.info.table_name != read.table_name
        || table.history_id != read.table_info.history_id
        || TargetIncarnation::from(*table.history_id.as_ref()) != read.table_incarnation
    {
        return Err(StorageError::internal("expiration candidate table changed"));
    }
    let batch = read_expiration_candidate_items_in_attempt(attempt, table, &read.batch).await?;
    Ok(ExpirationCurrentObservation {
        table_name: read.table_name.clone(),
        table_incarnation: read.table_incarnation.clone(),
        policy_generation: read.policy_generation,
        batch,
    })
}

pub async fn read_expiration_candidate_items_in_attempt<A: GetItemAttempt>(
    attempt: &mut A,
    table: &BoundTableInfo,
    batch: &ExpirationCandidateBatch,
) -> StorageResult<ExpirationCandidateObservation> {
    let pairs = batch
        .candidates
        .iter()
        .map(|candidate| (table, &candidate.key))
        .collect::<Vec<_>>();
    let items = attempt.read_primary_pairs(&pairs).await?;
    if items.len() != pairs.len() {
        return Err(StorageError::internal(
            "expiration candidate read count changed",
        ));
    }
    let present = items
        .into_iter()
        .enumerate()
        .filter_map(|(candidate_index, item)| {
            item.map(|item| ExpirationObservedItem {
                candidate_index,
                item,
            })
        })
        .collect();
    Ok(ExpirationCandidateObservation {
        candidate_count: pairs.len(),
        present,
    })
}
