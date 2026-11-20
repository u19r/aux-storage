//! Pair end: leave, finish, force (design B R5 pair-end cleanup; REG-0 decision
//! 3).

use std::{cmp::Ordering, sync::Arc};

use async_trait::async_trait;
use regional_replication::message::{
    FinishPair, LineageHead, LineageRef, PairTable, RegionalRejection, RegionalResponse,
};
use storage_types::{BoundTableInfo, PairId, PairState, RegionalBinding, StorageResult, TableName};

use crate::{
    CatalogAttempt, DynamoPort, StreamRetentionPort,
    regional_pairing::{RegionalPairingAttempt, advance_pair_state_in_attempt},
    regional_progress::ProgressDirection,
    regional_receive::{load_paired_table, offset_order},
    table_history_read::TableHistoryReadState,
};

/// End the pair of `table` with its cleanup, in the caller's attempt (staged;
/// the caller commits).
pub(crate) async fn unpair_in_attempt<A: RegionalPairingAttempt>(
    attempt: &mut A,
    table: &BoundTableInfo,
) -> StorageResult<()> {
    attempt.clear_table_regional_progress(table)?;
    attempt.clear_regional_markers(table)?;
    attempt.store_regional_binding(table, None).await?;
    attempt.finish_dynamo_write().await
}

/// `pair`: end only that pair (`None`: any pair, the restore procedure). False
/// when nothing matched.
pub(crate) async fn force_unpair_in_attempt<A: RegionalPairingAttempt>(
    attempt: &mut A,
    name: &TableName,
    pair: Option<PairId>,
) -> StorageResult<bool> {
    // Before the first read: the cleanup must see every marker and progress
    // row committed before the lock (`PAIR_END_CHANGE`).
    attempt.lock_catalog_for_change(name).await?;
    let Some(table) = attempt.observe_table_update(name).await? else {
        return Ok(false);
    };
    let matches = match (table.binding.regional.as_deref(), pair) {
        (None, _) => false,
        (Some(_), None) => true,
        (Some(binding), Some(pair)) => binding.pair == pair,
    };
    if !matches {
        return Ok(false);
    }
    unpair_in_attempt(attempt, &table).await?;
    Ok(true)
}

/// The current history heads of a paired table (read after any committed state
/// change).
pub(crate) async fn history_heads_in_attempt<A>(
    attempt: &mut A,
    name: &TableName,
    pair: PairId,
) -> StorageResult<Option<Vec<LineageHead>>>
where
    A: CatalogAttempt + StreamRetentionPort,
{
    let Some(table) = load_paired_table(attempt, name, pair).await? else {
        return Ok(None);
    };
    let history = TableHistoryReadState::load(attempt, &table).await?;
    Ok(Some(
        history
            .lanes
            .iter()
            .map(|lane| LineageHead {
                lineage: LineageRef {
                    shard: lane.lane.shard_id().clone(),
                    lineage: lane.lane.lineage_id().clone(),
                },
                through: Some(lane.through.offset().clone()),
                sealed: lane.lineage.sealed(),
            })
            .collect(),
    ))
}

/// Receiver of `Finish`: end the pair when every head of `last` is received.
pub(crate) async fn finish_pair_in_attempt<A: RegionalPairingAttempt>(
    attempt: &mut A,
    request: &FinishPair,
) -> StorageResult<RegionalResponse> {
    // Before the first read: the progress check and the cleanup must see every
    // row committed before the lock (`PAIR_END_CHANGE`).
    attempt.lock_catalog_for_change(&request.table).await?;
    let Some(table) = load_paired_table(attempt, &request.table, request.pair).await? else {
        return Ok(RegionalResponse::Rejected(RegionalRejection::UnknownPair));
    };
    let state = table
        .binding
        .regional
        .as_deref()
        .map(|binding| binding.state);
    if !matches!(state, Some(PairState::Active | PairState::Leaving)) {
        return Ok(RegionalResponse::Rejected(RegionalRejection::Conflict));
    }
    let lineages = request
        .last
        .iter()
        .map(|head| head.lineage.clone())
        .collect::<Vec<_>>();
    let cursors = attempt
        .read_regional_progress(&table, request.pair, ProgressDirection::Receive, &lineages)
        .await?;
    for (head, cursor) in request.last.iter().zip(cursors) {
        let through = cursor.and_then(|cursor| cursor.through);
        if offset_order(through.as_ref(), head.through.as_ref())? == Ordering::Less {
            return Ok(RegionalResponse::Pending);
        }
    }
    unpair_in_attempt(attempt, &table).await?;
    Ok(RegionalResponse::Finished)
}

#[async_trait]
pub trait RegionalUnpairOperations: Send + Sync {
    /// The table's binding, whatever its pair (read-only).
    async fn current_binding(&self, table: &TableName) -> StorageResult<Option<RegionalBinding>>;
    /// Leaving region: Active → Leaving (`Some(Leaving)` also when already
    /// Leaving).
    async fn begin_leave(&self, target: &PairTable) -> StorageResult<Option<PairState>>;
    async fn history_heads(
        &self,
        table: &TableName,
        pair: PairId,
    ) -> StorageResult<Option<Vec<LineageHead>>>;
    async fn finish_pair(&self, request: FinishPair) -> StorageResult<RegionalResponse>;
    async fn force_unpair(&self, table: &TableName, pair: Option<PairId>) -> StorageResult<bool>;
}

#[async_trait]
impl<P: DynamoPort> RegionalUnpairOperations for P
where for<'native> P::Attempt<'native>: RegionalPairingAttempt
{
    async fn current_binding(&self, table: &TableName) -> StorageResult<Option<RegionalBinding>> {
        let name = table.clone();
        DynamoPort::run(
            self,
            crate::constants::POINT_READ,
            Arc::new(name),
            |attempt, name, _| {
                Box::pin(async move {
                    let Some(stamp) = attempt.read_catalog_stamp(name).await? else {
                        return Ok(None);
                    };
                    let table = attempt.read_table_at_stamp(name, stamp).await?;
                    Ok(table.binding.regional.as_deref().cloned())
                })
            },
        )
        .await
    }

    async fn begin_leave(&self, target: &PairTable) -> StorageResult<Option<PairState>> {
        let name = target.table.clone();
        let pair = target.pair;
        let state = storage_common::retry::retry_tries(
            self,
            self.attempt_deadlines(),
            crate::constants::SCHEMA_CHANGE,
            |attempt_request, _| {
                let name = name.clone();
                async move {
                    self.with_attempt(
                        attempt_request.scoped(storage_types::AttemptScope::Schema),
                        move |attempt| {
                            Box::pin(async move {
                                advance_pair_state_in_attempt(
                                    attempt,
                                    &name,
                                    pair,
                                    PairState::Active,
                                    PairState::Leaving,
                                )
                                .await
                            })
                        },
                    )
                    .await
                }
            },
        )
        .await?;
        self.metadata_cache().invalidate(&target.table);
        Ok(state)
    }

    async fn history_heads(
        &self,
        table: &TableName,
        pair: PairId,
    ) -> StorageResult<Option<Vec<LineageHead>>> {
        let name = table.clone();
        DynamoPort::run(
            self,
            crate::constants::BACKGROUND_READ_PAGE,
            Arc::new((name, pair)),
            |attempt, (name, pair), _| Box::pin(history_heads_in_attempt(attempt, name, *pair)),
        )
        .await
    }

    async fn finish_pair(&self, request: FinishPair) -> StorageResult<RegionalResponse> {
        let name = request.table.clone();
        let request = std::sync::Arc::new(request);
        let response = storage_common::retry::retry_tries(
            self,
            self.attempt_deadlines(),
            crate::constants::PAIR_END_CHANGE,
            |attempt_request, _| {
                let request = std::sync::Arc::clone(&request);
                async move {
                    self.with_attempt(
                        attempt_request.scoped(storage_types::AttemptScope::Schema),
                        move |attempt| {
                            Box::pin(async move { finish_pair_in_attempt(attempt, &request).await })
                        },
                    )
                    .await
                }
            },
        )
        .await?;
        self.metadata_cache().invalidate(&name);
        Ok(response)
    }

    async fn force_unpair(&self, table: &TableName, pair: Option<PairId>) -> StorageResult<bool> {
        let name = table.clone();
        let changed = storage_common::retry::retry_tries(
            self,
            self.attempt_deadlines(),
            crate::constants::PAIR_END_CHANGE,
            |attempt_request, _| {
                let name = name.clone();
                async move {
                    self.with_attempt(
                        attempt_request.scoped(storage_types::AttemptScope::Schema),
                        move |attempt| {
                            Box::pin(
                                async move { force_unpair_in_attempt(attempt, &name, pair).await },
                            )
                        },
                    )
                    .await
                }
            },
        )
        .await?;
        self.metadata_cache().invalidate(table);
        Ok(changed)
    }
}
