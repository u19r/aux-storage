//! Enable, prepare, scan, activate, sweep (design B R2, R3, R5; REG-0 states).

use std::{num::NonZeroU32, sync::Arc};

use async_trait::async_trait;
use regional_replication::message::{
    LineageHead, LineageRef, PairTable, PrepareTarget, RegionalRejection, RegionalResponse,
    ScanItem, ScanPage,
};
use storage_types::{
    BoundTableInfo, ItemKey, KeyAttributes, PairId, PairState, RegionId, RegionalBinding,
    RegionalMark, RegionalOrder, RegionalRole, StorageError, StorageResult, TableName, TableStatus,
    TimestampMillis,
};

use crate::{
    CachePublication, CatalogAttempt, DynamoPort, ItemWriteObservation, ItemWriteObservationRead,
    PrimaryStateScan, PrimaryStateScanAttempt, RegionalCatalogAttempt, StreamRetentionPort,
    TableMetadataCache,
    dynamo_create_table::{CreateTableAttempt, create_bound_table_in_attempt},
    regional_progress::{LineageCursor, ProgressDirection, RegionalProgressAttempt},
    regional_receive::{
        Incoming, ItemChange, KeyOutcome, KeyState, MarkerChange, RegionalReceiveAttempt,
        apply_key_outcome, group_by_key, load_paired_table, load_paired_table_cached, resolve_key,
    },
    table_history_read::TableHistoryReadState,
    table_update_transaction::TableUpdateTransaction,
};

/// Everything a pairing attempt uses (all implemented by each backend's catalog
/// attempt).
pub trait RegionalPairingAttempt:
    RegionalReceiveAttempt
    + RegionalCatalogAttempt
    + TableUpdateTransaction
    + CreateTableAttempt
    + PrimaryStateScanAttempt
    + StreamRetentionPort
{
}
impl<A> RegionalPairingAttempt for A where A: RegionalReceiveAttempt
        + RegionalCatalogAttempt
        + TableUpdateTransaction
        + CreateTableAttempt
        + PrimaryStateScanAttempt
        + StreamRetentionPort
{
}

#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub enum ScanTurn {
    /// The pair is no longer bound, or the table is no longer `Seeding`.
    Stopped,
    /// `next`: the continuation of the following page; `None` = the scan is
    /// complete.
    Page {
        items: Vec<ScanItem>,
        next: Option<String>,
    },
}

#[derive(Debug)]
pub enum SweepTurn {
    /// The pair is no longer bound, or the table is not `Active`.
    Stopped,
    /// `next`: the marker key to continue after; `None` = the sweep is
    /// complete.
    Page { next: Option<Vec<u8>> },
}

fn binding_of(table: &BoundTableInfo) -> StorageResult<&RegionalBinding> {
    table
        .binding
        .regional
        .as_deref()
        .ok_or_else(|| StorageError::internal("paired table has no binding"))
}

fn lineage_of(lane: &crate::table_history_read::TableHistoryReadLane) -> LineageRef {
    LineageRef {
        shard: lane.lane.shard_id().clone(),
        lineage: lane.lane.lineage_id().clone(),
    }
}

pub(crate) async fn begin_pairing_in_attempt<A: RegionalPairingAttempt>(
    attempt: &mut A,
    name: &TableName,
    local: RegionId,
    peer: RegionId,
    peer_name: &str,
) -> StorageResult<crate::SemanticDecision<PairId>> {
    use crate::SemanticDecision::{Accepted, Rejected};
    let table = attempt
        .observe_table_update(name)
        .await?
        .ok_or_else(|| StorageError::table_not_found(name))?;
    if let Some(binding) = table.binding.regional.as_deref() {
        return Ok(
            if binding.peer == peer && binding.state == PairState::Seeding {
                Accepted(binding.pair)
            } else {
                Rejected(StorageError::validation("the table already has a replica"))
            },
        );
    }
    if !table
        .info
        .stream_specification
        .as_ref()
        .is_some_and(|stream| stream.stream_enabled)
    {
        return Ok(Rejected(StorageError::validation(
            "a replicated table needs an enabled stream",
        )));
    }
    let pair = PairId::generate();
    // Holds at the current heads: earlier history is unpaired (never sent); the
    // scan carries its state.
    let history = TableHistoryReadState::load(attempt, &table).await?;
    for lane in &history.lanes {
        let cursor = LineageCursor {
            through: Some(lane.through.offset().clone()),
            sealed: lane.lineage.sealed(),
        };
        attempt.stage_regional_progress(
            &table,
            pair,
            ProgressDirection::Send,
            &lineage_of(lane),
            &cursor,
        )?;
    }
    let binding = RegionalBinding {
        pair,
        local,
        peer,
        state: PairState::Seeding,
        peer_name: peer_name.to_owned(),
    };
    attempt
        .store_regional_binding(&table, Some(Arc::new(binding)))
        .await?;
    attempt.finish_dynamo_write().await?;
    Ok(Accepted(pair))
}

/// `None` when the table is no longer `Seeding` in `pair`.
pub(crate) async fn prepare_request_in_attempt<A>(
    attempt: &mut A,
    name: &TableName,
    pair: PairId,
) -> StorageResult<Option<PrepareTarget>>
where
    A: CatalogAttempt + RegionalProgressAttempt,
{
    let Some(table) = load_paired_table(attempt, name, pair).await? else {
        return Ok(None);
    };
    if binding_of(&table)?.state != PairState::Seeding {
        return Ok(None);
    }
    let heads = attempt
        .list_regional_progress(&table, pair, ProgressDirection::Send)
        .await?
        .into_iter()
        .map(|(lineage, cursor)| LineageHead {
            lineage,
            through: cursor.through,
            sealed: cursor.sealed,
        })
        .collect();
    Ok(Some(PrepareTarget {
        pair,
        table: table.info.as_ref().clone(),
        heads,
    }))
}

/// Undo a `Seeding` pairing the target refused. False when the table is not
/// `Seeding` in `pair`.
pub(crate) async fn abandon_pairing_in_attempt<A: RegionalPairingAttempt>(
    attempt: &mut A,
    name: &TableName,
    pair: PairId,
) -> StorageResult<bool> {
    // Before the first read: the cleanup must see every marker and progress
    // row committed before the lock (`PAIR_END_CHANGE`).
    attempt.lock_catalog_for_change(name).await?;
    let Some(table) = attempt.observe_table_update(name).await? else {
        return Ok(false);
    };
    if !table
        .binding
        .regional
        .as_deref()
        .is_some_and(|binding| binding.pair == pair && binding.state == PairState::Seeding)
    {
        return Ok(false);
    }
    attempt.clear_regional_progress(&table, pair)?;
    attempt.clear_regional_markers(&table)?;
    attempt.store_regional_binding(&table, None).await?;
    attempt.finish_dynamo_write().await?;
    Ok(true)
}

/// CAS of the pair state. Returns the state after the call (`to` when changed
/// or already there); `None` when the table does not carry `pair`.
pub(crate) async fn advance_pair_state_in_attempt<A: RegionalPairingAttempt>(
    attempt: &mut A,
    name: &TableName,
    pair: PairId,
    from: PairState,
    to: PairState,
) -> StorageResult<Option<PairState>> {
    let Some(table) = attempt.observe_table_update(name).await? else {
        return Ok(None);
    };
    let Some(binding) = table
        .binding
        .regional
        .as_deref()
        .filter(|binding| binding.pair == pair)
    else {
        return Ok(None);
    };
    if binding.state != from {
        return Ok(Some(binding.state));
    }
    let next = RegionalBinding {
        state: to,
        ..binding.clone()
    };
    attempt
        .store_regional_binding(&table, Some(Arc::new(next)))
        .await?;
    Ok(Some(to))
}

pub(crate) async fn prepare_target_in_attempt<A: RegionalPairingAttempt>(
    attempt: &mut A,
    request: &PrepareTarget,
    local: RegionId,
    peer: RegionId,
    peer_name: &str,
    now: TimestampMillis,
) -> StorageResult<RegionalResponse> {
    if let Some(existing) = attempt
        .observe_table_update(&request.table.table_name)
        .await?
    {
        let same = existing
            .binding
            .regional
            .as_deref()
            .is_some_and(|binding| binding.pair == request.pair);
        return Ok(if same {
            RegionalResponse::Prepared
        } else {
            RegionalResponse::Rejected(RegionalRejection::TableExists)
        });
    }
    let mut info = request.table.clone();
    info.created_at = now;
    info.table_status = TableStatus::Active;
    info.table_size_bytes = 0;
    info.item_count = 0;
    let binding = Arc::new(RegionalBinding {
        pair: request.pair,
        local,
        peer,
        state: PairState::Joining,
        peer_name: peer_name.to_owned(),
    });
    let table = match create_bound_table_in_attempt(attempt, info, Some(binding)).await? {
        crate::SemanticDecision::Accepted(table) => table,
        crate::SemanticDecision::Rejected(_) => {
            return Ok(RegionalResponse::Rejected(RegionalRejection::TableExists));
        }
    };
    for head in &request.heads {
        let cursor = LineageCursor {
            through: head.through.clone(),
            sealed: head.sealed,
        };
        attempt.stage_regional_progress(
            &table,
            request.pair,
            ProgressDirection::Receive,
            &head.lineage,
            &cursor,
        )?;
    }
    attempt.finish_dynamo_write().await?;
    Ok(RegionalResponse::Prepared)
}

pub(crate) async fn next_scan_page_in_attempt<A>(
    attempt: &mut A,
    name: &TableName,
    pair: PairId,
    after: Option<String>,
    limit: NonZeroU32,
) -> StorageResult<ScanTurn>
where
    A: CatalogAttempt + PrimaryStateScanAttempt,
{
    let Some(table) = load_paired_table(attempt, name, pair).await? else {
        return Ok(ScanTurn::Stopped);
    };
    let binding = binding_of(&table)?;
    if binding.state != PairState::Seeding {
        return Ok(ScanTurn::Stopped);
    }
    let local = binding.local;
    let scan = PrimaryStateScan::try_new(Arc::clone(&table), after, Some(limit.get()))?;
    let page = attempt.scan_primary_state(&scan).await?;
    let items = page
        .items
        .into_iter()
        .filter_map(|row| match row.state {
            ItemWriteObservation::Live {
                stored, provenance, ..
            } => Some(ScanItem {
                item: *stored,
                order: provenance
                    .order()
                    .unwrap_or_else(|| RegionalOrder::baseline(local)),
            }),
            _ => None,
        })
        .collect();
    Ok(ScanTurn::Page {
        items,
        next: page.continuation,
    })
}

pub(crate) async fn apply_scan_page_in_attempt<A: RegionalReceiveAttempt>(
    attempt: &mut A,
    page: &ScanPage,
    created_at: TimestampMillis,
    cache: &TableMetadataCache,
) -> StorageResult<(RegionalResponse, Option<CachePublication>)> {
    let Some(table) = load_paired_table(attempt, &page.table, page.pair).await? else {
        return Ok((
            RegionalResponse::Rejected(RegionalRejection::UnknownPair),
            None,
        ));
    };
    let binding = binding_of(&table)?;
    if binding.state != PairState::Joining {
        return Ok((
            RegionalResponse::Rejected(RegionalRejection::NotJoining),
            None,
        ));
    }
    let local = binding.local;
    let groups = group_by_key(&table, &page.items, |item| {
        table.info.table_key_from_wire(&item.item.item)
    })?;
    if groups.is_empty() {
        return Ok((RegionalResponse::ScanApplied, None));
    }
    let publication =
        crate::regional_receive::prepare_regional_write(attempt, &table, cache).await?;
    let reads = groups
        .iter()
        .map(|group| ItemWriteObservationRead::Write(&table, &group.key))
        .collect::<Vec<_>>();
    let observations = attempt.observe_for_writes(&reads).await?;
    for (group, (observation, lifecycle)) in observations.zip(groups) {
        let incoming = group
            .entries
            .iter()
            .map(|item| Incoming::Put(item.order, &item.item));
        let outcome = resolve_key(KeyState::observe(&observation, local), true, incoming);
        apply_key_outcome(
            attempt,
            &table,
            group.key,
            observation,
            lifecycle,
            outcome,
            created_at,
        )
        .await?;
    }
    attempt.finish_dynamo_write().await?;
    Ok((RegionalResponse::ScanApplied, publication))
}

fn marker_key_attributes(table: &BoundTableInfo, suffix: &[u8]) -> StorageResult<KeyAttributes> {
    ItemKey::from_sorted_storage_suffix(&table.info, None, suffix)?.key_attributes(&table.info)
}

/// R2: received markers become echoes after activation; origin markers stay.
pub(crate) async fn sweep_received_markers_in_attempt<A: RegionalReceiveAttempt>(
    attempt: &mut A,
    name: &TableName,
    pair: PairId,
    after: Option<&[u8]>,
    limit: NonZeroU32,
    created_at: TimestampMillis,
    cache: &TableMetadataCache,
) -> StorageResult<(SweepTurn, Option<CachePublication>)> {
    let Some(table) = load_paired_table(attempt, name, pair).await? else {
        return Ok((SweepTurn::Stopped, None));
    };
    if binding_of(&table)?.state != PairState::Active {
        return Ok((SweepTurn::Stopped, None));
    }
    let page = attempt.list_regional_markers(&table, after, limit).await?;
    let next = if page.more {
        Some(
            page.markers
                .last()
                .map(|(key, _)| key.clone())
                .ok_or_else(|| StorageError::internal("marker page is empty but has more"))?,
        )
    } else {
        None
    };
    let keys = page
        .markers
        .iter()
        .filter(|(_, marker)| marker.received)
        .map(|(suffix, _)| marker_key_attributes(&table, suffix))
        .collect::<StorageResult<Vec<_>>>()?;
    if keys.is_empty() {
        return Ok((SweepTurn::Page { next }, None));
    }
    let publication =
        crate::regional_receive::prepare_regional_write(attempt, &table, cache).await?;
    let reads = keys
        .iter()
        .map(|key| ItemWriteObservationRead::Write(&table, key))
        .collect::<Vec<_>>();
    let observations = attempt.observe_for_writes(&reads).await?;
    for (key, (observation, lifecycle)) in observations.zip(keys) {
        // Re-checked under item protection: a later write may have replaced the
        // marker.
        let ItemWriteObservation::Deleted { marker } = observation else {
            continue;
        };
        if !marker.received {
            continue;
        }
        let mark = RegionalMark {
            order: marker.order,
            role: RegionalRole::Echo,
            hidden: true,
        };
        let outcome = KeyOutcome {
            item: ItemChange::Hidden { mark },
            marker: MarkerChange::Delete,
        };
        apply_key_outcome(
            attempt,
            &table,
            key,
            observation,
            lifecycle,
            outcome,
            created_at,
        )
        .await?;
    }
    attempt.finish_dynamo_write().await?;
    Ok((SweepTurn::Page { next }, publication))
}

#[async_trait]
pub trait RegionalPairingOperations: Send + Sync {
    /// Source: unpaired → Seeding with holds; or the existing Seeding pair of
    /// the same peer.
    async fn begin_pairing(
        &self,
        table: &TableName,
        local: RegionId,
        peer: RegionId,
        peer_name: &str,
    ) -> StorageResult<PairId>;
    async fn prepare_request(
        &self,
        table: &TableName,
        pair: PairId,
    ) -> StorageResult<Option<PrepareTarget>>;
    async fn abandon_pairing(&self, table: &TableName, pair: PairId) -> StorageResult<bool>;
    /// Target: create the Joining table and its receive cursors.
    async fn prepare_target(
        &self,
        request: PrepareTarget,
        local: RegionId,
        peer: RegionId,
        peer_name: &str,
    ) -> StorageResult<RegionalResponse>;
    /// The binding when the table carries `pair` (read-only).
    async fn regional_binding(
        &self,
        table: &TableName,
        pair: PairId,
    ) -> StorageResult<Option<RegionalBinding>>;
    async fn advance_pair_state(
        &self,
        table: &TableName,
        pair: PairId,
        from: PairState,
        to: PairState,
    ) -> StorageResult<Option<PairState>>;
    async fn next_scan_page(
        &self,
        table: &TableName,
        pair: PairId,
        after: Option<String>,
        limit: NonZeroU32,
    ) -> StorageResult<ScanTurn>;
    async fn apply_scan_page(&self, page: ScanPage) -> StorageResult<RegionalResponse>;
    /// Target: Joining → Active (`Activated` also when already Active in this
    /// pair).
    async fn activate_target(&self, target: PairTable) -> StorageResult<RegionalResponse>;
    async fn sweep_received_markers(
        &self,
        table: &TableName,
        pair: PairId,
        after: Option<Vec<u8>>,
        limit: NonZeroU32,
    ) -> StorageResult<SweepTurn>;
}

/// The request of one pair-scan page read.
struct ScanPageInput {
    name: TableName,
    pair: PairId,
    after: Option<String>,
    limit: NonZeroU32,
}

/// The request of one received-marker sweep page.
struct SweepInput {
    name: TableName,
    pair: PairId,
    after: Option<Vec<u8>>,
    limit: NonZeroU32,
    created_at: TimestampMillis,
    cache: Arc<TableMetadataCache>,
}

#[async_trait]
impl<P: DynamoPort> RegionalPairingOperations for P
where for<'native> P::Attempt<'native>: RegionalPairingAttempt
{
    async fn begin_pairing(
        &self,
        table: &TableName,
        local: RegionId,
        peer: RegionId,
        peer_name: &str,
    ) -> StorageResult<PairId> {
        let name = table.clone();
        let peer_name = peer_name.to_owned();
        let pair = storage_common::retry::retry_tries(
            self,
            self.attempt_deadlines(),
            crate::constants::SCHEMA_CHANGE,
            |attempt_request, _| {
                let name = name.clone();
                let peer_name = peer_name.clone();
                async move {
                    self.with_attempt(
                        attempt_request.scoped(storage_types::AttemptScope::Schema),
                        move |attempt| {
                            Box::pin(async move {
                                begin_pairing_in_attempt(attempt, &name, local, peer, &peer_name)
                                    .await?
                                    .into_result()
                            })
                        },
                    )
                    .await
                }
            },
        )
        .await?;
        self.metadata_cache().invalidate(table);
        Ok(pair)
    }

    async fn prepare_request(
        &self,
        table: &TableName,
        pair: PairId,
    ) -> StorageResult<Option<PrepareTarget>> {
        let name = table.clone();
        DynamoPort::run(
            self,
            crate::constants::BACKGROUND_READ_PAGE,
            Arc::new((name, pair)),
            |attempt, (name, pair), _| Box::pin(prepare_request_in_attempt(attempt, name, *pair)),
        )
        .await
    }

    async fn abandon_pairing(&self, table: &TableName, pair: PairId) -> StorageResult<bool> {
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
                            Box::pin(async move {
                                abandon_pairing_in_attempt(attempt, &name, pair).await
                            })
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

    async fn prepare_target(
        &self,
        request: PrepareTarget,
        local: RegionId,
        peer: RegionId,
        peer_name: &str,
    ) -> StorageResult<RegionalResponse> {
        let peer_name = peer_name.to_owned();
        let name = request.table.table_name.clone();
        let request = Arc::new(request);
        let now = self.now();
        let response = storage_common::retry::retry_tries(
            self,
            self.attempt_deadlines(),
            crate::constants::SCHEMA_CHANGE,
            |attempt_request, _| {
                let request = Arc::clone(&request);
                let peer_name = peer_name.clone();
                async move {
                    self.with_attempt(
                        attempt_request.scoped(storage_types::AttemptScope::Schema),
                        move |attempt| {
                            Box::pin(async move {
                                prepare_target_in_attempt(
                                    attempt, &request, local, peer, &peer_name, now,
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
        self.metadata_cache().invalidate(&name);
        Ok(response)
    }

    async fn regional_binding(
        &self,
        table: &TableName,
        pair: PairId,
    ) -> StorageResult<Option<RegionalBinding>> {
        let name = table.clone();
        let cache = self.metadata_cache();
        let (binding, publication) = DynamoPort::run(
            self,
            crate::constants::BACKGROUND_READ_PAGE,
            Arc::new((name, pair, Arc::clone(&cache))),
            |attempt, (name, pair, cache), _| {
                Box::pin(async move {
                    let (table, publication) =
                        load_paired_table_cached(attempt, cache, name, *pair).await?;
                    let binding = match table {
                        Some(table) => Some(binding_of(&table)?.clone()),
                        None => None,
                    };
                    Ok((binding, publication))
                })
            },
        )
        .await?;
        cache.publish(publication);
        Ok(binding)
    }

    async fn advance_pair_state(
        &self,
        table: &TableName,
        pair: PairId,
        from: PairState,
        to: PairState,
    ) -> StorageResult<Option<PairState>> {
        let name = table.clone();
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
                                advance_pair_state_in_attempt(attempt, &name, pair, from, to).await
                            })
                        },
                    )
                    .await
                }
            },
        )
        .await?;
        self.metadata_cache().invalidate(table);
        Ok(state)
    }

    async fn next_scan_page(
        &self,
        table: &TableName,
        pair: PairId,
        after: Option<String>,
        limit: NonZeroU32,
    ) -> StorageResult<ScanTurn> {
        let name = table.clone();
        let input = Arc::new(ScanPageInput {
            name,
            pair,
            after,
            limit,
        });
        DynamoPort::run(
            self,
            crate::constants::BACKGROUND_READ_PAGE,
            input,
            |attempt, input, _| {
                Box::pin(next_scan_page_in_attempt(
                    attempt,
                    &input.name,
                    input.pair,
                    input.after.clone(),
                    input.limit,
                ))
            },
        )
        .await
    }

    async fn apply_scan_page(&self, page: ScanPage) -> StorageResult<RegionalResponse> {
        let created_at = self.now();
        let cache = self.metadata_cache();
        let (response, publication) = DynamoPort::run(
            self,
            crate::constants::BACKGROUND_ITEM_PAGE,
            Arc::new((page, created_at, Arc::clone(&cache))),
            |attempt, (page, created_at, cache), _| {
                Box::pin(apply_scan_page_in_attempt(
                    attempt,
                    page,
                    *created_at,
                    cache,
                ))
            },
        )
        .await?;
        cache.publish(publication);
        Ok(response)
    }

    async fn activate_target(&self, target: PairTable) -> StorageResult<RegionalResponse> {
        Ok(
            match self
                .advance_pair_state(
                    &target.table,
                    target.pair,
                    PairState::Joining,
                    PairState::Active,
                )
                .await?
            {
                Some(PairState::Active | PairState::Leaving) => RegionalResponse::Activated,
                Some(_) => RegionalResponse::Rejected(RegionalRejection::NotJoining),
                None => RegionalResponse::Rejected(RegionalRejection::UnknownPair),
            },
        )
    }

    async fn sweep_received_markers(
        &self,
        table: &TableName,
        pair: PairId,
        after: Option<Vec<u8>>,
        limit: NonZeroU32,
    ) -> StorageResult<SweepTurn> {
        let name = table.clone();
        let created_at = self.now();
        let cache = self.metadata_cache();
        let input = Arc::new(SweepInput {
            name,
            pair,
            after,
            limit,
            created_at,
            cache: Arc::clone(&cache),
        });
        let (turn, publication) = DynamoPort::run(
            self,
            crate::constants::BACKGROUND_ITEM_PAGE,
            input,
            |attempt, input, _| {
                Box::pin(sweep_received_markers_in_attempt(
                    attempt,
                    &input.name,
                    input.pair,
                    input.after.as_deref(),
                    input.limit,
                    input.created_at,
                    &input.cache,
                ))
            },
        )
        .await?;
        cache.publish(publication);
        Ok(turn)
    }
}
