//! Receive attempt (design B R1, R2, R5; P1, P2, P4, P5, P6).

use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use regional_replication::message::{
    PageAck, PageChange, RegionalRejection, RegionalResponse, ReplicationPage,
};
use storage_backfill::GsiProjectionLifecycle;
use storage_types::{
    BoundTableInfo, ItemProvenance, KeyAttributes, PairId, PairState, PutItemRequest, RegionId,
    RegionalMark, RegionalMarker, RegionalOrder, RegionalRole, StorageError, StorageResult,
    StoredWireItem, SystemStreamOffset, TableName, TimestampMillis, regional_marker_item_key,
};
use stream_provider::StreamEntryInput;

use crate::{
    AcceptedDeleteItem, AcceptedUpsert, CachePublication, CatalogAttempt, DynamoPort,
    ItemWriteAttempt, ItemWriteObservation, ItemWriteObservationRead, PutItemShape,
    TableMetadataCache,
    dynamo_put_item::ObservedWrite,
    dynamo_write_execution::prepare_history_route,
    regional_progress::{
        LineageCursor, ProgressDirection, RegionalMarkerAttempt, RegionalProgressAttempt,
    },
};

/// Everything one receive, scan or sweep attempt uses.
pub trait RegionalReceiveAttempt:
    ItemWriteAttempt + CatalogAttempt + RegionalProgressAttempt + RegionalMarkerAttempt
{
}
impl<A> RegionalReceiveAttempt for A where A: ItemWriteAttempt + CatalogAttempt + RegionalProgressAttempt + RegionalMarkerAttempt
{}

// ---- pure decision
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KeyState {
    Absent,
    Live(RegionalOrder),
    Marker(RegionalMarker),
}

impl KeyState {
    pub(crate) fn observe(observation: &ItemWriteObservation, local: RegionId) -> Self {
        match observation {
            ItemWriteObservation::Absent => Self::Absent,
            ItemWriteObservation::Deleted { marker } => Self::Marker(*marker),
            ItemWriteObservation::Live { provenance, .. } => Self::Live(
                provenance
                    .order()
                    .unwrap_or_else(|| RegionalOrder::baseline(local)),
            ),
        }
    }

    fn winner(self) -> Option<RegionalOrder> {
        match self {
            Self::Absent => None,
            Self::Live(order) => Some(order),
            Self::Marker(marker) => Some(marker.order),
        }
    }

    fn marker(self) -> Option<RegionalMarker> {
        match self {
            Self::Marker(marker) => Some(marker),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub(crate) enum Incoming<'a> {
    Put(RegionalOrder, &'a StoredWireItem),
    Delete(RegionalOrder),
    Echo(RegionalOrder),
}

#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub(crate) enum ItemChange<'a> {
    None,
    Put {
        order: RegionalOrder,
        item: &'a StoredWireItem,
    },
    /// The observed live item is removed; public REMOVE with this mark.
    Remove {
        mark: RegionalMark,
    },
    /// No item; hidden entry with this mark (R1 echo of an absent-key delete).
    Hidden {
        mark: RegionalMark,
    },
}

/// `StoredWireItem` has no equality; the resolver passes page items through
/// unchanged, so a put compares by order and item identity.
impl PartialEq for ItemChange<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::None, Self::None) => true,
            (
                Self::Put { order, item },
                Self::Put {
                    order: other_order,
                    item: other_item,
                },
            ) => order == other_order && std::ptr::eq(*item, *other_item),
            (Self::Remove { mark }, Self::Remove { mark: other }) => mark == other,
            (Self::Hidden { mark }, Self::Hidden { mark: other }) => mark == other,
            _ => false,
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum MarkerChange {
    Keep,
    Store(RegionalMarker),
    Delete,
}

#[derive(PartialEq)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub(crate) struct KeyOutcome<'a> {
    pub item: ItemChange<'a>,
    pub marker: MarkerChange,
}

/// Apply one key's entries in source position order to its observed state;
/// return the single net effect. `joining`: R2 (keep a received marker, no
/// echo).
pub(crate) fn resolve_key<'a>(
    initial: KeyState,
    joining: bool,
    entries: impl IntoIterator<Item = Incoming<'a>>,
) -> KeyOutcome<'a> {
    let mut state = initial;
    let mut put = None;
    let mut deleted_at = None;
    for entry in entries {
        match entry {
            Incoming::Put(order, item) => {
                // Equal order = the same write (duplicate or scan copy): not
                // newer.
                if state.winner().is_some_and(|winner| winner >= order) {
                    continue;
                }
                state = KeyState::Live(order);
                put = Some((order, item));
                deleted_at = None;
            }
            Incoming::Delete(order) => {
                let applies = match state {
                    KeyState::Absent => true,
                    KeyState::Live(winner) => order > winner,
                    // R1: an absent key or a lower OR EQUAL marker (equal: both
                    // regions expired the same item, P6) is replaced.
                    KeyState::Marker(marker) => order >= marker.order,
                };
                if !applies {
                    continue;
                }
                state = if joining {
                    KeyState::Marker(RegionalMarker {
                        order,
                        received: true,
                    })
                } else {
                    KeyState::Absent
                };
                put = None;
                deleted_at = Some(order);
            }
            Incoming::Echo(order) => {
                if state
                    == KeyState::Marker(RegionalMarker {
                        order,
                        received: false,
                    })
                {
                    state = KeyState::Absent;
                }
            }
        }
    }
    let marker = match (initial.marker(), state.marker()) {
        (before, after) if before == after => MarkerChange::Keep,
        (_, Some(after)) => MarkerChange::Store(after),
        (_, None) => MarkerChange::Delete,
    };
    let item = match (put, deleted_at) {
        (Some((order, item)), _) => ItemChange::Put { order, item },
        (None, Some(order)) => {
            let role = if joining {
                RegionalRole::Applied
            } else {
                RegionalRole::Echo
            };
            let mark = RegionalMark {
                order,
                role,
                hidden: false,
            };
            match (initial, joining) {
                (KeyState::Live(_), _) => ItemChange::Remove { mark },
                (_, true) => ItemChange::None,
                (_, false) => ItemChange::Hidden { mark },
            }
        }
        (None, None) => ItemChange::None,
    };
    KeyOutcome { item, marker }
}

pub(crate) fn offset_order(
    left: Option<&SystemStreamOffset>,
    right: Option<&SystemStreamOffset>,
) -> StorageResult<Ordering> {
    match (left, right) {
        (None, None) => Ok(Ordering::Equal),
        (None, Some(_)) => Ok(Ordering::Less),
        (Some(_), None) => Ok(Ordering::Greater),
        (Some(left), Some(right)) => left
            .compare_order(right)
            .map_err(|_| StorageError::internal("regional offsets of one lineage do not compare")),
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum Coverage {
    Gap,
    Covered,
    Apply {
        skip_through: Option<SystemStreamOffset>,
    },
}

pub(crate) fn coverage(cursor: &LineageCursor, page: &ReplicationPage) -> StorageResult<Coverage> {
    let covered = offset_order(page.through.as_ref(), cursor.through.as_ref())?
        != Ordering::Greater
        && (cursor.sealed || !page.sealed);
    Ok(
        match offset_order(cursor.through.as_ref(), page.after.as_ref())? {
            Ordering::Less => Coverage::Gap,
            _ if covered => Coverage::Covered,
            Ordering::Equal => Coverage::Apply { skip_through: None },
            Ordering::Greater => Coverage::Apply {
                skip_through: cursor.through.clone(),
            },
        },
    )
}

// ---- attempt
// ---------------------------------------------------------------------------------

/// Whether the binding of `table` admits pair traffic of `pair`.
fn admits_pair(table: &BoundTableInfo, pair: PairId) -> bool {
    table.binding.regional.as_deref().is_some_and(|binding| {
        binding.pair == pair
            && matches!(
                binding.state,
                PairState::Joining | PairState::Seeding | PairState::Active | PairState::Leaving
            )
    })
}

/// The table when its binding admits pair traffic of `pair`.
pub(crate) async fn load_paired_table(
    attempt: &mut impl CatalogAttempt,
    name: &TableName,
    pair: PairId,
) -> StorageResult<Option<Arc<BoundTableInfo>>> {
    let Some(stamp) = attempt.read_catalog_stamp(name).await? else {
        return Ok(None);
    };
    let table = attempt.read_table_at_stamp(name, stamp).await?;
    let admitted = admits_pair(&table, pair);
    Ok(admitted.then_some(table))
}

/// `load_paired_table` through the metadata cache (regional hot paths). A
/// cached entry is used only when this attempt's catalog version (FoundationDB,
/// RocksDB) or stamp (SQL) matches it. The caller publishes the returned entry
/// after its attempt returns Ok, also when the table is not admitted.
pub(crate) async fn load_paired_table_cached(
    attempt: &mut impl CatalogAttempt,
    cache: &TableMetadataCache,
    name: &TableName,
    pair: PairId,
) -> StorageResult<(Option<Arc<BoundTableInfo>>, Option<CachePublication>)> {
    let decision = crate::dynamo_catalog::resolve_prepared_table_in_attempt(
        attempt,
        cache,
        name,
        &crate::dynamo_table_bindings::RequestTableBindings::default(),
    )
    .await?;
    // Default bindings reject only an absent name (`pin_stamp` of `None`).
    let crate::SemanticDecision::Accepted((prepared, publication)) = decision else {
        return Ok((None, None));
    };
    let table = prepared.table;
    let admitted = admits_pair(&table, pair);
    Ok((admitted.then_some(table), publication))
}

/// Entries of one key in page order. `key_bytes` = `regional_marker_item_key`
/// (stable identity).
pub(crate) struct KeyGroup<'a, T> {
    pub key: KeyAttributes,
    pub entries: Vec<&'a T>,
}

pub(crate) fn group_by_key<'a, T>(
    table: &BoundTableInfo,
    values: &'a [T],
    key_of: impl Fn(&'a T) -> StorageResult<KeyAttributes>,
) -> StorageResult<Vec<KeyGroup<'a, T>>> {
    let mut index = HashMap::<Vec<u8>, usize>::with_capacity(values.len());
    let mut groups = Vec::<KeyGroup<'a, T>>::with_capacity(values.len());
    for value in values {
        let key = key_of(value)?;
        let identity = regional_marker_item_key(table, &key)?;
        match index.get(&identity) {
            Some(&position) => groups[position].entries.push(value),
            None => {
                index.insert(identity, groups.len());
                groups.push(KeyGroup {
                    key,
                    entries: vec![value],
                });
            }
        }
    }
    Ok(groups)
}

fn missing_route() -> StorageError {
    StorageError::internal("a paired table has no stream route")
}

/// Install the KV writer routes (through the metadata cache) only when the
/// write needs a stream entry, then observe the write prerequisites (paired
/// tables only reach this). The caller publishes the returned cache entry after
/// the attempt commits.
pub(crate) async fn prepare_regional_write<A: RegionalReceiveAttempt>(
    attempt: &mut A,
    table: &BoundTableInfo,
    cache: &TableMetadataCache,
) -> StorageResult<Option<CachePublication>> {
    let publication = if table.info.requires_stream_entry() {
        crate::prepare_writer_routes_in_attempt(attempt, Some(cache)).await?
    } else {
        None
    };
    attempt.prepare_item_write(table).await?;
    Ok(publication)
}

pub(crate) async fn receive_page_in_attempt<A: RegionalReceiveAttempt>(
    attempt: &mut A,
    page: &ReplicationPage,
    created_at: TimestampMillis,
    cache: &TableMetadataCache,
) -> StorageResult<(RegionalResponse, [Option<CachePublication>; 2])> {
    let (table, table_publication) =
        load_paired_table_cached(attempt, cache, &page.table, page.pair).await?;
    let Some(table) = table else {
        return Ok((
            RegionalResponse::Rejected(RegionalRejection::UnknownPair),
            [table_publication, None],
        ));
    };
    let binding = table
        .binding
        .regional
        .as_deref()
        .ok_or_else(|| StorageError::internal("pair checked"))?;
    let (joining, local) = (binding.state == PairState::Joining, binding.local);
    // R5: items (Postgres: their advisory locks) before the cursor record.
    let groups = group_by_key(&table, &page.entries, |entry| Ok(entry.keys.clone()))?;
    let publication = if groups.is_empty() {
        None
    } else {
        prepare_regional_write(attempt, &table, cache).await?
    };
    let publications = [table_publication, publication];
    let observations = if groups.is_empty() {
        storage_types::Exact::empty()
    } else {
        let reads = groups
            .iter()
            .map(|group| ItemWriteObservationRead::Write(&table, &group.key))
            .collect::<Vec<_>>();
        attempt.observe_for_writes(&reads).await?
    };
    let mut lineages = Vec::with_capacity(1 + page.parents.len());
    lineages.push(page.lineage.clone());
    lineages.extend(page.parents.iter().cloned());
    let mut cursors = attempt
        .read_regional_progress(&table, page.pair, ProgressDirection::Receive, &lineages)
        .await?
        .into_iter();
    let current = cursors.next().flatten();
    if current.is_none() && !cursors.all(|parent| parent.is_some_and(|parent| parent.sealed)) {
        return Ok((
            RegionalResponse::Rejected(RegionalRejection::ParentNotSealed),
            publications,
        ));
    }
    let current = current.unwrap_or_default();
    let skip_through = match coverage(&current, page)? {
        Coverage::Gap => {
            return Ok((
                RegionalResponse::Gap {
                    expected: current.through,
                },
                publications,
            ));
        }
        Coverage::Covered => {
            return Ok((
                RegionalResponse::Ack(PageAck {
                    through: current.through,
                    sealed: current.sealed,
                }),
                publications,
            ));
        }
        Coverage::Apply { skip_through } => skip_through,
    };
    for (group, (observation, lifecycle)) in groups.into_iter().zip(observations) {
        let mut incoming = Vec::with_capacity(group.entries.len());
        for entry in group.entries {
            // P4: filter by committed source position before key collapse.
            if offset_order(Some(&entry.position), skip_through.as_ref())? != Ordering::Greater {
                continue;
            }
            incoming.push(match &entry.change {
                PageChange::Put { item } => Incoming::Put(entry.order, item),
                PageChange::Delete => Incoming::Delete(entry.order),
                PageChange::Echo => Incoming::Echo(entry.order),
            });
        }
        let outcome = resolve_key(KeyState::observe(&observation, local), joining, incoming);
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
    let cursor = LineageCursor {
        through: page.through.clone(),
        sealed: page.sealed,
    };
    attempt.stage_regional_progress(
        &table,
        page.pair,
        ProgressDirection::Receive,
        &page.lineage,
        &cursor,
    )?;
    attempt.finish_dynamo_write().await?;
    Ok((
        RegionalResponse::Ack(PageAck {
            through: cursor.through,
            sealed: cursor.sealed,
        }),
        publications,
    ))
}

/// Stage one key's net effect (receive, scan apply REG-10, sweep REG-10).
pub(crate) async fn apply_key_outcome<A: RegionalReceiveAttempt>(
    attempt: &mut A,
    table: &Arc<BoundTableInfo>,
    key: KeyAttributes,
    observation: ItemWriteObservation,
    lifecycle: GsiProjectionLifecycle,
    outcome: KeyOutcome<'_>,
    created_at: TimestampMillis,
) -> StorageResult<()> {
    match outcome.marker {
        MarkerChange::Keep => {}
        MarkerChange::Store(marker) => attempt.stage_regional_marker(table, &key, Some(marker))?,
        MarkerChange::Delete => attempt.stage_regional_marker(table, &key, None)?,
    }
    match outcome.item {
        ItemChange::None => Ok(()),
        ItemChange::Put { order, item } => {
            let request = PutItemRequest::builder()
                .table_name(table.table_name.clone())
                .item(item.item.clone())
                .indexers(Some(item.indexers.clone()))
                .build();
            let bound =
                Arc::new(PutItemShape::try_from_request(request)?).bind_table(Arc::clone(table))?;
            let (accepted, _) = bound.accept_old(observation, lifecycle)?.into_result()?;
            stage_received_upsert(attempt, accepted, order, created_at).await
        }
        ItemChange::Remove { mark } => {
            let old = ObservedWrite::try_new(observation, &key)?;
            let accepted =
                AcceptedDeleteItem::try_from_observed(Arc::clone(table), key, old, lifecycle)?;
            stage_received_delete(attempt, &accepted, mark, created_at).await
        }
        ItemChange::Hidden { mark } => {
            let route = prepare_history_route(table, &key)?.ok_or_else(missing_route)?;
            let entry = StreamEntryInput::hidden(table, &route, created_at, mark)?;
            attempt
                .prepare_logical_stream_write(table, entry, route.routing_key())
                .await
        }
    }
}

/// `prepare_accepted_upsert` without local admission: the order and role come
/// from the peer.
async fn stage_received_upsert<A: ItemWriteAttempt>(
    attempt: &mut A,
    mut item: AcceptedUpsert,
    order: RegionalOrder,
    created_at: TimestampMillis,
) -> StorageResult<()> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let mut ttl = item.ttl_effects()?;
        let writes = ttl.take_writes();
        let effects = ttl.metadata(ItemProvenance::regional(order));
        attempt
            .prepare_primary_upsert(&item, &effects, writes)
            .await?;
        let route = item.history_route.take().ok_or_else(missing_route)?;
        let mark = RegionalMark {
            order,
            role: RegionalRole::Applied,
            hidden: false,
        };
        item.capture_history(attempt, route, created_at, Some(mark))
            .await?;
        let gsi = item.prepared_gsi.finish(
            &item.gsi_lifecycle,
            &item.table.info,
            item.old_item.as_deref(),
        )?;
        attempt.prepare_gsi_writes(&item.table, gsi).await
    }
    .await;
    settlement.complete(result)
}

async fn stage_received_delete<A: ItemWriteAttempt>(
    attempt: &mut A,
    item: &AcceptedDeleteItem,
    mark: RegionalMark,
    created_at: TimestampMillis,
) -> StorageResult<()> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let ttl = item.ttl_writes()?;
        attempt
            .prepare_gsi_writes(&item.table, item.gsi_writes()?)
            .await?;
        let route = item.history_route.as_ref().ok_or_else(missing_route)?;
        item.capture_history(attempt, route, created_at, Some(mark))
            .await?;
        attempt.stage_primary_delete(item, ttl)
    }
    .await;
    settlement.complete(result)
}

#[async_trait]
pub trait RegionalReceiveOperations: Send + Sync {
    /// One native attempt per try. Known aborts retry with fresh observations;
    /// an unknown commit outcome is the error (the handler answers
    /// `Unavailable`; the sender repeats the page).
    async fn receive_page(&self, page: ReplicationPage) -> StorageResult<RegionalResponse>;
}

#[async_trait]
impl<P: DynamoPort> RegionalReceiveOperations for P
where for<'native> P::Attempt<'native>: RegionalReceiveAttempt
{
    async fn receive_page(&self, page: ReplicationPage) -> StorageResult<RegionalResponse> {
        let created_at = self.now();
        let cache = self.metadata_cache();
        let (response, publications) = DynamoPort::run(
            self,
            crate::constants::BACKGROUND_ITEM_PAGE,
            Arc::new((page, created_at, Arc::clone(&cache))),
            |attempt, (page, created_at, cache), _| {
                Box::pin(receive_page_in_attempt(attempt, page, *created_at, cache))
            },
        )
        .await?;
        cache.publish(publications.into_iter().flatten());
        Ok(response)
    }
}
