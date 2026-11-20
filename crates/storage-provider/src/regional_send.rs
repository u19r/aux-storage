//! Sender turn halves (design B R1, R5; P4, P11): page read, ACK store.

use std::{cmp::Ordering, num::NonZeroU32, sync::Arc};

use async_trait::async_trait;
use regional_replication::message::{LineageRef, PageChange, PageEntry, ReplicationPage};
use storage_types::{
    BoundTableInfo, PairId, PairState, RegionalRole, StorageError, StorageResult, TableName,
    TableStreamSequence, TimestampMillis,
};
use stream_provider::{PositionedStreamEntry, ReadDirection, StreamChange, TableHistoryIndexRead};

use crate::{
    CachePublication, CatalogAttempt, DynamoPort, DynamoStreamWriteAttempt, StreamRetentionPort,
    TableMetadataCache,
    regional_progress::{LineageCursor, ProgressDirection, RegionalProgressAttempt},
    regional_receive::{load_paired_table_cached, offset_order},
    table_history_read::TableHistoryReadState,
};

#[derive(Clone, Copy, Debug)]
pub struct SendLimits {
    pub entries: NonZeroU32,
    /// Raw JSON budget of the entries; the first entry is always taken.
    pub bytes: usize,
}

#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub enum SendTurn {
    /// The binding no longer carries this pair (ended, replaced, table gone):
    /// the job completes.
    PairGone,
    /// No lineage has an entry after its acknowledgment, nor a seal (or, while
    /// Leaving, a head) to send.
    Idle,
    /// `lag_millis`: age of the oldest entry the page read (0 when it read
    /// none). `caught_up`: the page reached the head of its lane.
    Page {
        page: Box<ReplicationPage>,
        next_lane: u32,
        lag_millis: u64,
        caught_up: bool,
    },
}

/// A lane that may have something to send, in rotation order (one batched read
/// covers all of them).
struct SendCandidate {
    index: usize,
    lineage: LineageRef,
    parents: Vec<LineageRef>,
    after: Option<storage_types::SystemStreamOffset>,
}

pub(crate) fn page_entry(positioned: &PositionedStreamEntry) -> StorageResult<Option<PageEntry>> {
    let Some(mark) = positioned.entry.regional() else {
        return Ok(None);
    };
    let change = match mark.role {
        RegionalRole::Applied => return Ok(None),
        RegionalRole::Echo => PageChange::Echo,
        RegionalRole::Origin => match positioned.entry.change() {
            StreamChange::Remove => PageChange::Delete,
            StreamChange::Insert | StreamChange::Modify => PageChange::Put {
                item: Box::new(positioned.entry.new_image().cloned().ok_or_else(|| {
                    StorageError::internal("an origin put entry has no new image")
                })?),
            },
        },
    };
    Ok(Some(PageEntry {
        position: positioned.position.offset().clone(),
        keys: positioned.entry.keys().clone(),
        order: mark.order,
        change,
    }))
}

pub(crate) async fn next_send_page_in_attempt<A>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    name: &TableName,
    pair: PairId,
    next_lane: u32,
    limits: SendLimits,
    now: TimestampMillis,
) -> StorageResult<(SendTurn, Option<CachePublication>)>
where
    A: StreamRetentionPort + CatalogAttempt + RegionalProgressAttempt,
{
    let (table, publication) = load_paired_table_cached(attempt, cache, name, pair).await?;
    let turn = match table {
        Some(table) => {
            next_send_page_of_table(attempt, &table, pair, next_lane, limits, now).await?
        }
        None => SendTurn::PairGone,
    };
    Ok((turn, publication))
}

async fn next_send_page_of_table<A>(
    attempt: &mut A,
    table: &BoundTableInfo,
    pair: PairId,
    next_lane: u32,
    limits: SendLimits,
    now: TimestampMillis,
) -> StorageResult<SendTurn>
where
    A: StreamRetentionPort + CatalogAttempt + RegionalProgressAttempt,
{
    // While Leaving, the Finish heads are publication bounds: empty pages carry
    // the peer's cursor to them.
    let leaving = table
        .binding
        .regional
        .as_deref()
        .is_some_and(|binding| binding.state == PairState::Leaving);
    let state = TableHistoryReadState::load(attempt, table).await?;
    let progress = attempt
        .list_regional_progress(table, pair, ProgressDirection::Send)
        .await?;
    let lanes = &state.lanes;
    let record = |lineage: &LineageRef| {
        progress
            .iter()
            .find(|(known, _)| known == lineage)
            .map(|(_, cursor)| cursor.clone())
    };
    let mut candidates = Vec::new();
    let mut reads = Vec::new();
    for step in 0..lanes.len() {
        let index = (next_lane as usize + step) % lanes.len();
        let lane = &lanes[index];
        let lineage = LineageRef {
            shard: lane.lane.shard_id().clone(),
            lineage: lane.lane.lineage_id().clone(),
        };
        let started = record(&lineage);
        let current = started.clone().unwrap_or_default();
        if current.sealed {
            continue;
        }
        let head = lane.through.offset();
        let behind = offset_order(current.through.as_ref(), Some(head))? == Ordering::Less;
        if !behind && !lane.lineage.sealed() {
            continue;
        }
        // A child is sent after its parent is acknowledged and sealed (R5).
        let mut parents = Vec::new();
        if started.is_none()
            && let Some(parent_id) = lane.lineage.parent_lineage_id()
        {
            let parent = lanes
                .iter()
                .map(|other| LineageRef {
                    shard: other.lane.shard_id().clone(),
                    lineage: other.lane.lineage_id().clone(),
                })
                .chain(progress.iter().map(|(known, _)| known.clone()))
                .find(|candidate| &candidate.lineage == parent_id);
            if let Some(parent) = parent {
                if !record(&parent).is_some_and(|cursor| cursor.sealed) {
                    continue;
                }
                parents.push(parent);
            }
        }
        // Never send past a trim (the hold must have prevented it).
        let retired = state
            .retention
            .frontier
            .lineages
            .iter()
            .find(|known| known.shard_id == lineage.shard && known.lineage_id == lineage.lineage)
            .and_then(|known| known.retired_through.as_ref());
        if offset_order(retired, current.through.as_ref())? == Ordering::Greater {
            return Err(StorageError::internal(
                "regional send history was trimmed past the peer acknowledgment",
            ));
        }
        // The first candidate reads a full page; a later one only proves its
        // lane is not empty.
        let limit = if candidates.is_empty() {
            limits.entries
        } else {
            NonZeroU32::MIN
        };
        reads.push(TableHistoryIndexRead {
            lane: lane.lane.clone(),
            exclusive_start: current
                .through
                .as_ref()
                .map(TableStreamSequence::try_from)
                .transpose()?,
            through: TableStreamSequence::try_from(head)?,
            direction: ReadDirection::Forward,
            limit,
        });
        candidates.push(SendCandidate {
            index,
            lineage,
            parents,
            after: current.through,
        });
    }
    if candidates.is_empty() {
        return Ok(SendTurn::Idle);
    }
    let pages = stream_provider::read_table_history_indexes(attempt, &reads).await?;
    for (position, ((candidate, read), page)) in
        candidates.into_iter().zip(reads).zip(pages).enumerate()
    {
        let lane = &lanes[candidate.index];
        // The publication bound moves without entries (FDB read version,
        // RocksDB sequence, Postgres xmin): an empty range is sent only to seal
        // the lane, to continue a `more` page, or, while Leaving, to
        // reach the Finish heads.
        if page.entries.is_empty() && !page.more && !lane.lineage.sealed() && !leaving {
            continue;
        }
        // A later candidate was only probed (limit 1). When its lane holds
        // more, read it again with the full limit in this attempt: one more
        // wave instead of a one-entry page and a second page next turn.
        let reread = position > 0 && page.more;
        let page = if reread {
            let full = TableHistoryIndexRead {
                limit: limits.entries,
                ..read
            };
            stream_provider::read_table_history_indexes(attempt, std::slice::from_ref(&full))
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| StorageError::internal("regional send page is missing"))?
        } else {
            page
        };
        let head = lane.through.offset();
        let oldest = page
            .entries
            .first()
            .map(|positioned| positioned.entry.created_at());
        let lag_millis = oldest.map_or(0, |created| {
            u64::try_from(now.timestamp_millis() - created.timestamp_millis()).unwrap_or(0)
        });
        let mut complete = !page.more;
        let mut through = candidate.after.clone();
        let mut entries = Vec::new();
        let mut bytes = 0_usize;
        for positioned in &page.entries {
            if let Some(entry) = page_entry(positioned)? {
                let size = serde_json::to_vec(&entry)?.len();
                if !entries.is_empty() && bytes.saturating_add(size) > limits.bytes {
                    complete = false;
                    break;
                }
                bytes += size;
                entries.push(entry);
            }
            through = Some(positioned.position.offset().clone());
        }
        if complete {
            through = Some(head.clone());
        }
        let sealed = complete && lane.lineage.sealed();
        return Ok(SendTurn::Page {
            page: Box::new(ReplicationPage {
                pair,
                table: table.table_name.clone(),
                lineage: candidate.lineage,
                parents: candidate.parents,
                after: candidate.after,
                through,
                sealed,
                entries,
            }),
            next_lane: u32::try_from(
                (candidate.index + usize::from(complete || position == 0 || reread)) % lanes.len(),
            )
            .unwrap_or(0),
            lag_millis,
            caught_up: complete,
        });
    }
    Ok(SendTurn::Idle)
}

/// `rewind`: a `Gap` answer (move back to `cursor`); otherwise an `Ack` (only
/// forward).
pub(crate) async fn store_send_progress_in_attempt<A>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    name: &TableName,
    pair: PairId,
    lineage: &LineageRef,
    cursor: &LineageCursor,
    rewind: bool,
) -> StorageResult<Option<CachePublication>>
where
    A: CatalogAttempt + RegionalProgressAttempt + DynamoStreamWriteAttempt,
{
    let (table, publication) = load_paired_table_cached(attempt, cache, name, pair).await?;
    let Some(table) = table else {
        return Ok(publication);
    };
    let current = attempt
        .read_regional_progress(
            &table,
            pair,
            ProgressDirection::Send,
            std::slice::from_ref(lineage),
        )
        .await?
        .into_iter()
        .next()
        .flatten()
        .unwrap_or_default();
    let order = offset_order(cursor.through.as_ref(), current.through.as_ref())?;
    let change = if rewind {
        order == Ordering::Less
    } else {
        order == Ordering::Greater || (order == Ordering::Equal && cursor.sealed && !current.sealed)
    };
    if !change {
        return Ok(publication);
    }
    attempt.stage_regional_progress(&table, pair, ProgressDirection::Send, lineage, cursor)?;
    attempt.finish_dynamo_write().await?;
    Ok(publication)
}

#[async_trait]
pub trait RegionalSendOperations: Send + Sync {
    async fn next_send_page(
        &self,
        table: &TableName,
        pair: PairId,
        next_lane: u32,
        limits: SendLimits,
    ) -> StorageResult<SendTurn>;
    async fn store_send_progress(
        &self,
        table: &TableName,
        pair: PairId,
        lineage: LineageRef,
        cursor: LineageCursor,
        rewind: bool,
    ) -> StorageResult<()>;
}

/// The request of one send-page read.
struct SendPageInput {
    name: TableName,
    pair: PairId,
    next_lane: u32,
    limits: SendLimits,
    now: TimestampMillis,
    cache: Arc<TableMetadataCache>,
}

/// The cursor change of one send-progress store.
struct SendProgressInput {
    name: TableName,
    pair: PairId,
    lineage: LineageRef,
    cursor: LineageCursor,
    rewind: bool,
    cache: Arc<TableMetadataCache>,
}

#[async_trait]
impl<P: DynamoPort> RegionalSendOperations for P
where for<'native> P::Attempt<'native>: RegionalProgressAttempt
{
    async fn next_send_page(
        &self,
        table: &TableName,
        pair: PairId,
        next_lane: u32,
        limits: SendLimits,
    ) -> StorageResult<SendTurn> {
        let name = table.clone();
        let now = self.now();
        let cache = self.metadata_cache();
        let input = Arc::new(SendPageInput {
            name,
            pair,
            next_lane,
            limits,
            now,
            cache: Arc::clone(&cache),
        });
        let (turn, publication) = DynamoPort::run(
            self,
            crate::constants::BACKGROUND_READ_PAGE,
            input,
            |attempt, input, _| {
                Box::pin(next_send_page_in_attempt(
                    attempt,
                    &input.cache,
                    &input.name,
                    input.pair,
                    input.next_lane,
                    input.limits,
                    input.now,
                ))
            },
        )
        .await?;
        cache.publish(publication);
        Ok(turn)
    }

    async fn store_send_progress(
        &self,
        table: &TableName,
        pair: PairId,
        lineage: LineageRef,
        cursor: LineageCursor,
        rewind: bool,
    ) -> StorageResult<()> {
        let name = table.clone();
        let cache = self.metadata_cache();
        let input = Arc::new(SendProgressInput {
            name,
            pair,
            lineage,
            cursor,
            rewind,
            cache: Arc::clone(&cache),
        });
        let publication = DynamoPort::run(
            self,
            crate::constants::BACKGROUND_PAGE,
            input,
            |attempt, input, _| {
                Box::pin(store_send_progress_in_attempt(
                    attempt,
                    &input.cache,
                    &input.name,
                    input.pair,
                    &input.lineage,
                    &input.cursor,
                    input.rewind,
                ))
            },
        )
        .await?;
        cache.publish(publication);
        Ok(())
    }
}
