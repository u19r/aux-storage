//! Bounded key discovery and live revision observations feed the history
//! planner.

use std::num::NonZeroUsize;

use storage_types::{
    ItemHistoryAddress, ItemHistoryChange, ItemHistoryEntry, ItemHistoryReference,
    ItemStreamVersion, KeyAttributes, StorageEnum, StorageError, StorageResult,
};
use stream_provider::{HistoryRowDelete, ItemHistoryReadBatch};

use super::{
    HistoryCleanupContinuation, ItemHistoryCleanupObservation, ItemHistoryCleanupPlan,
    ItemHistoryMetadataPage, StreamRetentionPort, table_cleanup::TableCleanupWork,
};

pub(super) struct HistoryCleanupRead {
    pub(super) work: usize,
    pub(super) history: ItemHistoryAddress,
    pub(super) page: ItemHistoryMetadataPage,
    pub(super) frontier: super::TableHistoryTrimFrontier,
}

pub(super) struct HistoryReclaimDecision {
    pub(super) history: ItemHistoryAddress,
    pub(super) delete: Option<ItemHistoryReference>,
    pub(super) complete: bool,
}

pub(super) async fn plan_history_reclaims(
    attempt: &mut impl StreamRetentionPort,
    works: &[TableCleanupWork],
) -> StorageResult<Vec<HistoryReclaimDecision>> {
    let selected = select_histories(works);
    let histories = selected
        .iter()
        .map(|(_, history)| history.clone())
        .collect::<Vec<_>>();
    let pages = attempt.load_item_history_metadata_pages(&histories).await?;
    if pages.len() != selected.len() {
        return Err(invalid_history());
    }
    let mut decisions = Vec::new();
    let mut pending = Vec::new();
    for ((work, history), page) in selected.into_iter().zip(pages) {
        if page.entries.is_empty() {
            decisions.push(HistoryReclaimDecision {
                history,
                delete: None,
                complete: true,
            });
        } else {
            pending.push(HistoryCleanupRead {
                frontier: works[work].state.frontier.clone(),
                work,
                history,
                page,
            });
        }
    }
    decisions.extend(load_reclaim_decisions(attempt, works, pending).await?);
    Ok(decisions)
}

fn select_histories(works: &[TableCleanupWork]) -> Vec<(usize, ItemHistoryAddress)> {
    let mut histories = Vec::new();
    for (index, work) in works.iter().enumerate() {
        let pointers = work
            .lanes
            .iter()
            .flat_map(|lane| &lane.candidates)
            .take(table_pointer_quota(works.len(), index));
        for pointer in pointers {
            let history = pointer.metadata().address();
            if !histories.iter().any(|(_, existing)| existing == history) {
                histories.push((index, history.clone()));
            }
        }
    }
    histories
}

pub(super) async fn load_reclaim_decisions(
    attempt: &mut impl StreamRetentionPort,
    works: &[TableCleanupWork],
    pending: Vec<HistoryCleanupRead>,
) -> StorageResult<Vec<HistoryReclaimDecision>> {
    if pending.is_empty() {
        return Ok(Vec::new());
    }
    let mut pending = pending;
    let mut decisions = pending
        .extract_if(.., |read| {
            !works[read.work].table.info.requires_stream_entry()
        })
        .map(|read| read.plan(None))
        .collect::<StorageResult<Vec<_>>>()?;
    if pending.is_empty() {
        return Ok(decisions);
    }
    let (pending, keys) = load_key_images(attempt, works, pending).await?;
    let reads = pending
        .iter()
        .zip(&keys)
        .map(|(read, key)| (works[read.work].table.as_ref(), key))
        .collect::<Vec<_>>();
    let revisions = attempt.read_primary_live_revisions(&reads).await?;
    if revisions.len() != pending.len() {
        return Err(invalid_history());
    }
    decisions.extend(
        pending
            .into_iter()
            .zip(revisions)
            .map(|(read, revision)| {
                read.plan(revision.map(|revision| ItemStreamVersion::new(revision.get())))
            })
            .collect::<StorageResult<Vec<_>>>()?,
    );
    Ok(decisions)
}

impl HistoryCleanupRead {
    fn plan(self, revision: Option<ItemStreamVersion>) -> StorageResult<HistoryReclaimDecision> {
        let plan = ItemHistoryCleanupObservation::try_new(
            self.history.clone(),
            self.frontier,
            self.page,
            revision,
        )?
        .plan_cleanup()?;
        HistoryReclaimDecision::from_plan(self.history, plan)
    }
}

async fn load_key_images(
    attempt: &mut impl StreamRetentionPort,
    works: &[TableCleanupWork],
    mut pending: Vec<HistoryCleanupRead>,
) -> StorageResult<(Vec<HistoryCleanupRead>, Vec<KeyAttributes>)> {
    let references = pending
        .iter()
        .map(|read| read.page.entries[0].reference().clone())
        .collect::<Vec<_>>();
    let bound = crate::constants::TABLE_HISTORY_READ_BYTES;
    let read = ItemHistoryReadBatch {
        references: &references,
        maximum_bytes: bound,
    };
    let headers = attempt.load_item_history_headers(read).await?;
    let (admitted, maximum_bytes) = admit_key_images(&pending, headers)?;
    pending.truncate(admitted);
    let entries = attempt
        .load_item_history_entries(ItemHistoryReadBatch {
            references: &references[..admitted],
            maximum_bytes,
        })
        .await?;
    if entries.len() != admitted {
        return Err(invalid_history());
    }
    let keys = pending
        .iter()
        .zip(entries)
        .map(|(read, entry)| recover_key(&works[read.work], read, entry))
        .collect::<StorageResult<Vec<_>>>()?;
    Ok((pending, keys))
}

pub(super) fn admit_key_images(
    pending: &[HistoryCleanupRead],
    headers: Vec<Option<storage_types::ItemHistoryRecordHeader>>,
) -> StorageResult<(usize, NonZeroUsize)> {
    if headers.len() != pending.len() {
        return Err(invalid_history());
    }
    let mut bytes = 0usize;
    for (index, (read, header)) in pending.iter().zip(headers).enumerate() {
        let header = header.ok_or_else(invalid_history)?;
        if header.metadata() != &read.page.entries[0] {
            return Err(invalid_history());
        }
        if header.record_bytes() > storage_types::ItemHistoryRecord::encoded_length_bound() {
            return Err(invalid_history());
        }
        let next = bytes.saturating_add(header.record_bytes()).saturating_add(
            stream_provider::StreamRowValue::history_envelope_bytes(&read.history)?,
        );
        // One legal wide value must make progress; later values retain the
        // ordinary page bound.
        if index > 0 && next > crate::constants::TABLE_HISTORY_READ_BYTES.get() {
            return Ok((index, NonZeroUsize::new(bytes).ok_or_else(invalid_history)?));
        }
        bytes = next;
    }
    Ok((
        pending.len(),
        NonZeroUsize::new(bytes).ok_or_else(invalid_history)?,
    ))
}

fn recover_key(
    work: &TableCleanupWork,
    read: &HistoryCleanupRead,
    entry: Option<ItemHistoryEntry>,
) -> StorageResult<KeyAttributes> {
    let entry = entry.ok_or_else(invalid_history)?;
    if entry.metadata() != &read.page.entries[0] {
        return Err(invalid_history());
    }
    match entry.change() {
        ItemHistoryChange::Deleted { key } => Ok(key.clone()),
        ItemHistoryChange::Image { item, .. } => {
            let fields = work
                .table
                .key_schema
                .iter()
                .map(|field| field.attribute_name.as_str())
                .collect::<Vec<_>>();
            let values = item.select_attribute_values(&fields)?;
            let mut key = KeyAttributes::with_capacity(fields.len());
            for (field, value) in fields.into_iter().zip(values) {
                key.insert(field.to_owned(), value.ok_or_else(invalid_history)?);
            }
            Ok(key)
        }
    }
}

impl HistoryReclaimDecision {
    fn from_plan(history: ItemHistoryAddress, plan: ItemHistoryCleanupPlan) -> StorageResult<Self> {
        Ok(match plan {
            ItemHistoryCleanupPlan::Complete => Self {
                history,
                delete: None,
                complete: true,
            },
            ItemHistoryCleanupPlan::Delete {
                through,
                continuation,
                ..
            } => Self {
                delete: Some(ItemHistoryReference::try_new(history.clone(), through)?),
                history,
                complete: continuation == HistoryCleanupContinuation::Complete,
            },
        })
    }
}

pub(super) fn select_cleanup_deletes(
    works: &mut [TableCleanupWork],
    decisions: Vec<HistoryReclaimDecision>,
) -> StorageResult<(Vec<HistoryRowDelete>, Vec<Option<usize>>)> {
    let mut application = HistoryReclaimApplication {
        decisions,
        deletes: Vec::new(),
        applied: Vec::new(),
        remaining: crate::constants::TABLE_CLEANUP_PAGE_ROWS,
    };
    let mut last_lanes = vec![None; works.len()];
    let work_count = works.len();
    for (index, work) in works.iter_mut().enumerate() {
        application.remaining = table_pointer_quota(work_count, index);
        for lane in 0..work.lanes.len() {
            if application.apply_lane(work, lane)? {
                last_lanes[index] = Some(lane);
            }
        }
    }
    Ok((application.deletes, last_lanes))
}

fn table_pointer_quota(work_count: usize, index: usize) -> usize {
    let maximum = crate::constants::TABLE_CLEANUP_PAGE_ROWS;
    maximum / work_count + usize::from(index < maximum % work_count)
}

/// Apply a decision once per history while keeping each lane's pointer prefix
/// contiguous.
struct HistoryReclaimApplication {
    decisions: Vec<HistoryReclaimDecision>,
    deletes: Vec<HistoryRowDelete>,
    applied: Vec<ItemHistoryAddress>,
    remaining: usize,
}

impl HistoryReclaimApplication {
    fn apply_lane(&mut self, work: &mut TableCleanupWork, lane: usize) -> StorageResult<bool> {
        let mut visited = false;
        for pointer in std::mem::take(&mut work.lanes[lane].candidates) {
            if self.remaining == 0 {
                break;
            }
            let Some(decision) = self
                .decisions
                .iter()
                .find(|decision| &decision.history == pointer.metadata().address())
            else {
                break;
            };
            self.remaining -= 1;
            visited = true;
            if !self.applied.contains(&decision.history) {
                if let Some(reference) = &decision.delete {
                    self.deletes.push(HistoryRowDelete::Item(reference.clone()));
                    work.deleted_rows += 1;
                }
                self.applied.push(decision.history.clone());
            }
            if !decision.complete {
                break;
            }
            self.deletes
                .push(prepare_pointer_delete(&work.lanes[lane].lane, &pointer)?);
            work.complete_pointer(lane, &pointer)?;
        }
        Ok(visited)
    }
}

fn prepare_pointer_delete(
    lane: &storage_types::TableHistoryLane,
    pointer: &storage_types::ItemHistoryPointer,
) -> StorageResult<HistoryRowDelete> {
    let storage_types::ItemHistoryPublication::Event { table_position, .. } =
        pointer.metadata().publication()
    else {
        return Err(invalid_history());
    };
    Ok(HistoryRowDelete::Table {
        lane: lane.clone(),
        sequence: storage_types::TableStreamSequence::try_from(table_position.offset())?,
    })
}

fn invalid_history() -> StorageError {
    StorageEnum::HistoryUnavailable.into()
}
