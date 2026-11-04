//! Qualified table-pointer prefixes own discoverable item-history cleanup work.

use std::{num::NonZeroU32, sync::Arc};

use storage_types::{
    BoundTableInfo, ItemHistoryPointer, ItemHistoryPublication, StorageError, StorageResult,
    SystemStreamPosition, TableHistoryLane, TableStreamSequence, TimestampMillis,
};
use stream_provider::{
    ReadDirection, SystemStreamHistory, TableHistoryIndexPage, TableHistoryIndexRead,
};

use super::{
    StreamRetentionFinish, StreamRetentionFinishReason, StreamTrimDueMarker, StreamTrimPageLimit,
    StreamTrimState, StreamTrimStateUpdate, StreamTrimStateWrite, TableHistoryTrimLineage,
};

pub(super) struct TableCleanupLane {
    pub lane: TableHistoryLane,
    pub through: SystemStreamPosition,
    pub candidates: Vec<ItemHistoryPointer>,
}

pub(super) struct TableCleanupWork {
    pub marker: StreamTrimDueMarker,
    pub table: Arc<BoundTableInfo>,
    pub expected: StreamTrimState,
    pub state: StreamTrimState,
    pub lanes: Vec<TableCleanupLane>,
    next_event_due: Option<TimestampMillis>,
    needs_more: bool,
    pub deleted_rows: usize,
}

impl TableCleanupWork {
    pub(super) fn prepare(
        marker: StreamTrimDueMarker,
        table: Arc<BoundTableInfo>,
        state: StreamTrimState,
        source: &SystemStreamHistory,
    ) -> StorageResult<Self> {
        state.require_valid_frontier()?;
        if state.frontier.table_history_id != table.history_id
            || state.frontier.source_scope != *source.as_ref().source_scope()
        {
            return Err(invalid_cleanup());
        }
        let expected = state.clone();
        let mut work = Self {
            marker,
            table,
            expected,
            state,
            lanes: Vec::new(),
            next_event_due: None,
            needs_more: false,
            deleted_rows: 0,
        };
        work.bind_source_lanes(source)?;
        Ok(work)
    }

    fn bind_source_lanes(&mut self, source: &SystemStreamHistory) -> StorageResult<()> {
        let boundaries = source
            .retirement_boundaries()
            .map_err(|_| invalid_cleanup())?;
        for through in &boundaries {
            if !self.state.frontier.lineages.iter().any(|lane| {
                lane.lineage_id == *through.lineage_id() && lane.shard_id == *through.shard_id()
            }) {
                self.state.frontier.lineages.push(TableHistoryTrimLineage {
                    shard_id: through.shard_id().clone(),
                    lineage_id: through.lineage_id().clone(),
                    retired_through: None,
                    cleaned_through: None,
                });
            }
        }
        for lineage in &self.state.frontier.lineages {
            let through = boundaries
                .iter()
                .find(|position| {
                    position.shard_id() == &lineage.shard_id
                        && position.lineage_id() == &lineage.lineage_id
                })
                .ok_or_else(invalid_cleanup)?
                .clone();
            self.lanes.push(TableCleanupLane {
                lane: TableHistoryLane::try_from((
                    self.table.history_id,
                    &self.state.frontier.source_scope,
                    &through,
                ))?,
                through,
                candidates: Vec::new(),
            });
        }
        Ok(())
    }

    pub(super) fn reads(
        &self,
        limit: StreamTrimPageLimit,
    ) -> StorageResult<Vec<TableHistoryIndexRead>> {
        let quota = u32::from(limit)
            .min(crate::constants::TABLE_CLEANUP_PAGE_ROWS as u32)
            .div_ceil(self.lanes.len() as u32)
            .max(1);
        self.lanes
            .iter()
            .zip(&self.state.frontier.lineages)
            .map(|(lane, progress)| {
                let through = if self.state.retention
                    == storage_types::StreamRetentionDuration::Forever
                {
                    progress
                        .retired_through
                        .as_ref()
                        .map(TableStreamSequence::try_from)
                        .transpose()?
                        .unwrap_or_else(|| {
                            TableStreamSequence::from(storage_types::StreamItemId::from([0; 12]))
                        })
                } else {
                    TableStreamSequence::try_from(lane.through.offset())?
                };
                Ok(TableHistoryIndexRead {
                    lane: lane.lane.clone(),
                    exclusive_start: progress
                        .cleaned_through
                        .as_ref()
                        .map(TableStreamSequence::try_from)
                        .transpose()?,
                    through,
                    direction: ReadDirection::Forward,
                    limit: NonZeroU32::new(quota)
                        .ok_or_else(|| StorageError::internal("table cleanup quota is zero"))?,
                })
            })
            .collect()
    }

    pub(super) fn observe_pages(
        &mut self,
        pages: Vec<TableHistoryIndexPage>,
        now: TimestampMillis,
    ) -> StorageResult<()> {
        if pages.len() != self.lanes.len() {
            return Err(invalid_cleanup());
        }
        for (index, page) in pages.into_iter().enumerate() {
            self.observe_lane(index, page, now)?;
        }
        Ok(())
    }

    fn observe_lane(
        &mut self,
        index: usize,
        page: TableHistoryIndexPage,
        now: TimestampMillis,
    ) -> StorageResult<()> {
        if page.pointers.is_empty() {
            let progress = &mut self.state.frontier.lineages[index];
            if self.state.retention != storage_types::StreamRetentionDuration::Forever {
                progress.retired_through = Some(self.lanes[index].through.offset().clone());
            }
            progress.cleaned_through = progress.retired_through.clone();
            return Ok(());
        }
        let count = page.pointers.len();
        for pointer in page.pointers {
            if !self.include_pointer(index, pointer, now)? {
                break;
            }
        }
        self.needs_more |= self.lanes[index].candidates.len() == count && page.more;
        Ok(())
    }

    fn include_pointer(
        &mut self,
        index: usize,
        pointer: ItemHistoryPointer,
        now: TimestampMillis,
    ) -> StorageResult<bool> {
        let ItemHistoryPublication::Event {
            table_position,
            created_at,
            ..
        } = pointer.metadata().publication()
        else {
            return Err(invalid_cleanup());
        };
        let retired = self.state.frontier.is_retired(pointer.metadata())?;
        let expires = self
            .state
            .retention
            .cutoff_at(now)
            .is_some_and(|cutoff| *created_at <= cutoff);
        if !retired && !expires {
            if let storage_types::StreamRetentionDuration::FiniteHours(hours) = self.state.retention
            {
                let due = *created_at + i64::from(hours) * 3600 * 1000;
                self.next_event_due =
                    Some(self.next_event_due.map_or(due, |current| current.min(due)));
            }
            return Ok(false);
        }
        if !retired {
            self.state.frontier.lineages[index].retired_through =
                Some(table_position.offset().clone());
        }
        self.lanes[index].candidates.push(pointer);
        Ok(true)
    }

    pub(super) fn complete_pointer(
        &mut self,
        lane: usize,
        pointer: &ItemHistoryPointer,
    ) -> StorageResult<()> {
        let ItemHistoryPublication::Event { table_position, .. } = pointer.metadata().publication()
        else {
            return Err(invalid_cleanup());
        };
        self.state.frontier.lineages[lane].cleaned_through = Some(table_position.offset().clone());
        self.deleted_rows += 1;
        Ok(())
    }

    pub(super) fn finish(
        mut self,
        now: TimestampMillis,
        last_lane: Option<usize>,
    ) -> StorageResult<StreamRetentionFinish> {
        let pending = self.schedule_cleanup(now)?;
        if last_lane.is_some() || !pending {
            self.state.updated_at = now;
        }
        if let Some(lane) = last_lane {
            let count = self.state.frontier.lineages.len();
            self.state.frontier.lineages.rotate_left((lane + 1) % count);
        }
        self.state.require_valid_frontier()?;
        let next_marker = self.state.next_due_at.map(|due| {
            StreamTrimDueMarker::new(due, self.state.scope.clone(), self.state.policy_version)
        });
        Ok(StreamRetentionFinish {
            marker: self.marker,
            reason: if self.state.retention == storage_types::StreamRetentionDuration::Forever
                && !pending
            {
                StreamRetentionFinishReason::Forever
            } else {
                StreamRetentionFinishReason::Page {
                    deleted_rows: self.deleted_rows,
                }
            },
            update: Some(StreamTrimStateUpdate {
                expected_state: self.expected,
                write: StreamTrimStateWrite {
                    state: self.state,
                    next_marker,
                    replaced_marker: None,
                },
            }),
        })
    }

    fn schedule_cleanup(&mut self, now: TimestampMillis) -> StorageResult<bool> {
        let pending = self.needs_more || self.state.frontier.has_pending_cleanup()?;
        let sweep_due = self
            .state
            .frontier
            .item_history_sweep
            .as_ref()
            .map(|sweep| sweep.not_before);
        self.state.next_due_at = if pending {
            // Yield this exact discovery window to other due tables. A bucket
            // or equal deadline can repeatedly select the same full page.
            Some(TimestampMillis::from_timestamp(
                now.timestamp_millis()
                    .checked_add(i64::from(crate::constants::TABLE_CLEANUP_YIELD_MILLIS))
                    .ok_or_else(invalid_cleanup)?,
            ))
        } else {
            self.next_event_due
                .into_iter()
                .chain(sweep_due)
                .min()
                .or_else(|| match self.state.retention {
                    storage_types::StreamRetentionDuration::Forever => None,
                    storage_types::StreamRetentionDuration::FiniteHours(hours) => {
                        Some(now + i64::from(hours) * 3600 * 1000)
                    }
                })
        };
        Ok(pending)
    }
}

fn invalid_cleanup() -> StorageError {
    StorageError::internal("Table cleanup differs from its source authority")
}
