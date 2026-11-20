//! Canonical entry prefixes own table retention and atomic deletion progress.

use std::{num::NonZeroU32, sync::Arc};

use storage_types::{
    BoundTableInfo, StorageError, StorageResult, SystemStreamOffset, SystemStreamPosition,
    TableHistoryLane, TableStreamSequence, TimestampMillis,
};
use stream_provider::{
    ReadDirection, SystemStreamHistory, TableHistoryIndexRead, TableHistoryObservationPage,
};

use super::{
    StreamRetentionFinish, StreamRetentionFinishReason, StreamTrimDueMarker, StreamTrimPageLimit,
    StreamTrimState, StreamTrimStateUpdate, StreamTrimStateWrite, TableHistoryTrimLineage,
};

pub(super) struct TableCleanupLane {
    pub lane: TableHistoryLane,
    pub through: SystemStreamPosition,
    pub candidates: Vec<stream_provider::TableHistoryObservation>,
}

pub(super) struct TableCleanupWork {
    pub marker: StreamTrimDueMarker,
    pub table: Arc<BoundTableInfo>,
    pub expected: StreamTrimState,
    pub state: StreamTrimState,
    pub lanes: Vec<TableCleanupLane>,
    /// Send progress of a paired table (`None`: unpaired).
    holds: Option<
        Vec<(
            regional_replication::message::LineageRef,
            crate::LineageCursor,
        )>,
    >,
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
            holds: None,
            next_event_due: None,
            needs_more: false,
            deleted_rows: 0,
        };
        work.bind_source_lanes(source)?;
        Ok(work)
    }

    pub(super) fn set_holds(
        &mut self,
        holds: Vec<(
            regional_replication::message::LineageRef,
            crate::LineageCursor,
        )>,
    ) {
        self.holds = Some(holds);
    }

    /// A paired table keeps every entry its peer has not acknowledged (P11).
    fn held(
        &self,
        index: usize,
        entry: &stream_provider::TableHistoryObservation,
    ) -> StorageResult<bool> {
        let Some(holds) = &self.holds else {
            return Ok(false);
        };
        let lineage = &self.state.frontier.lineages[index];
        let Some((_, hold)) = holds.iter().find(|(known, _)| {
            known.shard == lineage.shard_id && known.lineage == lineage.lineage_id
        }) else {
            return Ok(true);
        };
        Ok(crate::regional_receive::offset_order(
            Some(entry.position.offset()),
            hold.through.as_ref(),
        )? == std::cmp::Ordering::Greater)
    }

    /// How far an empty lane retires: its head; on a paired table at most its
    /// send record, and nothing without one (the sender refuses history retired
    /// past the acknowledgment).
    fn empty_lane_bound(&self, index: usize) -> StorageResult<Option<SystemStreamOffset>> {
        use crate::regional_receive::offset_order;
        let head = self.lanes[index].through.offset();
        let Some(holds) = &self.holds else {
            return Ok(Some(head.clone()));
        };
        let lineage = &self.state.frontier.lineages[index];
        let Some((_, hold)) = holds.iter().find(|(known, _)| {
            known.shard == lineage.shard_id && known.lineage == lineage.lineage_id
        }) else {
            return Ok(None);
        };
        let bound = if offset_order(Some(head), hold.through.as_ref())?.is_gt() {
            hold.through.as_ref()
        } else {
            Some(head)
        };
        Ok(offset_order(bound, lineage.retired_through.as_ref())?
            .is_gt()
            .then(|| bound.cloned())
            .flatten())
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
        maximum: usize,
    ) -> StorageResult<Vec<TableHistoryIndexRead>> {
        let quota = u32::from(limit)
            .min(maximum as u32)
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
        pages: &mut impl Iterator<Item = TableHistoryObservationPage>,
        now: TimestampMillis,
    ) -> StorageResult<()> {
        // `reads` issued one read per lane and the driver proved one page per
        // read, so this lane's pages are exactly the next `lanes.len()`.
        for (index, page) in pages.take(self.lanes.len()).enumerate() {
            self.observe_lane(index, page, now)?;
        }
        Ok(())
    }

    fn observe_lane(
        &mut self,
        index: usize,
        page: TableHistoryObservationPage,
        now: TimestampMillis,
    ) -> StorageResult<()> {
        if page.entries.is_empty() {
            if self.state.retention != storage_types::StreamRetentionDuration::Forever
                && let Some(bound) = self.empty_lane_bound(index)?
            {
                self.state.frontier.lineages[index].retired_through = Some(bound);
            }
            let progress = &mut self.state.frontier.lineages[index];
            progress.cleaned_through = progress.retired_through.clone();
            return Ok(());
        }
        let count = page.entries.len();
        for entry in page.entries {
            if !self.include_entry(index, entry, now)? {
                break;
            }
        }
        self.needs_more |= self.lanes[index].candidates.len() == count && page.more;
        Ok(())
    }

    fn include_entry(
        &mut self,
        index: usize,
        entry: stream_provider::TableHistoryObservation,
        now: TimestampMillis,
    ) -> StorageResult<bool> {
        let retired = self.state.frontier.lineages[index]
            .retired_through
            .as_ref()
            .map(|through| entry.position.offset().compare_order(through))
            .transpose()
            .map_err(|_| invalid_cleanup())?
            .is_some_and(|order| !order.is_gt());
        if !retired && self.held(index, &entry)? {
            let recheck = now + crate::constants::REGIONAL_HOLD_RECHECK_MILLIS;
            self.next_event_due = Some(
                self.next_event_due
                    .map_or(recheck, |current| current.min(recheck)),
            );
            return Ok(false);
        }
        let created_at = entry.created_at;
        let expires = self
            .state
            .retention
            .cutoff_at(now)
            .is_some_and(|cutoff| created_at <= cutoff);
        // Commit order does not imply timestamp order: retain the entire suffix
        // after the first unexpired event.
        if !retired && !expires {
            if let storage_types::StreamRetentionDuration::FiniteHours(hours) = self.state.retention
            {
                let due = created_at + i64::from(hours) * 3600 * 1000;
                self.next_event_due =
                    Some(self.next_event_due.map_or(due, |current| current.min(due)));
            }
            return Ok(false);
        }
        if !retired {
            self.state.frontier.lineages[index].retired_through =
                Some(entry.position.offset().clone());
        }
        self.lanes[index].candidates.push(entry);
        Ok(true)
    }

    pub(super) fn select_deletes(
        &mut self,
        maximum: usize,
    ) -> (Vec<stream_provider::StreamEntryAddress>, Option<usize>) {
        let mut deletes = Vec::new();
        let mut last_lane = None;
        for (index, lane) in self.lanes.iter_mut().enumerate() {
            for entry in lane
                .candidates
                .drain(..)
                .take(maximum.saturating_sub(deletes.len()))
            {
                self.state.frontier.lineages[index].cleaned_through =
                    Some(entry.position.offset().clone());
                deletes.push(stream_provider::StreamEntryAddress {
                    history_id: entry.history_id,
                    position: entry.position,
                });
                self.deleted_rows += 1;
                last_lane = Some(index);
            }
        }
        (deletes, last_lane)
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
