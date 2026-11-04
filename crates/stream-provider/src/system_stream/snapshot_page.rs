//! Snapshot visibility and byte admission preserve raw source progress.

use storage_types::{
    ItemHistoryPointer, StorageEnum, StorageError, StorageResult, StreamRecord,
    SystemStreamCheckpoint, TimestampMillis,
};

use super::{
    SystemStreamHistoryObservation, SystemStreamHistoryPage, SystemStreamHistoryRead,
    SystemStreamHistoryRecord, page_preparation::PreparedSystemStreamPage,
};
use crate::{
    ItemHistoryImages, StreamAttempt, SystemStreamHistoryReadPage,
    constants::{
        SYSTEM_HISTORY_READ_BYTES, SYSTEM_HISTORY_WINDOW_MILLIS,
        SYSTEM_STREAM_PAGE_ENVELOPE_RESERVE_BYTES,
    },
};

pub(super) struct SystemSnapshotPage<'a> {
    plan: &'a SystemStreamHistoryPage,
    prepared: &'a PreparedSystemStreamPage,
    records: Vec<StreamRecord>,
    observations: Vec<SystemStreamHistoryObservation<'a>>,
    more: Vec<bool>,
    encoded_bytes: usize,
    initial_visibility_cutoff: TimestampMillis,
}

enum SnapshotRow {
    Skipped(SystemStreamHistoryRecord),
    Visible(SystemStreamHistoryRecord),
}

struct PreparedSnapshotRows {
    rows: Vec<SnapshotRow>,
    pointers: Vec<ItemHistoryPointer>,
    more: bool,
}

impl<'a> SystemSnapshotPage<'a> {
    pub(super) fn new(
        plan: &'a SystemStreamHistoryPage,
        prepared: &'a PreparedSystemStreamPage,
        initial_visibility_cutoff: TimestampMillis,
    ) -> Self {
        Self {
            plan,
            prepared,
            records: Vec::new(),
            observations: Vec::new(),
            more: Vec::new(),
            encoded_bytes: 0,
            initial_visibility_cutoff,
        }
    }

    pub(super) async fn include_pages<A: StreamAttempt>(
        &mut self,
        attempt: &mut A,
        reads: &'a [SystemStreamHistoryRead],
        pages: Vec<SystemStreamHistoryReadPage>,
    ) -> StorageResult<bool> {
        if pages.len() != reads.len() {
            return Err(history_unavailable());
        }
        for (read, page) in reads.iter().zip(pages) {
            if self.include_page(attempt, read, page).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn include_page<A: StreamAttempt>(
        &mut self,
        attempt: &mut A,
        read: &'a SystemStreamHistoryRead,
        page: SystemStreamHistoryReadPage,
    ) -> StorageResult<bool> {
        if let Some(retired) = &page.retired_through
            && read
                .after
                .offset()
                .compare_order(retired.offset())
                .map_err(|_| history_unavailable())?
                .is_lt()
        {
            return Err(history_unavailable());
        }
        let retired_through = page.retired_through.clone();
        let prepared = self.prepare_rows(page)?;
        let resolved = crate::resolve_item_history(
            attempt,
            &prepared.pointers,
            ItemHistoryImages::Snapshot,
            SYSTEM_HISTORY_READ_BYTES,
        )
        .await?;
        let mut values = resolved.into_iter();
        let mut observations = Vec::with_capacity(prepared.rows.len());
        let mut more = prepared.more;
        for row in prepared.rows {
            let observation = match row {
                SnapshotRow::Skipped(observation) => observation,
                SnapshotRow::Visible(observation) => {
                    let resolved = values.next().ok_or_else(history_unavailable)?;
                    if let Some(resolved) = resolved
                        && !self.include_record(StreamRecord::try_from(&resolved)?)?
                    {
                        more = true;
                        break;
                    }
                    observation
                }
            };
            observations.push(observation);
        }
        if more && observations.is_empty() {
            return Ok(true);
        }
        let short = observations.len() < read.limit as usize;
        self.observations.push(SystemStreamHistoryObservation {
            retired_through,
            read,
            records: observations,
        });
        self.more.push(more);
        Ok(
            short && (read.ordering == super::SystemStreamHistoryOrdering::Contiguous || more)
                || self.records.len() == self.prepared.request.limit() as usize,
        )
    }

    fn prepare_rows(
        &self,
        page: SystemStreamHistoryReadPage,
    ) -> StorageResult<PreparedSnapshotRows> {
        if page.records.len() != page.observations.len() {
            return Err(history_unavailable());
        }
        let mut prepared = PreparedSnapshotRows {
            rows: Vec::new(),
            pointers: Vec::new(),
            more: page.more,
        };
        let mut admitted = 0_usize;
        for (record, observation) in page.records.into_iter().zip(page.observations) {
            if record.position() != &observation.position {
                return Err(history_unavailable());
            }
            record.require_scope(self.plan.expanded_checkpoint().scope())?;
            let Some(pointer) = record.pointer() else {
                prepared.rows.push(SnapshotRow::Skipped(observation));
                continue;
            };
            if !self.is_visible(record.created_at())? {
                prepared.rows.push(SnapshotRow::Skipped(observation));
                continue;
            }
            let bytes = pointer.record_bytes()
                + crate::StreamRowValue::history_envelope_bytes(pointer.metadata().address())?;
            admitted = admitted
                .checked_add(bytes)
                .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            if admitted > SYSTEM_HISTORY_READ_BYTES.get() {
                prepared.more = true;
                break;
            }
            prepared.pointers.push(pointer.clone());
            prepared.rows.push(SnapshotRow::Visible(observation));
        }
        Ok(prepared)
    }

    fn is_visible(&self, created_at: TimestampMillis) -> StorageResult<bool> {
        let cutoff = self.prepared.issued_at - SYSTEM_HISTORY_WINDOW_MILLIS;
        if created_at <= cutoff {
            return if created_at > self.initial_visibility_cutoff {
                Err(history_unavailable())
            } else {
                Ok(false)
            };
        }
        Ok(true)
    }

    fn include_record(&mut self, record: StreamRecord) -> StorageResult<bool> {
        let bytes = serde_json::to_vec(&record)?.len() + usize::from(!self.records.is_empty());
        let next = self.encoded_bytes.saturating_add(bytes);
        if self.records.len() == self.prepared.request.limit() as usize
            || next + SYSTEM_STREAM_PAGE_ENVELOPE_RESERVE_BYTES
                > storage_types::STREAM_RECORDS_MAX_ENCODED_BYTES as usize
        {
            if self.records.is_empty() {
                return Err(StorageError::validation(
                    "The first SystemStream record exceeds the response byte limit",
                ));
            }
            return Ok(false);
        }
        self.encoded_bytes = next;
        self.records.push(record);
        Ok(true)
    }

    pub(super) fn finish(self) -> StorageResult<(Vec<StreamRecord>, SystemStreamCheckpoint)> {
        let next = self
            .plan
            .finish_bounded(&self.observations, &self.more)
            .map_err(|_| history_unavailable())?;
        Ok((self.records, next))
    }
}

fn history_unavailable() -> StorageError {
    StorageEnum::HistoryUnavailable.into()
}
