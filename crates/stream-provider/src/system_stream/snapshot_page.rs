//! Snapshot visibility and byte admission preserve raw source progress.

use storage_types::{
    StorageEnum, StorageError, StorageResult, StreamRecord, SystemStreamCheckpoint, TimestampMillis,
};

use super::{
    SystemStreamHistoryObservation, SystemStreamHistoryPage, SystemStreamHistoryRead,
    SystemStreamHistoryRecord, page_preparation::PreparedSystemStreamPage,
};
use crate::{
    SystemStreamHistoryReadPage,
    constants::{SYSTEM_HISTORY_WINDOW_MILLIS, SYSTEM_STREAM_PAGE_ENVELOPE_RESERVE_BYTES},
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

    pub(super) fn include_pages(
        &mut self,
        reads: &'a [SystemStreamHistoryRead],
        pages: Vec<SystemStreamHistoryReadPage>,
    ) -> StorageResult<bool> {
        if pages.len() != reads.len() {
            return Err(history_unavailable());
        }
        for (read, page) in reads.iter().zip(pages) {
            if self.include_page(read, page)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn include_page(
        &mut self,
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
        let mut observations = Vec::with_capacity(page.records.len());
        let mut more = page.more;
        for (record, state) in page.records {
            let observation = SystemStreamHistoryRecord {
                position: record.position.clone(),
            };
            if &record.source_scope != self.plan.expanded_checkpoint().scope() {
                return Err(history_unavailable());
            }
            if let Some(storage_types::TableHistoryState::Active(schema)) = state {
                if schema.source_table_name.as_str() != record.source_table_name.as_ref() {
                    return Err(history_unavailable());
                }
                if self.is_visible(record.entry.created_at())? && record.entry.is_public() {
                    let item = record
                        .entry
                        .new_image()
                        .map(|image| image.item.to_attribute_map())
                        .transpose()?;
                    let public = StreamRecord {
                        cursor: None,
                        source_table_name: Some(record.source_table_name),
                        keys: record.entry.keys().clone(),
                        sequence_number: record.position.offset().as_str().to_owned(),
                        item,
                        system_position: Some(record.position),
                    };
                    if !self.include_record(public)? {
                        more = true;
                        break;
                    }
                }
            }
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
        Ok(short && more || self.records.len() == self.prepared.request.limit() as usize)
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
