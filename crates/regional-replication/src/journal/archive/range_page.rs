use std::collections::BTreeMap;

use infrastructure_provider::{
    page::{RecordPage, RecordPageRequest},
    record::DurableRecord,
};
use storage_types::StorageResult;

use crate::{archive::ArchivePointer, journal::JournalStore, native_record::corrupt_record};

pub(super) enum ArchiveRecordCandidate {
    Hot(DurableRecord),
    Archived(ArchivePointer),
}

impl JournalStore {
    pub(super) fn merge_archive_pages(
        &self,
        request: &RecordPageRequest,
        pages: (RecordPage, RecordPage),
    ) -> StorageResult<(Vec<ArchiveRecordCandidate>, bool)> {
        let (hot, indexed) = pages;
        let index_boundary = indexed
            .continuation
            .as_ref()
            .map(|key| self.archive_original_key(key))
            .transpose()?;
        let boundary = hot.continuation.iter().chain(index_boundary.iter()).min();
        let more = boundary.is_some();
        let mut candidates = BTreeMap::new();
        for row in indexed.records {
            let key = self.archive_original_key(&row.key)?;
            let pointer = self.decode_archive_pointer(&row)?;
            if pointer.key != key
                || !request.range.contains(&key)
                || request.after.as_ref().is_some_and(|after| &key <= after)
            {
                return Err(corrupt_record());
            }
            if boundary.is_none_or(|end| &key <= end) {
                candidates.insert(key, ArchiveRecordCandidate::Archived(pointer));
            }
        }
        for row in hot.records {
            if boundary.is_none_or(|end| &row.key <= end) {
                if let Some(ArchiveRecordCandidate::Archived(pointer)) = candidates.get(&row.key) {
                    pointer.require_record(&row)?;
                }
                candidates.insert(row.key.clone(), ArchiveRecordCandidate::Hot(row));
            }
        }
        Ok((candidates.into_values().collect(), more))
    }
}
