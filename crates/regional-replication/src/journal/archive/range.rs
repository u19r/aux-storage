use infrastructure_provider::{
    page::{BoundedRecordRange, RecordPage, RecordPageRequest},
    physical_attempt::PhysicalRecordAttempt,
    record::DurableRecordKey,
};
use storage_types::StorageResult;

use super::range_page::ArchiveRecordCandidate;
use crate::{journal::JournalStore, native_record::corrupt_record};

impl JournalStore {
    /// Merge only complete known prefixes; unknown next-page keys cannot be
    /// skipped.
    pub(in crate::journal) async fn scan_immutable_records<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: RecordPageRequest,
    ) -> StorageResult<RecordPage> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            request.validate(self.records.limits)?;
            if self.archive.get().is_none() {
                return self.records.scan(attempt, request).await;
            }
            let hot = self
                .records
                .scan(
                    attempt,
                    RecordPageRequest {
                        range: request.range.clone(),
                        after: request.after.clone(),
                        max_bytes: request.max_bytes,
                        protection: request.protection,
                    },
                )
                .await?;
            let indexed = self
                .records
                .scan(attempt, self.archive_range_request(&request)?)
                .await?;
            let (candidates, mut more) = self.merge_archive_pages(&request, (hot, indexed))?;
            let mut selected = Vec::with_capacity(candidates.len());
            let mut used = 0usize;
            for candidate in candidates {
                let size = match &candidate {
                    ArchiveRecordCandidate::Hot(record) => {
                        self.records.limits.validate_record(record)?
                    }
                    ArchiveRecordCandidate::Archived(pointer) => {
                        self.archive_record_size(pointer)?
                    }
                };
                let next = used.checked_add(size).ok_or_else(corrupt_record)?;
                if selected.len() == request.range.bounds().2.get()
                    || next > request.max_bytes.get()
                {
                    more = true;
                    break;
                }
                selected.push(candidate);
                used = next;
            }
            // The whole selected prefix is admitted before any archive object
            // I/O.
            let archived_publications = selected
                .iter()
                .filter_map(|candidate| match candidate {
                    ArchiveRecordCandidate::Archived(pointer)
                        if crate::archive::ArchiveKind::from_key(&pointer.key)
                            .is_some_and(crate::archive::ArchiveKind::has_publication_coverage) =>
                    {
                        Some(pointer.key.clone())
                    }
                    _ => None,
                })
                .collect::<std::collections::BTreeSet<_>>();
            let results =
                futures::future::join_all(selected.into_iter().map(|candidate| async move {
                    match candidate {
                        ArchiveRecordCandidate::Hot(record) => Ok(record),
                        ArchiveRecordCandidate::Archived(pointer) => {
                            self.load_archive_record(&pointer).await
                        }
                    }
                }))
                .await;
            let records = results.into_iter().collect::<StorageResult<Vec<_>>>()?;
            for record in records
                .iter()
                .filter(|record| archived_publications.contains(&record.key))
            {
                self.require_archive_publication_coverage(attempt, record, request.protection)
                    .await?;
            }
            RecordPage::try_new(
                records.into_iter().map(Ok),
                more,
                self.records.limits,
                &request,
            )
        }
        .await;
        settlement_guard.complete(result)
    }

    fn archive_range_request(
        &self,
        request: &RecordPageRequest,
    ) -> StorageResult<RecordPageRequest> {
        let (first, last, count) = request.range.bounds();
        Ok(RecordPageRequest {
            range: BoundedRecordRange::try_new(
                self.archive_pointer_key(first)?,
                self.archive_pointer_key(last)?,
                count,
            )?,
            after: request
                .after
                .as_ref()
                .map(|key| self.archive_pointer_key(key))
                .transpose()?,
            max_bytes: self.records.limits.max_bytes(),
            protection: request.protection,
        })
    }

    pub(super) fn archive_original_key(
        &self,
        key: &DurableRecordKey,
    ) -> StorageResult<DurableRecordKey> {
        let suffix = key
            .as_ref()
            .suffix
            .strip_prefix(crate::constants::ARCHIVE_POINTER_SUFFIX)
            .ok_or_else(corrupt_record)?;
        let original = self.record_key(suffix.to_vec())?;
        if self.archive_pointer_key(&original)? != *key {
            return Err(corrupt_record());
        }
        Ok(original)
    }
}
