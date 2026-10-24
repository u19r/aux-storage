use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey},
};
use storage_types::{ReadProtection, Revision, StorageResult};

use crate::{
    archive::{ArchiveKind, ArchivePointer},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn load_archive_dependency_metadata<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        key: &DurableRecordKey,
        protection: ReadProtection,
    ) -> StorageResult<DurableRecord> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            if let Some(record) = self
                .records
                .load(attempt, std::slice::from_ref(key), protection)
                .await?
                .pop()
                .flatten()
            {
                return Ok(record);
            }
            let pointer_key = self.archive_pointer_key(key)?;
            let row = self
                .records
                .load(attempt, &[pointer_key], protection)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let pointer = self.decode_archive_pointer(&row)?;
            if pointer.key != *key {
                return Err(corrupt_record());
            }
            self.load_archive_record(&pointer).await
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(super) fn archive_metadata_pointer(
        &self,
        record: &DurableRecord,
    ) -> StorageResult<ArchivePointer> {
        let digest = replication_types::protocol::CanonicalDigest::of(&record.bytes);
        let pointer = ArchivePointer {
            key: record.key.clone(),
            revision: record.revision,
            manifest: digest,
            metadata_length: record.bytes.len(),
            content: digest,
            length: record.bytes.len(),
        };
        self.archive_record_size(&pointer)?;
        Ok(pointer)
    }

    pub(in crate::journal) async fn load_immutable_waves<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        keys: &[DurableRecordKey],
        protection: ReadProtection,
    ) -> StorageResult<Vec<Option<DurableRecord>>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let waves = self.records.read_waves(keys, |key| key)?;
            let mut rows = self.records.load(attempt, keys, protection).await?;
            if self.archive.get().is_none() {
                return Ok(rows);
            }
            let missing = keys
                .iter()
                .enumerate()
                .filter(|(slot, key)| rows[*slot].is_none() && ArchiveKind::from_key(key).is_some())
                .map(|(slot, key)| self.archive_pointer_key(key).map(|pointer| (slot, pointer)))
                .collect::<StorageResult<Vec<_>>>()?;
            if missing.is_empty() {
                return Ok(rows);
            }
            let pointer_keys: Vec<_> = missing.iter().map(|(_, key)| key.clone()).collect();
            let pointers = self
                .records
                .load(attempt, &pointer_keys, protection)
                .await?;
            let mut recovered = Vec::with_capacity(missing.len());
            let mut sizes = keys
                .iter()
                .zip(&rows)
                .map(|(key, row)| match row {
                    Some(record) => self.records.limits.validate_record(record),
                    None => self.records.limits.validate_key(key),
                })
                .collect::<StorageResult<Vec<_>>>()?;
            for ((slot, _), pointer) in missing.into_iter().zip(pointers) {
                let Some(pointer) = pointer else {
                    continue;
                };
                let pointer = self.decode_archive_pointer(&pointer)?;
                if pointer.key != keys[slot] {
                    return Err(corrupt_record());
                }
                sizes[slot] = self.archive_record_size(&pointer)?;
                recovered.push((slot, pointer));
            }
            for wave in &waves {
                let bytes = sizes[wave.clone()].iter().try_fold(0usize, |sum, size| {
                    sum.checked_add(*size).ok_or_else(corrupt_record)
                })?;
                self.records.limits.check_batch(wave.len(), bytes)?;
            }
            let mut recovered = recovered.into_iter().peekable();
            for wave in waves {
                let mut selected = Vec::new();
                while recovered
                    .peek()
                    .is_some_and(|(slot, _)| wave.contains(slot))
                {
                    selected.push(recovered.next().ok_or_else(corrupt_record)?);
                }
                // Settle every dispatched bounded wave before releasing source
                // capacity.
                let results = futures::future::join_all(selected.into_iter().map(
                    |(slot, pointer)| async move {
                        self.load_archive_record(&pointer)
                            .await
                            .map(|record| (slot, record))
                    },
                ))
                .await;
                for result in results {
                    let (slot, record) = result?;
                    if ArchiveKind::from_key(&record.key)
                        .is_some_and(ArchiveKind::has_publication_coverage)
                    {
                        self.require_archive_publication_coverage(attempt, &record, protection)
                            .await?;
                    }
                    rows[slot] = Some(record);
                }
            }
            Ok(rows)
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) async fn load_archive_record(
        &self,
        pointer: &ArchivePointer,
    ) -> StorageResult<DurableRecord> {
        self.archive_record_size(pointer)?;
        let archive = self.archive.get().ok_or_else(corrupt_record)?;
        let bytes = archive
            .fetch_object(pointer.manifest, pointer.metadata_length)
            .await?;
        let record = DurableRecord {
            key: pointer.key.clone(),
            revision: pointer.revision,
            bytes,
        };
        pointer.require_record(&record)?;
        self.records.limits.validate_record(&record)?;
        Ok(record)
    }

    pub(in crate::journal) fn archive_record_size(
        &self,
        pointer: &ArchivePointer,
    ) -> StorageResult<usize> {
        let kind = ArchiveKind::from_key(&pointer.key).ok_or_else(corrupt_record)?;
        if pointer.revision != Revision::INITIAL
            || pointer.length == 0
            || (pointer.content == pointer.manifest && pointer.length != pointer.metadata_length)
            || (!kind.is_payload()
                && (pointer.content != pointer.manifest
                    || pointer.length != pointer.metadata_length))
            || self.record_key(pointer.key.as_ref().suffix.clone())? != pointer.key
            || pointer.metadata_length == 0
            || pointer.metadata_length > self.records.limits.max_record_bytes().get()
        {
            return Err(corrupt_record());
        }
        let size = self
            .records
            .limits
            .validate_key(&pointer.key)?
            .checked_add(pointer.metadata_length)
            .ok_or_else(corrupt_record)?;
        self.records.limits.check_batch(1, size)?;
        Ok(size)
    }
}
