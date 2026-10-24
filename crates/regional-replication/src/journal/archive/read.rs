use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::DurableRecord};
use storage_types::{ReadProtection, StorageResult};

use crate::{archive::ArchivePointer, journal::JournalStore, native_record::corrupt_record};

impl JournalStore {
    pub(in crate::journal) async fn load_archive_pointer<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &DurableRecord,
        protection: ReadProtection,
    ) -> StorageResult<Option<ArchivePointer>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let key = self.archive_pointer_key(&record.key)?;
            let Some(row) = self
                .records
                .load(attempt, &[key], protection)
                .await?
                .pop()
                .flatten()
            else {
                return Ok(None);
            };
            let pointer = self.decode_archive_pointer(&row)?;
            pointer.require_record(record)?;
            Ok(Some(pointer))
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) fn decode_archive_pointer(
        &self,
        row: &DurableRecord,
    ) -> StorageResult<ArchivePointer> {
        let pointer: ArchivePointer = self.records.decode(row)?;
        if row.revision != storage_types::Revision::INITIAL
            || row.key != self.archive_pointer_key(&pointer.key)?
        {
            return Err(corrupt_record());
        }
        self.archive_record_size(&pointer)?;
        Ok(pointer)
    }

    pub(in crate::journal) async fn fetch_archive_original(
        archive: &crate::archive::CanonicalArchive,
        pointer: &ArchivePointer,
        manifest: &stream_provider::retained_image::RetainedImageManifest,
    ) -> StorageResult<std::sync::Arc<[u8]>> {
        if pointer.length != manifest.original_length() {
            return Err(corrupt_record());
        }
        let bytes = archive
            .fetch_object(pointer.content, pointer.length)
            .await?;
        manifest.require_original(&bytes)?;
        Ok(bytes)
    }
}
