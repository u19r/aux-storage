use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey},
};
use storage_types::{ReadProtection, StorageResult};

use crate::{archive::ArchivePointer, journal::JournalStore, native_record::corrupt_record};

impl JournalStore {
    pub(in crate::journal) async fn find_archive_pointer<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        key: &DurableRecordKey,
        protection: ReadProtection,
    ) -> StorageResult<Option<ArchivePointer>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = operation.as_mut();
        let result = async {
            let rows = self
                .records
                .load(attempt, &[self.archive_pointer_key(key)?], protection)
                .await?;
            let Some(row) = &rows[0] else {
                return Ok(None);
            };
            let pointer = self.decode_archive_pointer(row)?;
            if pointer.key != *key {
                return Err(corrupt_record());
            }
            Ok(Some(pointer))
        }
        .await;
        operation.complete(result)
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
}
