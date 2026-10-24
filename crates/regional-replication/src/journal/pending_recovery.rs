//! Durable round-robin selection shared by accepted regional control workflows.
use infrastructure_provider::{
    page::{BoundedRecordRange, RecordPageRequest},
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey},
};
use storage_types::{ReadProtection, StorageResult};

use super::JournalStore;

impl JournalStore {
    pub(in crate::journal) async fn select_pending_record<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        suffixes: (&[u8], &[u8]),
    ) -> StorageResult<Option<DurableRecord>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let cursor_key = self.record_key(suffixes.1.to_vec())?;
            let cursor = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&cursor_key),
                    ReadProtection::Conflict,
                )
                .await?
                .pop()
                .flatten();
            let after: Option<DurableRecordKey> = cursor
                .as_ref()
                .map(|row| self.records.decode(row))
                .transpose()?
                .flatten();
            let first = self.record_key(suffixes.0.to_vec())?;
            let bounds = self.record_prefix_range(&first)?;
            let range = BoundedRecordRange::try_new(
                first,
                bounds.bounds().1.clone(),
                std::num::NonZeroUsize::MIN,
            )?;
            let page = self
                .records
                .scan(
                    attempt,
                    RecordPageRequest {
                        range: range.clone(),
                        after: after.clone(),
                        max_bytes: self.records.limits.max_bytes(),
                        protection: ReadProtection::Shared,
                    },
                )
                .await?;
            let mut selected = page.records.into_iter().next();
            if selected.is_none() && after.is_some() {
                let page = self
                    .records
                    .scan(
                        attempt,
                        RecordPageRequest {
                            range,
                            after: None,
                            max_bytes: self.records.limits.max_bytes(),
                            protection: ReadProtection::Shared,
                        },
                    )
                    .await?;
                selected = page.records.into_iter().next();
            }
            let next = selected.as_ref().map(|row| row.key.clone());
            if next != after {
                let change = self.records.prepare(
                    cursor_key,
                    cursor.as_ref().map(|row| row.revision),
                    &next,
                )?;
                self.records.store(attempt, &[change]).await?;
            }
            Ok(selected)
        }
        .await;
        settlement.complete(result)
    }
}

impl JournalStore {
    pub(in crate::journal) fn decode_pending_control_reference(
        &self,
        row: &DurableRecord,
    ) -> StorageResult<crate::control::pending::PendingControlReference> {
        let reference: crate::control::pending::PendingControlReference =
            self.records.decode(row)?;
        if row.revision != storage_types::Revision::INITIAL
            || row.key
                != self.identity_key(
                    crate::constants::CONTROL_PENDING_SUFFIX,
                    &reference.operation(),
                )?
        {
            return Err(crate::native_record::corrupt_record());
        }
        Ok(reference)
    }
}
