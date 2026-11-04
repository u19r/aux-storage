//! One verified original is published and released in its native attempt.
use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::RecordChange};
use storage_types::{ReadProtection, StorageResult};

use crate::{
    archive::{ArchivePointer, ArchiveState, VerifiedArchive},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn publish_archive_pointer<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        verified: &VerifiedArchive,
    ) -> StorageResult<()> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let result = self
            .publish_verified_original(operation.as_mut(), verified)
            .await;
        operation.complete(result)
    }

    async fn publish_verified_original<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        verified: &VerifiedArchive,
    ) -> StorageResult<()> {
        let archive = self
            .archive
            .get()
            .filter(|archive| archive.identity() == verified.archive_identity())
            .ok_or_else(corrupt_record)?;
        self.bind_archive(attempt, archive.physical_identity())
            .await?;
        let hold = verified.hold();
        self.require_archive_hold(hold)?;
        let family = self
            .find_archive_state(attempt, hold.cursor.kind, ReadProtection::Conflict)
            .await?
            .ok_or_else(corrupt_record)?;
        let existing = self
            .find_archive_pointer(attempt, &hold.pointer.key, ReadProtection::Conflict)
            .await?;
        if !matches!(&family.state, ArchiveState::Pending(pending) if pending == hold) {
            return if existing.as_ref() == Some(&hold.pointer) {
                Ok(())
            } else {
                Err(corrupt_record())
            };
        }
        let changes = self
            .prepare_original_handoff(attempt, &hold.pointer, existing.as_ref())
            .await?;
        self.complete_archive_hold(attempt, family, hold, changes)
            .await
    }

    async fn prepare_original_handoff<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        pointer: &ArchivePointer,
        existing: Option<&ArchivePointer>,
    ) -> StorageResult<Vec<RecordChange>> {
        if existing.is_some_and(|existing| existing != pointer) {
            return Err(corrupt_record());
        }
        let record = self
            .records
            .load(
                attempt,
                std::slice::from_ref(&pointer.key),
                ReadProtection::Conflict,
            )
            .await?
            .pop()
            .flatten();
        if record.is_none() && existing.is_none() {
            return Err(corrupt_record());
        }
        let mut changes = Vec::new();
        if existing.is_none() {
            changes.push(self.records.prepare(
                self.archive_pointer_key(&pointer.key)?,
                None,
                pointer,
            )?);
        }
        if let Some(record) = record {
            pointer.require_record(&record)?;
            changes.push(RecordChange::Delete {
                key: record.key,
                expected: record.revision,
            });
        }
        Ok(changes)
    }

    async fn complete_archive_hold<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        observed: super::scan::ArchiveFamilyObservation,
        hold: &crate::archive::ArchiveHold,
        mut changes: Vec<infrastructure_provider::record::RecordChange>,
    ) -> StorageResult<()> {
        let super::scan::ArchiveFamilyObservation {
            row: root,
            cursor_row,
            cursor,
            index,
            ..
        } = observed;
        let index = index.ok_or_else(corrupt_record)?;
        if cursor != hold.cursor {
            return Err(corrupt_record());
        }
        changes.push(self.prepare_archive_scan_advance((cursor_row, cursor), &hold.pointer.key)?);
        let next = ArchiveState::Scanning(hold.cursor.kind);
        changes.push(self.records.prepare(root.key, Some(root.revision), &next)?);
        changes.push(infrastructure_provider::record::RecordChange::Delete {
            key: index.key,
            expected: index.revision,
        });
        self.retain_journal_epoch(attempt).await?;
        self.records.store(attempt, &changes).await
    }
}
