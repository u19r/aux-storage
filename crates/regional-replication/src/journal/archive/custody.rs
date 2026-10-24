use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey},
};
use storage_types::{ReadProtection, StorageResult};

use crate::{
    archive::{ArchiveHold, ArchiveKind, ArchiveState},
    history::hold_discovery::ReaderHoldObservation,
    journal::{JournalStore, reader_hold::RetentionHoldReference},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(in crate::journal) fn archive_hold_prefix(&self) -> StorageResult<DurableRecordKey> {
        let mut suffix = crate::constants::READER_HOLD_SUFFIX.to_vec();
        suffix.push(6);
        self.record_key(suffix)
    }

    pub(in crate::journal) fn archive_hold_key(
        &self,
        kind: ArchiveKind,
    ) -> StorageResult<DurableRecordKey> {
        let prefix = self.archive_hold_prefix()?;
        self.identity_key(&prefix.as_ref().suffix, &kind)
    }

    pub(super) fn require_archive_state_index(
        &self,
        state: &ArchiveState,
        index: Option<&DurableRecord>,
    ) -> StorageResult<()> {
        match state {
            ArchiveState::Scanning(_) => {}
            ArchiveState::Pending(hold) | ArchiveState::Qualifying { hold, .. } => {
                self.require_archive_hold(hold)?
            }
        }
        match (state, index) {
            (ArchiveState::Scanning(_) | ArchiveState::Qualifying { .. }, None) => Ok(()),
            (ArchiveState::Pending(hold), Some(row))
                if self.decode_retention_hold(row)?
                    == RetentionHoldReference::Archive(hold.clone()) =>
            {
                Ok(())
            }
            _ => Err(corrupt_record()),
        }
    }

    pub(in crate::journal) async fn observe_archive_hold<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        expected: &ArchiveHold,
        protection: ReadProtection,
    ) -> StorageResult<ReaderHoldObservation> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            self.require_archive_hold(expected)?;
            let family = self
                .load_archive_state(attempt, expected.cursor.kind, protection)
                .await?;
            let ArchiveState::Pending(hold) = &family.state else {
                return Err(corrupt_record());
            };
            if hold != expected {
                return Err(corrupt_record());
            }
            Ok(ReaderHoldObservation::ArchivePending {
                generation: self.generation,
                metadata: hold.pointer.manifest,
                content: hold.pointer.content,
            })
        }
        .await;
        settlement_guard.complete(result)
    }
}
