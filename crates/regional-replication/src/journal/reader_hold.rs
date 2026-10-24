//! One typed generation-wide index for both native reader checkpoint families.
use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey},
};
use replication_types::identity::resource::TableLineage;
use serde::{Deserialize, Serialize};
use storage_types::{ReadProtection, StorageResult};

use super::JournalStore;
use crate::{history::checkpoint::CanonicalCheckpointId, native_record::corrupt_record};

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::journal) enum ReaderHoldKind {
    Receiver,
    Source,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::journal) struct ReaderHoldReference {
    pub kind: ReaderHoldKind,
    pub lineage: TableLineage,
    pub operation: CanonicalCheckpointId,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::journal) enum RetentionHoldReference {
    Reader(ReaderHoldReference),
    Reenable(replication_types::identity::resource::ControlOperationId),
    Capture(super::history_hold_index::SourceHoldReference),
    Adoption(replication_types::identity::resource::ControlOperationId),
    Archive(crate::archive::ArchiveHold),
    ImportFragment(super::import_fragment_retention::FragmentHoldReference),
}

impl JournalStore {
    pub(in crate::journal) async fn retain_reader_hold<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        reference: ReaderHoldReference,
        retention: crate::history::retention::CheckpointRetention,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            self.retain_history_epoch(attempt, reference.lineage)
                .await?;
            let key = self.reader_hold_key(&reference)?;
            let mut changes = vec![self.records.prepare(
                key,
                None,
                &RetentionHoldReference::Reader(reference),
            )?];
            if let Some(expiry) = self.prepare_reader_expiry(reference, retention)? {
                changes.push(expiry);
            }
            self.records.store(attempt, &changes).await
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) async fn require_reader_hold<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        reference: ReaderHoldReference,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let key = self.reader_hold_key(&reference)?;
            let row = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Snapshot,
                )
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            if self.decode_retention_hold(&row)? != RetentionHoldReference::Reader(reference) {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) fn reader_hold_key(
        &self,
        reference: &ReaderHoldReference,
    ) -> StorageResult<DurableRecordKey> {
        let mut suffix = crate::constants::READER_HOLD_SUFFIX.to_vec();
        suffix.push(match reference.kind {
            ReaderHoldKind::Receiver => 1,
            ReaderHoldKind::Source => 2,
        });
        suffix.extend_from_slice(uuid::Uuid::from(reference.operation).as_bytes());
        self.record_key(suffix)
    }

    pub(in crate::journal) fn decode_retention_hold(
        &self,
        row: &DurableRecord,
    ) -> StorageResult<RetentionHoldReference> {
        let reference: RetentionHoldReference = self.records.decode(row)?;
        if row.revision != storage_types::Revision::INITIAL
            || row.key != self.retention_hold_key(&reference)?
        {
            return Err(corrupt_record());
        }
        Ok(reference)
    }

    pub(in crate::journal) fn retention_hold_key(
        &self,
        reference: &RetentionHoldReference,
    ) -> StorageResult<DurableRecordKey> {
        match reference {
            RetentionHoldReference::Reader(reference) => self.reader_hold_key(reference),
            RetentionHoldReference::Reenable(operation) => self.reenable_custody_key(*operation),
            RetentionHoldReference::Capture(reference) => self.source_hold_index_key(reference),
            RetentionHoldReference::Adoption(operation) => self.adoption_inventory_key(*operation),
            RetentionHoldReference::ImportFragment(reference) => self.fragment_hold_key(reference),
            RetentionHoldReference::Archive(hold) => {
                self.require_archive_hold(hold)?;
                self.archive_hold_key(hold.cursor.kind)
            }
        }
    }
}
