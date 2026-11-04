//! Erase table-bound reader roots together with their retention indexes.
use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, RecordChange},
};
use storage_types::{ReadProtection, StorageResult, TableHistoryId};

use super::{
    JournalStore,
    reader_expiry::ReaderExpiryEntry,
    reader_hold::{ReaderHoldKind, ReaderHoldReference},
};
use crate::{
    history::{checkpoint::CanonicalCheckpointState, retention::CheckpointRetention},
    native_record::corrupt_record,
};

struct ReaderRemoval {
    reference: ReaderHoldReference,
    retention: CheckpointRetention,
    state: CanonicalCheckpointState,
}

impl JournalStore {
    pub(super) async fn reclaim_receiver_checkpoint<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        row: &DurableRecord,
        history: TableHistoryId,
    ) -> StorageResult<()> {
        let record = self.decode_receiver_checkpoint(row)?;
        if record.physical != history {
            return Ok(());
        }
        let range = record.scope.retained_partition(record.high)?;
        let removal = ReaderRemoval {
            reference: ReaderHoldReference {
                kind: ReaderHoldKind::Receiver,
                lineage: range.lineage,
                operation: record.operation,
            },
            retention: record.retention,
            state: record.state,
        };
        self.remove_reader_checkpoint(attempt, row, removal).await
    }

    pub(super) async fn reclaim_portable_checkpoint<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        row: &DurableRecord,
        history: TableHistoryId,
    ) -> StorageResult<()> {
        let record = self.decode_portable_record(row)?;
        if record.incarnation.physical != history {
            return Ok(());
        }
        let removal = ReaderRemoval {
            reference: ReaderHoldReference {
                kind: ReaderHoldKind::Source,
                lineage: record.cut.lineage,
                operation: record.operation,
            },
            retention: record.retention,
            state: record.state,
        };
        self.remove_reader_checkpoint(attempt, row, removal).await
    }

    async fn remove_reader_checkpoint<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        row: &DurableRecord,
        removal: ReaderRemoval,
    ) -> StorageResult<()> {
        let change = RecordChange::Delete {
            key: row.key.clone(),
            expected: row.revision,
        };
        if removal.state == CanonicalCheckpointState::Held {
            return self
                .store_terminal_reader(attempt, (removal.reference, removal.retention, change))
                .await;
        }
        let mut keys = vec![self.reader_hold_key(&removal.reference)?];
        if let CheckpointRetention::Until(deadline) = removal.retention {
            keys.push(self.reader_expiry_key(ReaderExpiryEntry {
                reference: removal.reference,
                deadline,
            })?);
        }
        if self
            .records
            .load(attempt, &keys, ReadProtection::Conflict)
            .await?
            .iter()
            .any(Option::is_some)
        {
            return Err(corrupt_record());
        }
        self.records.store(attempt, &[change]).await
    }
}
