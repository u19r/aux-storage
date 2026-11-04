//! Bounded scans for operation-addressed records with typed physical owners.
use infrastructure_provider::{
    page::RecordPageRequest,
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey, RecordChange},
};
use serde::{Deserialize, Serialize};
use storage_types::{ReadProtection, StorageResult, TableHistoryId};

use super::{JournalStore, causal_stream_record::CausalStreamRecord};
use crate::{constants, native_record::corrupt_record};

#[derive(Clone, Copy, Serialize, Deserialize)]
pub(super) enum TableMetadataFamily {
    CausalStream,
    ReceiverCheckpoint,
    PortableCheckpoint,
    SourceCapture,
    TargetBootstrap,
    HistoryCapture,
    ReenableInventory,
}

impl TableMetadataFamily {
    pub(super) fn next(self) -> Option<Self> {
        match self {
            Self::CausalStream => Some(Self::ReceiverCheckpoint),
            Self::ReceiverCheckpoint => Some(Self::PortableCheckpoint),
            Self::PortableCheckpoint => Some(Self::SourceCapture),
            Self::SourceCapture => Some(Self::TargetBootstrap),
            Self::TargetBootstrap => Some(Self::HistoryCapture),
            Self::HistoryCapture => Some(Self::ReenableInventory),
            Self::ReenableInventory => None,
        }
    }

    fn prefix(self) -> &'static [u8] {
        match self {
            Self::CausalStream => constants::CAUSAL_STREAM_SUFFIX,
            Self::ReceiverCheckpoint => constants::CANONICAL_CHECKPOINT_SUFFIX,
            Self::PortableCheckpoint => constants::PORTABLE_CHECKPOINT_SUFFIX,
            Self::SourceCapture => constants::SOURCE_CAPTURE_SUFFIX,
            Self::TargetBootstrap => constants::TARGET_BOOTSTRAP_SUFFIX,
            Self::HistoryCapture => constants::HISTORY_CAPTURE_SUFFIX,
            Self::ReenableInventory => constants::REENABLE_INVENTORY_SUFFIX,
        }
    }
}

impl JournalStore {
    pub(super) async fn reclaim_table_metadata<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        history: TableHistoryId,
        cursor: (TableMetadataFamily, Option<DurableRecordKey>),
    ) -> StorageResult<Option<DurableRecordKey>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let result = async {
            let page = self
                .scan_reclaim_family(operation.as_mut(), cursor.0.prefix(), cursor.1)
                .await?;
            for row in &page.records {
                match cursor.0 {
                    TableMetadataFamily::CausalStream => {
                        self.reclaim_causal_stream(operation.as_mut(), row, history)
                            .await?
                    }
                    TableMetadataFamily::ReceiverCheckpoint => {
                        self.reclaim_receiver_checkpoint(operation.as_mut(), row, history)
                            .await?
                    }
                    TableMetadataFamily::PortableCheckpoint => {
                        self.reclaim_portable_checkpoint(operation.as_mut(), row, history)
                            .await?
                    }
                    TableMetadataFamily::SourceCapture => {
                        self.reclaim_source_capture(operation.as_mut(), row, history)
                            .await?
                    }
                    TableMetadataFamily::TargetBootstrap => {
                        self.reclaim_hidden_target(operation.as_mut(), row, history)
                            .await?
                    }
                    TableMetadataFamily::HistoryCapture => {
                        self.reclaim_history_capture(operation.as_mut(), row, history)
                            .await?
                    }
                    TableMetadataFamily::ReenableInventory => {
                        self.reclaim_reenable_inventory(operation.as_mut(), row, history)
                            .await?
                    }
                }
            }
            Ok(page.continuation)
        }
        .await;
        operation.complete(result)
    }

    pub(super) async fn scan_reclaim_family<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        prefix: &[u8],
        after: Option<DurableRecordKey>,
    ) -> StorageResult<infrastructure_provider::page::RecordPage> {
        let first = self.record_key(prefix.to_vec())?;
        self.records
            .scan(
                attempt,
                RecordPageRequest {
                    range: self.record_prefix_range(&first)?,
                    after,
                    max_bytes: self.records.limits.max_bytes(),
                    protection: ReadProtection::Conflict,
                },
            )
            .await
    }

    async fn reclaim_causal_stream<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        row: &DurableRecord,
        history: TableHistoryId,
    ) -> StorageResult<()> {
        let record: CausalStreamRecord = self.records.decode(row)?;
        if row.key != self.identity_key(constants::CAUSAL_STREAM_SUFFIX, &record.operation)? {
            return Err(corrupt_record());
        }
        if record.physical != history {
            return Ok(());
        }
        self.records
            .store(
                attempt,
                &[RecordChange::Delete {
                    key: row.key.clone(),
                    expected: row.revision,
                }],
            )
            .await
    }
}
