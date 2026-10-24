//! Completed exact-content verification carried into protected trim decisions.
use storage_types::{
    ItemStreamVersion, StorageError, StorageResult, SystemStreamEffectDigest, TableHistoryId,
};

use crate::SystemStreamStoredRecord;

pub struct VerifiedSystemStreamRecord {
    record: SystemStreamStoredRecord,
    history: TableHistoryId,
    version: ItemStreamVersion,
    effect: SystemStreamEffectDigest,
}

impl VerifiedSystemStreamRecord {
    pub(super) fn from_verified_content(
        record: SystemStreamStoredRecord,
        history: TableHistoryId,
        version: ItemStreamVersion,
        effect: SystemStreamEffectDigest,
    ) -> Self {
        Self {
            record,
            history,
            version,
            effect,
        }
    }

    pub fn record(&self) -> &SystemStreamStoredRecord {
        &self.record
    }

    pub fn into_record(self) -> SystemStreamStoredRecord {
        self.record
    }

    pub(super) fn require_pointer(&self, pointer: Option<&crate::StreamItem>) -> StorageResult<()> {
        let Some(pointer) = pointer else {
            return Ok(());
        };
        if pointer.data_type != crate::StreamDataType::StreamPointer {
            return Err(StorageError::internal(
                "Verified stream pointer differs from its system record",
            ));
        }
        let pointer: crate::StoredStreamPointer =
            storage_types::storage_serde::from_bytes(&pointer.data)?;
        if pointer.history_address().history_id != self.history
            || pointer.target_item_stream_version() != self.version
        {
            return Err(StorageError::internal(
                "Verified stream pointer differs from its system record",
            ));
        }
        Ok(())
    }

    pub fn require_effect(
        &self,
        history: TableHistoryId,
        version: ItemStreamVersion,
        effect: &SystemStreamEffectDigest,
    ) -> StorageResult<()> {
        if self.history != history || self.version != version || &self.effect != effect {
            return Err(StorageError::internal(
                "Verified stream content differs from its effect receipt",
            ));
        }
        Ok(())
    }
}
