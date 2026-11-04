//! Stream indexes carry history facts and byte admission, never image payloads.

use std::num::NonZeroU32;

use serde::{Deserialize, Deserializer, Serialize};

use crate::{
    ItemHistoryMetadata, ItemHistoryPublication, ItemHistoryRecord, LogicalItemSize, StorageError,
    StorageResult,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ItemHistoryPointer {
    metadata: ItemHistoryMetadata,
    record_bytes: NonZeroU32,
    predecessor_payload_bytes: Option<NonZeroU32>,
}

impl ItemHistoryPointer {
    pub fn try_new(
        metadata: ItemHistoryMetadata,
        record_bytes: usize,
        predecessor_payload_bytes: Option<usize>,
    ) -> StorageResult<Self> {
        if !matches!(metadata.publication(), ItemHistoryPublication::Event { .. })
            || metadata.previous_version().is_some() != predecessor_payload_bytes.is_some()
            || record_bytes <= crate::constants::ITEM_HISTORY_RECORD_HEADER_BYTES
            || record_bytes > ItemHistoryRecord::encoded_length_bound()
        {
            return Err(invalid_pointer());
        }
        let predecessor_payload_bytes = predecessor_payload_bytes
            .map(|bytes| {
                if bytes > LogicalItemSize::maximum_json_projection_bytes() {
                    return Err(invalid_pointer());
                }
                require_nonzero_bytes(bytes)
            })
            .transpose()?;
        Ok(Self {
            metadata,
            record_bytes: require_nonzero_bytes(record_bytes)?,
            predecessor_payload_bytes,
        })
    }

    pub fn metadata(&self) -> &ItemHistoryMetadata {
        &self.metadata
    }

    pub fn record_bytes(&self) -> usize {
        self.record_bytes.get() as usize
    }

    /// The protected current value preserves the predecessor's exact raw JSON.
    /// Its history declaration and framing have their own bounded allowance.
    pub fn predecessor_record_bytes_bound(&self) -> Option<usize> {
        self.predecessor_payload_bytes
            .map(|bytes| bytes.get() as usize + ItemHistoryRecord::metadata_length_bound())
    }

    pub fn with_versionstamp(mut self, stamp: [u8; 12]) -> StorageResult<Self> {
        self.metadata = self.metadata.bind_versionstamp(stamp)?;
        Ok(self)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ItemHistoryPointerInput {
    metadata: ItemHistoryMetadata,
    record_bytes: usize,
    predecessor_payload_bytes: Option<usize>,
}

impl<'de> Deserialize<'de> for ItemHistoryPointer {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let input = ItemHistoryPointerInput::deserialize(deserializer)?;
        Self::try_new(
            input.metadata,
            input.record_bytes,
            input.predecessor_payload_bytes,
        )
        .map_err(serde::de::Error::custom)
    }
}

fn require_nonzero_bytes(bytes: usize) -> StorageResult<NonZeroU32> {
    u32::try_from(bytes)
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or_else(invalid_pointer)
}

fn invalid_pointer() -> StorageError {
    StorageError::internal("Invalid item history pointer")
}
