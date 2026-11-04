//! An item-history namespace remains valid after its logical table is replaced.

use serde::{Deserialize, Serialize};

use crate::{StorageError, StorageResult, StreamName, TableHistoryId};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ItemHistoryAddress {
    pub history_id: TableHistoryId,
    pub stream_name: StreamName,
}

impl ItemHistoryAddress {
    /// Map a physical namespace back to its bound logical item address.
    pub fn try_from_physical_stream(
        table: &crate::BoundTableInfo,
        stream: &StreamName,
    ) -> StorageResult<Option<Self>> {
        let prefix = table.history_id.stream_prefix();
        let suffix = stream
            .as_ref()
            .strip_prefix(prefix.as_slice())
            .ok_or_else(|| StorageError::internal("History namespace belongs to another table"))?;
        if !suffix.starts_with(&StreamName::ITEM_STREAM_SEGMENT[1..]) {
            return Ok(None);
        }
        let mut logical = table.table_name.sanitized_name().as_bytes().to_vec();
        logical.push(b'/');
        logical.extend_from_slice(suffix);
        let address = Self {
            history_id: table.history_id,
            stream_name: StreamName::new(&logical),
        };
        address.physical_stream_name()?;
        Ok(Some(address))
    }

    /// Physical storage uses the immutable ID and stream kind. The logical
    /// table name remains a catalog label and never enters the namespace.
    pub fn physical_stream_name(&self) -> StorageResult<StreamName> {
        let logical = self.stream_name.as_ref();
        let separator = logical
            .iter()
            .position(|byte| *byte == b'/')
            .filter(|separator| *separator > 0)
            .ok_or_else(|| StorageError::internal("history address has no stream kind"))?;
        let suffix = &logical[separator..];
        if suffix != StreamName::TABLE_STREAM_SUFFIX
            && !suffix
                .strip_prefix(StreamName::ITEM_STREAM_SEGMENT)
                .is_some_and(|key| !key.is_empty())
        {
            return Err(StorageError::internal(
                "history address has invalid stream kind",
            ));
        }
        let mut bytes = self.history_id.stream_prefix();
        bytes.extend_from_slice(&suffix[1..]);
        Ok(StreamName::new(&bytes))
    }
}
