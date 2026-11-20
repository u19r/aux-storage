//! One accepted mutation owns one self-contained stream entry. Native keys
//! carry its incarnation and position; the value never contains a pending
//! stamp.

use serde::{Deserialize, Serialize};
use storage_types::{
    StorageResult, SystemStreamPosition, SystemStreamScope, TableHistoryId, TableName,
    storage_serde,
};

use crate::StreamEntry;

/// Canonical positioned entry shared by native writers and public readers.
#[derive(Clone)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct PositionedStreamEntry {
    pub history_id: TableHistoryId,
    pub position: SystemStreamPosition,
    pub source_table_name: TableName,
    pub source_scope: SystemStreamScope,
    pub entry: StreamEntry,
}

#[derive(Serialize, Deserialize)]
struct StreamEntryValue {
    source_table_name: TableName,
    source_scope: SystemStreamScope,
    entry: StreamEntry,
}

#[derive(Serialize)]
struct StreamEntryValueRef<'a, E> {
    source_table_name: &'a TableName,
    source_scope: &'a SystemStreamScope,
    entry: &'a E,
}

impl PositionedStreamEntry {
    pub const VALUE_PREFIX_BYTES: usize = 12;

    pub fn decode_created_at(bytes: &[u8]) -> StorageResult<storage_types::TimestampMillis> {
        let prefix = bytes
            .get(..Self::VALUE_PREFIX_BYTES)
            .ok_or_else(invalid_prefix)?;
        if &prefix[..4] != b"SEV1" {
            return Err(invalid_prefix());
        }
        let millis = i64::from_be_bytes(prefix[4..].try_into().map_err(|_| invalid_prefix())?);
        Ok(storage_types::TimestampMillis::from_timestamp(millis))
    }

    pub fn encode_value(&self) -> StorageResult<Vec<u8>> {
        Self::encode_body(&self.source_table_name, &self.source_scope, &self.entry)
    }

    /// Encode the accepted record before its native address is assigned.
    pub fn encode_body(
        source_table_name: &TableName,
        source_scope: &SystemStreamScope,
        entry: &StreamEntry,
    ) -> StorageResult<Vec<u8>> {
        Self::encode_entry(source_table_name, source_scope, entry, entry.created_at())
    }

    pub fn encode_input(
        source_table_name: &TableName,
        source_scope: &SystemStreamScope,
        entry: &crate::StreamEntryInput<'_>,
    ) -> StorageResult<Vec<u8>> {
        Self::encode_entry(source_table_name, source_scope, entry, entry.created_at())
    }

    fn encode_entry<E: Serialize>(
        source_table_name: &TableName,
        source_scope: &SystemStreamScope,
        entry: &E,
        created_at: storage_types::TimestampMillis,
    ) -> StorageResult<Vec<u8>> {
        let mut bytes = storage_serde::to_bytes(&StreamEntryValueRef {
            source_table_name,
            source_scope,
            entry,
        })?;
        let body_len = bytes.len();
        bytes.reserve_exact(Self::VALUE_PREFIX_BYTES);
        bytes.resize(body_len + Self::VALUE_PREFIX_BYTES, 0);
        bytes.copy_within(..body_len, Self::VALUE_PREFIX_BYTES);
        bytes[..4].copy_from_slice(b"SEV1");
        bytes[4..Self::VALUE_PREFIX_BYTES]
            .copy_from_slice(&created_at.timestamp_millis().to_be_bytes());
        Ok(bytes)
    }

    /// Native readers supply the address from the selected key/index row.
    pub fn decode_value(
        history_id: TableHistoryId,
        position: SystemStreamPosition,
        bytes: &[u8],
    ) -> StorageResult<Self> {
        let created_at = Self::decode_created_at(bytes)?;
        let value: StreamEntryValue =
            storage_serde::from_bytes(&bytes[Self::VALUE_PREFIX_BYTES..])?;
        if value.entry.created_at() != created_at {
            return Err(invalid_prefix());
        }
        Ok(Self {
            history_id,
            position,
            source_table_name: value.source_table_name,
            source_scope: value.source_scope,
            entry: value.entry,
        })
    }
}

fn invalid_prefix() -> storage_types::StorageError {
    storage_types::StorageError::internal("Invalid canonical stream entry prefix")
}
