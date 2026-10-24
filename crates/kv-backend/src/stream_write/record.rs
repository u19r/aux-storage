//! KV SystemStream row binds its logical record to its physical pointer.

use storage_types::{StorageError, StorageResult, StreamItemId, TimestampMillis, storage_serde};
use stream_provider::retained_image::system_stream::RetainedSystemStreamRecord;

pub(crate) struct KvSystemStreamRecord {
    pub pointer_item_id: StreamItemId,
    pub effect_id: String,
    pub created_at: TimestampMillis,
    pub record: RetainedSystemStreamRecord,
}

impl KvSystemStreamRecord {
    pub(crate) fn encode(&self) -> StorageResult<Vec<u8>> {
        let identity =
            serde_json::to_vec(&(self.pointer_item_id, &self.effect_id, self.created_at))?;
        let record = self
            .record
            .encode(crate::constants::STREAM_READ_BYTES.get())?;
        let length = 8usize
            .checked_add(identity.len())
            .and_then(|bytes| bytes.checked_add(record.len()))
            .ok_or_else(invalid_descriptor)?;
        if length > crate::constants::STREAM_READ_BYTES.get() {
            return Err(invalid_descriptor());
        }
        let mut bytes = Vec::with_capacity(length);
        bytes.extend_from_slice(b"KSR1");
        bytes.extend_from_slice(
            &u32::try_from(identity.len())
                .map_err(|_| invalid_descriptor())?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(&identity);
        bytes.extend_from_slice(&record);
        Ok(bytes)
    }

    pub(crate) fn decode(key: &[u8], bytes: &[u8]) -> StorageResult<Self> {
        if bytes.len() > crate::constants::STREAM_READ_BYTES.get() || !bytes.starts_with(b"KSR1") {
            return Err(invalid_descriptor());
        }
        let identity_length = u32::from_be_bytes(
            bytes
                .get(4..8)
                .ok_or_else(invalid_descriptor)?
                .try_into()
                .map_err(|_| invalid_descriptor())?,
        ) as usize;
        let (identity, record) = bytes
            .get(8..)
            .ok_or_else(invalid_descriptor)?
            .split_at_checked(identity_length)
            .ok_or_else(invalid_descriptor)?;
        let (pointer_item_id, effect_id, created_at) = serde_json::from_slice(identity)?;
        let record =
            RetainedSystemStreamRecord::decode(record, crate::constants::STREAM_READ_BYTES.get())?;
        let mut row = Self {
            pointer_item_id,
            effect_id,
            created_at,
            record,
        };
        if row.record.position.offset().versionstamp().is_some() {
            let prefix = super::key_layout::record_prefix(&row.record.position);
            let stamp: [u8; 12] = key
                .strip_prefix(prefix.as_slice())
                .and_then(|bytes| bytes.try_into().ok())
                .ok_or_else(|| StorageError::internal("Invalid native stream record key"))?;
            row.record.position = row
                .record
                .position
                .with_versionstamp(stamp)
                .map_err(|_| StorageError::internal("Invalid native stream position"))?;
            row.pointer_item_id = StreamItemId::from(stamp);
        }
        if super::key_layout::record_key(&row.record.position)? != key {
            return Err(StorageError::internal(
                "Stream record key differs from its position",
            ));
        }
        Ok(row)
    }
}

pub(crate) fn decode_effect(bytes: &[u8]) -> StorageResult<stream_provider::DynamoStreamEffectRow> {
    if bytes.starts_with(b"\0vs/") {
        let stamp = bytes
            .get(4..16)
            .ok_or_else(|| StorageError::internal("Missing effect commit stamp"))?
            .try_into()
            .map_err(|_| StorageError::internal("Invalid effect commit stamp"))?;
        let row: stream_provider::DynamoStreamEffectRow = storage_serde::from_bytes(&bytes[16..])?;
        row.with_versionstamp(stamp)
    } else {
        storage_serde::from_bytes(bytes)
    }
}

fn invalid_descriptor() -> StorageError {
    StorageError::internal("Invalid native SystemStream descriptor")
}
