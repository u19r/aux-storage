//! Native source keys qualify committed system history references.
use storage_types::{StorageError, StorageResult, SystemHistoryRecord, storage_serde};

pub(crate) fn decode_record(key: &[u8], bytes: &[u8]) -> StorageResult<SystemHistoryRecord> {
    let mut record: SystemHistoryRecord = storage_serde::from_bytes(bytes)?;
    if record.position().offset().versionstamp().is_some() {
        let prefix = super::key_layout::record_prefix(record.position());
        let stamp = key
            .strip_prefix(prefix.as_slice())
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or_else(|| StorageError::internal("Invalid native system history key"))?;
        record = record.with_versionstamp(stamp)?;
    }
    if super::key_layout::record_key(record.position())? != key {
        return Err(StorageError::internal(
            "System history differs from its source key",
        ));
    }
    Ok(record)
}
