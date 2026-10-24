use storage_provider::{
    ItemTombstone, ItemTombstoneScan, ItemWriteObservation, PrimaryRevisionRecord,
};
use storage_types::{BoundTableInfo, ItemRevision, StorageError, StorageResult, storage_serde};

use super::key_layout::decode_key_json;
use crate::KvEntry;

pub(crate) fn decode_deleted(
    table: &BoundTableInfo,
    key_json: &str,
    bytes: &[u8],
) -> StorageResult<ItemWriteObservation> {
    let record = decode_record(table, key_json, bytes)?;
    Ok(ItemWriteObservation::Deleted {
        revision: ItemRevision::try_from(record.revision)?,
        provenance: record.provenance,
    })
}

pub(super) fn decode_scan_entry(
    scan: &ItemTombstoneScan,
    entry: KvEntry,
) -> StorageResult<ItemTombstone> {
    let json = decode_key_json(scan.table(), &entry.key)?;
    let record = decode_record(scan.table(), json, &entry.value)?;
    Ok(ItemTombstone {
        key: scan.decode_key(json)?,
        revision: ItemRevision::try_from(record.revision)?,
        provenance: record.provenance,
    })
}

fn decode_record(
    table: &BoundTableInfo,
    key_json: &str,
    bytes: &[u8],
) -> StorageResult<PrimaryRevisionRecord> {
    let record: PrimaryRevisionRecord = storage_serde::from_bytes(bytes)?;
    if !record.deleted
        || record.table_name != table.info.table_name.as_ref()
        || record.key_json != key_json
    {
        return Err(StorageError::internal("KV tombstone identity is invalid"));
    }
    Ok(record)
}
