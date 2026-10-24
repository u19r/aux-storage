use storage_provider::{ItemTombstone, ItemTombstoneScan, ItemWriteObservation};
use storage_types::{ItemProvenance, ItemRevision, StorageError, StorageResult};

use crate::{SqlRow, SqlValue};

pub(crate) fn decode_deleted(row: SqlRow) -> StorageResult<ItemWriteObservation> {
    let [revision, provenance] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("SQL revision observation width is invalid"))?;
    Ok(ItemWriteObservation::Deleted {
        revision: decode_revision(revision)?,
        provenance: decode_provenance(provenance)?,
    })
}

pub(crate) fn decode_revision(value: SqlValue) -> StorageResult<ItemRevision> {
    let SqlValue::Integer(value) = value else {
        return Err(StorageError::internal(
            "SQL stored revision has invalid type",
        ));
    };
    ItemRevision::try_from(value)
}

pub(crate) fn decode_provenance(value: SqlValue) -> StorageResult<ItemProvenance> {
    let SqlValue::Blob(bytes) = value else {
        return Err(StorageError::internal(
            "SQL stored provenance has invalid type",
        ));
    };
    ItemProvenance::decode(&bytes)
}
pub(super) fn decode_scan_row(
    scan: &ItemTombstoneScan,
    row: SqlRow,
) -> StorageResult<ItemTombstone> {
    let [key, revision, provenance] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("SQL tombstone scan width is invalid"))?;
    let SqlValue::Text(key) = key else {
        return Err(StorageError::internal("SQL tombstone key has invalid type"));
    };
    Ok(ItemTombstone {
        key: scan.decode_key(&key)?,
        revision: decode_revision(revision)?,
        provenance: decode_provenance(provenance)?,
    })
}
