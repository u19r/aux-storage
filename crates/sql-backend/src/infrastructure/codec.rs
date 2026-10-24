//! The layout-1 row codec preserves unsigned address and revision ordering.
use std::sync::Arc;

use infrastructure_provider::record::{DurableRecord, DurableRecordKey, RecordAddress};
use storage_types::{
    InfrastructureContext, InfrastructureResultKind, Revision, StorageError, StorageResult,
};

use crate::{SqlRow, SqlValue, row_view::SqlValueRef};

pub(super) fn encode_key(key: &DurableRecordKey) -> StorageResult<[SqlValue; 4]> {
    let address = key.as_ref();
    Ok([
        SqlValue::try_from(SqlValueRef::Text(&address.namespace))?,
        SqlValue::try_from(SqlValueRef::Blob(&address.resource))?,
        SqlValue::try_from(SqlValueRef::Blob(&address.generation.to_be_bytes()))?,
        SqlValue::try_from(SqlValueRef::Blob(&address.suffix))?,
    ])
}

pub(super) fn prepare_key_binds(
    key: &DurableRecordKey,
    count: usize,
) -> StorageResult<Vec<SqlValue>> {
    let mut binds = Vec::new();
    binds
        .try_reserve_exact(count)
        .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
    binds.extend(encode_key(key)?);
    Ok(binds)
}

pub(super) fn encode_revision(revision: Revision) -> StorageResult<SqlValue> {
    SqlValue::try_from(SqlValueRef::Blob(&u64::from(revision).to_be_bytes()))
}

pub(super) fn decode_record(row: SqlRow) -> StorageResult<DurableRecord> {
    let [
        SqlValue::Text(namespace),
        SqlValue::Blob(resource),
        SqlValue::Blob(generation),
        SqlValue::Blob(suffix),
        SqlValue::Blob(revision),
        SqlValue::Blob(bytes),
    ] = <[SqlValue; 6]>::try_from(row.into_values()).map_err(|_| invalid_row())?
    else {
        return Err(invalid_row());
    };
    let generation = u64::from_be_bytes(generation.try_into().map_err(|_| invalid_row())?);
    let revision = u64::from_be_bytes(revision.try_into().map_err(|_| invalid_row())?).into();
    let key = RecordAddress {
        namespace,
        resource,
        generation,
        suffix,
    }
    .try_into()?;
    Ok(DurableRecord {
        key,
        revision,
        bytes: Arc::from(bytes),
    })
}

pub(super) fn decode_revision(row: SqlRow) -> StorageResult<Revision> {
    let [SqlValue::Blob(revision)] =
        <[SqlValue; 1]>::try_from(row.into_values()).map_err(|_| invalid_row())?
    else {
        return Err(invalid_row());
    };
    Ok(u64::from_be_bytes(revision.try_into().map_err(|_| invalid_row())?).into())
}

fn invalid_row() -> StorageError {
    StorageError::from(InfrastructureResultKind::Encoding {
        context: InfrastructureContext::RecordEncoding,
    })
}
