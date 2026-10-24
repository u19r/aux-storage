//! Locked local record serialization supplies component data, not native fit.

use infrastructure_provider::{
    limits::InfrastructureLimits,
    physical_store::{NativeTransactionByteLimit, PhysicalRecordCapacity},
};
use storage_types::StorageResult;

use crate::{SqlEngine, infrastructure::RecordStatementShape};

pub(super) fn qualify_record_format(
    limits: InfrastructureLimits,
) -> StorageResult<PhysicalRecordCapacity> {
    let shape = RecordStatementShape::try_new(SqlEngine::Turso, limits)?;
    // Turso 0.8.1 Record::build writes the SQLite record header: one length
    // varint and one serial-type varint per field, each at most nine bytes.
    // The SDK is local; this is serialized row/statement data, not wire framing
    // or the native VM's allocations. No connection limit is inferred.
    let data = shape.bound_batch_data(limits, (crate::constants::SQLITE_RECORD_HEADER_BOUND, 0))?;
    Ok(PhysicalRecordCapacity::new(
        NativeTransactionByteLimit::Unknown,
        Some(data),
    ))
}
