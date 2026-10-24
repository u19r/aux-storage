//! Locked Postgres driver framing qualifies message shapes, not transactions.

use infrastructure_provider::{
    limits::InfrastructureLimits,
    physical_store::{NativeTransactionByteLimit, PhysicalRecordCapacity},
};
use storage_types::{InfrastructureContext, InfrastructureResultKind, StorageResult};

use crate::{SqlEngine, constants, infrastructure::RecordStatementShape};

pub(super) fn qualify_record_protocol(
    limits: InfrastructureLimits,
) -> StorageResult<PhysicalRecordCapacity> {
    let shape = RecordStatementShape::try_new(SqlEngine::Postgres, limits)?;
    let maximum = i32::MAX as usize;
    let protected_sql = shape
        .sql_bytes
        .checked_add(
            super::statements::query_protection_suffix(storage_types::ReadProtection::Conflict)
                .len(),
        )
        .ok_or_else(invalid_shape)?;
    let payload = bound_bind_payload(limits, shape.initialization_bind_bytes)?;
    super::input_limits::require_protocol_shape(protected_sql, shape.parameters, payload)?;
    let row = limits
        .max_key_bytes()
        .get()
        .checked_add(limits.max_record_bytes().get())
        .and_then(|bytes| bytes.checked_add(constants::POSTGRES_RECORD_ROW_FRAME_BYTES))
        .ok_or_else(invalid_shape)?;
    let format =
        constants::INFRASTRUCTURE_FORMAT_PAYLOAD_BYTES + constants::POSTGRES_FORMAT_ROW_FRAME_BYTES;
    if row.max(format) > maximum {
        return Err(invalid_shape());
    }
    // The wire's signed lengths and unsigned parameter counts establish no
    // server allocation, native aggregate, disk, memory or duration allowance.
    let framing = shape
        .parameters
        .checked_mul(
            constants::POSTGRES_PARSE_PARAMETER_BYTES + constants::POSTGRES_BIND_PARAMETER_BYTES,
        )
        .and_then(|bytes| {
            bytes.checked_add(
                constants::POSTGRES_PARSE_FRAME_BYTES + constants::POSTGRES_BIND_FRAME_BYTES + 2,
            )
        })
        .and_then(|bytes| bytes.checked_add(protected_sql.checked_sub(shape.sql_bytes)?))
        .ok_or_else(invalid_shape)?;
    let data = shape.bound_batch_data(
        limits,
        (constants::POSTGRES_RECORD_ROW_FRAME_BYTES + 1, framing),
    )?;
    Ok(PhysicalRecordCapacity::new(
        NativeTransactionByteLimit::Unknown,
        Some(data),
    ))
}

fn bound_bind_payload(
    limits: InfrastructureLimits,
    initialization_bind_bytes: usize,
) -> StorageResult<usize> {
    let key = limits.max_key_bytes().get();
    let count = limits
        .max_records()
        .get()
        .min(constants::INFRASTRUCTURE_POINT_LOAD_KEYS);
    let points = key.checked_mul(count).ok_or_else(invalid_shape)?;
    // Complete lower/upper/continuation keys conservatively cover range binds.
    let scan = key.checked_mul(3).ok_or_else(invalid_shape)?;
    // Logical key bounds include generation and the new revision. An UPDATE
    // additionally binds the old revision, unlike an absent INSERT.
    let store = key
        .checked_add(limits.max_record_bytes().get())
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<u64>()))
        .ok_or_else(invalid_shape)?;
    // Fixed initialization inputs are derived from the rendered statement,
    // independently of logical protected-record budgets.
    Ok(points.max(scan).max(store).max(initialization_bind_bytes))
}

fn invalid_shape() -> storage_types::StorageError {
    InfrastructureResultKind::InvalidBound {
        context: InfrastructureContext::NativeCapacity,
    }
    .into()
}
