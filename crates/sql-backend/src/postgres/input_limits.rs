//! Complete locked-driver Parse and Bind framing before native preparation.

use storage_types::{InfrastructureContext, InfrastructureResultKind, StorageError, StorageResult};

use crate::{ReadProtection, SqlStatement, constants};

pub(super) fn require_statement_input(
    statement: &SqlStatement,
    protection: ReadProtection,
) -> StorageResult<()> {
    let shape = statement.input_shape()?;
    let suffix = super::statements::query_protection_suffix(protection).len();
    let sql = shape
        .sql_bytes
        .checked_add(suffix)
        .ok_or_else(invalid_input)?;
    // postgres-types also accepts ltree/lquery/ltxtquery string parameters,
    // whose binary encoding adds one version byte. Preparation selects types.
    let values = shape
        .value_bytes
        .checked_add(shape.text_values)
        .ok_or_else(invalid_input)?;
    require_protocol_shape(sql, shape.parameters, values)
}

pub(super) fn require_protocol_shape(
    sql_bytes: usize,
    parameters: usize,
    value_bytes: usize,
) -> StorageResult<()> {
    if parameters > u16::MAX as usize {
        return Err(InfrastructureResultKind::NativeCapacityRejected.into());
    }
    let parse = parameters
        .checked_mul(constants::POSTGRES_PARSE_PARAMETER_BYTES)
        .and_then(|bytes| bytes.checked_add(sql_bytes))
        .and_then(|bytes| bytes.checked_add(constants::POSTGRES_PARSE_FRAME_BYTES))
        .ok_or_else(invalid_input)?;
    let bind = parameters
        .checked_mul(constants::POSTGRES_BIND_PARAMETER_BYTES)
        .and_then(|bytes| bytes.checked_add(value_bytes))
        .and_then(|bytes| bytes.checked_add(constants::POSTGRES_BIND_FRAME_BYTES))
        .ok_or_else(invalid_input)?;
    if parse.max(bind) > i32::MAX as usize {
        return Err(InfrastructureResultKind::NativeCapacityRejected.into());
    }
    Ok(())
}

fn invalid_input() -> StorageError {
    InfrastructureResultKind::InvalidBound {
        context: InfrastructureContext::NativeCapacity,
    }
    .into()
}
