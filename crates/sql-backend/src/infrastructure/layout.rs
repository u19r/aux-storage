//! Ordinary retained images and explicit record attachment share one physical
//! pool.
use std::num::NonZeroUsize;

use storage_types::{
    InfrastructureContext, InfrastructureResultKind, ReadProtection, StorageError, StorageResult,
};

use super::sql_statements;
use crate::{SqlAttempt, SqlEngine, SqlValue};

pub(crate) async fn initialize_layout<A: SqlAttempt>(
    attempt: &mut A,
    engine: SqlEngine,
) -> StorageResult<[u8; 16]> {
    attempt
        .execute(sql_statements::render_format_schema(engine))
        .await?;
    if let Some(identity) = load_identity(attempt, engine).await? {
        // An established marker cannot authorize recreating missing record
        // data.
        attempt
            .query(
                sql_statements::existing_record(),
                ReadProtection::Conflict,
                NonZeroUsize::MIN,
            )
            .await?;
        return Ok(identity);
    }
    initialize_empty_layout(attempt, engine).await
}

async fn initialize_empty_layout<A: SqlAttempt>(
    attempt: &mut A,
    engine: SqlEngine,
) -> StorageResult<[u8; 16]> {
    attempt
        .execute(sql_statements::render_record_schema(engine))
        .await?;
    let page = attempt
        .query(
            sql_statements::existing_record(),
            ReadProtection::Conflict,
            NonZeroUsize::MIN,
        )
        .await?;
    if !page.rows.is_empty() || page.more {
        return Err(create_layout_error());
    }
    let identity = uuid::Uuid::new_v4().into_bytes();
    let affected = attempt
        .execute(sql_statements::initialize_version(engine, &identity))
        .await?;
    if affected > 1 {
        return Err(create_layout_error());
    }
    load_identity(attempt, engine)
        .await?
        .ok_or_else(create_layout_error)
}

async fn load_identity<A: SqlAttempt>(
    attempt: &mut A,
    engine: SqlEngine,
) -> StorageResult<Option<[u8; 16]>> {
    let mut page = attempt
        .query(
            sql_statements::load_version(engine),
            ReadProtection::Conflict,
            NonZeroUsize::MIN
                .saturating_add(crate::constants::INFRASTRUCTURE_FORMAT_PAYLOAD_BYTES - 1),
        )
        .await?;
    if page.rows.is_empty() && !page.more {
        return Ok(None);
    }
    if page.more || page.rows.len() != 1 {
        return Err(create_layout_error());
    }
    let values = page.rows.remove(0).into_values();
    let [
        SqlValue::Integer(1),
        SqlValue::Integer(crate::constants::INFRASTRUCTURE_LAYOUT_VERSION),
        SqlValue::Blob(identity),
    ] = values.as_slice()
    else {
        return Err(create_layout_error());
    };
    decode_identity(identity).map(Some)
}

fn decode_identity(bytes: &[u8]) -> StorageResult<[u8; 16]> {
    let identity = bytes.try_into().map_err(|_| create_layout_error())?;
    if identity == [0; 16] {
        return Err(create_layout_error());
    }
    Ok(identity)
}

fn create_layout_error() -> StorageError {
    StorageError::from(InfrastructureResultKind::Encoding {
        context: InfrastructureContext::PhysicalLayout,
    })
}
