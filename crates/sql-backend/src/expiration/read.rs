//! Protected SQL TTL index scan with a byte bound and stable continuation.

use storage_provider::{
    TtlIndexRead,
    maintenance::{EXPIRATION_SCAN_BYTES, ExpirationIndexLocator},
};
use storage_types::{StorageError, StorageResult, TableHistoryId};

use super::sql_statements;
use crate::{ReadProtection, SqlAttempt, SqlEngine, SqlLayout, SqlValue};

pub(super) async fn read_expiration_index<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    read: &TtlIndexRead,
    history_id: TableHistoryId,
) -> StorageResult<(Vec<ExpirationIndexLocator>, bool)> {
    let page = attempt
        .query(
            sql_statements::expiration_index_page(engine, read, history_id)?,
            if engine.engine() == SqlEngine::Postgres {
                ReadProtection::Snapshot
            } else {
                ReadProtection::Conflict
            },
            EXPIRATION_SCAN_BYTES,
        )
        .await?;
    let more = page.more;
    let mut rows = Vec::new();
    rows.try_reserve_exact(page.rows.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    for row in page.rows {
        let mut values = row.into_values().into_iter();
        let (Some(SqlValue::Integer(seconds)), Some(SqlValue::Text(token))) =
            (values.next(), values.next())
        else {
            return Err(StorageError::internal(
                "SQL expiration index row has invalid types",
            ));
        };
        if values.next().is_some() {
            return Err(StorageError::internal(
                "SQL expiration index row has extra columns",
            ));
        }
        rows.push(ExpirationIndexLocator {
            indexed_expiration_seconds: seconds,
            ttl_item_key: token,
        });
    }
    Ok((rows, more))
}
