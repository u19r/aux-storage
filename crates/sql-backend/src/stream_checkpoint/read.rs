//! SQL checkpoint lookup within the caller's coherent native attempt.

use storage_types::{
    ReadProtection, StorageError, StorageResult, SystemStreamCheckpointToken, SystemStreamPosition,
    storage_serde,
};
use stream_provider::{
    SystemStreamCheckpointLookup, SystemStreamCheckpointReceipt, SystemStreamHistory,
};

use super::sql_statements;
use crate::{
    SqlAttempt, SqlLayout, SqlQueryPage, SqlStatement, SqlValue,
    constants::SYSTEM_STREAM_PAGE_READ_BYTES, stream_history,
};

/// Pipeline the independent history and token reads on one native attempt.
pub(crate) async fn read_page_start<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    token: Option<&SystemStreamCheckpointToken>,
) -> StorageResult<(SystemStreamHistory, Option<SystemStreamCheckpointReceipt>)> {
    let mut statements = vec![(
        stream_history::history_statement(engine, crate::stream_history::HistoryRead::Reader),
        ReadProtection::Snapshot,
    )];
    if let Some(token) = token {
        statements.push((
            sql_statements::by_token(engine, token.as_str()),
            ReadProtection::Conflict,
        ));
    }
    let mut pages = attempt
        .query_batch_protected(statements, SYSTEM_STREAM_PAGE_READ_BYTES)
        .await?
        .into_iter();
    let history = stream_history::decode_history_page(
        pages.next().ok_or_else(|| {
            StorageError::internal("SystemStream history batch omitted its catalog")
        })?,
        engine,
    )?;
    let receipt = if token.is_some() {
        decode_receipt_page(pages.next().ok_or_else(|| {
            StorageError::internal("SystemStream history batch omitted its checkpoint")
        })?)?
    } else {
        None
    };
    if pages.next().is_some() {
        return Err(StorageError::internal(
            "SystemStream history batch returned extra pages",
        ));
    }
    Ok((history, receipt))
}

pub(crate) async fn read_receipt<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    lookup: SystemStreamCheckpointLookup<'_>,
) -> StorageResult<Option<SystemStreamCheckpointReceipt>> {
    let statement = match lookup {
        SystemStreamCheckpointLookup::Token(token) => {
            sql_statements::by_token(engine, token.as_str())
        }
        SystemStreamCheckpointLookup::Operation(key) => sql_statements::by_operation(engine, key),
    };
    load_receipt(attempt, statement).await
}

pub(crate) fn decode_protected_frontiers(
    page: SqlQueryPage,
) -> StorageResult<Vec<SystemStreamPosition>> {
    if page.more || page.rows.len() > storage_types::SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES {
        return Err(StorageError::internal(
            "SQL SystemStream frontier exceeds its bound",
        ));
    }
    page.rows
        .into_iter()
        .map(|row| {
            let [shard, lineage, order, offset]: [SqlValue; 4] = row
                .into_values()
                .try_into()
                .map_err(|_| invalid_frontier())?;
            let (
                SqlValue::Text(shard),
                SqlValue::Text(lineage),
                SqlValue::Blob(order),
                SqlValue::Text(offset),
            ) = (shard, lineage, order, offset)
            else {
                return Err(invalid_frontier());
            };
            let order: [u8; 8] = order.try_into().map_err(|_| invalid_frontier())?;
            let position = SystemStreamPosition::try_new(shard, lineage, offset)
                .map_err(|_| invalid_frontier())?;
            if position.offset().order().map_err(|_| invalid_frontier())?
                != u64::from_be_bytes(order)
            {
                return Err(invalid_frontier());
            }
            Ok(position)
        })
        .collect()
}

fn invalid_frontier() -> StorageError {
    StorageError::internal("SQL SystemStream protected frontier is invalid")
}

async fn load_receipt<A: SqlAttempt>(
    attempt: &mut A,
    statement: SqlStatement,
) -> StorageResult<Option<SystemStreamCheckpointReceipt>> {
    let page = attempt
        .query(
            statement,
            ReadProtection::Conflict,
            SYSTEM_STREAM_PAGE_READ_BYTES,
        )
        .await?;
    decode_receipt_page(page)
}

fn decode_receipt_page(page: SqlQueryPage) -> StorageResult<Option<SystemStreamCheckpointReceipt>> {
    if page.more || page.rows.len() > 1 {
        return Err(StorageError::internal(
            "SystemStream checkpoint lookup is not unique",
        ));
    }
    let Some(row) = page.rows.into_iter().next() else {
        return Ok(None);
    };
    let values: [SqlValue; 1] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("SystemStream checkpoint row is invalid"))?;
    let [SqlValue::Blob(bytes)] = values else {
        return Err(StorageError::internal(
            "SystemStream checkpoint value is invalid",
        ));
    };
    let receipt: SystemStreamCheckpointReceipt = storage_serde::from_bytes(&bytes)?;
    Ok(Some(receipt))
}
