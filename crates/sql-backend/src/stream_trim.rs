//! Bounded ordered SystemStream record scans on the caller's SQL attempt.

use std::future::Future;

use storage_provider::CATALOG_READ_BYTES;
use storage_types::{
    StorageEnum, StorageError, StorageResult, SystemStreamPosition, TimestampMillis, storage_serde,
};
use stream_provider::{
    MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS, SystemStreamTrimAttempt, SystemStreamTrimOperationId,
    SystemStreamTrimPageReceipt, SystemStreamTrimPageRequest, SystemStreamTrimRecord,
};

use crate::{
    ReadProtection, SqlAttempt, SqlLayout, SqlStatement, SqlValue, catalog::SqlCatalogAttempt,
};

impl<A: SqlAttempt, L: SqlLayout> SystemStreamTrimAttempt for SqlCatalogAttempt<'_, A, L> {
    fn read_trim_receipts(
        &mut self,
        request: &SystemStreamTrimPageRequest,
    ) -> impl Future<
        Output = StorageResult<(
            Option<SystemStreamTrimPageReceipt>,
            Option<SystemStreamTrimPageReceipt>,
        )>,
    > + Send {
        read_receipts(self.attempt, self.engine, request)
    }

    fn scan_trim_records(
        &mut self,
        after: Option<&SystemStreamPosition>,
    ) -> impl Future<Output = StorageResult<(Vec<SystemStreamTrimRecord>, bool)>> + Send {
        scan_records(self.attempt, self.engine, after)
    }

    fn apply_trim_page(
        &mut self,
        selected: &[SystemStreamTrimRecord],
        receipt: &SystemStreamTrimPageReceipt,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        apply_page(self.attempt, self.engine, selected, receipt)
    }
}

pub(crate) async fn initialize_trim_receipts<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    attempt
        .execute(SqlStatement::new(
            format!(
                "CREATE TABLE IF NOT EXISTS {relation_sys_system_stream_trim_receipts} \
                 (operation_key {} NOT NULL, input_sequence {} NOT NULL, receipt_blob {} NOT \
                 NULL, PRIMARY KEY (operation_key, input_sequence)){}",
                engine.engine().blob_type(),
                engine.engine().integer_type(),
                engine.engine().blob_type(),
                engine.engine().table_suffix(),
                relation_sys_system_stream_trim_receipts =
                    engine.relation_name("sys_system_stream_trim_receipts"),
            ),
            vec![],
        ))
        .await?;
    Ok(())
}

pub(crate) async fn read_receipts<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    request: &SystemStreamTrimPageRequest,
) -> StorageResult<(
    Option<SystemStreamTrimPageReceipt>,
    Option<SystemStreamTrimPageReceipt>,
)> {
    let sequence = request.continuation().map_or(0, |cursor| cursor.sequence());
    let mut statements = vec![receipt_read_statement(
        engine,
        request.operation_id(),
        sequence,
    )?];
    if let Some(previous) = sequence.checked_sub(1) {
        statements.push(receipt_read_statement(
            engine,
            request.operation_id(),
            previous,
        )?);
    }
    let pages = attempt
        .query_batch(statements, ReadProtection::Conflict, CATALOG_READ_BYTES)
        .await?;
    if pages.len() != if sequence == 0 { 1 } else { 2 } {
        return Err(invalid_receipt());
    }
    let mut pages = pages.into_iter();
    let current = decode_receipt(pages.next().ok_or_else(invalid_receipt)?, request, sequence)?;
    let predecessor = if sequence == 0 {
        None
    } else {
        decode_receipt(
            pages.next().ok_or_else(invalid_receipt)?,
            request,
            sequence - 1,
        )?
    };
    Ok((current, predecessor))
}

fn receipt_insert_statement(
    engine: impl SqlLayout,
    receipt: &SystemStreamTrimPageReceipt,
) -> StorageResult<SqlStatement> {
    let sequence = i64::try_from(receipt.input_sequence).map_err(|_| invalid_receipt())?;
    Ok(SqlStatement::new(
        format!(
            "INSERT INTO {relation_sys_system_stream_trim_receipts} (operation_key, \
             input_sequence, receipt_blob) VALUES ({}) ON CONFLICT DO NOTHING",
            engine.engine().placeholders(3),
            relation_sys_system_stream_trim_receipts =
                engine.relation_name("sys_system_stream_trim_receipts"),
        ),
        vec![
            SqlValue::Blob(storage_serde::to_bytes(&receipt.operation_id)?),
            SqlValue::Integer(sequence),
            SqlValue::Blob(storage_serde::to_bytes(receipt)?),
        ],
    ))
}

fn receipt_read_statement(
    engine: impl SqlLayout,
    operation: &SystemStreamTrimOperationId,
    sequence: u64,
) -> StorageResult<SqlStatement> {
    let sequence = i64::try_from(sequence).map_err(|_| invalid_receipt())?;
    Ok(SqlStatement::new(
        format!(
            "SELECT receipt_blob FROM {relation_sys_system_stream_trim_receipts} WHERE \
             operation_key = {} AND input_sequence = {}",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            relation_sys_system_stream_trim_receipts =
                engine.relation_name("sys_system_stream_trim_receipts"),
        ),
        vec![
            SqlValue::Blob(storage_serde::to_bytes(operation)?),
            SqlValue::Integer(sequence),
        ],
    ))
}

fn decode_receipt(
    page: crate::SqlQueryPage,
    request: &SystemStreamTrimPageRequest,
    sequence: u64,
) -> StorageResult<Option<SystemStreamTrimPageReceipt>> {
    if page.more || page.rows.len() > 1 {
        return Err(invalid_receipt());
    }
    page.rows
        .into_iter()
        .next()
        .map(|row| {
            let [SqlValue::Blob(bytes)]: [SqlValue; 1] = row
                .into_values()
                .try_into()
                .map_err(|_| invalid_receipt())?
            else {
                return Err(invalid_receipt());
            };
            let receipt: SystemStreamTrimPageReceipt = storage_serde::from_bytes(&bytes)?;
            if receipt.operation_id != *request.operation_id() || receipt.input_sequence != sequence
            {
                return Err(invalid_receipt());
            }
            Ok(receipt)
        })
        .transpose()
}

fn invalid_receipt() -> StorageError {
    StorageError::internal("SQL SystemStream trim receipt is invalid")
}

pub(crate) async fn scan_records<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    after: Option<&SystemStreamPosition>,
) -> StorageResult<(Vec<SystemStreamTrimRecord>, bool)> {
    let page = attempt
        .query(
            crate::stream_read::scan_trim_records(engine, after)?,
            ReadProtection::Conflict,
            CATALOG_READ_BYTES,
        )
        .await?;
    let more = page.more || page.rows.len() > MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS;
    let records = page
        .rows
        .into_iter()
        .take(MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS)
        .map(decode_trim_record)
        .collect::<StorageResult<Vec<_>>>()?;
    Ok((records, more))
}

fn decode_trim_record(row: crate::SqlRow) -> StorageResult<SystemStreamTrimRecord> {
    let [shard, lineage, order, offset, history, created]: [SqlValue; 6] =
        row.into_values().try_into().map_err(|_| invalid_record())?;
    let (
        SqlValue::Text(shard),
        SqlValue::Text(lineage),
        SqlValue::Blob(order),
        SqlValue::Text(offset),
        SqlValue::Integer(created),
    ) = (shard, lineage, order, offset, created)
    else {
        return Err(invalid_record());
    };
    let position =
        SystemStreamPosition::try_new(shard, lineage, offset).map_err(|_| invalid_record())?;
    if order.as_slice()
        != position
            .offset()
            .order()
            .map_err(|_| invalid_record())?
            .to_be_bytes()
    {
        return Err(invalid_record());
    }
    let table_history_id = match history {
        SqlValue::Null => None,
        SqlValue::Blob(bytes) => Some(storage_types::TableHistoryId::from(
            <[u8; 16]>::try_from(bytes).map_err(|_| invalid_record())?,
        )),
        _ => return Err(invalid_record()),
    };
    Ok(SystemStreamTrimRecord {
        position,
        created_at: TimestampMillis::from_timestamp(created),
        table_history_id,
    })
}

fn invalid_record() -> StorageError {
    StorageError::internal("SQL SystemStream trim record is invalid")
}

async fn apply_page<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    selected: &[SystemStreamTrimRecord],
    receipt: &SystemStreamTrimPageReceipt,
) -> StorageResult<()> {
    let mut statements = selected
        .iter()
        .map(|record| delete_record_statement(engine, &record.position))
        .collect::<StorageResult<Vec<_>>>()?;
    statements.push(receipt_insert_statement(engine, receipt)?);
    let counts = attempt.execute_batch(statements).await?;
    if counts.len() != selected.len() + 1 || counts.into_iter().any(|count| count != 1) {
        return Err(StorageEnum::TransactionConflict {
            message: "SystemStream trim changed while deleting its page".to_owned(),
        }
        .into());
    }
    crate::stream_allocation::store_retired_offsets(attempt, engine, selected).await
}

fn delete_record_statement(
    engine: impl SqlLayout,
    position: &SystemStreamPosition,
) -> StorageResult<SqlStatement> {
    Ok(SqlStatement::new(
        format!(
            "DELETE FROM {} WHERE shard_id = {} AND lineage_id = {} AND offset_order = {} AND \
             offset_text = {}",
            engine.relation_name("sys_dynamo_stream_records"),
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            engine.engine().placeholder(3),
            engine.engine().placeholder(4)
        ),
        vec![
            SqlValue::Text(position.shard_id().as_str().to_owned()),
            SqlValue::Text(position.lineage_id().as_str().to_owned()),
            SqlValue::Blob(
                position
                    .offset()
                    .order()
                    .map_err(|_| invalid_record())?
                    .to_be_bytes()
                    .to_vec(),
            ),
            SqlValue::Text(position.offset().as_str().to_owned()),
        ],
    ))
}
