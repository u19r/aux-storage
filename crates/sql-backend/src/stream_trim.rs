//! Bounded ordered SystemStream record scans on the caller's SQL attempt.

use storage_provider::CATALOG_READ_BYTES;
use storage_types::{
    MissExpectation, StorageError, StorageResult, SystemStreamPosition, TimestampMillis,
    storage_serde,
};
use stream_provider::{
    MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS, SystemStreamTrimOperationId, SystemStreamTrimPageReceipt,
    SystemStreamTrimRecord,
};

use crate::{ReadProtection, SqlAttempt, SqlLayout, SqlStatement, SqlValue};

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

pub(crate) async fn load_receipts<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    operation: &SystemStreamTrimOperationId,
    sequences: &[u64],
) -> StorageResult<storage_types::Exact<Option<SystemStreamTrimPageReceipt>>> {
    let statements = sequences
        .iter()
        .map(|sequence| receipt_read_statement(engine, operation, *sequence))
        .collect::<StorageResult<Vec<_>>>()?;
    let pages = attempt
        .query_batch_exact(statements, ReadProtection::Conflict, CATALOG_READ_BYTES)
        .await?;
    pages.try_map(decode_receipt)
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

fn decode_receipt(page: crate::SqlQueryPage) -> StorageResult<Option<SystemStreamTrimPageReceipt>> {
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
    starts: &[SystemStreamPosition],
) -> StorageResult<(Vec<SystemStreamTrimRecord>, bool)> {
    let mut records = Vec::new();
    for start in starts {
        let remaining = MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS + 1 - records.len();
        let page = attempt
            .query(
                crate::stream_read::scan_trim_records(engine, start, remaining)?,
                ReadProtection::Conflict,
                CATALOG_READ_BYTES,
            )
            .await?;
        let more = page.more;
        records.extend(
            page.rows
                .into_iter()
                .map(decode_trim_record)
                .collect::<StorageResult<Vec<_>>>()?,
        );
        if more || records.len() > MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS {
            records.truncate(MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS);
            return Ok((records, true));
        }
    }
    Ok((records, false))
}

fn decode_trim_record(row: crate::SqlRow) -> StorageResult<SystemStreamTrimRecord> {
    let [shard, lineage, order, offset, created]: [SqlValue; 5] =
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
    Ok(SystemStreamTrimRecord {
        position,
        created_at: TimestampMillis::from_timestamp(created),
    })
}

fn invalid_record() -> StorageError {
    StorageError::internal("SQL SystemStream trim record is invalid")
}

pub(crate) async fn apply_page<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    frontiers: &[SystemStreamPosition],
    receipt: &SystemStreamTrimPageReceipt,
) -> StorageResult<()> {
    let counts = attempt
        .execute_batch(vec![receipt_insert_statement(engine, receipt)?])
        .await?;
    if counts != vec![1] {
        return Err(StorageError::write_miss(
            "stream_trim_receipt",
            MissExpectation::Absent,
        ));
    }
    crate::stream_allocation::store_retired_offsets(attempt, engine, frontiers).await
}
