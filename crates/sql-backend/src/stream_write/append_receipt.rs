//! SQL response receipts join the native append transaction.

use storage_types::{ReadProtection, StorageError, StorageResult, storage_serde};
use stream_provider::{StreamAppendOperationId, StreamAppendReceipt};

use crate::{ColumnIndex, SqlAttempt, SqlLayout, SqlStatement, SqlValue};

pub(crate) async fn load_append_receipt<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    operation: StreamAppendOperationId,
) -> StorageResult<Option<StreamAppendReceipt>> {
    let statement = SqlStatement::new(
        format!(
            "SELECT receipt_blob FROM {} WHERE operation_id = {}",
            engine.relation_name("sys_stream_append_receipts"),
            engine.engine().placeholder(1)
        ),
        vec![SqlValue::Blob(operation.bytes().to_vec())],
    );
    let page = attempt
        .query(statement, protection, storage_provider::CATALOG_READ_BYTES)
        .await?;
    if page.more || page.rows.len() > 1 {
        return Err(StorageError::internal("Invalid SQL append receipt lookup"));
    }
    let Some(row) = page.rows.first() else {
        return Ok(None);
    };
    let SqlValue::Blob(bytes) = row.value(ColumnIndex::new(0))? else {
        return Err(StorageError::internal("Invalid SQL append receipt row"));
    };
    let receipt: StreamAppendReceipt = storage_serde::from_bytes(bytes)?;
    receipt.require_identity(operation)?;
    receipt.response.resolve(None)?;
    Ok(Some(receipt))
}

pub(crate) async fn store_append_receipt<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    receipt: &StreamAppendReceipt,
) -> StorageResult<()> {
    receipt.require_identity(receipt.operation)?;
    receipt.response.resolve(None)?;
    if load_append_receipt(attempt, engine, ReadProtection::Conflict, receipt.operation)
        .await?
        .is_some()
    {
        return Err(StorageError::internal("Append receipt already exists"));
    }
    attempt
        .execute(SqlStatement::new(
            format!(
                "INSERT INTO {} (operation_id, receipt_blob) VALUES ({})",
                engine.relation_name("sys_stream_append_receipts"),
                engine.engine().placeholders(2)
            ),
            vec![
                SqlValue::Blob(receipt.operation.bytes().to_vec()),
                SqlValue::Blob(storage_serde::to_bytes(receipt)?),
            ],
        ))
        .await?;
    Ok(())
}
