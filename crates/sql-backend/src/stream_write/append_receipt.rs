//! SQL response receipts join the native append transaction.

use storage_types::{ReadProtection, StorageError, StorageResult, storage_serde};
use stream_provider::{StreamAppendReceipt, TableAppendReceiptKey};

use crate::{ColumnIndex, SqlAttempt, SqlLayout, SqlValue};

pub(crate) async fn load_append_receipt<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    key: TableAppendReceiptKey,
) -> StorageResult<Option<StreamAppendReceipt>> {
    let statement = super::sql_statements::read_append_receipt(engine, key);
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
    receipt.require_identity(key)?;
    receipt.response.resolve(None)?;
    Ok(Some(receipt))
}

pub(crate) async fn store_append_receipt<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    receipt: &StreamAppendReceipt,
) -> StorageResult<()> {
    receipt.require_identity(receipt.into())?;
    receipt.response.resolve(None)?;
    if load_append_receipt(attempt, engine, ReadProtection::Conflict, receipt.into())
        .await?
        .is_some()
    {
        return Err(StorageError::internal("Append receipt already exists"));
    }
    attempt
        .execute(super::sql_statements::store_append_receipt(
            engine, receipt,
        )?)
        .await?;
    Ok(())
}
