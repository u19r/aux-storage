//! Concern-owned SQL for opaque transaction token receipts.

use crate::{SqlLayout, SqlStatement, SqlValue};

pub(super) fn create_receipts(engine: impl SqlLayout) -> SqlStatement {
    SqlStatement::new(
        format!(
            "CREATE TABLE IF NOT EXISTS {relation_sys_transact_receipts} (token {} PRIMARY KEY, \
             receipt_blob {} NOT NULL){}",
            engine.engine().text_type(),
            engine.engine().blob_type(),
            engine.engine().table_suffix(),
            relation_sys_transact_receipts = engine.relation_name("sys_transact_receipts"),
        ),
        Vec::new(),
    )
}

pub(super) fn read_receipt(engine: impl SqlLayout, token: &str) -> SqlStatement {
    SqlStatement::new(
        format!(
            "SELECT receipt_blob FROM {relation_sys_transact_receipts} WHERE token = {}",
            engine.engine().placeholder(1),
            relation_sys_transact_receipts = engine.relation_name("sys_transact_receipts"),
        ),
        vec![SqlValue::Text(token.to_owned())],
    )
}

pub(super) fn scan_receipts(
    engine: impl SqlLayout,
    after: Option<&str>,
    limit: u32,
) -> SqlStatement {
    SqlStatement::new(
        format!(
            "SELECT token, receipt_blob FROM {relation_sys_transact_receipts} WHERE token > {} \
             ORDER BY token LIMIT {limit}",
            engine.engine().placeholder(1),
            relation_sys_transact_receipts = engine.relation_name("sys_transact_receipts"),
        ),
        vec![SqlValue::Text(after.unwrap_or_default().to_owned())],
    )
}

/// Delete only while the bytes still equal the scanned receipt, so a receipt
/// rewritten after the scan survives.
pub(super) fn delete_receipt(engine: impl SqlLayout, token: &str, receipt: &[u8]) -> SqlStatement {
    SqlStatement::new(
        format!(
            "DELETE FROM {relation_sys_transact_receipts} WHERE token = {} AND receipt_blob = {}",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            relation_sys_transact_receipts = engine.relation_name("sys_transact_receipts"),
        ),
        vec![
            SqlValue::Text(token.to_owned()),
            SqlValue::Blob(receipt.to_vec()),
        ],
    )
}

pub(super) fn write_receipt(engine: impl SqlLayout, token: &str, receipt: Vec<u8>) -> SqlStatement {
    SqlStatement::new(
        receipt_sql(engine),
        vec![SqlValue::Text(token.to_owned()), SqlValue::Blob(receipt)],
    )
}

pub(crate) fn counted_receipt(
    engine: impl SqlLayout,
    token: &str,
    receipt: Vec<u8>,
) -> storage_types::StorageResult<crate::SqlCountedWrite> {
    let mut sql = crate::statement::SqlText::start_counted_write()?;
    sql.append(format_args!("{}", receipt_sql(engine)))?;
    sql.finish_counted_write(
        vec![SqlValue::Text(token.to_owned()), SqlValue::Blob(receipt)],
        crate::catalog::SqlDynamoCount::ExactlyOne,
        super::transact_write::receipt_count_error,
    )
}

fn receipt_sql(engine: impl SqlLayout) -> String {
    format!(
        "INSERT INTO {relation_sys_transact_receipts} (token, receipt_blob) VALUES ({}, {}) ON \
         CONFLICT(token) DO UPDATE SET receipt_blob = excluded.receipt_blob",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        relation_sys_transact_receipts = engine.relation_name("sys_transact_receipts"),
    )
}
