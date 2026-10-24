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

pub(super) fn write_receipt(engine: impl SqlLayout, token: &str, receipt: Vec<u8>) -> SqlStatement {
    SqlStatement::new(
        format!(
            "INSERT INTO {relation_sys_transact_receipts} (token, receipt_blob) VALUES ({}, {}) \
             ON CONFLICT(token) DO UPDATE SET receipt_blob = excluded.receipt_blob",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            relation_sys_transact_receipts = engine.relation_name("sys_transact_receipts"),
        ),
        vec![SqlValue::Text(token.to_owned()), SqlValue::Blob(receipt)],
    )
}
