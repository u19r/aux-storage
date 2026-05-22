use std::collections::HashMap;

use storage_backfill::{
    LogicalBackfillDomain, LogicalBackfillRecord, LogicalExportPage, LogicalExportRequest,
};
use storage_types::{StorageError, StorageResult};
use turso::Value as TursoValue;

use super::{
    TursoStorageProvider,
    logical_backfill::{
        payload_i64, payload_optional_string, payload_string, row_i64, row_optional_text, row_text,
        unchecked_checksum,
    },
};

pub(super) async fn export_gsi_records(
    provider: &TursoStorageProvider,
    request: LogicalExportRequest,
) -> StorageResult<LogicalExportPage> {
    let conn = provider.connect().await?;
    let limit = i64::from(request.limit);
    let (sql, params) = if let Some(table_name) = request.table_name {
        (
            r"SELECT table_name, index_name, status, scan_lek, captured_stream_tail,
              created_at, updated_at
              FROM gsi_backfill
              WHERE table_name = ?1
              ORDER BY table_name, index_name
              LIMIT ?2",
            vec![TursoValue::Text(table_name), TursoValue::Integer(limit)],
        )
    } else {
        (
            r"SELECT table_name, index_name, status, scan_lek, captured_stream_tail,
              created_at, updated_at
              FROM gsi_backfill
              ORDER BY table_name, index_name
              LIMIT ?1",
            vec![TursoValue::Integer(limit)],
        )
    };
    let rows = provider.query_rows(&conn, sql, params).await?;
    let mut records = Vec::with_capacity(rows.len());
    for row in &rows {
        records.push(turso_gsi_backfill_record_from_row(row)?);
    }
    Ok(LogicalExportPage {
        domain: LogicalBackfillDomain::GsiRecords,
        records,
        next_cursor: None,
        checksum: unchecked_checksum()?,
    })
}

pub(super) async fn import_gsi_record<C>(
    provider: &TursoStorageProvider,
    conn: &C,
    payload_json: &str,
) -> StorageResult<()>
where
    C: super::provider::TursoSqlConnection + ?Sized,
{
    let payload = serde_json::from_str::<serde_json::Value>(payload_json)?;
    match payload_string(&payload, "gsi_record_type")?.as_str() {
        "backfill_state" => import_turso_gsi_backfill_record(provider, conn, &payload).await,
        other => Err(StorageError::validation(format!(
            "unsupported turso logical gsi record type {other}"
        ))),
    }
}

fn turso_gsi_backfill_record_from_row(
    row: &HashMap<String, TursoValue>,
) -> StorageResult<LogicalBackfillRecord> {
    let table_name = row_text(row, "table_name")?.to_string();
    let index_name = row_text(row, "index_name")?.to_string();
    let status = row_text(row, "status")?.to_string();
    let scan_lek = row_optional_text(row, "scan_lek")?;
    let captured_stream_tail = row_optional_text(row, "captured_stream_tail")?;
    let created_at = row_i64(row, "created_at")?;
    let updated_at = row_i64(row, "updated_at")?;
    let payload_json = serde_json::json!({
        "gsi_record_type": "backfill_state",
        "table_name": table_name,
        "index_name": index_name,
        "status": status,
        "scan_lek": scan_lek,
        "captured_stream_tail": captured_stream_tail,
        "created_at": created_at,
        "updated_at": updated_at,
    })
    .to_string();
    Ok(LogicalBackfillRecord::DomainRecord {
        domain: LogicalBackfillDomain::GsiRecords,
        record_key_json: serde_json::json!({
            "gsi_record_type": "backfill_state",
            "table_name": table_name,
            "index_name": index_name,
        })
        .to_string(),
        payload_json,
    })
}

async fn import_turso_gsi_backfill_record<C>(
    provider: &TursoStorageProvider,
    conn: &C,
    payload: &serde_json::Value,
) -> StorageResult<()>
where
    C: super::provider::TursoSqlConnection + ?Sized,
{
    let table_name = payload_string(payload, "table_name")?;
    let index_name = payload_string(payload, "index_name")?;
    let status = payload_string(payload, "status")?;
    let scan_lek = payload_optional_string(payload, "scan_lek")?;
    let captured_stream_tail = payload_optional_string(payload, "captured_stream_tail")?;
    let created_at = payload_i64(payload, "created_at")?;
    let updated_at = payload_i64(payload, "updated_at")?;
    let _ = provider
        .execute(
            conn,
            r"INSERT INTO gsi_backfill (
                table_name, index_name, status, scan_lek, captured_stream_tail, created_at,
                updated_at
              )
              VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
              ON CONFLICT(table_name, index_name)
              DO UPDATE SET
                status = excluded.status,
                scan_lek = excluded.scan_lek,
                captured_stream_tail = excluded.captured_stream_tail,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at",
            vec![
                TursoValue::Text(table_name),
                TursoValue::Text(index_name),
                TursoValue::Text(status),
                scan_lek.map_or(TursoValue::Null, TursoValue::Text),
                captured_stream_tail.map_or(TursoValue::Null, TursoValue::Text),
                TursoValue::Integer(created_at),
                TursoValue::Integer(updated_at),
            ],
        )
        .await?;
    Ok(())
}
