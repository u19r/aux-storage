use storage_backfill::{
    LogicalBackfillDomain, LogicalBackfillRecord, LogicalExportPage, LogicalExportRequest,
};
use storage_types::{StorageError, StorageResult};

use super::{
    PostgresStorageProvider,
    logical_backfill::{payload_i64, payload_optional_string, payload_string, unchecked_checksum},
};

pub(super) async fn export_gsi_records(
    provider: &PostgresStorageProvider,
    request: LogicalExportRequest,
) -> StorageResult<LogicalExportPage> {
    let client = provider
        .pool
        .get()
        .await
        .map_err(PostgresStorageProvider::map_postgres_client_acquire_error)?;
    let limit = i64::from(request.limit);
    let rows = if let Some(table_name) = request.table_name {
        client
            .query(
                r"SELECT table_name, index_name, status, scan_lek, captured_stream_tail,
                  created_at, updated_at
                  FROM gsi_backfill
                  WHERE table_name = $1
                  ORDER BY table_name, index_name
                  LIMIT $2",
                &[&table_name, &limit],
            )
            .await
    } else {
        client
            .query(
                r"SELECT table_name, index_name, status, scan_lek, captured_stream_tail,
                  created_at, updated_at
                  FROM gsi_backfill
                  ORDER BY table_name, index_name
                  LIMIT $1",
                &[&limit],
            )
            .await
    }
    .map_err(|err| PostgresStorageProvider::map_postgres_error("export gsi backfill", err))?;
    let mut records = Vec::with_capacity(rows.len());
    for row in &rows {
        records.push(postgres_gsi_backfill_record_from_row(row)?);
    }
    Ok(LogicalExportPage {
        domain: LogicalBackfillDomain::GsiRecords,
        records,
        next_cursor: None,
        checksum: unchecked_checksum()?,
    })
}

pub(super) async fn import_gsi_record(
    provider: &PostgresStorageProvider,
    payload_json: &str,
) -> StorageResult<()> {
    let payload = serde_json::from_str::<serde_json::Value>(payload_json)?;
    match payload_string(&payload, "gsi_record_type")?.as_str() {
        "backfill_state" => import_postgres_gsi_backfill_record(provider, &payload).await,
        other => Err(StorageError::validation(format!(
            "unsupported postgres logical gsi record type {other}"
        ))),
    }
}

fn postgres_gsi_backfill_record_from_row(
    row: &tokio_postgres::Row,
) -> StorageResult<LogicalBackfillRecord> {
    let table_name = row
        .try_get::<_, String>(0)
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode table_name", err))?;
    let index_name = row
        .try_get::<_, String>(1)
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode index_name", err))?;
    let status = row
        .try_get::<_, String>(2)
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode status", err))?;
    let scan_lek = row
        .try_get::<_, Option<String>>(3)
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode scan_lek", err))?;
    let captured_stream_tail = row.try_get::<_, Option<String>>(4).map_err(|err| {
        PostgresStorageProvider::map_postgres_error("decode captured_stream_tail", err)
    })?;
    let created_at = row
        .try_get::<_, i64>(5)
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode created_at", err))?;
    let updated_at = row
        .try_get::<_, i64>(6)
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode updated_at", err))?;
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

async fn import_postgres_gsi_backfill_record(
    provider: &PostgresStorageProvider,
    payload: &serde_json::Value,
) -> StorageResult<()> {
    let table_name = payload_string(payload, "table_name")?;
    let index_name = payload_string(payload, "index_name")?;
    let status = payload_string(payload, "status")?;
    let scan_lek = payload_optional_string(payload, "scan_lek")?;
    let captured_stream_tail = payload_optional_string(payload, "captured_stream_tail")?;
    let created_at = payload_i64(payload, "created_at")?;
    let updated_at = payload_i64(payload, "updated_at")?;
    let client = provider
        .pool
        .get()
        .await
        .map_err(PostgresStorageProvider::map_postgres_client_acquire_error)?;
    client
        .execute(
            r"INSERT INTO gsi_backfill (
                table_name, index_name, status, scan_lek, captured_stream_tail, created_at,
                updated_at
              )
              VALUES ($1, $2, $3, $4, $5, $6, $7)
              ON CONFLICT(table_name, index_name)
              DO UPDATE SET
                status = excluded.status,
                scan_lek = excluded.scan_lek,
                captured_stream_tail = excluded.captured_stream_tail,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at",
            &[
                &table_name,
                &index_name,
                &status,
                &scan_lek,
                &captured_stream_tail,
                &created_at,
                &updated_at,
            ],
        )
        .await
        .map_err(|err| {
            PostgresStorageProvider::map_postgres_write_error("import gsi backfill", err)
        })?;
    Ok(())
}
