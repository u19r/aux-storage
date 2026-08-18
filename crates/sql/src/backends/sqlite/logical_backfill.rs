use rusqlite::OptionalExtension as _;
use storage_backfill::{
    LogicalBackfillChunk, LogicalBackfillDomain, LogicalBackfillExport, LogicalBackfillImport,
    LogicalBackfillManifest, LogicalBackfillRecord, LogicalBackfillResult, LogicalExportPage,
    LogicalExportRequest, validate_logical_chunk_for_manifest,
};
use storage_provider::StorageProvider;
use storage_types::{ScanTableRequest, StorageError, StorageResult, TableName};

use super::{
    SQLiteStorageProvider,
    logical_backfill_gsi::{append_gsi_backfill_records, append_physical_gsi_records},
    logical_backfill_import::import_logical_records,
    logical_backfill_stream::{
        append_stream_cursor_records, append_stream_format_records, append_stream_item_records,
        append_user_stream_records, resolve_stream_record_filter,
    },
};
use crate::{error_handler::map_sqlite_error, utils::call_sqlite};

const STORAGE_CONTROL_PLANE_TABLE: &str = "sys_storage_replication";

#[async_trait::async_trait]
impl LogicalBackfillExport for SQLiteStorageProvider {
    async fn export_logical_page(
        &self,
        request: LogicalExportRequest,
    ) -> Result<LogicalExportPage, StorageError> {
        match request.domain {
            LogicalBackfillDomain::TableMetadata => self.export_table_metadata(request).await,
            LogicalBackfillDomain::ItemRecords => self.export_item_records(request).await,
            LogicalBackfillDomain::DurableRevisions => self.export_durable_revisions(request).await,
            LogicalBackfillDomain::TtlRecords => self.export_ttl_records(request).await,
            LogicalBackfillDomain::StreamRecords => self.export_stream_records(request).await,
            LogicalBackfillDomain::GsiRecords => self.export_gsi_records(request).await,
            LogicalBackfillDomain::Tombstones | LogicalBackfillDomain::BackgroundJobs => {
                self.export_empty_domain(request).await
            }
            LogicalBackfillDomain::StorageControlPlane => {
                self.export_storage_control_plane_records(request).await
            }
            LogicalBackfillDomain::SyncControlPlane => self.export_empty_domain(request).await,
        }
    }
}

#[async_trait::async_trait]
impl LogicalBackfillImport for SQLiteStorageProvider {
    async fn import_logical_chunk(
        &self,
        manifest: &LogicalBackfillManifest,
        chunk: LogicalBackfillChunk,
    ) -> Result<LogicalBackfillResult, StorageError> {
        validate_logical_chunk_for_manifest(manifest, &chunk).map_err(|error| {
            StorageError::validation(format!("logical chunk rejected: {error}"))
        })?;
        import_logical_records(self, chunk.records).await?;
        Ok(LogicalBackfillResult::ChunkImported)
    }
}

impl SQLiteStorageProvider {
    async fn export_table_metadata(
        &self,
        request: LogicalExportRequest,
    ) -> StorageResult<LogicalExportPage> {
        let table_filter = request.table_name;
        let limit = i64::from(request.limit);
        let records = call_sqlite(&self.connection, move |conn| {
            let mut records = Vec::new();
            if let Some(table_name) = table_filter {
                let mut stmt = conn
                    .prepare(
                        "SELECT id, table_name, table_status, created_at, attribute_definitions, \
                         key_schema, max_indexers, global_secondary_indexes, table_size_bytes, \
                         item_count, stream_specification, deletion_protection_enabled, \
                         table_stream_duration_hours, default_item_stream_duration_hours FROM \
                         tables WHERE table_name = ?1 ORDER BY table_name LIMIT ?2",
                    )
                    .map_err(map_sqlite_error)?;
                let rows = stmt
                    .query_map((table_name.as_str(), limit), table_metadata_record_from_row)
                    .map_err(map_sqlite_error)?;
                for row in rows {
                    records.push(row.map_err(map_sqlite_error)?);
                }
            } else {
                let mut stmt = conn
                    .prepare(
                        "SELECT id, table_name, table_status, created_at, attribute_definitions, \
                         key_schema, max_indexers, global_secondary_indexes, table_size_bytes, \
                         item_count, stream_specification, deletion_protection_enabled, \
                         table_stream_duration_hours, default_item_stream_duration_hours FROM \
                         tables ORDER BY table_name LIMIT ?1",
                    )
                    .map_err(map_sqlite_error)?;
                let rows = stmt
                    .query_map([limit], table_metadata_record_from_row)
                    .map_err(map_sqlite_error)?;
                for row in rows {
                    records.push(row.map_err(map_sqlite_error)?);
                }
            }
            Ok(records)
        })
        .await?;

        Ok(LogicalExportPage {
            domain: LogicalBackfillDomain::TableMetadata,
            records,
            next_cursor: None,
            checksum: unchecked_checksum()?,
        })
    }

    async fn export_item_records(
        &self,
        request: LogicalExportRequest,
    ) -> StorageResult<LogicalExportPage> {
        let table_name = request
            .table_name
            .as_deref()
            .map(TableName::new)
            .ok_or_else(|| StorageError::validation("item export requires table_name"))?;
        let scan = ScanTableRequest {
            table_name: table_name.clone(),
            index_name: None,
            limit: Some(request.limit),
            exclusive_start_key: request.cursor,
            consistent_read: true,
        };

        let (items, next_cursor) =
            <Self as StorageProvider>::scan_table_with_item_stream_versions(self, &scan).await?;
        let table_info = self.get_table_info_cached_arc(&table_name).await?;
        let mut records = Vec::with_capacity(items.len());
        for versioned in items {
            let item = versioned.item.to_attribute_map()?;
            let key_attributes = self.get_key_attributes(&item, &table_info.key_schema)?;
            records.push(LogicalBackfillRecord::PresentItem {
                table_name: table_name.as_ref().to_string(),
                key_json: key_attributes.canonical_dynamo_json().map_err(|error| {
                    StorageError::validation(format!("logical export key encoding failed: {error}"))
                })?,
                item_json: serde_json::to_string(&item)?,
                indexers: versioned.indexers,
                item_stream_version: versioned.item_stream_version,
            });
        }

        Ok(LogicalExportPage {
            domain: LogicalBackfillDomain::ItemRecords,
            records,
            next_cursor,
            checksum: unchecked_checksum()?,
        })
    }

    async fn export_durable_revisions(
        &self,
        request: LogicalExportRequest,
    ) -> StorageResult<LogicalExportPage> {
        let table_filter = request.table_name;
        let limit = i64::from(request.limit);
        let records = call_sqlite(&self.connection, move |conn| {
            let mut records = Vec::new();
            if let Some(table_name) = table_filter {
                let mut stmt = conn
                    .prepare(
                        "SELECT table_name, key_json, revision FROM item_revisions WHERE \
                         table_name = ?1 ORDER BY table_name, key_json LIMIT ?2",
                    )
                    .map_err(map_sqlite_error)?;
                let rows = stmt
                    .query_map(
                        (table_name.as_str(), limit),
                        durable_revision_record_from_row,
                    )
                    .map_err(map_sqlite_error)?;
                for row in rows {
                    records.push(row.map_err(map_sqlite_error)?);
                }
            } else {
                let mut stmt = conn
                    .prepare(
                        "SELECT table_name, key_json, revision FROM item_revisions ORDER BY \
                         table_name, key_json LIMIT ?1",
                    )
                    .map_err(map_sqlite_error)?;
                let rows = stmt
                    .query_map([limit], durable_revision_record_from_row)
                    .map_err(map_sqlite_error)?;
                for row in rows {
                    records.push(row.map_err(map_sqlite_error)?);
                }
            }
            Ok(records)
        })
        .await?;

        Ok(LogicalExportPage {
            domain: LogicalBackfillDomain::DurableRevisions,
            records,
            next_cursor: None,
            checksum: unchecked_checksum()?,
        })
    }

    async fn export_ttl_records(
        &self,
        request: LogicalExportRequest,
    ) -> StorageResult<LogicalExportPage> {
        let table_filter = request.table_name;
        let limit = i64::from(request.limit);
        let records = call_sqlite(&self.connection, move |conn| {
            let mut records = Vec::new();
            if let Some(table_name) = table_filter {
                let mut stmt = conn
                    .prepare(
                        "SELECT table_name, config_blob FROM sys_ttl_configs WHERE table_name = \
                         ?1 ORDER BY table_name LIMIT ?2",
                    )
                    .map_err(map_sqlite_error)?;
                let rows = stmt
                    .query_map((table_name.as_str(), limit), ttl_record_from_row)
                    .map_err(map_sqlite_error)?;
                for row in rows {
                    records.push(row.map_err(map_sqlite_error)?);
                }
            } else {
                let mut stmt = conn
                    .prepare(
                        "SELECT table_name, config_blob FROM sys_ttl_configs ORDER BY table_name \
                         LIMIT ?1",
                    )
                    .map_err(map_sqlite_error)?;
                let rows = stmt
                    .query_map([limit], ttl_record_from_row)
                    .map_err(map_sqlite_error)?;
                for row in rows {
                    records.push(row.map_err(map_sqlite_error)?);
                }
            }
            Ok(records)
        })
        .await?;

        Ok(LogicalExportPage {
            domain: LogicalBackfillDomain::TtlRecords,
            records,
            next_cursor: None,
            checksum: unchecked_checksum()?,
        })
    }

    async fn export_stream_records(
        &self,
        request: LogicalExportRequest,
    ) -> StorageResult<LogicalExportPage> {
        let stream_filter = request.table_name;
        let limit = i64::from(request.limit);
        let records = call_sqlite(&self.connection, move |conn| {
            let mut records = Vec::new();
            let resolved_stream_filter =
                resolve_stream_record_filter(conn, stream_filter.as_deref())?;
            append_stream_format_records(conn, &mut records, limit)?;
            append_user_stream_records(conn, &mut records, stream_filter.as_deref(), limit)?;
            append_stream_item_records(
                conn,
                &mut records,
                resolved_stream_filter.as_deref(),
                limit,
            )?;
            append_stream_cursor_records(
                conn,
                &mut records,
                resolved_stream_filter.as_deref(),
                limit,
            )?;
            Ok(records)
        })
        .await?;

        Ok(LogicalExportPage {
            domain: LogicalBackfillDomain::StreamRecords,
            records,
            next_cursor: None,
            checksum: unchecked_checksum()?,
        })
    }

    async fn export_gsi_records(
        &self,
        request: LogicalExportRequest,
    ) -> StorageResult<LogicalExportPage> {
        let table_filter = request.table_name;
        let limit = i64::from(request.limit);
        let records = call_sqlite(&self.connection, move |conn| {
            let mut records = Vec::new();
            append_gsi_backfill_records(conn, &mut records, table_filter.as_deref(), limit)?;
            append_physical_gsi_records(conn, &mut records, table_filter.as_deref(), limit)?;
            Ok(records)
        })
        .await?;

        Ok(LogicalExportPage {
            domain: LogicalBackfillDomain::GsiRecords,
            records,
            next_cursor: None,
            checksum: unchecked_checksum()?,
        })
    }

    async fn export_storage_control_plane_records(
        &self,
        request: LogicalExportRequest,
    ) -> StorageResult<LogicalExportPage> {
        if !self
            .logical_table_exists(STORAGE_CONTROL_PLANE_TABLE.to_string())
            .await?
        {
            return Ok(LogicalExportPage {
                domain: LogicalBackfillDomain::StorageControlPlane,
                records: Vec::new(),
                next_cursor: None,
                checksum: unchecked_checksum()?,
            });
        }
        let mut request = request;
        request.domain = LogicalBackfillDomain::ItemRecords;
        request.table_name = Some(STORAGE_CONTROL_PLANE_TABLE.to_string());
        let mut page = self.export_item_records(request).await?;
        page.domain = LogicalBackfillDomain::StorageControlPlane;
        Ok(page)
    }

    async fn export_empty_domain(
        &self,
        request: LogicalExportRequest,
    ) -> StorageResult<LogicalExportPage> {
        Ok(LogicalExportPage {
            domain: request.domain,
            records: Vec::new(),
            next_cursor: None,
            checksum: unchecked_checksum()?,
        })
    }

    async fn logical_table_exists(&self, table_name: String) -> StorageResult<bool> {
        call_sqlite(&self.connection, move |conn| {
            conn.query_row(
                "SELECT 1 FROM tables WHERE table_name = ?1",
                [table_name.as_str()],
                |_| Ok(true),
            )
            .optional()
            .map(|value| value.unwrap_or(false))
            .map_err(map_sqlite_error)
        })
        .await
    }
}

fn durable_revision_record_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<LogicalBackfillRecord> {
    let table_name: String = row.get(0)?;
    let key_json: String = row.get(1)?;
    let revision: i64 = row.get(2)?;
    let payload_json = serde_json::json!({
        "table_name": table_name,
        "key_json": key_json,
        "revision": revision,
    })
    .to_string();
    Ok(LogicalBackfillRecord::DomainRecord {
        domain: LogicalBackfillDomain::DurableRevisions,
        record_key_json: serde_json::json!({
            "table_name": table_name,
            "key_json": key_json,
        })
        .to_string(),
        payload_json,
    })
}

fn ttl_record_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<LogicalBackfillRecord> {
    let table_name: String = row.get(0)?;
    let config_blob: Vec<u8> = row.get(1)?;
    let payload_json = serde_json::json!({
        "table_name": table_name,
        "config_blob": config_blob,
    })
    .to_string();
    Ok(LogicalBackfillRecord::DomainRecord {
        domain: LogicalBackfillDomain::TtlRecords,
        record_key_json: serde_json::json!({
            "table_name": table_name,
        })
        .to_string(),
        payload_json,
    })
}

fn table_metadata_record_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<LogicalBackfillRecord> {
    let id: String = row.get(0)?;
    let table_name: String = row.get(1)?;
    let table_status: String = row.get(2)?;
    let created_at: i64 = row.get(3)?;
    let attribute_definitions: String = row.get(4)?;
    let key_schema: String = row.get(5)?;
    let max_indexers: i64 = row.get(6)?;
    let global_secondary_indexes: Option<String> = row.get(7)?;
    let table_size_bytes: i64 = row.get(8)?;
    let item_count: i64 = row.get(9)?;
    let stream_specification: Option<String> = row.get(10)?;
    let deletion_protection_enabled: bool = row.get(11)?;
    let table_stream_duration_hours: i64 = row.get(12)?;
    let default_item_stream_duration_hours: i64 = row.get(13)?;
    let payload_json = serde_json::json!({
        "id": id,
        "table_name": table_name,
        "table_status": table_status,
        "created_at": created_at,
        "attribute_definitions": attribute_definitions,
        "key_schema": key_schema,
        "max_indexers": max_indexers,
        "global_secondary_indexes": global_secondary_indexes,
        "table_size_bytes": table_size_bytes,
        "item_count": item_count,
        "stream_specification": stream_specification,
        "deletion_protection_enabled": deletion_protection_enabled,
        "table_stream_duration_hours": table_stream_duration_hours,
        "default_item_stream_duration_hours": default_item_stream_duration_hours,
    })
    .to_string();
    Ok(LogicalBackfillRecord::DomainRecord {
        domain: LogicalBackfillDomain::TableMetadata,
        record_key_json: serde_json::json!({
            "table_name": table_name,
        })
        .to_string(),
        payload_json,
    })
}

pub(super) fn unchecked_checksum() -> StorageResult<storage_backfill::LogicalBackfillChecksum> {
    storage_backfill::LogicalBackfillChecksum::new("unchecked").map_err(|error| {
        StorageError::internal(&format!("logical export checksum failed: {error}"))
    })
}

pub(super) fn payload_string(payload: &serde_json::Value, field: &str) -> StorageResult<String> {
    payload
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| StorageError::validation(format!("logical domain record missing {field}")))
}

pub(super) fn payload_optional_string(
    payload: &serde_json::Value,
    field: &str,
) -> StorageResult<Option<String>> {
    match payload.get(field) {
        Some(value) if value.is_null() => Ok(None),
        Some(value) => value.as_str().map(str::to_string).map(Some).ok_or_else(|| {
            StorageError::validation(format!("logical domain record has invalid {field}"))
        }),
        None => Ok(None),
    }
}

pub(super) fn payload_i64(payload: &serde_json::Value, field: &str) -> StorageResult<i64> {
    payload
        .get(field)
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| StorageError::validation(format!("logical domain record missing {field}")))
}

pub(super) fn payload_optional_i64(
    payload: &serde_json::Value,
    field: &str,
) -> StorageResult<Option<i64>> {
    match payload.get(field) {
        Some(value) if value.is_null() => Ok(None),
        Some(value) => value.as_i64().map(Some).ok_or_else(|| {
            StorageError::validation(format!("logical domain record has invalid {field}"))
        }),
        None => Ok(None),
    }
}
