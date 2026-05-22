use serde::{Deserialize, Serialize};
use storage_backfill::{
    LogicalBackfillChecksum, LogicalBackfillDomain, LogicalBackfillRecord, LogicalExportPage,
    LogicalExportRequest,
};
use storage_types::{StorageError, StorageResult, StoredTableInfo, TableName};

pub(super) fn table_metadata_record(
    table_info: StoredTableInfo,
) -> StorageResult<LogicalBackfillRecord> {
    Ok(LogicalBackfillRecord::DomainRecord {
        domain: LogicalBackfillDomain::TableMetadata,
        record_key_json: serde_json::json!({
            "table_name": table_info.table_name,
        })
        .to_string(),
        payload_json: serde_json::to_string(&table_info)?,
    })
}

pub(super) fn durable_revision_record(
    payload: RevisionRecordPayload,
) -> StorageResult<LogicalBackfillRecord> {
    Ok(LogicalBackfillRecord::DomainRecord {
        domain: LogicalBackfillDomain::DurableRevisions,
        record_key_json: serde_json::json!({
            "table_name": payload.table_name,
            "key_json": payload.key_json,
        })
        .to_string(),
        payload_json: serde_json::to_string(&payload)?,
    })
}

pub(super) fn ttl_record(
    table_name: TableName,
    config: storage_common::ttl::TtlConfigRecord,
) -> StorageResult<LogicalBackfillRecord> {
    let payload = TtlRecordPayload {
        table_name: table_name.as_ref().to_string(),
        config,
    };
    Ok(LogicalBackfillRecord::DomainRecord {
        domain: LogicalBackfillDomain::TtlRecords,
        record_key_json: serde_json::json!({
            "table_name": payload.table_name,
        })
        .to_string(),
        payload_json: serde_json::to_string(&payload)?,
    })
}

pub(super) fn empty_page(request: LogicalExportRequest) -> StorageResult<LogicalExportPage> {
    Ok(LogicalExportPage {
        domain: request.domain,
        records: Vec::new(),
        next_cursor: None,
        checksum: unchecked_checksum()?,
    })
}

pub(super) fn unchecked_checksum() -> StorageResult<LogicalBackfillChecksum> {
    LogicalBackfillChecksum::new("unchecked").map_err(|error| {
        StorageError::internal(&format!("logical export checksum failed: {error}"))
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct RevisionRecordPayload {
    pub(super) table_name: String,
    pub(super) key_json: String,
    pub(super) revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct TtlRecordPayload {
    pub(super) table_name: String,
    pub(super) config: storage_common::ttl::TtlConfigRecord,
}
