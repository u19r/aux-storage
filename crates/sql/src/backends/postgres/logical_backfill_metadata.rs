use storage_backfill::{LogicalBackfillDomain, LogicalBackfillRecord};
use storage_provider::StorageProvider;
use storage_types::{
    BillingMode, CreateGlobalSecondaryIndex, CreateTableRequest, StorageResult, StoredTableInfo,
    TableName,
};

use super::{
    PostgresStorageProvider,
    logical_backfill_values::{payload_i64, payload_string},
};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct TtlRecordPayload {
    table_name: String,
    config: storage_common::ttl::TtlConfigRecord,
}

pub(super) async fn import_table_metadata_record(
    provider: &PostgresStorageProvider,
    payload_json: &str,
) -> StorageResult<()> {
    let table_info = serde_json::from_str::<StoredTableInfo>(payload_json)?;
    if provider.table_exists(&table_info.table_name).await? {
        return Ok(());
    }
    let gsis = table_info.global_secondary_indexes.clone().map(|indexes| {
        indexes
            .into_iter()
            .map(|index| CreateGlobalSecondaryIndex {
                index_name: index.index_name,
                key_schema: index.key_schema,
                projection: index.projection,
                provisioned_throughput: None,
            })
            .collect()
    });
    let request = CreateTableRequest::new(
        table_info.table_name.clone(),
        table_info.attribute_definitions,
        table_info.key_schema,
        BillingMode::PayPerRequest,
    )
    .with_global_secondary_indexes(gsis)
    .with_stream_specification(table_info.stream_specification);
    let request = CreateTableRequest {
        max_indexers: table_info.max_indexers,
        deletion_protection_enabled: Some(table_info.deletion_protection_enabled),
        ..request
    };
    provider.create_table(&request).await
}

pub(super) async fn import_durable_revision_record(
    provider: &PostgresStorageProvider,
    payload_json: &str,
) -> StorageResult<()> {
    let payload: serde_json::Value = serde_json::from_str(payload_json)?;
    let table_name = payload_string(&payload, "table_name")?;
    let key_json = payload_string(&payload, "key_json")?;
    let revision = payload_i64(&payload, "revision")?;
    let client = provider
        .pool
        .get()
        .await
        .map_err(PostgresStorageProvider::map_postgres_client_acquire_error)?;
    client
        .execute(
            r"INSERT INTO item_revisions (table_name, key_json, revision)
              VALUES ($1, $2, $3)
              ON CONFLICT(table_name, key_json)
              DO UPDATE SET revision = excluded.revision",
            &[&table_name, &key_json, &revision],
        )
        .await
        .map_err(|err| {
            PostgresStorageProvider::map_postgres_write_error("import item revision", err)
        })?;
    Ok(())
}

pub(super) async fn import_ttl_record(
    provider: &PostgresStorageProvider,
    payload_json: &str,
) -> StorageResult<()> {
    let payload = serde_json::from_str::<TtlRecordPayload>(payload_json)?;
    provider
        .save_ttl_config(&TableName::new(&payload.table_name), &payload.config)
        .await
}

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

pub(super) fn durable_revision_record_from_row(
    row: &tokio_postgres::Row,
) -> StorageResult<LogicalBackfillRecord> {
    let table_name = row
        .try_get::<_, String>(0)
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode table_name", err))?;
    let key_json = row
        .try_get::<_, String>(1)
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode key_json", err))?;
    let revision = row
        .try_get::<_, i64>(2)
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode revision", err))?;
    Ok(LogicalBackfillRecord::DomainRecord {
        domain: LogicalBackfillDomain::DurableRevisions,
        record_key_json: serde_json::json!({
            "table_name": table_name,
            "key_json": key_json,
        })
        .to_string(),
        payload_json: serde_json::json!({
            "table_name": table_name,
            "key_json": key_json,
            "revision": revision,
        })
        .to_string(),
    })
}
