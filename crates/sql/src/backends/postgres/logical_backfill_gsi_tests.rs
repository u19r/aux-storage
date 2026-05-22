use storage_backfill::{
    LogicalBackfillChecksum, LogicalBackfillChunk, LogicalBackfillChunkId,
    LogicalBackfillChunkSummary, LogicalBackfillDomain, LogicalBackfillExport, LogicalBackfillId,
    LogicalBackfillImport, LogicalBackfillManifest, LogicalExportRequest, SyncLearnerCatchupPolicy,
};
use storage_provider::StorageProvider;

use super::PostgresStorageProvider;

#[tokio::test]
async fn postgres_logical_gsi_export_import_preserves_backfill_state() {
    let Some(source) = initialized_provider().await else {
        return;
    };
    let Some(destination) = initialized_provider().await else {
        return;
    };
    insert_gsi_backfill_row(&source).await;

    let page = source
        .export_logical_page(LogicalExportRequest {
            manifest_id: LogicalBackfillId::new("manifest").expect("manifest id"),
            domain: LogicalBackfillDomain::GsiRecords,
            table_name: Some("gsi_table".to_string()),
            cursor: None,
            limit: 10,
        })
        .await
        .expect("export gsi records");
    assert_eq!(page.records.len(), 1);

    destination
        .import_logical_chunk(
            &logical_manifest(),
            LogicalBackfillChunk {
                summary: LogicalBackfillChunkSummary {
                    id: LogicalBackfillChunkId::new("chunk-1").expect("chunk id"),
                    domain: LogicalBackfillDomain::GsiRecords,
                    record_count: page.records.len() as u64,
                    checksum: LogicalBackfillChecksum::new("unchecked").expect("checksum"),
                },
                records: page.records,
            },
        )
        .await
        .expect("import gsi records");

    let client = destination.pool.get().await.expect("connect destination");
    let rows = client
        .query(
            "SELECT status, scan_lek, captured_stream_tail FROM gsi_backfill WHERE table_name = $1",
            &[&"gsi_table"],
        )
        .await
        .expect("read gsi backfill");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("status"), "running");
    assert_eq!(
        rows[0].get::<_, Option<String>>("scan_lek").as_deref(),
        Some(r#"{"pk":{"S":"a"}}"#)
    );
    assert_eq!(
        rows[0]
            .get::<_, Option<String>>("captured_stream_tail")
            .as_deref(),
        Some("tail-1")
    );
}

async fn initialized_provider() -> Option<PostgresStorageProvider> {
    let dsn = std::env::var("TEST_POSTGRES_DSN")
        .ok()
        .or_else(|| std::env::var("CUCUMBER_POSTGRES_DSN").ok())?;
    let provider = PostgresStorageProvider::new(&dsn, 8)
        .await
        .expect("create postgres provider");
    provider
        .initialize_storage()
        .await
        .expect("initialize postgres storage");
    Some(provider)
}

async fn insert_gsi_backfill_row(provider: &PostgresStorageProvider) {
    let client = provider.pool.get().await.expect("connect source");
    client
        .execute(
            r"INSERT INTO gsi_backfill (
                table_name, index_name, status, scan_lek, captured_stream_tail, created_at,
                updated_at
              )
              VALUES ($1, $2, $3, $4, $5, $6, $7)
              ON CONFLICT(table_name, index_name)
              DO UPDATE SET status = excluded.status",
            &[
                &"gsi_table",
                &"status_index",
                &"running",
                &Some(r#"{"pk":{"S":"a"}}"#),
                &Some("tail-1"),
                &1_i64,
                &2_i64,
            ],
        )
        .await
        .expect("insert gsi backfill row");
}

fn logical_manifest() -> LogicalBackfillManifest {
    LogicalBackfillManifest::for_policy(
        LogicalBackfillId::new("manifest").expect("manifest id"),
        &SyncLearnerCatchupPolicy,
        "postgres",
        "postgres",
        vec![LogicalBackfillDomain::GsiRecords],
    )
}
