use storage_backfill::{
    LogicalBackfillChecksum, LogicalBackfillChunk, LogicalBackfillChunkId,
    LogicalBackfillChunkSummary, LogicalBackfillDomain, LogicalBackfillExport, LogicalBackfillId,
    LogicalBackfillImport, LogicalBackfillManifest, LogicalExportRequest, SyncLearnerCatchupPolicy,
};
use storage_provider::StorageProvider;
use stream_provider::StreamProvider;
use turso::Value as TursoValue;

use super::TursoStorageProvider;

#[tokio::test]
async fn turso_logical_gsi_export_import_preserves_backfill_state() {
    let source = initialized_provider().await;
    let destination = initialized_provider().await;
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

    let conn = destination.connect().await.expect("connect destination");
    let rows = destination
        .query_rows(
            &conn,
            "SELECT status, scan_lek, captured_stream_tail FROM gsi_backfill WHERE table_name = ?1",
            vec![TursoValue::Text("gsi_table".to_string())],
        )
        .await
        .expect("read gsi backfill");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("status"),
        Some(&TursoValue::Text("running".to_string()))
    );
    assert_eq!(
        rows[0].get("scan_lek"),
        Some(&TursoValue::Text(r#"{"pk":{"S":"a"}}"#.to_string()))
    );
    assert_eq!(
        rows[0].get("captured_stream_tail"),
        Some(&TursoValue::Text("tail-1".to_string()))
    );
}

async fn initialized_provider() -> TursoStorageProvider {
    let provider = TursoStorageProvider::new(":memory:")
        .await
        .expect("create turso provider");
    provider
        .initialize_storage()
        .await
        .expect("initialize turso storage");
    provider
        .initialize_stream()
        .await
        .expect("initialize turso streams");
    provider
}

async fn insert_gsi_backfill_row(provider: &TursoStorageProvider) {
    let conn = provider.connect().await.expect("connect source");
    provider
        .execute(
            &conn,
            r"INSERT INTO gsi_backfill (
                table_name, index_name, status, scan_lek, captured_stream_tail, created_at,
                updated_at
              )
              VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            vec![
                TursoValue::Text("gsi_table".to_string()),
                TursoValue::Text("status_index".to_string()),
                TursoValue::Text("running".to_string()),
                TursoValue::Text(r#"{"pk":{"S":"a"}}"#.to_string()),
                TursoValue::Text("tail-1".to_string()),
                TursoValue::Integer(1),
                TursoValue::Integer(2),
            ],
        )
        .await
        .expect("insert gsi backfill row");
}

fn logical_manifest() -> LogicalBackfillManifest {
    LogicalBackfillManifest::for_policy(
        LogicalBackfillId::new("manifest").expect("manifest id"),
        &SyncLearnerCatchupPolicy,
        "turso",
        "turso",
        vec![LogicalBackfillDomain::GsiRecords],
    )
}
