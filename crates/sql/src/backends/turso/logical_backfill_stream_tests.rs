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
async fn turso_logical_stream_export_import_preserves_rows() {
    let source = initialized_provider().await;
    let destination = initialized_provider().await;
    insert_stream_rows(&source).await;

    let page = source
        .export_logical_page(LogicalExportRequest {
            manifest_id: LogicalBackfillId::new("manifest").expect("manifest id"),
            domain: LogicalBackfillDomain::StreamRecords,
            table_name: None,
            cursor: None,
            limit: 10,
        })
        .await
        .expect("export stream records");
    assert_eq!(page.records.len(), 4);

    destination
        .import_logical_chunk(
            &logical_manifest(),
            LogicalBackfillChunk {
                summary: LogicalBackfillChunkSummary {
                    id: LogicalBackfillChunkId::new("chunk-1").expect("chunk id"),
                    domain: LogicalBackfillDomain::StreamRecords,
                    record_count: page.records.len() as u64,
                    checksum: LogicalBackfillChecksum::new("unchecked").expect("checksum"),
                },
                records: page.records,
            },
        )
        .await
        .expect("import stream records");

    let conn = destination.connect().await.expect("connect destination");
    let stream_items = destination
        .query_rows(
            &conn,
            "SELECT stream_name, item_id, data_type FROM sys_stream_items",
            Vec::new(),
        )
        .await
        .expect("read stream items");
    assert_eq!(stream_items.len(), 1);
    assert_eq!(
        stream_items[0].get("stream_name"),
        Some(&TursoValue::Text("stream-internal".to_string()))
    );
    assert_eq!(
        stream_items[0].get("item_id"),
        Some(&TursoValue::Text("item-1".to_string()))
    );
    assert_eq!(
        stream_items[0].get("data_type"),
        Some(&TursoValue::Integer(2))
    );

    let cursors = destination
        .query_rows(
            &conn,
            "SELECT position FROM sys_stream_cursors WHERE cursor_name = ?1",
            vec![TursoValue::Text("cursor-1".to_string())],
        )
        .await
        .expect("read stream cursor");
    assert_eq!(
        cursors[0].get("position"),
        Some(&TursoValue::Text("item-1".to_string()))
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

async fn insert_stream_rows(provider: &TursoStorageProvider) {
    let provider = provider.clone();
    provider
        .with_exclusive_transaction(true, |conn| {
            let provider = provider.clone();
            Box::pin(async move {
                provider
                    .execute(
                        conn,
                        r"INSERT INTO sys_stream_format_metadata (format_key, format_version)
                          VALUES (?1, ?2)
                          ON CONFLICT(format_key)
                          DO UPDATE SET format_version = excluded.format_version",
                        vec![
                            TursoValue::Text("item_versioned_stream".to_string()),
                            TursoValue::Integer(1),
                        ],
                    )
                    .await?;
                provider
                    .execute(
                        conn,
                        r"INSERT INTO sys_user_streams (
                            stream_name, internal_id, ttl_seconds, created_at, updated_at
                          )
                          VALUES (?1, ?2, ?3, ?4, ?5)",
                        vec![
                            TursoValue::Text("stream-name".to_string()),
                            TursoValue::Text("stream-internal".to_string()),
                            TursoValue::Integer(60),
                            TursoValue::Integer(1),
                            TursoValue::Integer(2),
                        ],
                    )
                    .await?;
                provider
                    .execute(
                        conn,
                        r"INSERT INTO sys_stream_items (
                            stream_name, item_id, data, created_at, data_type
                          )
                          VALUES (?1, ?2, ?3, ?4, ?5)",
                        vec![
                            TursoValue::Text("stream-internal".to_string()),
                            TursoValue::Text("item-1".to_string()),
                            TursoValue::Blob(vec![1, 2, 3]),
                            TursoValue::Integer(3),
                            TursoValue::Integer(2),
                        ],
                    )
                    .await?;
                provider
                    .execute(
                        conn,
                        r"INSERT INTO sys_stream_cursors (
                            cursor_name, stream_name, position, created_at
                          )
                          VALUES (?1, ?2, ?3, ?4)",
                        vec![
                            TursoValue::Text("cursor-1".to_string()),
                            TursoValue::Text("stream-internal".to_string()),
                            TursoValue::Text("item-1".to_string()),
                            TursoValue::Integer(4),
                        ],
                    )
                    .await?;
                Ok(())
            })
        })
        .await
        .expect("insert stream rows");
}

fn logical_manifest() -> LogicalBackfillManifest {
    LogicalBackfillManifest::for_policy(
        LogicalBackfillId::new("manifest").expect("manifest id"),
        &SyncLearnerCatchupPolicy,
        "turso",
        "turso",
        vec![LogicalBackfillDomain::StreamRecords],
    )
}
