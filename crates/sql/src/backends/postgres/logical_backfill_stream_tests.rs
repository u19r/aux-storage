use storage_backfill::{
    LogicalBackfillChecksum, LogicalBackfillChunk, LogicalBackfillChunkId,
    LogicalBackfillChunkSummary, LogicalBackfillDomain, LogicalBackfillExport, LogicalBackfillId,
    LogicalBackfillImport, LogicalBackfillManifest, LogicalExportRequest, SyncLearnerCatchupPolicy,
};
use storage_provider::StorageProvider;
use stream_provider::StreamProvider;

use super::PostgresStorageProvider;

#[tokio::test]
async fn postgres_logical_stream_export_import_preserves_rows() {
    let Some(source) = initialized_provider().await else {
        return;
    };
    let Some(destination) = initialized_provider().await else {
        return;
    };
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
    assert_eq!(page.records.len(), 5);

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

    let client = destination.pool.get().await.expect("connect destination");
    let stream_items = client
        .query(
            "SELECT stream_name, item_id, data_type FROM sys_stream_items WHERE item_id = $1",
            &[&"item-1"],
        )
        .await
        .expect("read stream items");
    assert_eq!(stream_items.len(), 1);
    assert_eq!(
        stream_items[0].get::<_, String>("stream_name"),
        "stream-internal"
    );
    assert_eq!(stream_items[0].get::<_, i32>("data_type"), 2);

    let cursors = client
        .query(
            "SELECT position FROM sys_stream_cursors WHERE cursor_name = $1",
            &[&"cursor-1"],
        )
        .await
        .expect("read stream cursor");
    assert_eq!(cursors[0].get::<_, String>("position"), "item-1");
}

async fn initialized_provider() -> Option<PostgresStorageProvider> {
    let dsn = std::env::var("TEST_POSTGRES_DSN")
        .ok()
        .or_else(|| std::env::var("CUCUMBER_POSTGRES_DSN").ok())?;
    let dsn = isolated_schema_dsn(&dsn).await;
    let provider = PostgresStorageProvider::new(&dsn, 8)
        .await
        .expect("create postgres provider");
    provider
        .initialize_storage()
        .await
        .expect("initialize postgres storage");
    provider
        .initialize_stream()
        .await
        .expect("initialize postgres stream storage");
    Some(provider)
}

async fn isolated_schema_dsn(base_dsn: &str) -> String {
    let schema = format!("test_{}", uuid::Uuid::now_v7().simple());
    let (client, connection) = tokio_postgres::connect(base_dsn, tokio_postgres::NoTls)
        .await
        .expect("connect postgres to create isolated schema");
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("postgres isolated schema connection failed: {err}");
        }
    });
    client
        .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
        .await
        .expect("create isolated postgres schema");
    dsn_with_search_path(base_dsn, &schema)
}

fn dsn_with_search_path(base_dsn: &str, schema: &str) -> String {
    if base_dsn.contains("://") {
        let separator = if base_dsn.contains('?') { '&' } else { '?' };
        format!("{base_dsn}{separator}options=-csearch_path%3D{schema}")
    } else {
        format!("{base_dsn} options='-csearch_path={schema}'")
    }
}

async fn insert_stream_rows(provider: &PostgresStorageProvider) {
    let client = provider.pool.get().await.expect("connect source");
    client
        .execute(
            r"INSERT INTO sys_user_streams (
                stream_name, internal_id, ttl_seconds, created_at, updated_at
              )
              VALUES ($1, $2, $3, $4, $5)
              ON CONFLICT(stream_name) DO UPDATE SET internal_id = excluded.internal_id",
            &[
                &"stream-public",
                &"stream-internal",
                &Some(60_i64),
                &1_i64,
                &2_i64,
            ],
        )
        .await
        .expect("insert user stream");
    client
        .execute(
            r"INSERT INTO sys_stream_items (stream_name, item_id, data, created_at, data_type)
              VALUES ($1, $2, $3, $4, $5)
              ON CONFLICT(stream_name, item_id) DO UPDATE SET data = excluded.data",
            &[
                &"stream-internal",
                &"item-1",
                &vec![1_u8, 2, 3],
                &3_i64,
                &2_i32,
            ],
        )
        .await
        .expect("insert stream item");
    client
        .execute(
            r"INSERT INTO sys_stream_cursors (cursor_name, stream_name, position, created_at)
              VALUES ($1, $2, $3, $4)
              ON CONFLICT(cursor_name, stream_name) DO UPDATE SET position = excluded.position",
            &[&"cursor-1", &"stream-internal", &"item-1", &4_i64],
        )
        .await
        .expect("insert stream cursor");
}

fn logical_manifest() -> LogicalBackfillManifest {
    LogicalBackfillManifest::for_policy(
        LogicalBackfillId::new("manifest").expect("manifest id"),
        &SyncLearnerCatchupPolicy,
        "postgres",
        "postgres",
        vec![LogicalBackfillDomain::StreamRecords],
    )
}
