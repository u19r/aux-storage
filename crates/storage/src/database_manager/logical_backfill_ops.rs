use storage_backfill::{
    LogicalBackfillChunk, LogicalBackfillManifest, LogicalBackfillResult, LogicalExportPage,
    LogicalExportRequest,
};
use storage_types::StorageResult;

use crate::database_manager::{DatabaseManager, ROUTED_DEFAULT_CONNECTION_ID};

#[async_trait::async_trait]
impl storage_backfill::LogicalBackfillExport for DatabaseManager {
    async fn export_logical_page(
        &self,
        request: LogicalExportRequest,
    ) -> StorageResult<LogicalExportPage> {
        self.export_logical_backfill_page(request).await
    }
}

#[async_trait::async_trait]
impl storage_backfill::LogicalBackfillImport for DatabaseManager {
    async fn import_logical_chunk(
        &self,
        manifest: &LogicalBackfillManifest,
        chunk: LogicalBackfillChunk,
    ) -> StorageResult<LogicalBackfillResult> {
        self.import_logical_backfill_chunk(manifest, chunk).await
    }
}

impl DatabaseManager {
    pub async fn export_logical_backfill_page(
        &self,
        request: LogicalExportRequest,
    ) -> StorageResult<LogicalExportPage> {
        self.run_control_admitted(ROUTED_DEFAULT_CONNECTION_ID, move |provider| async move {
            let database_call = metrics_facade::begin_database_call("export_logical_backfill_page");
            let result = provider.export_logical_backfill_page(request).await;
            drop(database_call);
            result
        })
        .await
    }

    pub async fn import_logical_backfill_chunk(
        &self,
        manifest: &LogicalBackfillManifest,
        chunk: LogicalBackfillChunk,
    ) -> StorageResult<LogicalBackfillResult> {
        self.run_control_admitted(ROUTED_DEFAULT_CONNECTION_ID, |provider| async move {
            let database_call =
                metrics_facade::begin_database_call("import_logical_backfill_chunk");
            let result = provider
                .import_logical_backfill_chunk(manifest, chunk)
                .await;
            drop(database_call);
            result
        })
        .await
    }
}
