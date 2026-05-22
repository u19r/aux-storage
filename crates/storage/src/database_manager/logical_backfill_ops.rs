use storage_backfill::{
    LogicalBackfillChunk, LogicalBackfillManifest, LogicalBackfillResult, LogicalExportPage,
    LogicalExportRequest,
};
use storage_types::StorageResult;

use crate::DatabaseManager;

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
        self.storage_provider()
            .export_logical_backfill_page(request)
            .await
    }

    pub async fn import_logical_backfill_chunk(
        &self,
        manifest: &LogicalBackfillManifest,
        chunk: LogicalBackfillChunk,
    ) -> StorageResult<LogicalBackfillResult> {
        self.storage_provider()
            .import_logical_backfill_chunk(manifest, chunk)
            .await
    }
}
