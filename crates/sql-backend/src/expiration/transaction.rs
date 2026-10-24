//! Physical expiration index and task-progress operations inside a SQL catalog
//! attempt.

use storage_common::TtlIndexWrite;
use storage_provider::maintenance::{ExpirationIndexLocator, ExpirationPort, ExpirationScanRead};
use storage_types::{BoundTableInfo, StorageResult};

use super::read::read_expiration_index;
use crate::{
    SqlAttempt, SqlLayout, catalog::SqlCatalogAttempt, item_write::apply_ttl_index_writes,
};

impl<A: SqlAttempt, L: SqlLayout> ExpirationPort for SqlCatalogAttempt<'_, A, L> {
    async fn scan_expiration_index(
        &mut self,
        table: &BoundTableInfo,
        read: &ExpirationScanRead,
    ) -> StorageResult<(Vec<ExpirationIndexLocator>, bool)> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            read_expiration_index(
                catalog.attempt,
                catalog.engine,
                &read.into(),
                table.history_id,
            )
            .await
        }
        .await;
        settlement.complete(result)
    }

    async fn delete_expiration_index(
        &mut self,
        table: &BoundTableInfo,
        writes: impl Iterator<Item = TtlIndexWrite> + Send,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result =
            async { apply_ttl_index_writes(catalog.attempt, catalog.engine, table, writes).await }
                .await;
        settlement.complete(result)
    }
}

impl<A: SqlAttempt, L: SqlLayout> storage_provider::TtlProjectionScanAttempt
    for SqlCatalogAttempt<'_, A, L>
{
    async fn scan_ttl_projections(
        &mut self,
        scan: &storage_provider::TtlProjectionScan,
    ) -> StorageResult<storage_provider::TtlProjectionPage> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let (rows, more) = read_expiration_index(
                catalog.attempt,
                catalog.engine,
                scan.read(),
                scan.table().history_id,
            )
            .await?;
            scan.finish_native(rows, more)
        }
        .await;
        settlement.complete(result)
    }
}
