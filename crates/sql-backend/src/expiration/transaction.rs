//! Physical expiration index and task-progress operations inside a SQL catalog
//! attempt.

use storage_common::TtlIndexWrite;
use storage_provider::maintenance::{
    ExpirationIndexLocator, ExpirationPort, ExpirationScanRead, TransactReceiptRow,
};
use storage_types::{BoundTableInfo, StorageResult};

use super::read::read_expiration_index;
use crate::{SqlAttempt, SqlLayout, catalog::SqlCatalogAttempt};

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
        // The primary partial index has no independently stale SQL entry.
        // Never clear metadata here: the current item may have been renewed.
        let _ = (table, writes);
        Ok(())
    }

    async fn scan_transact_receipts(
        &mut self,
        after: Option<&str>,
        limit: std::num::NonZeroU32,
    ) -> StorageResult<Vec<TransactReceiptRow>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result =
            crate::transact_write::scan_receipts(catalog.attempt, catalog.engine, after, limit)
                .await;
        settlement.complete(result)
    }

    async fn delete_transact_receipts(
        &mut self,
        receipts: &[TransactReceiptRow],
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result =
            crate::transact_write::delete_receipts(catalog.attempt, catalog.engine, receipts).await;
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
