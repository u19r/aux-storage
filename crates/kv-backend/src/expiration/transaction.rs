//! Physical expiration index and task-progress operations inside a KV catalog
//! attempt.

use storage_common::TtlIndexWrite;
use storage_provider::maintenance::{ExpirationIndexLocator, ExpirationPort, ExpirationScanRead};
use storage_types::{BoundTableInfo, StorageResult};

use super::read::read_expiration_index;
use crate::{KvAttempt, catalog::KvCatalogAttempt, item_write::apply_ttl_index_writes};

impl<A: KvAttempt> ExpirationPort for KvCatalogAttempt<'_, A> {
    async fn scan_expiration_index(
        &mut self,
        table: &BoundTableInfo,
        read: &ExpirationScanRead,
    ) -> StorageResult<(Vec<ExpirationIndexLocator>, bool)> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let table_id = catalog.identity_for(&table.table_name)?.table_id;
            read_expiration_index(catalog.attempt, table_id, &read.into()).await
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
        let result = async {
            let table_id = catalog.identity_for(&table.table_name)?.table_id;
            apply_ttl_index_writes(catalog.attempt, table_id, writes).await
        }
        .await;
        settlement.complete(result)
    }
}

impl<A: KvAttempt> storage_provider::TtlProjectionScanAttempt for KvCatalogAttempt<'_, A> {
    async fn scan_ttl_projections(
        &mut self,
        scan: &storage_provider::TtlProjectionScan,
    ) -> StorageResult<storage_provider::TtlProjectionPage> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let table_id = catalog.identity_for(&scan.table().table_name)?.table_id;
            let (rows, more) =
                read_expiration_index(catalog.attempt, table_id, scan.read()).await?;
            scan.finish_native(rows, more)
        }
        .await;
        settlement.complete(result)
    }
}
