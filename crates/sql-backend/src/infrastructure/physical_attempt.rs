//! Physical record lowering reuses the catalog's caller-owned SQL attempt.

use infrastructure_provider::{
    batch::RecordBatch,
    limits::InfrastructureLimits,
    page::{RecordPage, RecordPageRequest},
    physical_attempt::PhysicalRecordAttempt,
    record::{ConditionalWriteResult, DurableRecord, DurableRecordKey},
};
use storage_types::{ReadProtection, StorageResult};

use super::{read, write};
use crate::{SqlAttempt, SqlLayout, catalog::SqlCatalogAttempt};

impl<A: SqlAttempt, L: SqlLayout> PhysicalRecordAttempt for SqlCatalogAttempt<'_, A, L> {
    fn read_protection(&self) -> ReadProtection {
        self.protection
    }

    fn reject_settlement(&mut self) {
        self.attempt.reject_settlement();
    }

    fn reject_settlement_error(&mut self, error: &storage_types::StorageError) {
        self.attempt.reject_settlement_error(error);
    }

    async fn has_records_in_namespace(
        &mut self,
        namespace: &str,
        protection: ReadProtection,
    ) -> StorageResult<bool> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            let statement = super::sql_statements::existing_namespace_record(
                catalog.engine.engine(),
                namespace,
            )?;
            let page = catalog
                .attempt
                .query(statement, protection, std::num::NonZeroUsize::MIN)
                .await?;
            Ok(!page.rows.is_empty() || page.more)
        }
        .await;
        operation.complete(result)
    }

    async fn load_records(
        &mut self,
        keys: &[DurableRecordKey],
        protection: ReadProtection,
        limits: InfrastructureLimits,
    ) -> StorageResult<Vec<Option<DurableRecord>>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async { read::load_records(catalog, keys, protection, limits).await }.await;
        operation.complete(result)
    }

    async fn scan_records(
        &mut self,
        request: RecordPageRequest,
        limits: InfrastructureLimits,
    ) -> StorageResult<RecordPage> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async { read::scan_records(catalog, limits, request).await }.await;
        operation.complete(result)
    }

    async fn store_records(
        &mut self,
        batch: &RecordBatch<'_>,
    ) -> StorageResult<ConditionalWriteResult> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async { write::store_records(catalog, batch).await }.await;
        operation.complete(result)
    }
}

impl<A: SqlAttempt, L: SqlLayout> stream_provider::retained_image::RetainedOwnershipAttempt
    for SqlCatalogAttempt<'_, A, L>
{
    fn ownership_changes(
        &mut self,
    ) -> &mut stream_provider::retained_image::RetainedOwnershipChanges {
        self.attempt.ownership_changes()
    }
}
