use infrastructure_provider::{
    limits::InfrastructureLimits,
    physical_store::{PhysicalRecordCapacity, PhysicalRecordStore},
};
use storage_common::retry::retry_native_conflicts;
use storage_provider::DYNAMO_WRITE_RETRY;
use storage_types::StorageResult;

use crate::{SqlBackend, SqlStore};

impl<B: SqlBackend> PhysicalRecordStore for SqlStore<B> {
    fn record_capacity(
        &self,
        limits: InfrastructureLimits,
    ) -> StorageResult<PhysicalRecordCapacity> {
        self.backend.record_capacity(limits)
    }

    async fn initialize_records(&self, limits: InfrastructureLimits) -> StorageResult<[u8; 16]> {
        self.record_capacity(limits)?;
        let engine = self.backend.engine();
        retry_native_conflicts(self, DYNAMO_WRITE_RETRY, || {
            self.backend.with_schema_attempt(move |attempt| {
                Box::pin(async move { super::layout::initialize_layout(attempt, engine).await })
            })
        })
        .await
    }
}
