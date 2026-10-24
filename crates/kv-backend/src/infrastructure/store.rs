//! Ordinary retained images and explicit record attachment share one physical
//! pool.

use infrastructure_provider::{
    limits::InfrastructureLimits,
    physical_store::{PhysicalRecordCapacity, PhysicalRecordStore},
};
use storage_types::{AttemptMode, StorageResult};

use super::layout;
use crate::{KvBackend, KvStore};

impl<B: KvBackend> PhysicalRecordStore for KvStore<B> {
    fn record_capacity(
        &self,
        limits: InfrastructureLimits,
    ) -> StorageResult<PhysicalRecordCapacity> {
        layout::record_capacity(self.backend.as_ref(), limits)
    }

    async fn initialize_records(&self, limits: InfrastructureLimits) -> StorageResult<[u8; 16]> {
        self.record_capacity(limits)?;
        self.backend
            .with_attempt(AttemptMode::ReadWrite, |native| {
                Box::pin(async move { layout::initialize_layout(native).await })
            })
            .await
    }
}
