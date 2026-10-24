use infrastructure_provider::{
    limits::InfrastructureLimits, physical_attempt::PhysicalRecordAttempt,
};
use storage_types::StorageResult;

use super::{hydration::ImageHydrationBatch, manifest::RetainedImageManifest};

impl RetainedImageManifest {
    /// Single and multi-original reads share admission and verified hydration.
    pub async fn load<A: PhysicalRecordAttempt>(
        self,
        attempt: &mut A,
        limits: InfrastructureLimits,
    ) -> StorageResult<Vec<u8>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let maximum = self.length;
            let batch = ImageHydrationBatch::try_new(vec![self], maximum)?;
            batch
                .load(attempt, limits)
                .await?
                .pop()
                .ok_or_else(super::error::invalid_observation)
        }
        .await;
        settlement.complete(result)
    }
}
