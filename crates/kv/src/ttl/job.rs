use std::sync::Arc;

use async_trait::async_trait;
use bg_jobs::BackgroundJob;

use crate::{SortedKvDbStorageProvider, partition_family::PartitionFamilyKvStore};

pub struct TtlSweepJob<S: PartitionFamilyKvStore> {
    provider: Arc<SortedKvDbStorageProvider<S>>,
}

impl<S: PartitionFamilyKvStore> TtlSweepJob<S> {
    pub fn new(provider: Arc<SortedKvDbStorageProvider<S>>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl<S: PartitionFamilyKvStore + 'static> BackgroundJob for TtlSweepJob<S> {
    async fn execute(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let work_done = self.provider.run_ttl_sweep().await?;
        Ok(work_done)
    }
}
