use std::sync::Arc;

use async_trait::async_trait;
use bg_jobs::BackgroundJob;

use crate::{SortedKvDbStorageProvider, partition_family::PartitionFamilyKvStore};

pub struct StreamTrimJob<S: PartitionFamilyKvStore> {
    provider: Arc<SortedKvDbStorageProvider<S>>,
}

impl<S: PartitionFamilyKvStore> StreamTrimJob<S> {
    pub fn new(provider: Arc<SortedKvDbStorageProvider<S>>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl<S: PartitionFamilyKvStore + 'static> BackgroundJob for StreamTrimJob<S> {
    async fn execute(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let work_done = self.provider.run_stream_trim().await?;
        Ok(work_done)
    }
}
