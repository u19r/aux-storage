//! Physical family and bounded load-sample storage inside the caller's attempt.
use std::{future::Future, sync::Arc};

use storage_common::partition::{PartitionLoadSampleRecord, ResolvedPartitionFamily};
use storage_types::{StorageResult, StreamName};

use super::PartitionReconciliationRepository;

pub trait PartitionReconciliationSource: Send + Sync + 'static {
    fn admitted_partition_reconciliation(
        self: Arc<Self>,
        _binding: Arc<crate::native_admission::NativeAdmissionBinding>,
    ) -> StorageResult<Option<Arc<dyn PartitionReconciliationRepository>>> {
        if self.partition_reconciliation().is_some() {
            return Err(storage_types::StorageError::unsupported(
                "partition source has no admitted native repository",
            ));
        }
        Ok(None)
    }

    fn partition_reconciliation(
        self: Arc<Self>,
    ) -> Option<Arc<dyn PartitionReconciliationRepository>>;
}

pub struct PartitionSamplePage<Fence> {
    pub family: Option<ResolvedPartitionFamily>,
    pub records: Vec<PartitionLoadSampleRecord>,
    pub more: bool,
    pub consumed: Fence,
}

pub trait PartitionPort: super::super::MaintenancePort {
    type SampleFence: Send;

    fn scan_partition_sample_stream(
        &mut self,
        after: Option<&StreamName>,
    ) -> impl Future<Output = StorageResult<Option<StreamName>>> + Send;
    fn load_partition_family(
        &mut self,
        stream: &StreamName,
    ) -> impl Future<Output = StorageResult<ResolvedPartitionFamily>> + Send;
    fn load_partition_samples(
        &mut self,
        stream: &StreamName,
    ) -> impl Future<Output = StorageResult<PartitionSamplePage<Self::SampleFence>>> + Send;
    fn store_partition_reconciliation(
        &mut self,
        stream: &StreamName,
        family: Option<&ResolvedPartitionFamily>,
        consumed: Self::SampleFence,
    ) -> impl Future<Output = StorageResult<()>> + Send;
    fn store_partition_samples(
        &mut self,
        stream: &StreamName,
        records: &[PartitionLoadSampleRecord],
    ) -> impl Future<Output = StorageResult<()>> + Send;
}
