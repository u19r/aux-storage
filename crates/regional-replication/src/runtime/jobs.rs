use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::proposal::ControlProposal;
use scheduled_bg_jobs::{JobCatalog, JobHandle};
use storage_provider::DynamoPort;
use storage_types::{StorageError, StorageResult};

use super::node::RegionalRuntime;
use crate::{control::ControlJob, delivery::DeliveryJob};

pub struct RegionalJobHandles {
    pub enable: JobHandle<()>,
    pub addition: JobHandle<()>,
    pub delivery: JobHandle<()>,
    pub control: JobHandle<()>,
    pub reader_expiry: JobHandle<()>,
    pub archive: Option<JobHandle<()>>,
    pub source_capture: JobHandle<ControlProposal>,
    pub bootstrap_transfer: JobHandle<crate::bootstrap::BaselineCopyScope>,
}

/// Maintenance repositories bound to the same regional native admission owner.
pub struct RegionalJobRepositories {
    pub partition:
        Option<Arc<dyn storage_provider::maintenance::PartitionReconciliationRepository>>,
    pub queue: Arc<dyn queue_provider::QueueMessageCleanupRepository>,
}

impl<P: DynamoPort + 'static> RegionalRuntime<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
        + storage_provider::PrimaryStateScanAttempt
        + storage_provider::ItemTombstoneScanAttempt
{
    /// Register all handlers atomically in the host's shared catalog. The host
    /// owns scheduler identity, native job store, role selection, and driving.
    pub fn register_jobs(&self, catalog: &mut JobCatalog) -> StorageResult<RegionalJobHandles> {
        let delivery = DeliveryJob::try_new(
            Arc::clone(&self.port),
            Arc::clone(&self.journal),
            Arc::clone(&self.codec),
            self.transport.clone(),
        )
        .map_err(|_| StorageError::validation("invalid regional delivery registration"))?;
        let control = ControlJob::try_new(
            Arc::clone(&self.port),
            Arc::clone(&self.journal),
            Arc::clone(&self.codec),
            self.transport.clone(),
        )
        .map_err(|_| StorageError::validation("invalid regional control registration"))?;
        let reader_expiry = crate::history::expiry_job::ReaderExpiryJob::try_new(
            Arc::clone(&self.port),
            Arc::clone(&self.journal),
            self.transport.clone(),
        )
        .map_err(|_| StorageError::validation("invalid regional reader expiry registration"))?;
        let source_capture = crate::bootstrap::SourceCaptureJob::try_new(
            Arc::clone(&self.port),
            Arc::clone(&self.journal),
            Arc::clone(&self.codec),
            self.transport.clone(),
        )
        .map_err(|_| StorageError::validation("invalid regional source capture registration"))?;
        let bootstrap_transfer = crate::bootstrap::BootstrapTransferJob::try_new(
            Arc::clone(&self.port),
            Arc::clone(&self.journal),
            Arc::clone(&self.codec),
            self.transport.clone(),
        )
        .map_err(|_| {
            StorageError::validation("invalid regional bootstrap transfer registration")
        })?;
        let enable = crate::enable::EnableGlobalJob::try_new(
            Arc::clone(&self.port),
            Arc::clone(&self.journal),
            Arc::clone(&self.codec),
            self.transport.clone(),
        )
        .map_err(|_| StorageError::validation("invalid regional enable registration"))?;
        let addition = crate::addition::AdditionJob::try_new(
            Arc::clone(&self.port),
            Arc::clone(&self.journal),
            Arc::clone(&self.codec),
            self.transport.clone(),
        )
        .map_err(|_| StorageError::validation("invalid regional addition registration"))?;
        let archive = if self.journal.archive.get().is_some() {
            Some(
                crate::archive::ArchiveJob::try_new(
                    Arc::clone(&self.port),
                    Arc::clone(&self.journal),
                    Arc::clone(&self.codec),
                    self.transport.clone(),
                )
                .map_err(|_| StorageError::validation("invalid regional archive registration"))?,
            )
        } else {
            None
        };
        let mut staged = catalog.clone();
        let now = scheduled_bg_jobs::Timestamp::from_millis(self.port.now().timestamp_millis());
        let enable = enable.register(&mut staged, now).map_err(invalid_catalog)?;
        let addition = addition
            .register(&mut staged, now)
            .map_err(invalid_catalog)?;
        let delivery = delivery
            .register(&mut staged, now)
            .map_err(invalid_catalog)?;
        let control = control
            .register(&mut staged, now)
            .map_err(invalid_catalog)?;
        let reader_expiry = reader_expiry
            .register(&mut staged, now)
            .map_err(invalid_catalog)?;
        let source_capture = staged
            .register(source_capture.specification().map_err(invalid_catalog)?)
            .map_err(invalid_catalog)?;
        let bootstrap_transfer = staged
            .register(
                bootstrap_transfer
                    .specification()
                    .map_err(invalid_catalog)?,
            )
            .map_err(invalid_catalog)?;
        let archive = archive
            .map(|job| job.register(&mut staged, now))
            .transpose()
            .map_err(invalid_catalog)?;
        *catalog = staged;
        Ok(RegionalJobHandles {
            enable,
            addition,
            delivery,
            control,
            reader_expiry,
            source_capture,
            bootstrap_transfer,
            archive,
        })
    }
}

fn invalid_catalog(_: scheduled_bg_jobs::ValidationError) -> StorageError {
    StorageError::validation("invalid regional job catalog")
}

impl<
    P: DynamoPort
        + storage_provider::maintenance::PartitionReconciliationSource
        + queue_provider::QueuePort
        + 'static,
> RegionalRuntime<P>
{
    pub fn scheduled_repositories(&self) -> StorageResult<RegionalJobRepositories> {
        let partition = Arc::clone(&self.port)
            .admitted_partition_reconciliation(Arc::clone(&self.journal.native_admission))?;
        let queue = storage_provider::queue_cleanup_admission::admitted_queue_cleanup(
            Arc::clone(&self.port),
            Arc::clone(&self.journal.native_admission),
        );
        Ok(RegionalJobRepositories { partition, queue })
    }
}
