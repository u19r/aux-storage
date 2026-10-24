use std::{
    num::{NonZeroU16, NonZeroU32},
    sync::Arc,
    time::Duration,
};

use queue_provider::QueueMessageCleanupHandler;
use scheduled_bg_jobs::{
    EffectMode, EnqueueOptions, FairnessClass, FairnessPolicy, JobCatalog, JobHandler, JobId,
    JobSpec, RetryPreset, RetrySelection, Timestamp,
};
use storage_backfill::GsiPageLimit;
use storage_common::{DatabaseJobIntervals, StorageJob};
use storage_provider::{
    StreamDurationTrimConfig, TableReclaimHandler, TableReclaimRepository,
    maintenance::{
        ExpirationHandler, GsiBuildHandler, PartitionReconciliationHandler, RetentionHandler,
    },
};
use storage_types::{StorageError, StorageResult};

use crate::{
    constants::{MAINTENANCE_JOB_SCHEMA_VERSION, QUEUE_MESSAGE_CLEANUP_PAGE_LIMIT},
    provider_bundle::MaintenanceRepositories,
};

pub(super) struct MaintenanceJobCatalog;

impl MaintenanceJobCatalog {
    pub(super) fn try_new(
        repositories: MaintenanceRepositories,
        table_reclaim_repository: Option<Arc<dyn TableReclaimRepository>>,
        intervals: DatabaseJobIntervals,
        seed_now: Timestamp,
    ) -> StorageResult<JobCatalog> {
        let mut catalog = repositories.job_catalog;
        let schema_version = NonZeroU16::new(MAINTENANCE_JOB_SCHEMA_VERSION)
            .ok_or_else(|| StorageError::validation("maintenance job schema version"))?;
        let registration = MaintenanceJobRegistration {
            intervals,
            schema_version,
            seed_now,
        };
        registration.register_job(&mut catalog, StorageJob::TtlSweep, |interval| {
            ExpirationHandler::try_new(repositories.expiration_repository, interval)
        })?;
        let page_limit = GsiPageLimit::try_from(256usize)
            .map_err(|_| StorageError::validation("invalid GSI maintenance page limit"))?;
        registration.register_job(&mut catalog, StorageJob::GsiBackfill, |interval| {
            GsiBuildHandler::try_new(repositories.gsi_build_repository, interval, page_limit)
        })?;
        if let Some(repository) = repositories.partition_reconciliation_repository {
            registration.register_job(
                &mut catalog,
                StorageJob::PartitionReconciliation,
                |interval| PartitionReconciliationHandler::try_new(repository, interval),
            )?;
        }
        if let Some(repository) = repositories.queue_message_cleanup_repository {
            let page_limit = NonZeroU32::new(QUEUE_MESSAGE_CLEANUP_PAGE_LIMIT)
                .ok_or_else(|| StorageError::internal("queue cleanup page limit is zero"))?;
            registration.register_job(
                &mut catalog,
                StorageJob::QueueMessageCleanup,
                |interval| QueueMessageCleanupHandler::try_new(repository, interval, page_limit),
            )?;
        }
        registration.register_job(&mut catalog, StorageJob::StreamTrim, |interval| {
            RetentionHandler::try_new(
                repositories.retention_repository,
                interval,
                StreamDurationTrimConfig::default(),
            )
        })?;
        if let Some(repository) = table_reclaim_repository {
            registration.register_job(&mut catalog, StorageJob::TableReclaim, |interval| {
                TableReclaimHandler::try_new(repository, interval)
            })?;
        }
        Ok(catalog)
    }
}

struct MaintenanceJobRegistration {
    intervals: DatabaseJobIntervals,
    schema_version: NonZeroU16,
    seed_now: Timestamp,
}

impl MaintenanceJobRegistration {
    fn register_job<H, F>(
        &self,
        catalog: &mut JobCatalog,
        job: StorageJob,
        build: F,
    ) -> StorageResult<()>
    where
        H: JobHandler,
        F: FnOnce(Duration) -> StorageResult<H>,
    {
        let interval = job.interval(self.intervals);
        let job_id = JobId::from(md5::compute(job.as_str().as_bytes()).0);
        let handler = build(interval)?;
        catalog
            .register_seeded(
                JobSpec::unit(job.kind(), self.schema_version, job.queue(), handler).policy(
                    EffectMode::Fenced,
                    FairnessPolicy::low_volume(),
                    RetrySelection::Preset(RetryPreset::Maintenance),
                    interval.max(Duration::from_secs(1)),
                ),
                (),
                EnqueueOptions::new(FairnessClass::low_volume())
                    .job_id(job_id)
                    .scheduled_at(self.seed_now),
                self.seed_now,
            )
            .map(|_| ())
            .map_err(|_| StorageError::validation("invalid maintenance catalog"))
    }
}
