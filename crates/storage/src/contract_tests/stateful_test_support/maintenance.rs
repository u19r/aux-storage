use std::{
    num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize},
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use bg_jobs_tokio::{
    JobRuntime,
    test_support::{TestClock, runtime_builder},
};
use queue_provider::{QueueMessageCleanupHandler, QueueMessageCleanupRepository};
use scheduled_bg_jobs::{
    DriveBudget, DriveBudgetInput, DriveStopReason, EffectMode, EnqueueOptions, FairnessClass,
    FairnessPolicy, JobCatalog, JobHandler, JobId, JobSpec, Namespace, OwnerId, QueueSelection,
    RetryPreset, RetrySelection, Timestamp, runtime::MonotonicDeadline,
};
use storage_backfill::GsiPageLimit;
use storage_common::StorageJob;
use storage_provider::maintenance::{
    ExpirationHandler, ExpirationRepository, ExpirationRequest, GsiBuildHandler, GsiBuildRepository,
};
use storage_types::{
    StorageError, StorageResult, TimestampMillis, context::WrappedError as _,
    maintenance::StepResult,
};

use super::{
    PendingFault,
    fault::{ExpirationFaults, FaultingExpirationRepository},
};
use crate::{
    ScheduledJobStore, ScheduledJobStoreError, constants::MAINTENANCE_DRIVER_MAX_STORE_OPERATIONS,
    provider_bundle::MaintenanceRepositories,
};

const CLOCK_START: Timestamp = Timestamp::from_millis(1_800_000_000_000);
const EXPIRATION_INTERVAL: Duration = Duration::from_secs(1);
const DRIVE_ADVANCE: Duration = Duration::from_secs(10);
const DRIVE_START_MAX_ATTEMPTS: usize = 64;
const QUEUE_CLEANUP_MAX_STEPS: usize = 512;

pub(super) struct MaintenanceHarness {
    store: ScheduledJobStore,
    repository: Arc<dyn ExpirationRepository>,
    expiration_namespace: Namespace,
    clock: TestClock,
    faults: Arc<ExpirationFaults>,
    paused_owner: Arc<JobRuntime<ScheduledJobStore>>,
    active_owner: Arc<JobRuntime<ScheduledJobStore>>,
    gsi_owner: Arc<JobRuntime<ScheduledJobStore>>,
    gsi_error: Arc<Mutex<Option<String>>>,
    queue_owner: Option<Arc<JobRuntime<ScheduledJobStore>>>,
    next_owner: u8,
}

impl MaintenanceHarness {
    pub(super) async fn prime_expiration_task(&self) -> StorageResult<()> {
        for _ in 0..DRIVE_START_MAX_ATTEMPTS {
            let outcome = self
                .repository
                .advance_expiration(ExpirationRequest {
                    destination: self.expiration_namespace.clone(),
                    job: JobId::from(md5::compute(StorageJob::TtlSweep.as_str().as_bytes()).0),
                    fence: NonZeroU64::MIN,
                    requested_cutoff: TimestampMillis::from_timestamp(1_800_000_000_000),
                    candidate_limit: NonZeroUsize::new(256).expect("positive fixture page limit"),
                })
                .await?;
            match outcome {
                StepResult::Complete => return Ok(()),
                StepResult::Advanced(()) => continue,
                outcome => {
                    return Err(Self::fixture_error(format!(
                        "expiration task did not seed cleanly: {outcome:?}"
                    )));
                }
            }
        }
        Err(Self::fixture_error(
            "expiration fixture did not finish its empty sweep".to_owned(),
        ))
    }

    pub(super) async fn try_new(
        store: ScheduledJobStore,
        repositories: MaintenanceRepositories,
    ) -> StorageResult<Self> {
        let expiration_namespace = Self::namespace(StorageJob::TtlSweep)?;
        let clock = TestClock::new(CLOCK_START, MonotonicDeadline::ZERO);
        let faults = Arc::new(ExpirationFaults::default());
        let MaintenanceRepositories {
            expiration_repository,
            gsi_build_repository,
            partition_reconciliation_repository: _,
            queue_message_cleanup_repository,
            retention_repository: _,
            job_catalog: _,
        } = repositories;
        let gsi_error = Arc::new(Mutex::new(None));
        let gsi_build_repository = Arc::new(DiagnosticGsiBuildRepository {
            repository: gsi_build_repository,
            last_error: Arc::clone(&gsi_error),
        });
        let repository = Arc::new(FaultingExpirationRepository::new(
            expiration_repository,
            Arc::clone(&faults),
        ));
        let paused_owner = Self::expiration_runtime(
            store.clone(),
            Arc::clone(&repository) as Arc<dyn ExpirationRepository>,
            expiration_namespace.clone(),
            clock.clone(),
            1,
        )?;
        let active_owner = Self::expiration_runtime(
            store.clone(),
            Arc::clone(&repository) as Arc<dyn ExpirationRepository>,
            expiration_namespace.clone(),
            clock.clone(),
            2,
        )?;
        let gsi_owner = Self::gsi_runtime(
            store.clone(),
            gsi_build_repository,
            Self::namespace(StorageJob::GsiBackfill)?,
            clock.clone(),
            3,
        )?;
        let queue_owner = queue_message_cleanup_repository
            .map(|repository| {
                Self::queue_runtime(
                    store.clone(),
                    repository,
                    Self::namespace(StorageJob::QueueMessageCleanup)?,
                    clock.clone(),
                    4,
                )
            })
            .transpose()?;
        paused_owner
            .ensure_seeds()
            .await
            .map_err(|error| Self::fixture_error(format!("seed expiration job: {error:?}")))?;
        gsi_owner
            .ensure_seeds()
            .await
            .map_err(|error| Self::fixture_error(format!("seed GSI job: {error:?}")))?;
        if let Some(queue_owner) = queue_owner.as_ref() {
            queue_owner.ensure_seeds().await.map_err(|error| {
                Self::fixture_error(format!("seed queue message cleanup job: {error:?}"))
            })?;
        }
        Ok(Self {
            store,
            repository,
            expiration_namespace,
            clock,
            faults,
            paused_owner,
            active_owner,
            gsi_owner,
            gsi_error,
            queue_owner,
            next_owner: 6,
        })
    }

    pub(super) async fn drive_ready_work(&self) -> StorageResult<()> {
        self.clock.advance(DRIVE_ADVANCE);
        let report = self
            .active_owner
            .drive_once()
            .await
            .map_err(|error| Self::drive_error("expiration", error))?;
        if report.counts.claimed == 0 {
            return Err(Self::fixture_error(format!(
                "expiration drive claimed no work: {report:?}"
            )));
        }
        Ok(())
    }

    pub(super) async fn shutdown_before_next_page(&self) -> StorageResult<()> {
        self.active_owner.request_shutdown();
        self.clock.advance(DRIVE_ADVANCE);
        let report = self
            .active_owner
            .drive_once()
            .await
            .map_err(|error| Self::drive_error("shutdown", error))?;
        if report.stop_reason != DriveStopReason::Shutdown || report.counts.claimed != 0 {
            return Err(Self::fixture_error(format!(
                "shutdown runtime started expiration work: {report:?}"
            )));
        }
        Ok(())
    }

    pub(super) async fn drive_gsi_until_idle(&self) -> StorageResult<()> {
        *self.gsi_error.lock().expect("GSI diagnostic lock") = None;
        let result = async {
            self.drive_one(&self.gsi_owner, "GSI maintenance").await?;
            self.drive_until_idle(&self.gsi_owner, 64, "GSI maintenance")
                .await
        }
        .await;
        match result {
            Ok(()) => Ok(()),
            Err(error) => {
                let handler_error = self.gsi_error.lock().expect("GSI diagnostic lock").take();
                Err(Self::fixture_error(format!(
                    "GSI drive failed: {error:?}; handler error: {handler_error:?}"
                )))
            }
        }
    }

    pub(super) async fn drive_queue_cleanup_once(&self) -> StorageResult<()> {
        let queue_owner = self.queue_owner.as_ref().ok_or_else(|| {
            StorageError::unsupported("queue message cleanup is not selected for this backend")
        })?;
        self.drive_one(queue_owner, "queue message cleanup").await
    }

    pub(super) async fn drive_queue_cleanup_until_idle(&self) -> StorageResult<()> {
        let queue_owner = self.queue_owner.as_ref().ok_or_else(|| {
            StorageError::unsupported("queue message cleanup is not selected for this backend")
        })?;
        self.drive_until_idle(
            queue_owner,
            QUEUE_CLEANUP_MAX_STEPS,
            "queue message cleanup",
        )
        .await
    }

    async fn drive_one(
        &self,
        owner: &Arc<JobRuntime<ScheduledJobStore>>,
        operation: &str,
    ) -> StorageResult<()> {
        self.clock.advance(DRIVE_ADVANCE);
        let mut last_report = None;
        for _ in 0..DRIVE_START_MAX_ATTEMPTS {
            let report = owner
                .drive_once()
                .await
                .map_err(|error| Self::drive_error(operation, error))?;
            if report.counts.retried > 0
                || report.counts.dead_lettered > 0
                || report.counts.cancelled > 0
            {
                return Err(Self::fixture_error(format!(
                    "{operation} did not complete successfully: {report:?}"
                )));
            }
            if report.counts.claimed > 0 {
                return Ok(());
            }
            if !report.has_immediately_visible_work {
                return Err(Self::fixture_error(format!(
                    "{operation} drive claimed no work: {report:?}"
                )));
            }
            last_report = Some(report);
            self.clock.advance(Duration::from_millis(1));
        }
        Err(Self::fixture_error(format!(
            "{operation} drive did not claim visible work within its bounded attempts: \
             {last_report:?}"
        )))
    }

    async fn drive_until_idle(
        &self,
        owner: &Arc<JobRuntime<ScheduledJobStore>>,
        max_steps: usize,
        operation: &str,
    ) -> StorageResult<()> {
        for _ in 0..max_steps {
            self.clock.advance(Duration::from_millis(1));
            let report = owner
                .drive_once()
                .await
                .map_err(|error| Self::drive_error(operation, error))?;
            if report.counts.retried > 0
                || report.counts.dead_lettered > 0
                || report.counts.cancelled > 0
            {
                return Err(Self::fixture_error(format!(
                    "{operation} did not complete successfully: {report:?}"
                )));
            }
            if report.counts.claimed == 0 {
                if report.has_immediately_visible_work {
                    continue;
                }
                return Ok(());
            }
        }
        Err(Self::fixture_error(format!(
            "{operation} did not become idle within its bounded steps"
        )))
    }

    pub(super) async fn pause_before_commit(&self) -> PendingFault {
        let gate = self.faults.arm_pause();
        let runtime = Arc::clone(&self.paused_owner);
        let drive = tokio::spawn(async move {
            runtime
                .drive_once()
                .await
                .map(|_| ())
                .map_err(|error| Self::drive_error("paused expiration", error))
        });
        gate.wait_until_entered().await;
        self.clock.advance(DRIVE_ADVANCE);
        PendingFault::new(gate, drive)
    }

    pub(super) fn lose_commit_ack(&self) {
        self.faults.arm_lost_ack();
    }

    pub(super) fn restart(&mut self) -> StorageResult<()> {
        self.active_owner = Self::expiration_runtime(
            self.store.clone(),
            Arc::clone(&self.repository),
            self.expiration_namespace.clone(),
            self.clock.clone(),
            self.next_owner,
        )?;
        self.next_owner = self.next_owner.saturating_add(1);
        Ok(())
    }

    fn drive_error(
        operation: &str,
        error: scheduled_bg_jobs::JobSystemError<ScheduledJobStoreError>,
    ) -> StorageError {
        Self::fixture_error(format!("drive {operation} job: {error:?}"))
    }

    fn fixture_error(context: String) -> StorageError {
        StorageError::internal("maintenance fixture drive failed").with_context(context)
    }

    fn namespace(job: StorageJob) -> StorageResult<Namespace> {
        Namespace::try_new(format!(
            "aux-storage:stateful-maintenance:{}:v1",
            job.as_str()
        ))
        .map_err(|error| StorageError::validation(error.to_string()))
    }

    fn gsi_runtime(
        store: ScheduledJobStore,
        repository: Arc<dyn GsiBuildRepository>,
        namespace: Namespace,
        clock: TestClock,
        owner: u8,
    ) -> StorageResult<Arc<JobRuntime<ScheduledJobStore>>> {
        let job = StorageJob::GsiBackfill;
        let job_id = JobId::from(md5::compute(job.as_str().as_bytes()).0);
        let page_limit = GsiPageLimit::try_from(1usize)
            .map_err(|error| StorageError::validation(error.to_string()))?;
        let handler = GsiBuildHandler::try_new(repository, EXPIRATION_INTERVAL, page_limit)?;
        Self::job_runtime(
            store,
            Self::catalog(job, job_id, handler)?,
            job,
            namespace,
            clock,
            owner,
        )
    }

    fn queue_runtime(
        store: ScheduledJobStore,
        repository: Arc<dyn QueueMessageCleanupRepository>,
        namespace: Namespace,
        clock: TestClock,
        owner: u8,
    ) -> StorageResult<Arc<JobRuntime<ScheduledJobStore>>> {
        let job = StorageJob::QueueMessageCleanup;
        let job_id = JobId::from(md5::compute(job.as_str().as_bytes()).0);
        let handler = QueueMessageCleanupHandler::try_new(
            repository,
            EXPIRATION_INTERVAL,
            NonZeroU32::new(1).expect("bounded cleanup page"),
        )?;
        Self::job_runtime(
            store,
            Self::catalog(job, job_id, handler)?,
            job,
            namespace,
            clock,
            owner,
        )
    }

    fn expiration_runtime(
        store: ScheduledJobStore,
        repository: Arc<dyn ExpirationRepository>,
        namespace: Namespace,
        clock: TestClock,
        owner: u8,
    ) -> StorageResult<Arc<JobRuntime<ScheduledJobStore>>> {
        let job = StorageJob::TtlSweep;
        let job_id = JobId::from(md5::compute(job.as_str().as_bytes()).0);
        let handler = ExpirationHandler::try_new(repository, EXPIRATION_INTERVAL)?;
        Self::job_runtime(
            store,
            Self::catalog(job, job_id, handler)?,
            job,
            namespace,
            clock,
            owner,
        )
    }

    fn job_runtime(
        store: ScheduledJobStore,
        catalog: JobCatalog,
        job: StorageJob,
        namespace: Namespace,
        clock: TestClock,
        owner: u8,
    ) -> StorageResult<Arc<JobRuntime<ScheduledJobStore>>> {
        let queues = QueueSelection::only(vec![job.queue()])
            .map_err(|error| StorageError::validation(error.to_string()))?;
        let budget = DriveBudget::try_from(DriveBudgetInput {
            queues,
            max_claims: 1,
            max_concurrency: 1,
            max_store_operations: MAINTENANCE_DRIVER_MAX_STORE_OPERATIONS,
            stop_claiming_after: Duration::from_secs(5),
            finish_after: Duration::from_secs(10),
        })
        .map_err(|error| StorageError::validation(error.to_string()))?;
        let runtime = runtime_builder(store, catalog, budget)
            .namespace(namespace)
            .owner(OwnerId::from([owner; OwnerId::BYTE_LENGTH]))
            .clock(Arc::new(clock))
            .build()
            .map_err(|error| StorageError::validation(error.to_string()))?;
        Ok(Arc::new(runtime))
    }

    fn catalog<H: JobHandler>(
        job: StorageJob,
        job_id: JobId,
        handler: H,
    ) -> StorageResult<JobCatalog> {
        let mut catalog = JobCatalog::new();
        catalog
            .register_seeded(
                JobSpec::unit(job.kind(), NonZeroU16::MIN, job.queue(), handler).policy(
                    EffectMode::Fenced,
                    FairnessPolicy::low_volume(),
                    RetrySelection::Preset(RetryPreset::Maintenance),
                    EXPIRATION_INTERVAL,
                ),
                (),
                EnqueueOptions::new(FairnessClass::low_volume())
                    .job_id(job_id)
                    .scheduled_at(CLOCK_START),
                CLOCK_START,
            )
            .map_err(|error| StorageError::validation(error.to_string()))?;
        Ok(catalog)
    }
}

struct DiagnosticGsiBuildRepository {
    repository: Arc<dyn GsiBuildRepository>,
    last_error: Arc<Mutex<Option<String>>>,
}

#[async_trait]
impl GsiBuildRepository for DiagnosticGsiBuildRepository {
    async fn advance_gsi_build(
        &self,
        request: storage_provider::maintenance::GsiBuildRequest,
    ) -> StorageResult<storage_types::maintenance::StepResult<()>> {
        let result = self.repository.advance_gsi_build(request).await;
        if let Err(error) = &result {
            let (kind, context) = error.recursive_context(Vec::new());
            *self.last_error.lock().expect("GSI diagnostic lock") =
                Some(format!("{kind:?}; context={context:?}"));
        }
        result
    }
}
