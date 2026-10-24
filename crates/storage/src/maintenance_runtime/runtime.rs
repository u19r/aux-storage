use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bg_jobs_tokio::{JobRuntime, WaitOutcome};
use scheduled_bg_jobs::{
    DriveBudget, DriveBudgetInput, Namespace, OwnerId, QueueSelection, Timestamp,
};
use storage_common::DatabaseJobIntervals;
use storage_provider::TableReclaimRepository;
use storage_types::{StorageError, StorageResult};

use super::catalog::MaintenanceJobCatalog;
use crate::{
    ScheduledJobStore, ScheduledJobStoreError,
    constants::{
        MAINTENANCE_DRIVER_CLAIMS, MAINTENANCE_DRIVER_CONCURRENCY,
        MAINTENANCE_DRIVER_ERROR_WAKE_MS, MAINTENANCE_DRIVER_FINISH_WINDOW_MS,
        MAINTENANCE_DRIVER_MAX_STORE_OPERATIONS, MAINTENANCE_DRIVER_STOP_WINDOW_MS,
        MAINTENANCE_NAMESPACE_PREFIX,
    },
    provider_bundle::MaintenanceRepositories,
};

pub(crate) struct MaintenanceRuntime {
    runtime: Arc<JobRuntime<ScheduledJobStore>>,
}

pub(crate) struct SeededMaintenanceRuntime {
    runtime: Arc<MaintenanceRuntime>,
}

pub(crate) struct MaintenanceDriver {
    runtime: Arc<MaintenanceRuntime>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for MaintenanceDriver {
    fn drop(&mut self) {
        self.request_shutdown();
    }
}

impl MaintenanceDriver {
    pub(crate) fn request_shutdown(&self) {
        self.runtime.runtime.request_shutdown();
    }

    pub(crate) fn is_running(&self) -> bool {
        self.handle
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
    }

    pub(crate) async fn shutdown(mut self) -> StorageResult<()> {
        self.request_shutdown();
        if let Some(handle) = self.handle.take() {
            handle
                .await
                .map_err(|_| StorageError::internal("maintenance driver failed during shutdown"))?;
        }
        Ok(())
    }
}

impl MaintenanceRuntime {
    pub(crate) fn try_new(
        destination_namespace: &str,
        job_store: ScheduledJobStore,
        repositories: MaintenanceRepositories,
        table_reclaim_repository: Option<Arc<dyn TableReclaimRepository>>,
        intervals: DatabaseJobIntervals,
    ) -> StorageResult<Arc<Self>> {
        let namespace = Self::namespace(destination_namespace)?;
        let catalog = MaintenanceJobCatalog::try_new(
            repositories,
            table_reclaim_repository,
            intervals,
            Self::wall_now(),
        )?;
        let owner = OwnerId::generate()
            .map_err(|_| StorageError::internal("maintenance owner generation failed"))?;
        let budget = Self::drive_profile(job_store.capabilities().max_transaction_duration())?;
        let runtime = if job_store.capabilities().is_distributed() {
            JobRuntime::distributed(job_store, catalog, budget, namespace, owner)
        } else {
            JobRuntime::local(job_store, catalog, budget, namespace, owner)
        }
        .map_err(|_| StorageError::validation("invalid maintenance runtime"))?;
        Ok(Arc::new(Self {
            runtime: Arc::new(runtime),
        }))
    }

    pub(crate) async fn initialize_seeds(
        self: Arc<Self>,
    ) -> StorageResult<SeededMaintenanceRuntime> {
        self.runtime
            .ensure_seeds()
            .await
            .map_err(Self::map_seed_error)?;
        Ok(SeededMaintenanceRuntime { runtime: self })
    }

    async fn drive(&self) {
        loop {
            let Err(error) = self.runtime.run_until_shutdown().await else {
                break;
            };
            tracing::warn!(error = ?error, "storage maintenance drive failed");
            let wait = self
                .runtime
                .wait_after_error(Duration::from_millis(MAINTENANCE_DRIVER_ERROR_WAKE_MS))
                .await;
            if !matches!(wait, Ok(WaitOutcome::Woken)) {
                break;
            }
        }
    }

    fn namespace(destination: &str) -> StorageResult<Namespace> {
        Namespace::try_new(format!("{MAINTENANCE_NAMESPACE_PREFIX}:{destination}:v1"))
            .map_err(|_| StorageError::validation("invalid maintenance namespace"))
    }

    fn drive_profile(store_duration: Duration) -> StorageResult<DriveBudget> {
        DriveBudget::try_from(DriveBudgetInput {
            queues: QueueSelection::all(),
            max_claims: MAINTENANCE_DRIVER_CLAIMS,
            max_concurrency: MAINTENANCE_DRIVER_CONCURRENCY,
            max_store_operations: MAINTENANCE_DRIVER_MAX_STORE_OPERATIONS,
            stop_claiming_after: Duration::from_millis(MAINTENANCE_DRIVER_STOP_WINDOW_MS)
                .max(store_duration.saturating_mul(2)),
            finish_after: Duration::from_millis(MAINTENANCE_DRIVER_FINISH_WINDOW_MS)
                .max(store_duration.saturating_mul(4)),
        })
        .map_err(|_| StorageError::validation("invalid maintenance drive profile"))
    }

    fn map_seed_error(
        error: scheduled_bg_jobs::JobSystemError<ScheduledJobStoreError>,
    ) -> StorageError {
        let message = "maintenance seed initialization failed";
        let detail = match error {
            scheduled_bg_jobs::JobSystemError::Store(error) => format!("Store({error})"),
            error => format!("{error:?}"),
        };
        StorageError::internal(message).with_context(format!("{message}: {detail}"))
    }

    fn wall_now() -> Timestamp {
        let milliseconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_millis());
        Timestamp::from_millis(i64::try_from(milliseconds).unwrap_or(i64::MAX))
    }
}

impl SeededMaintenanceRuntime {
    pub(crate) fn start(self) -> MaintenanceDriver {
        let runtime = Arc::clone(&self.runtime);
        let handle = tokio::spawn(async move { runtime.drive().await });
        MaintenanceDriver {
            runtime: self.runtime,
            handle: Some(handle),
        }
    }
}
