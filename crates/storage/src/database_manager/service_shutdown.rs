use storage_types::{StorageError, StorageResult};

use super::DatabaseManager;
use crate::maintenance_runtime::MaintenanceDriver;

/// One ownership boundary for started services, shared by Arc-hosted callers.
#[derive(Default)]
pub(super) struct BackgroundServices {
    pub(super) maintenance: Vec<MaintenanceDriver>,
    #[cfg(feature = "regional")]
    pub(super) regional: Vec<regional_replication::runtime::RegionalServices>,
    #[cfg(feature = "regional")]
    pub(super) pending_regional: Vec<regional_replication::runtime::AdmittedRegionalServices>,
}

pub(super) enum BackgroundServiceState {
    Running(BackgroundServices),
    Draining(tokio::task::JoinHandle<StorageResult<()>>),
    Stopped,
    Failed,
}

impl Default for BackgroundServiceState {
    fn default() -> Self {
        Self::Running(BackgroundServices::default())
    }
}

impl BackgroundServiceState {
    pub(super) fn publish_services(
        &mut self,
        runtimes: Vec<crate::maintenance_runtime::SeededMaintenanceRuntime>,
    ) -> StorageResult<()> {
        let Self::Running(services) = self else {
            return Err(StorageError::internal(
                "cannot start services during shutdown",
            ));
        };
        #[cfg(feature = "regional")]
        services.regional.extend(
            std::mem::take(&mut services.pending_regional)
                .into_iter()
                .map(|service| service.start()),
        );
        services
            .maintenance
            .extend(runtimes.into_iter().map(|runtime| runtime.start()));
        Ok(())
    }

    async fn shutdown(
        &mut self,
        admission: crate::admission::AdmissionRegistry,
    ) -> StorageResult<()> {
        *self = match std::mem::replace(self, Self::Stopped) {
            Self::Running(mut services) => Self::Draining(tokio::spawn(async move {
                let result = services.shutdown().await;
                admission.wait_for_native_idle().await;
                result
            })),
            Self::Draining(task) => Self::Draining(task),
            Self::Stopped => return Ok(()),
            Self::Failed => {
                *self = Self::Failed;
                return Err(StorageError::internal("background service shutdown failed"));
            }
        };
        let Self::Draining(task) = self else {
            return Err(StorageError::internal("missing service shutdown task"));
        };
        let result = task
            .await
            .map_err(|_| StorageError::internal("background service shutdown failed"))
            .and_then(|result| result);
        *self = if result.is_ok() {
            Self::Stopped
        } else {
            Self::Failed
        };
        result
    }
}

impl BackgroundServices {
    fn require_running(&self) -> StorageResult<()> {
        #[cfg(feature = "regional")]
        if !self.pending_regional.is_empty() {
            return Err(StorageError::internal(
                "regional services are awaiting initialization",
            ));
        }
        if self.maintenance.iter().any(|driver| !driver.is_running()) {
            return Err(StorageError::internal("maintenance driver stopped"));
        }
        #[cfg(feature = "regional")]
        if self.regional.iter().any(|services| !services.is_running()) {
            return Err(StorageError::internal("regional receiver stopped"));
        }
        Ok(())
    }

    async fn shutdown(&mut self) -> StorageResult<()> {
        let mut failure = None;
        #[cfg(feature = "regional")]
        self.pending_regional.clear();
        for driver in &self.maintenance {
            driver.request_shutdown();
        }
        #[cfg(feature = "regional")]
        for services in &self.regional {
            services.request_shutdown();
        }
        for driver in std::mem::take(&mut self.maintenance) {
            if let Err(error) = driver.shutdown().await {
                failure.get_or_insert(error);
            }
        }
        #[cfg(feature = "regional")]
        for services in std::mem::take(&mut self.regional) {
            if let Err(_error) = services.shutdown().await {
                failure.get_or_insert_with(|| {
                    storage_types::StorageError::internal(
                        "regional receiver failed during shutdown",
                    )
                });
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

impl DatabaseManager {
    #[cfg(feature = "test-support")]
    pub async fn stop_database_jobs_for_test(&self) -> StorageResult<()> {
        let mut state = self.background_services.lock().await;
        let BackgroundServiceState::Running(services) = &mut *state else {
            return Err(StorageError::internal("database services are stopping"));
        };
        for driver in &services.maintenance {
            driver.request_shutdown();
        }
        for driver in std::mem::take(&mut services.maintenance) {
            driver.shutdown().await?;
        }
        Ok(())
    }

    /// Probe live task ownership without waiting behind service drainage.
    /// Remote peer lag does not affect local process health.
    pub fn check_background_services(&self) -> StorageResult<()> {
        let state = self
            .background_services
            .try_lock()
            .map_err(|_| StorageError::internal("background services are changing state"))?;
        match &*state {
            BackgroundServiceState::Running(services) => services.require_running(),
            BackgroundServiceState::Draining(_)
            | BackgroundServiceState::Stopped
            | BackgroundServiceState::Failed => {
                Err(StorageError::internal("background services are stopping"))
            }
        }
    }

    /// The host stops public intake first. Serialize completion so concurrent
    /// callers cannot return while another caller is still draining services.
    /// Drain every configured connection after workers settle, including
    /// ordinary zero-reserve work. The retained task survives cancellation
    /// of its waiter.
    pub async fn shutdown_background_services(&self) -> StorageResult<()> {
        self.background_services
            .lock()
            .await
            .shutdown(self.admission_registry.clone())
            .await
    }
}
