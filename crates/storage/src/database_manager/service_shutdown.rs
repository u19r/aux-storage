use storage_types::{StorageError, StorageResult};

use super::DatabaseManager;
use crate::maintenance_runtime::MaintenanceDriver;

/// One ownership boundary for started services, shared by Arc-hosted callers.
#[derive(Default)]
pub(super) struct BackgroundServices {
    pub(super) maintenance: Vec<MaintenanceDriver>,
    pub(super) regional_servers: Vec<tokio::task::JoinHandle<()>>,
    /// SNS delivery runners (`MessagingServices::start_delivery`).
    pub(super) delivery: Vec<pubsub::DeliveryRunner>,
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
        servers: Vec<tokio::task::JoinHandle<()>>,
    ) -> StorageResult<()> {
        let Self::Running(services) = self else {
            return Err(StorageError::internal(
                "cannot start services during shutdown",
            ));
        };
        services
            .maintenance
            .extend(runtimes.into_iter().map(|runtime| runtime.start()));
        services.regional_servers.extend(servers);
        Ok(())
    }

    async fn shutdown(
        &mut self,
        admission: Vec<crate::admission::AdmissionController>,
    ) -> StorageResult<()> {
        *self = match std::mem::replace(self, Self::Stopped) {
            Self::Running(mut services) => Self::Draining(tokio::spawn(async move {
                let result = services.shutdown().await;
                futures::future::join_all(
                    admission
                        .iter()
                        .map(crate::admission::AdmissionController::wait_for_native_idle),
                )
                .await;
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
        if self.maintenance.iter().any(|driver| !driver.is_running()) {
            return Err(StorageError::internal("maintenance driver stopped"));
        }
        if self
            .regional_servers
            .iter()
            .any(tokio::task::JoinHandle::is_finished)
        {
            return Err(StorageError::internal("regional server stopped"));
        }
        Ok(())
    }

    /// One stop order for every task kind: regional servers, then the tasks
    /// that still settle native attempts (delivery runners, maintenance
    /// drivers). The caller then waits for native admission to go idle.
    async fn shutdown(&mut self) -> StorageResult<()> {
        let mut failure = None;
        for server in std::mem::take(&mut self.regional_servers) {
            server.abort();
        }
        for driver in &self.maintenance {
            driver.request_shutdown();
        }
        // Delivery stops claiming and settles its in-flight deliveries
        // (bounded by the request timeout and the settle margin) while the
        // maintenance drivers finish their turns.
        for runner in std::mem::take(&mut self.delivery) {
            if runner.shutdown().await.is_err() {
                failure.get_or_insert(StorageError::internal(
                    "pubsub delivery runner failed during shutdown",
                ));
            }
        }
        for driver in std::mem::take(&mut self.maintenance) {
            if let Err(error) = driver.shutdown().await {
                failure.get_or_insert(error);
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

    /// Hand a started delivery runner to the background services, which stop
    /// it in `shutdown_background_services` before native admission drains.
    /// During shutdown the runner is stopped at once and the call fails.
    pub(crate) async fn publish_delivery_runner(
        &self,
        runner: pubsub::DeliveryRunner,
    ) -> StorageResult<()> {
        let mut state = self.background_services.lock().await;
        if let BackgroundServiceState::Running(services) = &mut *state {
            services.delivery.push(runner);
            return Ok(());
        }
        drop(state);
        // The call fails either way; a runner that also fails to drain adds
        // nothing a caller can act on.
        let _ = runner.shutdown().await;
        Err(StorageError::internal(
            "cannot start services during shutdown",
        ))
    }

    /// Probe live task ownership without waiting behind service drainage.
    /// Remote peer lag does not affect local process health. A native store
    /// that stopped after a failed sync makes the process unhealthy.
    pub fn check_background_services(&self) -> StorageResult<()> {
        if self
            .connections()
            .filter_map(|connection| connection.durability.as_ref())
            .any(storage_common::durability::DurabilityHealth::is_stopped)
        {
            return Err(StorageError::from(
                storage_types::AttemptFailure::Unavailable(
                    storage_types::NativeUnavailable::DurabilityFailed,
                ),
            ));
        }
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

    /// Completes when a native store stops after a failed sync; the host then
    /// shuts the process down with a failure so that its supervisor restarts
    /// it and the startup barrier proves the disk. Never completes otherwise.
    pub async fn native_durability_failed(&self) {
        let stopped = self
            .connections()
            .filter_map(|connection| connection.durability.clone())
            .map(|health| Box::pin(health.stopped()))
            .collect::<Vec<_>>();
        if stopped.is_empty() {
            std::future::pending::<()>().await;
        }
        futures::future::select_all(stopped).await;
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
            .shutdown(
                self.connections()
                    .map(|connection| connection.admission.clone())
                    .collect(),
            )
            .await
    }
}
