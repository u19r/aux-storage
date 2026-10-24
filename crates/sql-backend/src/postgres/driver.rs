#[cfg(feature = "fault-injection")]
use std::sync::Arc;
use std::{future::Future, panic::AssertUnwindSafe};

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use futures::{FutureExt as _, future::BoxFuture};
use storage_types::{StorageError, StorageResult};
use tokio_postgres::{Config, NoTls};

use super::{attempt::PostgresAttempt, network::build_tls_connector};
use crate::{AttemptMode, SqlAttempt, SqlBackend, SqlDataAccess, SqlEngine};

pub(super) enum PostgresTransaction {
    Snapshot,
    Serializable,
    SerializableData,
    Data(SqlDataAccess),
}

impl From<AttemptMode> for PostgresTransaction {
    fn from(mode: AttemptMode) -> Self {
        match mode {
            AttemptMode::ReadOnly => Self::Snapshot,
            AttemptMode::ReadWrite => Self::Serializable,
            AttemptMode::ReadWriteItems => Self::Data(SqlDataAccess::Write),
        }
    }
}

impl PostgresTransaction {
    pub(super) fn capability(&self) -> AttemptMode {
        match self {
            Self::Snapshot => AttemptMode::ReadOnly,
            Self::Serializable | Self::SerializableData | Self::Data(_) => AttemptMode::ReadWrite,
        }
    }

    pub(super) fn begin_sql(&self) -> &'static str {
        match self {
            Self::Snapshot => "START TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
            Self::Serializable => "START TRANSACTION ISOLATION LEVEL SERIALIZABLE",
            Self::SerializableData => {
                "START TRANSACTION ISOLATION LEVEL SERIALIZABLE; LOCK TABLE \
                 sys_dynamo_stream_records IN ROW EXCLUSIVE MODE"
            }
            Self::Data(SqlDataAccess::Write) => {
                "START TRANSACTION ISOLATION LEVEL READ COMMITTED; LOCK TABLE \
                 sys_dynamo_stream_records IN ROW EXCLUSIVE MODE"
            }
            Self::Data(SqlDataAccess::Exclusive) => {
                "START TRANSACTION ISOLATION LEVEL READ COMMITTED; LOCK TABLE \
                 sys_dynamo_stream_records IN SHARE MODE; LOCK TABLE sys_system_stream_scope IN \
                 SHARE ROW EXCLUSIVE MODE"
            }
        }
    }
}

/// Pool-backed driver. Every attempt keeps one exclusive client until native
/// settlement; a client with an unacknowledged settlement is discarded.
pub struct PostgresBackend {
    work_limits: std::sync::OnceLock<storage_types::NativeWorkLimits>,
    pool: Pool,
    #[cfg(feature = "fault-injection")]
    fault_probe: Option<Arc<dyn crate::SqlFaultProbe>>,
}

impl PostgresBackend {
    /// Open and check a pool using this provider's connection settings.
    /// Shared SQL catalog setup follows before the store is published.
    pub async fn initialize(dsn: &str, max_pool_size: usize, tls: bool) -> StorageResult<Self> {
        let config: Config = dsn
            .parse()
            .map_err(|_| StorageError::validation("invalid Postgres DSN"))?;
        if max_pool_size == 0 {
            return Err(StorageError::validation(
                "Postgres pool size must be positive",
            ));
        }
        let manager_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let pool = if tls {
            let connector = build_tls_connector()?;
            Pool::builder(Manager::from_config(config, connector, manager_config))
                .max_size(max_pool_size)
                .build()
        } else {
            Pool::builder(Manager::from_config(config, NoTls, manager_config))
                .max_size(max_pool_size)
                .build()
        }
        .map_err(|_| StorageError::internal("failed to build Postgres pool"))?;
        let client = pool
            .get()
            .await
            .map_err(|_| StorageError::internal("Postgres pool acquisition failed"))?;
        client
            .simple_query("SELECT 1")
            .await
            .map_err(super::error::map_driver_error)?;
        drop(client);
        Ok(Self {
            work_limits: Default::default(),
            pool,
            #[cfg(feature = "fault-injection")]
            fault_probe: None,
        })
    }

    #[cfg(feature = "fault-injection")]
    pub fn with_fault_probe(mut self, probe: Arc<dyn crate::SqlFaultProbe>) -> Self {
        self.fault_probe = Some(probe);
        self
    }

    async fn run_attempt<R, F>(
        pool: Pool,
        execution: (PostgresTransaction, Option<storage_types::NativeWorkLimits>),
        #[cfg(feature = "fault-injection")] fault_probe: Option<Arc<dyn crate::SqlFaultProbe>>,
        operation: F,
    ) -> StorageResult<R>
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut PostgresAttempt) -> BoxFuture<'a, StorageResult<R>>
            + Send
            + 'static,
    {
        let client = pool
            .get()
            .await
            .map_err(|_| StorageError::internal("Postgres pool acquisition failed"))?;
        let mut attempt = PostgresAttempt::new(
            client,
            execution,
            #[cfg(feature = "fault-injection")]
            fault_probe,
        );
        attempt.begin().await?;
        let outcome = AssertUnwindSafe(operation(&mut attempt))
            .catch_unwind()
            .await;
        match outcome {
            Ok(Ok(value)) => {
                attempt.commit().await?;
                Ok(value)
            }
            Ok(Err(error)) => {
                attempt.rollback().await?;
                Err(error.preserve_native_settlement(attempt.require_settlement()))
            }
            Err(panic) => {
                let _ = attempt.rollback().await;
                std::panic::resume_unwind(panic);
            }
        }
    }
}

impl SqlBackend for PostgresBackend {
    fn bind_work_limits(&self, limits: storage_types::NativeWorkLimits) -> StorageResult<()> {
        self.work_limits.set(limits).map_err(|_| {
            storage_types::StorageError::validation("native work budgets are already bound")
        })
    }

    type Attempt = PostgresAttempt;

    fn record_capacity(
        &self,
        limits: infrastructure_provider::limits::InfrastructureLimits,
    ) -> StorageResult<infrastructure_provider::physical_store::PhysicalRecordCapacity> {
        super::record_limits::qualify_record_protocol(limits)
    }

    fn attempt_capacity(&self) -> StorageResult<storage_types::NativeAttemptCapacity> {
        let connections =
            std::num::NonZeroUsize::new(self.pool.status().max_size).ok_or_else(|| {
                storage_types::StorageError::internal("initialized Postgres pool has no capacity")
            })?;
        Ok(storage_types::NativeAttemptCapacity::SharedPool { connections })
    }

    fn engine(&self) -> SqlEngine {
        SqlEngine::Postgres
    }

    fn with_attempt<R, F>(
        &self,
        mode: AttemptMode,
        operation: F,
    ) -> impl Future<Output = StorageResult<R>> + Send
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut Self::Attempt) -> BoxFuture<'a, StorageResult<R>>
            + Send
            + 'static,
    {
        let work_limits = self.work_limits.get().copied();
        let pool = self.pool.clone();
        #[cfg(feature = "fault-injection")]
        let fault_probe = self.fault_probe.clone();
        async move {
            crate::backend::supervise_attempt(
                Self::run_attempt(
                    pool,
                    (mode.into(), work_limits),
                    #[cfg(feature = "fault-injection")]
                    fault_probe,
                    operation,
                ),
                "Postgres attempt task stopped",
            )
            .await
        }
    }
    fn with_data_attempt<R, F>(
        &self,
        mode: AttemptMode,
        access: SqlDataAccess,
        operation: F,
    ) -> impl Future<Output = StorageResult<R>> + Send
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut Self::Attempt) -> BoxFuture<'a, StorageResult<R>>
            + Send
            + 'static,
    {
        let work_limits = self.work_limits.get().copied();
        let pool = self.pool.clone();
        #[cfg(feature = "fault-injection")]
        let fault_probe = self.fault_probe.clone();
        async move {
            crate::backend::supervise_attempt(
                Self::run_attempt(
                    pool,
                    (
                        match (mode, access) {
                            (AttemptMode::ReadOnly, _) => PostgresTransaction::Snapshot,
                            (AttemptMode::ReadWrite, SqlDataAccess::Write) => {
                                PostgresTransaction::SerializableData
                            }
                            (AttemptMode::ReadWriteItems, SqlDataAccess::Write) => {
                                PostgresTransaction::Data(access)
                            }
                            (
                                AttemptMode::ReadWrite | AttemptMode::ReadWriteItems,
                                SqlDataAccess::Exclusive,
                            ) => PostgresTransaction::Data(access),
                        },
                        work_limits,
                    ),
                    #[cfg(feature = "fault-injection")]
                    fault_probe,
                    operation,
                ),
                "Postgres attempt task stopped",
            )
            .await
        }
    }
}
