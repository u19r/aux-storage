//! One initialized SQL family store for all three native drivers.

use std::{future::Future, sync::Arc};

use futures::future::BoxFuture;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{
    DYNAMO_WRITE_RETRY, DynamoPort, TableMetadataCache, TableUpdateTransaction,
};
use storage_types::{
    AttemptMode, AttemptPlan, AttemptRequest, Clock, DeadlineClass, StorageResult, SystemClock,
    TimestampMillis, UpdateTableRequest, UpdateTableResponse,
};

use super::{SqlAttempt as _, SqlBackend, catalog::SqlCatalogAttempt};

/// Transaction advisory lock key of schema setup ("auxschm").
const SCHEMA_SETUP_LOCK: i64 = 0x0061_7578_7363_686d;

#[derive(Clone)]
enum SchemaStep {
    Ddl(String),
    Seed,
    Layout,
    StreamPositions,
}

pub struct SqlStore<B> {
    pub(crate) backend: Arc<B>,
    pub(crate) metadata_cache: Arc<TableMetadataCache>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) queue_wake: Option<Arc<queue_provider::QueueWakeBoard>>,
    pub(crate) queue_catalog_hints: Arc<queue_provider::QueueCatalogHints>,
    pub(crate) admission: storage_common::native_admission::AttemptAdmission,
}

impl<B> Clone for SqlStore<B> {
    fn clone(&self) -> Self {
        Self {
            backend: Arc::clone(&self.backend),
            metadata_cache: Arc::clone(&self.metadata_cache),
            clock: Arc::clone(&self.clock),
            queue_wake: self.queue_wake.clone(),
            queue_catalog_hints: Arc::clone(&self.queue_catalog_hints),
            admission: self.admission.clone(),
        }
    }
}

impl<B> storage_common::native_admission::AdmittedStore for SqlStore<B> {
    fn admitted(&self, admission: storage_common::native_admission::AttemptAdmission) -> Self {
        Self {
            admission,
            ..self.clone()
        }
    }
}

impl<B: SqlBackend> SqlStore<B> {
    /// Publish a store only after every current SQL family format is ready.
    /// Each DDL statement, the seed rows, and the record layout are separate
    /// short transactions; a long one would stop the Postgres stream frontier
    /// of the whole database. Postgres serializes concurrent setups of one
    /// database statement by statement with a transaction advisory lock.
    pub async fn initialize_schema(
        backend: B,
        initial_stream_lineages: storage_types::InitialStreamLineages,
        regional: bool,
    ) -> StorageResult<Self> {
        let engine = backend.engine();
        let mut steps = crate::data_schema::schema_statements(engine, regional)
            .await?
            .into_iter()
            .map(SchemaStep::Ddl)
            .collect::<Vec<_>>();
        steps.extend([SchemaStep::Seed, SchemaStep::Layout]);
        if engine == crate::SqlEngine::Postgres {
            steps.push(SchemaStep::StreamPositions);
        }
        for step in steps {
            // Numbering reads in READ COMMITTED: when it must raise the offset,
            // it locks out writers and its next statement sees the writers
            // that finished while it waited.
            let mode = if matches!(step, SchemaStep::StreamPositions) {
                AttemptMode::ReadWriteItems
            } else {
                AttemptMode::ReadWrite
            };
            retry_native_conflicts(
                &SystemClock,
                DYNAMO_WRITE_RETRY,
                backend.attempt_deadlines(),
                AttemptPlan::new(mode, DeadlineClass::SchemaChange),
                |attempt_request| {
                    let step = step.clone();
                    backend.with_attempt(attempt_request, move |attempt| {
                        Box::pin(async move {
                            if engine == crate::SqlEngine::Postgres {
                                attempt
                                    .query(
                                        crate::SqlStatement::new(
                                            format!(
                                                "SELECT count(*) FROM (SELECT \
                                                 pg_advisory_xact_lock({SCHEMA_SETUP_LOCK})) AS \
                                                 locked"
                                            ),
                                            vec![],
                                        ),
                                        crate::ReadProtection::Snapshot,
                                        storage_provider::CATALOG_READ_BYTES,
                                    )
                                    .await?;
                            }
                            match step {
                                SchemaStep::Ddl(sql) => {
                                    attempt
                                        .execute(crate::SqlStatement::new(sql, vec![]))
                                        .await?;
                                }
                                SchemaStep::Seed => {
                                    crate::data_schema::seed_data_schema(
                                        attempt,
                                        engine,
                                        initial_stream_lineages,
                                    )
                                    .await?;
                                }
                                SchemaStep::StreamPositions => {
                                    crate::stream_allocation::initialize_postgres_positions(
                                        attempt,
                                    )
                                    .await?;
                                }
                                SchemaStep::Layout => {
                                    crate::infrastructure::layout::initialize_layout(
                                        attempt, engine,
                                    )
                                    .await?;
                                }
                            }
                            Ok(())
                        })
                    })
                },
            )
            .await?;
        }
        let queue_wake = match backend.engine() {
            crate::SqlEngine::Postgres => backend.queue_wake_board(),
            _ => Some(queue_provider::QueueWakeBoard::local()),
        };
        Ok(Self {
            backend: Arc::new(backend),
            metadata_cache: Arc::new(TableMetadataCache::new()),
            clock: Arc::new(SystemClock),
            queue_wake,
            queue_catalog_hints: Arc::default(),
            admission: storage_common::native_admission::AttemptAdmission::Caller,
        })
    }

    /// Give native fault tests a second store with an independent metadata
    /// cache.
    #[cfg(feature = "fault-injection")]
    pub fn with_metadata_cache_ttl(mut self, ttl: std::time::Duration) -> Self {
        self.metadata_cache = Arc::new(TableMetadataCache::with_ttl(ttl));
        self
    }

    /// Override time for this store; normal construction uses the system clock.
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }
}

impl<B: SqlBackend> Clock for SqlStore<B> {
    fn now(&self) -> TimestampMillis {
        self.clock.now()
    }

    fn sleep(
        &self,
        duration: std::time::Duration,
    ) -> std::pin::Pin<Box<dyn Future<Output = StorageResult<()>> + Send + '_>> {
        self.clock.sleep(duration)
    }
}

impl<B: SqlBackend + 'static> storage_types::AttemptDeadlineSource for SqlStore<B> {
    fn attempt_deadlines(&self) -> storage_types::AttemptDeadlines {
        self.backend.attempt_deadlines()
    }
}

impl<B: SqlBackend + 'static> DynamoPort for SqlStore<B> {
    type PreparedWrites = crate::write_preparation::SqlPreparedWrites;
    fn prepare_put_items(
        &self,
        items: &[&storage_provider::BoundPutItem],
    ) -> StorageResult<Self::PreparedWrites> {
        Self::PreparedWrites::try_new(self.backend.engine(), items)
    }

    type Attempt<'native>
        = SqlCatalogAttempt<'native, B::Attempt>
    where Self: 'native;

    fn clock(&self) -> Arc<dyn Clock> {
        Arc::clone(&self.clock)
    }

    fn native_transaction_byte_limit(
        &self,
    ) -> infrastructure_provider::physical_store::NativeTransactionByteLimit {
        infrastructure_provider::physical_store::NativeTransactionByteLimit::Unknown
    }

    fn metadata_cache(&self) -> Arc<TableMetadataCache> {
        Arc::clone(&self.metadata_cache)
    }

    fn with_attempt<R, F>(
        &self,
        request: AttemptRequest,
        step: F,
    ) -> impl Future<Output = StorageResult<R>> + Send
    where
        R: Send + 'static,
        F: for<'native, 'borrow> FnOnce(
                &'borrow mut Self::Attempt<'native>,
            ) -> BoxFuture<'borrow, StorageResult<R>>
            + Send
            + 'static,
    {
        let request = request.with_default_scope(storage_types::AttemptScope::Data);
        let backend = Arc::clone(&self.backend);
        let engine = backend.engine();
        let attempt = async move {
            let complete_write = request.scope == storage_types::AttemptScope::ItemWrite;
            if complete_write {
                request.mode.require_write()?;
            }
            let (result, ddl) = backend
                .with_attempt(request, move |native| {
                    Box::pin(async move {
                        let mut attempt = if complete_write {
                            SqlCatalogAttempt::new_complete_write(native, engine, request)
                        } else {
                            SqlCatalogAttempt::new(native, engine, request)
                        };
                        let result = step(&mut attempt).await?;
                        attempt.flush_layout_ddl().await?;
                        Ok((result, attempt.deferred_ddl))
                    })
                })
                .await?;
            Self::execute_deferred_ddl(&backend, ddl).await?;
            Ok(result)
        };
        self.admission.settle(request, attempt)
    }

    async fn update_table_once(
        &self,
        attempt_request: AttemptRequest,
        request: UpdateTableRequest,
        captured_at: TimestampMillis,
        bindings: storage_provider::RequestTableBindings,
    ) -> StorageResult<UpdateTableResponse> {
        let backend = Arc::clone(&self.backend);
        let engine = backend.engine();
        let attempt = async move {
            backend
                .with_attempt(attempt_request, move |native| {
                    Box::pin(async move {
                        let mut attempt = SqlCatalogAttempt::new(native, engine, attempt_request);
                        let (_, response) = attempt
                            .execute_table_update(&request, captured_at, &bindings)
                            .await?
                            .into_result()?;
                        attempt.flush_layout_ddl().await?;
                        Ok(response)
                    })
                })
                .await
        };
        self.admission.settle(attempt_request, attempt).await
    }
}

impl<B: SqlBackend + 'static> storage_provider::maintenance::PartitionReconciliationSource
    for SqlStore<B>
{
    fn partition_reconciliation(
        self: Arc<Self>,
    ) -> Option<Arc<dyn storage_provider::maintenance::PartitionReconciliationRepository>> {
        None
    }
}

impl<B: SqlBackend> storage_common::native_admission::NativeAttemptSource for SqlStore<B> {
    fn bind_work_limits(&self, limits: storage_types::NativeWorkLimits) -> StorageResult<()> {
        self.backend.bind_work_limits(limits)
    }

    fn attempt_capacity(&self) -> StorageResult<storage_types::NativeAttemptCapacity> {
        self.backend.attempt_capacity()
    }

    fn durability_health(&self) -> Option<storage_common::durability::DurabilityHealth> {
        self.backend.durability_health()
    }
}
