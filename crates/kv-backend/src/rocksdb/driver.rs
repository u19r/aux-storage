use std::{fs, future::Future, path::PathBuf, sync::Arc};

use futures::future::BoxFuture;
use rocksdb::{OptimisticTransactionDB, Options};
use storage_common::durability::DurabilityBatch;
use storage_types::{StorageError, StorageResult};
use tokio::sync::{mpsc, oneshot};
#[cfg(feature = "fault-injection")]
use tracing::{Instrument as _, instrument::WithSubscriber as _};

use super::{
    attempt::RocksDbAttempt,
    error::map_operation_error,
    transaction::drive_attempt,
    writer::{RocksDbWriter, WriteAttempt},
};
use crate::{AttemptMode, KvAttempt, KvBackend, KvTopology, constants::BACKEND_HEALTH_KEY};

/// One DB handle. Native writers share a queue because RocksDB's
/// optimistic transactions do not protect range phantoms.
pub struct RocksDbBackend {
    work_limits: std::sync::OnceLock<storage_types::NativeWorkLimits>,
    database: Arc<OptimisticTransactionDB>,
    writer: Arc<RocksDbWriter>,
    durability: Arc<DurabilityBatch>,
    #[cfg(feature = "fault-injection")]
    fault_probe: Option<Arc<dyn crate::KvFaultProbe>>,
}

enum NativeWorker {
    Read(tokio::task::JoinHandle<StorageResult<()>>),
    Write(oneshot::Receiver<StorageResult<()>>),
}

impl NativeWorker {
    async fn finish(self) -> StorageResult<()> {
        match self {
            Self::Read(worker) => worker
                .await
                .map_err(|_| StorageError::internal("RocksDB worker stopped"))?,
            Self::Write(completion) => completion
                .await
                .map_err(|_| StorageError::internal("RocksDB worker stopped"))?,
        }
    }
}

impl RocksDbBackend {
    pub fn initialize(path: PathBuf) -> StorageResult<Self> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent)
                .map_err(|_| StorageError::internal("create RocksDB parent directory failed"))?;
        }
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_enable_pipelined_write(true);
        options.set_log_level(rocksdb::LogLevel::Warn);
        options.set_use_fsync(true);
        let database = OptimisticTransactionDB::open(&options, path)
            .map_err(|_| StorageError::internal("open RocksDB failed"))?;
        database
            .get(BACKEND_HEALTH_KEY)
            .map_err(map_operation_error)?;
        let database = Arc::new(database);
        let sync_database = Arc::downgrade(&database);
        let durability = Arc::new(DurabilityBatch::new(move || {
            let database = sync_database.clone();
            async move {
                let database = database.upgrade().ok_or_else(|| {
                    StorageError::sync_submission_ambiguous(None, "RocksDB closed before WAL sync")
                })?;
                tokio::task::spawn_blocking(move || database.flush_wal(true))
                    .await
                    .map_err(|_| {
                        StorageError::sync_submission_ambiguous(
                            None,
                            "RocksDB WAL sync worker stopped",
                        )
                    })?
                    .map_err(|_| {
                        StorageError::sync_submission_ambiguous(
                            None,
                            "RocksDB WAL sync was not acknowledged",
                        )
                    })
            }
        }));
        Ok(Self {
            work_limits: Default::default(),
            database,
            durability,
            writer: RocksDbWriter::try_new()?,
            #[cfg(feature = "fault-injection")]
            fault_probe: None,
        })
    }

    #[cfg(feature = "fault-injection")]
    pub fn with_fault_probe(mut self, probe: Arc<dyn crate::KvFaultProbe>) -> Self {
        self.fault_probe = Some(probe);
        self
    }

    async fn run_attempt<R, F>(
        database: Arc<OptimisticTransactionDB>,
        writer: Arc<RocksDbWriter>,
        execution: (AttemptMode, Option<storage_types::NativeWorkLimits>),
        #[cfg(feature = "fault-injection")] fault_probe: Option<Arc<dyn crate::KvFaultProbe>>,
        operation: F,
    ) -> StorageResult<R>
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut RocksDbAttempt) -> BoxFuture<'a, StorageResult<R>>
            + Send
            + 'static,
    {
        let (mode, limits) = execution;
        let (sender, receiver) = mpsc::unbounded_channel();
        let settlement = Arc::new(storage_types::NativeAttemptSettlement::new(limits));
        let native_settlement = Arc::clone(&settlement);
        let worker = match mode {
            AttemptMode::ReadOnly => NativeWorker::Read(tokio::task::spawn_blocking(move || {
                drive_attempt(database, mode, receiver, native_settlement)
            })),
            AttemptMode::ReadWrite | AttemptMode::ReadWriteItems => {
                let (settled, completion) = oneshot::channel();
                writer.submit_attempt(WriteAttempt {
                    database,
                    mode,
                    commands: receiver,
                    settlement: native_settlement,
                    settled,
                })?;
                NativeWorker::Write(completion)
            }
        };
        let mut attempt = RocksDbAttempt::new(
            sender,
            mode,
            settlement,
            #[cfg(feature = "fault-injection")]
            fault_probe,
        );
        let result = operation(&mut attempt)
            .await
            .map_err(|error| error.preserve_native_settlement(attempt.settlement().require_open()));
        let settled = attempt.finish(result.is_ok()).await;
        let joined = worker.finish().await;
        settled?;
        joined?;
        result
    }
}

impl KvBackend for RocksDbBackend {
    fn bind_work_limits(&self, limits: storage_types::NativeWorkLimits) -> StorageResult<()> {
        self.work_limits
            .set(limits)
            .map_err(|_| StorageError::validation("native work budgets are already bound"))
    }

    type Attempt = RocksDbAttempt;

    fn attempt_capacity(&self) -> StorageResult<storage_types::NativeAttemptCapacity> {
        Ok(storage_types::NativeAttemptCapacity::SerializedWriter)
    }

    fn topology(&self) -> KvTopology {
        KvTopology::Local
    }

    fn native_layout(&self) -> crate::KvNativeLayout {
        crate::KvNativeLayout {
            key_bytes: None,
            value_bytes: None,
            transaction_bytes:
                infrastructure_provider::physical_store::NativeTransactionByteLimit::NoFixedCeiling,
            prefix_bytes: 0,
            batch_metadata_bytes: crate::constants::ROCKSDB_BATCH_METADATA_BYTES,
            mutation_metadata_bytes: crate::constants::ROCKSDB_MUTATION_METADATA_BYTES,
            conflict_ranges: crate::NativeConflictRangeLayout::NotCharged,
        }
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
        let database = Arc::clone(&self.database);
        let writer = self.writer.clone();
        let durability = Arc::clone(&self.durability);
        #[cfg(feature = "fault-injection")]
        let fault_probe = self.fault_probe.clone();
        #[cfg(feature = "fault-injection")]
        let commit_probe = fault_probe.clone();
        async move {
            // The supervisor survives cancellation of the public request and
            // waits for native settlement and durable acknowledgement.
            let attempt = async move {
                // Keep the native handle alive through acknowledgement, without
                // an idle durability worker retaining the RocksDB file lock
                // after store drop.
                let durable_database = Arc::clone(&database);
                let value = Self::run_attempt(
                    database,
                    writer,
                    (mode, work_limits),
                    #[cfg(feature = "fault-injection")]
                    fault_probe,
                    operation,
                )
                .await?;
                if mode != AttemptMode::ReadOnly {
                    durability.wait().await?;
                    #[cfg(feature = "fault-injection")]
                    crate::fault_injection::KvCommitBoundary::AfterDurableAcknowledgement
                        .observe(commit_probe.as_deref())
                        .await?;
                }
                drop(durable_database);
                Ok(value)
            };
            let attempt = metrics_facade::inherit_request_cost(attempt);
            // Scoped test subscribers do not cross Tokio task boundaries.
            #[cfg(feature = "fault-injection")]
            let attempt = attempt
                .instrument(tracing::Span::current())
                .with_current_subscriber();
            let task = tokio::spawn(attempt);
            match task.await {
                Ok(result) => result,
                Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
                Err(_) => Err(StorageError::internal("RocksDB attempt supervisor stopped")),
            }
        }
    }

    async fn with_versionstamped_attempt<R, F>(
        &self,
        _operation: F,
    ) -> StorageResult<(R, Option<[u8; 10]>)>
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut Self::Attempt) -> BoxFuture<'a, StorageResult<R>>
            + Send
            + 'static,
    {
        Err(StorageError::unsupported(
            "RocksDB has no native commit versionstamp",
        ))
    }
}
