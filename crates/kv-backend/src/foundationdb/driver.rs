use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use foundationdb::{
    Database,
    options::TransactionOption,
    tuple::{Bytes, Element, pack},
};
use futures::future::BoxFuture;
use storage_types::{StorageError, StorageResult, context::ErrorContext as _};
#[cfg(feature = "fault-injection")]
use tracing::{Instrument as _, instrument::WithSubscriber as _};

use super::{attempt::FoundationDbAttempt, error::map_operation_error, network::ensure_network};
use crate::{AttemptMode, KvBackend, KvTopology};

/// Native connection options; byte limits are enforced, not capability
/// assertions.
#[derive(Default)]
pub struct FoundationDbOptions<'a> {
    pub cluster_file: Option<&'a str>,
    pub subspace_prefix: Option<&'a [u8]>,
    pub tenant_name: Option<&'a [u8]>,
    pub cache_read_version_ms: u16,
    pub report_conflicting_keys: bool,
    pub transaction_byte_limit: Option<NonZeroUsize>,
}

#[derive(Clone, Copy, Default)]
struct NativeTransactionOptions {
    report_conflicting_keys: bool,
    byte_limit: Option<NativeTransactionByteLimit>,
}

#[derive(Clone, Copy)]
struct NativeTransactionByteLimit {
    bytes: NonZeroUsize,
    option: i64,
}

impl TryFrom<NonZeroUsize> for NativeTransactionByteLimit {
    type Error = StorageError;

    fn try_from(bytes: NonZeroUsize) -> StorageResult<Self> {
        if bytes.get() < crate::constants::FOUNDATIONDB_MIN_TRANSACTION_BYTES
            || bytes > crate::constants::FOUNDATIONDB_TRANSACTION_BYTES
        {
            return Err(StorageError::validation(
                "FoundationDB transaction byte limit is outside native bounds",
            ));
        }
        let option = i64::try_from(bytes.get()).map_err(|_| {
            StorageError::validation("FoundationDB transaction byte limit is invalid")
        })?;
        Ok(Self { bytes, option })
    }
}

impl FoundationDbOptions<'_> {
    fn require_transaction_options(&self) -> StorageResult<NativeTransactionOptions> {
        let byte_limit = self
            .transaction_byte_limit
            .map(NativeTransactionByteLimit::try_from)
            .transpose()?;
        Ok(NativeTransactionOptions {
            report_conflicting_keys: self.report_conflicting_keys,
            byte_limit,
        })
    }
}

impl NativeTransactionOptions {
    fn configure_transaction(self, transaction: &foundationdb::Transaction) -> StorageResult<()> {
        if self.report_conflicting_keys {
            transaction
                .set_option(TransactionOption::ReportConflictingKeys)
                .map_err(map_operation_error)?;
        }
        if let Some(limit) = self.byte_limit {
            transaction
                .set_option(TransactionOption::SizeLimit(limit.option))
                .map_err(map_operation_error)?;
        }
        Ok(())
    }
}

struct NativeAttemptInput {
    work_limits: Option<storage_types::NativeWorkLimits>,
    database: Arc<Database>,
    prefix: Arc<[u8]>,
    mode: AttemptMode,
    versionstamped: bool,
    options: NativeTransactionOptions,
    #[cfg(feature = "fault-injection")]
    fault_probe: Option<Arc<dyn crate::KvFaultProbe>>,
}

/// Cluster connection and durable namespace prefix. Each attempt creates its
/// own FDB transaction; the initialized store owns this driver.
pub struct FoundationDbBackend {
    work_limits: std::sync::OnceLock<storage_types::NativeWorkLimits>,
    database: Arc<Database>,
    prefix: Arc<[u8]>,
    transaction_options: NativeTransactionOptions,
    #[cfg(feature = "simulation")]
    simulator_owned: bool,
    #[cfg(feature = "fault-injection")]
    fault_probe: Option<Arc<dyn crate::KvFaultProbe>>,
}

impl FoundationDbBackend {
    /// Delete every physical key in this driver's namespace, including control
    /// records and value chunks. Test scope owners call this after all writers
    /// stop.
    #[cfg(feature = "test-support")]
    pub async fn delete_namespace_keys(&self) -> StorageResult<()> {
        let end = crate::range::prefix_upper_bound(&self.prefix)
            .ok_or_else(|| StorageError::internal("FoundationDB namespace has no upper bound"))?;
        let transaction = self.database.create_trx().map_err(map_operation_error)?;
        transaction.clear_range(&self.prefix, &end);
        transaction
            .commit()
            .await
            .map_err(super::error::map_commit_error)?;
        Ok(())
    }

    /// Observe the complete namespace rather than its logical table inventory.
    #[cfg(feature = "test-support")]
    pub async fn namespace_has_keys(&self) -> StorageResult<bool> {
        let end = crate::range::prefix_upper_bound(&self.prefix)
            .ok_or_else(|| StorageError::internal("FoundationDB namespace has no upper bound"))?;
        let transaction = self.database.create_trx().map_err(map_operation_error)?;
        let mut range = foundationdb::RangeOption::from((self.prefix.as_ref(), end.as_slice()));
        range.limit = Some(1);
        let keys = transaction
            .get_range(&range, 1, false)
            .await
            .map_err(map_operation_error)?;
        Ok(!keys.is_empty())
    }

    /// Open the production cluster. The network guard stays process-owned;
    /// callers may create more than one backend without starting it again.
    pub async fn initialize(options: FoundationDbOptions<'_>) -> StorageResult<Self> {
        let transaction_options = options.require_transaction_options()?;
        ensure_network(options.cache_read_version_ms).context("start FoundationDB network")?;
        let database = match options.cluster_file {
            Some(path) => Database::from_path(path),
            None => Database::default(),
        }
        .map_err(map_operation_error)
        .context("open FoundationDB database")?;
        let database = Arc::new(database);
        super::shutdown_observation::register_database(&database)?;
        let mut backend =
            Self::from_database(database, options.subspace_prefix, options.tenant_name);
        backend.transaction_options = transaction_options;
        backend.verify_connection().await?;
        Ok(backend)
    }

    async fn verify_connection(&self) -> StorageResult<()> {
        tokio::time::timeout(Duration::from_secs(10), async {
            let transaction = self
                .database
                .create_trx()
                .map_err(map_operation_error)
                .context("create FoundationDB startup transaction")?;
            self.transaction_options
                .configure_transaction(&transaction)?;
            transaction
                .get(b"__aux_healthcheck", false)
                .await
                .map_err(map_operation_error)
                .context("read FoundationDB startup healthcheck")?;
            Ok::<(), StorageError>(())
        })
        .await
        .map_err(|_| StorageError::internal("FoundationDB startup check timed out"))?
    }

    /// Borrow an existing native database while retaining production attempt
    /// supervision.
    #[must_use]
    fn from_database(
        database: Arc<Database>,
        subspace_prefix: Option<&[u8]>,
        tenant_name: Option<&[u8]>,
    ) -> Self {
        let subspace =
            subspace_prefix.map_or(Element::Nil, |prefix| Element::Bytes(Bytes::from(prefix)));
        let tenant = tenant_name.map_or(Element::Nil, |name| Element::Bytes(Bytes::from(name)));
        let prefix: Arc<[u8]> = pack(&(subspace, tenant)).into();
        Self {
            database,
            prefix,
            work_limits: Default::default(),
            transaction_options: NativeTransactionOptions::default(),
            #[cfg(feature = "simulation")]
            simulator_owned: false,
            #[cfg(feature = "fault-injection")]
            fault_probe: None,
        }
    }

    /// FoundationDB simulation polls native futures on its own deterministic
    /// executor. The workload owns the entire attempt; individual request
    /// cancellation is not modeled by this constructor. Production
    /// connections retain Tokio supervision.
    #[cfg(feature = "simulation")]
    #[must_use]
    pub fn from_simulation(
        database: Arc<Database>,
        subspace_prefix: Option<&[u8]>,
        tenant_name: Option<&[u8]>,
    ) -> Self {
        let mut backend = Self::from_database(database, subspace_prefix, tenant_name);
        backend.simulator_owned = true;
        backend
    }

    #[cfg(feature = "fault-injection")]
    pub fn with_fault_probe(mut self, probe: Arc<dyn crate::KvFaultProbe>) -> Self {
        self.fault_probe = Some(probe);
        self
    }

    async fn run_attempt<R, F>(
        input: NativeAttemptInput,
        operation: F,
    ) -> StorageResult<(R, Option<[u8; 10]>)>
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut FoundationDbAttempt) -> BoxFuture<'a, StorageResult<R>>
            + Send
            + 'static,
    {
        // The input database owner outlives this attempt and its native
        // futures, including when Tokio destroys a cancelled supervisor task.
        let transaction = input.database.create_trx().map_err(map_operation_error)?;
        input.options.configure_transaction(&transaction)?;
        let mut attempt = FoundationDbAttempt::new(
            transaction,
            input.prefix,
            (input.mode, input.work_limits),
            #[cfg(feature = "fault-injection")]
            input.fault_probe,
        );
        let value = operation(&mut attempt)
            .await
            .map_err(|error| error.preserve_native_settlement(attempt.require_settlement()))?;
        attempt.require_settlement()?;
        let stamp = match (input.mode, input.versionstamped) {
            (AttemptMode::ReadWrite | AttemptMode::ReadWriteItems, true) => {
                attempt.commit_versionstamped().await?
            }
            (AttemptMode::ReadWrite | AttemptMode::ReadWriteItems, false) => {
                attempt.commit().await?;
                None
            }
            (AttemptMode::ReadOnly, false) => None,
            (AttemptMode::ReadOnly, true) => {
                return Err(StorageError::internal("versionstamped read-only attempt"));
            }
        };
        Ok((value, stamp))
    }

    async fn supervise_attempt<R, F>(
        &self,
        mode: AttemptMode,
        versionstamped: bool,
        operation: F,
    ) -> StorageResult<(R, Option<[u8; 10]>)>
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut FoundationDbAttempt) -> BoxFuture<'a, StorageResult<R>>
            + Send
            + 'static,
    {
        let attempt = Self::run_attempt(
            NativeAttemptInput {
                work_limits: self.work_limits.get().copied(),
                database: Arc::clone(&self.database),
                prefix: Arc::clone(&self.prefix),
                mode,
                versionstamped,
                options: self.transaction_options,
                #[cfg(feature = "fault-injection")]
                fault_probe: self.fault_probe.clone(),
            },
            operation,
        );
        #[cfg(feature = "simulation")]
        if self.simulator_owned {
            return attempt.await;
        }
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
            Err(_) => Err(StorageError::internal(
                "FoundationDB attempt supervisor stopped",
            )),
        }
    }
}

impl KvBackend for FoundationDbBackend {
    fn bind_work_limits(&self, limits: storage_types::NativeWorkLimits) -> StorageResult<()> {
        self.work_limits
            .set(limits)
            .map_err(|_| StorageError::validation("native work budgets are already bound"))
    }

    type Attempt = FoundationDbAttempt;

    fn attempt_capacity(&self) -> StorageResult<storage_types::NativeAttemptCapacity> {
        Ok(storage_types::NativeAttemptCapacity::Independent)
    }

    fn topology(&self) -> KvTopology {
        KvTopology::Distributed
    }

    fn native_layout(&self) -> crate::KvNativeLayout {
        crate::KvNativeLayout {
            key_bytes: std::num::NonZeroUsize::new(crate::constants::FOUNDATIONDB_MAX_KEY_BYTES),
            value_bytes: std::num::NonZeroUsize::new(
                crate::constants::FOUNDATIONDB_NATIVE_VALUE_BYTES,
            ),
            transaction_bytes:
                infrastructure_provider::physical_store::NativeTransactionByteLimit::Fixed(
                    self.transaction_options
                        .byte_limit
                        .map_or(crate::constants::FOUNDATIONDB_TRANSACTION_BYTES, |limit| {
                            limit.bytes
                        }),
                ),
            prefix_bytes: self.prefix.len(),
            batch_metadata_bytes: 0,
            mutation_metadata_bytes: crate::constants::FOUNDATIONDB_MUTATION_METADATA_BYTES,
            conflict_ranges: crate::NativeConflictRangeLayout::CoalescedReadYourWrites {
                metadata_bytes: crate::constants::FOUNDATIONDB_CONFLICT_RANGE_METADATA_BYTES,
            },
        }
    }

    async fn with_attempt<R, F>(&self, mode: AttemptMode, operation: F) -> StorageResult<R>
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut Self::Attempt) -> BoxFuture<'a, StorageResult<R>>
            + Send
            + 'static,
    {
        // The supervisor keeps the transaction and commit future alive
        // after request cancellation, then reports its real outcome.
        self.supervise_attempt(mode, false, operation)
            .await
            .map(|(value, _)| value)
    }

    async fn with_versionstamped_attempt<R, F>(
        &self,
        operation: F,
    ) -> StorageResult<(R, Option<[u8; 10]>)>
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut Self::Attempt) -> BoxFuture<'a, StorageResult<R>>
            + Send
            + 'static,
    {
        self.supervise_attempt(AttemptMode::ReadWrite, true, operation)
            .await
    }
}
