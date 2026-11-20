use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use foundationdb::{
    Database,
    options::TransactionOption,
    tuple::{Bytes, Element, pack},
};
use futures::future::{BoxFuture, Either};
use storage_common::native_attempt_lifecycle::AttemptLifecycle;
use storage_types::{
    AttemptRequest, StorageError, StorageResult, context::ErrorContext as _, expired_in,
};

use super::{
    attempt::FoundationDbAttempt,
    error::map_operation_error,
    network::{NetworkPolicy, ensure_network},
};
use crate::{AttemptMode, KvBackend, KvTopology, native_deadline::NativeDeadline};

/// Native connection options; byte limits are enforced, not capability
/// assertions.
pub struct FoundationDbOptions<'a> {
    pub cluster_file: Option<&'a str>,
    pub subspace_prefix: Option<&'a [u8]>,
    pub tenant_name: Option<&'a [u8]>,
    pub cache_read_version_ms: u16,
    /// Process network `reply_byte_limit` knob; the first backend sets it.
    pub reply_byte_limit: u32,
    pub report_conflicting_keys: bool,
    pub transaction_byte_limit: Option<NonZeroUsize>,
    pub attempt_deadlines: storage_types::AttemptDeadlines,
}

impl Default for FoundationDbOptions<'_> {
    fn default() -> Self {
        Self {
            cluster_file: None,
            subspace_prefix: None,
            tenant_name: None,
            cache_read_version_ms: 0,
            reply_byte_limit: storage_types::FOUNDATIONDB_DEFAULT_REPLY_BYTE_LIMIT,
            report_conflicting_keys: false,
            transaction_byte_limit: None,
            attempt_deadlines: storage_types::AttemptDeadlines::default(),
        }
    }
}

#[derive(Clone, Copy, Default)]
pub(super) struct NativeTransactionOptions {
    report_conflicting_keys: bool,
    byte_limit: Option<NativeTransactionByteLimit>,
    /// The simulator's client has no shared GRV cache state, so the native
    /// option fails with `invalid_option` (2007). Simulation keeps the cached
    /// policy's validation (fresh rerun of a mutation-free commit) and reads a
    /// fresh version.
    #[cfg(feature = "simulation")]
    omit_native_grv_cache: bool,
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
            #[cfg(feature = "simulation")]
            omit_native_grv_cache: false,
        })
    }
}

impl NativeTransactionOptions {
    pub(super) fn configure_transaction(
        self,
        transaction: &foundationdb::Transaction,
    ) -> StorageResult<()> {
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
    #[cfg(test)]
    trace_label: Option<String>,
    #[cfg(test)]
    pinned_read_version: Option<i64>,
    pub(crate) database: Arc<Database>,
    pub(crate) prefix: Arc<[u8]>,
    request: AttemptRequest,
    deadline: NativeDeadline,
    versionstamped: bool,
    options: NativeTransactionOptions,
    fault_probe: Option<Arc<dyn crate::KvFaultProbe>>,
}

#[cfg(test)]
struct OperationTrace {
    label: String,
    attempts: usize,
}

/// The caller owns one serialized public operation, including preparation and
/// retries.
#[cfg(all(test, feature = "test-support", feature = "fault-injection"))]
pub(crate) struct OperationTraceGuard<'a>(&'a FoundationDbBackend);

#[cfg(all(test, feature = "test-support", feature = "fault-injection"))]
impl Drop for OperationTraceGuard<'_> {
    fn drop(&mut self) {
        use std::io::Write as _;
        let capture = self
            .0
            .operation_trace
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(capture) = capture {
            let _ = writeln!(
                std::io::stdout(),
                "cached_write_capture {} attempts={}",
                capture.label,
                capture.attempts
            );
        }
    }
}

/// Cluster connection and durable namespace prefix. Each attempt creates its
/// own FDB transaction; the initialized store owns this driver.
pub struct FoundationDbBackend {
    #[cfg(test)]
    trace_label: std::sync::Mutex<Option<String>>,
    #[cfg(test)]
    operation_trace: std::sync::Mutex<Option<OperationTrace>>,
    #[cfg(test)]
    pinned_read_version: std::sync::Mutex<Option<i64>>,
    work_limits: storage_types::BoundWorkLimits,
    pub(crate) database: Arc<Database>,
    pub(crate) prefix: Arc<[u8]>,
    transaction_options: NativeTransactionOptions,
    queue_wake: std::sync::OnceLock<Option<queue_provider::QueueWakeTasks>>,
    deadlines: storage_types::AttemptDeadlines,
    clock: Arc<dyn storage_types::Clock>,
    ordinary_write_read_version: storage_types::ReadVersionPolicy,
    #[cfg(feature = "simulation")]
    simulator_owned: bool,
    fault_probe: Option<Arc<dyn crate::KvFaultProbe>>,
    /// The lifecycle observer of every attempt; `None` in production.
    commit_observer: Option<Arc<dyn storage_common::native_observer::NativeObserver>>,
}

impl FoundationDbBackend {
    /// Pin one real SDK snapshot for native stale-attempt characterization.
    #[cfg(test)]
    pub(super) fn pin_next_read_version(&self, version: i64) {
        assert!(
            self.pinned_read_version
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .replace(version)
                .is_none()
        );
    }

    /// Label one native attempt in the owned network-count qualification only.
    #[cfg(all(test, feature = "test-support", feature = "fault-injection"))]
    pub(crate) fn trace_next_attempt(&self, label: &str) {
        let previous = self
            .trace_label
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(label.to_owned());
        assert!(previous.is_none(), "previous trace label was not consumed");
    }

    #[cfg(all(test, feature = "test-support", feature = "fault-injection"))]
    pub(crate) fn trace_operation(&self, label: &str) -> OperationTraceGuard<'_> {
        assert!(
            self.trace_label
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_none()
        );
        let previous = self
            .operation_trace
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(OperationTrace {
                label: label.to_owned(),
                attempts: 0,
            });
        assert!(previous.is_none(), "operation captures cannot overlap");
        OperationTraceGuard(self)
    }

    #[cfg(test)]
    fn take_attempt_label(&self, request: AttemptRequest) -> Option<String> {
        if let Some(capture) = self
            .operation_trace
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_mut()
        {
            capture.attempts += 1;
            assert!(
                capture.attempts <= 64,
                "bounded operation capture exceeded 64 attempts"
            );
            return Some(format!(
                "{}-{:?}-attempt-{:02}",
                capture.label, request.read_version, capture.attempts
            ));
        }
        self.trace_label
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    /// Delete every physical key in this driver's namespace, including control
    /// records and value chunks. Test scope owners call this after all writers
    /// stop.
    #[cfg(feature = "test-support")]
    pub async fn delete_namespace_keys(&self) -> StorageResult<()> {
        let end = crate::range::prefix_upper_bound(&self.prefix)
            .ok_or_else(|| StorageError::internal("FoundationDB namespace has no upper bound"))?;
        let mut transaction = self.database.create_trx().map_err(map_operation_error)?;
        loop {
            transaction.clear_range(&self.prefix, &end);
            match transaction.commit().await {
                Ok(_) => return Ok(()),
                // A blind range clear is idempotent: a retryable commit error
                // (1007 on a loaded cluster) runs it again.
                Err(error) => {
                    transaction = error.on_error().await.map_err(|error| {
                        storage_common::native_outcome::classify_commit(
                            true,
                            super::error::commit_error(error),
                        )
                    })?;
                }
            }
        }
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
        ensure_network(NetworkPolicy {
            cache_read_version_ms: options.cache_read_version_ms,
            reply_byte_limit: options.reply_byte_limit,
        })
        .context("start FoundationDB network")?;
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
        backend.ordinary_write_read_version = if options.cache_read_version_ms > 0 {
            storage_types::ReadVersionPolicy::Cached
        } else {
            storage_types::ReadVersionPolicy::Fresh
        };
        backend.transaction_options = transaction_options;
        backend.deadlines = options.attempt_deadlines;
        backend.verify_connection().await?;
        Ok(backend)
    }

    /// Budgets given at construction.
    #[must_use]
    pub fn attempt_deadlines(&self) -> storage_types::AttemptDeadlines {
        self.deadlines
    }

    async fn verify_connection(&self) -> StorageResult<()> {
        tokio::time::timeout(Duration::from_secs(10), async {
            // DisableClientBypass can report cluster_version_changed (1039)
            // on the first connection. The SDK retries only this read-only
            // startup observation; no customer mutation or unknown write is
            // replayed.
            let options = self.transaction_options;
            self.database
                .run(|transaction, _| async move {
                    options
                        .configure_transaction(&transaction)
                        .map_err(|error| {
                            foundationdb::FdbBindingError::CustomError(Box::new(error))
                        })?;
                    transaction.get(b"__aux_healthcheck", false).await?;
                    Ok(())
                })
                .await
                .map_err(|error| match error {
                    foundationdb::FdbBindingError::CustomError(error) => match error
                        .downcast::<StorageError>()
                    {
                        Ok(error) => *error,
                        Err(_) => StorageError::internal("FoundationDB startup observation failed"),
                    },
                    error => error
                        .get_fdb_error()
                        .map(map_operation_error)
                        .unwrap_or_else(|| {
                            StorageError::internal("FoundationDB startup observation failed")
                        }),
                })
                .context("read FoundationDB startup healthcheck")
        })
        .await
        .map_err(|_| StorageError::internal("FoundationDB startup check timed out"))?
    }

    /// Check the complete configured subspace before an inspection database
    /// is initialized. The operator must retain exclusive use after this read.
    pub async fn is_empty_subspace(&self) -> StorageResult<bool> {
        let options = self.transaction_options;
        let prefix = Arc::clone(&self.prefix);
        tokio::time::timeout(Duration::from_secs(10), async {
            self.database
                .run(|transaction, _| {
                    let prefix = Arc::clone(&prefix);
                    async move {
                        options
                            .configure_transaction(&transaction)
                            .map_err(|error| {
                                foundationdb::FdbBindingError::CustomError(Box::new(error))
                            })?;
                        let selector =
                            foundationdb::KeySelector::first_greater_or_equal(prefix.as_ref());
                        let key = transaction.get_key(&selector, true).await?;
                        Ok(!key.as_ref().starts_with(prefix.as_ref()))
                    }
                })
                .await
                .map_err(|error| match error {
                    foundationdb::FdbBindingError::CustomError(error) => {
                        match error.downcast::<StorageError>() {
                            Ok(error) => *error,
                            Err(_) => {
                                StorageError::internal("FoundationDB inspection observation failed")
                            }
                        }
                    }
                    error => error
                        .get_fdb_error()
                        .map(map_operation_error)
                        .unwrap_or_else(|| {
                            StorageError::internal("FoundationDB inspection observation failed")
                        }),
                })
        })
        .await
        .map_err(|_| StorageError::internal("FoundationDB inspection observation timed out"))?
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
            #[cfg(test)]
            trace_label: Default::default(),
            #[cfg(test)]
            operation_trace: Default::default(),
            #[cfg(test)]
            pinned_read_version: Default::default(),
            work_limits: Default::default(),
            queue_wake: Default::default(),
            transaction_options: NativeTransactionOptions::default(),
            deadlines: storage_types::AttemptDeadlines::default(),
            clock: Arc::new(storage_types::SystemClock),
            ordinary_write_read_version: storage_types::ReadVersionPolicy::Fresh,
            #[cfg(feature = "simulation")]
            simulator_owned: false,
            fault_probe: None,
            commit_observer: None,
        }
    }

    /// FoundationDB simulation polls native futures on its own deterministic
    /// executor. The workload owns the entire attempt; individual request
    /// cancellation is not modeled by this constructor. Production
    /// connections retain Tokio supervision. Attempt deadlines sleep on the
    /// workload's reactor-free clock.
    #[cfg(feature = "simulation")]
    #[must_use]
    pub fn from_simulation(
        database: Arc<Database>,
        subspace_prefix: Option<&[u8]>,
        tenant_name: Option<&[u8]>,
        read_version: storage_types::ReadVersionPolicy,
        clock: Arc<dyn storage_types::Clock>,
    ) -> Self {
        let mut backend = Self::from_database(database, subspace_prefix, tenant_name);
        backend.simulator_owned = true;
        backend.transaction_options.omit_native_grv_cache = true;
        backend.ordinary_write_read_version = read_version;
        backend.clock = clock;
        backend
    }

    /// Replace the attempt budgets of a simulation backend, for workloads
    /// that hold an attempt past the native read window on purpose.
    #[cfg(feature = "simulation")]
    #[must_use]
    pub fn with_attempt_deadlines(mut self, deadlines: storage_types::AttemptDeadlines) -> Self {
        self.deadlines = deadlines;
        self
    }

    #[cfg(feature = "fault-injection")]
    pub fn with_fault_probe(mut self, probe: Arc<dyn crate::KvFaultProbe>) -> Self {
        self.commit_observer = Some(crate::fault_injection::commit_observer(&probe));
        self.fault_probe = Some(probe);
        self
    }

    async fn run_attempt<R, F>(
        input: NativeAttemptInput,
        lifecycle: Arc<AttemptLifecycle>,
        operation: F,
    ) -> StorageResult<(R, Option<[u8; 10]>)>
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut FoundationDbAttempt) -> BoxFuture<'a, StorageResult<R>>
            + Send
            + 'static,
    {
        let deadline = input.deadline;
        deadline
            .deadline
            .require_remaining(storage_types::AttemptPhase::Acquire)?;
        if !lifecycle.open() {
            return Err(expired_in(storage_types::AttemptPhase::Acquire));
        }
        // The input database owner outlives this attempt and its native
        // futures, including when Tokio destroys a cancelled supervisor task.
        let transaction = input.database.create_trx().map_err(map_operation_error)?;
        input.options.configure_transaction(&transaction)?;
        transaction
            .set_option(TransactionOption::Timeout(deadline.native_timeout_ms()))
            .map_err(map_operation_error)?;
        // Fault builds report the ranges of a commit conflict.
        if input.fault_probe.is_some() && !input.options.report_conflicting_keys {
            transaction
                .set_option(TransactionOption::ReportConflictingKeys)
                .map_err(map_operation_error)?;
        }
        let cached = input.request.read_version == storage_types::ReadVersionPolicy::Cached;
        #[cfg(feature = "simulation")]
        let native_cached = cached && !input.options.omit_native_grv_cache;
        #[cfg(not(feature = "simulation"))]
        let native_cached = cached;
        if native_cached {
            transaction
                .set_option(TransactionOption::UseGrvCache)
                .map_err(map_operation_error)?;
        }
        #[cfg(test)]
        if let Some(version) = input.pinned_read_version {
            transaction.set_read_version(version);
        }
        #[cfg(test)]
        if let Some(label) = input.trace_label {
            super::native_cache_tests::trace_transaction(&transaction, &label);
        }
        let mut attempt = FoundationDbAttempt::new(
            transaction,
            input.prefix,
            (input.request.mode, input.work_limits),
            input.request.read_version,
            input.fault_probe,
        );
        // Expiry drops the callback and its native futures first; returning
        // then destroys the unsubmitted transaction.
        let value = {
            let callback = operation(&mut attempt);
            match futures::future::select(
                callback,
                std::pin::pin!(deadline.deadline.expired(deadline.clock.as_ref())),
            )
            .await
            {
                Either::Left((value, _)) => value,
                Either::Right(((), callback)) => {
                    drop(callback);
                    Err(expired_in(storage_types::AttemptPhase::Statement))
                }
            }
        };
        attempt.observe_failure(&value);
        let fresh_control = |error: StorageError| {
            if cached && error.attempt_kind() == Some(storage_types::AttemptKind::Conflict) {
                storage_types::AttemptFailure::FRESH_VALIDATION.into()
            } else {
                error
            }
        };
        let value = value.map_err(|error| {
            fresh_control(error.preserve_native_settlement(attempt.require_settlement()))
        })?;
        attempt.require_settlement().map_err(fresh_control)?;
        let stamp = match (input.request.mode, input.versionstamped) {
            (AttemptMode::ReadWrite | AttemptMode::ReadWriteItems, true) => {
                attempt.commit_versionstamped(&deadline, &lifecycle).await?
            }
            (AttemptMode::ReadWrite | AttemptMode::ReadWriteItems, false) => {
                let version = attempt
                    .commit(&deadline, &lifecycle)
                    .await
                    .map_err(fresh_control)?;
                if cached && version == -1 {
                    return Err(storage_types::AttemptFailure::FRESH_VALIDATION.into());
                }
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
        request: AttemptRequest,
        versionstamped: bool,
        operation: F,
    ) -> StorageResult<(R, Option<[u8; 10]>)>
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut FoundationDbAttempt) -> BoxFuture<'a, StorageResult<R>>
            + Send
            + 'static,
    {
        let attempt_class = request.class();
        let input = NativeAttemptInput {
            #[cfg(test)]
            trace_label: self.take_attempt_label(request),
            #[cfg(test)]
            pinned_read_version: self
                .pinned_read_version
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take(),
            work_limits: self.work_limits.get(),
            database: Arc::clone(&self.database),
            prefix: Arc::clone(&self.prefix),
            deadline: NativeDeadline {
                deadline: request.deadline,
                clock: Arc::clone(&self.clock),
            },
            request,
            versionstamped,
            options: self.transaction_options,
            fault_probe: self.fault_probe.clone(),
        };
        let attempt =
            move |lifecycle: Arc<AttemptLifecycle>| Self::run_attempt(input, lifecycle, operation);
        let observer = self.commit_observer.clone();
        #[cfg(feature = "simulation")]
        if self.simulator_owned {
            // The simulator runs attempts on its own executor without a
            // supervisor task; the attempt still has its lifecycle.
            return attempt(Arc::new(AttemptLifecycle::observed(observer))).await;
        }
        storage_common::native_supervision::supervise_attempt(
            storage_common::native_supervision::AttemptLabel {
                provider: "foundationdb",
                class: attempt_class,
            },
            observer,
            attempt,
            "FoundationDB attempt supervisor stopped",
        )
        .await
    }
}

impl KvBackend for FoundationDbBackend {
    fn attempt_deadlines(&self) -> storage_types::AttemptDeadlines {
        self.deadlines
    }

    fn ordinary_write_read_version_policy(&self) -> storage_types::ReadVersionPolicy {
        self.ordinary_write_read_version
    }

    fn bind_work_limits(&self, limits: storage_types::NativeWorkLimits) -> StorageResult<()> {
        self.work_limits.bind(limits)
    }

    type Attempt = FoundationDbAttempt;

    fn attempt_capacity(&self) -> StorageResult<storage_types::NativeAttemptCapacity> {
        Ok(storage_types::NativeAttemptCapacity::Independent)
    }

    fn topology(&self) -> KvTopology {
        KvTopology::Distributed
    }

    fn queue_wake_board(&self) -> Option<Arc<queue_provider::QueueWakeBoard>> {
        #[cfg(feature = "simulation")]
        if self.simulator_owned {
            return None;
        }
        self.queue_wake
            .get_or_init(|| {
                super::queue_wake::start(
                    &self.database,
                    Arc::clone(&self.prefix),
                    self.transaction_options,
                )
            })
            .as_ref()
            .map(queue_provider::QueueWakeTasks::board)
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

    async fn with_attempt<R, F>(&self, request: AttemptRequest, operation: F) -> StorageResult<R>
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut Self::Attempt) -> BoxFuture<'a, StorageResult<R>>
            + Send
            + 'static,
    {
        // `request.durability` needs no handling here. FoundationDB
        // acknowledges a commit only after the transaction logs made it
        // durable, and a read version never exceeds an acknowledged commit, so
        // every observation is durable and Relaxed would gain nothing.
        // The supervisor keeps the transaction and commit future alive
        // after request cancellation, then reports its real outcome.
        self.supervise_attempt(request, false, operation)
            .await
            .map(|(value, _)| value)
    }

    async fn with_versionstamped_attempt<R, F>(
        &self,
        request: AttemptRequest,
        operation: F,
    ) -> StorageResult<(R, Option<[u8; 10]>)>
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut Self::Attempt) -> BoxFuture<'a, StorageResult<R>>
            + Send
            + 'static,
    {
        // A cached read version needs a fresh rerun when the commit holds no
        // mutation; this path has none, and no caller sends one.
        if request.read_version == storage_types::ReadVersionPolicy::Cached {
            return Err(StorageError::internal(
                "versionstamped attempt with a cached read version",
            ));
        }
        self.supervise_attempt(request, true, operation).await
    }
}
