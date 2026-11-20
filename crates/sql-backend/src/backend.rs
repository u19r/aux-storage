use std::{borrow::Cow, fmt::Write as _, future::Future, num::NonZeroUsize};

use futures::future::BoxFuture;
pub use storage_types::{AttemptMode, ReadProtection};
use storage_types::{AttemptPlan, AttemptRequest, DeadlineClass, KeyAttributeType, StorageResult};

use crate::{SqlQueryPage, SqlStatement};

/// Native SQL engine. The SQL family derives placeholder and DDL rendering
/// from this one enum instead of maintaining separate dialect/config types.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SqlEngine {
    Sqlite,
    Turso,
    Postgres,
}

impl SqlEngine {
    #[must_use]
    pub fn placeholder(self, index: usize) -> Cow<'static, str> {
        let cached = match self {
            Self::Postgres => crate::constants::POSTGRES_PLACEHOLDERS.get(index),
            Self::Sqlite | Self::Turso => crate::constants::SQLITE_PLACEHOLDERS.get(index),
        };
        if let Some(value) = cached {
            return Cow::Borrowed(value);
        }
        let prefix = self.placeholder_prefix();
        Cow::Owned(format!("{prefix}{index}"))
    }

    #[must_use]
    pub(crate) fn numeric_placeholder(self, index: usize) -> Cow<'static, str> {
        if self == Self::Postgres
            && let Some(value) = crate::constants::POSTGRES_NUMERIC_PLACEHOLDERS.get(index)
        {
            return Cow::Borrowed(value);
        }
        Cow::Owned(format!(
            "CAST({} AS TEXT)::NUMERIC",
            self.placeholder(index)
        ))
    }

    #[must_use]
    pub fn placeholders(self, count: usize) -> String {
        let mut result = String::with_capacity(count.saturating_mul(5));
        for index in 1..=count {
            if index > 1 {
                result.push_str(", ");
            }
            let _ = write!(result, "{}{index}", self.placeholder_prefix());
        }
        result
    }

    pub(crate) fn placeholder_prefix(self) -> char {
        match self {
            Self::Postgres => '$',
            Self::Sqlite | Self::Turso => '?',
        }
    }

    #[must_use]
    pub fn text_type(self) -> &'static str {
        match self {
            Self::Sqlite => "TEXT COLLATE BINARY",
            Self::Postgres => "TEXT COLLATE \"C\"",
            Self::Turso => "TEXT",
        }
    }

    #[must_use]
    pub fn integer_type(self) -> &'static str {
        match self {
            Self::Postgres => "BIGINT",
            Self::Sqlite | Self::Turso => "INTEGER",
        }
    }

    #[must_use]
    pub fn blob_type(self) -> &'static str {
        match self {
            Self::Postgres => "BYTEA",
            Self::Sqlite | Self::Turso => "BLOB",
        }
    }

    #[must_use]
    pub fn boolean_column(self, column: &str) -> String {
        match self {
            Self::Postgres => "BOOLEAN NOT NULL DEFAULT FALSE".to_owned(),
            Self::Sqlite | Self::Turso => {
                format!("INTEGER NOT NULL DEFAULT 0 CHECK ({column} IN (0, 1))")
            }
        }
    }

    #[must_use]
    pub fn table_suffix(self) -> &'static str {
        match self {
            Self::Sqlite => " WITHOUT ROWID",
            Self::Turso | Self::Postgres => "",
        }
    }

    #[must_use]
    pub fn key_column_type(self, key_type: KeyAttributeType) -> &'static str {
        match key_type {
            KeyAttributeType::S => self.text_type(),
            KeyAttributeType::N if matches!(self, Self::Postgres) => "NUMERIC",
            KeyAttributeType::N => self.text_type(),
            KeyAttributeType::B if matches!(self, Self::Postgres) => "BYTEA",
            KeyAttributeType::B => "BLOB",
        }
    }
}

/// Original input position: zero is the observation, following reads start at
/// one.
pub struct SqlReadBatchFailure {
    pub statement_index: usize,
    pub error: storage_types::StorageError,
}

impl SqlReadBatchFailure {
    /// The error of a failed statement that reads no candidate item table:
    /// a missing relation there is no stale table binding.
    pub(crate) fn outside_candidates(self) -> storage_types::StorageError {
        if self.error.attempt_kind() == Some(storage_types::AttemptKind::FreshValidation) {
            storage_types::StorageError::internal("SQL system relation is missing")
        } else {
            self.error
        }
    }
}

pub struct SqlReadBatchResult {
    pub observation: Option<SqlQueryPage>,
    pub reads: Result<storage_types::Exact<SqlQueryPage>, SqlReadBatchFailure>,
}

/// Completed original prefix; a failure index refers to the submitted statement
/// order.
pub struct SqlOpenReadBatchResult {
    pub pages: Vec<SqlQueryPage>,
    pub failure: Option<SqlReadBatchFailure>,
}

pub enum SqlWriteBatchFailure {
    CountAssertion {
        statement_index: usize,
        error: storage_types::StorageError,
    },
    Other {
        statement_index: usize,
        error: storage_types::StorageError,
    },
}

/// Statements issued on one native transaction or coherent read context.
///
/// One logical operation may invoke these methods many times. The backend may
/// combine independent reads in one network request through `query_batch`.
/// The default executes each read on this same attempt, never on another
/// connection or transaction. A query streams rows until its byte budget;
/// the SQL lowerer supplies ordering and continuation predicates.
/// Native failure or cancellation after a call starts rejects settlement of
/// the whole attempt. A later successful statement cannot reset rejection.
pub trait SqlAttempt: Send {
    /// The store's probe; `None` in production.
    fn fault_probe(&self) -> Option<&dyn crate::SqlFaultProbe> {
        None
    }

    fn reject_settlement(&mut self);
    fn reject_settlement_error(&mut self, error: &storage_types::StorageError);
    fn require_settlement(&self) -> StorageResult<()>;

    /// Final read on this attempt. Local engines retain their existing outer
    /// settlement.
    fn finish_read_batch(
        &mut self,
        observation: SqlStatement,
        statements: Vec<(SqlStatement, ReadProtection)>,
        max_bytes: NonZeroUsize,
    ) -> impl Future<Output = StorageResult<SqlReadBatchResult>> + Send {
        async move {
            let observation = self
                .query(observation, ReadProtection::Snapshot, max_bytes)
                .await?;
            let reads = self
                .query_batch_protected_exact(statements, max_bytes)
                .await?;
            Ok(SqlReadBatchResult {
                observation: Some(observation),
                reads: Ok(reads),
            })
        }
    }

    /// Read a bounded batch without settling successful reads. Local engines
    /// retain ordinary outer rollback when a query fails.
    fn query_batch_open(
        &mut self,
        statements: Vec<(SqlStatement, ReadProtection)>,
        max_bytes: NonZeroUsize,
    ) -> impl Future<Output = StorageResult<SqlOpenReadBatchResult>> + Send {
        async move {
            let pages = self.query_batch_protected(statements, max_bytes).await?;
            Ok(SqlOpenReadBatchResult {
                pages,
                failure: None,
            })
        }
    }

    /// Terminal server-asserted writes. Existing local callers retain checked
    /// execute counts.
    fn finish_write_batch(
        &mut self,
        _statements: Vec<crate::SqlCountedWrite>,
        _max_bytes: NonZeroUsize,
    ) -> impl Future<Output = StorageResult<Result<Vec<SqlQueryPage>, SqlWriteBatchFailure>>> + Send
    {
        async move {
            let error =
                storage_types::StorageError::internal("terminal counted writes require Postgres");
            self.reject_settlement_error(&error);
            Err(error)
        }
    }

    fn query(
        &mut self,
        statement: SqlStatement,
        protection: ReadProtection,
        max_bytes: NonZeroUsize,
    ) -> impl Future<Output = StorageResult<SqlQueryPage>> + Send;

    fn execute(
        &mut self,
        statement: SqlStatement,
    ) -> impl Future<Output = StorageResult<u64>> + Send;

    /// Independent statements stay in this native transaction. Networked
    /// drivers may pipeline them; local drivers can execute in order. Return
    /// one affected-row count per input statement, preserving input order.
    /// Settle every dispatched statement before returning, including on errors;
    /// caller rollback and admission release must not race abandoned siblings.
    fn execute_batch(
        &mut self,
        statements: Vec<SqlStatement>,
    ) -> impl Future<Output = StorageResult<Vec<u64>>> + Send;

    /// The byte budget applies separately to each statement. A domain
    /// operation with an aggregate budget accounts for pages above this trait.
    fn query_batch(
        &mut self,
        statements: Vec<SqlStatement>,
        protection: ReadProtection,
        max_bytes: NonZeroUsize,
    ) -> impl Future<Output = StorageResult<Vec<SqlQueryPage>>> + Send {
        self.query_batch_protected(
            statements
                .into_iter()
                .map(|statement| (statement, protection))
                .collect(),
            max_bytes,
        )
    }

    /// Pipeline independent reads with their own lock strength. The history
    /// scope read locks only its non-null scope row, while neighboring point
    /// reads may require conflict protection in the same network round.
    fn query_batch_protected(
        &mut self,
        statements: Vec<(SqlStatement, ReadProtection)>,
        max_bytes: NonZeroUsize,
    ) -> impl Future<Output = StorageResult<Vec<SqlQueryPage>>> + Send;

    /// `execute_batch` with the one-count-per-statement proof built here.
    fn execute_batch_exact(
        &mut self,
        statements: Vec<SqlStatement>,
    ) -> impl Future<Output = StorageResult<storage_types::Exact<u64>>> + Send {
        async move {
            let expected = statements.len();
            storage_types::Exact::try_new(expected, self.execute_batch(statements).await?)
        }
    }

    /// `query_batch` with the one-page-per-statement proof built here.
    fn query_batch_exact(
        &mut self,
        statements: Vec<SqlStatement>,
        protection: ReadProtection,
        max_bytes: NonZeroUsize,
    ) -> impl Future<Output = StorageResult<storage_types::Exact<SqlQueryPage>>> + Send {
        async move {
            let expected = statements.len();
            storage_types::Exact::try_new(
                expected,
                self.query_batch(statements, protection, max_bytes).await?,
            )
        }
    }

    /// `query_batch_protected` with the one-page-per-statement proof built
    /// here.
    fn query_batch_protected_exact(
        &mut self,
        statements: Vec<(SqlStatement, ReadProtection)>,
        max_bytes: NonZeroUsize,
    ) -> impl Future<Output = StorageResult<storage_types::Exact<SqlQueryPage>>> + Send {
        async move {
            let expected = statements.len();
            storage_types::Exact::try_new(
                expected,
                self.query_batch_protected(statements, max_bytes).await?,
            )
        }
    }
}

/// Native SQL driver. Only this owner begins and settles the attempt.
///
/// The callback lends a nonescaping attempt. SQLite can supervise the entire
/// callback on its worker; Postgres and Turso can retain an exclusive lease.
/// A known-aborted conflict may start a new attempt. An ambiguous commit must
/// be returned as unknown, never silently retried by the driver.
pub trait SqlBackend: Send + Sync {
    fn attempt_deadlines(&self) -> storage_types::AttemptDeadlines;

    /// Execute DDL that PostgreSQL forbids inside a transaction. The native
    /// session owns the operation and the exact object serialization lock.
    fn execute_autocommit_ddl(
        &self,
        _ddl: crate::SqlDdl,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        async {
            Err(storage_types::StorageError::internal(
                "Autocommit DDL is unsupported",
            ))
        }
    }
    fn bind_work_limits(&self, _limits: storage_types::NativeWorkLimits) -> StorageResult<()> {
        Err(storage_types::StorageError::validation(
            "native backend does not enforce work budgets",
        ))
    }

    type Attempt: SqlAttempt;

    /// Report actual initialized native scheduling, independently of registry
    /// settings.
    fn attempt_capacity(&self) -> StorageResult<storage_types::NativeAttemptCapacity>;
    /// Whether the store's durability batch stopped after a failed sync.
    /// Stores that make each commit durable before it is visible have none.
    fn durability_health(&self) -> Option<storage_common::durability::DurabilityHealth> {
        None
    }

    /// The concrete driver qualifies physical shapes against initialized
    /// limits. Unknown aggregate transaction accounting grants no regional
    /// authority.
    fn record_capacity(
        &self,
        limits: infrastructure_provider::limits::InfrastructureLimits,
    ) -> StorageResult<infrastructure_provider::physical_store::PhysicalRecordCapacity>;

    fn engine(&self) -> SqlEngine;

    /// The process board of a store shared by processes (Postgres, N1c).
    fn queue_wake_board(&self) -> Option<std::sync::Arc<queue_provider::QueueWakeBoard>> {
        None
    }

    /// The driver reads `request.scope`; only Postgres maps it.
    fn with_attempt<R, F>(
        &self,
        request: AttemptRequest,
        operation: F,
    ) -> impl Future<Output = StorageResult<R>> + Send
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut Self::Attempt) -> BoxFuture<'a, StorageResult<R>>
            + Send
            + 'static;

    /// Schema changes use the driver's ordinary read-write transaction.
    fn with_schema_attempt<R, F>(
        &self,
        operation: F,
    ) -> impl Future<Output = StorageResult<R>> + Send
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut Self::Attempt) -> BoxFuture<'a, StorageResult<R>>
            + Send
            + 'static,
    {
        self.with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::SchemaChange)
                .scoped(storage_types::AttemptScope::Schema)
                .start(self.attempt_deadlines()),
            operation,
        )
    }
}

/// The busy wait of a local BEGIN for which the attempt has time left
/// (FIN-b), or `expired_in(Begin)` when none is left.
#[cfg(any(feature = "sqlite", feature = "turso"))]
pub(crate) fn local_busy_wait(
    deadline: &storage_types::AttemptDeadline,
) -> StorageResult<std::time::Duration> {
    let busy = deadline
        .remaining()
        .min(crate::constants::LOCAL_SQL_BUSY_TIMEOUT);
    if busy.is_zero() {
        return Err(storage_types::expired_in(
            storage_types::AttemptPhase::Begin,
        ));
    }
    Ok(busy)
}

/// A local BEGIN that waited on another holder's lock (busy or locked: a
/// `Conflict`) and ended after the attempt deadline is the deadline's expiry:
/// nothing began (FIN-b).
#[cfg(any(feature = "sqlite", feature = "turso"))]
pub(crate) fn local_begin_failure(
    error: storage_types::StorageError,
    deadline: &storage_types::AttemptDeadline,
) -> storage_types::StorageError {
    if error.attempt_kind() == Some(storage_types::AttemptKind::Conflict)
        && deadline
            .require_remaining(storage_types::AttemptPhase::Begin)
            .is_err()
    {
        return storage_types::expired_in(storage_types::AttemptPhase::Begin);
    }
    error
}
