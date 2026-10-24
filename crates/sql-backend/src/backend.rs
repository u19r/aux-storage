use std::{borrow::Cow, fmt::Write as _, future::Future, num::NonZeroUsize};

use futures::future::BoxFuture;
pub use storage_types::{AttemptMode, ReadProtection};
use storage_types::{KeyAttributeType, StorageResult};

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
    #[cfg(feature = "fault-injection")]
    fn fault_probe(&self) -> Option<&dyn crate::SqlFaultProbe> {
        None
    }

    fn reject_settlement(&mut self);
    fn reject_settlement_error(&mut self, error: &storage_types::StorageError);
    fn require_settlement(&self) -> StorageResult<()>;

    fn ownership_changes(
        &mut self,
    ) -> &mut stream_provider::retained_image::RetainedOwnershipChanges;

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
}

/// Coordination of writes to the shared stream data relation. Ordinary writers
/// coexist; topology changes drain them before observing and sealing the tail.
#[derive(Clone, Copy)]
pub enum SqlDataAccess {
    Write,
    Exclusive,
}

/// Native SQL driver. Only this owner begins and settles the attempt.
///
/// The callback lends a nonescaping attempt. SQLite can supervise the entire
/// callback on its worker; Postgres and Turso can retain an exclusive lease.
/// A known-aborted conflict may start a new attempt. An ambiguous commit must
/// be returned as unknown, never silently retried by the driver.
pub trait SqlBackend: Send + Sync {
    fn bind_work_limits(&self, _limits: storage_types::NativeWorkLimits) -> StorageResult<()> {
        Err(storage_types::StorageError::validation(
            "native backend does not enforce work budgets",
        ))
    }

    type Attempt: SqlAttempt;

    /// Report actual initialized native scheduling, independently of registry
    /// settings.
    fn attempt_capacity(&self) -> StorageResult<storage_types::NativeAttemptCapacity>;

    /// The concrete driver qualifies physical shapes against initialized
    /// limits. Unknown aggregate transaction accounting grants no regional
    /// authority.
    fn record_capacity(
        &self,
        limits: infrastructure_provider::limits::InfrastructureLimits,
    ) -> StorageResult<infrastructure_provider::physical_store::PhysicalRecordCapacity>;

    fn engine(&self) -> SqlEngine;

    fn with_attempt<R, F>(
        &self,
        mode: AttemptMode,
        operation: F,
    ) -> impl Future<Output = StorageResult<R>> + Send
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut Self::Attempt) -> BoxFuture<'a, StorageResult<R>>
            + Send
            + 'static;

    /// Data operations protect exact item targets and stream routing
    /// explicitly. Engines with native statement snapshots can avoid
    /// predicate tracking.
    fn with_data_attempt<R, F>(
        &self,
        mode: AttemptMode,
        _access: SqlDataAccess,
        operation: F,
    ) -> impl Future<Output = StorageResult<R>> + Send
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut Self::Attempt) -> BoxFuture<'a, StorageResult<R>>
            + Send
            + 'static,
    {
        self.with_attempt(mode, operation)
    }

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
        self.with_attempt(AttemptMode::ReadWrite, operation)
    }
}

/// Keep native settlement alive after caller cancellation and propagate worker
/// panics.
#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
pub(crate) async fn supervise_attempt<R: Send + 'static>(
    attempt: impl Future<Output = StorageResult<R>> + Send + 'static,
    stopped: &'static str,
) -> StorageResult<R> {
    #[cfg(feature = "fault-injection")]
    let attempt = {
        use tracing::{Instrument as _, instrument::WithSubscriber as _};
        attempt
            .instrument(tracing::Span::current())
            .with_subscriber(tracing::dispatcher::get_default(Clone::clone))
    };
    match tokio::spawn(attempt).await {
        Ok(result) => result,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(_) => Err(storage_types::StorageError::internal(stopped)),
    }
}
