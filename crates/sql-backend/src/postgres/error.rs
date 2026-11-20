use storage_common::{
    diagnostics::{NativeOperation, trace_native_failure},
    native_outcome::NativeCommitError,
};
use storage_types::{
    AttemptFailure, CapacityKind, FreshCause, NativeCallPhase, NativeCause, NativeCode,
    NativeEngine, StorageError, StorageResult,
};
use tokio_postgres::{Error, error::SqlState};

pub(super) fn map_driver_error(error: &Error) -> StorageError {
    let sqlstate = error.code().map(|code| code.code());
    // The server ended the statement (`statement_timeout`) or the transaction
    // (`transaction_timeout`, 25P04) at the attempt deadline set in BEGIN.
    if matches!(sqlstate, Some("57014" | "25P04")) {
        return storage_types::expired_in(storage_types::AttemptPhase::Statement);
    }
    let code = sqlstate.map_or(NativeCode::None, NativeCode::sql_state);
    match classify_operation_rejection(error) {
        Some(PostgresRejection::Conflict) => StorageError::native_conflict(NativeCause {
            engine: NativeEngine::Postgres,
            code,
            phase: NativeCallPhase::Statement,
        }),
        Some(PostgresRejection::Capacity) => AttemptFailure::Capacity {
            kind: CapacityKind::NativeLimit,
            code,
        }
        .into(),
        None => StorageError::internal(&format!("Postgres native operation failed ({code})")),
    }
}

/// A failed COMMIT: only a serialization, deadlock or lock answer proves that
/// the server aborted it.
pub(super) fn commit_error(error: &Error) -> NativeCommitError {
    let code = error
        .code()
        .map_or(NativeCode::None, |code| NativeCode::sql_state(code.code()));
    let cause = NativeCause {
        engine: NativeEngine::Postgres,
        code,
        phase: NativeCallPhase::Commit,
    };
    if classify_operation_rejection(error) == Some(PostgresRejection::Conflict) {
        return NativeCommitError::Conflict(cause);
    }
    NativeCommitError::Unproven {
        reason: "Postgres commit was not acknowledged",
        cause: Some(cause),
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PostgresRejection {
    Conflict,
    Capacity,
}

fn classify_operation_rejection(error: &Error) -> Option<PostgresRejection> {
    let failure = error.as_db_error()?;
    match failure.code() {
        // A bounded lock wait (`lock_timeout`) ends the statement before it
        // changed anything; the transaction is aborted, as for a deadlock.
        &SqlState::T_R_SERIALIZATION_FAILURE
        | &SqlState::T_R_DEADLOCK_DETECTED
        | &SqlState::LOCK_NOT_AVAILABLE => Some(PostgresRejection::Conflict),
        &SqlState::UNIQUE_VIOLATION
            if failure.schema() == Some("pg_catalog")
                && failure.table() == Some("pg_type")
                && failure.constraint() == Some("pg_type_typname_nsp_index") =>
        {
            // Concurrent CREATE TABLE IF NOT EXISTS can abort in the native
            // catalog.
            Some(PostgresRejection::Conflict)
        }
        &SqlState::PROGRAM_LIMIT_EXCEEDED
        | &SqlState::STATEMENT_TOO_COMPLEX
        | &SqlState::TOO_MANY_COLUMNS
        | &SqlState::TOO_MANY_ARGUMENTS
        | &SqlState::DISK_FULL
        | &SqlState::OUT_OF_MEMORY
        | &SqlState::CONFIGURATION_LIMIT_EXCEEDED => Some(PostgresRejection::Capacity),
        _ => None,
    }
}

pub(super) enum PostgresCallError {
    Native(Error),
    Local(StorageError),
    CountAssertion(StorageError),
}

impl From<Error> for PostgresCallError {
    fn from(error: Error) -> Self {
        Self::Native(error)
    }
}

impl From<StorageError> for PostgresCallError {
    fn from(error: StorageError) -> Self {
        Self::Local(error)
    }
}

impl PostgresCallError {
    pub(super) fn reject_settlement(&self, settlement: &storage_types::NativeAttemptSettlement) {
        match self {
            Self::Local(error) | Self::CountAssertion(error) => settlement.reject_error(error),
            // A follow-on of an earlier failure in the same pipelined wave: the
            // root statement merges its own class, so this one must not outrank
            // it with an internal error.
            Self::Native(error) if error.code() == Some(&SqlState::IN_FAILED_SQL_TRANSACTION) => {
                settlement.reject();
            }
            // An attempt failure (deadline, conflict, capacity) merges its
            // class. Any other native error (schema SQLSTATE, follow-on 25P02)
            // only closes the attempt: the caller's own error or a stronger
            // later failure is reported.
            Self::Native(error) => {
                let mapped = map_driver_error(error);
                if mapped.attempt_kind().is_some() {
                    settlement.reject_error(&mapped);
                } else {
                    settlement.reject();
                }
            }
        }
    }

    pub(super) fn is_server_error(&self) -> bool {
        matches!(self, Self::Native(error) if error.as_db_error().is_some())
    }

    pub(super) fn is_native(&self) -> bool {
        matches!(self, Self::Native(_))
    }

    /// A failed read statement. A missing relation or column (42P01, 42703)
    /// means the statement was rendered from stale table metadata: a known
    /// abort that one fresh try decides again. Its caller sends the ROLLBACK
    /// before the step sees the error.
    pub(super) fn into_read_error(self) -> StorageError {
        if let Self::Native(error) = &self
            && let Some(code) = error.code()
            && (*code == SqlState::UNDEFINED_TABLE || *code == SqlState::UNDEFINED_COLUMN)
        {
            let error: StorageError =
                AttemptFailure::FreshValidation(FreshCause::MissingRelation(NativeCause {
                    engine: NativeEngine::Postgres,
                    code: NativeCode::sql_state(code.code()),
                    phase: NativeCallPhase::Statement,
                }))
                .into();
            trace_native_failure(NativeEngine::Postgres, NativeOperation::SqlQuery, &error);
            return error;
        }
        self.report(NativeOperation::SqlQuery)
    }

    pub(super) fn report(self, operation: NativeOperation) -> StorageError {
        let error = match self {
            Self::Native(error) => map_driver_error(&error),
            Self::Local(error) | Self::CountAssertion(error) => error,
        };
        trace_native_failure(NativeEngine::Postgres, operation, &error);
        error
    }
}

pub(super) fn collect_pipeline<T>(
    mut results: Vec<Result<T, PostgresCallError>>,
    operation: NativeOperation,
) -> StorageResult<Vec<T>> {
    // All submitted calls have settled. Choose their failure before attempting
    // output allocation, and keep unknown outcomes above local capacity
    // failure.
    let failure = pipeline_failure_index(&results);
    if let Some(index) = failure
        && let Err(error) = results.swap_remove(index)
    {
        return Err(error.report(operation));
    }
    let mut values = Vec::new();
    values
        .try_reserve_exact(results.len())
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    for result in results {
        values.push(result.map_err(|error| error.report(operation))?);
    }
    Ok(values)
}

pub(super) fn pipeline_failure_index<T>(results: &[Result<T, PostgresCallError>]) -> Option<usize> {
    results
        .iter()
        .position(|result| {
            matches!(result,
        Err(PostgresCallError::Local(error)) if error.is_commit_unknown())
        })
        .or_else(|| {
            results.iter().position(|result| match result {
                Err(PostgresCallError::Native(error)) => {
                    error.code() != Some(&SqlState::IN_FAILED_SQL_TRANSACTION)
                }
                Err(PostgresCallError::Local(_) | PostgresCallError::CountAssertion(_)) => true,
                Ok(_) => false,
            })
        })
        .or_else(|| results.iter().position(Result::is_err))
}

// Only connection creation has not yet submitted customer work. Preserve native
// startup/authentication/settings failures; recognized transport loss is
// unavailable.
pub(super) fn map_connection_error(error: Error) -> StorageError {
    use std::error::Error as _;
    if error.as_db_error().is_some() {
        return map_driver_error(&error);
    }
    let mut cause = error.source();
    while let Some(source) = cause {
        let Some(io) = source.downcast_ref::<std::io::Error>() else {
            cause = source.source();
            continue;
        };
        if matches!(
            io.kind(),
            std::io::ErrorKind::ConnectionRefused
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::NotConnected
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::UnexpectedEof
                | std::io::ErrorKind::NotFound
        ) {
            return AttemptFailure::Unavailable(storage_types::NativeUnavailable::ConnectionLost)
                .into();
        }
        // The kind and OS code name the socket failure (a rejected socket
        // option, a refused address) without the configuration text.
        return StorageError::internal(&match io.raw_os_error() {
            Some(os) => format!("Postgres connection failed ({}, os error {os})", io.kind()),
            None => format!("Postgres connection failed ({})", io.kind()),
        });
    }
    map_driver_error(&error)
}

pub(super) fn map_acquisition_error(
    error: deadpool::managed::PoolError<StorageError>,
) -> StorageError {
    match error {
        deadpool::managed::PoolError::Backend(error) => error,
        _ => StorageError::internal("Postgres pool acquisition failed"),
    }
}
