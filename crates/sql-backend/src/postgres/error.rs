use storage_common::diagnostics::{NativeEngine, NativeOperation, trace_native_failure};
use storage_types::{NativeAttemptRejection, StorageEnum, StorageError, StorageResult};
use tokio_postgres::{Error, error::SqlState};

pub(super) fn map_driver_error(error: Error) -> StorageError {
    let sqlstate = error.code().map(|code| code.code());
    match classify_operation_rejection(&error) {
        Some(NativeAttemptRejection::Conflict) => StorageEnum::TransactionConflict {
            message: format!(
                "Postgres transaction aborted (SQLSTATE {})",
                sqlstate.unwrap_or("unknown")
            ),
        }
        .into(),
        Some(NativeAttemptRejection::Capacity) => {
            storage_types::InfrastructureResultKind::NativeCapacityRejected.into()
        }
        None => match sqlstate {
            Some(code) => StorageError::internal(&format!(
                "Postgres native operation failed (SQLSTATE {code})"
            )),
            None => StorageError::internal("Postgres native operation failed (no SQLSTATE)"),
        },
    }
}

pub(super) fn map_commit_error(error: Error) -> StorageError {
    if classify_operation_rejection(&error) == Some(NativeAttemptRejection::Conflict) {
        return map_driver_error(error);
    }
    unknown_settlement("Postgres commit was not acknowledged")
}

pub(super) fn unknown_settlement(message: &'static str) -> StorageError {
    StorageError::sync_submission_ambiguous(None, message)
}

fn classify_operation_rejection(error: &Error) -> Option<NativeAttemptRejection> {
    let failure = error.as_db_error()?;
    match failure.code() {
        &SqlState::T_R_SERIALIZATION_FAILURE | &SqlState::T_R_DEADLOCK_DETECTED => {
            Some(NativeAttemptRejection::Conflict)
        }
        &SqlState::UNIQUE_VIOLATION
            if failure.schema() == Some("pg_catalog")
                && failure.table() == Some("pg_type")
                && failure.constraint() == Some("pg_type_typname_nsp_index") =>
        {
            // Concurrent CREATE TABLE IF NOT EXISTS can abort in the native
            // catalog.
            Some(NativeAttemptRejection::Conflict)
        }
        &SqlState::PROGRAM_LIMIT_EXCEEDED
        | &SqlState::STATEMENT_TOO_COMPLEX
        | &SqlState::TOO_MANY_COLUMNS
        | &SqlState::TOO_MANY_ARGUMENTS
        | &SqlState::DISK_FULL
        | &SqlState::OUT_OF_MEMORY
        | &SqlState::CONFIGURATION_LIMIT_EXCEEDED => Some(NativeAttemptRejection::Capacity),
        _ => None,
    }
}

pub(super) enum PostgresCallError {
    Native(Error),
    Local(StorageError),
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
            Self::Local(error) => settlement.reject_error(error),
            Self::Native(error)
                if classify_operation_rejection(error)
                    == Some(NativeAttemptRejection::Capacity) =>
            {
                settlement.reject_error(
                    &storage_types::InfrastructureResultKind::NativeCapacityRejected.into(),
                );
            }
            Self::Native(_) => settlement.reject(),
        }
    }

    pub(super) fn report(self, operation: NativeOperation) -> StorageError {
        let error = match self {
            Self::Native(error) => map_driver_error(error),
            Self::Local(error) => error,
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
    let failure = results
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
                Err(PostgresCallError::Local(_)) => true,
                Ok(_) => false,
            })
        })
        .or_else(|| results.iter().position(Result::is_err));
    if let Some(index) = failure
        && let Err(error) = results.swap_remove(index)
    {
        return Err(error.report(operation));
    }
    let mut values = Vec::new();
    values
        .try_reserve_exact(results.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    for result in results {
        values.push(result.map_err(|error| error.report(operation))?);
    }
    Ok(values)
}
