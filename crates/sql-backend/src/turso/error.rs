use storage_types::{StorageEnum, StorageError};

pub(super) fn map_driver_error(error: turso::Error) -> StorageError {
    let error = unwrap_statement_error(error);
    if is_aborted_conflict(&error) {
        return StorageEnum::TransactionConflict {
            message: "Turso transaction aborted".to_owned(),
        }
        .into();
    }
    if is_capacity_rejection(&error) {
        return storage_types::InfrastructureResultKind::NativeCapacityRejected.into();
    }
    match error {
        turso::Error::BatchRollbackFailed { .. } => {
            unknown_settlement("Turso batch rollback was not acknowledged")
        }
        other => {
            let code = match other {
                turso::Error::ToSqlConversionFailure(_) => "ToSqlConversionFailure",
                turso::Error::QueryReturnedNoRows => "QueryReturnedNoRows",
                turso::Error::ConversionFailure(_) => "ConversionFailure",
                turso::Error::Error(_) => "Error",
                turso::Error::Misuse(_) => "Misuse",
                turso::Error::Constraint(_) => "Constraint",
                turso::Error::Readonly(_) => "Readonly",
                turso::Error::DatabaseFull(_) => "DatabaseFull",
                turso::Error::NotAdb(_) => "NotAdb",
                turso::Error::Corrupt(_) => "Corrupt",
                turso::Error::IoError(_, _) => "IoError",
                turso::Error::BatchStatementFailed { .. } => "BatchStatementFailed",
                turso::Error::BatchRollbackFailed { .. } => "BatchRollbackFailed",
                turso::Error::Busy(_)
                | turso::Error::BusySnapshot(_)
                | turso::Error::Interrupt(_) => "TransactionConflict",
            };
            StorageError::internal(&format!("Turso native operation failed ({code})"))
        }
    }
}

fn unwrap_statement_error(mut error: turso::Error) -> turso::Error {
    loop {
        error = match error {
            turso::Error::BatchStatementFailed { error, .. } => *error,
            other => return other,
        };
    }
}

pub(super) fn map_commit_error(error: turso::Error) -> StorageError {
    if is_aborted_conflict(&error) {
        return map_driver_error(error);
    }
    unknown_settlement("Turso commit was not acknowledged")
}

fn is_aborted_conflict(error: &turso::Error) -> bool {
    match error {
        turso::Error::Busy(_) | turso::Error::BusySnapshot(_) | turso::Error::Interrupt(_) => true,
        // Turso 0.8.1 exposes these native MVCC aborts through its generic
        // Error variant. Match only the engine's conflict forms, never
        // arbitrary errors.
        turso::Error::Error(message) => {
            message == "Write-write conflict" || message.starts_with("Conflict: ")
        }
        _ => false,
    }
}

pub(super) fn unknown_settlement(message: &'static str) -> StorageError {
    StorageError::sync_submission_ambiguous(None, message)
}

fn is_capacity_rejection(error: &turso::Error) -> bool {
    match error {
        turso::Error::DatabaseFull(_) => true,
        // Locked Turso 0.8.1 erases LimboError::OutOfMemory and TooBig into
        // Error with these exact Display values. Do not match arbitrary text.
        turso::Error::Error(message) => {
            matches!(message.as_str(), "Out of memory" | "string or blob too big")
        }
        _ => false,
    }
}
