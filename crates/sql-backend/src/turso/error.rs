use storage_common::native_outcome::NativeCommitError;
use storage_types::{
    AttemptFailure, CapacityKind, NativeCallPhase, NativeCause, NativeCode, NativeEngine,
    StorageError,
};

fn cause(error: &turso::Error, phase: NativeCallPhase) -> NativeCause {
    NativeCause {
        engine: NativeEngine::Turso,
        code: NativeCode::Named(code_name(error)),
        phase,
    }
}

/// A fixed name of the native error; never its message.
fn code_name(error: &turso::Error) -> &'static str {
    match error {
        turso::Error::ToSqlConversionFailure(_) => "ToSqlConversionFailure",
        turso::Error::QueryReturnedNoRows => "QueryReturnedNoRows",
        turso::Error::ConversionFailure(_) => "ConversionFailure",
        turso::Error::Error(message) if message == "Write-write conflict" => "WriteWriteConflict",
        turso::Error::Error(message) if message.starts_with("Conflict: ") => "Conflict",
        turso::Error::Error(message) if message == "Out of memory" => "OutOfMemory",
        turso::Error::Error(message) if message == "string or blob too big" => "TooBig",
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
        turso::Error::Busy(_) => "Busy",
        turso::Error::BusySnapshot(_) => "BusySnapshot",
        turso::Error::Interrupt(_) => "Interrupt",
    }
}

pub(super) fn map_driver_error(error: turso::Error) -> StorageError {
    let error = unwrap_statement_error(error);
    if is_aborted_conflict(&error) {
        return StorageError::native_conflict(cause(&error, NativeCallPhase::Statement));
    }
    if is_capacity_rejection(&error) {
        return AttemptFailure::Capacity {
            kind: CapacityKind::NativeLimit,
            code: NativeCode::Named(code_name(&error)),
        }
        .into();
    }
    match error {
        turso::Error::BatchRollbackFailed { .. } => {
            StorageError::commit_unknown("Turso batch rollback was not acknowledged")
        }
        other => StorageError::internal(&format!(
            "Turso native operation failed ({})",
            code_name(&other)
        )),
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

/// A failed COMMIT: only an engine conflict proves that nothing committed.
pub(super) fn commit_error(error: turso::Error) -> NativeCommitError {
    let error = unwrap_statement_error(error);
    if is_aborted_conflict(&error) {
        return NativeCommitError::Conflict(cause(&error, NativeCallPhase::Commit));
    }
    NativeCommitError::Unproven {
        reason: "Turso commit was not acknowledged",
        cause: Some(cause(&error, NativeCallPhase::Commit)),
    }
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
