use rusqlite::{Error, ErrorCode};
use storage_types::{StorageEnum, StorageError};

/// Map SQLite failures into storage-layer error categories.
#[expect(clippy::needless_pass_by_value)]
pub fn map_sqlite_error(err: Error) -> StorageError {
    match &err {
        Error::SqliteFailure(failure, msg_opt) => {
            let code = failure.code;
            match code {
                // Busy/locked -> retry paths usually handle; surface as TransactionConflict
                ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked => {
                    StorageError::Base(StorageEnum::TransactionConflict {
                        message: format!("sqlite busy: {err}"),
                    })
                }
                // Constraint failures often correspond to conditional checks (unique / not null)
                ErrorCode::ConstraintViolation => {
                    StorageError::Base(StorageEnum::ConditionalCheckFailed)
                }
                ErrorCode::SchemaChanged => {
                    StorageError::validation(format!("sqlite schema changed: {err}"))
                }
                _ => StorageError::internal(&format!(
                    "sqlite error failed: code={code:?} msg={msg_opt:?}: {err}"
                )),
            }
        }
        Error::FromSqlConversionFailure(_, _, _) => {
            StorageError::validation(format!("sqlite conversion failed: {err}"))
        }
        Error::IntegralValueOutOfRange(_, _) => {
            StorageError::validation(format!("sqlite numeric out of range: {err}"))
        }
        Error::InvalidQuery => StorageError::validation("invalid sqlite query"),
        _ => StorageError::internal(&format!("sqlite error failed: {err}")),
    }
}
