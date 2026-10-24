use rusqlite::{Error, ErrorCode};
use storage_types::{StorageEnum, StorageError};

pub(super) fn map_driver_error(error: Error) -> StorageError {
    match error {
        Error::SqliteFailure(failure, _) => {
            let diagnostic = format!(
                "SQLite code {:?}, extended {}",
                failure.code, failure.extended_code
            );
            match failure.code {
                ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked => {
                    StorageError::from(StorageEnum::TransactionConflict {
                        message: format!("SQLite transaction was busy or locked ({diagnostic})"),
                    })
                }
                ErrorCode::TooBig | ErrorCode::DiskFull | ErrorCode::OutOfMemory => {
                    storage_types::InfrastructureResultKind::NativeCapacityRejected.into()
                }
                _ => StorageError::internal(&format!(
                    "SQLite native operation failed ({diagnostic})"
                )),
            }
        }
        _ => StorageError::internal("SQLite native operation failed"),
    }
}

pub(super) fn map_worker_error(error: tokio_rusqlite::Error) -> StorageError {
    match error {
        tokio_rusqlite::Error::Rusqlite(native) => map_driver_error(native),
        _ => StorageError::internal("SQLite worker failed"),
    }
}
