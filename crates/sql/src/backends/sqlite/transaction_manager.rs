use storage_types::{StorageError, StorageResult, context::ErrorContext as _};
use tokio_rusqlite::Connection;
use tracing::warn;

use crate::{
    error_handler::map_sqlite_error,
    utils::{SqliteConn, call_sqlite},
};

/// Manages a database transaction by creating a transaction, executing
/// operations, and committing or rolling back
///
/// # Arguments
///
/// * `conn` - The database connection
/// * `operation` - A closure that takes a transaction and returns a Result<(),
///   `StorageError`>
///
/// # Returns
///
/// A Result<(), `StorageError`> indicating success or failure
pub async fn with_transaction<F, T>(connection: &Connection, operation: F) -> StorageResult<T>
where
    F: for<'a> FnOnce(&'a SqliteConn<'a>) -> Result<T, StorageError> + Send + 'static,
    T: Send + 'static,
{
    call_sqlite(connection, move |conn| {
        let t_id = std::thread::current().id();
        let p_id = std::process::id();
        let txn = conn
            .unchecked_transaction()
            .map_err(map_sqlite_error)
            .context("transaction start")?;
        let sqlite = SqliteConn::Transaction(&txn);

        match operation(&sqlite) {
            Ok(x) => {
                txn.commit()
                    .inspect_err(|e| {
                        warn!(
                            "Transaction commit failed on thread {:?} process {}: {:?}",
                            t_id, p_id, e
                        );
                    })
                    .map_err(map_sqlite_error)
                    .context("transaction commit")?;
                Ok(x)
            }
            Err(error) => {
                txn.rollback()
                    .map_err(map_sqlite_error)
                    .context("transaction rollback")?;
                Err(error)
            }
        }
    })
    .await
}
