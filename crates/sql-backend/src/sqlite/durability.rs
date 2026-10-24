//! Batch acknowledgements in memory; synchronize the WAL without a SQL write.

use std::{fs::OpenOptions, path::PathBuf, sync::Arc};

use storage_common::durability::DurabilityBatch;
use storage_types::{StorageError, StorageResult};

use super::error::map_worker_error;

pub(super) async fn initialize_batch(
    connection: tokio_rusqlite::Connection,
) -> StorageResult<Option<Arc<DurabilityBatch>>> {
    let path = connection
        .call(|native| {
            let mode: String = native.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
            Ok(mode.eq_ignore_ascii_case("wal").then(|| {
                let mut path = PathBuf::from(native.path().unwrap_or_default()).into_os_string();
                path.push("-wal");
                PathBuf::from(path)
            }))
        })
        .await
        .map_err(map_worker_error)?;
    Ok(path.map(|path| {
        let file = Arc::new(std::sync::OnceLock::new());
        Arc::new(DurabilityBatch::new(move || {
            let connection = connection.clone();
            let path = path.clone();
            let file = file.clone();
            async move {
                connection
                    .call(move |_| Ok(synchronize_wal(&path, &file)))
                    .await
                    .map_err(|_| unknown_durability())?
            }
        }))
    }))
}

fn synchronize_wal(
    path: &std::path::Path,
    file: &std::sync::OnceLock<std::fs::File>,
) -> StorageResult<()> {
    if file.get().is_none() {
        let opened = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|_| unknown_durability())?;
        let _ = file.set(opened);
    }
    file.get()
        .ok_or_else(unknown_durability)?
        .sync_all()
        .map_err(|_| unknown_durability())
}

fn unknown_durability() -> StorageError {
    StorageError::sync_submission_ambiguous(None, "SQLite WAL sync was not acknowledged")
}
