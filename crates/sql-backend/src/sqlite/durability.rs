//! Batch acknowledgements in memory; synchronize the WAL without a SQL write
//! and off the writer connection.
//!
//! The sync uses its own descriptor of the WAL file on a blocking thread, so
//! the writer commits during a sync. `fsync` on another descriptor flushes the
//! same file (macOS `sync_all` uses `F_FULLFSYNC`). The descriptor stays valid:
//! a WAL reset after a checkpoint rewrites the same file, and the probe
//! connection keeps the database open, so SQLite does not delete the WAL
//! while this batch lives. A checkpoint on the writer during a sync syncs the
//! main database itself under `synchronous=NORMAL`.
//!
//! The probe connection reads `PRAGMA data_version`, which changes when any
//! other connection or process commits. The barrier reads it before the sync
//! begins; an attempt that wrote nothing compares it with the capture of the
//! last completed barrier to find commits of another process.

use std::{
    fs::OpenOptions,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

use storage_common::durability::{BarrierFailure, DurabilityBatch};
use storage_types::StorageResult;

use super::error::map_worker_error;

/// Version reads a barrier tries before it syncs without a version.
const SQLITE_PROBE_ATTEMPTS: usize = 3;

/// Runs on the sync thread before the WAL sync; an error stands for a failed
/// sync (tests only pass one).
pub(super) type SyncHook = Arc<dyn Fn() -> std::io::Result<()> + Send + Sync>;

/// Receives a `DurabilityBarrier` event when each barrier starts; the store
/// sets the probe after the batch is built.
pub(super) type BarrierObserver = Arc<OnceLock<Arc<dyn crate::SqlFaultProbe>>>;

pub(super) async fn initialize_batch(
    connection: tokio_rusqlite::Connection,
    observer: BarrierObserver,
) -> StorageResult<Option<Arc<DurabilityBatch>>> {
    build_batch(connection, None, observer).await
}

#[cfg(test)]
pub(super) async fn initialize_batch_with_sync_hook(
    connection: tokio_rusqlite::Connection,
    hook: SyncHook,
) -> StorageResult<Option<Arc<DurabilityBatch>>> {
    build_batch(connection, Some(hook), BarrierObserver::default()).await
}

async fn build_batch(
    connection: tokio_rusqlite::Connection,
    hook: Option<SyncHook>,
    observer: BarrierObserver,
) -> StorageResult<Option<Arc<DurabilityBatch>>> {
    let database = connection
        .call(|native| {
            let mode: String = native.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
            Ok(mode
                .eq_ignore_ascii_case("wal")
                .then(|| PathBuf::from(native.path().unwrap_or_default())))
        })
        .await
        .map_err(map_worker_error)?;
    let Some(database) = database else {
        return Ok(None);
    };
    let mut wal = database.clone().into_os_string();
    wal.push("-wal");
    let wal = PathBuf::from(wal);
    let probe = open_probe(&database).await?;
    let file = Arc::new(OnceLock::new());
    let barrier_probe = probe.clone();
    Ok(Some(Arc::new(DurabilityBatch::with_version_probe(
        move |start| {
            let probe = barrier_probe.clone();
            let wal = wal.clone();
            let file = Arc::clone(&file);
            let hook = hook.clone();
            if let Some(probe) = observer.get() {
                probe.observe(crate::SqlNativeEvent::DurabilityBarrier);
            }
            let sync_probe = observer.get().cloned();
            async move {
                // A failed version read does not stop the store: the barrier
                // still syncs, and readers that need the version wait for it.
                let mut version = None;
                for _ in 0..SQLITE_PROBE_ATTEMPTS {
                    if let Ok(read) = data_version(&probe).await {
                        version = Some(read);
                        break;
                    }
                }
                let capture = start.begin(version);
                tokio::task::spawn_blocking(move || {
                    if let Some(hook) = hook {
                        hook().map_err(|_| BarrierFailure::SyncFailed)?;
                    }
                    if let Some(probe) = sync_probe {
                        probe
                            .before_wal_sync()
                            .map_err(|_| BarrierFailure::SyncFailed)?;
                    }
                    synchronize_wal(&wal, &file)
                })
                .await
                .map_err(|_| BarrierFailure::Interrupted)??;
                Ok(capture)
            }
        },
        move || {
            let probe = probe.clone();
            async move { data_version(&probe).await }
        },
    ))))
}

/// SQLite's default `wal_autocheckpoint` (pages).
#[cfg(feature = "fault-injection")]
const SQLITE_DEFAULT_WAL_AUTOCHECKPOINT: std::ffi::c_int = 1000;

/// Reports the WAL frames that each commit of the writer appends. The hook
/// replaces SQLite's default WAL hook, so it runs the same passive
/// checkpoint at the same page count and ignores its result as the default
/// hook does.
#[cfg(feature = "fault-injection")]
pub(super) async fn observe_wal_frames(
    connection: &tokio_rusqlite::Connection,
    observer: BarrierObserver,
) -> StorageResult<()> {
    connection
        .call(move |native| {
            let mut last = 0;
            native.wal_hook(Some(
                move |wal: &mut rusqlite::hooks::Wal, pages: std::ffi::c_int| {
                    // A smaller count means that the WAL restarted after a
                    // checkpoint.
                    let frames = if pages >= last { pages - last } else { pages };
                    last = pages;
                    if let Some(probe) = observer.get() {
                        probe.observe(crate::SqlNativeEvent::WalFrames {
                            frames: u64::try_from(frames).unwrap_or_default(),
                        });
                    }
                    if pages >= SQLITE_DEFAULT_WAL_AUTOCHECKPOINT {
                        let _ = wal.checkpoint();
                    }
                    Ok(())
                },
            ))?;
            Ok(())
        })
        .await
        .map_err(map_worker_error)
}

async fn open_probe(database: &Path) -> StorageResult<tokio_rusqlite::Connection> {
    let probe = tokio_rusqlite::Connection::open(database)
        .await
        .map_err(map_worker_error)?;
    probe
        .call(|native| {
            native.pragma_update(None, "busy_timeout", 5_000)?;
            native.pragma_update(None, "query_only", true)?;
            Ok(())
        })
        .await
        .map_err(map_worker_error)?;
    Ok(probe)
}

/// The native commit version of `connection`; it changes when any other
/// connection commits.
pub(super) fn read_data_version(connection: &rusqlite::Connection) -> rusqlite::Result<u64> {
    let version: i64 = connection.pragma_query_value(None, "data_version", |row| row.get(0))?;
    Ok(version.cast_unsigned())
}

async fn data_version(probe: &tokio_rusqlite::Connection) -> StorageResult<u64> {
    probe
        .call(|native| Ok(read_data_version(native)?))
        .await
        .map_err(map_worker_error)
}

/// Any error here means the WAL may not be on disk: the store stops.
fn synchronize_wal(path: &Path, file: &OnceLock<std::fs::File>) -> Result<(), BarrierFailure> {
    if file.get().is_none() {
        let opened = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|_| BarrierFailure::SyncFailed)?;
        let _ = file.set(opened);
    }
    file.get()
        .ok_or(BarrierFailure::SyncFailed)?
        .sync_all()
        .map_err(|_| BarrierFailure::SyncFailed)
}
