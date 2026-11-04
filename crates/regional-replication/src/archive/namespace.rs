//! One durable revocation authority per table, independent of its objects.
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, Write},
    num::NonZeroUsize,
    path::PathBuf,
};

use storage_types::{StorageResult, TableHistoryId};

use super::{
    CanonicalArchive,
    filesystem::{archive_io, prepare_namespace_directory, sync_directory},
};
use crate::native_record::corrupt_record;

pub(super) struct TableArchiveLease {
    pub(super) path: PathBuf,
    // Keep the OS lock until publication or a bounded removal has settled.
    _authority: File,
}

enum ArchiveAccess {
    Publish,
    Revoke,
}

impl CanonicalArchive {
    pub(super) async fn admit_table_publication(
        &self,
        history: TableHistoryId,
    ) -> StorageResult<TableArchiveLease> {
        let lease = self
            .lock_table_namespace(history, ArchiveAccess::Publish)
            .await?;
        prepare_namespace_directory(&lease.path).await?;
        sync_directory(&self.root).await?;
        Ok(lease)
    }

    /// Revocation is durable before any unlink. A stale exporter, including a
    /// separate process, must take the same lock and cannot recreate the data.
    pub(crate) async fn reclaim_table_objects(
        &self,
        history: TableHistoryId,
        limit: NonZeroUsize,
    ) -> StorageResult<bool> {
        let lease = self
            .lock_table_namespace(history, ArchiveAccess::Revoke)
            .await?;
        sync_directory(&self.root).await?;
        let metadata = match tokio::fs::symlink_metadata(&lease.path).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(true),
            Err(error) => return Err(archive_io(error)),
        };
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(corrupt_record());
        }
        let mut entries = match tokio::fs::read_dir(&lease.path).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(true),
            Err(error) => return Err(archive_io(error)),
        };
        for _ in 0..limit.get() {
            let Some(entry) = entries.next_entry().await.map_err(archive_io)? else {
                drop(entries);
                tokio::fs::remove_dir(&lease.path)
                    .await
                    .map_err(archive_io)?;
                sync_directory(&self.root).await?;
                return Ok(true);
            };
            let kind = entry.file_type().await.map_err(archive_io)?;
            if !kind.is_file() || kind.is_symlink() {
                return Err(corrupt_record());
            }
            tokio::fs::remove_file(entry.path())
                .await
                .map_err(archive_io)?;
        }
        sync_directory(&lease.path).await?;
        Ok(false)
    }

    async fn lock_table_namespace(
        &self,
        history: TableHistoryId,
        access: ArchiveAccess,
    ) -> StorageResult<TableArchiveLease> {
        let path = self.object_namespace(history);
        let authority = path.with_extension(crate::constants::ARCHIVE_AUTHORITY_EXTENSION);
        let file = tokio::task::spawn_blocking(move || lock_authority(authority, access))
            .await
            .map_err(|_| {
                storage_types::StorageError::internal("Archive authority lock failed")
            })??;
        Ok(TableArchiveLease {
            path,
            _authority: file,
        })
    }
}

fn lock_authority(path: PathBuf, access: ArchiveAccess) -> StorageResult<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(
            (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32,
        );
    }
    let mut file = options.open(path).map_err(archive_io)?;
    if !file.metadata().map_err(archive_io)?.is_file() {
        return Err(corrupt_record());
    }
    match access {
        ArchiveAccess::Publish => file.lock_shared().map_err(archive_io)?,
        ArchiveAccess::Revoke => file.lock().map_err(archive_io)?,
    }
    qualify_authority(&mut file, access)?;
    Ok(file)
}

fn qualify_authority(file: &mut File, access: ArchiveAccess) -> StorageResult<()> {
    let length = file.metadata().map_err(archive_io)?.len();
    let mut bytes = [0u8; 1];
    if length > 1 {
        return Err(corrupt_record());
    }
    if length == 1 {
        file.read_exact(&mut bytes).map_err(archive_io)?;
        if bytes != [1] {
            return Err(corrupt_record());
        }
    }
    match access {
        ArchiveAccess::Publish if length == 1 => {
            Err(storage_types::StorageEnum::HistoryUnavailable.into())
        }
        ArchiveAccess::Publish => Ok(()),
        ArchiveAccess::Revoke => {
            file.rewind().map_err(archive_io)?;
            file.write_all(&[1]).map_err(archive_io)?;
            file.sync_all().map_err(archive_io)
        }
    }
}
