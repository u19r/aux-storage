use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
};

use replication_types::protocol::CanonicalDigest;
use storage_types::{StorageError, StorageResult};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// One configured durable filesystem. Object addresses never come from peers.
pub(crate) struct CanonicalArchive {
    root: PathBuf,
    identity: [u8; 16],
    physical_identity: [u8; 16],
    maximum_object_bytes: NonZeroUsize,
}

/// The metadata belongs to the admitted regular-file handle, not a later path
/// lookup.
struct ArchiveFile {
    file: tokio::fs::File,
    metadata: std::fs::Metadata,
}

impl CanonicalArchive {
    pub(crate) async fn try_new(
        root: &Path,
        physical_identity: [u8; 16],
        maximum_object_bytes: NonZeroUsize,
    ) -> StorageResult<Self> {
        if physical_identity == [0; 16] {
            return Err(crate::native_record::corrupt_record());
        }
        if !root.is_absolute() {
            return Err(StorageError::validation("archive root must be absolute"));
        }
        let root = tokio::fs::canonicalize(root).await.map_err(archive_io)?;
        if !tokio::fs::metadata(&root)
            .await
            .map_err(archive_io)?
            .is_dir()
        {
            return Err(StorageError::validation("archive root must be a directory"));
        }
        let root = prepare_archive_namespace(&root, physical_identity).await?;
        let mut declaration = Vec::with_capacity(40);
        declaration.extend_from_slice(crate::constants::ARCHIVE_IDENTITY_FORMAT);
        declaration.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
        declaration.extend_from_slice(&physical_identity);
        let path = root.join(crate::constants::ARCHIVE_IDENTITY_FILE);
        publish_file(&root, &path, &declaration).await?;
        let ArchiveFile { mut file, metadata } = ArchiveFile::load(&path).await?;
        if metadata.len() != 40 {
            return Err(crate::native_record::corrupt_record());
        }
        let mut bytes = Vec::with_capacity(40);
        (&mut file)
            .take(41)
            .read_to_end(&mut bytes)
            .await
            .map_err(archive_io)?;
        if bytes.len() != 40
            || bytes.get(..8) != Some(crate::constants::ARCHIVE_IDENTITY_FORMAT.as_slice())
        {
            return Err(crate::native_record::corrupt_record());
        }
        let identity: [u8; 16] = bytes[8..24]
            .try_into()
            .map_err(|_| crate::native_record::corrupt_record())?;
        if bytes[24..] != physical_identity || identity == [0; 16] {
            return Err(crate::native_record::corrupt_record());
        }
        Ok(Self {
            root,
            identity,
            physical_identity,
            maximum_object_bytes,
        })
    }

    pub(crate) fn physical_identity(&self) -> [u8; 16] {
        self.physical_identity
    }

    pub(crate) fn identity(&self) -> [u8; 16] {
        self.identity
    }

    pub(crate) async fn fetch_object(
        &self,
        digest: CanonicalDigest,
        length: usize,
    ) -> StorageResult<Arc<[u8]>> {
        self.require_object_length(length)?;
        let ArchiveFile { mut file, metadata } =
            ArchiveFile::load(&self.object_path(digest)).await?;
        if usize::try_from(metadata.len()).ok() != Some(length) {
            return Err(crate::native_record::corrupt_record());
        }
        let bound = u64::try_from(length)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(crate::native_record::corrupt_record)?;
        let mut bytes = Vec::with_capacity(length);
        (&mut file)
            .take(bound)
            .read_to_end(&mut bytes)
            .await
            .map_err(archive_io)?;
        if bytes.len() != length {
            return Err(crate::native_record::corrupt_record());
        }
        digest
            .verify(&bytes)
            .map_err(|_| crate::native_record::corrupt_record())?;
        Ok(bytes.into())
    }

    pub(crate) async fn publish_object(
        &self,
        digest: CanonicalDigest,
        bytes: &[u8],
    ) -> StorageResult<super::manifest::VerifiedObject> {
        self.require_object_length(bytes.len())?;
        digest
            .verify(bytes)
            .map_err(|_| crate::native_record::corrupt_record())?;
        publish_file(&self.root, &self.object_path(digest), bytes).await?;
        self.fetch_object(digest, bytes.len()).await?;
        Ok(super::manifest::VerifiedObject::new(
            self.identity,
            digest,
            bytes.len(),
        ))
    }

    fn object_path(&self, digest: CanonicalDigest) -> PathBuf {
        let digits = crate::constants::ARCHIVE_HEX_DIGITS;
        let mut name = String::with_capacity(64);
        for byte in digest.as_ref() {
            name.push(char::from(digits[(byte >> 4) as usize]));
            name.push(char::from(digits[(byte & 15) as usize]));
        }
        self.root.join(name)
    }

    pub(super) fn require_object_length(&self, length: usize) -> StorageResult<()> {
        if length == 0 || length > self.maximum_object_bytes.get() {
            return Err(crate::native_record::corrupt_record());
        }
        Ok(())
    }
}

async fn prepare_archive_namespace(
    root: &Path,
    physical_identity: [u8; 16],
) -> StorageResult<PathBuf> {
    let namespace = root.join(format!(
        "native-{}",
        uuid::Uuid::from_bytes(physical_identity).simple()
    ));
    match tokio::fs::create_dir(&namespace).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(archive_io(error)),
    }
    sync_directory(root).await?;
    let metadata = tokio::fs::symlink_metadata(&namespace)
        .await
        .map_err(archive_io)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(crate::native_record::corrupt_record());
    }
    let resolved = tokio::fs::canonicalize(&namespace)
        .await
        .map_err(archive_io)?;
    if resolved.parent() != Some(root) {
        return Err(crate::native_record::corrupt_record());
    }
    Ok(resolved)
}

async fn publish_file(root: &Path, destination: &Path, bytes: &[u8]) -> StorageResult<()> {
    write_file(root, destination, bytes).await?;
    sync_directory(root).await
}

async fn write_file(root: &Path, destination: &Path, bytes: &[u8]) -> StorageResult<()> {
    let root = root.to_path_buf();
    // Creation owns its cleanup guard inside the worker. Cancellation before
    // transfer drops the worker's returned file rather than leaking its path.
    let temporary = tokio::task::spawn_blocking(move || {
        tempfile::Builder::new()
            .prefix(".archive-")
            .suffix(".pending")
            .tempfile_in(root)
    })
    .await
    .map_err(|_| StorageError::internal("Archive temporary creation failed"))?
    .map_err(archive_io)?;
    let (file, temporary) = temporary.into_parts();
    let mut file = tokio::fs::File::from_std(file);
    file.write_all(bytes).await.map_err(archive_io)?;
    file.sync_all().await.map_err(archive_io)?;
    match tokio::fs::hard_link(&temporary, destination).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            ArchiveFile::load(destination)
                .await?
                .file
                .sync_all()
                .await
                .map_err(archive_io)
        }
        Err(error) => Err(archive_io(error)),
    }?;
    drop(file);
    // Normal cleanup reports unlink failures. On error or cancellation before
    // this point, TempPath owns best-effort cleanup without a detached async
    // task.
    tokio::task::spawn_blocking(move || temporary.close())
        .await
        .map_err(|_| StorageError::internal("Archive temporary cleanup failed"))?
        .map_err(archive_io)
}

/// Reject malformed entries before open; Unix flags also cover entry
/// replacement between the metadata observation and the opened-handle
/// qualification.
impl ArchiveFile {
    async fn load(path: &Path) -> StorageResult<Self> {
        let metadata = tokio::fs::symlink_metadata(path)
            .await
            .map_err(archive_io)?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(crate::native_record::corrupt_record());
        }
        let mut options = tokio::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        options.custom_flags(
            (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32,
        );
        let file = options.open(path).await.map_err(archive_io)?;
        let metadata = file.metadata().await.map_err(archive_io)?;
        if !metadata.is_file() {
            return Err(crate::native_record::corrupt_record());
        }
        Ok(Self { file, metadata })
    }
}

async fn sync_directory(path: &Path) -> StorageResult<()> {
    let metadata = tokio::fs::symlink_metadata(path)
        .await
        .map_err(archive_io)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(crate::native_record::corrupt_record());
    }
    let mut options = tokio::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(
        (rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::NONBLOCK)
            .bits() as i32,
    );
    let directory = options.open(path).await.map_err(archive_io)?;
    if !directory.metadata().await.map_err(archive_io)?.is_dir() {
        return Err(crate::native_record::corrupt_record());
    }
    directory.sync_all().await.map_err(archive_io)
}

fn archive_io(_: std::io::Error) -> StorageError {
    storage_types::StorageEnum::HistoryUnavailable.into()
}
