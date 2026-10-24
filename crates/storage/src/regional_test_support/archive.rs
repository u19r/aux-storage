use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result};

use super::{RegionalFixture, await_item};

pub fn archive_namespace(root: &Path) -> Result<PathBuf> {
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() && entry.file_name().to_string_lossy().starts_with("native-")
        {
            return Ok(entry.path());
        }
    }
    anyhow::bail!("initialized archive namespace missing")
}

pub async fn await_archived_original(namespace: &Path, original: &[u8]) -> Result<PathBuf> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    while tokio::time::Instant::now() < deadline {
        for entry in std::fs::read_dir(namespace)? {
            let entry = entry?;
            let name = entry.file_name();
            if name.to_string_lossy().len() == 64
                && entry.file_type()?.is_file()
                && std::fs::read(entry.path())? == original
            {
                return Ok(entry.path());
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    anyhow::bail!("canonical original was not published to the configured archive")
}

pub async fn await_archive_handoff(
    fixture: &RegionalFixture,
    selection: (usize, &str, &str),
    paths: (&Path, &Path),
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    while tokio::time::Instant::now() < deadline {
        std::fs::rename(paths.0, paths.1)?;
        let read =
            super::history::read_originals(&fixture.managers[0], 1, selection.0, selection.1).await;
        std::fs::rename(paths.1, paths.0)?;
        if let Err(error) = read {
            let error = error
                .downcast_ref::<storage_types::StorageError>()
                .context("history failed outside its storage boundary")?;
            anyhow::ensure!(
                matches!(
                    error.as_ref(),
                    storage_types::StorageEnum::HistoryUnavailable
                ),
                "missing archive object returned the wrong error: {error:?}"
            );
            await_item(&fixture.managers[0], selection.1, selection.2).await?;
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    anyhow::bail!("verified archive publication never replaced hot canonical custody")
}
