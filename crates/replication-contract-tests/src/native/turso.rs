//! Real local MVCC fixtures with one process owning each database file.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use infrastructure_provider::physical_store::PhysicalRecordStore;
use sql_backend::{SqlFaultProbe, SqlStore, turso::TursoBackend};
use storage_types::StorageError;

use super::fixture;
use crate::HarnessResult;

/// Contending requests share the store's native writer queue. Before a child
/// opens the path, drop every store and settle any outstanding native attempt.
pub struct FileTurso {
    pub store: SqlStore<TursoBackend>,
    pub path: PathBuf,
    _directory: tempfile::TempDir,
}

impl FileTurso {
    pub async fn try_new() -> HarnessResult<Self> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("physical.turso");
        let store = Self::open_store(&path).await?;
        Ok(Self {
            store,
            path,
            _directory: directory,
        })
    }

    pub async fn open_store(path: &Path) -> HarnessResult<SqlStore<TursoBackend>> {
        Self::open_configured_store(path, None).await
    }

    pub async fn open_store_with_fault_probe(
        path: &Path,
        probe: Arc<dyn SqlFaultProbe>,
    ) -> HarnessResult<SqlStore<TursoBackend>> {
        Self::open_configured_store(path, Some(probe)).await
    }

    async fn open_configured_store(
        path: &Path,
        probe: Option<Arc<dyn SqlFaultProbe>>,
    ) -> HarnessResult<SqlStore<TursoBackend>> {
        let path = path
            .to_str()
            .ok_or_else(|| StorageError::internal("Turso fixture path is not valid UTF-8"))?;
        let backend = TursoBackend::initialize(path).await?;
        let backend = if let Some(probe) = probe {
            backend.with_fault_probe(probe)
        } else {
            backend
        };
        let store = SqlStore::initialize_schema(backend).await?;
        store.initialize_records(fixture::limits()?).await?;
        Ok(store)
    }
}
