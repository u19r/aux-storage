//! One native RocksDB owner in a fixture-owned temporary directory.
//!
//! Concurrent logical actors share this store through `Arc`; they do not open
//! another native handle. Process handoff awaits settled attempts and drops the
//! final store owner before the child opens the path, then kills and reaps the
//! child before reopening. The fixture retains the directory through handoff.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use infrastructure_provider::physical_store::PhysicalRecordStore;
use kv_backend::{KvFaultProbe, KvStore, rocksdb::RocksDbBackend};

use super::fixture;
use crate::HarnessResult;

pub struct RocksDb {
    pub store: KvStore<RocksDbBackend>,
    pub path: PathBuf,
    _directory: tempfile::TempDir,
}

impl RocksDb {
    pub async fn try_new() -> HarnessResult<Self> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("records.rocksdb");
        let store = Self::open_store(&path).await?;
        Ok(Self {
            store,
            path,
            _directory: directory,
        })
    }

    pub async fn open_store(path: &Path) -> HarnessResult<KvStore<RocksDbBackend>> {
        Self::open_with_probe(path, None).await
    }

    pub async fn open_store_with_fault_probe(
        path: &Path,
        probe: Arc<dyn KvFaultProbe>,
    ) -> HarnessResult<KvStore<RocksDbBackend>> {
        Self::open_with_probe(path, Some(probe)).await
    }

    async fn open_with_probe(
        path: &Path,
        probe: Option<Arc<dyn KvFaultProbe>>,
    ) -> HarnessResult<KvStore<RocksDbBackend>> {
        let backend = RocksDbBackend::initialize(path.to_owned())?;
        let backend = if let Some(probe) = probe {
            backend.with_fault_probe(probe)
        } else {
            backend
        };
        let store = KvStore::initialize_format(backend).await?;
        store.initialize_records(fixture::limits()?).await?;
        Ok(store)
    }
}
