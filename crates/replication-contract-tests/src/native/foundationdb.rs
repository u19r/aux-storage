//! Actual FDB clients share only this fixture's owned UUID subspace.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use infrastructure_provider::physical_store::PhysicalRecordStore;
use kv_backend::{
    KvAttempt as _, KvBackend as _, KvFaultProbe, KvMutation, KvStore,
    foundationdb::FoundationDbBackend,
};
use storage_types::AttemptMode;

use super::fixture;
use crate::HarnessResult;

pub struct FoundationDb {
    pub store: KvStore<FoundationDbBackend>,
    pub cluster_file: PathBuf,
    pub prefix: [u8; 16],
}

impl FoundationDb {
    pub async fn try_new() -> HarnessResult<Self> {
        let cluster_file = select_cluster_file()?;
        let prefix = uuid::Uuid::new_v4().into_bytes();
        let store = Self::open_store(&cluster_file, &prefix, None).await?;
        Ok(Self {
            store,
            cluster_file,
            prefix,
        })
    }

    pub async fn open_store(
        cluster_file: &Path,
        prefix: &[u8; 16],
        probe: Option<Arc<dyn KvFaultProbe>>,
    ) -> HarnessResult<KvStore<FoundationDbBackend>> {
        let backend = open_backend(cluster_file, prefix).await?;
        let backend = match probe {
            Some(probe) => backend.with_fault_probe(probe),
            None => backend,
        };
        let store = KvStore::initialize_format(backend).await?;
        store.initialize_records(fixture::limits()?).await?;
        Ok(store)
    }

    pub async fn open_independent_store(&self) -> HarnessResult<KvStore<FoundationDbBackend>> {
        Self::open_store(&self.cluster_file, &self.prefix, None).await
    }

    /// Reap fixture actors before clearing; this never changes cluster
    /// configuration.
    pub async fn finish(self) -> HarnessResult<()> {
        drop(self.store);
        let backend = open_backend(&self.cluster_file, &self.prefix).await?;
        backend
            .with_attempt(AttemptMode::ReadWrite, |native| {
                Box::pin(async move {
                    native.native_stage(vec![KvMutation::DeleteRange {
                        begin: vec![],
                        end: vec![255],
                    }])
                })
            })
            .await?;
        Ok(())
    }
}

fn select_cluster_file() -> HarnessResult<PathBuf> {
    let path = match std::env::var_os("TEST_FDB_CLUSTER_FILE") {
        Some(path) => PathBuf::from(path),
        None => [
            "/usr/local/etc/foundationdb/fdb.cluster",
            "/etc/foundationdb/fdb.cluster",
        ]
        .into_iter()
        .map(PathBuf::from)
        .find(|path| path.is_file())
        .ok_or(
            "selected FoundationDB contracts require TEST_FDB_CLUSTER_FILE or a local cluster file",
        )?,
    };
    // Check readability without reading or printing the cluster
    // description/identifier.
    std::fs::File::open(&path).map_err(|_| "selected FoundationDB cluster file is not readable")?;
    Ok(std::fs::canonicalize(path)?)
}

async fn open_backend(
    cluster_file: &Path,
    prefix: &[u8; 16],
) -> HarnessResult<FoundationDbBackend> {
    let path = cluster_file
        .to_str()
        .ok_or("FoundationDB cluster file path is not UTF-8")?;
    FoundationDbBackend::initialize(kv_backend::foundationdb::FoundationDbOptions {
        cluster_file: Some(path),
        subspace_prefix: Some(prefix),
        ..Default::default()
    })
    .await
    .map_err(|error| {
        let (kind, reason) = storage_common::diagnostics::storage_error_diagnostic(&error);
        format!("FoundationDB fixture initialization failed: {kind}: {reason}").into()
    })
}
