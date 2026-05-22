use std::io::Cursor;

use openraft::{Snapshot, SnapshotMeta};
use serde::{Deserialize, Serialize};
use storage_backfill::{LogicalBackfillChunk, LogicalBackfillManifest};
use storage_types::{StorageError, StorageResult};

use crate::SyncTypeConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncSnapshotInstallPhase {
    Idle,
    RaftRecovering,
    Installed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncRaftSnapshotPayload {
    pub manifest: LogicalBackfillManifest,
    pub chunks: Vec<LogicalBackfillChunk>,
}

impl SyncRaftSnapshotPayload {
    #[must_use]
    pub const fn new(manifest: LogicalBackfillManifest, chunks: Vec<LogicalBackfillChunk>) -> Self {
        Self { manifest, chunks }
    }

    pub fn into_snapshot(
        self,
        meta: SnapshotMeta<u64, openraft::BasicNode>,
    ) -> StorageResult<Snapshot<SyncTypeConfig>> {
        let bytes = serde_json::to_vec(&self)?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }

    pub fn from_snapshot_bytes(bytes: &[u8]) -> StorageResult<Self> {
        serde_json::from_slice(bytes).map_err(StorageError::from)
    }
}
