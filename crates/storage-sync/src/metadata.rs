use serde::{Deserialize, Serialize};
use storage_types::{ItemStreamVersion, TableName, TimestampMillis};

use crate::ResolvedSyncMutationBatch;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SyncLogId {
    pub term: u64,
    pub index: u64,
}

impl SyncLogId {
    #[must_use]
    pub const fn new(term: u64, index: u64) -> Self {
        Self { term, index }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncCommitMetadata {
    pub log_id: SyncLogId,
    pub committed_at: TimestampMillis,
    pub leader_node_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedSyncLogEntry {
    pub metadata: SyncCommitMetadata,
    pub batch: ResolvedSyncMutationBatch,
}

impl ResolvedSyncLogEntry {
    #[must_use]
    pub const fn new(metadata: SyncCommitMetadata, batch: ResolvedSyncMutationBatch) -> Self {
        Self { metadata, batch }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncItemBaseVersion {
    pub table_name: TableName,
    pub key_json: String,
    pub item_stream_version: Option<ItemStreamVersion>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncReadSet {
    pub items: Vec<SyncItemBaseVersion>,
}

impl SyncReadSet {
    #[must_use]
    pub fn new(items: Vec<SyncItemBaseVersion>) -> Self {
        Self { items }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}
