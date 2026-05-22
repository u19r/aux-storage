use std::io::Cursor;

use openraft::{BasicNode, Entry, TokioRuntime};
use serde::{Deserialize, Serialize};

use crate::{ResolvedSyncMutationBatch, SyncMutationResponse};

pub type SyncNodeId = u64;
pub type SyncNode = BasicNode;
pub type SyncSnapshotData = Cursor<Vec<u8>>;

openraft::declare_raft_types!(
    pub SyncTypeConfig:
        D            = SyncRaftRequest,
        R            = SyncRaftResponse,
        NodeId       = SyncNodeId,
        Node         = SyncNode,
        Entry        = Entry<SyncTypeConfig>,
        SnapshotData = SyncSnapshotData,
        AsyncRuntime = TokioRuntime,
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncRaftRequest {
    pub batch: ResolvedSyncMutationBatch,
}

impl SyncRaftRequest {
    #[must_use]
    pub const fn new(batch: ResolvedSyncMutationBatch) -> Self {
        Self { batch }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncRaftResponse {
    pub responses: Vec<SyncMutationResponse>,
}

impl SyncRaftResponse {
    #[must_use]
    pub const fn new(responses: Vec<SyncMutationResponse>) -> Self {
        Self { responses }
    }
}
