use std::{collections::BTreeMap, fmt, io::Cursor, sync::Arc};

use openraft::{
    BasicNode, Entry, EntryPayload, LogId, SnapshotMeta, StoredMembership, TokioRuntime, Vote,
    storage::{
        LogFlushed, LogState, RaftLogReader, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine,
        Snapshot,
    },
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::{
    cluster_metrics,
    cluster_model::{ClusterState, RingState},
    cluster_transition::ClusterTransition,
};

// ---------------------------------------------------------------------------
// Type-config
// ---------------------------------------------------------------------------

openraft::declare_raft_types!(
    pub CacheTypeConfig:
        D            = CacheRequest,
        R            = CacheResponse,
        NodeId       = u64,
        Node         = BasicNode,
        Entry        = Entry<CacheTypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = TokioRuntime,
);

// ---------------------------------------------------------------------------
// Client request / response (replicated via Raft log)
// ---------------------------------------------------------------------------

/// Client request replicated through the Raft log. Each variant maps to a
/// [`ClusterTransition`] applied to the [`CacheStateMachine`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CacheRequest {
    /// Begin migrating a shard to a different node.
    InitiateMigration { shard: u8, target: u8 },
    /// Drain the source and send a shard-transfer message.
    CompleteMigrationDrain { shard: u8 },
    /// Cancel an in-progress migration.
    AbortMigration { shard: u8 },
    /// Advance the epoch counter for a (node, shard) pair.
    BumpEpoch { node: u8, shard: u8 },
    /// Replace the hash-ring shard-to-node assignment.
    UpdateRing { assignment: BTreeMap<u8, u8> },
}

/// Response returned for each applied Raft log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheResponse {
    pub ok: bool,
}

impl Default for CacheResponse {
    fn default() -> Self {
        Self { ok: true }
    }
}

// ---------------------------------------------------------------------------
// Snapshot payload (serialized ClusterState)
// ---------------------------------------------------------------------------

/// Serializable snapshot of the cluster state for Raft snapshot transfer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheSnapshot {
    pub last_applied_log: Option<LogId<u64>>,
    pub last_membership: StoredMembership<u64, BasicNode>,
    pub ring: RingState,
}

// ---------------------------------------------------------------------------
// In-memory state machine
// ---------------------------------------------------------------------------

/// In-memory Raft state machine backed by [`ClusterState`].
///
/// Applies [`CacheRequest`] entries and maintains snapshot metadata for
/// leadership transfer and catch-up.
pub struct CacheStateMachine {
    pub last_applied_log: Option<LogId<u64>>,
    pub last_membership: StoredMembership<u64, BasicNode>,
    pub cluster: ClusterState,
    snapshot_idx: u64,
    current_snapshot: Option<CacheSnapshot>,
}

impl Default for CacheStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl CacheStateMachine {
    pub fn new() -> Self {
        Self {
            last_applied_log: None,
            last_membership: StoredMembership::default(),
            cluster: ClusterState::initial(),
            snapshot_idx: 0,
            current_snapshot: None,
        }
    }

    fn apply_request(&mut self, req: &CacheRequest) {
        let transition = match req {
            CacheRequest::InitiateMigration { shard, target } => {
                cluster_metrics::record_cluster_migration(*shard, 0, *target as u64);
                ClusterTransition::InitiateMigration {
                    shard: *shard,
                    target: *target,
                }
            }
            CacheRequest::CompleteMigrationDrain { shard } => {
                ClusterTransition::CompleteMigrationDrain { shard: *shard }
            }
            CacheRequest::AbortMigration { shard } => {
                ClusterTransition::AbortMigration { shard: *shard }
            }
            CacheRequest::BumpEpoch { node, shard } => ClusterTransition::BumpEpoch {
                node: *node,
                shard: *shard,
            },
            CacheRequest::UpdateRing { assignment } => {
                let mut new_ring = self.cluster.ring.clone();
                new_ring.assignment = assignment.iter().map(|(s, n)| (*s, *n)).collect();
                self.cluster.ring = new_ring;
                cluster_metrics::record_cluster_reconfigure("ring_update");
                return;
            }
        };
        if let Some(next) = self.cluster.try_apply(&transition) {
            self.cluster = next;
        }
    }
}

impl RaftSnapshotBuilder<CacheTypeConfig> for CacheStateMachine {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<CacheTypeConfig>, openraft::StorageError<u64>> {
        let snap = CacheSnapshot {
            last_applied_log: self.last_applied_log,
            last_membership: self.last_membership.clone(),
            ring: self.cluster.ring.clone(),
        };

        let data = serde_json::to_vec(&snap)
            .map_err(|e| openraft::StorageIOError::read_state_machine(&e))?;

        self.snapshot_idx += 1;

        let meta = SnapshotMeta {
            last_log_id: self.last_applied_log,
            last_membership: self.last_membership.clone(),
            snapshot_id: format!("snap-{}", self.snapshot_idx),
        };

        self.current_snapshot = Some(snap);

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<CacheTypeConfig> for CacheStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), openraft::StorageError<u64>>
    {
        Ok((self.last_applied_log, self.last_membership.clone()))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<CacheResponse>, openraft::StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<CacheTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();
        for entry in entries {
            self.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => {
                    responses.push(CacheResponse::default());
                }
                EntryPayload::Normal(req) => {
                    self.apply_request(&req);
                    responses.push(CacheResponse { ok: true });
                }
                EntryPayload::Membership(mem) => {
                    self.last_membership = StoredMembership::new(Some(entry.log_id), mem);
                    responses.push(CacheResponse::default());
                }
            }
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        // Return a clone-like builder; since we hold mutable ref,
        // openraft guarantees exclusive access.
        // We create a new state machine copy for the builder.
        CacheStateMachine {
            last_applied_log: self.last_applied_log,
            last_membership: self.last_membership.clone(),
            cluster: self.cluster.clone(),
            snapshot_idx: self.snapshot_idx,
            current_snapshot: self.current_snapshot.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, openraft::StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        _meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), openraft::StorageError<u64>> {
        let data = snapshot.into_inner();
        let snap: CacheSnapshot = serde_json::from_slice(&data)
            .map_err(|e| openraft::StorageIOError::read_snapshot(None, &e))?;

        self.last_applied_log = snap.last_applied_log;
        self.last_membership = snap.last_membership;
        self.cluster.ring = snap.ring;
        self.current_snapshot = Some(CacheSnapshot {
            last_applied_log: self.last_applied_log,
            last_membership: self.last_membership.clone(),
            ring: self.cluster.ring.clone(),
        });
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<CacheTypeConfig>>, openraft::StorageError<u64>> {
        match &self.current_snapshot {
            Some(snap) => {
                let data = serde_json::to_vec(snap)
                    .map_err(|e| openraft::StorageIOError::read_state_machine(&e))?;
                let meta = SnapshotMeta {
                    last_log_id: snap.last_applied_log,
                    last_membership: snap.last_membership.clone(),
                    snapshot_id: format!("snap-{}", self.snapshot_idx),
                };
                Ok(Some(Snapshot {
                    meta,
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// In-memory log storage
// ---------------------------------------------------------------------------

pub struct MemLogStore {
    inner: Arc<Mutex<MemLogStoreInner>>,
}

struct MemLogStoreInner {
    vote: Option<Vote<u64>>,
    log: BTreeMap<u64, Entry<CacheTypeConfig>>,
    last_purged: Option<LogId<u64>>,
    committed: Option<LogId<u64>>,
}

impl Clone for MemLogStore {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl Default for MemLogStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemLogStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MemLogStoreInner {
                vote: None,
                log: BTreeMap::new(),
                last_purged: None,
                committed: None,
            })),
        }
    }
}

impl RaftLogReader<CacheTypeConfig> for MemLogStore {
    async fn try_get_log_entries<RB: std::ops::RangeBounds<u64> + Clone + fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<CacheTypeConfig>>, openraft::StorageError<u64>> {
        let inner = self.inner.lock().await;
        Ok(inner.log.range(range).map(|(_, v)| v.clone()).collect())
    }
}

impl RaftLogStorage<CacheTypeConfig> for MemLogStore {
    type LogReader = Self;

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<CacheTypeConfig>, openraft::StorageError<u64>> {
        let inner = self.inner.lock().await;
        let last_log_id = inner
            .log
            .iter()
            .next_back()
            .map(|(_, e)| e.log_id)
            .or(inner.last_purged);
        Ok(LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), openraft::StorageError<u64>> {
        let mut inner = self.inner.lock().await;
        inner.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, openraft::StorageError<u64>> {
        let inner = self.inner.lock().await;
        Ok(inner.vote)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<CacheTypeConfig>,
    ) -> Result<(), openraft::StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<CacheTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut inner = self.inner.lock().await;
        for entry in entries {
            inner.log.insert(entry.log_id.index, entry);
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), openraft::StorageError<u64>> {
        let mut inner = self.inner.lock().await;
        let to_remove: Vec<u64> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for k in to_remove {
            inner.log.remove(&k);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), openraft::StorageError<u64>> {
        let mut inner = self.inner.lock().await;
        let to_remove: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for k in to_remove {
            inner.log.remove(&k);
        }
        inner.last_purged = Some(log_id);
        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), openraft::StorageError<u64>> {
        let mut inner = self.inner.lock().await;
        inner.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, openraft::StorageError<u64>> {
        let inner = self.inner.lock().await;
        Ok(inner.committed)
    }
}
