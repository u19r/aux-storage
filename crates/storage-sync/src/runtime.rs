use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use openraft::{
    Config, Raft, ServerState,
    error::{
        CheckIsLeaderError, ClientWriteError, InitializeError, InstallSnapshotError, RaftError,
    },
    network::RaftNetworkFactory,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    storage::{RaftLogStorage, RaftStateMachine},
};
use storage_backfill::LogicalBackfillImport;
use storage_types::{StorageError, StorageResult};

use crate::{
    SyncApply, SyncHealthResponse, SyncLearnerPromoter, SyncLearnerPromotionReport, SyncNode,
    SyncNodeId, SyncPeerHealth, SyncRaftRequest, SyncRaftResponse, SyncRaftRole,
    SyncRaftStateMachine, SyncTypeConfig,
};

#[derive(Clone)]
pub struct SyncRaftRuntime {
    node_id: SyncNodeId,
    raft: Raft<SyncTypeConfig>,
    leader_hint: Option<String>,
    backend_compatibility: Option<String>,
}

impl SyncRaftRuntime {
    pub async fn new<LS, N, A>(
        node_id: SyncNodeId,
        config: Arc<Config>,
        network: N,
        log_store: LS,
        apply: Arc<A>,
        leader_hint: Option<String>,
        backend_compatibility: Option<String>,
    ) -> StorageResult<Self>
    where
        LS: RaftLogStorage<SyncTypeConfig>,
        N: RaftNetworkFactory<SyncTypeConfig>,
        A: SyncApply + LogicalBackfillImport + 'static,
    {
        let state_machine = SyncRaftStateMachine::new(apply);
        Self::new_with_state_machine(
            node_id,
            config,
            network,
            log_store,
            state_machine,
            leader_hint,
            backend_compatibility,
        )
        .await
    }

    pub async fn new_with_state_machine<LS, N, SM>(
        node_id: SyncNodeId,
        config: Arc<Config>,
        network: N,
        log_store: LS,
        state_machine: SM,
        leader_hint: Option<String>,
        backend_compatibility: Option<String>,
    ) -> StorageResult<Self>
    where
        LS: RaftLogStorage<SyncTypeConfig>,
        N: RaftNetworkFactory<SyncTypeConfig>,
        SM: RaftStateMachine<SyncTypeConfig>,
    {
        let raft = Raft::new(node_id, config, network, log_store, state_machine)
            .await
            .map_err(|error| {
                StorageError::internal(&format!("start sync raft runtime: {error}"))
            })?;
        Ok(Self {
            node_id,
            raft,
            leader_hint,
            backend_compatibility,
        })
    }

    pub async fn initialize(&self, members: BTreeMap<SyncNodeId, SyncNode>) -> StorageResult<()> {
        self.raft
            .initialize(members)
            .await
            .map_err(|error| StorageError::internal(&format!("initialize sync raft: {error}")))
    }

    pub async fn initialize_if_needed(
        &self,
        members: BTreeMap<SyncNodeId, SyncNode>,
    ) -> StorageResult<()> {
        if self.raft.is_initialized().await.map_err(|error| {
            StorageError::internal(&format!("inspect sync raft initialization: {error}"))
        })? {
            return Ok(());
        }
        match self.raft.initialize(members).await {
            Ok(()) => Ok(()),
            Err(RaftError::APIError(InitializeError::NotAllowed(_))) => Ok(()),
            Err(error) => Err(StorageError::internal(&format!(
                "initialize sync raft: {error}"
            ))),
        }
    }

    pub async fn propose(&self, request: SyncRaftRequest) -> StorageResult<SyncRaftResponse> {
        self.try_propose(request)
            .await
            .map_err(|error| StorageError::internal(&format!("propose sync raft command: {error}")))
    }

    pub async fn try_propose(
        &self,
        request: SyncRaftRequest,
    ) -> Result<SyncRaftResponse, RaftError<SyncNodeId, ClientWriteError<SyncNodeId, SyncNode>>>
    {
        self.raft
            .client_write(request)
            .await
            .map(|response| response.data)
    }

    pub async fn add_learner(
        &self,
        node_id: SyncNodeId,
        node: SyncNode,
    ) -> Result<u64, RaftError<SyncNodeId, ClientWriteError<SyncNodeId, SyncNode>>> {
        self.raft
            .add_learner(node_id, node, false)
            .await
            .map(|response| response.log_id.index)
    }

    pub async fn promote_learner(
        &self,
        node_id: SyncNodeId,
    ) -> Result<u64, RaftError<SyncNodeId, ClientWriteError<SyncNodeId, SyncNode>>> {
        let metrics = self.raft.metrics().borrow().clone();
        let mut voters: BTreeSet<SyncNodeId> =
            metrics.membership_config.membership().voter_ids().collect();
        voters.insert(node_id);
        self.raft
            .change_membership(voters, true)
            .await
            .map(|response| response.log_id.index)
    }

    pub async fn ensure_linearizable(&self) -> StorageResult<()> {
        self.try_ensure_linearizable().await.map_err(|error| {
            StorageError::internal(&format!("sync raft read-index failed: {error}"))
        })
    }

    pub async fn try_ensure_linearizable(
        &self,
    ) -> Result<(), RaftError<SyncNodeId, CheckIsLeaderError<SyncNodeId, SyncNode>>> {
        self.raft.ensure_linearizable().await.map(|_| ())
    }

    pub async fn append_entries(
        &self,
        request: AppendEntriesRequest<SyncTypeConfig>,
    ) -> Result<AppendEntriesResponse<SyncNodeId>, RaftError<SyncNodeId>> {
        self.raft.append_entries(request).await
    }

    pub async fn install_snapshot(
        &self,
        request: InstallSnapshotRequest<SyncTypeConfig>,
    ) -> Result<InstallSnapshotResponse<SyncNodeId>, RaftError<SyncNodeId, InstallSnapshotError>>
    {
        self.raft.install_snapshot(request).await
    }

    pub async fn vote(
        &self,
        request: VoteRequest<SyncNodeId>,
    ) -> Result<VoteResponse<SyncNodeId>, RaftError<SyncNodeId>> {
        self.raft.vote(request).await
    }

    #[must_use]
    pub fn backend_compatibility(&self) -> Option<&str> {
        self.backend_compatibility.as_deref()
    }

    #[must_use]
    pub fn health_snapshot(&self) -> SyncHealthResponse {
        let metrics = self.raft.metrics().borrow().clone();
        let membership = metrics.membership_config.membership();
        let voters = membership.voter_ids().collect::<Vec<_>>();
        let learners = membership.learner_ids().collect::<Vec<_>>();
        let peers = metrics
            .replication
            .unwrap_or_default()
            .into_iter()
            .map(|(node_id, match_log_id)| {
                let match_index = match_log_id.map(|log_id| log_id.index);
                let lag_entries = match (metrics.last_log_index, match_index) {
                    (Some(last), Some(matched)) => Some(last.saturating_sub(matched)),
                    (Some(last), None) => Some(last),
                    _ => None,
                };
                SyncPeerHealth {
                    node_id,
                    match_index,
                    lag_entries,
                }
            })
            .collect();

        SyncHealthResponse {
            local_node_id: Some(self.node_id),
            role: role_from_server_state(metrics.state),
            known_leader: metrics.current_leader,
            term: Some(metrics.current_term),
            commit_index: metrics.last_log_index,
            applied_index: metrics.last_applied.map(|log_id| log_id.index),
            voters,
            learners,
            peers,
            preferred_leader: false,
            leader_hint: self.leader_hint.clone(),
            logical_catchup_status: None,
            backend_compatibility: self.backend_compatibility.clone(),
        }
    }

    pub async fn shutdown(&self) -> StorageResult<()> {
        self.raft.shutdown().await.map_err(|error| {
            StorageError::internal(&format!("shutdown sync raft runtime: {error}"))
        })
    }
}

#[async_trait::async_trait]
impl SyncLearnerPromoter for SyncRaftRuntime {
    async fn promote_sync_learner(
        &self,
        node_id: SyncNodeId,
    ) -> StorageResult<SyncLearnerPromotionReport> {
        let log_index = self.promote_learner(node_id).await.map_err(|error| {
            StorageError::internal(&format!("promote sync learner {node_id}: {error}"))
        })?;
        Ok(SyncLearnerPromotionReport { node_id, log_index })
    }
}

pub(crate) fn role_from_server_state(state: ServerState) -> SyncRaftRole {
    match state {
        ServerState::Learner => SyncRaftRole::Learner,
        ServerState::Follower => SyncRaftRole::Follower,
        ServerState::Candidate => SyncRaftRole::Candidate,
        ServerState::Leader => SyncRaftRole::Leader,
        ServerState::Shutdown => SyncRaftRole::Disabled,
    }
}
