use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use http_error::HttpApiError;
use openraft::{
    error::{InstallSnapshotError, RaftError},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use storage::DatabaseManager;
use storage_sync::{
    SYNC_LEADER_HINT_HEADER, SYNC_NOT_LEADER_ERROR_TYPE, SyncBackendPairDecision,
    SyncHealthResponse, SyncLeaderForward, SyncLeaderForwardDecision, SyncMutationResolver,
    SyncNode, SyncNodeId, SyncNotLeader, SyncProposalResponse, SyncRaftRequest, SyncRaftRuntime,
    SyncTypeConfig, SyncWriteProposalRequest, plan_leader_forward, plan_sync_backend_pair_detailed,
};

use crate::{
    manager::{
        SyncHealthReporter, SyncReadBarrier, SyncWriteProposer,
        sync_raft_proposal_coalescer::{
            DEFAULT_SYNC_PROPOSAL_COALESCING_WINDOW, SyncRaftProposalCoalescer,
        },
    },
    types::{
        SyncLearnerJoinHandler, SyncLearnerJoinRequest, SyncLearnerJoinResponse,
        SyncLearnerPromotionResponse, SyncRaftRpcHandler,
    },
};

pub struct SyncRaftRuntimeAdapter {
    db: Arc<DatabaseManager>,
    runtime: SyncRaftRuntime,
    proposal_coalescer: SyncRaftProposalCoalescer,
}

impl SyncRaftRuntimeAdapter {
    #[must_use]
    pub fn new(db: Arc<DatabaseManager>, runtime: SyncRaftRuntime) -> Self {
        Self::new_with_coalescing_window(db, runtime, DEFAULT_SYNC_PROPOSAL_COALESCING_WINDOW)
    }

    #[must_use]
    pub fn new_with_coalescing_window(
        db: Arc<DatabaseManager>,
        runtime: SyncRaftRuntime,
        coalescing_window: Duration,
    ) -> Self {
        Self {
            db,
            runtime,
            proposal_coalescer: SyncRaftProposalCoalescer::new(coalescing_window),
        }
    }
}

#[async_trait]
impl SyncWriteProposer for SyncRaftRuntimeAdapter {
    async fn propose_sync_write(
        &self,
        request: SyncWriteProposalRequest,
    ) -> Result<SyncProposalResponse, HttpApiError> {
        let proposal = self
            .db
            .resolve_sync_mutation(request)
            .await
            .map_err(HttpApiError::from)?;
        self.proposal_coalescer
            .propose(proposal, |batch| {
                let runtime = self.runtime.clone();
                async move {
                    runtime
                        .try_propose(SyncRaftRequest::new(batch))
                        .await
                        .map(|response| response.responses)
                        .map_err(|error| {
                            error
                                .forward_to_leader::<SyncNode>()
                                .map(not_leader_error)
                                .unwrap_or_else(|| {
                                    HttpApiError::internal_server_error(format!(
                                        "propose sync raft command: {error}"
                                    ))
                                })
                        })
                }
            })
            .await
    }
}

#[async_trait]
impl SyncReadBarrier for SyncRaftRuntimeAdapter {
    async fn ensure_linearizable_read(&self) -> Result<(), HttpApiError> {
        self.runtime
            .try_ensure_linearizable()
            .await
            .map_err(|error| {
                error
                    .forward_to_leader::<SyncNode>()
                    .map(not_leader_error)
                    .unwrap_or_else(|| {
                        HttpApiError::internal_server_error(format!(
                            "sync raft read-index failed: {error}"
                        ))
                    })
            })
    }
}

#[async_trait]
impl SyncHealthReporter for SyncRaftRuntimeAdapter {
    async fn sync_health(&self) -> Result<SyncHealthResponse, HttpApiError> {
        Ok(self.runtime.health_snapshot())
    }
}

#[async_trait]
impl SyncLearnerJoinHandler for SyncRaftRuntimeAdapter {
    async fn add_sync_learner(
        &self,
        request: SyncLearnerJoinRequest,
    ) -> Result<SyncLearnerJoinResponse, HttpApiError> {
        validate_backend_pair(self.runtime.backend_compatibility(), &request)?;
        let log_index = self
            .runtime
            .add_learner(
                request.node_id,
                SyncNode::new(request.advertise_url.trim().to_string()),
            )
            .await
            .map_err(|error| {
                error
                    .forward_to_leader::<SyncNode>()
                    .map(not_leader_error)
                    .unwrap_or_else(|| {
                        HttpApiError::internal_server_error(format!("add sync learner: {error}"))
                    })
            })?;
        Ok(SyncLearnerJoinResponse {
            node_id: request.node_id,
            log_index,
        })
    }

    async fn promote_sync_learner(
        &self,
        node_id: SyncNodeId,
    ) -> Result<SyncLearnerPromotionResponse, HttpApiError> {
        let log_index = self
            .runtime
            .promote_learner(node_id)
            .await
            .map_err(|error| {
                error
                    .forward_to_leader::<SyncNode>()
                    .map(not_leader_error)
                    .unwrap_or_else(|| {
                        HttpApiError::internal_server_error(format!(
                            "promote sync learner {node_id}: {error}"
                        ))
                    })
            })?;
        Ok(SyncLearnerPromotionResponse { node_id, log_index })
    }
}

pub(crate) fn validate_backend_pair(
    local_backend: Option<&str>,
    request: &SyncLearnerJoinRequest,
) -> Result<(), HttpApiError> {
    let Some(remote_backend) = request.backend_compatibility.as_deref() else {
        return Ok(());
    };
    let Some(local_backend) = local_backend else {
        return Ok(());
    };
    let plan = plan_sync_backend_pair_detailed(local_backend, remote_backend);
    if plan.decision == SyncBackendPairDecision::Rejected {
        return Err(HttpApiError::validation_error(format!(
            "unsupported sync backend pair: source={local_backend}, destination={remote_backend}, \
             reason={}",
            plan.reason.as_str()
        )));
    }
    Ok(())
}

fn not_leader_error(
    forward: &openraft::error::ForwardToLeader<SyncNodeId, SyncNode>,
) -> HttpApiError {
    let decision = plan_leader_forward(SyncLeaderForward {
        local_is_leader: false,
        leader_hint: forward.leader_node.as_ref().map(|node| node.addr.clone()),
    });
    match decision {
        SyncLeaderForwardDecision::Serve => HttpApiError::internal_server_error(
            "sync raft forward-to-leader unexpectedly resolved to serve locally",
        ),
        SyncLeaderForwardDecision::NotLeader { leader_hint } => {
            let error = SyncNotLeader::new(leader_hint.clone());
            let mut api_error =
                HttpApiError::dynamodb_error(SYNC_NOT_LEADER_ERROR_TYPE, error.message(), 500);
            if let Some(leader_hint) = leader_hint {
                api_error = api_error.with_response_header(SYNC_LEADER_HINT_HEADER, leader_hint);
            }
            api_error
        }
    }
}

#[async_trait]
impl SyncRaftRpcHandler for SyncRaftRuntimeAdapter {
    async fn append_entries(
        &self,
        request: AppendEntriesRequest<SyncTypeConfig>,
    ) -> Result<AppendEntriesResponse<storage_sync::SyncNodeId>, RaftError<storage_sync::SyncNodeId>>
    {
        self.runtime.append_entries(request).await
    }

    async fn install_snapshot(
        &self,
        request: InstallSnapshotRequest<SyncTypeConfig>,
    ) -> Result<
        InstallSnapshotResponse<storage_sync::SyncNodeId>,
        RaftError<storage_sync::SyncNodeId, InstallSnapshotError>,
    > {
        self.runtime.install_snapshot(request).await
    }

    async fn vote(
        &self,
        request: VoteRequest<storage_sync::SyncNodeId>,
    ) -> Result<VoteResponse<storage_sync::SyncNodeId>, RaftError<storage_sync::SyncNodeId>> {
        self.runtime.vote(request).await
    }
}
