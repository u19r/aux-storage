use std::{collections::BTreeMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use config::StorageSyncReplicationConfig;
use http_request::reqwest::{Client, StatusCode, redirect::Policy};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use storage_sync::{
    SyncNode, SyncNodeId, SyncRaftRpcClient, SyncRaftTransportError, SyncTypeConfig,
};
use storage_types::{StorageError, StorageResult};

use crate::{
    constants::STORAGE_GATEWAY_API_KEY_HEADER,
    sync_raft_peer_status::classify_sync_raft_peer_status,
    types::{SyncLearnerJoinRequest, SyncLearnerJoinResponse},
};

const SYNC_RAFT_APPEND_PATH: &str = "/_internal/sync/raft/append";
const SYNC_RAFT_LEARNERS_PATH: &str = "/_internal/sync/raft/learners";
const SYNC_RAFT_SNAPSHOT_PATH: &str = "/_internal/sync/raft/snapshot";
const SYNC_RAFT_VOTE_PATH: &str = "/_internal/sync/raft/vote";

/// HTTP transport for the internal Raft peer surface.
///
/// The client disables redirects deliberately. Raft peer requests carry the
/// sync credential and must target the exact peer selected by OpenRaft or the
/// learner-join bootstrap rule, not an arbitrary redirected endpoint.
#[derive(Clone)]
pub struct HttpSyncRaftRpcClient {
    client: Client,
    peers: Arc<BTreeMap<SyncNodeId, SyncRaftPeerEndpoint>>,
    sync_internal_token: Arc<str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SyncRaftPeerEndpoint {
    base_url: String,
}

impl HttpSyncRaftRpcClient {
    pub fn from_config(config: &StorageSyncReplicationConfig) -> StorageResult<Self> {
        let token = config
            .sync_internal_token
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                StorageError::validation(
                    "features.storage_sync_replication.sync_internal_token is required for sync \
                     raft peer client",
                )
            })?;
        let mut peers = BTreeMap::new();
        for peer in &config.peers {
            peers.insert(
                peer.node_id,
                SyncRaftPeerEndpoint {
                    base_url: peer.endpoint_url.trim_end_matches('/').to_string(),
                },
            );
        }
        Self::new(peers, token)
    }

    fn new(
        peers: BTreeMap<SyncNodeId, SyncRaftPeerEndpoint>,
        sync_internal_token: impl Into<Arc<str>>,
    ) -> StorageResult<Self> {
        let client = Client::builder()
            .redirect(Policy::none())
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|error| {
                StorageError::internal(&format!("build sync raft http client: {error}"))
            })?;
        Ok(Self {
            client,
            peers: Arc::new(peers),
            sync_internal_token: sync_internal_token.into(),
        })
    }

    pub async fn request_learner_join(
        &self,
        target: SyncNodeId,
        request: &SyncLearnerJoinRequest,
    ) -> StorageResult<SyncLearnerJoinResponse> {
        self.post(target, None, SYNC_RAFT_LEARNERS_PATH, request)
            .await
            .map_err(|error| StorageError::internal(&format!("request sync learner join: {error}")))
    }

    async fn post<Req, Resp>(
        &self,
        target: SyncNodeId,
        node: Option<&SyncNode>,
        path: &str,
        request: &Req,
    ) -> Result<Resp, SyncRaftTransportError>
    where
        Req: serde::Serialize + Sync,
        Resp: serde::de::DeserializeOwned,
    {
        let base_url = self
            .peers
            .get(&target)
            .map(|peer| peer.base_url.as_str())
            .or_else(|| node.map(|node| node.addr.trim_end_matches('/')))
            .ok_or_else(|| {
                SyncRaftTransportError::unreachable(format!(
                    "sync raft peer {target} is not configured"
                ))
            })?;
        let url = format!("{base_url}{path}");
        let response = self
            .client
            .post(url.as_str())
            .header(
                STORAGE_GATEWAY_API_KEY_HEADER,
                self.sync_internal_token.as_ref(),
            )
            .json(request)
            .send()
            .await
            .map_err(|error| {
                SyncRaftTransportError::unreachable(format!(
                    "sync raft http request to node {target} failed: {error}"
                ))
            })?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(map_status_error(target, status, body));
        }
        response.json().await.map_err(|error| {
            SyncRaftTransportError::unreachable(format!(
                "sync raft http response from node {target} was invalid: {error}"
            ))
        })
    }
}

#[async_trait]
impl SyncRaftRpcClient for HttpSyncRaftRpcClient {
    async fn append_entries(
        &self,
        target: SyncNodeId,
        node: &SyncNode,
        rpc: AppendEntriesRequest<SyncTypeConfig>,
    ) -> Result<AppendEntriesResponse<SyncNodeId>, SyncRaftTransportError> {
        self.post(target, Some(node), SYNC_RAFT_APPEND_PATH, &rpc)
            .await
    }

    async fn install_snapshot(
        &self,
        target: SyncNodeId,
        node: &SyncNode,
        rpc: InstallSnapshotRequest<SyncTypeConfig>,
    ) -> Result<InstallSnapshotResponse<SyncNodeId>, SyncRaftTransportError> {
        self.post(target, Some(node), SYNC_RAFT_SNAPSHOT_PATH, &rpc)
            .await
    }

    async fn vote(
        &self,
        target: SyncNodeId,
        node: &SyncNode,
        rpc: VoteRequest<SyncNodeId>,
    ) -> Result<VoteResponse<SyncNodeId>, SyncRaftTransportError> {
        self.post(target, Some(node), SYNC_RAFT_VOTE_PATH, &rpc)
            .await
    }
}

fn map_status_error(
    target: SyncNodeId,
    status: StatusCode,
    body: String,
) -> SyncRaftTransportError {
    let kind = classify_sync_raft_peer_status(status.as_u16()).message();
    SyncRaftTransportError::unreachable(format!(
        "{kind} for node {target}: status={status}, body={body}"
    ))
}
