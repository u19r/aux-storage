use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use openraft::{
    error::{RPCError, RaftError, Unreachable},
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};

use crate::{SyncNode, SyncNodeId, SyncTypeConfig};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncRaftTransportError {
    message: String,
}

impl SyncRaftTransportError {
    #[must_use]
    pub fn unreachable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Display for SyncRaftTransportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for SyncRaftTransportError {}

#[async_trait]
pub trait SyncRaftRpcClient: Send + Sync + 'static {
    async fn append_entries(
        &self,
        target: SyncNodeId,
        node: &SyncNode,
        rpc: AppendEntriesRequest<SyncTypeConfig>,
    ) -> Result<AppendEntriesResponse<SyncNodeId>, SyncRaftTransportError>;

    async fn install_snapshot(
        &self,
        target: SyncNodeId,
        node: &SyncNode,
        rpc: InstallSnapshotRequest<SyncTypeConfig>,
    ) -> Result<InstallSnapshotResponse<SyncNodeId>, SyncRaftTransportError>;

    async fn vote(
        &self,
        target: SyncNodeId,
        node: &SyncNode,
        rpc: VoteRequest<SyncNodeId>,
    ) -> Result<VoteResponse<SyncNodeId>, SyncRaftTransportError>;
}

#[derive(Clone)]
pub struct SyncRaftNetworkFactory {
    client: Arc<dyn SyncRaftRpcClient>,
}

impl SyncRaftNetworkFactory {
    #[must_use]
    pub fn new(client: Arc<dyn SyncRaftRpcClient>) -> Self {
        Self { client }
    }
}

impl RaftNetworkFactory<SyncTypeConfig> for SyncRaftNetworkFactory {
    type Network = SyncRaftNetwork;

    async fn new_client(&mut self, target: SyncNodeId, node: &SyncNode) -> Self::Network {
        SyncRaftNetwork {
            target,
            node: node.clone(),
            client: self.client.clone(),
        }
    }
}

pub struct SyncRaftNetwork {
    target: SyncNodeId,
    node: SyncNode,
    client: Arc<dyn SyncRaftRpcClient>,
}

impl SyncRaftNetwork {
    fn unreachable<E>(&self, error: SyncRaftTransportError) -> RPCError<SyncNodeId, SyncNode, E>
    where E: std::error::Error {
        RPCError::Unreachable(Unreachable::new(&std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            format!("sync raft node {} unreachable: {error}", self.target),
        )))
    }
}

impl RaftNetwork<SyncTypeConfig> for SyncRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<SyncTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<SyncNodeId>,
        RPCError<SyncNodeId, SyncNode, RaftError<SyncNodeId>>,
    > {
        self.client
            .append_entries(self.target, &self.node, rpc)
            .await
            .map_err(|error| self.unreachable(error))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<SyncTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<SyncNodeId>,
        RPCError<
            SyncNodeId,
            SyncNode,
            RaftError<SyncNodeId, openraft::error::InstallSnapshotError>,
        >,
    > {
        self.client
            .install_snapshot(self.target, &self.node, rpc)
            .await
            .map_err(|error| self.unreachable(error))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<SyncNodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<SyncNodeId>, RPCError<SyncNodeId, SyncNode, RaftError<SyncNodeId>>>
    {
        self.client
            .vote(self.target, &self.node, rpc)
            .await
            .map_err(|error| self.unreachable(error))
    }
}
