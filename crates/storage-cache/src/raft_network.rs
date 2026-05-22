use std::{collections::HashMap, sync::Arc};

use openraft::{
    BasicNode, Raft,
    error::{RPCError, RaftError, RemoteError, Unreachable},
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use tokio::sync::RwLock;

use crate::raft_types::CacheTypeConfig;

type CacheRaft = Raft<CacheTypeConfig>;

/// A shared registry of Raft node handles, keyed by node id.
pub type NodeRouter = Arc<RwLock<HashMap<u64, CacheRaft>>>;

/// Network implementation that routes RPCs via the in-process [`NodeRouter`].
pub struct ChannelNetwork {
    target: u64,
    router: NodeRouter,
}

impl ChannelNetwork {
    pub fn new(target: u64, router: NodeRouter) -> Self {
        Self { target, router }
    }

    async fn get_raft(&self) -> Result<CacheRaft, Unreachable> {
        let map = self.router.read().await;
        map.get(&self.target).cloned().ok_or_else(|| {
            Unreachable::new(&std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("node {} not in router", self.target),
            ))
        })
    }
}

impl RaftNetwork<CacheTypeConfig> for ChannelNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<CacheTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let raft = self.get_raft().await?;
        raft.append_entries(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<CacheTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, openraft::error::InstallSnapshotError>>,
    > {
        let raft = self.get_raft().await?;
        raft.install_snapshot(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let raft = self.get_raft().await?;
        raft.vote(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

/// Factory that creates [`ChannelNetwork`] instances.
pub struct ChannelNetworkFactory {
    router: NodeRouter,
}

impl ChannelNetworkFactory {
    pub fn new(router: NodeRouter) -> Self {
        Self { router }
    }
}

impl RaftNetworkFactory<CacheTypeConfig> for ChannelNetworkFactory {
    type Network = ChannelNetwork;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        ChannelNetwork::new(target, self.router.clone())
    }
}
