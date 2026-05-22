use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use axum::Router;
use config::{StorageSyncReplicationConfig, StorageSyncReplicationPeerConfig};
use openraft::{
    Vote,
    error::{InstallSnapshotError, RaftError},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use storage::DatabaseManager;
use storage_sync::{SyncNode, SyncNodeId, SyncRaftRpcClient, SyncTypeConfig};
use tokio::net::TcpListener;

use crate::{
    AppState, HttpSyncRaftRpcClient, StorageApiManagerOptions, server_router,
    types::{
        SyncLearnerJoinHandler, SyncLearnerJoinRequest, SyncLearnerJoinResponse,
        SyncLearnerPromotionResponse, SyncRaftRpcHandler,
    },
};

struct RecordingSyncRaftRpcHandler {
    votes: AtomicUsize,
    learners: AtomicUsize,
}

#[async_trait]
impl SyncRaftRpcHandler for RecordingSyncRaftRpcHandler {
    async fn append_entries(
        &self,
        _request: AppendEntriesRequest<SyncTypeConfig>,
    ) -> Result<AppendEntriesResponse<SyncNodeId>, RaftError<SyncNodeId>> {
        Ok(AppendEntriesResponse::Success)
    }

    async fn install_snapshot(
        &self,
        _request: InstallSnapshotRequest<SyncTypeConfig>,
    ) -> Result<InstallSnapshotResponse<SyncNodeId>, RaftError<SyncNodeId, InstallSnapshotError>>
    {
        Ok(InstallSnapshotResponse {
            vote: Vote::new(1, 1),
        })
    }

    async fn vote(
        &self,
        request: VoteRequest<SyncNodeId>,
    ) -> Result<VoteResponse<SyncNodeId>, RaftError<SyncNodeId>> {
        self.votes.fetch_add(1, Ordering::Relaxed);
        Ok(VoteResponse::new(request.vote, None, true))
    }
}

#[async_trait]
impl SyncLearnerJoinHandler for RecordingSyncRaftRpcHandler {
    async fn add_sync_learner(
        &self,
        request: SyncLearnerJoinRequest,
    ) -> Result<SyncLearnerJoinResponse, http_error::HttpApiError> {
        self.learners.fetch_add(1, Ordering::Relaxed);
        Ok(SyncLearnerJoinResponse {
            node_id: request.node_id,
            log_index: 7,
        })
    }

    async fn promote_sync_learner(
        &self,
        node_id: SyncNodeId,
    ) -> Result<SyncLearnerPromotionResponse, http_error::HttpApiError> {
        Ok(SyncLearnerPromotionResponse {
            node_id,
            log_index: 8,
        })
    }
}

#[tokio::test]
async fn http_sync_raft_client_posts_vote_with_internal_token() {
    let handler = Arc::new(RecordingSyncRaftRpcHandler {
        votes: AtomicUsize::new(0),
        learners: AtomicUsize::new(0),
    });
    let base_url = spawn_peer_server("sync-secret", handler.clone()).await;
    let client = HttpSyncRaftRpcClient::from_config(&StorageSyncReplicationConfig {
        enabled: true,
        sync_internal_token: Some("sync-secret".to_string()),
        peers: vec![StorageSyncReplicationPeerConfig {
            node_id: 2,
            endpoint_url: format!("{base_url}/storage"),
        }],
        ..StorageSyncReplicationConfig::default()
    })
    .expect("client");

    let response = client
        .vote(
            2,
            &SyncNode::new(format!("{base_url}/storage")),
            VoteRequest::new(Vote::new(1, 1), None),
        )
        .await
        .expect("vote response");

    assert!(response.vote_granted);
    assert_eq!(handler.votes.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn http_sync_raft_client_maps_peer_auth_failure_to_unreachable() {
    let handler = Arc::new(RecordingSyncRaftRpcHandler {
        votes: AtomicUsize::new(0),
        learners: AtomicUsize::new(0),
    });
    let base_url = spawn_peer_server("sync-secret", handler.clone()).await;
    let client = HttpSyncRaftRpcClient::from_config(&StorageSyncReplicationConfig {
        enabled: true,
        sync_internal_token: Some("wrong-secret".to_string()),
        peers: vec![StorageSyncReplicationPeerConfig {
            node_id: 2,
            endpoint_url: format!("{base_url}/storage"),
        }],
        ..StorageSyncReplicationConfig::default()
    })
    .expect("client");

    let error = client
        .vote(
            2,
            &SyncNode::new(format!("{base_url}/storage")),
            VoteRequest::new(Vote::new(1, 1), None),
        )
        .await
        .expect_err("auth failure should be unreachable");

    assert!(error.to_string().contains("authentication failed"));
    assert_eq!(handler.votes.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn http_sync_raft_client_requests_learner_join_with_internal_token() {
    let handler = Arc::new(RecordingSyncRaftRpcHandler {
        votes: AtomicUsize::new(0),
        learners: AtomicUsize::new(0),
    });
    let base_url = spawn_peer_server("sync-secret", handler.clone()).await;
    let client = HttpSyncRaftRpcClient::from_config(&StorageSyncReplicationConfig {
        enabled: true,
        sync_internal_token: Some("sync-secret".to_string()),
        peers: vec![StorageSyncReplicationPeerConfig {
            node_id: 1,
            endpoint_url: format!("{base_url}/storage"),
        }],
        ..StorageSyncReplicationConfig::default()
    })
    .expect("client");

    let response = client
        .request_learner_join(
            1,
            &SyncLearnerJoinRequest {
                node_id: 3,
                advertise_url: "http://127.0.0.1:9003/storage".to_string(),
                backend_compatibility: Some("sqlite".to_string()),
            },
        )
        .await
        .expect("learner join response");

    assert_eq!(response.node_id, 3);
    assert_eq!(response.log_index, 7);
    assert_eq!(handler.learners.load(Ordering::Relaxed), 1);
}

async fn spawn_peer_server(token: &str, handler: Arc<RecordingSyncRaftRpcHandler>) -> String {
    let db = Arc::new(DatabaseManager::new_for_test().await.expect("db"));
    let app_state = Arc::new(
        AppState::new_with_manager_options(db, StorageApiManagerOptions::default())
            .with_sync_internal_token(token.to_string())
            .with_sync_raft_rpc_handler(handler.clone())
            .with_sync_learner_join_handler(handler),
    );
    spawn_router(server_router(app_state, false)).await
}

async fn spawn_router(router: Router) -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("serve test router");
    });
    format!("http://{addr}")
}
