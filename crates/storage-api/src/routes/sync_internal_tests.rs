use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use axum::{
    body::{self, Bytes},
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
};
use openraft::{
    Vote,
    error::{InstallSnapshotError, RaftError},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use storage_sync::{SyncNodeId, SyncTypeConfig};

use crate::{
    constants::STORAGE_GATEWAY_API_KEY_HEADER,
    manager::StorageApiManagerOptions,
    routes::{internal, routes_support_tests::create_test_db},
    types::{
        AppState, SyncLearnerJoinHandler, SyncLearnerJoinRequest, SyncLearnerJoinResponse,
        SyncLearnerPromotionResponse, SyncRaftRpcHandler,
    },
};

#[tokio::test]
async fn sync_health_route_defaults_closed_and_requires_configured_token() {
    let db = create_test_db().await;
    let app_state = Arc::new(AppState::new_with_manager_options(
        db,
        StorageApiManagerOptions::default(),
    ));

    let (status, error) =
        internal::sync_health_endpoint(State(app_state.clone()), HeaderMap::new())
            .await
            .expect_err("missing token config should reject");

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(error.0.error_type, "AccessDeniedException");

    let app_state = Arc::new(
        AppState::new_with_manager_options(
            app_state.db_manager.clone(),
            StorageApiManagerOptions::default(),
        )
        .with_sync_internal_token("sync-secret"),
    );

    let (status, _error) =
        internal::sync_health_endpoint(State(app_state.clone()), HeaderMap::new())
            .await
            .expect_err("missing header should reject");

    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let mut multi_region_headers = HeaderMap::new();
    multi_region_headers.insert(
        STORAGE_GATEWAY_API_KEY_HEADER,
        HeaderValue::from_static("multi-region-service-token"),
    );
    let (status, error) =
        internal::sync_health_endpoint(State(app_state.clone()), multi_region_headers)
            .await
            .expect_err("multi-region service token should not authorize sync endpoint");

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(error.0.error_type, "AccessDeniedException");

    let mut headers = HeaderMap::new();
    headers.insert(
        STORAGE_GATEWAY_API_KEY_HEADER,
        HeaderValue::from_static("sync-secret"),
    );
    let response = internal::sync_health_endpoint(State(app_state), headers)
        .await
        .expect("sync health response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(json["role"], "disabled");
}

struct RecordingSyncRaftRpcHandler {
    votes: AtomicUsize,
    learners: AtomicUsize,
    promotions: AtomicUsize,
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
            log_index: 42,
        })
    }

    async fn promote_sync_learner(
        &self,
        node_id: SyncNodeId,
    ) -> Result<SyncLearnerPromotionResponse, http_error::HttpApiError> {
        self.promotions.fetch_add(1, Ordering::Relaxed);
        Ok(SyncLearnerPromotionResponse {
            node_id,
            log_index: 43,
        })
    }
}

#[tokio::test]
async fn sync_raft_vote_route_requires_token_and_runtime_handler() {
    let db = create_test_db().await;
    let app_state = Arc::new(AppState::new_with_manager_options(
        db,
        StorageApiManagerOptions::default(),
    ));
    let body = Bytes::from(
        serde_json::to_vec(&VoteRequest::new(Vote::new(1, 2), None)).expect("vote request json"),
    );

    let (status, error) =
        internal::sync_raft_vote_endpoint(State(app_state.clone()), HeaderMap::new(), body.clone())
            .await
            .expect_err("missing token config should reject");

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(error.0.error_type, "AccessDeniedException");

    let mut headers = HeaderMap::new();
    headers.insert(
        STORAGE_GATEWAY_API_KEY_HEADER,
        HeaderValue::from_static("sync-secret"),
    );
    let app_state_without_handler = Arc::new(
        AppState::new_with_manager_options(
            app_state.db_manager.clone(),
            StorageApiManagerOptions::default(),
        )
        .with_sync_internal_token("sync-secret"),
    );

    let (status, error) = internal::sync_raft_vote_endpoint(
        State(app_state_without_handler),
        headers.clone(),
        body.clone(),
    )
    .await
    .expect_err("missing runtime handler should reject");

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error.0.error_type, "ServiceUnavailable");

    let handler = Arc::new(RecordingSyncRaftRpcHandler {
        votes: AtomicUsize::new(0),
        learners: AtomicUsize::new(0),
        promotions: AtomicUsize::new(0),
    });
    let app_state = Arc::new(
        AppState::new_with_manager_options(
            app_state.db_manager.clone(),
            StorageApiManagerOptions::default(),
        )
        .with_sync_internal_token("sync-secret")
        .with_sync_raft_rpc_handler(handler.clone()),
    );

    let response = internal::sync_raft_vote_endpoint(State(app_state), headers, body)
        .await
        .expect("vote response");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(handler.votes.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn sync_raft_add_learner_route_requires_token_and_runtime_handler() {
    let db = create_test_db().await;
    let app_state = Arc::new(
        AppState::new_with_manager_options(db, StorageApiManagerOptions::default())
            .with_sync_internal_token("sync-secret"),
    );
    let body = Bytes::from(
        serde_json::to_vec(&SyncLearnerJoinRequest {
            node_id: 3,
            advertise_url: "http://127.0.0.1:9003/storage".to_string(),
            backend_compatibility: Some("sqlite".to_string()),
        })
        .expect("learner request json"),
    );
    let mut headers = HeaderMap::new();
    headers.insert(
        STORAGE_GATEWAY_API_KEY_HEADER,
        HeaderValue::from_static("sync-secret"),
    );

    let (status, error) = internal::sync_raft_add_learner_endpoint(
        State(app_state.clone()),
        headers.clone(),
        body.clone(),
    )
    .await
    .expect_err("missing join handler should reject");

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error.0.error_type, "ServiceUnavailable");

    let handler = Arc::new(RecordingSyncRaftRpcHandler {
        votes: AtomicUsize::new(0),
        learners: AtomicUsize::new(0),
        promotions: AtomicUsize::new(0),
    });
    let app_state = Arc::new(
        AppState::new_with_manager_options(
            app_state.db_manager.clone(),
            StorageApiManagerOptions::default(),
        )
        .with_sync_internal_token("sync-secret")
        .with_sync_learner_join_handler(handler.clone()),
    );

    let response = internal::sync_raft_add_learner_endpoint(State(app_state), headers, body)
        .await
        .expect("learner response");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(handler.learners.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn sync_raft_promote_learner_route_requires_token_and_runtime_handler() {
    let db = create_test_db().await;
    let app_state = Arc::new(
        AppState::new_with_manager_options(db, StorageApiManagerOptions::default())
            .with_sync_internal_token("sync-secret"),
    );
    let mut headers = HeaderMap::new();
    headers.insert(
        STORAGE_GATEWAY_API_KEY_HEADER,
        HeaderValue::from_static("sync-secret"),
    );

    let (status, error) = internal::sync_raft_promote_learner_endpoint(
        State(app_state.clone()),
        axum::extract::Path(3),
        headers.clone(),
    )
    .await
    .expect_err("missing join handler should reject");

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error.0.error_type, "ServiceUnavailable");

    let handler = Arc::new(RecordingSyncRaftRpcHandler {
        votes: AtomicUsize::new(0),
        learners: AtomicUsize::new(0),
        promotions: AtomicUsize::new(0),
    });
    let app_state = Arc::new(
        AppState::new_with_manager_options(
            app_state.db_manager.clone(),
            StorageApiManagerOptions::default(),
        )
        .with_sync_internal_token("sync-secret")
        .with_sync_learner_join_handler(handler.clone()),
    );

    let response = internal::sync_raft_promote_learner_endpoint(
        State(app_state),
        axum::extract::Path(3),
        headers,
    )
    .await
    .expect("promotion response");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(handler.promotions.load(Ordering::Relaxed), 1);
}
