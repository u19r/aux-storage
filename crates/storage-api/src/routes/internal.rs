use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Json, Response as AxumResponse},
};
use http_error::{ErrorResponse, HttpApiError};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use storage_sync::{SyncNodeId, SyncTypeConfig};
use storage_types::{
    GetStreamRecordsRequest, ReplicationApplyRequest, ReplicationHeartbeatRequest,
};

use crate::{
    constants::STORAGE_GATEWAY_API_KEY_HEADER,
    routes::dynamodb::{
        parse_json_request, parse_json_value, parse_try_into_request, response_to_http,
    },
    types::{AppState, ReplicationLogicalBackfillImportRequest, SyncLearnerJoinRequest},
};

// Internal sync endpoints are part of the Raft control plane. They must stay
// credential-gated and internal-network only; public callers should use the
// DynamoDB-compatible `/storage` route and leader-hint retry path instead.

#[utoipa::path(
    post,
    path = "/_internal/test/clear-all-tables",
    responses(
        (status = 200, description = "Test storage cleanup completed", body = serde_json::Value),
        (status = 403, description = "Helper unavailable in this build", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
pub async fn clear_all_tables_endpoint(
    State(app_state): State<Arc<AppState>>,
) -> Result<AxumResponse, (StatusCode, Json<ErrorResponse>)> {
    let response = app_state
        .storage_manager
        .clear_all_tables(serde_json::json!({}))
        .await
        .map_err(<(StatusCode, Json<ErrorResponse>)>::from)?;
    Ok(response_to_http(response))
}

#[utoipa::path(
    post,
    path = "/_internal/test/background-jobs/{job_name}",
    params(
        ("job_name" = String, Path, description = "Background job name to run in the test harness")
    ),
    responses(
        (status = 200, description = "Background job executed", body = serde_json::Value),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
pub async fn run_background_job_endpoint(
    State(app_state): State<Arc<AppState>>,
    Path(job_name): Path<String>,
) -> Result<AxumResponse, (StatusCode, Json<ErrorResponse>)> {
    let response = app_state
        .storage_manager
        .run_background_job(serde_json::json!({ "JobName": job_name }))
        .await
        .map_err(<(StatusCode, Json<ErrorResponse>)>::from)?;
    Ok(response_to_http(response))
}

pub async fn cache_diagnostics_endpoint() -> Json<storage::StorageCacheReadDiagnostics> {
    Json(storage::storage_cache_read_diagnostics())
}

#[utoipa::path(
    post,
    path = "/_internal/test/table-stream-records",
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "Table stream record appended", body = serde_json::Value),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
pub async fn append_table_stream_record_endpoint(
    State(app_state): State<Arc<AppState>>,
    body: Bytes,
) -> Result<AxumResponse, (StatusCode, Json<ErrorResponse>)> {
    let payload = parse_json_value(&body)?;
    let response = app_state
        .storage_manager
        .append_table_stream_record(payload)
        .await
        .map_err(<(StatusCode, Json<ErrorResponse>)>::from)?;
    Ok(response_to_http(response))
}

#[utoipa::path(
    post,
    path = "/_internal/storage/streams/records",
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "Stream records returned", body = serde_json::Value),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
pub async fn get_stream_records_endpoint(
    State(app_state): State<Arc<AppState>>,
    body: Bytes,
) -> Result<AxumResponse, (StatusCode, Json<ErrorResponse>)> {
    let request = parse_try_into_request::<GetStreamRecordsRequest>(&body)?;
    let response = app_state
        .storage_manager
        .get_stream_records(request)
        .await
        .map_err(<(StatusCode, Json<ErrorResponse>)>::from)?;
    Ok(response_to_http(response))
}

#[utoipa::path(
    post,
    path = "/_internal/storage/replication/apply",
    request_body = ReplicationApplyRequest,
    responses(
        (status = 200, description = "Replication mutations were processed", body = storage_types::ReplicationApplyResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Wrong service token", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
pub async fn apply_replication_endpoint(
    State(app_state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<AxumResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_replication_service_request(&app_state, &headers)?;
    let request = parse_json_request::<ReplicationApplyRequest>(&body)?;
    let response = app_state
        .storage_manager
        .apply_replication(request)
        .await
        .map_err(<(StatusCode, Json<ErrorResponse>)>::from)?;
    Ok(response_to_http(response))
}

#[utoipa::path(
    post,
    path = "/_internal/storage/replication/logical-backfill/import",
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "Logical backfill chunk imported", body = serde_json::Value),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Wrong service token", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
pub async fn import_replication_logical_backfill_endpoint(
    State(app_state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<AxumResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_replication_service_request(&app_state, &headers)?;
    let request = parse_json_request::<ReplicationLogicalBackfillImportRequest>(&body)?;
    let response = app_state
        .storage_manager
        .import_replication_logical_backfill(request)
        .await
        .map_err(<(StatusCode, Json<ErrorResponse>)>::from)?;
    Ok(response_to_http(response))
}

#[utoipa::path(
    post,
    path = "/_internal/storage/replication/heartbeat",
    request_body = ReplicationHeartbeatRequest,
    responses(
        (status = 200, description = "Replication heartbeat acknowledged", body = storage_types::ReplicationHeartbeatResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Wrong service token", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
pub async fn replication_heartbeat_endpoint(
    State(app_state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<AxumResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_replication_service_request(&app_state, &headers)?;
    let request = parse_json_request::<ReplicationHeartbeatRequest>(&body)?;
    let response = app_state
        .storage_manager
        .heartbeat_replication(request)
        .await
        .map_err(<(StatusCode, Json<ErrorResponse>)>::from)?;
    Ok(response_to_http(response))
}

#[utoipa::path(
    get,
    path = "/_internal/storage/replication/health",
    responses(
        (status = 200, description = "Replication health returned", body = storage_types::ReplicationHealthResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Wrong service token", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
pub async fn replication_health_endpoint(
    State(app_state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<AxumResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_replication_service_request(&app_state, &headers)?;
    let response = app_state
        .storage_manager
        .replication_health()
        .await
        .map_err(<(StatusCode, Json<ErrorResponse>)>::from)?;
    Ok(response_to_http(response))
}

#[utoipa::path(
    get,
    path = "/_internal/sync/health",
    responses(
        (status = 200, description = "Sync health returned", body = serde_json::Value),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Wrong sync internal token", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
pub async fn sync_health_endpoint(
    State(app_state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<AxumResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_sync_internal_request(&app_state, &headers)?;
    let response = app_state
        .storage_manager
        .sync_health()
        .await
        .map_err(<(StatusCode, Json<ErrorResponse>)>::from)?;
    Ok(response_to_http(response))
}

#[utoipa::path(
    post,
    path = "/_internal/sync/raft/learners",
    request_body = SyncLearnerJoinRequest,
    responses(
        (status = 200, description = "Learner was admitted to the sync Raft cluster", body = serde_json::Value),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Wrong sync internal token", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
pub async fn sync_raft_add_learner_endpoint(
    State(app_state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<AxumResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_sync_internal_request(&app_state, &headers)?;
    let request = parse_json_request::<SyncLearnerJoinRequest>(&body)?;
    let response = sync_learner_join_handler(&app_state)?
        .add_sync_learner(request)
        .await
        .map_err(<(StatusCode, Json<ErrorResponse>)>::from)?;
    Ok(Json(response).into_response())
}

#[utoipa::path(
    post,
    path = "/_internal/sync/raft/learners/{node_id}/promote",
    responses(
        (status = 200, description = "Learner was promoted to a sync Raft voter", body = serde_json::Value),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Wrong sync internal token", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
pub async fn sync_raft_promote_learner_endpoint(
    State(app_state): State<Arc<AppState>>,
    Path(node_id): Path<u64>,
    headers: HeaderMap,
) -> Result<AxumResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_sync_internal_request(&app_state, &headers)?;
    let response = sync_learner_join_handler(&app_state)?
        .promote_sync_learner(node_id)
        .await
        .map_err(<(StatusCode, Json<ErrorResponse>)>::from)?;
    Ok(Json(response).into_response())
}

#[utoipa::path(
    post,
    path = "/_internal/sync/raft/append",
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "OpenRaft append-entries response returned", body = serde_json::Value),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Wrong sync internal token", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
pub async fn sync_raft_append_endpoint(
    State(app_state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<AxumResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_sync_internal_request(&app_state, &headers)?;
    let request = parse_json_request::<AppendEntriesRequest<SyncTypeConfig>>(&body)?;
    let response = sync_raft_rpc_handler(&app_state)?
        .append_entries(request)
        .await
        .map_err(sync_raft_error)?;
    Ok(Json(response).into_response())
}

#[utoipa::path(
    post,
    path = "/_internal/sync/raft/snapshot",
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "OpenRaft snapshot response returned", body = serde_json::Value),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Wrong sync internal token", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
pub async fn sync_raft_snapshot_endpoint(
    State(app_state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<AxumResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_sync_internal_request(&app_state, &headers)?;
    let request = parse_json_request::<InstallSnapshotRequest<SyncTypeConfig>>(&body)?;
    let response = sync_raft_rpc_handler(&app_state)?
        .install_snapshot(request)
        .await
        .map_err(sync_raft_error)?;
    Ok(Json(response).into_response())
}

#[utoipa::path(
    post,
    path = "/_internal/sync/raft/vote",
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "OpenRaft vote response returned", body = serde_json::Value),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Wrong sync internal token", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
pub async fn sync_raft_vote_endpoint(
    State(app_state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<AxumResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_sync_internal_request(&app_state, &headers)?;
    let request = parse_json_request::<VoteRequest<SyncNodeId>>(&body)?;
    let response = sync_raft_rpc_handler(&app_state)?
        .vote(request)
        .await
        .map_err(sync_raft_error)?;
    Ok(Json(response).into_response())
}

#[expect(
    clippy::result_large_err,
    reason = "internal Axum route helpers share the existing JSON error tuple shape"
)]
fn sync_raft_rpc_handler(
    app_state: &AppState,
) -> Result<&dyn crate::types::SyncRaftRpcHandler, (StatusCode, Json<ErrorResponse>)> {
    app_state.sync_raft_rpc_handler().ok_or_else(|| {
        HttpApiError::dynamodb_error(
            "ServiceUnavailable",
            "sync raft runtime is not configured",
            StatusCode::SERVICE_UNAVAILABLE.as_u16(),
        )
        .into()
    })
}

#[expect(
    clippy::result_large_err,
    reason = "internal Axum route helpers share the existing JSON error tuple shape"
)]
fn sync_learner_join_handler(
    app_state: &AppState,
) -> Result<&dyn crate::types::SyncLearnerJoinHandler, (StatusCode, Json<ErrorResponse>)> {
    app_state.sync_learner_join_handler().ok_or_else(|| {
        HttpApiError::dynamodb_error(
            "ServiceUnavailable",
            "sync learner join handler is not configured",
            StatusCode::SERVICE_UNAVAILABLE.as_u16(),
        )
        .into()
    })
}

fn sync_raft_error<E: std::fmt::Display>(error: E) -> (StatusCode, Json<ErrorResponse>) {
    HttpApiError::internal_server_error(format!("sync raft rpc failed: {error}")).into()
}

#[expect(
    clippy::result_large_err,
    reason = "internal Axum route helpers share the existing JSON error tuple shape"
)]
fn authorize_sync_internal_request(
    app_state: &AppState,
    headers: &HeaderMap,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    let Some(expected_token) = app_state.sync_internal_token() else {
        return Err(HttpApiError::access_denied_error(
            "sync internal endpoint token is not configured",
        )
        .into());
    };
    let Some(actual_token) = headers
        .get(STORAGE_GATEWAY_API_KEY_HEADER)
        .and_then(|value| value.to_str().ok())
    else {
        return Err(
            HttpApiError::unauthorized_error("missing sync internal endpoint token").into(),
        );
    };
    if actual_token != expected_token {
        return Err(
            HttpApiError::access_denied_error("invalid sync internal endpoint token").into(),
        );
    }
    Ok(())
}

#[expect(
    clippy::result_large_err,
    reason = "internal Axum route helpers share the existing JSON error tuple shape"
)]
fn authorize_replication_service_request(
    app_state: &AppState,
    headers: &HeaderMap,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    if !app_state.has_replication_service_tokens() {
        return Err(HttpApiError::access_denied_error(
            "replication service endpoint token is not configured",
        )
        .into());
    }
    let Some(actual_token) = headers
        .get(STORAGE_GATEWAY_API_KEY_HEADER)
        .and_then(|value| value.to_str().ok())
    else {
        return Err(
            HttpApiError::unauthorized_error("missing replication service endpoint token").into(),
        );
    };
    if !app_state.accepts_replication_service_token(actual_token) {
        return Err(HttpApiError::access_denied_error(
            "invalid replication service endpoint token",
        )
        .into());
    }
    Ok(())
}
