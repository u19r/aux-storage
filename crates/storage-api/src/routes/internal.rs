#![allow(clippy::result_large_err)]

use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{Json, Response as AxumResponse},
};
use http_error::{ErrorResponse, HttpApiError};
use serde::Deserialize;
use serde_json::{Value, json};
use storage::AdmissionClass;
use storage_types::{
    GetStreamRecordsRequest, SystemStreamContractError, SystemStreamScope, TimestampMillis,
};
use stream_provider::{
    SystemStreamLineage, SystemStreamSplitChild, SystemStreamSplitOperationId,
    SystemStreamSplitRequest, SystemStreamTrimContinuation, SystemStreamTrimOperationId,
    SystemStreamTrimPageRequest,
};

use crate::{
    manager::StorageApiManager,
    routes::dynamodb::{
        parse_json_request, parse_json_value, parse_try_into_request, response_to_http,
    },
    types::AppState,
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct SystemStreamSplitControlRequest {
    source_scope: SystemStreamScope,
    operation_id: String,
    parent: SystemStreamLineageControl,
    children: [SystemStreamSplitChildControl; 2],
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct SystemStreamLineageControl {
    shard_id: String,
    lineage_id: String,
    parent_lineage_id: Option<String>,
    sealed: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct SystemStreamSplitChildControl {
    lineage: SystemStreamLineageControl,
    initial_sequence: u64,
}

impl TryFrom<SystemStreamSplitControlRequest> for SystemStreamSplitRequest {
    type Error = HttpApiError;

    fn try_from(payload: SystemStreamSplitControlRequest) -> Result<Self, Self::Error> {
        let operation_id = SystemStreamSplitOperationId::try_new(payload.operation_id)
            .map_err(split_contract_error)?;
        let parent = payload.parent.try_into().map_err(split_contract_error)?;
        let [left, right] = payload.children.map(TryInto::try_into);
        let children = [
            left.map_err(split_contract_error)?,
            right.map_err(split_contract_error)?,
        ];
        SystemStreamSplitRequest::try_new(payload.source_scope, parent, children, operation_id)
            .map_err(split_contract_error)
    }
}

impl TryFrom<SystemStreamLineageControl> for SystemStreamLineage {
    type Error = SystemStreamContractError;

    fn try_from(value: SystemStreamLineageControl) -> Result<Self, Self::Error> {
        Self::try_new(
            value.shard_id,
            value.lineage_id,
            value.parent_lineage_id,
            value.sealed,
        )
    }
}

impl TryFrom<SystemStreamSplitChildControl> for SystemStreamSplitChild {
    type Error = SystemStreamContractError;

    fn try_from(value: SystemStreamSplitChildControl) -> Result<Self, Self::Error> {
        Self::try_new(value.lineage.try_into()?, value.initial_sequence)
    }
}

fn split_contract_error(error: SystemStreamContractError) -> HttpApiError {
    HttpApiError::validation_error(error.to_string())
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct SystemStreamTrimControlRequest {
    source_scope: SystemStreamScope,
    cutoff_timestamp: i64,
    limit: usize,
    continuation: Option<SystemStreamTrimContinuationControl>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct SystemStreamTrimContinuationControl {
    sequence: u64,
    bytes: Vec<u8>,
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct AdmissionConnectionQuery {
    connection_id: Option<String>,
}

impl AdmissionConnectionQuery {
    fn connection_id(&self) -> &str {
        self.connection_id.as_deref().unwrap_or("default")
    }
}

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
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "Background job executed", body = serde_json::Value),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
pub async fn run_background_job_endpoint(
    State(app_state): State<Arc<AppState>>,
    Path(job_name): Path<String>,
    body: Bytes,
) -> Result<AxumResponse, (StatusCode, Json<ErrorResponse>)> {
    if body.is_empty() {
        return Err(
            HttpApiError::validation_error("background job request body is required").into(),
        );
    }
    let mut payload = parse_json_value(&body)?;
    let Some(payload) = payload.as_object_mut() else {
        return Err(
            HttpApiError::validation_error("background job payload must be a JSON object").into(),
        );
    };
    payload.insert("JobName".to_string(), serde_json::Value::String(job_name));
    let response = app_state
        .storage_manager
        .run_background_job(serde_json::Value::Object(std::mem::take(payload)))
        .await
        .map_err(<(StatusCode, Json<ErrorResponse>)>::from)?;
    Ok(response_to_http(response))
}

/// Split one exact logical SystemStream lineage through the admitted
/// storage-manager boundary. The provider validates the request against its
/// current catalog and owns Applied/Replayed disposition.
pub async fn split_system_stream_endpoint(
    State(app_state): State<Arc<AppState>>,
    body: Bytes,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    let payload = parse_json_request::<SystemStreamSplitControlRequest>(&body)?;
    let request = payload
        .try_into()
        .map_err(|error: HttpApiError| -> (StatusCode, Json<ErrorResponse>) { error.into() })?;
    let result = app_state
        .storage_manager
        .split_system_stream(request)
        .await
        .map_err(<(StatusCode, Json<ErrorResponse>)>::from)?;
    Ok(Json(json!({
        "Disposition": format!("{:?}", result.disposition()),
        "SourceScope": result.source_scope(),
    })))
}

/// Invoke one real, provider-owned logical SystemStream trim page.  The
/// request deliberately contains no caller-supplied frontier or physical key;
/// the backend derives protection from its durable checkpoint/catalog state.
pub async fn trim_system_stream_endpoint(
    State(app_state): State<Arc<AppState>>,
    body: Bytes,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    let payload = parse_json_request::<SystemStreamTrimControlRequest>(&body)?;
    let cutoff_timestamp = TimestampMillis::from_timestamp(payload.cutoff_timestamp);
    let operation_id = SystemStreamTrimOperationId::new(&payload.source_scope, cutoff_timestamp);
    let continuation = payload
        .continuation
        .map(|continuation| {
            SystemStreamTrimContinuation::try_new(
                operation_id,
                continuation.sequence,
                continuation.bytes,
            )
        })
        .transpose()
        .map_err(storage_system_error)?;
    let request = SystemStreamTrimPageRequest::try_new(
        payload.source_scope,
        cutoff_timestamp,
        payload.limit,
        continuation,
    )
    .map_err(storage_system_error)?;
    let result = app_state
        .storage_manager
        .run_system_stream_trim_page(request)
        .await
        .map_err(<(StatusCode, Json<ErrorResponse>)>::from)?;
    Ok(Json(json!({
        "ReclaimedRecords": result.reclaimed_records(),
        "NextContinuation": result.next_continuation().map(|continuation| json!({
            "Sequence": continuation.sequence(),
            "Bytes": continuation.as_bytes(),
        })),
    })))
}

/// Test-only synchronization primitive mounted only with the existing
/// internal-helper flag. It holds a real foreground permit so BDD and local
/// canary checks exercise the production admission queue without injecting
/// fake provider latency. The optional connection query selects a configured
/// physical controller and defaults to the synthetic single-backend route.
pub async fn hold_admission_endpoint(
    State(app_state): State<Arc<AppState>>,
    Query(query): Query<AdmissionConnectionQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, HeaderMap, Json<ErrorResponse>)> {
    let permit = app_state
        .db_manager
        .acquire_admission(query.connection_id(), AdmissionClass::PointRead)
        .await
        .map_err(storage_admission_error)?;
    let released = app_state.admission_test_release.notified();
    app_state.admission_test_holds.lock().await.push(permit);
    released.await;
    Ok(Json(serde_json::json!({
        "status": "released",
        "connection_id": query.connection_id(),
    })))
}

pub async fn release_admission_endpoint(
    State(app_state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let held = std::mem::take(&mut *app_state.admission_test_holds.lock().await);
    drop(held);
    app_state.admission_test_release.notify_waiters();
    Json(serde_json::json!({"released": true}))
}

pub async fn admission_diagnostics_endpoint(
    State(app_state): State<Arc<AppState>>,
    Query(query): Query<AdmissionConnectionQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let controller = app_state
        .db_manager
        .admission_controller(query.connection_id())
        .ok_or_else(|| {
            HttpApiError::validation_error(format!(
                "storage connection '{}' is not configured",
                query.connection_id()
            ))
        })?;
    let snapshot = controller.snapshot();
    Ok(Json(serde_json::json!({
        "connection_id": snapshot.connection_id,
        "enabled": snapshot.enabled,
        "state": format!("{:?}", snapshot.state),
        "limit": snapshot.desired_limit,
        "queue_capacity": controller.config().queue_capacity,
        "max_queue_wait_ms": controller.config().max_queue_wait_ms,
        "in_flight": snapshot.in_flight,
        "control_in_flight": snapshot.control_in_flight,
        "queue_depth": snapshot.queue_depth,
        "rejection_count": snapshot.rejection_count,
    })))
}

/// Admit one provider handle through a selected production controller. This
/// endpoint exists only with internal helper routes and returns after the
/// named permit is admitted and completed.
pub async fn probe_admission_endpoint(
    State(app_state): State<Arc<AppState>>,
    Query(query): Query<AdmissionConnectionQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, HeaderMap, Json<ErrorResponse>)> {
    let connection_id = query.connection_id().to_string();
    app_state
        .db_manager
        .run_dynamo_admitted(&connection_id, AdmissionClass::PointRead, |_dynamo| async {
            Ok::<(), storage_types::StorageError>(())
        })
        .await
        .map_err(storage_admission_error)?;
    Ok(Json(serde_json::json!({
        "connection_id": connection_id,
    })))
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
    path = "/_internal/test/table-stream-records/reconcile",
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "Table append durability inspected", body = serde_json::Value),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
pub async fn reconcile_table_stream_record_endpoint(
    State(app_state): State<Arc<AppState>>,
    body: Bytes,
) -> Result<AxumResponse, (StatusCode, Json<ErrorResponse>)> {
    let response = app_state
        .storage_manager
        .reconcile_table_stream_record(parse_json_value(&body)?)
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

fn storage_system_error(error: storage_types::StorageError) -> (StatusCode, Json<ErrorResponse>) {
    HttpApiError::from(error).into()
}

fn storage_admission_error(
    error: storage_types::StorageError,
) -> (StatusCode, HeaderMap, Json<ErrorResponse>) {
    let (status, body) = HttpApiError::from(error).into();
    let mut headers = HeaderMap::new();
    if let Some(retry_after_seconds) = body.0.retry_after_seconds {
        headers.insert("retry-after", HeaderValue::from(retry_after_seconds));
    }
    (status, headers, body)
}
