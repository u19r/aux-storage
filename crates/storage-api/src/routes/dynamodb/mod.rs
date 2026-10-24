// TODO: move route execution into an owned module; retain only module wiring
// here.
use std::{future::Future, sync::Arc, time::Instant};

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Json, Response as AxumResponse},
};
use http_error::{ErrorResponse, HttpApiError};
use storage_provider::ListChangeIndexMarkersRequest;
use storage_types::{
    BatchGetItemRequest, BatchWriteItemRequest, DeleteItemRequest, DescribeStreamRequest,
    DescribeSystemStreamRequest, DescribeTimeToLiveRequest, GetItemRequest, GetRecordsRequest,
    GetShardIteratorRequest, ListStreamsRequest, PutItemRequest, QueryRequest,
    TransactGetItemsRequest, TransactWriteItemsRequest, UpdateItemRequest, UpdateTimeToLiveRequest,
};

use crate::{
    constants::{
        DYNAMODB_BODY_READ_STAGE as BODY_READ_STAGE, DYNAMODB_ERROR_STATUS as ERROR_STATUS,
        DYNAMODB_JSON_DECODE_STAGE as JSON_DECODE_STAGE, DYNAMODB_MANAGER_STAGE as MANAGER_STAGE,
        DYNAMODB_REQUEST_CONVERT_STAGE as REQUEST_CONVERT_STAGE,
        DYNAMODB_SUCCESS_STATUS as SUCCESS_STATUS,
    },
    errors::{http_api_error_headers, validation_error},
    manager::StorageApiManager,
    routes::dynamodb_metrics::{
        DynamoRouteTimer, dynamodb_operation_label, status_label_for_manager,
        status_label_for_parse,
    },
    types::{AppState, Response as ApiResponse, UpdateContinuousBackupsRequest},
};

#[derive(Debug)]
pub struct DynamoError {
    status: StatusCode,
    headers: Box<HeaderMap>,
    body: Box<Json<ErrorResponse>>,
}

impl DynamoError {
    fn new(status: StatusCode, headers: HeaderMap, body: Json<ErrorResponse>) -> Self {
        Self {
            status,
            headers: Box::new(headers),
            body: Box::new(body),
        }
    }
}

impl IntoResponse for DynamoError {
    fn into_response(self) -> AxumResponse {
        (self.status, *self.headers, *self.body).into_response()
    }
}

macro_rules! execute_try_into_operation {
    ($body:expr, $timer:expr, $manager_call:expr) => {{
        let request = parse_try_into_request_timed($body, $timer)?;
        record_manager_stage($timer, $manager_call(request)).await?
    }};
}

macro_rules! execute_dynamo_json_operation {
    ($body:expr, $timer:expr, $request_ty:ty, $manager_call:expr) => {{
        let request = parse_dynamo_json_request_timed::<$request_ty>($body, $timer)?;
        record_manager_stage($timer, $manager_call(request)).await?
    }};
}

macro_rules! execute_json_operation {
    ($body:expr, $timer:expr, $request_ty:ty, $manager_call:expr) => {{
        let request = parse_json_request_timed::<$request_ty>($body, $timer)?;
        record_manager_stage($timer, $manager_call(request)).await?
    }};
}

#[utoipa::path(
    post,
    path = "/",
    summary = "Execute a DynamoDB-compatible storage operation.",
    description = "Sends a DynamoDB-compatible JSON-RPC request through the storage API using the `x-amz-target` header. The Aux action `DynamoDB_20120810.QuerySourcePage` accepts a snake_case QueryTableRequest and returns unprojected source images, independently decoded physical keys, bounded evaluation progress and an opaque continuation. It preserves stored-entity identity validation across remote storage; ordinary Query remains unchanged. The Aux action `DynamoDB_20120810.GetCanonicalHistory` supports source discovery, scoped canonical views, native checkpoints and causal delivery on configured regional services. Its typed history request uses snake_case fields and rejects unknown fields; cursors remain bound to their serving receiver and selection.",
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "DynamoDB operation successful", body = serde_json::Value),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 404, description = "Resource not found", body = ErrorResponse),
        (status = 503, description = "Admission overloaded", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    ),
    params(
        ("x-amz-target" = String, Header, description = "DynamoDB operation target (e.g., DynamoDB_20120810.CreateTable)")
    ),
    extensions(
        ("x-aux-audience" = json!("internal_private")),
        ("x-aux-internal" = json!(true)),
    ))]
pub async fn dynamodb_endpoint(
    State(app_state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<AxumResponse, DynamoError> {
    execute_dynamodb_request(app_state.storage_manager.clone(), headers, body).await
}

/// Both ordinary and replicated assemblies use this protocol boundary. The
/// concrete manager is selected when assembling the router, not per request.
pub(crate) async fn execute_dynamodb_request<M: StorageApiManager + 'static>(
    manager: Arc<M>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<AxumResponse, DynamoError> {
    let target = headers
        .get("x-amz-target")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let operation = dynamodb_operation_label(target);
    let timer = DynamoRouteTimer::new(operation);
    let result = execute_dynamodb_operation(manager, target, &body, &timer).await;
    timer.record_request(status_label(&result));
    result
}

async fn execute_dynamodb_operation<M: StorageApiManager + 'static>(
    manager: Arc<M>,
    target: &str,
    body: &Bytes,
    timer: &DynamoRouteTimer,
) -> Result<AxumResponse, DynamoError> {
    timer.record_already_completed_stage(BODY_READ_STAGE, SUCCESS_STATUS);
    let response = match target {
        "DynamoDB_20120810.CreateTable" => {
            execute_try_into_operation!(body, timer, |request| manager.create_table(request))
        }
        "DynamoDB_20120810.ListTables" => {
            execute_try_into_operation!(body, timer, |request| manager.list_tables(request))
        }
        "DynamoDB_20120810.DeleteTable" => {
            execute_try_into_operation!(body, timer, |request| manager.delete_table(request))
        }
        "DynamoDB_20120810.DescribeTable" => {
            execute_try_into_operation!(body, timer, |request| manager.describe_table(request))
        }
        "DynamoDB_20120810.PutItem" => {
            execute_dynamo_json_operation!(body, timer, PutItemRequest, |request| manager
                .put_item(request))
        }
        "DynamoDB_20120810.GetItem" => {
            execute_dynamo_json_operation!(body, timer, GetItemRequest, |request| manager
                .get_item(request))
        }
        "DynamoDB_20120810.DeleteItem" => {
            execute_dynamo_json_operation!(body, timer, DeleteItemRequest, |request| manager
                .delete_item(request))
        }
        "DynamoDB_20120810.Query" => {
            execute_dynamo_json_operation!(body, timer, QueryRequest, |request| manager
                .query(request))
        }
        "DynamoDB_20120810.Scan" => {
            execute_dynamo_json_operation!(body, timer, storage_types::ScanRequest, |request| {
                manager.scan(request)
            })
        }
        "DynamoDB_20120810.BatchWriteItem" => {
            execute_dynamo_json_operation!(body, timer, BatchWriteItemRequest, |request| manager
                .batch_write_item(request))
        }
        "DynamoDB_20120810.BatchGetItem" => {
            execute_dynamo_json_operation!(body, timer, BatchGetItemRequest, |request| manager
                .batch_get_item(request))
        }
        "DynamoDB_20120810.TransactWriteItems" => {
            execute_dynamo_json_operation!(body, timer, TransactWriteItemsRequest, |request| {
                manager.transact_write_items(request)
            })
        }
        "DynamoDB_20120810.TransactGetItems" => {
            execute_dynamo_json_operation!(body, timer, TransactGetItemsRequest, |request| {
                manager.transact_get_items(request)
            })
        }
        "DynamoDB_20120810.ReadSequence" => {
            let request = parse_dynamo_json_request_timed(body, timer)?;
            record_manager_stage(timer, manager.read_sequence(request)).await?
        }
        "DynamoDB_20120810.UpdateItem" => {
            execute_dynamo_json_operation!(body, timer, UpdateItemRequest, |request| manager
                .update_item(request))
        }
        "DynamoDB_20120810.UpdateTable" => {
            execute_try_into_operation!(body, timer, |request| manager.update_table(request))
        }
        "DynamoDB_20120810.UpdateTimeToLive" => {
            execute_json_operation!(body, timer, UpdateTimeToLiveRequest, |request| manager
                .update_time_to_live(request))
        }
        "DynamoDB_20120810.GetCanonicalHistory" => {
            execute_json_operation!(body, timer, serde_json::Value, |request| manager
                .read_canonical_history(request))
        }
        "DynamoDB_20120810.GetStreamRecords" => {
            execute_try_into_operation!(body, timer, |request| {
                manager.get_stream_records(request)
            })
        }
        "DynamoDB_20120810.DescribeSystemStream" => {
            execute_try_into_operation!(body, timer, |request: DescribeSystemStreamRequest| {
                manager.describe_system_stream(request)
            })
        }
        "DynamoDBStreams_20120810.ListStreams" => {
            execute_try_into_operation!(body, timer, |request: ListStreamsRequest| {
                manager.list_streams(request)
            })
        }
        "DynamoDBStreams_20120810.DescribeStream" => {
            execute_try_into_operation!(body, timer, |request: DescribeStreamRequest| {
                manager.describe_stream(request)
            })
        }
        "DynamoDBStreams_20120810.GetShardIterator" => {
            execute_try_into_operation!(body, timer, |request: GetShardIteratorRequest| {
                manager.get_shard_iterator(request)
            })
        }
        "DynamoDBStreams_20120810.GetRecords" => {
            execute_try_into_operation!(body, timer, |request: GetRecordsRequest| {
                manager.get_records(request)
            })
        }
        "DynamoDB_20120810.DescribeTimeToLive" => {
            execute_json_operation!(body, timer, DescribeTimeToLiveRequest, |request| manager
                .describe_time_to_live(request))
        }
        "DynamoDB_20120810.UpdateContinuousBackups" => {
            execute_json_operation!(body, timer, UpdateContinuousBackupsRequest, |request| {
                manager.update_continuous_backups(request)
            })
        }
        "DynamoDB_20120810.QuerySourcePage" => {
            execute_json_operation!(body, timer, storage_types::QueryTableRequest, |request| {
                manager.query_source_page(request)
            })
        }
        "DynamoDB_20120810.ListChangeIndexMarkers" => {
            execute_json_operation!(body, timer, ListChangeIndexMarkersRequest, |request| {
                manager.list_change_index_markers(request)
            })
        }
        _ => {
            return Err(with_empty_error_headers(validation_error(format!(
                "Unknown operation: {target}"
            ))));
        }
    };

    timer.response_to_http(response)
}

async fn record_manager_stage<F>(
    timer: &DynamoRouteTimer,
    future: F,
) -> Result<ApiResponse, DynamoError>
where
    F: Future<Output = Result<ApiResponse, HttpApiError>>,
{
    let started = Instant::now();
    let result = future.await;
    timer.record_stage(MANAGER_STAGE, status_label_for_manager(&result), started);
    if let Err(error) = &result {
        if is_conditional_check_failed_api_error(error) {
            tracing::info!(
                error_type = %error.error_type,
                status_code = error.status_code,
                "dynamodb manager operation failed before protocol mapping"
            );
        } else {
            tracing::warn!(
                error_type = %error.error_type,
                status_code = error.status_code,
                "dynamodb manager operation failed before protocol mapping"
            );
        }
    }
    result.map_err(http_api_error_to_dynamo_error)
}

pub(super) fn is_conditional_check_failed_api_error(error: &HttpApiError) -> bool {
    error
        .error_type
        .ends_with("ConditionalCheckFailedException")
}

fn parse_try_into_request_timed<T>(
    body: &Bytes,
    timer: &DynamoRouteTimer,
) -> Result<T, DynamoError>
where
    T: TryFrom<serde_json::Value, Error = String>,
{
    let started = Instant::now();
    // TODO: benchmark direct typed decoding against this Value-based conversion
    // path.
    let payload = parse_json_value(body).map_err(with_empty_error_headers);
    timer.record_stage(JSON_DECODE_STAGE, status_label_for_parse(&payload), started);
    let payload = payload?;
    let started = Instant::now();
    let result = payload
        .try_into()
        .map_err(|message| with_empty_error_headers(validation_error(message)));
    timer.record_stage(
        REQUEST_CONVERT_STAGE,
        status_label_for_parse(&result),
        started,
    );
    result
}

// Shared Dynamo operation shapes own request validation, including direct
// callers.
fn parse_dynamo_json_request_timed<T>(
    body: &Bytes,
    timer: &DynamoRouteTimer,
) -> Result<T, DynamoError>
where
    T: serde::de::DeserializeOwned + TryFrom<serde_json::Value, Error = String>,
{
    let started = Instant::now();
    let result = parse_json_request_format(body);
    timer.record_stage(JSON_DECODE_STAGE, status_label_for_parse(&result), started);
    result
}

fn parse_json_request_timed<T>(body: &Bytes, timer: &DynamoRouteTimer) -> Result<T, DynamoError>
where T: serde::de::DeserializeOwned {
    let started = Instant::now();
    let result = parse_json_request(body).map_err(with_empty_error_headers);
    timer.record_stage(JSON_DECODE_STAGE, status_label_for_parse(&result), started);
    result
}

fn status_label(result: &Result<AxumResponse, DynamoError>) -> &'static str {
    if result.is_ok() {
        SUCCESS_STATUS
    } else {
        ERROR_STATUS
    }
}

#[expect(
    clippy::result_large_err,
    reason = "test helpers and legacy route parsers use Axum JSON error tuples"
)]
#[cfg(any(test, feature = "internal-test-routes"))]
pub(crate) fn parse_try_into_request<T>(
    body: &Bytes,
) -> Result<T, (StatusCode, Json<ErrorResponse>)>
where T: TryFrom<serde_json::Value, Error = String> {
    let payload = parse_json_value(body)?;
    payload.try_into().map_err(validation_error)
}

#[expect(
    clippy::result_large_err,
    reason = "test helpers and legacy route parsers use Axum JSON error tuples"
)]
pub(crate) fn parse_json_request<T>(body: &Bytes) -> Result<T, (StatusCode, Json<ErrorResponse>)>
where T: serde::de::DeserializeOwned {
    serde_json::from_slice(body).map_err(|error| strip_error_headers(invalid_json_error(&error)))
}

fn parse_json_request_format<T>(body: &Bytes) -> Result<T, DynamoError>
where T: serde::de::DeserializeOwned + TryFrom<serde_json::Value, Error = String> {
    serde_json::from_slice(body).map_err(|error| {
        let is_capacity_variant = error.classify() == serde_json::error::Category::Data
            && error
                .to_string()
                .contains("expected one of `NONE`, `TOTAL`, `INDEXES`");
        if is_capacity_variant
            && let Ok(value) = serde_json::from_slice::<serde_json::Value>(body)
            && let Err(message) = T::try_from(value)
        {
            return with_empty_error_headers(validation_error(message));
        }

        match error.classify() {
            serde_json::error::Category::Syntax | serde_json::error::Category::Eof => {
                invalid_json_error(&error)
            }
            serde_json::error::Category::Data | serde_json::error::Category::Io => {
                with_empty_error_headers(validation_error(format!(
                    "Invalid request format: {error}"
                )))
            }
        }
    })
}

#[expect(
    clippy::result_large_err,
    reason = "test helpers and legacy route parsers use Axum JSON error tuples"
)]
pub(crate) fn parse_json_value(
    body: &Bytes,
) -> Result<serde_json::Value, (StatusCode, Json<ErrorResponse>)> {
    serde_json::from_slice(body).map_err(|error| strip_error_headers(invalid_json_error(&error)))
}

pub(super) fn response_to_http(response: ApiResponse) -> AxumResponse {
    match response {
        ApiResponse::CreateTable(resp) => json_response(resp),
        ApiResponse::ListTables(resp) => json_response(resp),
        ApiResponse::DeleteTable(resp) => json_response(resp),
        ApiResponse::DescribeTable(resp) => json_response(resp),
        ApiResponse::PutItem(resp) => json_response(resp),
        ApiResponse::GetItem(resp) => crate::get_item_response::into_http_response(resp),
        ApiResponse::DeleteItem(resp) => json_response(resp),
        ApiResponse::Query(resp) => json_response(resp),
        ApiResponse::QuerySourcePage(resp) => json_response(resp),
        ApiResponse::Scan(resp) => json_response(resp),
        ApiResponse::BatchWriteItem(resp) => json_response(resp),
        ApiResponse::BatchGetItem(resp) => json_response(resp),
        ApiResponse::TransactWriteItems(resp) => json_response(resp),
        ApiResponse::TransactGetItems(resp) => json_response(resp),
        ApiResponse::ReadSequence(resp) => json_response(resp),
        ApiResponse::UpdateItem(resp) => json_response(resp),
        ApiResponse::UpdateTable(resp) => json_response(resp),
        ApiResponse::UpdateTimeToLive(resp) => json_response(resp),
        ApiResponse::GetStreamRecords(resp) => json_response(resp),
        ApiResponse::DescribeSystemStream(resp) => json_response(resp),
        ApiResponse::ListStreams(resp) => json_response(resp),
        ApiResponse::DescribeStream(resp) => json_response(resp),
        ApiResponse::GetShardIterator(resp) => json_response(resp),
        ApiResponse::GetRecords(resp) => json_response(resp),
        ApiResponse::DescribeTimeToLive(resp) => json_response(resp),
        ApiResponse::ListChangeIndexMarkers(resp) => json_response(resp),
        ApiResponse::CanonicalHistory(reply) => crate::history_response::into_http_response(reply),
        ApiResponse::Raw(value) => json_response(value),
    }
}

fn json_response<T>(payload: T) -> AxumResponse
where T: serde::Serialize {
    Json(payload).into_response()
}

#[cold]
#[inline(never)]
fn invalid_json_error(error: &serde_json::Error) -> DynamoError {
    with_empty_error_headers(validation_error(format!("Invalid JSON: {error}")))
}

fn with_empty_error_headers(error: (StatusCode, Json<ErrorResponse>)) -> DynamoError {
    let (status, body) = error;
    DynamoError::new(status, HeaderMap::new(), body)
}

fn strip_error_headers(error: DynamoError) -> (StatusCode, Json<ErrorResponse>) {
    (error.status, *error.body)
}

fn http_api_error_to_dynamo_error(error: HttpApiError) -> DynamoError {
    let headers = http_api_error_headers(&error);
    let (status, body) = <(StatusCode, Json<ErrorResponse>)>::from(error);
    DynamoError::new(status, headers, body)
}

#[cfg(test)]
mod decode_perf_tests;

#[cfg(test)]
mod dynamodb_tests;
