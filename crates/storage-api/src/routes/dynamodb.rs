use std::{future::Future, sync::Arc, time::Instant};

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::{IntoResponse, Json, Response as AxumResponse},
};
use http_error::{ErrorResponse, HttpApiError};
use storage_provider::ListChangeIndexMarkersRequest;
use storage_types::{
    BatchGetItemRequest, DeleteItemRequest, DescribeStreamRequest, DescribeTimeToLiveRequest,
    DynamoRequestValidate, GetItemRequest, GetRecordsRequest, GetShardIteratorRequest,
    ListStreamsRequest, PutItemRequest, QueryRequest, ReadSequenceRequest, TransactGetItemsRequest,
    TransactWriteItemsRequest, UpdateItemRequest, UpdateTimeToLiveRequest,
};

use crate::{
    errors::validation_error,
    routes::dynamodb_metrics::{
        DynamoRouteTimer, dynamodb_operation_label, status_label_for_manager,
        status_label_for_parse,
    },
    types::{AppState, Response as ApiResponse, UpdateContinuousBackupsRequest},
};

pub(super) const SUCCESS_STATUS: &str = "success";
pub(super) const ERROR_STATUS: &str = "error";
pub(super) const JSON_DECODE_STAGE: &str = "json_decode";
pub(super) const REQUEST_CONVERT_STAGE: &str = "request_convert";
pub(super) const MANAGER_STAGE: &str = "manager";
pub(super) const RESPONSE_ENCODE_STAGE: &str = "response_encode";
pub(super) const BODY_READ_STAGE: &str = "body_read";

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

    #[cfg(test)]
    pub(crate) fn into_parts(self) -> (StatusCode, HeaderMap, Json<ErrorResponse>) {
        (self.status, *self.headers, *self.body)
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

macro_rules! execute_validated_json_operation {
    ($body:expr, $timer:expr, $request_ty:ty, $manager_call:expr) => {{
        let request = parse_validated_json_request_timed::<$request_ty>($body, $timer)?;
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
    description = "Sends a DynamoDB-compatible JSON-RPC request through the AuxFn storage gateway using the `x-amz-target` header to select the operation. The aggregated internal-private `/storage` surface accepts either an authenticated bearer session or an `x-api-key` service token bound to the `storage-gateway` service, while non-DynamoDB helper traffic stays on separate internal routes.",
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "DynamoDB operation successful", body = serde_json::Value),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Wrong machine credential type or service binding", body = ErrorResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 404, description = "Resource not found", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    ),
    params(
        ("x-amz-target" = String, Header, description = "DynamoDB operation target (e.g., DynamoDB_20120810.CreateTable)"),
        ("x-api-key" = Option<String>, Header, description = "Optional service token bound to the storage-gateway service when the caller is a machine principal")
    ),
    extensions(
        ("x-aux-audience" = json!("internal_private")),
        ("x-aux-internal" = json!(true)),
        ("x-aux-authn-classification" = json!("session_or_service_token")),
    ))]
pub async fn dynamodb_endpoint(
    State(app_state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<AxumResponse, DynamoError> {
    let target = headers
        .get("x-amz-target")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let operation = dynamodb_operation_label(target);
    let timer = DynamoRouteTimer::new(operation);
    let result = execute_dynamodb_operation(app_state, target, &body, &timer).await;
    timer.record_request(status_label(&result));
    result
}

async fn execute_dynamodb_operation(
    app_state: Arc<AppState>,
    target: &str,
    body: &Bytes,
    timer: &DynamoRouteTimer,
) -> Result<AxumResponse, DynamoError> {
    let manager = app_state.storage_manager.clone();

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
            execute_validated_json_operation!(body, timer, PutItemRequest, |request| manager
                .put_item(request))
        }
        "DynamoDB_20120810.GetItem" => {
            execute_validated_json_operation!(body, timer, GetItemRequest, |request| manager
                .get_item(request))
        }
        "DynamoDB_20120810.DeleteItem" => {
            execute_validated_json_operation!(body, timer, DeleteItemRequest, |request| manager
                .delete_item(request))
        }
        "DynamoDB_20120810.Query" => {
            execute_validated_json_operation!(body, timer, QueryRequest, |request| manager
                .query(request))
        }
        "DynamoDB_20120810.Scan" => {
            execute_try_into_operation!(body, timer, |request| manager.scan(request))
        }
        "DynamoDB_20120810.BatchWriteItem" => {
            execute_try_into_operation!(body, timer, |request| manager.batch_write_item(request))
        }
        "DynamoDB_20120810.BatchGetItem" => {
            execute_validated_json_operation!(body, timer, BatchGetItemRequest, |request| manager
                .batch_get_item(request))
        }
        "DynamoDB_20120810.TransactWriteItems" => {
            execute_validated_json_operation!(body, timer, TransactWriteItemsRequest, |request| {
                manager.transact_write_items(request)
            })
        }
        "DynamoDB_20120810.TransactGetItems" => {
            execute_validated_json_operation!(body, timer, TransactGetItemsRequest, |request| {
                manager.transact_get_items(request)
            })
        }
        "DynamoDB_20120810.ReadSequence" => {
            execute_json_operation!(body, timer, ReadSequenceRequest, |request| {
                manager.read_sequence(request)
            })
        }
        "DynamoDB_20120810.UpdateItem" => {
            execute_validated_json_operation!(body, timer, UpdateItemRequest, |request| manager
                .update_item(request))
        }
        "DynamoDB_20120810.UpdateTable" => {
            execute_try_into_operation!(body, timer, |request| manager.update_table(request))
        }
        "DynamoDB_20120810.UpdateTimeToLive" => {
            execute_json_operation!(body, timer, UpdateTimeToLiveRequest, |request| manager
                .update_time_to_live(request))
        }
        "DynamoDB_20120810.GetStreamRecords" => {
            execute_try_into_operation!(body, timer, |request| {
                manager.get_stream_records(request)
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
                error = ?error,
                "dynamodb manager operation failed before protocol mapping"
            );
        } else {
            tracing::warn!(
                error = ?error,
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

fn parse_json_request_timed<T>(body: &Bytes, timer: &DynamoRouteTimer) -> Result<T, DynamoError>
where T: serde::de::DeserializeOwned {
    let started = Instant::now();
    let result = parse_json_request(body).map_err(with_empty_error_headers);
    timer.record_stage(JSON_DECODE_STAGE, status_label_for_parse(&result), started);
    result
}

fn parse_validated_json_request_timed<T>(
    body: &Bytes,
    timer: &DynamoRouteTimer,
) -> Result<T, DynamoError>
where
    T: serde::de::DeserializeOwned + DynamoRequestValidate,
{
    let started = Instant::now();
    let result = parse_json_request_format(body);
    timer.record_stage(JSON_DECODE_STAGE, status_label_for_parse(&result), started);
    let request: T = result?;

    let started = Instant::now();
    let result = request.validate_for_dynamodb().map(|()| request);
    let result = result.map_err(|message| with_empty_error_headers(validation_error(message)));
    timer.record_stage(
        REQUEST_CONVERT_STAGE,
        status_label_for_parse(&result),
        started,
    );
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
where T: serde::de::DeserializeOwned {
    serde_json::from_slice(body).map_err(|error| match error.classify() {
        serde_json::error::Category::Syntax | serde_json::error::Category::Eof => {
            invalid_json_error(&error)
        }
        serde_json::error::Category::Data | serde_json::error::Category::Io => {
            with_empty_error_headers(validation_error(format!("Invalid request format: {error}")))
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
        ApiResponse::GetItem(resp) => json_response(resp),
        ApiResponse::GetWire(resp) => resp.into_http_response(),
        ApiResponse::DeleteItem(resp) => json_response(resp),
        ApiResponse::Query(resp) => json_response(resp),
        ApiResponse::QueryWire(resp) => resp.into_http_response(),
        ApiResponse::Scan(resp) => json_response(resp),
        ApiResponse::BatchWriteItem(resp) => json_response(resp),
        ApiResponse::BatchGetItem(resp) => json_response(resp),
        ApiResponse::BatchGetWire(resp) => resp.into_http_response(),
        ApiResponse::TransactWriteItems(resp) => json_response(resp),
        ApiResponse::TransactGetItems(resp) => json_response(resp),
        ApiResponse::ReadSequence(resp) => json_response(resp),
        ApiResponse::UpdateItem(resp) => json_response(resp),
        ApiResponse::UpdateTable(resp) => json_response(resp),
        ApiResponse::UpdateTimeToLive(resp) => json_response(resp),
        ApiResponse::GetStreamRecords(resp) => json_response(resp),
        ApiResponse::ListStreams(resp) => json_response(resp),
        ApiResponse::DescribeStream(resp) => json_response(resp),
        ApiResponse::GetShardIterator(resp) => json_response(resp),
        ApiResponse::GetRecords(resp) => json_response(resp),
        ApiResponse::ReplicationApply(resp) => json_response(resp),
        ApiResponse::ReplicationHeartbeat(resp) => json_response(resp),
        ApiResponse::ReplicationHealth(resp) => json_response(resp),
        ApiResponse::SyncHealth(resp) => json_response(resp),
        ApiResponse::DescribeTimeToLive(resp) => json_response(resp),
        ApiResponse::ListChangeIndexMarkers(resp) => json_response(resp),
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
    let headers = error_headers(&error);
    let (status, body) = <(StatusCode, Json<ErrorResponse>)>::from(error);
    DynamoError::new(status, headers, body)
}

fn error_headers(error: &HttpApiError) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in &error.response_headers {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            headers.insert(name, value);
        }
    }
    headers
}

#[cfg(test)]
mod decode_perf_tests {
    use std::hint::black_box;

    use alloc_counter::AllocationGuard;
    use storage_types::{DynamoRequestValidate, GetItemRequest, PutItemRequest};

    use super::*;

    const ITERATIONS: usize = 1_000;
    const GET_BODY: &[u8] = br#"{"TableName":"table","Key":{"pk":{"S":"value"}}}"#;
    const PUT_BODY: &[u8] =
        br#"{"TableName":"table","Item":{"pk":{"S":"value"},"data":{"S":"payload"}}}"#;

    fn direct<T>(body: &[u8]) -> T
    where T: serde::de::DeserializeOwned + DynamoRequestValidate {
        let request: T = serde_json::from_slice(body).expect("direct request decode");
        request
            .validate_for_dynamodb()
            .expect("direct request validation");
        request
    }

    fn measure<T>(
        label: &'static str,
        decode: impl Fn() -> T,
    ) -> alloc_counter::AllocationReport<'static> {
        let guard = AllocationGuard::start(
            module_path!(),
            "dynamodb_wire_decode_allocation_profile_tests",
            file!(),
            line!(),
            Some(label),
        );
        for _ in 0..ITERATIONS {
            black_box(decode());
        }
        guard.finish()
    }

    #[test]
    fn direct_dynamodb_decode_reduces_small_request_allocations() {
        let legacy_get = measure("get_value_then_typed", || {
            parse_try_into_request::<GetItemRequest>(&Bytes::from_static(GET_BODY))
                .expect("legacy GetItem decode")
        });
        let direct_get = measure("get_direct_typed", || direct::<GetItemRequest>(GET_BODY));
        let legacy_put = measure("put_value_then_typed", || {
            parse_try_into_request::<PutItemRequest>(&Bytes::from_static(PUT_BODY))
                .expect("legacy PutItem decode")
        });
        let direct_put = measure("put_direct_typed", || direct::<PutItemRequest>(PUT_BODY));

        alloc_counter::emit_report(&legacy_get);
        alloc_counter::emit_report(&direct_get);
        alloc_counter::emit_report(&legacy_put);
        alloc_counter::emit_report(&direct_put);
        assert!(direct_get.allocation_count < legacy_get.allocation_count);
        assert!(direct_get.allocated_bytes < legacy_get.allocated_bytes);
        assert!(direct_put.allocation_count < legacy_put.allocation_count);
        assert!(direct_put.allocated_bytes < legacy_put.allocated_bytes);
    }

    #[test]
    fn direct_dynamodb_decode_rejects_duplicate_fields() {
        let error = parse_json_request_format::<GetItemRequest>(&Bytes::from_static(
            br#"{"TableName":"one","TableName":"two","Key":{"pk":{"S":"value"}}}"#,
        ))
        .expect_err("duplicate field must fail");

        assert!(error.body.0.message.contains("duplicate field `TableName`"));
    }
}
