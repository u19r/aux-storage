use std::time::Instant;

use axum::response::Response as AxumResponse;
use http_error::HttpApiError;

use crate::{
    constants::{
        STORAGE_API_DYNAMODB_REQUEST_LATENCY_MICROS_TOTAL_METRIC,
        STORAGE_API_DYNAMODB_REQUEST_LATENCY_MS_METRIC, STORAGE_API_DYNAMODB_REQUESTS_TOTAL_METRIC,
        STORAGE_API_DYNAMODB_STAGE_LATENCY_MS_METRIC, STORAGE_API_DYNAMODB_STAGE_TOTAL_METRIC,
    },
    routes::dynamodb::{
        DynamoError, ERROR_STATUS, RESPONSE_ENCODE_STAGE, SUCCESS_STATUS, response_to_http,
    },
    types::Response as ApiResponse,
};

fn record_dynamodb_stage_duration(
    operation: &str,
    stage: &'static str,
    status: &'static str,
    elapsed_ms: f64,
) {
    metrics::counter!(
        STORAGE_API_DYNAMODB_STAGE_TOTAL_METRIC.name(),
        "operation" => operation.to_string(),
        "stage" => stage,
        "status" => status,
    )
    .increment(1);
    metrics::histogram!(
        STORAGE_API_DYNAMODB_STAGE_LATENCY_MS_METRIC.name(),
        "operation" => operation.to_string(),
        "stage" => stage,
        "status" => status,
    )
    .record(elapsed_ms);
}

pub(super) struct DynamoRouteTimer {
    operation: String,
    request_started: Instant,
}

impl DynamoRouteTimer {
    pub(super) fn new(operation: String) -> Self {
        Self {
            operation,
            request_started: Instant::now(),
        }
    }

    pub(super) fn record_request(&self, status: &'static str) {
        record_dynamodb_request(&self.operation, status, self.request_started);
    }

    pub(super) fn record_stage(&self, stage: &'static str, status: &'static str, started: Instant) {
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        record_dynamodb_stage_duration(&self.operation, stage, status, elapsed_ms);
    }

    pub(super) fn record_already_completed_stage(&self, stage: &'static str, status: &'static str) {
        record_dynamodb_stage_duration(&self.operation, stage, status, 0.0);
    }

    pub(super) fn response_to_http(
        &self,
        response: ApiResponse,
    ) -> Result<AxumResponse, DynamoError> {
        let started = Instant::now();
        let response = response_to_http(response);
        self.record_stage(RESPONSE_ENCODE_STAGE, SUCCESS_STATUS, started);
        Ok(response)
    }
}

pub(super) fn status_label_for_manager(result: &Result<ApiResponse, HttpApiError>) -> &'static str {
    if result.is_ok() {
        SUCCESS_STATUS
    } else {
        ERROR_STATUS
    }
}

pub(super) fn status_label_for_parse<T>(result: &Result<T, DynamoError>) -> &'static str {
    if result.is_ok() {
        SUCCESS_STATUS
    } else {
        ERROR_STATUS
    }
}

fn record_dynamodb_request(operation: &str, status: &'static str, started: Instant) {
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    let elapsed_micros = (elapsed_ms * 1000.0).max(0.0).round() as u64;
    metrics::counter!(
        STORAGE_API_DYNAMODB_REQUESTS_TOTAL_METRIC.name(),
        "operation" => operation.to_string(),
        "status" => status,
    )
    .increment(1);
    metrics::counter!(
        STORAGE_API_DYNAMODB_REQUEST_LATENCY_MICROS_TOTAL_METRIC.name(),
        "operation" => operation.to_string(),
        "status" => status,
    )
    .increment(elapsed_micros);
    metrics::histogram!(
        STORAGE_API_DYNAMODB_REQUEST_LATENCY_MS_METRIC.name(),
        "operation" => operation.to_string(),
        "status" => status,
    )
    .record(elapsed_ms);
}
