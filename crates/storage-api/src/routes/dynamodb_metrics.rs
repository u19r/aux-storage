use std::{sync::LazyLock, time::Instant};

use axum::response::Response as AxumResponse;
use http_error::HttpApiError;
use metrics::{Counter, Histogram};

use crate::{
    constants::{
        STORAGE_API_DYNAMODB_REQUEST_LATENCY_MICROS_TOTAL_METRIC,
        STORAGE_API_DYNAMODB_REQUEST_LATENCY_MS_METRIC, STORAGE_API_DYNAMODB_REQUESTS_TOTAL_METRIC,
        STORAGE_API_DYNAMODB_STAGE_LATENCY_MS_METRIC, STORAGE_API_DYNAMODB_STAGE_TOTAL_METRIC,
    },
    routes::dynamodb::{
        BODY_READ_STAGE, DynamoError, ERROR_STATUS, JSON_DECODE_STAGE, MANAGER_STAGE,
        REQUEST_CONVERT_STAGE, RESPONSE_ENCODE_STAGE, SUCCESS_STATUS, response_to_http,
    },
    types::Response as ApiResponse,
};

const UNKNOWN_OPERATION: &str = "unknown";

const DYNAMODB_OPERATIONS: &[&str] = &[
    "batch_get_item",
    "batch_write_item",
    "create_table",
    "delete_item",
    "delete_table",
    "describe_table",
    "describe_time_to_live",
    "get_item",
    "get_stream_records",
    "list_tables",
    "put_item",
    "query",
    "scan",
    "transact_get_items",
    "transact_write_items",
    "update_continuous_backups",
    "update_item",
    "update_table",
    "update_time_to_live",
    UNKNOWN_OPERATION,
];

const DYNAMODB_STATUSES: &[&str] = &[SUCCESS_STATUS, ERROR_STATUS];

const DYNAMODB_STAGES: &[&str] = &[
    BODY_READ_STAGE,
    JSON_DECODE_STAGE,
    REQUEST_CONVERT_STAGE,
    MANAGER_STAGE,
    RESPONSE_ENCODE_STAGE,
];

struct RequestMetricHandles {
    requests_total: Counter,
    latency_micros_total: Counter,
    latency_ms: Histogram,
}

struct StageMetricHandles {
    total: Counter,
    latency_ms: Histogram,
}

static REQUEST_METRICS: LazyLock<Vec<RequestMetricHandles>> = LazyLock::new(|| {
    let mut handles = Vec::with_capacity(DYNAMODB_OPERATIONS.len() * DYNAMODB_STATUSES.len());
    for operation in DYNAMODB_OPERATIONS {
        for status in DYNAMODB_STATUSES {
            handles.push(RequestMetricHandles {
                requests_total: metrics::counter!(
                    STORAGE_API_DYNAMODB_REQUESTS_TOTAL_METRIC.name(),
                    "operation" => *operation,
                    "status" => *status,
                ),
                latency_micros_total: metrics::counter!(
                    STORAGE_API_DYNAMODB_REQUEST_LATENCY_MICROS_TOTAL_METRIC.name(),
                    "operation" => *operation,
                    "status" => *status,
                ),
                latency_ms: metrics::histogram!(
                    STORAGE_API_DYNAMODB_REQUEST_LATENCY_MS_METRIC.name(),
                    "operation" => *operation,
                    "status" => *status,
                ),
            });
        }
    }
    handles
});

static STAGE_METRICS: LazyLock<Vec<StageMetricHandles>> = LazyLock::new(|| {
    let mut handles = Vec::with_capacity(
        DYNAMODB_OPERATIONS.len() * DYNAMODB_STAGES.len() * DYNAMODB_STATUSES.len(),
    );
    for operation in DYNAMODB_OPERATIONS {
        for stage in DYNAMODB_STAGES {
            for status in DYNAMODB_STATUSES {
                handles.push(StageMetricHandles {
                    total: metrics::counter!(
                        STORAGE_API_DYNAMODB_STAGE_TOTAL_METRIC.name(),
                        "operation" => *operation,
                        "stage" => *stage,
                        "status" => *status,
                    ),
                    latency_ms: metrics::histogram!(
                        STORAGE_API_DYNAMODB_STAGE_LATENCY_MS_METRIC.name(),
                        "operation" => *operation,
                        "stage" => *stage,
                        "status" => *status,
                    ),
                });
            }
        }
    }
    handles
});

fn record_dynamodb_stage_duration(
    operation: &'static str,
    stage: &'static str,
    status: &'static str,
    elapsed_ms: f64,
) {
    let handles = stage_metric_handles(operation, stage, status);
    handles.total.increment(1);
    handles.latency_ms.record(elapsed_ms);
}

pub(super) struct DynamoRouteTimer {
    operation: &'static str,
    request_started: Instant,
}

impl DynamoRouteTimer {
    pub(super) fn new(operation: &'static str) -> Self {
        Self {
            operation,
            request_started: Instant::now(),
        }
    }

    pub(super) fn record_request(&self, status: &'static str) {
        record_dynamodb_request(self.operation, status, self.request_started);
    }

    pub(super) fn record_stage(&self, stage: &'static str, status: &'static str, started: Instant) {
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        record_dynamodb_stage_duration(self.operation, stage, status, elapsed_ms);
    }

    pub(super) fn record_already_completed_stage(&self, stage: &'static str, status: &'static str) {
        record_dynamodb_stage_duration(self.operation, stage, status, 0.0);
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

pub(super) fn dynamodb_operation_label(target: &str) -> &'static str {
    match target {
        "DynamoDB_20120810.BatchGetItem" => "batch_get_item",
        "DynamoDB_20120810.BatchWriteItem" => "batch_write_item",
        "DynamoDB_20120810.CreateTable" => "create_table",
        "DynamoDB_20120810.DeleteItem" => "delete_item",
        "DynamoDB_20120810.DeleteTable" => "delete_table",
        "DynamoDB_20120810.DescribeTable" => "describe_table",
        "DynamoDB_20120810.DescribeTimeToLive" => "describe_time_to_live",
        "DynamoDB_20120810.GetItem" => "get_item",
        "DynamoDB_20120810.GetStreamRecords" => "get_stream_records",
        "DynamoDBStreams_20120810.DescribeStream" => "describe_stream",
        "DynamoDBStreams_20120810.GetRecords" => "get_records",
        "DynamoDBStreams_20120810.GetShardIterator" => "get_shard_iterator",
        "DynamoDBStreams_20120810.ListStreams" => "list_streams",
        "DynamoDB_20120810.ListTables" => "list_tables",
        "DynamoDB_20120810.PutItem" => "put_item",
        "DynamoDB_20120810.Query" => "query",
        "DynamoDB_20120810.Scan" => "scan",
        "DynamoDB_20120810.TransactGetItems" => "transact_get_items",
        "DynamoDB_20120810.TransactWriteItems" => "transact_write_items",
        "DynamoDB_20120810.UpdateContinuousBackups" => "update_continuous_backups",
        "DynamoDB_20120810.UpdateItem" => "update_item",
        "DynamoDB_20120810.UpdateTable" => "update_table",
        "DynamoDB_20120810.UpdateTimeToLive" => "update_time_to_live",
        _ => UNKNOWN_OPERATION,
    }
}

fn record_dynamodb_request(operation: &'static str, status: &'static str, started: Instant) {
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    let elapsed_micros = (elapsed_ms * 1000.0).max(0.0).round() as u64;
    let handles = request_metric_handles(operation, status);
    handles.requests_total.increment(1);
    handles.latency_micros_total.increment(elapsed_micros);
    handles.latency_ms.record(elapsed_ms);
}

fn request_metric_handles(
    operation: &'static str,
    status: &'static str,
) -> &'static RequestMetricHandles {
    let index = operation_index(operation) * DYNAMODB_STATUSES.len() + status_index(status);
    &REQUEST_METRICS[index]
}

fn stage_metric_handles(
    operation: &'static str,
    stage: &'static str,
    status: &'static str,
) -> &'static StageMetricHandles {
    let index = ((operation_index(operation) * DYNAMODB_STAGES.len()) + stage_index(stage))
        * DYNAMODB_STATUSES.len()
        + status_index(status);
    &STAGE_METRICS[index]
}

fn operation_index(operation: &'static str) -> usize {
    match operation {
        "batch_get_item" => 0,
        "batch_write_item" => 1,
        "create_table" => 2,
        "delete_item" => 3,
        "delete_table" => 4,
        "describe_table" => 5,
        "describe_time_to_live" => 6,
        "get_item" => 7,
        "get_stream_records" => 8,
        "list_tables" => 9,
        "put_item" => 10,
        "query" => 11,
        "scan" => 12,
        "transact_get_items" => 13,
        "transact_write_items" => 14,
        "update_continuous_backups" => 15,
        "update_item" => 16,
        "update_table" => 17,
        "update_time_to_live" => 18,
        _ => 19,
    }
}

fn stage_index(stage: &'static str) -> usize {
    match stage {
        BODY_READ_STAGE => 0,
        JSON_DECODE_STAGE => 1,
        REQUEST_CONVERT_STAGE => 2,
        MANAGER_STAGE => 3,
        RESPONSE_ENCODE_STAGE => 4,
        _ => 3,
    }
}

fn status_index(status: &'static str) -> usize {
    match status {
        SUCCESS_STATUS => 0,
        _ => 1,
    }
}
