use std::{
    sync::{Mutex, atomic::AtomicUsize},
    time::{Duration, Instant},
};

use aws_sigv4_signing::SigningError;
use http::Uri;
use http_request::reqwest::Client;
use metrics_facade::{HistogramMetric, histogram};
use rand::{RngExt as _, rng};
use storage_provider::RemoteTimeoutOverrides;
use storage_types::{
    ExclusiveStartKey, GlobalSecondaryIndex, KeyAttributes, QueryRequest, QueryTableRequest,
    ScanRequest, ScanTableRequest, StorageEnum, StorageError, StorageResult, StoredTableInfo,
    TableDescription, TimestampMillis, context::WrappedError,
};

use crate::{
    constants::{
        BASE_BACKOFF_MS, MAX_BACKOFF_MS, RETRY_TOKEN_CAPACITY, RETRY_TOKEN_REFILL_PER_SECOND,
    },
    provider::{AttemptError, EndpointState},
};

pub(crate) fn build_client(timeouts: Option<&RemoteTimeoutOverrides>) -> StorageResult<Client> {
    let mut builder = Client::builder().redirect(http_request::reqwest::redirect::Policy::none());
    if let Some(overrides) = timeouts {
        if let Some(connect_ms) = overrides.connect_timeout_ms {
            builder = builder.connect_timeout(Duration::from_millis(connect_ms));
        }
        if let Some(request_ms) = overrides.request_timeout_ms {
            builder = builder.timeout(Duration::from_millis(request_ms));
        }
    }
    builder
        .build()
        .map_err(|err| StorageError::internal(&format!("build remote http client: {err}")))
}

pub(crate) fn build_endpoints(urls: &[String], tls: bool) -> StorageResult<Vec<EndpointState>> {
    if urls.is_empty() {
        return Err(StorageError::validation(
            "remote storage requires at least one endpoint",
        ));
    }

    let mut endpoints = Vec::with_capacity(urls.len());
    for raw in urls {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(StorageError::validation("endpoint URL may not be empty"));
        }
        let normalized = if trimmed.contains("://") {
            trimmed.to_string()
        } else {
            let scheme = if tls { "https" } else { "http" };
            format!("{scheme}://{trimmed}")
        };
        let uri: Uri = normalized.parse().map_err(|_| {
            StorageError::validation(format!("invalid endpoint URL supplied: {normalized}"))
        })?;
        let requires_signature = uri
            .host()
            .is_some_and(|host| host.to_ascii_lowercase().ends_with("amazonaws.com"));
        endpoints.push(EndpointState {
            url: normalized,
            uri,
            requires_signature,
            failures: AtomicUsize::new(0),
        });
    }
    Ok(endpoints)
}

pub(crate) fn to_table_info(table: TableDescription) -> StoredTableInfo {
    StoredTableInfo {
        table_name: table.table_name,
        table_status: table.table_status,
        created_at: TimestampMillis::from(table.created_at),
        attribute_definitions: table.attribute_definitions,
        key_schema: table.key_schema,
        max_indexers: table.max_indexers,
        global_secondary_indexes: table.global_secondary_indexes.map(|indexes| {
            indexes
                .into_iter()
                .map(|index| GlobalSecondaryIndex {
                    index_name: index.index_name,
                    key_schema: index.key_schema,
                    projection: index.projection,
                })
                .collect()
        }),
        table_size_bytes: table.table_size_bytes,
        item_count: table.item_count,
        stream_specification: table.stream_specification,
        table_stream_duration: storage_types::StreamRetentionDuration::default(),
        default_item_stream_duration: storage_types::StreamRetentionDuration::default(),
        deletion_protection_enabled: table.deletion_protection_enabled,
    }
}

pub(super) fn build_scan_request(request: &ScanTableRequest) -> ScanRequest {
    let mut remote_request = ScanRequest::new(request.table_name.clone())
        .with_index_name(request.index_name.clone())
        .with_limit(request.limit)
        .with_exclusive_start_key(None);
    remote_request.exclusive_start_key = request
        .exclusive_start_key
        .as_deref()
        .map(exclusive_start_key_from_string);
    remote_request.consistent_read = Some(request.consistent_read);
    remote_request
}

pub(super) fn build_query_request(request: &QueryTableRequest) -> QueryRequest {
    let mut remote_request = QueryRequest::new(
        request.table_name.clone(),
        request.key_condition_expression.clone(),
    )
    .with_index_name(request.index_name.clone())
    .with_expression_attribute_names(request.expression_attribute_names.clone())
    .with_expression_attribute_values(request.expression_attribute_values.clone())
    .with_limit(request.limit)
    .with_exclusive_start_key(None)
    .with_scan_index_forward(request.scan_index_forward);
    remote_request.projection_expression = request.projection_expression.clone();
    remote_request.exclusive_start_key = request
        .exclusive_start_key
        .as_deref()
        .map(exclusive_start_key_from_string);
    remote_request.consistent_read = Some(request.consistent_read);
    remote_request
}

fn exclusive_start_key_from_string(value: &str) -> ExclusiveStartKey {
    serde_json::from_str::<KeyAttributes>(value)
        .map(ExclusiveStartKey::Key)
        .unwrap_or_else(|_| ExclusiveStartKey::Token(value.to_owned()))
}

pub(crate) fn record_latency(operation: &str, endpoint: &str, outcome: &str, elapsed: Duration) {
    let latency_ms = elapsed.as_secs_f64() * 1_000.0;
    let latency_ms = (latency_ms * 1_000.0).round() / 1_000.0;
    histogram!(
        HistogramMetric::RemoteStorageRequestLatencyMs,
        "endpoint" => endpoint.to_owned(),
        "operation" => operation.to_owned(),
        "outcome" => outcome.to_owned()
    )
    .record(latency_ms);
}

pub(crate) fn retry_backoff_cap(attempt: usize) -> Duration {
    if attempt == 0 {
        return Duration::ZERO;
    }
    let capped_attempt = attempt.saturating_sub(1).min(8);
    Duration::from_millis(
        BASE_BACKOFF_MS
            .saturating_mul(1_u64 << capped_attempt)
            .min(MAX_BACKOFF_MS),
    )
}

pub(crate) fn full_jitter_delay(attempt: usize, sample: u64) -> Duration {
    let cap = retry_backoff_cap(attempt).as_millis() as u64;
    if cap == 0 {
        return Duration::ZERO;
    }
    Duration::from_millis(sample % cap.saturating_add(1))
}

pub(crate) fn compute_backoff(attempt: usize) -> Duration {
    let cap = retry_backoff_cap(attempt).as_millis() as u64;
    if cap == 0 {
        return Duration::ZERO;
    }
    let mut prng = rng();
    full_jitter_delay(attempt, prng.random_range(0..=cap))
}

#[derive(Debug)]
struct RetryTokenState {
    tokens: f64,
    last_refill: Instant,
}

impl RetryTokenState {
    fn new(now: Instant) -> Self {
        Self {
            tokens: RETRY_TOKEN_CAPACITY as f64,
            last_refill: now,
        }
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now
            .saturating_duration_since(self.last_refill)
            .as_secs_f64();
        if elapsed <= 0.0 {
            return;
        }
        self.tokens = (self.tokens + elapsed * RETRY_TOKEN_REFILL_PER_SECOND as f64)
            .min(RETRY_TOKEN_CAPACITY as f64);
        self.last_refill = now;
    }

    fn try_take(&mut self, now: Instant) -> bool {
        self.refill(now);
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }

    fn refund(&mut self, now: Instant) {
        self.refill(now);
        self.tokens = (self.tokens + 1.0).min(RETRY_TOKEN_CAPACITY as f64);
    }
}

/// Per-provider retry budget. The mutex is held only for the arithmetic update;
/// no network or timer operation is performed while it is locked.
pub(crate) struct RetryTokenBucket {
    state: Mutex<RetryTokenState>,
}

impl RetryTokenBucket {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(RetryTokenState::new(Instant::now())),
        }
    }

    pub(crate) fn try_take(&self) -> bool {
        self.try_take_at(Instant::now())
    }

    pub(crate) fn try_take_at(&self, now: Instant) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.try_take(now)
    }

    pub(crate) fn refund_one(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.refund(Instant::now());
    }
}

pub(super) fn attempt_internal(message: &str) -> AttemptError {
    AttemptError::new(StorageError::internal(&message), false, None, None)
}

pub(super) fn signing_error_to_storage_error(err: &SigningError) -> StorageError {
    StorageError::internal(&format!("sigv4 signing failed: {err}"))
}

pub(super) fn attempt_signing(err: &SigningError) -> AttemptError {
    AttemptError::new(signing_error_to_storage_error(err), false, None, None)
}

pub(super) fn attempt_transport(err: &http_request::reqwest::Error) -> AttemptError {
    let retryable = err.is_timeout() || err.is_connect() || err.is_request();
    AttemptError::new(
        StorageError::internal(&format!("remote transport error: {err}")),
        retryable,
        None,
        None,
    )
}

pub(super) fn error_label(error: &StorageError) -> &'static str {
    match error.to_enum() {
        StorageEnum::Database(_) => "database",
        StorageEnum::AwsSerialization(_) => "aws_serialization",
        StorageEnum::Serialization(_) => "serialization",
        StorageEnum::ResourceNotFound { .. } => "resource_not_found",
        StorageEnum::ResourceExists { .. } => "resource_exists",
        StorageEnum::IndexNotFound { .. } => "index_not_found",
        StorageEnum::KeyValidation(_) => "key_validation",
        StorageEnum::InternalServerError { .. } => "internal_server_error",
        StorageEnum::GuardConflict { .. } => "guard_conflict",
        StorageEnum::Unsupported { .. } => "unsupported",
        StorageEnum::ConditionalCheckFailed
        | StorageEnum::ConditionalCheckFailedWithItem { .. } => "conditional_check_failed",
        StorageEnum::TableAlreadyExists { .. } => "table_exists",
        StorageEnum::TableNotFound { .. } => "table_not_found",
        StorageEnum::Validation { .. }
        | StorageEnum::RawValidation { .. }
        | StorageEnum::DeletionProtectionEnabled { .. } => "validation",
        StorageEnum::TransactionConflict { .. } => "transaction_conflict",
        StorageEnum::TransactionInProgress { .. } => "transaction_in_progress",
        StorageEnum::TransactionCanceled { .. } => "transaction_canceled",
        StorageEnum::ProvisionedThroughputExceeded { .. } => "provisioned_throughput",
        StorageEnum::Throttled { .. } => "throttled",
        StorageEnum::LimitExceeded { .. } => "limit_exceeded",
        StorageEnum::RequestLimitExceeded => "request_limit_exceeded",
        StorageEnum::ServiceUnavailable { .. } => "service_unavailable",
        StorageEnum::Authentication { .. } => "authentication",
        StorageEnum::MissingAuthenticationToken => "missing_authentication_token",
        StorageEnum::AccessDenied { .. } => "access_denied",
        StorageEnum::AwsService { .. } => "aws_service",
    }
}

pub(super) fn extract_operation(target: &str) -> &str {
    target.rsplit('.').next().unwrap_or(target).trim()
}
