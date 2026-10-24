use std::{
    collections::HashSet,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use aws_sigv4_signing::SigningError;
use http::Uri;
use http_request::{Url, reqwest::Client};
use metrics_facade::{HistogramMetric, histogram};
use rand::{RngExt as _, rng};
use serde::Serialize;
use storage_common::{DEFAULT_QUERY_LIMIT, DEFAULT_SCAN_LIMIT, MAX_QUERY_LIMIT, MAX_SCAN_LIMIT};
use storage_provider::{GsiIndexReadiness, RemoteTimeoutOverrides};
use storage_types::{
    ExclusiveStartKey, GlobalSecondaryIndex, GlobalSecondaryIndexDescription, IndexName, ItemKey,
    KeyAttributes, QueryTableRequest, ScanRequest, ScanTableRequest, StorageEnum, StorageError,
    StorageResult, StoredTableInfo, TableDescription, TableName, TimestampMillis,
    context::WrappedError, subset_expression_attribute_names_for_expression,
};

use crate::{
    constants::{
        BASE_BACKOFF_MS, MAX_BACKOFF_MS, RETRY_TOKEN_CAPACITY, RETRY_TOKEN_REFILL_PER_SECOND,
    },
    provider::{AttemptError, EndpointState},
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum RemoteRedirectPolicy {
    #[default]
    Disabled,
    ScheduledJobHttp,
}

pub(crate) fn redirect_url(
    current: &http_request::Url,
    location: &str,
    visited: &mut HashSet<String>,
) -> Result<http_request::Url, &'static str> {
    let next = current
        .join(location)
        .map_err(|_| "scheduled-job redirect location is invalid")?;
    if next.scheme() != "http"
        || next.host().is_none()
        || next.cannot_be_a_base()
        || next.fragment().is_some()
    {
        return Err("scheduled-job redirects must remain HTTP URLs without fragments");
    }
    if next.username() != "" || next.password().is_some() {
        return Err("scheduled-job redirects may not contain URL credentials");
    }
    if !visited.insert(next.as_str().to_string()) {
        return Err("scheduled-job redirect loop detected");
    }
    Ok(next)
}

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
        let request_url = Url::parse(&normalized).map_err(|_| {
            StorageError::validation(format!("invalid endpoint URL supplied: {normalized}"))
        })?;
        let requires_signature = uri
            .host()
            .is_some_and(|host| host.to_ascii_lowercase().ends_with("amazonaws.com"));
        endpoints.push(EndpointState {
            url: normalized,
            uri,
            request_url,
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

pub(crate) fn gsi_index_readiness(
    indexes: &[GlobalSecondaryIndexDescription],
    index_names: &[IndexName],
) -> StorageResult<Vec<(IndexName, GsiIndexReadiness)>> {
    index_names
        .iter()
        .map(|index_name| {
            let index = indexes
                .iter()
                .find(|index| index.index_name == *index_name)
                .ok_or_else(|| {
                    StorageError::internal("remote GSI index readiness metadata is incomplete")
                })?;
            let index_status = index.index_status.clone().ok_or_else(|| {
                StorageError::internal("remote GSI index readiness metadata is incomplete")
            })?;
            Ok((
                index_name.clone(),
                GsiIndexReadiness {
                    index_status,
                    backfilling: index.backfilling.unwrap_or(false),
                },
            ))
        })
        .collect()
}

pub(super) fn build_scan_request(request: &ScanTableRequest) -> StorageResult<ScanRequest> {
    let limit = storage_common::normalize_limit(request.limit, DEFAULT_SCAN_LIMIT, MAX_SCAN_LIMIT)?;
    let mut remote_request = ScanRequest::builder()
        .table_name(request.table_name.clone())
        .index_name(request.index_name.clone())
        .limit(Some(limit))
        .build();
    remote_request.exclusive_start_key = request
        .exclusive_start_key
        .as_deref()
        .map(exclusive_start_key_from_string);
    remote_request.consistent_read = Some(request.consistent_read);
    Ok(remote_request)
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub(super) struct QueryRequestView<'a> {
    pub(super) table_name: &'a TableName,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) index_name: Option<&'a IndexName>,

    pub(super) key_condition_expression: &'a str,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) expression_attribute_names: Option<storage_types::VecMap<String, String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) expression_attribute_values: Option<&'a storage_types::AttributeMap>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) limit: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) exclusive_start_key: Option<ExclusiveStartKey>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) consistent_read: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) scan_index_forward: Option<bool>,
}

pub(super) fn build_query_request_view(
    request: &QueryTableRequest,
) -> StorageResult<QueryRequestView<'_>> {
    let limit =
        storage_common::normalize_limit(request.limit, DEFAULT_QUERY_LIMIT, MAX_QUERY_LIMIT)?;
    Ok(QueryRequestView {
        table_name: &request.table_name,
        index_name: request.index_name.as_ref(),
        key_condition_expression: &request.key_condition_expression,
        expression_attribute_names: subset_expression_attribute_names_for_expression(
            &request.key_condition_expression,
            request.expression_attribute_names.as_ref(),
        ),
        expression_attribute_values: request.expression_attribute_values.as_ref(),
        limit: Some(limit),
        exclusive_start_key: request
            .exclusive_start_key
            .as_deref()
            .map(exclusive_start_key_from_string),
        consistent_read: Some(request.consistent_read),
        scan_index_forward: request.scan_index_forward,
    })
}

pub(super) fn exclusive_start_key_from_string(value: &str) -> ExclusiveStartKey {
    serde_json::from_str::<KeyAttributes>(value)
        .map(ExclusiveStartKey::Key)
        .unwrap_or_else(|_| ExclusiveStartKey::Token(value.to_owned()))
}

pub(super) fn remote_key_to_page_token(
    key: &KeyAttributes,
    table_info: &StoredTableInfo,
    index_name: Option<&IndexName>,
) -> StorageResult<String> {
    let item_key = if let Some(index_name) = index_name {
        let index = table_info
            .global_secondary_indexes
            .as_ref()
            .and_then(|indexes| indexes.iter().find(|index| index.index_name == *index_name))
            .ok_or_else(StorageError::invalid_or_missing_key)?;
        ItemKey::from_key_schema_for_index(
            table_info.table_name.clone(),
            &table_info.key_schema,
            index_name,
            &index.key_schema,
            key,
        )?
        .ok_or_else(StorageError::invalid_or_missing_key)?
    } else {
        ItemKey::from_key_schema(table_info.table_name.clone(), &table_info.key_schema, key)?
    };

    Ok(item_key.next_page_token()?)
}

pub(crate) fn record_latency(operation: &str, outcome: &str, elapsed: Duration) {
    let latency_ms = elapsed.as_secs_f64() * 1_000.0;
    let latency_ms = (latency_ms * 1_000.0).round() / 1_000.0;
    histogram!(
        HistogramMetric::RemoteStorageRequestLatencyMs,
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
    pending_refunds: AtomicUsize,
}

impl RetryTokenBucket {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(RetryTokenState::new(Instant::now())),
            pending_refunds: AtomicUsize::new(0),
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
        let taken = state.try_take(now);
        if taken {
            self.pending_refunds.fetch_add(1, Ordering::Release);
        }
        taken
    }

    pub(crate) fn refund_one(&self) {
        if self
            .pending_refunds
            .try_update(Ordering::AcqRel, Ordering::Acquire, |pending| {
                pending.checked_sub(1)
            })
            .is_err()
        {
            return;
        }
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

pub(super) fn attempt_transport(err: http_request::reqwest::Error) -> AttemptError {
    let retryable = err.is_timeout() || err.is_connect() || err.is_request();
    let message = format!("remote transport error: {err}");
    let dispatch_status = http_request::HttpRequestError::from(err).dispatch_status();
    AttemptError::new(StorageError::internal(&message), retryable, None, None)
        .with_dispatch_status(dispatch_status)
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
        StorageEnum::HistoryUnavailable => "history_unavailable",
        StorageEnum::HistoryExpired => "history_expired",
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
        StorageEnum::IdempotentParameterMismatch => "idempotent_parameter_mismatch",
        StorageEnum::TransactionCanceled { .. } => "transaction_canceled",
        StorageEnum::ProvisionedThroughputExceeded { .. } => "provisioned_throughput",
        StorageEnum::Throttled { .. } => "throttled",
        StorageEnum::LimitExceeded { .. } => "limit_exceeded",
        StorageEnum::RequestLimitExceeded => "request_limit_exceeded",
        StorageEnum::ServiceUnavailable { .. } => "service_unavailable",
        StorageEnum::SyncNotLeader { .. } => "sync_not_leader",
        StorageEnum::SyncSubmissionAmbiguous { .. } => "sync_submission_ambiguous",
        StorageEnum::InfrastructureResult(_) => "internal_server_error",
        StorageEnum::Authentication { .. } => "authentication",
        StorageEnum::MissingAuthenticationToken => "missing_authentication_token",
        StorageEnum::AccessDenied { .. } => "access_denied",
        StorageEnum::AwsService { .. } => "aws_service",
        StorageEnum::IdempotencyReplay { .. } => "idempotency_replay",
    }
}

pub(super) fn extract_operation(target: &str) -> &str {
    target.rsplit('.').next().unwrap_or(target).trim()
}
