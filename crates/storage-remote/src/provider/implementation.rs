use std::{
    collections::HashSet,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant as LatencyInstant, SystemTime},
};

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, StatusCode, Uri};
use http_request::{HttpRequestDispatchStatus, HttpRequestError, Url, reqwest::Client};
use metrics_facade::counter;
use rand::{RngExt as _, rng};
use serde::{Serialize, de::DeserializeOwned};
use storage_common::{MAX_QUERY_LIMIT, record_read_cost, wire_items_payload_bytes};
use storage_provider::{
    MAX_RANGE_READ_EVALUATED_BYTES, RangeReadBudget, RangeReadPage, RemoteCredentialStrategy,
    RemoteStorageSettings,
};
use storage_types::{
    DescribeTableRequest, DescribeTableResponse, QueryTableRequest, STORAGE_LEADER_HINT_HEADER,
    STORAGE_NOT_LEADER_ERROR_TYPE, StorageEnum, StorageError, StorageResult, StoredTableInfo,
    TableName, TableStatus, TimeToLiveSpecification, UpdateTableRequest, UpdateTableResponse,
    UpdateTimeToLiveRequest, UpdateTimeToLiveResponse, WireItem,
    context::{ErrorContext as _, WrappedError},
};
use tokio::time::{Instant, sleep};
use tracing::{Span, instrument};

fn record_items_returned(count: usize) {
    Span::current().record("items_returned", count as u64);
}

fn record_remote_payload_bytes(operation: &str, request_bytes: u64, response_bytes: u64) {
    counter!(
        REMOTE_STORAGE_REQUEST_BYTES_TOTAL_METRIC,
        "operation" => operation.to_string()
    )
    .increment(request_bytes);
    counter!(
        REMOTE_STORAGE_RESPONSE_BYTES_TOTAL_METRIC,
        "operation" => operation.to_string()
    )
    .increment(response_bytes);
}

use aws_sigv4_signing::{AwsRequestSigner, AwsStaticCredentials, CredentialSource, SignableBody};

use super::{
    provider_helpers::{
        RemoteRedirectPolicy, RetryTokenBucket, attempt_internal, attempt_signing,
        attempt_transport, build_client, build_endpoints, build_query_request_view,
        compute_backoff, extract_operation, record_latency, redirect_url,
        signing_error_to_storage_error, to_table_info,
    },
    replay_policy::{AfterFailure, REMOTE_WRITE_OUTCOME_UNKNOWN, ReplayPolicy},
    wire_item_helper::parse_scan_query_wire,
};
use crate::{
    constants::{
        AWS_SERVICE_NAME, FAILURE_ALERT_THRESHOLD, MANAGED_TABLE_TTL_ATTRIBUTE,
        MAX_ENDPOINT_RETRIES, MAX_REMOTE_RETRIES, MAX_RETRY_AFTER_SECS,
        MAX_SCHEDULED_JOB_REDIRECT_HOPS, MIN_RETRY_ATTEMPT_BUDGET_MS, NO_ENDPOINT,
        PITR_RETRY_ATTEMPTS, PITR_RETRY_DELAY_SECS, REMOTE_STORAGE_REQUEST_BYTES_TOTAL_METRIC,
        REMOTE_STORAGE_RESPONSE_BYTES_TOTAL_METRIC, TABLE_ACTIVE_RETRY_ATTEMPTS,
        TABLE_ACTIVE_RETRY_DELAY_MS,
    },
    error::{RemoteErrorResponse, classify_error_response},
};

pub(super) struct AttemptError {
    error: StorageError,
    retryable: bool,
    code: Option<String>,
    status: Option<StatusCode>,
    leader_hint: Option<String>,
    retry_after: Option<Duration>,
    dispatch_status: HttpRequestDispatchStatus,
}

pub(super) struct RemoteInvocationError {
    pub(super) error: StorageError,
    pub(super) dispatch_status: HttpRequestDispatchStatus,
}

impl RemoteInvocationError {
    pub(super) fn new(error: StorageError, dispatch_status: HttpRequestDispatchStatus) -> Self {
        Self {
            error,
            dispatch_status,
        }
    }
}

impl AttemptError {
    pub(super) fn new(
        error: StorageError,
        retryable: bool,
        code: Option<String>,
        status: Option<StatusCode>,
    ) -> Self {
        Self {
            error,
            retryable,
            code,
            status,
            leader_hint: None,
            retry_after: None,
            dispatch_status: HttpRequestDispatchStatus::NotDispatched,
        }
    }

    fn with_leader_hint(mut self, leader_hint: Option<String>) -> Self {
        self.leader_hint = leader_hint;
        self
    }

    fn with_retry_after(mut self, retry_after: Option<Duration>) -> Self {
        self.retry_after = retry_after;
        self
    }

    pub(super) fn with_dispatch_status(
        mut self,
        dispatch_status: HttpRequestDispatchStatus,
    ) -> Self {
        self.dispatch_status = dispatch_status;
        self
    }
}

/// Header the internal listener of the storage owner requires.
pub(crate) const INTERNAL_TOKEN_HEADER: &str = "x-aux-internal-token";

pub(super) fn signer_credentials(strategy: &RemoteCredentialStrategy) -> CredentialSource {
    match strategy {
        RemoteCredentialStrategy::DefaultChain => CredentialSource::DefaultChain,
        RemoteCredentialStrategy::Static(creds) => CredentialSource::Static(AwsStaticCredentials {
            access_key_id: creds.access_key_id.clone(),
            secret_access_key: creds.secret_access_key.clone(),
            session_token: creds.session_token.clone(),
        }),
    }
}

pub struct EndpointState {
    pub url: String,
    pub uri: Uri,
    pub request_url: Url,
    pub requires_signature: bool,
    pub failures: AtomicUsize,
}

pub struct RemoteStorageProvider {
    pub client: Client,
    pub endpoints: Vec<EndpointState>,
    pub signer: Option<AwsRequestSigner>,
    pub credential_source: &'static str,
    pub primary_endpoint: AtomicUsize,
    pub probation_endpoint: AtomicUsize,
    pub(crate) retry_budget: RetryTokenBucket,
    pub(crate) request_timeout: Option<Duration>,
    pub(crate) redirect_policy: RemoteRedirectPolicy,
    pub(crate) internal_service_token: Option<HeaderValue>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct UpdateContinuousBackupsRequest {
    table_name: TableName,
    point_in_time_recovery_specification: PointInTimeRecoverySpecification,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct PointInTimeRecoverySpecification {
    point_in_time_recovery_enabled: bool,
}

impl RemoteStorageProvider {
    pub(super) async fn query_table_page(
        &self,
        request: &QueryTableRequest,
    ) -> StorageResult<RangeReadPage> {
        let remote_request = build_query_request_view(request)?;
        let max_items = remote_request
            .limit
            .map_or(MAX_QUERY_LIMIT, |limit| limit.min(MAX_QUERY_LIMIT));
        let response = self
            .invoke_bytes("DynamoDB_20120810.Query", &remote_request)
            .await?;
        let parsed = parse_scan_query_wire(&response)?;
        let table_info = self
            .load_range_table_info_if_needed(
                &parsed.items,
                parsed.last_evaluated_key.as_ref(),
                &request.table_name,
            )
            .await?;
        let items_returned = parsed.items.len();
        let budget = RangeReadBudget::try_new(max_items, MAX_RANGE_READ_EVALUATED_BYTES)?;
        let page = parsed.into_page(table_info.as_ref(), &request.index_name, budget)?;
        record_items_returned(items_returned);
        record_read_cost(
            "query_table",
            "query",
            1,
            wire_items_payload_bytes(page.items.as_slice()),
        );
        Ok(page)
    }

    async fn load_range_table_info_if_needed(
        &self,
        items: &[WireItem],
        raw_key: Option<&String>,
        table_name: &TableName,
    ) -> StorageResult<Option<StoredTableInfo>> {
        if items.is_empty() && raw_key.is_none() {
            return Ok(None);
        }
        self.get_table_info_inner(table_name).await.map(Some)
    }

    pub(super) async fn get_table_info_inner(
        &self,
        table_name: &TableName,
    ) -> StorageResult<StoredTableInfo> {
        let request = DescribeTableRequest {
            table_name: table_name.clone(),
        };
        let response: DescribeTableResponse = self
            .invoke("DynamoDB_20120810.DescribeTable", &request)
            .await?;
        record_items_returned(1);
        Ok(to_table_info(response.table))
    }

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "table_exists"))]
    pub(super) async fn table_exists(&self, table_name: &TableName) -> StorageResult<bool> {
        let request = DescribeTableRequest {
            table_name: table_name.clone(),
        };
        match self
            .invoke::<_, DescribeTableResponse>("DynamoDB_20120810.DescribeTable", &request)
            .await
        {
            Ok(_) => Ok(true),
            Err(err) => {
                if matches!(
                    err.to_enum(),
                    StorageEnum::TableNotFound { .. } | StorageEnum::ResourceNotFound { .. }
                ) {
                    Ok(false)
                } else {
                    Err(err)
                }
            }
        }
    }

    #[instrument(name = "remote_storage.init", skip_all, fields(feature = "storage"))]
    pub async fn new(settings: RemoteStorageSettings) -> StorageResult<Self> {
        settings
            .validate()
            .context("validate remote storage settings")?;

        let credential_source = match &settings.credentials {
            RemoteCredentialStrategy::DefaultChain => "default_chain",
            RemoteCredentialStrategy::Static(_) => "static",
        };

        let endpoints = build_endpoints(&settings.endpoint_urls, settings.tls)
            .context("build remote storage endpoint list")?;
        let signer = if endpoints.iter().any(|endpoint| endpoint.requires_signature) {
            let region = settings.region.clone().ok_or_else(|| {
                StorageError::validation(
                    "AWS endpoints require a region when using the remote storage provider",
                )
            })?;
            Some(
                AwsRequestSigner::new(
                    &region,
                    signer_credentials(&settings.credentials),
                    AWS_SERVICE_NAME,
                )
                .map_err(|err| signing_error_to_storage_error(&err))
                .context("create remote storage SigV4 signer")?,
            )
        } else {
            None
        };

        let client =
            build_client(settings.timeouts.as_ref()).context("build remote storage HTTP client")?;
        let request_timeout = settings
            .timeouts
            .as_ref()
            .and_then(|timeouts| timeouts.request_timeout_ms)
            .map(Duration::from_millis);

        Ok(Self {
            client,
            endpoints,
            signer,
            credential_source,
            primary_endpoint: AtomicUsize::new(0),
            probation_endpoint: AtomicUsize::new(NO_ENDPOINT),
            retry_budget: RetryTokenBucket::new(),
            request_timeout,
            redirect_policy: RemoteRedirectPolicy::Disabled,
            internal_service_token: None,
        })
    }

    /// Sends `x-aux-internal-token` on every request to the configured
    /// endpoints. Only unsigned private-network endpoints accept it, and the
    /// header is never forwarded to a redirect target on another origin.
    pub(crate) fn with_internal_service_token(mut self, token: &str) -> StorageResult<Self> {
        if self
            .endpoints
            .iter()
            .any(|endpoint| endpoint.requires_signature)
        {
            return Err(StorageError::validation(
                "an internal service token cannot be sent to signed AWS endpoints",
            ));
        }
        let mut value = HeaderValue::from_str(token.trim()).map_err(|_| {
            StorageError::validation("internal service token is not a valid header")
        })?;
        if value.as_bytes().is_empty() {
            return Err(StorageError::validation("internal service token is empty"));
        }
        value.set_sensitive(true);
        self.internal_service_token = Some(value);
        Ok(self)
    }

    pub(crate) fn enable_scheduled_job_http_redirects(&mut self) {
        self.redirect_policy = RemoteRedirectPolicy::ScheduledJobHttp;
    }

    fn should_enforce_managed_table_controls(table_name: &TableName) -> bool {
        let name = table_name.as_ref();
        matches!(name, "ana" | "job" | "sys" | "rep")
            || (name.starts_with('n') && (name.len() == 24 || name == "nsystem"))
            || (name.len() == 6
                && name
                    .strip_prefix('s')
                    .is_some_and(|suffix| suffix.chars().all(|ch| ch.is_ascii_digit())))
    }

    pub(super) async fn enable_managed_table_controls(
        &self,
        table_name: &TableName,
    ) -> StorageResult<Option<String>> {
        if !Self::should_enforce_managed_table_controls(table_name) {
            return Ok(None);
        }

        self.wait_for_table_active(table_name).await?;
        if let Err(err) = self.enable_point_in_time_recovery(table_name).await {
            if Self::is_unsupported_managed_table_control(&err) {
                tracing::warn!(
                    "remote storage endpoint does not support managed table controls; skipping"
                );
                return Ok(None);
            }
            return Err(err);
        }
        let (update_table_response, _) = tokio::try_join!(
            self.enable_deletion_protection(table_name),
            self.enable_time_to_live(table_name),
        )?;
        self.wait_for_table_active(table_name).await?;
        Ok(update_table_response.table_description.latest_stream_arn)
    }

    fn is_unsupported_managed_table_control(error: &StorageError) -> bool {
        matches!(
            error.to_enum(),
            StorageEnum::Unsupported { .. } | StorageEnum::Validation { .. }
        ) && error
            .to_string()
            .contains("not yet supported on the AuxFn storage compatibility surface")
    }

    pub(super) async fn wait_for_table_active(&self, table_name: &TableName) -> StorageResult<()> {
        let delay = Duration::from_millis(TABLE_ACTIVE_RETRY_DELAY_MS);
        for _ in 0..TABLE_ACTIVE_RETRY_ATTEMPTS {
            let request = DescribeTableRequest {
                table_name: table_name.clone(),
            };
            match self
                .invoke::<_, DescribeTableResponse>("DynamoDB_20120810.DescribeTable", &request)
                .await
            {
                Ok(response) => match response.table.table_status {
                    TableStatus::Active => return Ok(()),
                    TableStatus::Creating | TableStatus::Updating => {}
                    status => {
                        return Err(StorageError::internal(&format!(
                            "table {table_name} unexpected status while applying controls: \
                             {status:?}"
                        )));
                    }
                },
                Err(err) => {
                    if !matches!(
                        err.to_enum(),
                        StorageEnum::TableNotFound { .. } | StorageEnum::ResourceNotFound { .. }
                    ) {
                        return Err(err);
                    }
                }
            }
            sleep(delay).await;
        }

        Err(StorageError::internal(&format!(
            "table {table_name} did not become ACTIVE after {TABLE_ACTIVE_RETRY_ATTEMPTS} attempts"
        )))
    }

    async fn enable_point_in_time_recovery(&self, table_name: &TableName) -> StorageResult<()> {
        let request = UpdateContinuousBackupsRequest {
            table_name: table_name.clone(),
            point_in_time_recovery_specification: PointInTimeRecoverySpecification {
                point_in_time_recovery_enabled: true,
            },
        };
        for attempt in 1..=PITR_RETRY_ATTEMPTS {
            match self
                .invoke_bytes("DynamoDB_20120810.UpdateContinuousBackups", &request)
                .await
            {
                Ok(_) => return Ok(()),
                Err(error)
                    if attempt < PITR_RETRY_ATTEMPTS
                        && Self::should_retry_point_in_time_recovery_error(&error) =>
                {
                    tracing::warn!(attempt, "point-in-time recovery setup failed, retrying");
                    sleep(Duration::from_secs(PITR_RETRY_DELAY_SECS)).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(StorageError::internal(
            "point-in-time recovery setup exhausted retries",
        ))
    }

    async fn enable_deletion_protection(
        &self,
        table_name: &TableName,
    ) -> StorageResult<UpdateTableResponse> {
        let request = UpdateTableRequest {
            deletion_protection_enabled: Some(true),
            ..UpdateTableRequest::new(table_name.clone())
        };
        self.invoke::<_, UpdateTableResponse>("DynamoDB_20120810.UpdateTable", &request)
            .await
    }

    async fn enable_time_to_live(&self, table_name: &TableName) -> StorageResult<()> {
        let request = UpdateTimeToLiveRequest {
            table_name: table_name.clone(),
            time_to_live_specification: TimeToLiveSpecification {
                attribute_name: MANAGED_TABLE_TTL_ATTRIBUTE.to_string(),
                enabled: true,
            },
        };
        self.invoke::<_, UpdateTimeToLiveResponse>("DynamoDB_20120810.UpdateTimeToLive", &request)
            .await?;
        Ok(())
    }

    pub(super) fn should_retry_point_in_time_recovery_error(error: &StorageError) -> bool {
        if Self::is_unsupported_managed_table_control(error) {
            return false;
        }
        match error.to_enum() {
            StorageEnum::TableNotFound { .. }
            | StorageEnum::ResourceNotFound { .. }
            | StorageEnum::ProvisionedThroughputExceeded { .. }
            | StorageEnum::Throttled { .. }
            | StorageEnum::LimitExceeded { .. }
            | StorageEnum::RequestLimitExceeded
            | StorageEnum::TransactionInProgress { .. }
            | StorageEnum::Validation { .. }
            | StorageEnum::InternalServerError { .. } => true,
            StorageEnum::AwsService { code, .. } => match code.as_deref() {
                Some(
                    "ContinuousBackupsUnavailableException"
                    | "PointInTimeRecoveryUnavailableException"
                    | "ResourceInUseException"
                    | "LimitExceededException"
                    | "ThrottlingException",
                ) => true,
                Some(_) => false,
                None => true,
            },
            _ => false,
        }
    }

    pub(super) fn should_suppress_operation_warning(operation: &str, error: &StorageError) -> bool {
        if Self::is_normal_operation_error(error)
            || matches!(error.to_enum(), StorageEnum::TransactionCanceled { .. })
        {
            return true;
        }

        operation == "DescribeTable"
            && matches!(
                error.to_enum(),
                StorageEnum::TableNotFound { .. } | StorageEnum::ResourceNotFound { .. }
            )
    }

    pub(super) fn is_normal_operation_error(error: &StorageError) -> bool {
        matches!(
            error.to_enum(),
            StorageEnum::ConditionalCheckFailed
                | StorageEnum::ConditionalCheckFailedWithItem { .. }
        )
    }

    pub(super) fn select_initial_endpoint(&self) -> (usize, bool) {
        let total = self.endpoints.len();
        debug_assert!(total > 0);
        let mut primary = self.primary_endpoint.load(Ordering::Relaxed);
        if primary == NO_ENDPOINT {
            increment_remote_leader_cache("miss");
            primary = 0;
            self.primary_endpoint.store(primary, Ordering::Relaxed);
        } else if primary >= total {
            increment_remote_leader_cache("invalidate");
            primary %= total;
            self.primary_endpoint.store(primary, Ordering::Relaxed);
        } else {
            increment_remote_leader_cache("hit");
        }

        let probation = self.probation_endpoint.load(Ordering::Relaxed);
        if total == 1 || probation == NO_ENDPOINT || probation >= total {
            if probation >= total && probation != NO_ENDPOINT {
                self.probation_endpoint
                    .store(NO_ENDPOINT, Ordering::Relaxed);
            }
            return (primary, false);
        }

        let mut prng = rng();
        let draw: f64 = prng.random();
        if draw < 0.9 {
            (primary, false)
        } else {
            (probation, true)
        }
    }

    pub(super) fn promote_primary(&self, new_primary: usize, previous_primary: usize) {
        if self.endpoints.len() <= 1 || new_primary >= self.endpoints.len() {
            return;
        }

        self.primary_endpoint.store(new_primary, Ordering::Relaxed);

        if previous_primary >= self.endpoints.len() || previous_primary == new_primary {
            self.probation_endpoint
                .store(NO_ENDPOINT, Ordering::Relaxed);
        } else {
            self.probation_endpoint
                .store(previous_primary, Ordering::Relaxed);
        }
    }

    pub(super) fn restore_primary_if_match(&self, endpoint_index: usize) {
        if self.endpoints.len() <= 1 {
            return;
        }

        if self.probation_endpoint.load(Ordering::Relaxed) == endpoint_index {
            self.primary_endpoint
                .store(endpoint_index, Ordering::Relaxed);
            self.probation_endpoint
                .store(NO_ENDPOINT, Ordering::Relaxed);
        }
    }

    pub(super) fn next_endpoint_index(&self, current: usize) -> usize {
        if self.endpoints.len() <= 1 {
            return current;
        }
        (current + 1) % self.endpoints.len()
    }

    pub(super) fn endpoint_index_for_leader_hint(&self, leader_hint: &str) -> Option<usize> {
        let leader_hint = normalized_endpoint_match_key(leader_hint);
        self.endpoints
            .iter()
            .position(|endpoint| normalized_endpoint_match_key(&endpoint.url) == leader_hint)
    }

    pub(super) async fn invoke<Request, Response>(
        &self,
        target: &str,
        request: &Request,
    ) -> StorageResult<Response>
    where
        Request: Serialize + ?Sized,
        Response: DeserializeOwned,
    {
        let bytes = self.invoke_bytes(target, request).await?;
        serde_json::from_slice(&bytes).map_err(StorageError::from)
    }

    pub(super) async fn invoke_bytes<Request>(
        &self,
        target: &str,
        request: &Request,
    ) -> StorageResult<Bytes>
    where
        Request: Serialize + ?Sized,
    {
        self.invoke_bytes_with_dispatch_status(target, request)
            .await
            .map_err(|failure| failure.error)
    }

    pub(super) async fn invoke_bytes_with_dispatch_status<Request>(
        &self,
        target: &str,
        request: &Request,
    ) -> Result<Bytes, RemoteInvocationError>
    where
        Request: Serialize + ?Sized,
    {
        let operation = extract_operation(target).to_string();
        let body = serde_json::to_vec(request).map_err(|err| {
            RemoteInvocationError::new(
                StorageError::from(err),
                HttpRequestDispatchStatus::NotDispatched,
            )
        })?;
        let body = Bytes::from(body);
        let total_endpoints = self.endpoints.len();
        let (mut current_index, mut using_probation) = self.select_initial_endpoint();
        if current_index >= total_endpoints {
            current_index = 0;
            using_probation = false;
        }
        // The last attempt's failure with its own dispatch status. A
        // `SendOnce` request never records an ambiguous failure: that attempt
        // returns `CommitUnknown`.
        let mut last_failure: Option<RemoteInvocationError> = None;
        let mut visited = 0;
        let mut total_attempts = 0;
        // Admission observes one pressure marker per logical request.  A
        // remote request may retry several times, but recording every retry
        // leaves connection-wide markers behind for later requests.
        let deadline = self
            .request_timeout
            .and_then(|timeout| Instant::now().checked_add(timeout));

        while visited < total_endpoints {
            let endpoint = &self.endpoints[current_index];
            let mut attempts_on_endpoint = 0;
            let mut next_override = None;

            loop {
                attempts_on_endpoint += 1;
                total_attempts += 1;

                let start = LatencyInstant::now();

                match self
                    .invoke_attempt_bytes(
                        endpoint,
                        &operation,
                        target,
                        &body,
                        attempts_on_endpoint - 1,
                        deadline,
                    )
                    .await
                {
                    Ok(response) => {
                        endpoint.failures.store(0, Ordering::Relaxed);
                        record_latency(&operation, "ok", start.elapsed());
                        if attempts_on_endpoint > 1 || visited > 0 {
                            tracing::info!(
                                latency_ms = start.elapsed().as_millis(),
                                "remote storage request succeeded after retry"
                            );
                        }
                        if total_attempts == 1 && visited == 0 {
                            self.retry_budget.refund_one();
                        }
                        if using_probation {
                            self.restore_primary_if_match(current_index);
                        }
                        return Ok(response);
                    }
                    Err(attempt_error) => {
                        let AttemptError {
                            error,
                            retryable,
                            code,
                            status,
                            leader_hint,
                            retry_after,
                            dispatch_status,
                        } = attempt_error;
                        let outcome = error.label();
                        let normal_operation_error = Self::is_normal_operation_error(&error);
                        record_latency(&operation, outcome, start.elapsed());

                        if !Self::should_suppress_operation_warning(&operation, &error) {
                            tracing::warn!(
                                retryable,
                                http_status = status.map(|s| s.as_u16()),
                                error_variant = outcome,
                                operation = operation.as_str(),
                                "remote storage request failed"
                            );
                        } else {
                            tracing::debug!(
                                operation = operation.as_str(),
                                "remote storage request failed"
                            );
                        }

                        if normal_operation_error {
                            endpoint.failures.store(0, Ordering::Relaxed);
                        } else {
                            let failures = endpoint.failures.fetch_add(1, Ordering::Relaxed) + 1;
                            if failures == FAILURE_ALERT_THRESHOLD {
                                tracing::error!(
                                    "remote storage endpoint marked unhealthy after repeated \
                                     failures"
                                );
                            }
                        }

                        let failure = RemoteInvocationError::new(error, dispatch_status);
                        match ReplayPolicy::for_target(target).classify_failure(&failure, retryable)
                        {
                            AfterFailure::CommitUnknown => {
                                record_retry_suppressed("commit_unknown");
                                return Err(RemoteInvocationError::new(
                                    StorageError::commit_unknown(REMOTE_WRITE_OUTCOME_UNKNOWN),
                                    HttpRequestDispatchStatus::PossiblyDispatched,
                                ));
                            }
                            AfterFailure::Return => {
                                if replay_safety_blocks_retry(code.as_deref()) {
                                    record_retry_suppressed("replay_safety");
                                }
                                return Err(failure);
                            }
                            AfterFailure::Retry => {}
                        }

                        last_failure = Some(failure);

                        let hinted_endpoint = code
                            .as_deref()
                            .filter(|code| *code == STORAGE_NOT_LEADER_ERROR_TYPE)
                            .and(leader_hint.as_deref())
                            .and_then(|hint| self.endpoint_index_for_leader_hint(hint))
                            .filter(|hinted_index| *hinted_index != current_index);
                        if let Some(hinted_index) = hinted_endpoint {
                            metrics_facade::counter!(
                                metrics_facade::CounterMetric::RemoteStorageFailoverCount,
                                "from_endpoint" => current_index.to_string(),
                                "to_endpoint" => hinted_index.to_string(),
                                "operation" => operation.clone()
                            )
                            .increment(1);
                            increment_remote_leader_cache("invalidate");
                            self.promote_primary(hinted_index, current_index);
                            next_override = Some(hinted_index);
                            break;
                        }

                        if total_attempts >= MAX_REMOTE_RETRIES {
                            record_retry_suppressed("attempt_budget");
                            break;
                        }

                        if attempts_on_endpoint >= MAX_ENDPOINT_RETRIES {
                            record_retry_suppressed("attempt_budget");
                            break;
                        }

                        let delay = retry_after
                            .unwrap_or(Duration::ZERO)
                            .max(compute_backoff(attempts_on_endpoint));
                        if !self.retry_budget.try_take() {
                            record_retry_suppressed("token_budget");
                            return Err(exhausted_failure(
                                last_failure,
                                "remote retry budget exhausted without a recorded error",
                            ));
                        }
                        if !retry_deadline_allows(deadline, delay) {
                            record_retry_suppressed("deadline");
                            return Err(exhausted_failure(
                                last_failure,
                                "remote retry deadline reached without a recorded error",
                            ));
                        }
                        if delay > Duration::ZERO {
                            tracing::info!(
                                retry_delay_ms = delay.as_millis(),
                                "retrying remote storage request with backoff"
                            );
                            sleep(delay).await;
                        }
                    }
                }
            }

            visited += 1;

            if total_attempts >= MAX_REMOTE_RETRIES {
                break;
            }

            if total_endpoints == 1 {
                break;
            }

            let next_index =
                next_override.unwrap_or_else(|| self.next_endpoint_index(current_index));
            if next_index == current_index {
                break;
            }

            if !self.retry_budget.try_take() {
                record_retry_suppressed("token_budget");
                return Err(exhausted_failure(
                    last_failure,
                    "remote retry budget exhausted without a recorded error",
                ));
            }
            if !retry_deadline_allows(deadline, Duration::ZERO) {
                record_retry_suppressed("deadline");
                return Err(exhausted_failure(
                    last_failure,
                    "remote retry deadline reached without a recorded error",
                ));
            }

            metrics_facade::counter!(
                metrics_facade::CounterMetric::RemoteStorageFailoverCount,
                "from_endpoint" => current_index.to_string(),
                "to_endpoint" => next_index.to_string(),
                "operation" => operation.clone()
            )
            .increment(1);

            if self.primary_endpoint.load(Ordering::Relaxed) == current_index {
                self.promote_primary(next_index, current_index);
            }

            current_index = next_index;
            using_probation = false;
        }

        Err(exhausted_failure(
            last_failure,
            "remote storage request exhausted retry budget without definitive outcome",
        ))
    }

    async fn invoke_once_bytes(
        &self,
        endpoint: &EndpointState,
        operation: &str,
        target: &str,
        body: &Bytes,
        deadline: Option<Instant>,
    ) -> Result<Bytes, AttemptError> {
        let redirectable = self.redirect_policy == RemoteRedirectPolicy::ScheduledJobHttp;
        if redirectable && endpoint.requires_signature {
            return Err(attempt_internal(
                "scheduled-job HTTP redirects cannot carry signed requests",
            ));
        }

        let mut base_headers = HeaderMap::new();
        base_headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/x-amz-json-1.0"),
        );
        base_headers.insert(
            "x-amz-target",
            HeaderValue::from_str(target)
                .map_err(|_| attempt_internal("invalid x-amz-target header value"))?,
        );
        let mut base_headers = Some(base_headers);
        let mut first_headers = Some(if endpoint.requires_signature {
            let signer = self
                .signer
                .as_ref()
                .ok_or_else(|| attempt_internal("missing SigV4 signer for AWS endpoint"))?;
            signer
                .sign_request(
                    "POST",
                    &endpoint.uri,
                    base_headers
                        .as_ref()
                        .ok_or_else(|| attempt_internal("base headers are missing"))?,
                    SignableBody::Bytes(body),
                )
                .await
                .map_err(|err| attempt_signing(&err))?
        } else if redirectable {
            base_headers
                .as_ref()
                .ok_or_else(|| attempt_internal("base headers are missing"))?
                .clone()
        } else {
            base_headers
                .take()
                .ok_or_else(|| attempt_internal("base headers are missing"))?
        });

        let mut request_url = endpoint.request_url.clone();
        let mut visited_urls =
            redirectable.then(|| HashSet::from([request_url.as_str().to_string()]));
        let mut possibly_dispatched = false;

        for redirect_hop in 0..=MAX_SCHEDULED_JOB_REDIRECT_HOPS {
            let request_headers = if redirect_hop == 0 {
                first_headers
                    .take()
                    .ok_or_else(|| attempt_internal("first request headers are missing"))?
            } else {
                base_headers
                    .as_ref()
                    .ok_or_else(|| attempt_internal("redirect base headers are missing"))?
                    .clone()
            };
            let mut request_headers = request_headers;
            if let Some(token) = self.internal_service_token.as_ref()
                && request_url.origin() == endpoint.request_url.origin()
            {
                request_headers.insert(INTERNAL_TOKEN_HEADER, token.clone());
            }
            let request = self
                .client
                .post(request_url.clone())
                .headers(request_headers)
                .body(body.clone())
                .send();
            let response = match deadline {
                Some(deadline) => {
                    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                        // Nothing of this hop was sent; an earlier hop may have
                        // been.
                        return Err(deadline_failure(possibly_dispatched));
                    };
                    tokio::time::timeout(remaining, request)
                        .await
                        // The request may be on the wire.
                        .map_err(|_| deadline_failure(true))?
                        .map_err(|error| {
                            merge_redirect_dispatch(attempt_transport(error), possibly_dispatched)
                        })?
                }
                None => request.await.map_err(|error| {
                    merge_redirect_dispatch(attempt_transport(error), possibly_dispatched)
                })?,
            };
            let status = response.status();

            if redirectable
                && matches!(
                    status,
                    StatusCode::TEMPORARY_REDIRECT | StatusCode::PERMANENT_REDIRECT
                )
            {
                possibly_dispatched = true;
                if redirect_hop == MAX_SCHEDULED_JOB_REDIRECT_HOPS {
                    return Err(redirect_failure(
                        status,
                        "scheduled-job redirect hop limit exceeded",
                    ));
                }
                let location = response
                    .headers()
                    .get(http::header::LOCATION)
                    .ok_or_else(|| {
                        redirect_failure(status, "scheduled-job redirect has no Location")
                    })?
                    .to_str()
                    .map_err(|_| {
                        redirect_failure(status, "scheduled-job redirect Location is invalid")
                    })?;
                let Some(visited_urls) = visited_urls.as_mut() else {
                    return Err(redirect_failure(
                        status,
                        "redirect policy did not create redirect state",
                    ));
                };
                request_url =
                    redirect_url(&request_url, location, visited_urls).map_err(|message| {
                        merge_redirect_dispatch(
                            redirect_failure(status, message),
                            possibly_dispatched,
                        )
                    })?;
                continue;
            }
            if redirectable && status.is_redirection() {
                return Err(redirect_failure(
                    status,
                    "scheduled-job redirects must preserve POST with 307 or 308",
                ));
            }

            let leader_hint = leader_hint_header(response.headers());
            let retry_after = parse_retry_after(response.headers());
            let bytes = match deadline {
                Some(deadline) => {
                    // The server has answered with a status: the request was
                    // sent.
                    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                        return Err(deadline_failure(true));
                    };
                    tokio::time::timeout(remaining, response.bytes())
                        .await
                        .map_err(|_| deadline_failure(true))?
                        .map_err(|error| {
                            merge_redirect_dispatch(attempt_transport(error), possibly_dispatched)
                        })?
                }
                None => response.bytes().await.map_err(|error| {
                    merge_redirect_dispatch(attempt_transport(error), possibly_dispatched)
                })?,
            };
            record_remote_payload_bytes(operation, body.len() as u64, bytes.len() as u64);

            if !status.is_success() {
                let error_body: RemoteErrorResponse =
                    serde_json::from_slice(&bytes).unwrap_or_default();
                let (error, retryable, code) = classify_error_response(status, error_body);
                let error = preserve_retry_after(error, retry_after);
                return Err(merge_redirect_dispatch(
                    AttemptError::new(error, retryable, code, Some(status))
                        .with_dispatch_status(
                            HttpRequestError::HttpStatus { status, body: None }.dispatch_status(),
                        )
                        .with_leader_hint(leader_hint)
                        .with_retry_after(retry_after),
                    possibly_dispatched,
                ));
            }

            return Ok(bytes);
        }

        Err(attempt_internal(
            "scheduled-job redirect processing exhausted",
        ))
    }

    #[instrument(
        name = "remote_storage.request",
        skip_all,
        fields(feature = "storage",

            remote.operation = operation,
            remote.retry_attempt = attempt,
            remote.credential_source = %self.credential_source)
    )]
    async fn invoke_attempt_bytes(
        &self,
        endpoint: &EndpointState,
        operation: &str,
        target: &str,
        body: &Bytes,
        attempt: usize,
        deadline: Option<Instant>,
    ) -> Result<Bytes, AttemptError> {
        self.invoke_once_bytes(endpoint, operation, target, body, deadline)
            .await
    }
}

pub(super) fn leader_hint_header(headers: &HeaderMap) -> Option<String> {
    headers
        .get(STORAGE_LEADER_HINT_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

fn redirect_failure(status: StatusCode, message: &'static str) -> AttemptError {
    AttemptError::new(StorageError::validation(message), false, None, Some(status))
        .with_dispatch_status(HttpRequestError::HttpStatus { status, body: None }.dispatch_status())
}

/// The client deadline elapsed. Known not applied only when nothing was sent.
fn deadline_failure(possibly_dispatched: bool) -> AttemptError {
    AttemptError::new(
        storage_types::expired_in(storage_types::AttemptPhase::Statement),
        true,
        None,
        None,
    )
    .with_dispatch_status(if possibly_dispatched {
        HttpRequestDispatchStatus::PossiblyDispatched
    } else {
        HttpRequestDispatchStatus::NotDispatched
    })
}

/// A transport failure that no hop could have sent never reached storage, so it
/// is reported as unavailable and any caller may repeat the operation.
fn merge_redirect_dispatch(mut error: AttemptError, possibly_dispatched: bool) -> AttemptError {
    if error.dispatch_status == HttpRequestDispatchStatus::NotDispatched {
        if possibly_dispatched {
            error.dispatch_status = HttpRequestDispatchStatus::PossiblyDispatched;
        } else {
            error.error = StorageError::service_unavailable(1);
        }
    }
    error
}

pub(super) fn parse_retry_after(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get("retry-after")?.to_str().ok()?.trim();
    let delay = value
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
        .or_else(|| {
            httpdate::parse_http_date(value)
                .ok()
                .and_then(|date| date.duration_since(SystemTime::now()).ok())
        })?;
    Some(delay.min(Duration::from_secs(MAX_RETRY_AFTER_SECS)))
}

pub(super) fn preserve_retry_after(
    error: StorageError,
    retry_after: Option<Duration>,
) -> StorageError {
    let Some(retry_after) = retry_after else {
        return error;
    };
    if !matches!(error.to_enum(), StorageEnum::ServiceUnavailable { .. }) {
        return error;
    }
    let retry_after_seconds = retry_after
        .as_secs()
        .saturating_add(u64::from(retry_after.subsec_nanos() > 0))
        .max(1);
    StorageError::service_unavailable(retry_after_seconds)
}

fn retry_deadline_allows(deadline: Option<Instant>, delay: Duration) -> bool {
    let Some(deadline) = deadline else {
        return true;
    };
    let minimum_attempt_budget = Duration::from_millis(MIN_RETRY_ATTEMPT_BUDGET_MS);
    Instant::now()
        .checked_add(delay.saturating_add(minimum_attempt_budget))
        .is_some_and(|next| next < deadline)
}

/// The answer when no attempt is left: the last attempt's failure, or fixed
/// text when no attempt failed (nothing was sent).
fn exhausted_failure(
    last_failure: Option<RemoteInvocationError>,
    missing: &'static str,
) -> RemoteInvocationError {
    last_failure.unwrap_or_else(|| {
        RemoteInvocationError::new(
            StorageError::internal(missing),
            HttpRequestDispatchStatus::NotDispatched,
        )
    })
}

fn normalized_endpoint_match_key(endpoint: &str) -> &str {
    endpoint.trim_end_matches('/')
}

fn increment_remote_leader_cache(outcome: &'static str) {
    match outcome {
        "hit" => metrics::counter!("storage.remote.leader.cache.hit.total").increment(1),
        "miss" => metrics::counter!("storage.remote.leader.cache.miss.total").increment(1),
        reason => metrics::counter!(
            "storage.remote.leader.cache.invalidate.total",
            "reason" => reason
        )
        .increment(1),
    }
}

fn record_retry_suppressed(reason: &'static str) {
    metrics::counter!("storage.remote.retry.suppressed.total", "reason" => reason).increment(1);
}

fn replay_safety_blocks_retry(code: Option<&str>) -> bool {
    matches!(
        code,
        Some("ConditionalCheckFailedException" | "TransactionCanceledException")
    )
}
