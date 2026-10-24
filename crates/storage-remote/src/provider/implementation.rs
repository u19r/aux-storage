use std::{
    collections::{HashMap, HashSet},
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant as LatencyInstant, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{StreamExt as _, TryStreamExt as _, stream as future_stream};
use http::{HeaderMap, HeaderValue, StatusCode, Uri};
use http_request::{HttpRequestDispatchStatus, HttpRequestError, Url, reqwest::Client};
use metrics_facade::counter;
use rand::{RngExt as _, rng};
use serde::{Serialize, de::DeserializeOwned};
use storage_common::{
    WriteCostTally, record_read_cost, record_write_cost, serializable_payload_bytes,
    wire_items_payload_bytes,
};
use storage_provider::{
    BatchWriteContexts, MAX_RANGE_READ_EVALUATED_BYTES, OrderedOperationContexts, PhysicalWrite,
    RangeReadBudget, RangeReadPage, RemoteCredentialStrategy, RemoteStorageSettings,
    StorageProvider, TtlWriteContext,
};
use storage_types::{
    AllOld, AttributeValue, BatchGetItemRequest, BatchGetWireItemResponse, BatchWriteItemRequest,
    BatchWriteItemResponse, CreateTableRequest, CreateTableResponse, DeleteItemRequest,
    DeleteItemResponse, DeleteTableRequest, DeleteTableResponse, DescribeTableRequest,
    DescribeTableResponse, DescribeTimeToLiveRequest, DescribeTimeToLiveResponse, DurationSeconds,
    GetItemRequest, GetStreamRecordsRequest, GetStreamRecordsResponse, KeyAttributes,
    KeysAndAttributes, ListTablesRequest, ListTablesResponse, MAX_LIST_TABLES_LIMIT,
    PutItemRequest, PutItemResponse, QueryTableRequest, STORAGE_LEADER_HINT_HEADER,
    STORAGE_NOT_LEADER_ERROR_TYPE, ScanTableRequest, StorageEnum, StorageError, StorageResult,
    StoredTableInfo, StreamItemId, StreamName, StreamRecord, TableName, TableStatus,
    TimeToLiveSpecification, TransactGetItemsRequest, TransactGetItemsResponse,
    TransactWriteItemsRequest, TransactWriteItemsResponse, UpdateItemRequest, UpdateItemResponse,
    UpdateTableRequest, UpdateTableResponse, UpdateTimeToLiveRequest, UpdateTimeToLiveResponse,
    UserStreamName, WireItem,
    context::{ErrorContext as _, WrappedError},
};
use stream::{
    CursorName, CursorPage, CursorPosition, Stream, StreamCursor, StreamError, StreamPage,
    StreamPartitioningMode, StreamProvider, StreamResult,
};
use tokio::time::{Instant, sleep};
use tracing::{Span, instrument};

fn record_items_returned(count: usize) {
    Span::current().record("items_returned", count as u64);
}

fn record_items_updated(count: usize) {
    Span::current().record("items_updated", count as u64);
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
        build_scan_request, compute_backoff, error_label, extract_operation, record_latency,
        redirect_url, signing_error_to_storage_error, to_table_info,
    },
    wire_item_helper::{parse_batch_get_wire, parse_get_item_wire, parse_scan_query_wire},
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

const LIST_TABLE_DESCRIBE_CONCURRENCY: usize = 8;

#[derive(Default)]
pub(super) struct BatchGetProjection {
    attributes_to_get: Option<Vec<String>>,
    projection_expression: Option<String>,
    expression_attribute_names: Option<storage_types::VecMap<String, String>>,
    consistent_read: Option<bool>,
}

impl BatchGetProjection {
    pub(super) fn restore(self, target: &mut KeysAndAttributes) {
        target.attributes_to_get = self.attributes_to_get;
        target.projection_expression = self.projection_expression;
        target.expression_attribute_names = self.expression_attribute_names;
        target.consistent_read = self.consistent_read;
    }
}

pub(super) fn strip_batch_get_projections(
    request: &mut BatchGetItemRequest,
) -> HashMap<TableName, BatchGetProjection> {
    let mut projections = HashMap::with_capacity(request.request_items.len());
    for (table_name, keys_and_attributes) in &mut request.request_items {
        projections.insert(
            table_name.clone(),
            BatchGetProjection {
                attributes_to_get: keys_and_attributes.attributes_to_get.take(),
                projection_expression: keys_and_attributes.projection_expression.take(),
                expression_attribute_names: keys_and_attributes.expression_attribute_names.take(),
                consistent_read: keys_and_attributes.consistent_read,
            },
        );
    }
    projections
}

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
    pub(crate) pressure_signals: AtomicUsize,
    pub(crate) retry_budget: RetryTokenBucket,
    pub(crate) request_timeout: Option<Duration>,
    pub(crate) redirect_policy: RemoteRedirectPolicy,
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

#[async_trait]
impl StorageProvider for RemoteStorageProvider {
    fn take_admission_pressure_signal(&self) -> bool {
        self.pressure_signals.swap(0, Ordering::AcqRel) > 0
    }

    fn supports_native_transact_get_items(&self) -> bool {
        true
    }

    async fn initialize_storage(&self) -> StorageResult<()> {
        Ok(())
    }

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "table_exists"))]
    async fn table_exists(&self, table_name: &TableName) -> StorageResult<bool> {
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

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "create_table"))]
    async fn create_table(&self, request: &CreateTableRequest) -> StorageResult<()> {
        let _response: CreateTableResponse = self
            .invoke("DynamoDB_20120810.CreateTable", request)
            .await?;
        self.enable_managed_table_controls(&request.table_name)
            .await?;
        Ok(())
    }

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "update_table_status"))]
    async fn update_table_status(
        &self,
        _table_name: &TableName,
        _status: TableStatus,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn get_table_info(&self, table_name: &TableName) -> StorageResult<StoredTableInfo> {
        let request = DescribeTableRequest {
            table_name: table_name.clone(),
        };
        let response: DescribeTableResponse = self
            .invoke("DynamoDB_20120810.DescribeTable", &request)
            .await?;
        record_items_returned(1);
        Ok(to_table_info(response.table))
    }

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "list_tables"))]
    async fn list_tables(
        &self,
        limit: u32,
        exclusive_start_table_name: Option<TableName>,
    ) -> StorageResult<Vec<StoredTableInfo>> {
        let page_limit = limit.min(MAX_LIST_TABLES_LIMIT);
        let requested = limit as usize;
        let mut exclusive_start_table_name = exclusive_start_table_name;
        let mut tables = Vec::with_capacity(requested.min(page_limit as usize));

        loop {
            if tables.len() >= requested {
                break;
            }

            let request = ListTablesRequest {
                exclusive_start_table_name: exclusive_start_table_name.clone(),
                limit: Some(page_limit),
            };
            let response: ListTablesResponse = self
                .invoke("DynamoDB_20120810.ListTables", &request)
                .await?;

            let remaining = requested.saturating_sub(tables.len());
            let describe_results =
                future_stream::iter(response.table_names.into_iter().take(remaining).map(
                    |name| async move {
                        self.invoke_optional::<_, DescribeTableResponse>(
                            "DynamoDB_20120810.DescribeTable",
                            &DescribeTableRequest { table_name: name },
                        )
                        .await
                    },
                ))
                .buffered(LIST_TABLE_DESCRIBE_CONCURRENCY)
                .try_collect::<Vec<_>>()
                .await?;
            for info in describe_results.into_iter().flatten() {
                tables.push(to_table_info(info.table));
            }

            let Some(next_start) = response.last_evaluated_table_name else {
                break;
            };
            if exclusive_start_table_name.as_ref() == Some(&next_start) {
                return Err(StorageError::internal(
                    "remote ListTables continuation did not advance",
                ));
            }
            exclusive_start_table_name = Some(next_start);
        }

        Ok(tables)
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "delete_table",
            ddb_write = true,
            items_updated = tracing::field::Empty)
    )]
    async fn delete_table(&self, table_name: &TableName) -> StorageResult<()> {
        let request = DeleteTableRequest {
            table_name: table_name.clone(),
        };
        self.invoke::<_, DeleteTableResponse>("DynamoDB_20120810.DeleteTable", &request)
            .await?;
        record_items_updated(1);
        Ok(())
    }

    async fn create_table_storage(
        &self,
        _table_name: &TableName,
        _request: &CreateTableRequest,
    ) -> StorageResult<()> {
        Ok(())
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "put_item",
            ddb_write = true,
            items_updated = tracing::field::Empty)
    )]
    async fn put_item_request(
        &self,
        write: PhysicalWrite<PutItemRequest, TtlWriteContext>,
    ) -> StorageResult<PutItemResponse> {
        let request = write.request().clone();
        let response = self.invoke("DynamoDB_20120810.PutItem", &request).await?;
        record_items_updated(1);
        record_write_cost(
            "put_item",
            "put",
            1,
            serializable_payload_bytes(&request.item),
        );
        Ok(response)
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "get_item",
            ddb_read = true,
            items_returned = tracing::field::Empty)
    )]
    async fn get_item(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        consistent_read: bool,
    ) -> StorageResult<Option<WireItem>> {
        let mut request = GetItemRequest::new(table_name, key);
        request.consistent_read = Some(consistent_read);
        let response = self
            .invoke_bytes("DynamoDB_20120810.GetItem", &request)
            .await?;
        let item = parse_get_item_wire(&response)?;
        let items_returned = usize::from(item.is_some());
        record_items_returned(items_returned);
        let read_bytes = item
            .as_ref()
            .map_or(0, |wire_item| wire_item.payload_len() as u64);
        record_read_cost("get_item", "get", 1, read_bytes);
        Ok(item)
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "delete_item",
            ddb_write = true,
            items_updated = tracing::field::Empty)
    )]
    async fn delete_item_request(
        &self,
        write: PhysicalWrite<DeleteItemRequest, TtlWriteContext>,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        // The storage manager applies the customer's ReturnValues policy after
        // the provider returns its preimage. Native providers therefore return
        // the old item for every delete; request it explicitly from Remote.
        let mut request = write.request().clone();
        request.return_values = Some(AllOld::AllOld);
        let response: DeleteItemResponse = self
            .invoke("DynamoDB_20120810.DeleteItem", &request)
            .await?;
        record_items_updated(usize::from(response.attributes.is_some()));
        record_write_cost(
            "delete_item",
            "delete",
            1,
            serializable_payload_bytes(&request.key),
        );
        Ok(response.attributes.map(Into::into))
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "scan_table",
            ddb_read = true,
            items_returned = tracing::field::Empty)
    )]
    async fn scan_table(&self, request: &ScanTableRequest) -> StorageResult<RangeReadPage> {
        let remote_request = build_scan_request(request)?;
        let max_items = remote_request
            .limit
            .ok_or_else(|| StorageError::internal("remote scan request is missing its limit"))?;
        let response = self
            .invoke_bytes("DynamoDB_20120810.Scan", &remote_request)
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
            "scan_table",
            "scan",
            1,
            wire_items_payload_bytes(page.items.as_slice()),
        );
        Ok(page)
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "query_table",
            ddb_read = true,
            items_returned = tracing::field::Empty)
    )]
    async fn query_table(&self, request: &QueryTableRequest) -> StorageResult<RangeReadPage> {
        self.query_table_with_metadata(request, None).await
    }

    async fn query_table_with_table_info(
        &self,
        request: &QueryTableRequest,
        table_info: &StoredTableInfo,
    ) -> StorageResult<RangeReadPage> {
        self.query_table_with_metadata(request, Some(table_info))
            .await
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "batch_write_item",
            ddb_write = true,
            items_updated = tracing::field::Empty)
    )]
    async fn batch_write_item(
        &self,
        request: BatchWriteItemRequest,
        _contexts: BatchWriteContexts,
        _should_write_to_stream: bool,
    ) -> StorageResult<BatchWriteItemResponse> {
        let mut requested_tally = WriteCostTally::default();
        for write_requests in request.request_items.values() {
            for write_request in write_requests {
                requested_tally.record_write_request(write_request);
            }
        }
        let total_requested = request.request_items.values().map(Vec::len).sum::<usize>();
        let response: BatchWriteItemResponse = self
            .invoke("DynamoDB_20120810.BatchWriteItem", &request)
            .await?;
        let unprocessed = response
            .unprocessed_items
            .as_ref()
            .map_or(0, |items| items.values().map(Vec::len).sum::<usize>());
        record_items_updated(total_requested.saturating_sub(unprocessed));
        let mut unprocessed_tally = WriteCostTally::default();
        if let Some(unprocessed_items) = response.unprocessed_items.as_ref() {
            for write_requests in unprocessed_items.values() {
                for write_request in write_requests {
                    unprocessed_tally.record_write_request(write_request);
                }
            }
        }
        requested_tally
            .subtract(&unprocessed_tally)
            .emit("batch_write_item");
        Ok(response)
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "batch_get_item",
            ddb_read = true,
            items_returned = tracing::field::Empty)
    )]
    async fn batch_get_item(
        &self,
        mut request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetWireItemResponse> {
        // Projection is applied by the API manager after wire items are
        // decoded. Forward only the native BatchGet key/read envelope so a
        // remote origin cannot project the response a first time and cause a
        // second projection (notably list indexes) to erase attributes.
        let mut projections = strip_batch_get_projections(&mut request);
        let response = self
            .invoke_bytes("DynamoDB_20120810.BatchGetItem", &request)
            .await?;
        let parsed = parse_batch_get_wire(&response)?;
        let responses = parsed.responses;
        let returned = responses
            .as_ref()
            .map_or(0, |map| map.values().map(Vec::len).sum::<usize>());
        record_items_returned(returned);
        let requested = request
            .request_items
            .values()
            .map(|keys| keys.keys.len())
            .sum::<usize>();
        let read_bytes = responses.as_ref().map_or(0, |map| {
            map.values()
                .map(|items| wire_items_payload_bytes(items.as_slice()))
                .sum::<u64>()
        });
        record_read_cost("batch_get_item", "get", requested, read_bytes);
        let mut unprocessed_keys = parsed.unprocessed_keys;
        if let Some(unprocessed_keys) = unprocessed_keys.as_mut() {
            for (table_name, keys_and_attributes) in unprocessed_keys {
                if let Some(projection) = projections.remove(table_name) {
                    projection.restore(keys_and_attributes);
                }
            }
        }
        Ok(BatchGetWireItemResponse {
            responses: responses.map(Into::into),
            unprocessed_keys: unprocessed_keys.map(Into::into),
            consumed_capacity: None,
        })
    }

    #[instrument(
        skip_all,
        fields(feature = "storage", ddb_op = "transact_get_items", ddb_read = true)
    )]
    async fn transact_get_items(
        &self,
        request: TransactGetItemsRequest,
    ) -> StorageResult<TransactGetItemsResponse> {
        self.invoke("DynamoDB_20120810.TransactGetItems", &request)
            .await
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "update_item",
            ddb_write = true,
            items_updated = tracing::field::Empty)
    )]
    async fn update_item(
        &self,
        write: PhysicalWrite<UpdateItemRequest, TtlWriteContext>,
    ) -> StorageResult<UpdateItemResponse> {
        let request = write.request().clone();
        let response = self
            .invoke("DynamoDB_20120810.UpdateItem", &request)
            .await?;
        record_items_updated(1);
        record_write_cost(
            "update_item",
            "update",
            1,
            serializable_payload_bytes(&request),
        );
        Ok(response)
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "transact_write_items",
            ddb_write = true,
            items_updated = tracing::field::Empty)
    )]
    async fn transact_write_items(
        &self,
        request: TransactWriteItemsRequest,
        _contexts: OrderedOperationContexts,
    ) -> StorageResult<TransactWriteItemsResponse> {
        let mut tally = WriteCostTally::default();
        for transact_item in &request.transact_items {
            tally.record_transact_item(transact_item);
        }
        let total_items_updated = request
            .transact_items
            .iter()
            .filter(|item| item.put.is_some() || item.delete.is_some() || item.update.is_some())
            .count();
        let response = self
            .invoke("DynamoDB_20120810.TransactWriteItems", &request)
            .await?;
        record_items_updated(total_items_updated);
        tally.emit("transact_write_items");
        Ok(response)
    }

    async fn update_table(
        &self,
        request: UpdateTableRequest,
    ) -> StorageResult<UpdateTableResponse> {
        self.invoke("DynamoDB_20120810.UpdateTable", &request).await
    }

    async fn update_time_to_live(
        &self,
        request: UpdateTimeToLiveRequest,
    ) -> StorageResult<UpdateTimeToLiveResponse> {
        self.invoke("DynamoDB_20120810.UpdateTimeToLive", &request)
            .await
    }

    async fn describe_time_to_live(
        &self,
        table_name: &TableName,
    ) -> StorageResult<DescribeTimeToLiveResponse> {
        let request = DescribeTimeToLiveRequest {
            table_name: table_name.clone(),
        };
        self.invoke("DynamoDB_20120810.DescribeTimeToLive", &request)
            .await
    }
}

#[async_trait]
impl StreamProvider for RemoteStorageProvider {
    async fn initialize_stream(&self) -> StreamResult<()> {
        Ok(())
    }

    async fn create_stream(
        &self,
        _stream_name: UserStreamName,
        _ttl_seconds: Option<DurationSeconds>,
        _partitioning_mode: StreamPartitioningMode,
    ) -> StreamResult<StreamName> {
        Err(StreamError::internal("remote streams not implemented"))
    }

    async fn delete_stream(&self, _stream_name: UserStreamName) -> StreamResult<()> {
        Err(StreamError::internal("remote streams not implemented"))
    }

    async fn get_stream(&self, _stream_name: UserStreamName) -> StreamResult<Option<Stream>> {
        Err(StreamError::internal("remote streams not implemented"))
    }

    async fn append_item(
        &self,
        _stream_name: StreamName,
        _item_data: &[u8],
        _partition_key: Option<&str>,
    ) -> StreamResult<StreamItemId> {
        Err(StreamError::internal("remote streams not implemented"))
    }

    async fn read_forward(
        &self,
        _stream_name: StreamName,
        _exclusive_start_key: Option<StreamItemId>,
        _limit: u32,
    ) -> StreamResult<StreamPage> {
        Err(StreamError::internal("remote streams not implemented"))
    }

    async fn read_backward(
        &self,
        _stream_name: StreamName,
        _exclusive_start_key: Option<StreamItemId>,
        _limit: u32,
    ) -> StreamResult<StreamPage> {
        Err(StreamError::internal("remote streams not implemented"))
    }

    async fn get_stream_records_from_pointer_stream(
        &self,
        pointer_stream_name: StreamName,
        _key_schema: &[storage_types::KeySchemaElement],
        starting_item_id: Option<StreamItemId>,
        limit: Option<u32>,
    ) -> StreamResult<(Vec<StreamRecord>, Option<StreamItemId>)> {
        let stream_name = pointer_stream_name.as_ref();
        if pointer_stream_name == StreamName::system_table_stream() {
            return Err(StorageError::unsupported(
                "remote SystemStream logical lineage is unavailable",
            )
            .into());
        }
        let table_name = stream_name
            .strip_suffix(StreamName::TABLE_STREAM_SUFFIX)
            .and_then(|name| std::str::from_utf8(name).ok())
            .filter(|name| !name.is_empty())
            .ok_or_else(|| {
                StreamError::internal(format!(
                    "remote pointer stream is not a table stream: {pointer_stream_name:?}"
                ))
            })?;
        let request = GetStreamRecordsRequest::table(
            TableName::new(table_name),
            starting_item_id.map(|item_id| item_id.to_string()),
            limit,
        );
        let response: GetStreamRecordsResponse = self
            .invoke("DynamoDB_20120810.GetStreamRecords", &request)
            .await
            .map_err(StreamError::from)?;
        let (records, last_evaluated_key) = response.into_parts();
        let last_evaluated_key = last_evaluated_key
            .as_deref()
            .map(str::parse::<StreamItemId>)
            .transpose()
            .map_err(|error| {
                StreamError::internal(format!(
                    "remote stream response returned an invalid cursor: {error}"
                ))
            })?;
        Ok((records, last_evaluated_key))
    }

    async fn create_cursor(
        &self,
        _stream_name: StreamName,
        _cursor_name: CursorName,
        _position: CursorPosition,
    ) -> StreamResult<()> {
        Err(StreamError::internal("remote streams not implemented"))
    }

    async fn delete_cursor(
        &self,
        _stream_name: StreamName,
        _cursor_name: CursorName,
    ) -> StreamResult<()> {
        Err(StreamError::internal("remote streams not implemented"))
    }

    async fn read_from_cursor(
        &self,
        _stream_name: StreamName,
        _cursor_name: CursorName,
        _limit: u32,
    ) -> StreamResult<CursorPage> {
        Err(StreamError::internal("remote streams not implemented"))
    }

    async fn advance_cursor(
        &self,
        _stream_name: StreamName,
        _cursor_name: CursorName,
        _to_item_id: StreamItemId,
    ) -> StreamResult<()> {
        Err(StreamError::internal("remote streams not implemented"))
    }

    async fn get_cursor(
        &self,
        _stream_name: StreamName,
        _cursor_name: CursorName,
    ) -> StreamResult<Option<StreamCursor>> {
        Err(StreamError::internal("remote streams not implemented"))
    }
}

impl RemoteStorageProvider {
    pub(super) async fn query_table_with_metadata(
        &self,
        request: &QueryTableRequest,
        table_info: Option<&StoredTableInfo>,
    ) -> StorageResult<RangeReadPage> {
        let remote_request = build_query_request_view(request)?;
        let max_items = remote_request
            .limit
            .ok_or_else(|| StorageError::internal("remote query request is missing its limit"))?;
        let response = self
            .invoke_bytes("DynamoDB_20120810.Query", &remote_request)
            .await?;
        let parsed = parse_scan_query_wire(&response)?;
        let loaded_table_info = if table_info.is_none() {
            self.load_range_table_info_if_needed(
                &parsed.items,
                parsed.last_evaluated_key.as_ref(),
                &request.table_name,
            )
            .await?
        } else {
            None
        };
        let resolved_table_info = table_info.or(loaded_table_info.as_ref());
        let items_returned = parsed.items.len();
        let budget = RangeReadBudget::try_new(max_items, MAX_RANGE_READ_EVALUATED_BYTES)?;
        let page = parsed.into_page(resolved_table_info, &request.index_name, budget)?;
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
        self.get_table_info(table_name).await.map(Some)
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
            pressure_signals: AtomicUsize::new(0),
            retry_budget: RetryTokenBucket::new(),
            request_timeout,
            redirect_policy: RemoteRedirectPolicy::Disabled,
        })
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

    pub(super) async fn invoke_transaction(
        &self,
        request: &TransactWriteItemsRequest,
    ) -> Result<TransactWriteItemsResponse, RemoteInvocationError> {
        let bytes = self
            .invoke_bytes_with_dispatch_status("DynamoDB_20120810.TransactWriteItems", request)
            .await?;
        serde_json::from_slice(&bytes).map_err(|err| {
            RemoteInvocationError::new(
                StorageError::from(err),
                HttpRequestError::Decode.dispatch_status(),
            )
        })
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

    async fn invoke_bytes_with_dispatch_status<Request>(
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
        let mut last_error: Option<StorageError> = None;
        let mut request_dispatch_status = HttpRequestDispatchStatus::NotDispatched;
        let mut visited = 0;
        let mut total_attempts = 0;
        // Admission observes one pressure marker per logical request.  A
        // remote request may retry several times, but recording every retry
        // leaves connection-wide markers behind for later requests.
        let mut pressure_signalled = false;
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
                            dispatch_status: attempt_dispatch_status,
                        } = attempt_error;
                        if attempt_dispatch_status == HttpRequestDispatchStatus::PossiblyDispatched
                        {
                            request_dispatch_status = HttpRequestDispatchStatus::PossiblyDispatched;
                        }
                        let outcome = error_label(&error);
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

                        if !retryable {
                            if replay_safety_blocks_retry(code.as_deref()) {
                                record_retry_suppressed("replay_safety");
                            }
                            return Err(RemoteInvocationError::new(error, request_dispatch_status));
                        }

                        if !pressure_signalled && is_provider_pressure_error(&error) {
                            self.pressure_signals.fetch_add(1, Ordering::Relaxed);
                            pressure_signalled = true;
                        }

                        last_error = Some(error);

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
                            let Some(error) = last_error.take() else {
                                return Err(RemoteInvocationError::new(
                                    StorageError::internal(
                                        "remote retry budget exhausted without a recorded error",
                                    ),
                                    request_dispatch_status,
                                ));
                            };
                            return Err(RemoteInvocationError::new(error, request_dispatch_status));
                        }
                        if !retry_deadline_allows(deadline, delay) {
                            record_retry_suppressed("deadline");
                            let Some(error) = last_error.take() else {
                                return Err(RemoteInvocationError::new(
                                    StorageError::internal(
                                        "remote retry deadline reached without a recorded error",
                                    ),
                                    request_dispatch_status,
                                ));
                            };
                            return Err(RemoteInvocationError::new(error, request_dispatch_status));
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
                let Some(error) = last_error.take() else {
                    return Err(RemoteInvocationError::new(
                        StorageError::internal(
                            "remote retry budget exhausted without a recorded error",
                        ),
                        request_dispatch_status,
                    ));
                };
                return Err(RemoteInvocationError::new(error, request_dispatch_status));
            }
            if !retry_deadline_allows(deadline, Duration::ZERO) {
                record_retry_suppressed("deadline");
                let Some(error) = last_error.take() else {
                    return Err(RemoteInvocationError::new(
                        StorageError::internal(
                            "remote retry deadline reached without a recorded error",
                        ),
                        request_dispatch_status,
                    ));
                };
                return Err(RemoteInvocationError::new(error, request_dispatch_status));
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

        Err(RemoteInvocationError::new(
            last_error.unwrap_or_else(|| {
                StorageError::internal(
                    "remote storage request exhausted retry budget without definitive outcome",
                )
            }),
            request_dispatch_status,
        ))
    }

    async fn invoke_optional<Request, Response>(
        &self,
        target: &str,
        request: &Request,
    ) -> StorageResult<Option<Response>>
    where
        Request: Serialize + ?Sized,
        Response: DeserializeOwned,
    {
        match self.invoke(target, request).await {
            Ok(response) => Ok(Some(response)),
            Err(err) => match err.to_enum() {
                StorageEnum::TableNotFound { .. } | StorageEnum::ResourceNotFound { .. } => {
                    Ok(None)
                }
                _ => Err(err),
            },
        }
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
            let request = self
                .client
                .post(request_url.clone())
                .headers(request_headers)
                .body(body.clone())
                .send();
            let response = match deadline {
                Some(deadline) => {
                    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                        return Err(redirect_timeout_failure(possibly_dispatched));
                    };
                    tokio::time::timeout(remaining, request)
                        .await
                        .map_err(|_| redirect_timeout_failure(possibly_dispatched))?
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
                    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                        return Err(redirect_timeout_failure(possibly_dispatched));
                    };
                    tokio::time::timeout(remaining, response.bytes())
                        .await
                        .map_err(|_| redirect_timeout_failure(possibly_dispatched))?
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

fn redirect_timeout_failure(possibly_dispatched: bool) -> AttemptError {
    AttemptError::new(StorageError::service_unavailable(1), true, None, None).with_dispatch_status(
        if possibly_dispatched {
            HttpRequestDispatchStatus::PossiblyDispatched
        } else {
            HttpRequestDispatchStatus::NotDispatched
        },
    )
}

fn merge_redirect_dispatch(mut error: AttemptError, possibly_dispatched: bool) -> AttemptError {
    if possibly_dispatched && error.dispatch_status == HttpRequestDispatchStatus::NotDispatched {
        error.dispatch_status = HttpRequestDispatchStatus::PossiblyDispatched;
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

fn is_provider_pressure_error(error: &StorageError) -> bool {
    match error.to_enum() {
        StorageEnum::ProvisionedThroughputExceeded { .. }
        | StorageEnum::Throttled { .. }
        | StorageEnum::LimitExceeded { .. }
        | StorageEnum::RequestLimitExceeded
        | StorageEnum::ServiceUnavailable { .. }
        | StorageEnum::InternalServerError { .. } => true,
        StorageEnum::AwsService {
            code: Some(code), ..
        } => matches!(
            code.as_str(),
            "ServiceUnavailable"
                | "ServiceUnavailableException"
                | "RequestTimeout"
                | "RequestTimeoutException"
                | "ThrottlingException"
                | "ProvisionedThroughputExceededException"
                | "LimitExceededException"
        ),
        StorageEnum::AwsService { code: None, .. } => true,
        _ => false,
    }
}
