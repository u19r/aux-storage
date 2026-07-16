use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use http::{HeaderMap, HeaderValue, StatusCode, Uri};
use http_request::reqwest::Client;
use metrics_facade::counter;
use rand::{RngExt as _, rng};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use storage_provider::{RemoteCredentialStrategy, RemoteStorageSettings, StorageProvider};
use storage_sync::{SYNC_LEADER_HINT_HEADER, SYNC_NOT_LEADER_ERROR_TYPE};
use storage_types::{
    AttributeValue, BatchGetItemRequest, BatchGetWireItemResponse, BatchWriteItemRequest,
    BatchWriteItemResponse, CreateTableRequest, CreateTableResponse, DeleteItemRequest,
    DeleteItemResponse, DeleteTableRequest, DeleteTableResponse, DescribeTableRequest,
    DescribeTableResponse, DescribeTimeToLiveRequest, DescribeTimeToLiveResponse, DurationSeconds,
    GetItemRequest, KeyAttributes, ListTablesRequest, ListTablesResponse, PutItemRequest,
    PutItemResponse, QueryTableRequest, ScanTableRequest, StorageEnum, StorageError, StorageResult,
    StoredTableInfo, StreamItemId, StreamName, TableName, TableStatus, TimeToLiveSpecification,
    TransactWriteItemsRequest, TransactWriteItemsResponse, UpdateItemRequest, UpdateItemResponse,
    UpdateTableRequest, UpdateTableResponse, UpdateTimeToLiveRequest, UpdateTimeToLiveResponse,
    UserStreamName, WireItem,
    context::{ErrorContext as _, WrappedError},
};
use stream::{
    CursorName, CursorPage, CursorPosition, Stream, StreamCursor, StreamError, StreamPage,
    StreamPartitioningMode, StreamProvider, StreamResult,
};
use tokio::time::sleep;
use tracing::{Span, instrument};

fn record_items_returned(count: usize) {
    Span::current().record("items_returned", count as u64);
}

fn record_items_updated(count: usize) {
    Span::current().record("items_updated", count as u64);
}

fn record_remote_payload_bytes(
    operation: &str,
    endpoint: &str,
    request_bytes: u64,
    response_bytes: u64,
) {
    counter!(
        REMOTE_STORAGE_REQUEST_BYTES_TOTAL_METRIC,
        "operation" => operation.to_string(),
        "endpoint" => endpoint.to_string()
    )
    .increment(request_bytes);
    counter!(
        REMOTE_STORAGE_RESPONSE_BYTES_TOTAL_METRIC,
        "operation" => operation.to_string(),
        "endpoint" => endpoint.to_string()
    )
    .increment(response_bytes);
}

fn record_billed_item_ops(ddb_op: &str, item_kind: &str, direction: &str, count: u64) {
    counter!(
        STORAGE_BILLED_ITEM_OPS_TOTAL_METRIC,
        "ddb_op" => ddb_op.to_string(),
        "item_kind" => item_kind.to_string(),
        "direction" => direction.to_string()
    )
    .increment(count);
}

fn record_logical_item_bytes(ddb_op: &str, item_kind: &str, direction: &str, bytes: u64) {
    counter!(
        STORAGE_LOGICAL_ITEM_BYTES_TOTAL_METRIC,
        "ddb_op" => ddb_op.to_string(),
        "item_kind" => item_kind.to_string(),
        "direction" => direction.to_string()
    )
    .increment(bytes);
}

fn record_read_cost(ddb_op: &str, item_kind: &str, count: usize, bytes: u64) {
    record_billed_item_ops(ddb_op, item_kind, "read", count as u64);
    record_logical_item_bytes(ddb_op, item_kind, "read", bytes);
}

fn record_write_cost(ddb_op: &str, item_kind: &str, count: usize, bytes: u64) {
    record_billed_item_ops(ddb_op, item_kind, "write", count as u64);
    record_logical_item_bytes(ddb_op, item_kind, "write", bytes);
}

fn attr_map_payload_bytes<T>(item: &T) -> u64
where T: Serialize + ?Sized {
    serde_json::to_vec(item).map_or(0, |bytes| bytes.len() as u64)
}

fn serializable_payload_bytes<T>(value: &T) -> u64
where T: Serialize + ?Sized {
    serde_json::to_vec(value).map_or(0, |bytes| bytes.len() as u64)
}

fn wire_items_payload_bytes(items: &[WireItem]) -> u64 {
    items.iter().map(|item| item.payload_len() as u64).sum()
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct RemoteRequestContext {
    pub(super) table_name: Option<String>,
    pub(super) index_name: Option<String>,
    pub(super) item_pk: Option<String>,
    pub(super) item_sk: Option<String>,
}

pub(super) fn remote_request_context(body: &[u8]) -> RemoteRequestContext {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return RemoteRequestContext::default();
    };

    let table_name = value
        .get("TableName")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let index_name = value
        .get("IndexName")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let item_pk = dynamodb_string_attribute(&value, "Item", "pk");
    let item_sk = dynamodb_string_attribute(&value, "Item", "sk");

    RemoteRequestContext {
        table_name,
        index_name,
        item_pk,
        item_sk,
    }
}

fn dynamodb_string_attribute(value: &Value, item_key: &str, attribute_key: &str) -> Option<String> {
    value
        .get(item_key)?
        .get(attribute_key)?
        .get("S")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

#[derive(Default)]
pub(super) struct WriteCostTally {
    pub(super) put_ops: usize,
    pub(super) put_bytes: u64,
    pub(super) delete_ops: usize,
    pub(super) delete_bytes: u64,
    pub(super) update_ops: usize,
    pub(super) update_bytes: u64,
    pub(super) condition_check_ops: usize,
    pub(super) condition_check_bytes: u64,
}

impl WriteCostTally {
    pub(super) fn record_write_request(&mut self, request: &storage_types::WriteRequest) {
        if let Some(put_request) = request.put_request.as_ref() {
            self.put_ops = self.put_ops.saturating_add(1);
            self.put_bytes = self
                .put_bytes
                .saturating_add(attr_map_payload_bytes(&put_request.item));
        }
        if let Some(delete_request) = request.delete_request.as_ref() {
            self.delete_ops = self.delete_ops.saturating_add(1);
            self.delete_bytes = self
                .delete_bytes
                .saturating_add(attr_map_payload_bytes(&delete_request.key));
        }
    }

    pub(super) fn record_transact_item(&mut self, item: &storage_types::TransactWriteItem) {
        if let Some(put_request) = item.put.as_ref() {
            self.put_ops = self.put_ops.saturating_add(1);
            self.put_bytes = self
                .put_bytes
                .saturating_add(attr_map_payload_bytes(&put_request.item));
        }
        if let Some(delete_request) = item.delete.as_ref() {
            self.delete_ops = self.delete_ops.saturating_add(1);
            self.delete_bytes = self
                .delete_bytes
                .saturating_add(attr_map_payload_bytes(&delete_request.key));
        }
        if let Some(update_request) = item.update.as_ref() {
            self.update_ops = self.update_ops.saturating_add(1);
            self.update_bytes = self
                .update_bytes
                .saturating_add(serializable_payload_bytes(update_request));
        }
        if let Some(condition_check) = item.condition_check.as_ref() {
            self.condition_check_ops = self.condition_check_ops.saturating_add(1);
            self.condition_check_bytes = self
                .condition_check_bytes
                .saturating_add(serializable_payload_bytes(condition_check));
        }
    }

    pub(super) fn subtract(&self, other: &Self) -> Self {
        Self {
            put_ops: self.put_ops.saturating_sub(other.put_ops),
            put_bytes: self.put_bytes.saturating_sub(other.put_bytes),
            delete_ops: self.delete_ops.saturating_sub(other.delete_ops),
            delete_bytes: self.delete_bytes.saturating_sub(other.delete_bytes),
            update_ops: self.update_ops.saturating_sub(other.update_ops),
            update_bytes: self.update_bytes.saturating_sub(other.update_bytes),
            condition_check_ops: self
                .condition_check_ops
                .saturating_sub(other.condition_check_ops),
            condition_check_bytes: self
                .condition_check_bytes
                .saturating_sub(other.condition_check_bytes),
        }
    }

    fn emit(&self, ddb_op: &str) {
        record_write_cost(ddb_op, "put", self.put_ops, self.put_bytes);
        record_write_cost(ddb_op, "delete", self.delete_ops, self.delete_bytes);
        record_write_cost(ddb_op, "update", self.update_ops, self.update_bytes);
        record_write_cost(
            ddb_op,
            "condition_check",
            self.condition_check_ops,
            self.condition_check_bytes,
        );
    }
}

use aws_sigv4_signing::{AwsRequestSigner, AwsStaticCredentials, CredentialSource, SignableBody};

use super::{
    provider_helpers::{
        attempt_internal, attempt_signing, attempt_transport, build_client, build_endpoints,
        build_query_request, build_scan_request, compute_backoff, error_label, extract_operation,
        record_latency, signing_error_to_storage_error, to_table_info,
    },
    wire_item_helper::{parse_batch_get_wire, parse_get_item_wire, parse_scan_query_wire},
};
use crate::{
    constants::{
        AWS_SERVICE_NAME, FAILURE_ALERT_THRESHOLD, MANAGED_TABLE_TTL_ATTRIBUTE,
        MAX_ENDPOINT_RETRIES, MAX_REMOTE_RETRIES, PITR_RETRY_ATTEMPTS, PITR_RETRY_DELAY_SECS,
        REMOTE_STORAGE_REQUEST_BYTES_TOTAL_METRIC, REMOTE_STORAGE_RESPONSE_BYTES_TOTAL_METRIC,
        STORAGE_BILLED_ITEM_OPS_TOTAL_METRIC, STORAGE_LOGICAL_ITEM_BYTES_TOTAL_METRIC,
        TABLE_ACTIVE_RETRY_ATTEMPTS, TABLE_ACTIVE_RETRY_DELAY_MS,
    },
    error::{RemoteErrorResponse, classify_error_response},
};

pub(super) struct AttemptError {
    error: StorageError,
    retryable: bool,
    code: Option<String>,
    status: Option<StatusCode>,
    leader_hint: Option<String>,
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
        }
    }

    fn with_leader_hint(mut self, leader_hint: Option<String>) -> Self {
        self.leader_hint = leader_hint;
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
}

pub(super) const NO_ENDPOINT: usize = usize::MAX;

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
    async fn initialize_storage(&self) -> StorageResult<()> {
        Ok(())
    }

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "table_exists", table_name = %table_name))]
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

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "create_table", table_name = %request.table_name))]
    async fn create_table(&self, request: &CreateTableRequest) -> StorageResult<()> {
        let _response: CreateTableResponse = self
            .invoke("DynamoDB_20120810.CreateTable", request)
            .await?;
        self.enable_managed_table_controls(&request.table_name)
            .await?;
        Ok(())
    }

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "update_table_status", table_name = %_table_name))]
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
        let request = ListTablesRequest {
            exclusive_start_table_name,
            limit: Some(limit),
        };
        let response: ListTablesResponse = self
            .invoke("DynamoDB_20120810.ListTables", &request)
            .await?;

        let mut tables = Vec::with_capacity(response.table_names.len());
        for name in response.table_names {
            if let Some(info) = self
                .invoke_optional::<_, DescribeTableResponse>(
                    "DynamoDB_20120810.DescribeTable",
                    &DescribeTableRequest {
                        table_name: name.clone(),
                    },
                )
                .await?
            {
                tables.push(to_table_info(info.table));
            }
        }
        Ok(tables)
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "delete_table",
            table_name = %table_name,
            ddb_write = true,
            items_updated = tracing::field::Empty,
        )
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
            table_name = %request.table_name,
            ddb_write = true,
            items_updated = tracing::field::Empty,
        )
    )]
    async fn put_item_request(&self, request: PutItemRequest) -> StorageResult<PutItemResponse> {
        let response = self.invoke("DynamoDB_20120810.PutItem", &request).await?;
        record_items_updated(1);
        record_write_cost("put_item", "put", 1, attr_map_payload_bytes(&request.item));
        Ok(response)
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "get_item",
            table_name = %table_name,
            ddb_read = true,
            items_returned = tracing::field::Empty,
        )
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
            table_name = %request.table_name,
            ddb_write = true,
            items_updated = tracing::field::Empty,
        )
    )]
    async fn delete_item_request(
        &self,
        request: DeleteItemRequest,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        let response: DeleteItemResponse = self
            .invoke("DynamoDB_20120810.DeleteItem", &request)
            .await?;
        record_items_updated(usize::from(response.attributes.is_some()));
        record_write_cost(
            "delete_item",
            "delete",
            1,
            attr_map_payload_bytes(&request.key),
        );
        Ok(response.attributes.map(Into::into))
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "scan_table",
            table_name = %request.table_name,
            index_name = tracing::field::Empty,
            ddb_read = true,
            items_returned = tracing::field::Empty,
        )
    )]
    async fn scan_table(
        &self,
        request: &ScanTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        if let Some(idx) = request.index_name.as_ref() {
            Span::current().record("index_name", idx.to_string());
        }

        let remote_request = build_scan_request(request);
        let response = self
            .invoke_bytes("DynamoDB_20120810.Scan", &remote_request)
            .await?;
        let (items, last_evaluated_key) = parse_scan_query_wire(&response)?;
        record_items_returned(items.len());
        record_read_cost(
            "scan_table",
            "scan",
            1,
            wire_items_payload_bytes(items.as_slice()),
        );
        Ok((items, last_evaluated_key))
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "query_table",
            table_name = %request.table_name,
            index_name = tracing::field::Empty,
            ddb_read = true,
            items_returned = tracing::field::Empty,
        )
    )]
    async fn query_table(
        &self,
        request: &QueryTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        if let Some(idx) = request.index_name.as_ref() {
            Span::current().record("index_name", idx.to_string());
        }
        let remote_request = build_query_request(request);
        let response = self
            .invoke_bytes("DynamoDB_20120810.Query", &remote_request)
            .await?;
        let (items, last_evaluated_key) = parse_scan_query_wire(&response)?;
        record_items_returned(items.len());
        record_read_cost(
            "query_table",
            "query",
            1,
            wire_items_payload_bytes(items.as_slice()),
        );
        Ok((items, last_evaluated_key))
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "batch_write_item",
            ddb_write = true,
            items_updated = tracing::field::Empty,
        )
    )]
    async fn batch_write_item(
        &self,
        request: BatchWriteItemRequest,
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
            items_returned = tracing::field::Empty,
        )
    )]
    async fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetWireItemResponse> {
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
        Ok(BatchGetWireItemResponse {
            responses,
            unprocessed_keys: parsed.unprocessed_keys,
            consumed_capacity: None,
        })
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "update_item",
            table_name = %request.table_name,
            ddb_write = true,
            items_updated = tracing::field::Empty,
        )
    )]
    async fn update_item(&self, request: UpdateItemRequest) -> StorageResult<UpdateItemResponse> {
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
            items_updated = tracing::field::Empty,
        )
    )]
    async fn transact_write_items(
        &self,
        request: TransactWriteItemsRequest,
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

    async fn start_cleanup_task(&self, _interval_ms: usize) -> StreamResult<()> {
        Err(StreamError::internal("remote streams not implemented"))
    }

    async fn stop_cleanup_task(&self) -> StreamResult<()> {
        Err(StreamError::internal("remote streams not implemented"))
    }

    async fn cleanup_expired_items(&self) -> StreamResult<u64> {
        Err(StreamError::internal("remote streams not implemented"))
    }
}

impl RemoteStorageProvider {
    #[instrument(
        name = "remote_storage.init",
        skip(settings),
        fields(feature = "storage")
    )]
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

        Ok(Self {
            client,
            endpoints,
            signer,
            credential_source,
            primary_endpoint: AtomicUsize::new(0),
            probation_endpoint: AtomicUsize::new(NO_ENDPOINT),
        })
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

    async fn enable_managed_table_controls(
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
                    table = %table_name,
                    error = %err,
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

    async fn wait_for_table_active(&self, table_name: &TableName) -> StorageResult<()> {
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
                    tracing::warn!(
                        table = %table_name,
                        attempt,
                        error = %error,
                        "point-in-time recovery setup failed, retrying"
                    );
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
            table_name: table_name.clone(),
            attribute_definitions: None,
            billing_mode: None,
            provisioned_throughput: None,
            on_demand_throughput: None,
            deletion_protection_enabled: Some(true),
            global_secondary_index_updates: None,
            replica_updates: None,
            sse_specification: None,
            stream_specification: None,
            table_class: None,
            aux_stream_duration_hours: None,
            aux_default_item_stream_duration_hours: None,
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
        if Self::is_normal_operation_error(error) {
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

    async fn invoke<Request, Response>(
        &self,
        target: &str,
        request: &Request,
    ) -> StorageResult<Response>
    where
        Request: Serialize + ?Sized,
        Response: DeserializeOwned,
    {
        let bytes = self.invoke_bytes(target, request).await?;
        serde_json::from_slice(bytes.as_slice())
            .map_err(|err| StorageError::Base(StorageEnum::Serialization(err)))
    }

    async fn invoke_bytes<Request>(
        &self,
        target: &str,
        request: &Request,
    ) -> StorageResult<Vec<u8>>
    where
        Request: Serialize + ?Sized,
    {
        let operation = extract_operation(target).to_string();
        let body = serde_json::to_vec(request)
            .map_err(|err| StorageError::Base(StorageEnum::Serialization(err)))?;
        let total_endpoints = self.endpoints.len();
        let (mut current_index, mut using_probation) = self.select_initial_endpoint();
        if current_index >= total_endpoints {
            current_index = 0;
            using_probation = false;
        }
        let mut last_error: Option<StorageError> = None;
        let mut visited = 0;
        let mut total_attempts = 0;

        while visited < total_endpoints {
            let endpoint = &self.endpoints[current_index];
            let mut attempts_on_endpoint = 0;
            let mut next_override = None;

            loop {
                attempts_on_endpoint += 1;
                total_attempts += 1;

                let start = Instant::now();

                match self
                    .invoke_attempt_bytes(
                        endpoint,
                        &operation,
                        target,
                        &body,
                        attempts_on_endpoint - 1,
                    )
                    .await
                {
                    Ok(response) => {
                        endpoint.failures.store(0, Ordering::Relaxed);
                        record_latency(&operation, endpoint.url.as_str(), "ok", start.elapsed());
                        if attempts_on_endpoint > 1 || visited > 0 {
                            tracing::info!(
                                latency_ms = start.elapsed().as_millis(),
                                "remote storage request succeeded after retry"
                            );
                        }
                        if using_probation {
                            self.restore_primary_if_match(current_index);
                        }
                        return Ok(response);
                    }
                    Err(attempt_error) => {
                        let retryable = attempt_error.retryable;
                        let code = attempt_error.code.clone();
                        let status = attempt_error.status;
                        let leader_hint = attempt_error.leader_hint.clone();
                        let error = attempt_error.error;
                        let outcome = error_label(&error);
                        let normal_operation_error = Self::is_normal_operation_error(&error);
                        record_latency(&operation, endpoint.url.as_str(), outcome, start.elapsed());

                        if !Self::should_suppress_operation_warning(&operation, &error) {
                            let context = remote_request_context(&body);
                            tracing::warn!(
                                retryable,
                                http_status = status.map(|s| s.as_u16()),
                                error_code = code.as_deref().unwrap_or_default(),
                                error_variant = outcome,
                                error = %error,
                                operation = operation.as_str(),
                                endpoint = endpoint.url.as_str(),
                                table_name = context.table_name.as_deref().unwrap_or_default(),
                                index_name = context.index_name.as_deref().unwrap_or_default(),
                                item_pk = context.item_pk.as_deref().unwrap_or_default(),
                                item_sk = context.item_sk.as_deref().unwrap_or_default(),
                                "remote storage request failed"
                            );
                        }

                        if normal_operation_error {
                            endpoint.failures.store(0, Ordering::Relaxed);
                        } else {
                            let failures = endpoint.failures.fetch_add(1, Ordering::Relaxed) + 1;
                            if failures == FAILURE_ALERT_THRESHOLD {
                                tracing::error!(
                                    endpoint = endpoint.url.as_str(),
                                    "remote storage endpoint marked unhealthy after repeated \
                                     failures"
                                );
                            }
                        }

                        if !retryable {
                            return Err(error);
                        }

                        last_error = Some(error);

                        let hinted_endpoint = code
                            .as_deref()
                            .filter(|code| *code == SYNC_NOT_LEADER_ERROR_TYPE)
                            .and(leader_hint.as_deref())
                            .and_then(|hint| self.endpoint_index_for_leader_hint(hint))
                            .filter(|hinted_index| *hinted_index != current_index);
                        if let Some(hinted_index) = hinted_endpoint {
                            metrics_facade::counter!(
                                metrics_facade::CounterMetric::RemoteStorageFailoverCount,
                                "from_endpoint" => endpoint.url.clone(),
                                "to_endpoint" => self.endpoints[hinted_index].url.clone(),
                                "operation" => operation.clone()
                            )
                            .increment(1);
                            increment_remote_leader_cache("invalidate");
                            self.promote_primary(hinted_index, current_index);
                            next_override = Some(hinted_index);
                            break;
                        }

                        if total_attempts >= MAX_REMOTE_RETRIES {
                            break;
                        }

                        if attempts_on_endpoint >= MAX_ENDPOINT_RETRIES {
                            break;
                        }

                        let delay = compute_backoff(attempts_on_endpoint);
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

            metrics_facade::counter!(
                metrics_facade::CounterMetric::RemoteStorageFailoverCount,
                "from_endpoint" => self.endpoints[current_index].url.clone(),
                "to_endpoint" => self.endpoints[next_index].url.clone(),
                "operation" => operation.clone()
            )
            .increment(1);

            if self.primary_endpoint.load(Ordering::Relaxed) == current_index {
                self.promote_primary(next_index, current_index);
            }

            current_index = next_index;
            using_probation = false;
        }

        Err(last_error.unwrap_or_else(|| {
            StorageError::internal(
                "remote storage request exhausted retry budget without definitive outcome",
            )
        }))
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
        body: &[u8],
    ) -> Result<Vec<u8>, AttemptError> {
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

        let headers = if endpoint.requires_signature {
            let signer = self
                .signer
                .as_ref()
                .ok_or_else(|| attempt_internal("missing SigV4 signer for AWS endpoint"))?;
            signer
                .sign_request(
                    "POST",
                    &endpoint.uri,
                    &base_headers,
                    SignableBody::Bytes(body),
                )
                .await
                .map_err(|err| attempt_signing(&err))?
        } else {
            base_headers
        };

        let response = self
            .client
            .post(endpoint.url.as_str())
            .headers(headers.clone())
            .body(body.to_vec())
            .send()
            .await
            .map_err(|err| attempt_transport(&err))?;

        let status = response.status();
        let leader_hint = leader_hint_header(response.headers());
        let bytes = response
            .bytes()
            .await
            .map_err(|err| attempt_transport(&err))?;
        record_remote_payload_bytes(
            operation,
            endpoint.url.as_str(),
            body.len() as u64,
            bytes.len() as u64,
        );

        if !status.is_success() {
            let error_body: RemoteErrorResponse =
                serde_json::from_slice(&bytes).unwrap_or_default();
            let (error, retryable, code) = classify_error_response(status, error_body);
            return Err(AttemptError::new(error, retryable, code, Some(status))
                .with_leader_hint(leader_hint));
        }

        Ok(bytes.to_vec())
    }

    #[instrument(
        name = "remote_storage.request",
        skip_all,
        fields(feature = "storage",
            remote.endpoint = %endpoint.url,
            remote.operation = operation,
            remote.retry_attempt = attempt,
            remote.credential_source = %self.credential_source,
        )
    )]
    async fn invoke_attempt_bytes(
        &self,
        endpoint: &EndpointState,
        operation: &str,
        target: &str,
        body: &[u8],
        attempt: usize,
    ) -> Result<Vec<u8>, AttemptError> {
        let _ = attempt;
        self.invoke_once_bytes(endpoint, operation, target, body)
            .await
    }
}

fn leader_hint_header(headers: &HeaderMap) -> Option<String> {
    headers
        .get(SYNC_LEADER_HINT_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
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
