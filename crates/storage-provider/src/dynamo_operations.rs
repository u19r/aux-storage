//! The complete local DynamoDB data and table operation surface.
//!
//! SQL and KV stores implement `DynamoPort`; one shared blanket implementation
//! owns their attempt-scoped Dynamo lifecycle. Remote forwarding can implement
//! this public trait directly without a local native transaction.

use std::sync::Arc;

use async_trait::async_trait;
use storage_types::{
    BatchGetItemRequest, BatchGetItemResponse, BatchWriteItemRequest, BatchWriteItemResponse,
    CreateTableRequest, CreateTableResponse, DeleteItemRequest, DeleteItemResponse,
    DeleteTableRequest, DeleteTableResponse, DescribeTableRequest, DescribeTableResponse,
    DescribeTimeToLiveRequest, DescribeTimeToLiveResponse, GetItemRequest, GetItemResponse,
    GetStreamRecordsRequest, GetStreamRecordsResponse, ListStreamsRequest, ListStreamsResponse,
    ListTablesRequest, ListTablesResponse, PutItemRequest, PutItemResponse, QueryRequest,
    QueryResponse, ReadSequenceRequest, ReadSequenceResponse, ScanRequest, ScanResponse,
    StorageResult, StoredTableInfo, StreamItemId, TableName, TransactGetItemsRequest,
    TransactGetItemsResponse, TransactWriteItemsRequest, TransactWriteItemsResponse,
    UpdateItemRequest, UpdateItemResponse, UpdateTableRequest, UpdateTableResponse,
    UpdateTimeToLiveRequest, UpdateTimeToLiveResponse,
};

/// One public operation is one native attempt per try, including all required
/// metadata, condition, item, index, TTL, stream, and receipt effects.
///
/// ReadSequence may make several native requests within that attempt. Its
/// shared executor owns the graph and response semantics; backend capabilities
/// may combine independent steps without changing the public result.
#[async_trait]
pub trait DynamoOperations: Send + Sync {
    /// The replacement stores do not publish legacy change-index markers.
    async fn list_change_index_markers(
        &self,
        _request: crate::ListChangeIndexMarkersRequest,
    ) -> StorageResult<Vec<crate::ChangeIndexMarker>> {
        Err(storage_types::StorageError::unsupported(
            "change-index markers are unavailable",
        ))
    }

    async fn get_table_info(&self, table_name: TableName) -> StorageResult<Arc<StoredTableInfo>>;
    async fn create_table(&self, request: CreateTableRequest)
    -> StorageResult<CreateTableResponse>;
    async fn list_tables(&self, request: ListTablesRequest) -> StorageResult<ListTablesResponse>;
    async fn delete_table(&self, request: DeleteTableRequest)
    -> StorageResult<DeleteTableResponse>;
    async fn describe_table(
        &self,
        request: DescribeTableRequest,
    ) -> StorageResult<DescribeTableResponse>;
    async fn update_table(&self, request: UpdateTableRequest)
    -> StorageResult<UpdateTableResponse>;

    async fn put_item(&self, request: PutItemRequest) -> StorageResult<PutItemResponse>;
    async fn get_item(&self, request: GetItemRequest) -> StorageResult<GetItemResponse>;
    async fn update_item(&self, request: UpdateItemRequest) -> StorageResult<UpdateItemResponse>;
    async fn delete_item(&self, request: DeleteItemRequest) -> StorageResult<DeleteItemResponse>;
    async fn query(&self, request: QueryRequest) -> StorageResult<QueryResponse>;
    /// Internal bounded Query retains source accounting before projection.
    async fn query_page(
        &self,
        request: storage_types::QueryTableRequest,
    ) -> StorageResult<crate::RangeReadPage<storage_types::WireItem>>;
    /// Native source addresses are independent of persisted item attributes.
    async fn query_source_page(
        &self,
        _request: storage_types::QueryTableRequest,
    ) -> StorageResult<crate::RangeReadPage<crate::RangeReadItem>> {
        Err(storage_types::StorageError::unsupported(
            "physical range source addresses unavailable",
        ))
    }
    async fn scan(&self, request: ScanRequest) -> StorageResult<ScanResponse>;
    async fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetItemResponse>;
    async fn batch_write_item(
        &self,
        request: BatchWriteItemRequest,
    ) -> StorageResult<BatchWriteItemResponse>;
    async fn transact_get_items(
        &self,
        request: TransactGetItemsRequest,
    ) -> StorageResult<TransactGetItemsResponse>;
    async fn transact_write_items(
        &self,
        request: TransactWriteItemsRequest,
    ) -> StorageResult<TransactWriteItemsResponse>;
    /// Commits destination effects, their replay receipt, and the current
    /// scheduled-job authority check in one native transaction.
    async fn transact_write_job_effect(
        &self,
        _request: TransactWriteItemsRequest,
        _authority: scheduled_bg_jobs::JobEffectAuthority,
    ) -> StorageResult<TransactWriteItemsResponse> {
        Err(storage_types::StorageError::unsupported(
            "atomic scheduled-job effects unavailable",
        ))
    }
    /// Finds a committed logical effect without reapplying it. The caller token
    /// must bind the immutable business operation, including tenant and target.
    /// Receipt identity excludes replaceable claim generations. None proves no
    /// commit was observed; it does not authorize a subsequent effect.
    async fn find_job_effect_receipt(
        &self,
        _token: &str,
        _authority: scheduled_bg_jobs::JobEffectAuthority,
    ) -> StorageResult<Option<TransactWriteItemsResponse>> {
        Err(storage_types::StorageError::unsupported(
            "scheduled-job receipt lookup unavailable",
        ))
    }
    /// Publishes terminal status with a protected common lifecycle observation.
    /// This cannot authorize execution of a cancelled or dead-lettered job.
    async fn transact_write_job_projection(
        &self,
        _request: crate::JobTerminalProjection,
        _authority: scheduled_bg_jobs::JobTerminalAuthority,
    ) -> StorageResult<TransactWriteItemsResponse> {
        Err(storage_types::StorageError::unsupported(
            "atomic scheduled-job projections unavailable",
        ))
    }
    async fn read_sequence(
        &self,
        request: ReadSequenceRequest,
    ) -> StorageResult<ReadSequenceResponse>;

    async fn update_time_to_live(
        &self,
        request: UpdateTimeToLiveRequest,
    ) -> StorageResult<UpdateTimeToLiveResponse>;
    async fn describe_time_to_live(
        &self,
        request: DescribeTimeToLiveRequest,
    ) -> StorageResult<DescribeTimeToLiveResponse>;
    async fn get_stream_records(
        &self,
        request: GetStreamRecordsRequest,
    ) -> StorageResult<GetStreamRecordsResponse>;
    async fn latest_table_stream_pointer(
        &self,
        table_name: TableName,
    ) -> StorageResult<Option<StreamItemId>>;
    async fn list_streams(&self, request: ListStreamsRequest)
    -> StorageResult<ListStreamsResponse>;
}
