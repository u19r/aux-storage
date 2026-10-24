//! Dynamo operations forwarded to a remote Dynamo endpoint.
//!
//! The remote service owns each operation, including the complete ReadSequence
//! graph and its native snapshot.

use std::sync::Arc;

use async_trait::async_trait;
use storage_provider::DynamoOperations;
use storage_types::{
    BatchGetItemRequest, BatchGetItemResponse, BatchWriteItemRequest, BatchWriteItemResponse,
    CreateTableRequest, CreateTableResponse, DeleteItemRequest, DeleteItemResponse,
    DeleteTableRequest, DeleteTableResponse, DescribeTableRequest, DescribeTableResponse,
    DescribeTimeToLiveRequest, DescribeTimeToLiveResponse, GetItemRequest, GetItemResponse,
    GetStreamRecordsRequest, GetStreamRecordsResponse, ListStreamsRequest, ListStreamsResponse,
    ListTablesRequest, ListTablesResponse, PutItemRequest, PutItemResponse, QueryRequest,
    QueryResponse, ReadSequenceRequest, ReadSequenceResponse, ScanRequest, ScanResponse,
    StorageError, StorageResult, StoredTableInfo, StreamItemId, TableName, TransactGetItemsRequest,
    TransactGetItemsResponse, TransactWriteItemsRequest, TransactWriteItemsResponse,
    UpdateItemRequest, UpdateItemResponse, UpdateTableRequest, UpdateTableResponse,
    UpdateTimeToLiveRequest, UpdateTimeToLiveResponse, WireItem,
};

use super::RemoteStorageProvider;

#[async_trait]
impl DynamoOperations for RemoteStorageProvider {
    async fn list_change_index_markers(
        &self,
        request: storage_provider::ListChangeIndexMarkersRequest,
    ) -> StorageResult<Vec<storage_provider::ChangeIndexMarker>> {
        let response: storage_provider::ListChangeIndexMarkersResponse = self
            .invoke("DynamoDB_20120810.ListChangeIndexMarkers", &request)
            .await?;
        Ok(response.markers)
    }

    async fn get_table_info(&self, table_name: TableName) -> StorageResult<Arc<StoredTableInfo>> {
        storage_provider::StorageProvider::get_table_info(self, &table_name)
            .await
            .map(Arc::new)
    }

    async fn list_streams(
        &self,
        request: ListStreamsRequest,
    ) -> StorageResult<ListStreamsResponse> {
        self.invoke("DynamoDBStreams_20120810.ListStreams", &request)
            .await
    }

    async fn latest_table_stream_pointer(
        &self,
        _table_name: TableName,
    ) -> StorageResult<Option<StreamItemId>> {
        Err(StorageError::unsupported(
            "remote table stream tail is unavailable",
        ))
    }

    async fn get_stream_records(
        &self,
        request: GetStreamRecordsRequest,
    ) -> StorageResult<GetStreamRecordsResponse> {
        self.invoke("DynamoDB_20120810.GetStreamRecords", &request)
            .await
    }

    async fn create_table(
        &self,
        request: CreateTableRequest,
    ) -> StorageResult<CreateTableResponse> {
        let response = self
            .invoke("DynamoDB_20120810.CreateTable", &request)
            .await?;
        self.enable_managed_table_controls(&request.table_name)
            .await?;
        Ok(response)
    }

    async fn list_tables(&self, request: ListTablesRequest) -> StorageResult<ListTablesResponse> {
        self.invoke("DynamoDB_20120810.ListTables", &request).await
    }

    async fn delete_table(
        &self,
        request: DeleteTableRequest,
    ) -> StorageResult<DeleteTableResponse> {
        self.invoke("DynamoDB_20120810.DeleteTable", &request).await
    }

    async fn describe_table(
        &self,
        request: DescribeTableRequest,
    ) -> StorageResult<DescribeTableResponse> {
        self.invoke("DynamoDB_20120810.DescribeTable", &request)
            .await
    }

    async fn update_table(
        &self,
        request: UpdateTableRequest,
    ) -> StorageResult<UpdateTableResponse> {
        self.invoke("DynamoDB_20120810.UpdateTable", &request).await
    }

    async fn put_item(&self, request: PutItemRequest) -> StorageResult<PutItemResponse> {
        self.invoke("DynamoDB_20120810.PutItem", &request).await
    }

    async fn get_item(&self, request: GetItemRequest) -> StorageResult<GetItemResponse> {
        self.invoke("DynamoDB_20120810.GetItem", &request).await
    }

    async fn update_item(&self, request: UpdateItemRequest) -> StorageResult<UpdateItemResponse> {
        self.invoke("DynamoDB_20120810.UpdateItem", &request).await
    }

    async fn delete_item(&self, request: DeleteItemRequest) -> StorageResult<DeleteItemResponse> {
        self.invoke("DynamoDB_20120810.DeleteItem", &request).await
    }

    async fn query(&self, request: QueryRequest) -> StorageResult<QueryResponse> {
        self.invoke("DynamoDB_20120810.Query", &request).await
    }

    async fn query_page(
        &self,
        request: storage_types::QueryTableRequest,
    ) -> StorageResult<storage_provider::RangeReadPage<WireItem>> {
        self.query_table_with_metadata(&request, None).await
    }

    async fn query_source_page(
        &self,
        request: storage_types::QueryTableRequest,
    ) -> StorageResult<storage_provider::RangeReadPage<storage_provider::RangeReadItem>> {
        let page: storage_provider::RangeReadPage<storage_provider::RangeReadItem> = self
            .invoke("DynamoDB_20120810.QuerySourcePage", &request)
            .await?;
        page.finish()
    }

    async fn scan(&self, request: ScanRequest) -> StorageResult<ScanResponse> {
        self.invoke("DynamoDB_20120810.Scan", &request).await
    }

    async fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetItemResponse> {
        self.invoke("DynamoDB_20120810.BatchGetItem", &request)
            .await
    }

    async fn batch_write_item(
        &self,
        request: BatchWriteItemRequest,
    ) -> StorageResult<BatchWriteItemResponse> {
        self.invoke("DynamoDB_20120810.BatchWriteItem", &request)
            .await
    }

    async fn transact_get_items(
        &self,
        request: TransactGetItemsRequest,
    ) -> StorageResult<TransactGetItemsResponse> {
        self.invoke("DynamoDB_20120810.TransactGetItems", &request)
            .await
    }

    async fn transact_write_items(
        &self,
        request: TransactWriteItemsRequest,
    ) -> StorageResult<TransactWriteItemsResponse> {
        self.invoke("DynamoDB_20120810.TransactWriteItems", &request)
            .await
    }

    async fn read_sequence(
        &self,
        request: ReadSequenceRequest,
    ) -> StorageResult<ReadSequenceResponse> {
        self.invoke("DynamoDB_20120810.ReadSequence", &request)
            .await
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
        request: DescribeTimeToLiveRequest,
    ) -> StorageResult<DescribeTimeToLiveResponse> {
        self.invoke("DynamoDB_20120810.DescribeTimeToLive", &request)
            .await
    }
}
