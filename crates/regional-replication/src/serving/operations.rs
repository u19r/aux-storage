use std::sync::Arc;

use async_trait::async_trait;
use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::{
    DeleteItemShape, DynamoBatchWriteShape, DynamoOperations, DynamoPort, DynamoTransactWriteShape,
    PutItemShape, UpdateItemShape,
};
use storage_types::{
    BatchGetItemRequest, BatchGetItemResponse, BatchWriteItemRequest, BatchWriteItemResponse,
    CreateTableRequest, CreateTableResponse, DeleteItemRequest, DeleteItemResponse,
    DeleteTableRequest, DeleteTableResponse, DescribeTableRequest, DescribeTableResponse,
    DescribeTimeToLiveRequest, DescribeTimeToLiveResponse, DynamoRequestValidate as _,
    GetItemRequest, GetItemResponse, GetStreamRecordsRequest, GetStreamRecordsResponse,
    ListStreamsRequest, ListStreamsResponse, ListTablesRequest, ListTablesResponse, PutItemRequest,
    PutItemResponse, QueryRequest, QueryResponse, ReadSequenceRequest, ReadSequenceResponse,
    ScanRequest, ScanResponse, StorageResult, StoredTableInfo, StreamItemId, TableName,
    TransactGetItemsRequest, TransactGetItemsResponse, TransactWriteItemsRequest,
    TransactWriteItemsResponse, UpdateItemRequest, UpdateItemResponse, UpdateTableRequest,
    UpdateTableResponse, UpdateTimeToLiveRequest, UpdateTimeToLiveResponse,
};

use super::runtime::RegionalDynamo;
use crate::origin::{execute_batch_write, execute_item_write, execute_transact_write};

#[async_trait]
impl<P: DynamoPort + 'static> DynamoOperations for RegionalDynamo<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    async fn get_table_info(&self, table_name: TableName) -> StorageResult<Arc<StoredTableInfo>> {
        self.journal.native_admission.require()?;
        self.read_point(super::catalog_reads::TableInfoRead(table_name))
            .await
    }

    async fn create_table(
        &self,
        request: CreateTableRequest,
    ) -> StorageResult<CreateTableResponse> {
        self.journal.native_admission.require()?;
        self.port.create_table(request).await
    }

    async fn list_tables(&self, request: ListTablesRequest) -> StorageResult<ListTablesResponse> {
        self.journal.native_admission.require()?;
        RegionalDynamo::list_tables(self, request).await
    }

    async fn delete_table(
        &self,
        request: DeleteTableRequest,
    ) -> StorageResult<DeleteTableResponse> {
        self.journal.native_admission.require()?;
        self.execute_local_catalog(request).await
    }

    async fn describe_table(
        &self,
        request: DescribeTableRequest,
    ) -> StorageResult<DescribeTableResponse> {
        self.journal.native_admission.require()?;
        self.describe_regional_table(request).await
    }

    async fn update_table(
        &self,
        request: UpdateTableRequest,
    ) -> StorageResult<UpdateTableResponse> {
        self.journal.native_admission.require()?;
        if request.aux_configuration_primary_region.is_some() {
            return self.execute_lifecycle_update(request).await;
        }
        if request.replica_updates.is_some() {
            return self.execute_replica_update(request).await;
        }
        self.execute_metadata_update(request).await
    }

    async fn put_item(&self, request: PutItemRequest) -> StorageResult<PutItemResponse> {
        self.journal.native_admission.require()?;
        let shape = PutItemShape::try_from_request(request)?;
        let result = execute_item_write(self.port.as_ref(), self.origin_execution(), shape).await;
        self.settle_response(result).await
    }

    async fn get_item(&self, request: GetItemRequest) -> StorageResult<GetItemResponse> {
        self.journal.native_admission.require()?;
        request
            .validate_for_dynamodb()
            .map_err(storage_types::StorageError::raw_validation)?;
        self.read_point(request).await
    }

    async fn update_item(&self, request: UpdateItemRequest) -> StorageResult<UpdateItemResponse> {
        self.journal.native_admission.require()?;
        let shape = UpdateItemShape::try_from_request(request)?;
        let result = execute_item_write(self.port.as_ref(), self.origin_execution(), shape).await;
        self.settle_response(result).await
    }

    async fn delete_item(&self, request: DeleteItemRequest) -> StorageResult<DeleteItemResponse> {
        self.journal.native_admission.require()?;
        let shape = DeleteItemShape::try_from_request(request)?;
        let result = execute_item_write(self.port.as_ref(), self.origin_execution(), shape).await;
        self.settle_response(result).await
    }

    async fn query(&self, request: QueryRequest) -> StorageResult<QueryResponse> {
        self.journal.native_admission.require()?;
        self.read_range(request).await
    }

    async fn query_page(
        &self,
        request: storage_types::QueryTableRequest,
    ) -> StorageResult<storage_provider::RangeReadPage<storage_types::WireItem>> {
        self.journal.native_admission.require()?;
        self.read_range(request).await
    }

    async fn query_source_page(
        &self,
        request: storage_types::QueryTableRequest,
    ) -> StorageResult<storage_provider::RangeReadPage<storage_provider::RangeReadItem>> {
        self.journal.native_admission.require()?;
        self.read_range(storage_provider::SourceQueryRequest(request))
            .await
    }

    async fn scan(&self, request: ScanRequest) -> StorageResult<ScanResponse> {
        self.journal.native_admission.require()?;
        self.read_range(request).await
    }

    async fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetItemResponse> {
        self.journal.native_admission.require()?;
        request
            .validate_for_dynamodb()
            .map_err(storage_types::StorageError::raw_validation)?;
        self.read_point(storage_provider::BatchGetPlan::from(request))
            .await
    }

    async fn batch_write_item(
        &self,
        request: BatchWriteItemRequest,
    ) -> StorageResult<BatchWriteItemResponse> {
        self.journal.native_admission.require()?;
        let shape = DynamoBatchWriteShape::try_from_request(request)?;
        let result = execute_batch_write(self.port.as_ref(), self.origin_execution(), shape).await;
        self.settle_response(result).await
    }

    async fn transact_get_items(
        &self,
        request: TransactGetItemsRequest,
    ) -> StorageResult<TransactGetItemsResponse> {
        self.journal.native_admission.require()?;
        self.read_point(storage_provider::TransactGetPlan::try_from(request)?)
            .await
    }

    async fn transact_write_items(
        &self,
        request: TransactWriteItemsRequest,
    ) -> StorageResult<TransactWriteItemsResponse> {
        self.journal.native_admission.require()?;
        let shape = DynamoTransactWriteShape::try_from_request(request)?;
        let result =
            execute_transact_write(self.port.as_ref(), self.origin_execution(), shape).await;
        self.settle_response(result).await
    }

    async fn transact_write_job_effect(
        &self,
        request: TransactWriteItemsRequest,
        authority: scheduled_bg_jobs::JobEffectAuthority,
    ) -> StorageResult<TransactWriteItemsResponse> {
        self.journal.native_admission.require()?;
        let shape = DynamoTransactWriteShape::try_from_job_effect(request, authority)?;
        let result =
            execute_transact_write(self.port.as_ref(), self.origin_execution(), shape).await;
        self.settle_response(result).await
    }

    async fn find_job_effect_receipt(
        &self,
        token: &str,
        authority: scheduled_bg_jobs::JobEffectAuthority,
    ) -> StorageResult<Option<TransactWriteItemsResponse>> {
        self.journal.native_admission.require()?;
        self.port.find_job_effect_receipt(token, authority).await
    }

    async fn transact_write_job_projection(
        &self,
        request: storage_provider::JobTerminalProjection,
        authority: scheduled_bg_jobs::JobTerminalAuthority,
    ) -> StorageResult<TransactWriteItemsResponse> {
        self.journal.native_admission.require()?;
        let shape = DynamoTransactWriteShape::try_from_job_projection(request, authority)?;
        let result =
            execute_transact_write(self.port.as_ref(), self.origin_execution(), shape).await;
        self.settle_response(result).await
    }

    async fn read_sequence(
        &self,
        request: ReadSequenceRequest,
    ) -> StorageResult<ReadSequenceResponse> {
        self.journal.native_admission.require()?;
        RegionalDynamo::read_sequence(self, request).await
    }

    async fn update_time_to_live(
        &self,
        request: UpdateTimeToLiveRequest,
    ) -> StorageResult<UpdateTimeToLiveResponse> {
        self.journal.native_admission.require()?;
        self.execute_ttl_update(request).await
    }

    async fn describe_time_to_live(
        &self,
        request: DescribeTimeToLiveRequest,
    ) -> StorageResult<DescribeTimeToLiveResponse> {
        self.journal.native_admission.require()?;
        self.read_point(request).await
    }

    async fn get_stream_records(
        &self,
        request: GetStreamRecordsRequest,
    ) -> StorageResult<GetStreamRecordsResponse> {
        self.journal.native_admission.require()?;
        request
            .table_name()
            .ok_or_else(|| storage_types::StorageError::raw_validation("TableName is required"))?;
        self.read_point(request).await
    }

    async fn latest_table_stream_pointer(
        &self,
        table_name: TableName,
    ) -> StorageResult<Option<StreamItemId>> {
        self.journal.native_admission.require()?;
        self.read_point(super::catalog_reads::StreamPointerRead(table_name))
            .await
    }

    async fn list_streams(
        &self,
        request: ListStreamsRequest,
    ) -> StorageResult<ListStreamsResponse> {
        self.journal.native_admission.require()?;
        RegionalDynamo::list_streams(self, request).await
    }

    async fn list_change_index_markers(
        &self,
        request: storage_provider::ListChangeIndexMarkersRequest,
    ) -> StorageResult<Vec<storage_provider::ChangeIndexMarker>> {
        self.journal.native_admission.require()?;
        self.port.list_change_index_markers(request).await
    }
}
