use std::sync::Arc;

use async_trait::async_trait;
use http_error::HttpApiError;
use storage::{AdmissionClass, DatabaseManager};
use storage_provider::ListChangeIndexMarkersRequest;
use storage_types::{
    BatchGetItemRequest, BatchWriteItemRequest, CreateTableRequest, DeleteItemRequest,
    DeleteTableRequest, DescribeStreamRequest, DescribeSystemStreamRequest, DescribeTableRequest,
    DescribeTimeToLiveRequest, GetItemRequest, GetRecordsRequest, GetShardIteratorRequest,
    GetStreamRecordsRequest, ListStreamsRequest, ListTablesRequest, PutItemRequest, QueryRequest,
    ReadSequenceRequest, ScanRequest, TransactGetItemsRequest, TransactWriteItemsRequest,
    UpdateItemRequest, UpdateTableRequest, UpdateTimeToLiveRequest,
};
use stream_provider::{
    SystemStreamSplitRequest, SystemStreamSplitResult, SystemStreamTrimPageRequest,
    SystemStreamTrimPageResult,
};

use crate::types::{Response, UpdateContinuousBackupsRequest};

pub struct StorageApiManagerImpl {
    db: Arc<DatabaseManager>,
}

impl StorageApiManagerImpl {
    #[must_use]
    pub fn new(db: Arc<DatabaseManager>) -> Self {
        Self { db }
    }

    #[must_use]
    pub fn db(&self) -> &Arc<DatabaseManager> {
        &self.db
    }
}

#[async_trait]
pub trait StorageApiManager: Send + Sync {
    async fn create_table(&self, request: CreateTableRequest) -> Result<Response, HttpApiError>;
    async fn list_tables(&self, request: ListTablesRequest) -> Result<Response, HttpApiError>;
    async fn delete_table(&self, request: DeleteTableRequest) -> Result<Response, HttpApiError>;
    async fn describe_table(&self, request: DescribeTableRequest)
    -> Result<Response, HttpApiError>;
    async fn put_item(&self, request: PutItemRequest) -> Result<Response, HttpApiError>;
    async fn get_item(&self, request: GetItemRequest) -> Result<Response, HttpApiError>;
    async fn delete_item(&self, request: DeleteItemRequest) -> Result<Response, HttpApiError>;
    async fn query(&self, request: QueryRequest) -> Result<Response, HttpApiError>;
    async fn query_source_page(
        &self,
        request: storage_types::QueryTableRequest,
    ) -> Result<Response, HttpApiError>;
    async fn scan(&self, request: ScanRequest) -> Result<Response, HttpApiError>;
    async fn batch_write_item(
        &self,
        request: BatchWriteItemRequest,
    ) -> Result<Response, HttpApiError>;
    async fn batch_get_item(&self, request: BatchGetItemRequest) -> Result<Response, HttpApiError>;
    async fn transact_write_items(
        &self,
        request: TransactWriteItemsRequest,
    ) -> Result<Response, HttpApiError>;
    async fn transact_get_items(
        &self,
        request: TransactGetItemsRequest,
    ) -> Result<Response, HttpApiError>;
    async fn read_sequence(&self, request: ReadSequenceRequest) -> Result<Response, HttpApiError>;
    async fn update_item(&self, request: UpdateItemRequest) -> Result<Response, HttpApiError>;
    async fn update_table(&self, request: UpdateTableRequest) -> Result<Response, HttpApiError>;
    async fn update_time_to_live(
        &self,
        request: UpdateTimeToLiveRequest,
    ) -> Result<Response, HttpApiError>;
    async fn describe_time_to_live(
        &self,
        request: DescribeTimeToLiveRequest,
    ) -> Result<Response, HttpApiError>;
    async fn update_continuous_backups(
        &self,
        request: UpdateContinuousBackupsRequest,
    ) -> Result<Response, HttpApiError>;
    async fn list_change_index_markers(
        &self,
        request: ListChangeIndexMarkersRequest,
    ) -> Result<Response, HttpApiError>;
    async fn get_stream_records(
        &self,
        request: GetStreamRecordsRequest,
    ) -> Result<Response, HttpApiError>;
    async fn describe_system_stream(
        &self,
        request: DescribeSystemStreamRequest,
    ) -> Result<Response, HttpApiError>;
    async fn split_system_stream(
        &self,
        request: SystemStreamSplitRequest,
    ) -> Result<SystemStreamSplitResult, HttpApiError>;
    async fn run_system_stream_trim_page(
        &self,
        request: SystemStreamTrimPageRequest,
    ) -> Result<SystemStreamTrimPageResult, HttpApiError>;
    async fn list_streams(&self, request: ListStreamsRequest) -> Result<Response, HttpApiError>;
    async fn latest_table_stream_pointer(
        &self,
        table_name: storage_types::TableName,
    ) -> Result<Option<storage_types::StreamItemId>, HttpApiError>;
    async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> Result<Response, HttpApiError> {
        crate::dynamodb_streams::DynamoStreamReader { manager: self }
            .describe_stream(request)
            .await
    }
    async fn get_shard_iterator(
        &self,
        request: GetShardIteratorRequest,
    ) -> Result<Response, HttpApiError> {
        crate::dynamodb_streams::DynamoStreamReader { manager: self }
            .get_shard_iterator(request)
            .await
    }
    async fn get_records(&self, request: GetRecordsRequest) -> Result<Response, HttpApiError> {
        crate::dynamodb_streams::DynamoStreamReader { manager: self }
            .get_records(request)
            .await
    }
    async fn clear_all_tables(&self, payload: serde_json::Value) -> Result<Response, HttpApiError>;
    async fn append_table_stream_record(
        &self,
        payload: serde_json::Value,
    ) -> Result<Response, HttpApiError>;
    async fn reconcile_table_stream_record(
        &self,
        payload: serde_json::Value,
    ) -> Result<Response, HttpApiError>;
    async fn read_canonical_history(
        &self,
        payload: serde_json::Value,
    ) -> Result<Response, HttpApiError>;
    async fn run_background_job(
        &self,
        payload: serde_json::Value,
    ) -> Result<Response, HttpApiError>;
}

#[async_trait]
impl StorageApiManager for StorageApiManagerImpl {
    async fn create_table(&self, request: CreateTableRequest) -> Result<Response, HttpApiError> {
        self.db
            .run_dynamo_admitted("default", AdmissionClass::Write, |dynamo| async move {
                dynamo.create_table(request).await
            })
            .await
            .map(Response::CreateTable)
            .map_err(Into::into)
    }

    async fn list_tables(&self, request: ListTablesRequest) -> Result<Response, HttpApiError> {
        self.db
            .run_dynamo_admitted("default", AdmissionClass::RangeRead, |dynamo| async move {
                dynamo.list_tables(request).await
            })
            .await
            .map(Response::ListTables)
            .map_err(Into::into)
    }

    async fn delete_table(&self, request: DeleteTableRequest) -> Result<Response, HttpApiError> {
        self.db
            .run_dynamo_admitted("default", AdmissionClass::Write, |dynamo| async move {
                dynamo.delete_table(request).await
            })
            .await
            .map(Response::DeleteTable)
            .map_err(Into::into)
    }

    async fn describe_table(
        &self,
        request: DescribeTableRequest,
    ) -> Result<Response, HttpApiError> {
        self.db
            .run_dynamo_admitted("default", AdmissionClass::PointRead, |dynamo| async move {
                dynamo.describe_table(request).await
            })
            .await
            .map(Response::DescribeTable)
            .map_err(Into::into)
    }

    async fn put_item(&self, request: PutItemRequest) -> Result<Response, HttpApiError> {
        self.db
            .run_dynamo_admitted("default", AdmissionClass::Write, |dynamo| async move {
                dynamo.put_item(request).await
            })
            .await
            .map(Response::PutItem)
            .map_err(Into::into)
    }

    async fn get_item(&self, request: GetItemRequest) -> Result<Response, HttpApiError> {
        self.db
            .run_dynamo_admitted("default", AdmissionClass::PointRead, |dynamo| async move {
                dynamo.get_item(request).await
            })
            .await
            .map(Response::GetItem)
            .map_err(Into::into)
    }

    async fn delete_item(&self, request: DeleteItemRequest) -> Result<Response, HttpApiError> {
        self.db
            .run_dynamo_admitted("default", AdmissionClass::Write, |dynamo| async move {
                dynamo.delete_item(request).await
            })
            .await
            .map(Response::DeleteItem)
            .map_err(Into::into)
    }

    async fn query(&self, request: QueryRequest) -> Result<Response, HttpApiError> {
        self.db
            .run_dynamo_admitted("default", AdmissionClass::RangeRead, |dynamo| async move {
                dynamo.query(request).await
            })
            .await
            .map(Response::Query)
            .map_err(Into::into)
    }

    async fn query_source_page(
        &self,
        request: storage_types::QueryTableRequest,
    ) -> Result<Response, HttpApiError> {
        self.db
            .query_source_page(request)
            .await
            .map(Response::QuerySourcePage)
            .map_err(Into::into)
    }

    async fn scan(&self, request: ScanRequest) -> Result<Response, HttpApiError> {
        self.db
            .run_dynamo_admitted("default", AdmissionClass::RangeRead, |dynamo| async move {
                dynamo.scan(request).await
            })
            .await
            .map(Response::Scan)
            .map_err(Into::into)
    }

    async fn batch_write_item(
        &self,
        request: BatchWriteItemRequest,
    ) -> Result<Response, HttpApiError> {
        self.db
            .run_dynamo_admitted("default", AdmissionClass::Write, |dynamo| async move {
                dynamo.batch_write_item(request).await
            })
            .await
            .map(Response::BatchWriteItem)
            .map_err(Into::into)
    }

    async fn batch_get_item(&self, request: BatchGetItemRequest) -> Result<Response, HttpApiError> {
        self.db
            .run_dynamo_admitted("default", AdmissionClass::RangeRead, |dynamo| async move {
                dynamo.batch_get_item(request).await
            })
            .await
            .map(Response::BatchGetItem)
            .map_err(Into::into)
    }

    async fn transact_write_items(
        &self,
        request: TransactWriteItemsRequest,
    ) -> Result<Response, HttpApiError> {
        self.db
            .run_dynamo_admitted("default", AdmissionClass::Write, |dynamo| async move {
                dynamo.transact_write_items(request).await
            })
            .await
            .map(Response::TransactWriteItems)
            .map_err(Into::into)
    }

    async fn transact_get_items(
        &self,
        request: TransactGetItemsRequest,
    ) -> Result<Response, HttpApiError> {
        self.db
            .transact_get_items(request)
            .await
            .map(Response::TransactGetItems)
            .map_err(Into::into)
    }

    async fn read_sequence(&self, request: ReadSequenceRequest) -> Result<Response, HttpApiError> {
        self.db
            .read_sequence(request)
            .await
            .map(Response::ReadSequence)
            .map_err(Into::into)
    }

    async fn update_item(&self, request: UpdateItemRequest) -> Result<Response, HttpApiError> {
        self.db
            .run_dynamo_admitted("default", AdmissionClass::Write, |dynamo| async move {
                dynamo.update_item(request).await
            })
            .await
            .map(Response::UpdateItem)
            .map_err(Into::into)
    }

    async fn update_table(&self, request: UpdateTableRequest) -> Result<Response, HttpApiError> {
        self.db
            .run_dynamo_admitted("default", AdmissionClass::Write, |dynamo| async move {
                dynamo.update_table(request).await
            })
            .await
            .map(Response::UpdateTable)
            .map_err(Into::into)
    }

    async fn update_time_to_live(
        &self,
        request: UpdateTimeToLiveRequest,
    ) -> Result<Response, HttpApiError> {
        self.db
            .run_dynamo_admitted("default", AdmissionClass::Write, |dynamo| async move {
                dynamo.update_time_to_live(request).await
            })
            .await
            .map(Response::UpdateTimeToLive)
            .map_err(Into::into)
    }

    async fn describe_time_to_live(
        &self,
        request: DescribeTimeToLiveRequest,
    ) -> Result<Response, HttpApiError> {
        self.db
            .run_dynamo_admitted("default", AdmissionClass::PointRead, |dynamo| async move {
                dynamo.describe_time_to_live(request).await
            })
            .await
            .map(Response::DescribeTimeToLive)
            .map_err(Into::into)
    }

    async fn update_continuous_backups(
        &self,
        request: UpdateContinuousBackupsRequest,
    ) -> Result<Response, HttpApiError> {
        let UpdateContinuousBackupsRequest {
            table_name,
            point_in_time_recovery_specification,
        } = request;
        let _ = (
            table_name,
            point_in_time_recovery_specification.point_in_time_recovery_enabled,
        );
        Err(HttpApiError::validation_error(
            "UpdateContinuousBackups is not yet supported on the AuxFn storage compatibility \
             surface",
        ))
    }

    async fn list_change_index_markers(
        &self,
        request: ListChangeIndexMarkersRequest,
    ) -> Result<Response, HttpApiError> {
        self.list_change_index_markers_internal(request).await
    }

    async fn get_stream_records(
        &self,
        request: GetStreamRecordsRequest,
    ) -> Result<Response, HttpApiError> {
        self.get_stream_records_internal(request).await
    }

    async fn describe_system_stream(
        &self,
        request: DescribeSystemStreamRequest,
    ) -> Result<Response, HttpApiError> {
        self.describe_system_stream_internal(request).await
    }

    async fn split_system_stream(
        &self,
        request: SystemStreamSplitRequest,
    ) -> Result<SystemStreamSplitResult, HttpApiError> {
        self.split_system_stream_internal(request).await
    }

    async fn run_system_stream_trim_page(
        &self,
        request: SystemStreamTrimPageRequest,
    ) -> Result<SystemStreamTrimPageResult, HttpApiError> {
        self.run_system_stream_trim_page_internal(request).await
    }

    async fn list_streams(&self, request: ListStreamsRequest) -> Result<Response, HttpApiError> {
        self.db
            .run_dynamo_admitted("default", AdmissionClass::RangeRead, |dynamo| async move {
                dynamo.list_streams(request).await
            })
            .await
            .map(Response::ListStreams)
            .map_err(Into::into)
    }

    async fn latest_table_stream_pointer(
        &self,
        table_name: storage_types::TableName,
    ) -> Result<Option<storage_types::StreamItemId>, HttpApiError> {
        self.db
            .run_dynamo_admitted("default", AdmissionClass::RangeRead, |dynamo| async move {
                dynamo.latest_table_stream_pointer(table_name).await
            })
            .await
            .map_err(Into::into)
    }

    async fn clear_all_tables(&self, payload: serde_json::Value) -> Result<Response, HttpApiError> {
        self.clear_all_tables_internal(payload).await
    }

    async fn append_table_stream_record(
        &self,
        payload: serde_json::Value,
    ) -> Result<Response, HttpApiError> {
        self.append_table_stream_record_internal(payload).await
    }

    async fn reconcile_table_stream_record(
        &self,
        payload: serde_json::Value,
    ) -> Result<Response, HttpApiError> {
        self.reconcile_table_stream_record_internal(payload).await
    }

    async fn read_canonical_history(
        &self,
        payload: serde_json::Value,
    ) -> Result<Response, HttpApiError> {
        self.read_canonical_history_response(payload).await
    }

    async fn run_background_job(
        &self,
        payload: serde_json::Value,
    ) -> Result<Response, HttpApiError> {
        self.run_background_job_internal(payload).await
    }
}
