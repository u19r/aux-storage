//! Wire-level DynamoDB entries: an AWS request in, its AWS response out. The
//! HTTP front door calls these; typed library entries build a request and call
//! them. Each one is the single place that adds admission and operation
//! metrics.
use storage_types::{
    CreateTableRequest, CreateTableResponse, DeleteItemRequest, DeleteItemResponse,
    DeleteTableRequest, DeleteTableResponse, DescribeStreamRequest, DescribeStreamResponse,
    DescribeTableRequest, DescribeTableResponse, DescribeTimeToLiveRequest,
    DescribeTimeToLiveResponse, GetItemRequest, GetItemResponse, GetRecordsRequest,
    GetRecordsResponse, GetShardIteratorRequest, GetShardIteratorResponse, ListStreamsRequest,
    ListStreamsResponse, ListTablesRequest, ListTablesResponse, PutItemRequest, PutItemResponse,
    QueryRequest, QueryResponse, ScanRequest, ScanResponse, StorageResult, UpdateItemRequest,
    UpdateItemResponse,
};

use crate::{
    admission::AdmissionClass,
    database_manager::{DatabaseManager, ROUTED_DEFAULT_CONNECTION_ID, record_storage_operation},
    updated_at_apply::{inject_updated_at_into_update_expression, stamp_wire_item_now},
};

impl DatabaseManager {
    pub async fn create_table_request(
        &self,
        request: CreateTableRequest,
    ) -> StorageResult<CreateTableResponse> {
        self.run_dynamo_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::Write,
            |dynamo| {
                record_storage_operation("create_table", async move {
                    dynamo.create_table(request).await
                })
            },
        )
        .await
    }

    pub async fn delete_table_request(
        &self,
        request: DeleteTableRequest,
    ) -> StorageResult<DeleteTableResponse> {
        self.run_dynamo_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::Write,
            |dynamo| {
                record_storage_operation("delete_table", async move {
                    dynamo.delete_table(request).await
                })
            },
        )
        .await
    }

    pub async fn describe_table(
        &self,
        request: DescribeTableRequest,
    ) -> StorageResult<DescribeTableResponse> {
        self.run_dynamo_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::PointRead,
            |dynamo| {
                record_storage_operation("describe_table", async move {
                    dynamo.describe_table(request).await
                })
            },
        )
        .await
    }

    pub async fn list_tables(
        &self,
        request: ListTablesRequest,
    ) -> StorageResult<ListTablesResponse> {
        self.run_dynamo_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::RangeRead,
            |dynamo| {
                record_storage_operation(
                    "list_tables",
                    async move { dynamo.list_tables(request).await },
                )
            },
        )
        .await
    }

    /// Stamps `updated_at` in single-table mode, like `batch_write_item`.
    pub async fn put_item_request(
        &self,
        mut request: PutItemRequest,
    ) -> StorageResult<PutItemResponse> {
        if self.single_table_mode_enabled() {
            stamp_wire_item_now(&mut request.item)?;
        }
        self.run_dynamo_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::Write,
            |dynamo| {
                record_storage_operation("put_item", async move { dynamo.put_item(request).await })
            },
        )
        .await
    }

    pub async fn get_item_request(
        &self,
        request: GetItemRequest,
    ) -> StorageResult<GetItemResponse> {
        self.run_dynamo_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::PointRead,
            |dynamo| {
                record_storage_operation("get_item", async move { dynamo.get_item(request).await })
            },
        )
        .await
    }

    pub async fn delete_item_request(
        &self,
        request: DeleteItemRequest,
    ) -> StorageResult<DeleteItemResponse> {
        self.run_dynamo_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::Write,
            |dynamo| {
                record_storage_operation(
                    "delete_item",
                    async move { dynamo.delete_item(request).await },
                )
            },
        )
        .await
    }

    /// Injects `updated_at` into a non-empty update expression in single-table
    /// mode.
    pub async fn update_item_request(
        &self,
        mut request: UpdateItemRequest,
    ) -> StorageResult<UpdateItemResponse> {
        if self.single_table_mode_enabled()
            && let Some(expression) = request
                .update_expression
                .as_mut()
                .filter(|expression| !expression.trim().is_empty())
        {
            inject_updated_at_into_update_expression(
                expression,
                &mut request.expression_attribute_names,
                &mut request.expression_attribute_values,
            )?;
        }
        self.run_dynamo_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::Write,
            |dynamo| {
                record_storage_operation(
                    "update_item",
                    async move { dynamo.update_item(request).await },
                )
            },
        )
        .await
    }

    pub async fn query(&self, request: QueryRequest) -> StorageResult<QueryResponse> {
        self.run_dynamo_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::RangeRead,
            |dynamo| record_storage_operation("query", async move { dynamo.query(request).await }),
        )
        .await
    }

    pub async fn scan(&self, request: ScanRequest) -> StorageResult<ScanResponse> {
        self.run_dynamo_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::RangeRead,
            |dynamo| {
                record_storage_operation("scan_table", async move { dynamo.scan(request).await })
            },
        )
        .await
    }

    pub async fn describe_time_to_live_request(
        &self,
        request: DescribeTimeToLiveRequest,
    ) -> StorageResult<DescribeTimeToLiveResponse> {
        self.run_dynamo_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::PointRead,
            |dynamo| {
                record_storage_operation("describe_time_to_live", async move {
                    dynamo.describe_time_to_live(request).await
                })
            },
        )
        .await
    }

    pub async fn list_streams(
        &self,
        request: ListStreamsRequest,
    ) -> StorageResult<ListStreamsResponse> {
        self.run_dynamo_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::RangeRead,
            |dynamo| {
                record_storage_operation("list_streams", async move {
                    dynamo.list_streams(request).await
                })
            },
        )
        .await
    }

    pub async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> StorageResult<DescribeStreamResponse> {
        self.run_dynamo_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::RangeRead,
            |dynamo| {
                record_storage_operation("describe_stream", async move {
                    dynamo.describe_stream(request).await
                })
            },
        )
        .await
    }

    pub async fn get_shard_iterator(
        &self,
        request: GetShardIteratorRequest,
    ) -> StorageResult<GetShardIteratorResponse> {
        self.run_dynamo_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::RangeRead,
            |dynamo| {
                record_storage_operation("get_shard_iterator", async move {
                    dynamo.get_shard_iterator(request).await
                })
            },
        )
        .await
    }

    pub async fn get_records(
        &self,
        request: GetRecordsRequest,
    ) -> StorageResult<GetRecordsResponse> {
        self.run_dynamo_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::RangeRead,
            |dynamo| {
                record_storage_operation(
                    "get_records",
                    async move { dynamo.get_records(request).await },
                )
            },
        )
        .await
    }
}
