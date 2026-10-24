//! Shared public Dynamo operation lifecycle over one family-owned native
//! attempt.

use std::{future::Future, sync::Arc};

use async_trait::async_trait;
use futures::future::BoxFuture;
use storage_common::retry::retry_native_conflicts;
use storage_types::{
    AttemptMode, BatchGetItemRequest, BatchGetItemResponse, BatchWriteItemRequest,
    BatchWriteItemResponse, Clock, CreateTableRequest, CreateTableResponse, DeleteItemRequest,
    DeleteItemResponse, DeleteTableRequest, DeleteTableResponse, DescribeTableRequest,
    DescribeTableResponse, DescribeTimeToLiveRequest, DescribeTimeToLiveResponse, GetItemRequest,
    GetItemResponse, GetStreamRecordsRequest, GetStreamRecordsResponse, IndexName,
    ListStreamsRequest, ListStreamsResponse, ListTablesRequest, ListTablesResponse, PutItemRequest,
    PutItemResponse, QueryRequest, QueryResponse, ReadSequenceRequest, ReadSequenceResponse,
    ReadSequenceValidationCapabilities, ScanRequest, ScanResponse, StorageEnum, StorageResult,
    StoredTableInfo, StreamItemId, TableName, TimestampMillis, TransactGetItemsRequest,
    TransactGetItemsResponse, TransactWriteItemsRequest, TransactWriteItemsResponse,
    UpdateItemRequest, UpdateItemResponse, UpdateTableRequest, UpdateTableResponse,
    UpdateTimeToLiveRequest, UpdateTimeToLiveResponse, context::WrappedError as _,
};

use super::{
    DYNAMO_WRITE_RETRY, DeleteItemShape, DynamoBatchWriteShape, DynamoOperations,
    DynamoRangeReadAttempt, DynamoRangeReadShape, GsiReadinessRecord, PreparedWriteAttempt,
    PutItemShape, TableMetadataCache, UpdateItemShape, create_table_in_attempt,
    delete_table_in_attempt, describe_table_in_attempt, describe_time_to_live_in_attempt,
    dynamo_batch_write::DUPLICATE_BATCH_WRITE_KEY_MESSAGE, list_tables_in_attempt,
    read_sequence_in_attempt, resolve_table_in_attempt, update_time_to_live_in_attempt,
};
use crate::{
    dynamo_bound_write::{BoundItemWrite, DynamoItemWriteShape},
    dynamo_read_execution::{DynamoPointReadShape, ValidatedDynamoReadRequest},
    dynamo_table_stream::{
        get_table_stream_records_in_attempt, latest_table_stream_pointer_in_attempt,
    },
    dynamo_write_preparation::prepare_write_table,
    list_streams_in_attempt,
    maintenance::GsiLifecycleRepository,
};

/// A family store lends one native attempt to the shared operation executor.
/// Clones share the initialized connection, metadata cache and clock owners.
pub trait DynamoPort: Clock + Send + Sync + Clone + 'static {
    type PreparedWrites: Default + Send + 'static;
    fn prepare_put_items(
        &self,
        items: &[&crate::BoundPutItem],
    ) -> StorageResult<Self::PreparedWrites>;
    type Attempt<'native>: crate::DynamoAttempt
        + crate::PreparedWriteAttempt<PreparedWrites = Self::PreparedWrites>
    where Self: 'native;

    fn metadata_cache(&self) -> Arc<TableMetadataCache>;

    /// The selected store clock remains available inside an owned native
    /// attempt.
    fn clock(&self) -> Arc<dyn Clock>;

    fn with_dynamo_attempt<R, F>(
        &self,
        mode: AttemptMode,
        operation: F,
    ) -> impl Future<Output = StorageResult<R>> + Send
    where
        R: Send + 'static,
        F: for<'native, 'borrow> FnOnce(
                &'borrow mut Self::Attempt<'native>,
            ) -> BoxFuture<'borrow, StorageResult<R>>
            + Send
            + 'static;

    /// Return native commit completion without changing the Dynamo transaction
    /// contract. Engines without commit stamps and acknowledged no-write
    /// commits return None.
    fn with_committed_dynamo_attempt<R, F>(
        &self,
        operation: F,
    ) -> impl Future<Output = StorageResult<(R, Option<[u8; 10]>)>> + Send
    where
        R: Send + 'static,
        F: for<'native, 'borrow> FnOnce(
                &'borrow mut Self::Attempt<'native>,
            ) -> BoxFuture<'borrow, StorageResult<R>>
            + Send
            + 'static,
    {
        async move {
            self.with_dynamo_attempt(AttemptMode::ReadWrite, operation)
                .await
                .map(|value| (value, None))
        }
    }

    fn with_dynamo_schema_attempt<R, F>(
        &self,
        operation: F,
    ) -> impl Future<Output = StorageResult<R>> + Send
    where
        R: Send + 'static,
        F: for<'native, 'borrow> FnOnce(
                &'borrow mut Self::Attempt<'native>,
            ) -> BoxFuture<'borrow, StorageResult<R>>
            + Send
            + 'static,
    {
        self.with_dynamo_attempt(AttemptMode::ReadWrite, operation)
    }

    /// Lend the concrete transaction to the shared table-update decision.
    fn update_table_once(
        &self,
        request: UpdateTableRequest,
        captured_at: TimestampMillis,
    ) -> impl Future<Output = StorageResult<UpdateTableResponse>> + Send;
}

#[async_trait]
impl<P: DynamoPort> GsiLifecycleRepository for P {
    async fn read_gsi_readiness_records(
        &self,
        table_name: &TableName,
        index_names: &[IndexName],
    ) -> StorageResult<Vec<GsiReadinessRecord>> {
        let name = table_name.clone();
        let selected_index = (index_names.len() == 1).then(|| index_names[0].clone());
        let cache = self.metadata_cache();
        let publication_cache = Arc::clone(&cache);
        let (records, publication) = self
            .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
                Box::pin(async move {
                    let (table, publication) =
                        resolve_table_in_attempt(attempt, &cache, &name).await?;
                    let records = attempt
                        .read_gsi_readiness(&table, selected_index.as_ref())
                        .await?
                        .into_records();
                    Ok((records, publication))
                })
            })
            .await?;
        publication_cache.publish(publication);
        Ok(records)
    }
}

async fn write_item<P: DynamoPort, S: DynamoItemWriteShape>(
    port: &P,
    shape: S,
) -> StorageResult<<S::Bound as BoundItemWrite>::Response> {
    let created_at = port.now();
    let cache = port.metadata_cache();
    let outcome = retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let shape = shape.clone();
        let cache = Arc::clone(&cache);
        async move {
            let prepared = prepare_write_table(port, &cache, shape.table_name()).await?;
            let ttl = if S::CAUSE == crate::ItemWriteCause::Put {
                crate::dynamo_write_preparation::prepared_ttl_policy(&cache, &prepared.table)?
            } else {
                None
            };
            let bound = shape.bind(Arc::clone(&prepared.table), ttl.as_ref());
            let writes = match &bound {
                Ok(bound) => port.prepare_put_items(&bound.put_items()),
                Err(_) => Ok(P::PreparedWrites::default()),
            };
            port.with_dynamo_attempt(AttemptMode::ReadWriteItems, move |attempt| {
                Box::pin(async move {
                    prepared.confirm(attempt, &cache).await?;
                    attempt.install_prepared_writes(writes);
                    bound?.execute(attempt, created_at).await
                })
            })
            .await
        }
    })
    .await?;
    S::Bound::finish(outcome)
}

async fn read_item<P: DynamoPort, S: DynamoPointReadShape>(
    port: &P,
    shape: S,
) -> StorageResult<S::Response> {
    let cache = port.metadata_cache();
    let publication_cache = Arc::clone(&cache);
    let (response, publications) = port
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move { shape.execute(attempt, &cache).await })
        })
        .await?;
    publication_cache.publish(publications);
    Ok(response)
}

async fn read_range<P: DynamoPort, S: DynamoRangeReadShape>(
    port: &P,
    shape: S,
) -> StorageResult<S::Response> {
    let request =
        crate::dynamo_range_read::ValidatedDynamoRangeRequest::try_from(shape.into_request())?;
    let cache = port.metadata_cache();
    let publication_cache = Arc::clone(&cache);
    let (response, publication) = port
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                crate::dynamo_range_read::range_read_output_in_attempt(
                    attempt, &cache, request, false,
                )
                .await
            })
        })
        .await?;
    publication_cache.publish(publication);
    Ok(S::finish(response))
}

#[async_trait]
impl<P: DynamoPort> DynamoOperations for P {
    async fn get_table_info(&self, table_name: TableName) -> StorageResult<Arc<StoredTableInfo>> {
        let cache = self.metadata_cache();
        let publication_cache = Arc::clone(&cache);
        let (table, publication) = self
            .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
                Box::pin(async move {
                    let (table, publication) =
                        resolve_table_in_attempt(attempt, &cache, &table_name).await?;
                    Ok((Arc::clone(&table.info), publication))
                })
            })
            .await?;
        publication_cache.publish(publication);
        Ok(table)
    }

    async fn list_streams(
        &self,
        request: ListStreamsRequest,
    ) -> StorageResult<ListStreamsResponse> {
        self.with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move { list_streams_in_attempt(attempt, &request).await })
        })
        .await
    }

    async fn latest_table_stream_pointer(
        &self,
        table_name: TableName,
    ) -> StorageResult<Option<StreamItemId>> {
        let cache = self.metadata_cache();
        let publication_cache = Arc::clone(&cache);
        let (pointer, publication) = self
            .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
                Box::pin(async move {
                    latest_table_stream_pointer_in_attempt(attempt, &cache, &table_name).await
                })
            })
            .await?;
        publication_cache.publish(publication);
        Ok(pointer)
    }

    async fn get_stream_records(
        &self,
        request: GetStreamRecordsRequest,
    ) -> StorageResult<GetStreamRecordsResponse> {
        let cache = self.metadata_cache();
        let publication_cache = Arc::clone(&cache);
        let (response, publication) = self
            .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
                Box::pin(async move {
                    get_table_stream_records_in_attempt(attempt, &cache, &request).await
                })
            })
            .await?;
        publication_cache.publish(publication);
        Ok(response)
    }

    #[tracing::instrument(name = "dynamo_operation", skip_all, fields(operation = "CreateTable"))]
    async fn create_table(
        &self,
        request: CreateTableRequest,
    ) -> StorageResult<CreateTableResponse> {
        let table = request.stored_table_info_at(self.now())?;
        let name = table.table_name.clone();
        let response = retry_native_conflicts(self, DYNAMO_WRITE_RETRY, || {
            let table = table.clone();
            async move {
                self.with_dynamo_schema_attempt(move |attempt| {
                    Box::pin(
                        async move { create_table_in_attempt(attempt, table).await?.into_result() },
                    )
                })
                .await
            }
        })
        .await?;
        self.metadata_cache().invalidate(&name);
        Ok(response)
    }

    #[tracing::instrument(name = "dynamo_operation", skip_all, fields(operation = "ListTables"))]
    async fn list_tables(&self, request: ListTablesRequest) -> StorageResult<ListTablesResponse> {
        self.with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move { list_tables_in_attempt(attempt, &request).await })
        })
        .await
    }

    #[tracing::instrument(name = "dynamo_operation", skip_all, fields(operation = "DeleteTable"))]
    async fn delete_table(
        &self,
        request: DeleteTableRequest,
    ) -> StorageResult<DeleteTableResponse> {
        let name = request.table_name.clone();
        let captured_at = self.now();
        let cache = self.metadata_cache();
        let response = retry_native_conflicts(self, DYNAMO_WRITE_RETRY, || {
            let request = request.clone();
            let cache = Arc::clone(&cache);
            async move {
                self.with_dynamo_schema_attempt(move |attempt| {
                    Box::pin(async move {
                        delete_table_in_attempt(attempt, &cache, &request, captured_at)
                            .await?
                            .into_result()
                    })
                })
                .await
            }
        })
        .await?;
        cache.invalidate(&name);
        Ok(response)
    }

    #[tracing::instrument(
        name = "dynamo_operation",
        skip_all,
        fields(operation = "DescribeTable")
    )]
    async fn describe_table(
        &self,
        request: DescribeTableRequest,
    ) -> StorageResult<DescribeTableResponse> {
        let cache = self.metadata_cache();
        let publication_cache = Arc::clone(&cache);
        let (response, publication) = self
            .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
                Box::pin(async move { describe_table_in_attempt(attempt, &cache, &request).await })
            })
            .await?;
        publication_cache.publish(publication);
        Ok(response)
    }

    #[tracing::instrument(name = "dynamo_operation", skip_all, fields(operation = "UpdateTable"))]
    async fn update_table(
        &self,
        request: UpdateTableRequest,
    ) -> StorageResult<UpdateTableResponse> {
        let name = request.table_name.clone();
        let cache = self.metadata_cache();
        let captured_at = self.now();
        let response = retry_native_conflicts(self, DYNAMO_WRITE_RETRY, || {
            let request = request.clone();
            async move { self.update_table_once(request, captured_at).await }
        })
        .await?;
        cache.invalidate(&name);
        Ok(response)
    }

    #[tracing::instrument(name = "dynamo_operation", skip_all, fields(operation = "PutItem"))]
    async fn put_item(&self, request: PutItemRequest) -> StorageResult<PutItemResponse> {
        write_item(self, PutItemShape::try_from_request(request)?).await
    }

    #[tracing::instrument(name = "dynamo_operation", skip_all, fields(operation = "GetItem"))]
    async fn get_item(&self, request: GetItemRequest) -> StorageResult<GetItemResponse> {
        read_item(self, ValidatedDynamoReadRequest::try_new(request)?).await
    }

    #[tracing::instrument(name = "dynamo_operation", skip_all, fields(operation = "UpdateItem"))]
    async fn update_item(&self, request: UpdateItemRequest) -> StorageResult<UpdateItemResponse> {
        write_item(self, UpdateItemShape::try_from_request(request)?).await
    }

    #[tracing::instrument(name = "dynamo_operation", skip_all, fields(operation = "DeleteItem"))]
    async fn delete_item(&self, request: DeleteItemRequest) -> StorageResult<DeleteItemResponse> {
        write_item(self, DeleteItemShape::try_from_request(request)?).await
    }

    #[tracing::instrument(name = "dynamo_operation", skip_all, fields(operation = "Query"))]
    async fn query(&self, request: QueryRequest) -> StorageResult<QueryResponse> {
        read_range(self, request).await
    }

    #[tracing::instrument(name = "dynamo_operation", skip_all, fields(operation = "QueryPage"))]
    async fn query_page(
        &self,
        request: storage_types::QueryTableRequest,
    ) -> StorageResult<crate::RangeReadPage<storage_types::WireItem>> {
        read_range(self, request).await
    }

    async fn query_source_page(
        &self,
        request: storage_types::QueryTableRequest,
    ) -> StorageResult<crate::RangeReadPage<crate::RangeReadItem>> {
        read_range(self, crate::dynamo_range_read::SourceQueryRequest(request)).await
    }

    #[tracing::instrument(name = "dynamo_operation", skip_all, fields(operation = "Scan"))]
    async fn scan(&self, request: ScanRequest) -> StorageResult<ScanResponse> {
        read_range(self, request).await
    }

    async fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetItemResponse> {
        read_item(self, ValidatedDynamoReadRequest::try_new(request)?).await
    }

    #[tracing::instrument(
        name = "dynamo_operation",
        skip_all,
        fields(operation = "BatchWriteItem")
    )]
    async fn batch_write_item(
        &self,
        request: BatchWriteItemRequest,
    ) -> StorageResult<BatchWriteItemResponse> {
        let shape = DynamoBatchWriteShape::try_from_request(request)?;
        let cache = self.metadata_cache();
        let captured_at = self.now();
        let result = retry_native_conflicts(self, DYNAMO_WRITE_RETRY, || {
            let shape = shape.clone();
            let cache = Arc::clone(&cache);
            async move {
                let tables = crate::dynamo_write_preparation::PreparedWriteTables::load(
                    self,
                    &cache,
                    shape.table_names(),
                )
                .await?;
                let bound = shape.bind(tables.ensure_present().map(|()| {
                    |name: &TableName| {
                        let table = tables.table(name)?;
                        let ttl =
                            crate::dynamo_write_preparation::prepared_ttl_policy(&cache, &table)?;
                        Ok(crate::WriteTableBinding { table, ttl })
                    }
                }));
                let writes = match &bound {
                    Ok(bound) => self.prepare_put_items(&bound.put_items()),
                    Err(_) => Ok(P::PreparedWrites::default()),
                };
                self.with_dynamo_attempt(AttemptMode::ReadWriteItems, move |attempt| {
                    Box::pin(async move {
                        tables.confirm(attempt, &cache).await?;
                        attempt.install_prepared_writes(writes);
                        bound?.execute(attempt, captured_at).await
                    })
                })
                .await
            }
        })
        .await;
        let response = match result {
            Ok(result) => result,
            Err(error) => {
                if let Some(response) = shape.unprocessed_after_known_abort(&error) {
                    return Ok(response);
                }
                if matches!(error.to_enum(), StorageEnum::Validation { message, .. }
                    if message == DUPLICATE_BATCH_WRITE_KEY_MESSAGE)
                {
                    tracing::info!(
                        operation = "BatchWriteItem",
                        error_code = "ValidationException",
                        reason = "duplicate_item_key",
                        "batch write request rejected"
                    );
                }
                return Err(error);
            }
        };
        Ok(response)
    }

    #[tracing::instrument(
        name = "dynamo_operation",
        skip_all,
        fields(operation = "TransactGetItems")
    )]
    async fn transact_get_items(
        &self,
        request: TransactGetItemsRequest,
    ) -> StorageResult<TransactGetItemsResponse> {
        read_item(self, crate::TransactGetPlan::try_from(request)?).await
    }

    #[tracing::instrument(
        name = "dynamo_operation",
        skip_all,
        fields(operation = "TransactWriteItems")
    )]
    async fn transact_write_items(
        &self,
        request: TransactWriteItemsRequest,
    ) -> StorageResult<TransactWriteItemsResponse> {
        crate::dynamo_transact_write_execution::write_transaction(self, request, None).await
    }

    async fn transact_write_job_effect(
        &self,
        request: TransactWriteItemsRequest,
        authority: scheduled_bg_jobs::JobEffectAuthority,
    ) -> StorageResult<TransactWriteItemsResponse> {
        crate::dynamo_transact_write_execution::write_transaction(
            self,
            request,
            Some(crate::job_write_authority::JobWriteAuthority::Effect(
                authority,
            )),
        )
        .await
    }

    async fn find_job_effect_receipt(
        &self,
        token: &str,
        authority: scheduled_bg_jobs::JobEffectAuthority,
    ) -> StorageResult<Option<TransactWriteItemsResponse>> {
        storage_types::validate_client_request_token(Some(token))
            .map_err(storage_types::StorageError::raw_validation)?;
        let token = token.to_owned();
        let authority = crate::job_write_authority::JobWriteAuthority::Effect(authority);
        // The receipt port uses protected native reads, including RocksDB.
        // This attempt stages no destination mutation.
        self.with_dynamo_attempt(AttemptMode::ReadWriteItems, move |attempt| {
            Box::pin(async move {
                crate::transact_idempotency::find_job_effect_response(attempt, &token, &authority)
                    .await
            })
        })
        .await
    }

    async fn transact_write_job_projection(
        &self,
        request: crate::JobTerminalProjection,
        authority: scheduled_bg_jobs::JobTerminalAuthority,
    ) -> StorageResult<TransactWriteItemsResponse> {
        crate::dynamo_transact_write_execution::write_transaction(
            self,
            request.into_request(),
            Some(crate::job_write_authority::JobWriteAuthority::Terminal(
                authority,
            )),
        )
        .await
    }

    #[tracing::instrument(
        name = "dynamo_operation",
        skip_all,
        fields(operation = "ReadSequence")
    )]
    async fn read_sequence(
        &self,
        request: ReadSequenceRequest,
    ) -> StorageResult<ReadSequenceResponse> {
        let cache = self.metadata_cache();
        let publication_cache = Arc::clone(&cache);
        let (response, publications) = self
            .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
                Box::pin(async move {
                    // Every active GSI projection is staged with its primary
                    // item on the new path, in this same native transaction
                    // model.
                    read_sequence_in_attempt(
                        attempt,
                        &cache,
                        request,
                        ReadSequenceValidationCapabilities {
                            immediate_gsi_consistency: true,
                            ..ReadSequenceValidationCapabilities::default()
                        },
                    )
                    .await
                })
            })
            .await?;
        publication_cache.publish(publications);
        Ok(response)
    }

    #[tracing::instrument(
        name = "dynamo_operation",
        skip_all,
        fields(operation = "UpdateTimeToLive")
    )]
    async fn update_time_to_live(
        &self,
        request: UpdateTimeToLiveRequest,
    ) -> StorageResult<UpdateTimeToLiveResponse> {
        let name = request.table_name.clone();
        let now = self.now();
        let cache = self.metadata_cache();
        let response = retry_native_conflicts(self, DYNAMO_WRITE_RETRY, || {
            let request = request.clone();
            let cache = Arc::clone(&cache);
            async move {
                self.with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
                    Box::pin(async move {
                        update_time_to_live_in_attempt(attempt, &cache, &request, now)
                            .await?
                            .into_result()
                    })
                })
                .await
            }
        })
        .await?;
        cache.invalidate(&name);
        Ok(response)
    }

    #[tracing::instrument(
        name = "dynamo_operation",
        skip_all,
        fields(operation = "DescribeTimeToLive")
    )]
    async fn describe_time_to_live(
        &self,
        request: DescribeTimeToLiveRequest,
    ) -> StorageResult<DescribeTimeToLiveResponse> {
        let cache = self.metadata_cache();
        let publication_cache = Arc::clone(&cache);
        let (response, publication) = self
            .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
                Box::pin(async move {
                    describe_time_to_live_in_attempt(attempt, &cache, &request).await
                })
            })
            .await?;
        publication_cache.publish(publication);
        Ok(response)
    }
}
