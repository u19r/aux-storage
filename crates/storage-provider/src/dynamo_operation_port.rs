//! Shared public Dynamo operation lifecycle over one family-owned native
//! attempt.

use std::{future::Future, sync::Arc};

use async_trait::async_trait;
use futures::future::BoxFuture;
use storage_types::{
    AttemptPlan, AttemptRequest, BatchGetItemRequest, BatchGetItemResponse, BatchWriteItemRequest,
    BatchWriteItemResponse, CapacityUnitField, Clock, ConsumedCapacity, CreateTableRequest,
    CreateTableResponse, DeleteItemRequest, DeleteItemResponse, DeleteTableRequest,
    DeleteTableResponse, DescribeTableRequest, DescribeTableResponse, DescribeTimeToLiveRequest,
    DescribeTimeToLiveResponse, GetItemRequest, GetItemResponse, GetStreamRecordsRequest,
    GetStreamRecordsResponse, IndexName, ListStreamsRequest, ListStreamsResponse,
    ListTablesRequest, ListTablesResponse, PutItemRequest, PutItemResponse, QueryRequest,
    QueryResponse, ReadSequenceRequest, ReadSequenceResponse, ReadSequenceValidationCapabilities,
    ScanRequest, ScanResponse, StorageEnum, StorageError, StorageResult, StoredTableInfo,
    TableName, TimestampMillis, TransactGetItemsRequest, TransactGetItemsResponse,
    TransactWriteItemsRequest, TransactWriteItemsResponse, UpdateItemRequest, UpdateItemResponse,
    UpdateTableRequest, UpdateTableResponse, UpdateTimeToLiveRequest, UpdateTimeToLiveResponse,
    context::WrappedError as _,
};

use super::{
    DeleteItemShape, DynamoBatchWriteShape, DynamoOperations, DynamoRangeReadShape,
    GsiReadinessRecord, ItemReadAttempt, PutItemShape, TableMetadataCache, UpdateItemShape,
    create_table_in_attempt, delete_table_in_attempt, describe_table_in_attempt,
    describe_time_to_live_in_attempt, dynamo_batch_write::DUPLICATE_BATCH_WRITE_KEY_MESSAGE,
    list_tables_in_attempt, read_sequence_in_attempt, update_time_to_live_in_attempt,
};
use crate::{
    dynamo_bound_write::{BoundItemWrite, DynamoItemWriteShape},
    dynamo_read_execution::ValidatedDynamoReadRequest,
    dynamo_table_stream::{
        get_table_stream_records_in_attempt, table_stream_checkpoint_in_attempt,
    },
    list_streams_in_attempt,
};

/// A family store lends one native attempt to the shared operation executor.
/// Clones share the initialized connection, metadata cache and clock owners.
pub trait DynamoPort:
    Clock + storage_types::AttemptDeadlineSource + Send + Sync + Clone + 'static
{
    fn ordinary_write_read_version_policy(&self) -> storage_types::ReadVersionPolicy {
        storage_types::ReadVersionPolicy::Fresh
    }

    type PreparedWrites: Default + Send + 'static;
    fn prepare_put_items(
        &self,
        items: &[&crate::BoundPutItem],
    ) -> StorageResult<Self::PreparedWrites>;
    type Attempt<'native>: crate::CreateTableAttempt
        + crate::DeleteTableAttempt
        + crate::ItemReadAttempt
        + crate::ItemWriteAttempt
        + crate::DynamoTransactWriteAttempt
        + crate::ListingAttempt
        + crate::maintenance::StreamRetentionPort
        + crate::ItemWriteAttempt<PreparedWrites = Self::PreparedWrites>
    where Self: 'native;

    fn native_transaction_byte_limit(
        &self,
    ) -> infrastructure_provider::physical_store::NativeTransactionByteLimit;

    fn metadata_cache(&self) -> Arc<TableMetadataCache>;

    /// The selected store clock remains available inside an owned native
    /// attempt.
    fn clock(&self) -> Arc<dyn Clock>;

    /// One native attempt under this store value's admission lane.
    fn with_attempt<R, F>(
        &self,
        request: AttemptRequest,
        step: F,
    ) -> impl Future<Output = StorageResult<R>> + Send
    where
        R: Send + 'static,
        F: for<'native, 'borrow> FnOnce(
                &'borrow mut Self::Attempt<'native>,
            ) -> BoxFuture<'borrow, StorageResult<R>>
            + Send
            + 'static;

    /// Lend the concrete transaction to the shared table-update decision.
    /// A store that overrides this applies its admission lane here.
    fn update_table_once(
        &self,
        attempt_request: AttemptRequest,
        request: UpdateTableRequest,
        captured_at: TimestampMillis,
        bindings: crate::RequestTableBindings,
    ) -> impl Future<Output = StorageResult<UpdateTableResponse>> + Send;

    /// Retry known aborts with a fresh try; return an unknown outcome
    /// unchanged. Never overridden.
    fn run<I, R, S>(
        &self,
        plan: AttemptPlan,
        input: std::sync::Arc<I>,
        step: S,
    ) -> impl Future<Output = StorageResult<R>> + Send
    where
        I: Send + Sync + 'static,
        R: Send + 'static,
        S: for<'native, 'borrow> Fn(
                &'borrow mut Self::Attempt<'native>,
                &'borrow I,
                storage_types::AttemptTry,
            ) -> BoxFuture<'borrow, StorageResult<R>>
            + Copy
            + Send
            + Sync
            + 'static,
    {
        storage_common::retry::retry_tries(
            self,
            self.attempt_deadlines(),
            plan,
            move |attempt_request, attempt_try| {
                let input = std::sync::Arc::clone(&input);
                self.with_attempt(attempt_request, move |attempt| {
                    Box::pin(async move { step(attempt, &input, attempt_try).await })
                })
            },
        )
    }
}

/// Replay only known-aborted reads. Each try lends the operation's catalog
/// session to a clone of `operation` in a fresh native attempt; the answering
/// try's publications are published after `Ok`.
struct ReadOperationInput<F> {
    operation: F,
    session: crate::dynamo_table_bindings::CatalogSession,
}

pub(crate) async fn read_operation<P, R, Publications, F>(
    port: &P,
    plan: AttemptPlan,
    operation: F,
) -> StorageResult<R>
where
    P: DynamoPort,
    R: Send + 'static,
    Publications: IntoIterator<Item = crate::CachePublication> + Send + 'static,
    F: for<'native, 'borrow> FnOnce(
            &'borrow mut P::Attempt<'native>,
            crate::dynamo_table_bindings::CatalogSession,
        )
            -> BoxFuture<'borrow, StorageResult<(R, Publications)>>
        + Clone
        + Send
        + Sync
        + 'static,
{
    let session = crate::dynamo_table_bindings::CatalogSession::new(port.metadata_cache());
    let input = Arc::new(ReadOperationInput {
        operation,
        session: session.clone(),
    });
    let result = DynamoPort::run(port, plan, input, |attempt, input, _| {
        (input.operation.clone())(attempt, input.session.clone())
    })
    .await;
    session.settle(result, std::iter::empty())
}

async fn write_item<P: DynamoPort, S: DynamoItemWriteShape>(
    port: &P,
    shape: S,
) -> StorageResult<<S::Bound as BoundItemWrite>::Response> {
    let session = crate::dynamo_table_bindings::CatalogSession::new(port.metadata_cache());
    let plan = Arc::new(crate::dynamo_bound_write::ItemWritePlan::new(
        shape,
        port.now(),
    ));
    crate::dynamo_write_session::run_write(
        port,
        crate::constants::ITEM_WRITE.start(port.attempt_deadlines()),
        &session,
        plan,
    )
    .await
}

/// A byte-rejected BatchWriteItem group halves; groups never grow again. One
/// member over the native limit fails as TransactWriteItems does, because
/// retrying it can never succeed.
pub(crate) fn split_byte_rejected_group(
    error: storage_types::StorageError,
    members: usize,
) -> StorageResult<usize> {
    if !crate::dynamo_transact_write_execution::is_transaction_bytes_rejected(&error) {
        return Err(error);
    }
    if members > 1 {
        Ok(members / 2)
    } else {
        Err(crate::dynamo_transact_write_execution::transaction_size_validation())
    }
}

async fn read_range<P: DynamoPort, S: DynamoRangeReadShape>(
    port: &P,
    shape: S,
) -> StorageResult<S::Response> {
    let request =
        crate::dynamo_range_read::ValidatedDynamoRangeRequest::try_from(shape.into_request())?;
    let output =
        crate::dynamo_write_session::run_read(port, crate::constants::RANGE_READ, request).await?;
    Ok(S::finish(output))
}

#[async_trait]
impl<P: DynamoPort> DynamoOperations for P {
    async fn get_table_info(&self, table_name: TableName) -> StorageResult<Arc<StoredTableInfo>> {
        let state = Arc::new(table_name);
        read_operation(
            self,
            crate::constants::POINT_READ,
            move |attempt, session| {
                Box::pin(async move {
                    let (table, publication) = session.resolve_table(attempt, &state).await?;
                    Ok((Arc::clone(&table.info), publication))
                })
            },
        )
        .await
    }

    async fn list_streams(
        &self,
        request: ListStreamsRequest,
    ) -> StorageResult<ListStreamsResponse> {
        let state = Arc::new(request);
        read_operation(
            self,
            crate::constants::RANGE_READ,
            move |attempt, _session| {
                Box::pin(async move {
                    let response = list_streams_in_attempt(attempt, &state).await?;
                    Ok((response, None::<crate::CachePublication>))
                })
            },
        )
        .await
    }

    async fn table_stream_checkpoint(
        &self,
        target: storage_types::TableHistoryTarget,
    ) -> StorageResult<storage_types::TableStreamCheckpoint> {
        let state = Arc::new(target);
        read_operation(
            self,
            crate::constants::POINT_READ,
            move |attempt, session| {
                Box::pin(async move {
                    table_stream_checkpoint_in_attempt(
                        attempt,
                        session.cache(),
                        session.bindings(),
                        &state,
                    )
                    .await
                })
            },
        )
        .await
    }

    async fn describe_stream(
        &self,
        request: storage_types::DescribeStreamRequest,
    ) -> StorageResult<storage_types::DescribeStreamResponse> {
        crate::dynamodb_streams::read_dynamo_stream(
            self,
            crate::DescribeDynamoStream::try_from(request)?,
        )
        .await
    }
    async fn get_shard_iterator(
        &self,
        request: storage_types::GetShardIteratorRequest,
    ) -> StorageResult<storage_types::GetShardIteratorResponse> {
        crate::dynamodb_streams::read_dynamo_stream(
            self,
            crate::GetDynamoShardIterator::try_from(request)?,
        )
        .await
    }
    async fn get_records(
        &self,
        request: storage_types::GetRecordsRequest,
    ) -> StorageResult<storage_types::GetRecordsResponse> {
        crate::dynamodb_streams::read_dynamo_stream(
            self,
            crate::GetDynamoStreamRecords::try_from(request)?,
        )
        .await
    }

    async fn get_stream_records(
        &self,
        request: GetStreamRecordsRequest,
    ) -> StorageResult<GetStreamRecordsResponse> {
        let now = self.now();
        let state = Arc::new(request);
        read_operation(
            self,
            crate::constants::RANGE_READ,
            move |attempt, session| {
                Box::pin(async move {
                    get_table_stream_records_in_attempt(
                        attempt,
                        session.cache(),
                        session.bindings(),
                        &state,
                        now,
                    )
                    .await
                })
            },
        )
        .await
    }

    #[tracing::instrument(name = "dynamo_operation", skip_all, fields(operation = "CreateTable"))]
    async fn create_table(
        &self,
        request: CreateTableRequest,
    ) -> StorageResult<CreateTableResponse> {
        let table = request.stored_table_info_at(self.now())?;
        let name = table.table_name.clone();
        let response = storage_common::retry::retry_tries(
            self,
            self.attempt_deadlines(),
            crate::constants::SCHEMA_CHANGE,
            |attempt_request, _| {
                let table = table.clone();
                async move {
                    self.with_attempt(
                        attempt_request.scoped(storage_types::AttemptScope::Schema),
                        move |attempt| {
                            Box::pin(async move {
                                create_table_in_attempt(attempt, table).await?.into_result()
                            })
                        },
                    )
                    .await
                }
            },
        )
        .await?;
        self.metadata_cache().invalidate(&name);
        Ok(response)
    }

    #[tracing::instrument(name = "dynamo_operation", skip_all, fields(operation = "ListTables"))]
    async fn list_tables(&self, request: ListTablesRequest) -> StorageResult<ListTablesResponse> {
        let state = Arc::new(request);
        read_operation(
            self,
            crate::constants::RANGE_READ,
            move |attempt, _session| {
                Box::pin(async move {
                    let response = list_tables_in_attempt(attempt, &state).await?;
                    Ok((response, None::<crate::CachePublication>))
                })
            },
        )
        .await
    }

    #[tracing::instrument(name = "dynamo_operation", skip_all, fields(operation = "DeleteTable"))]
    async fn delete_table(
        &self,
        request: DeleteTableRequest,
    ) -> StorageResult<DeleteTableResponse> {
        let name = request.table_name.clone();
        let session = crate::dynamo_table_bindings::CatalogSession::new(self.metadata_cache());
        let response = storage_common::retry::retry_tries(
            self,
            self.attempt_deadlines(),
            crate::constants::SCHEMA_CHANGE,
            |attempt_request, _| {
                let request = request.clone();
                let session = session.clone();
                async move {
                    self.with_attempt(
                        attempt_request.scoped(storage_types::AttemptScope::Schema),
                        move |attempt| {
                            Box::pin(async move {
                                let (table, _) =
                                    session.resolve_table(attempt, &request.table_name).await?;
                                delete_table_in_attempt(attempt, &table)
                                    .await?
                                    .into_result()
                            })
                        },
                    )
                    .await
                }
            },
        )
        .await?;
        session.cache().invalidate(&name);
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
        let state = Arc::new(request);
        read_operation(
            self,
            crate::constants::POINT_READ,
            move |attempt, session| {
                Box::pin(async move {
                    describe_table_in_attempt(attempt, session.cache(), session.bindings(), &state)
                        .await
                })
            },
        )
        .await
    }

    #[tracing::instrument(name = "dynamo_operation", skip_all, fields(operation = "UpdateTable"))]
    async fn update_table(
        &self,
        request: UpdateTableRequest,
    ) -> StorageResult<UpdateTableResponse> {
        let name = request.table_name.clone();
        let cache = self.metadata_cache();
        let captured_at = self.now();
        let bindings = crate::RequestTableBindings::default();
        let response = storage_common::retry::retry_tries(
            self,
            self.attempt_deadlines(),
            crate::constants::SCHEMA_CHANGE,
            |attempt_request, _| {
                let request = request.clone();
                let bindings = bindings.clone();
                async move {
                    self.update_table_once(attempt_request, request, captured_at, bindings)
                        .await
                }
            },
        )
        .await?;
        cache.invalidate(&name);
        Ok(response)
    }

    #[tracing::instrument(name = "dynamo_operation", skip_all, fields(operation = "PutItem"))]
    async fn put_item(&self, request: PutItemRequest) -> StorageResult<PutItemResponse> {
        let capacity =
            ConsumedCapacity::zero(request.return_consumed_capacity, &request.table_name, None);
        let table_name = request.table_name.clone();
        let shape = match PutItemShape::try_from_request(request) {
            Ok(shape) => shape,
            // AWS looks the table up before sizing a PutItem item, so a missing
            // table masks the size refusal. BatchWriteItem sizes first.
            Err(error) if error.is_item_too_large() => {
                self.get_table_info(table_name)
                    .await
                    .map_err(StorageError::into_data_plane_table_not_found)?;
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        let mut response = write_item(self, shape)
            .await
            .map_err(StorageError::into_data_plane_table_not_found)?;
        response.consumed_capacity = capacity;
        Ok(response)
    }

    #[tracing::instrument(name = "dynamo_operation", skip_all, fields(operation = "GetItem"))]
    async fn get_item(&self, request: GetItemRequest) -> StorageResult<GetItemResponse> {
        let capacity =
            ConsumedCapacity::zero(request.return_consumed_capacity, &request.table_name, None);
        let mut response = crate::dynamo_write_session::run_read(
            self,
            crate::constants::POINT_READ,
            ValidatedDynamoReadRequest::try_new(request)?,
        )
        .await
        .map_err(StorageError::into_data_plane_table_not_found)?;
        response.consumed_capacity = capacity;
        Ok(response)
    }

    #[tracing::instrument(name = "dynamo_operation", skip_all, fields(operation = "UpdateItem"))]
    async fn update_item(&self, request: UpdateItemRequest) -> StorageResult<UpdateItemResponse> {
        let capacity =
            ConsumedCapacity::zero(request.return_consumed_capacity, &request.table_name, None);
        let mut response = write_item(self, UpdateItemShape::try_from_request(request)?)
            .await
            .map_err(StorageError::into_data_plane_table_not_found)?;
        response.consumed_capacity = capacity;
        Ok(response)
    }

    #[tracing::instrument(name = "dynamo_operation", skip_all, fields(operation = "DeleteItem"))]
    async fn delete_item(&self, request: DeleteItemRequest) -> StorageResult<DeleteItemResponse> {
        let capacity =
            ConsumedCapacity::zero(request.return_consumed_capacity, &request.table_name, None);
        let mut response = write_item(self, DeleteItemShape::try_from_request(request)?)
            .await
            .map_err(StorageError::into_data_plane_table_not_found)?;
        response.consumed_capacity = capacity;
        Ok(response)
    }

    #[tracing::instrument(name = "dynamo_operation", skip_all, fields(operation = "Query"))]
    async fn query(&self, request: QueryRequest) -> StorageResult<QueryResponse> {
        read_range(self, request)
            .await
            .map_err(StorageError::into_data_plane_table_not_found)
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
        read_range(self, request)
            .await
            .map_err(StorageError::into_data_plane_table_not_found)
    }

    async fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetItemResponse> {
        {
            use storage_types::DynamoRequestValidate as _;
            request
                .validate_for_dynamodb()
                .map_err(storage_types::StorageError::raw_validation)?;
            let capacity = ConsumedCapacity::zero_for_tables(
                request.return_consumed_capacity,
                CapacityUnitField::Plain,
                request.request_items.keys(),
            );
            let mut response = crate::dynamo_write_session::run_read(
                self,
                crate::constants::POINT_READ,
                crate::BatchGetPlan::from(request),
            )
            .await
            .map_err(StorageError::into_data_plane_table_not_found)?;
            response.consumed_capacity = capacity;
            Ok(response)
        }
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
        // One operation deadline for every group, started before validation.
        let attempt_request = crate::constants::TRANSACTION_WRITE.start(self.attempt_deadlines());
        let capacity = ConsumedCapacity::zero_for_tables(
            request.return_consumed_capacity,
            CapacityUnitField::Plain,
            request.request_items.keys(),
        );
        let shape = DynamoBatchWriteShape::try_from_request(request)?;
        let session = crate::dynamo_table_bindings::CatalogSession::new(self.metadata_cache());
        let captured_at = self.now();
        let mut start = 0;
        let mut group_members = shape.len();
        while start < shape.len() {
            // Before any partial effects, bind the complete inventory and
            // confirm all its tables inside the first group's attempt.
            let group = if start == 0 {
                shape.clone()
            } else {
                shape.select_members(start..(start + group_members).min(shape.len()))
            };
            let maximum = group_members.min(group.len());
            let plan = Arc::new(crate::dynamo_batch_write::BatchGroupPlan::new(
                group,
                maximum,
                captured_at,
            ));
            let result = crate::dynamo_write_session::run_write(
                self,
                attempt_request,
                &session,
                Arc::clone(&plan),
            )
            .await;
            let end = start + plan.members();
            match result {
                Ok(()) => start = end,
                Err(error) => {
                    // A known-aborted group gets a fresh attempt; committed
                    // prefix members never enter this retry owner again.
                    let error = match split_byte_rejected_group(error, end - start) {
                        Ok(members) => {
                            group_members = members;
                            continue;
                        }
                        Err(error) => error,
                    };
                    if let Some(response) = shape
                        .select_members(start..shape.len())
                        .unprocessed_after_known_abort(&error)
                    {
                        return Ok(BatchWriteItemResponse {
                            consumed_capacity: capacity,
                            ..response
                        });
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
            }
        }
        Ok(BatchWriteItemResponse {
            unprocessed_items: Some(storage_types::VecMap::new()),
            item_collection_metrics: None,
            consumed_capacity: capacity,
        })
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
        crate::dynamo_write_session::run_read(
            self,
            crate::constants::POINT_READ,
            crate::TransactGetPlan::try_from(request)?,
        )
        .await
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
        let capacity = ConsumedCapacity::zero_for_tables(
            request.return_consumed_capacity,
            CapacityUnitField::Write,
            request
                .transact_items
                .iter()
                .filter_map(storage_types::TransactWriteItem::table_name),
        );
        let mut response =
            crate::dynamo_transact_write_execution::write_transaction(self, request, None).await?;
        response.consumed_capacity = capacity;
        Ok(response)
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
        // One point read. The receipt is written once per execution and removed
        // only with it, so a snapshot read is exact.
        DynamoPort::with_attempt(
            self,
            crate::constants::BACKGROUND_SNAPSHOT_READ.start(self.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    crate::transact_idempotency::find_job_effect_response(
                        attempt, &token, &authority,
                    )
                    .await
                })
            },
        )
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
        let request = crate::dynamo_read_sequence::executor::PreparedReadSequence::try_new(
            request,
            ReadSequenceValidationCapabilities {
                immediate_gsi_consistency: true,
                ..ReadSequenceValidationCapabilities::default()
            },
        )?;
        let state = Arc::new(request);
        read_operation(
            self,
            crate::constants::RANGE_READ,
            move |attempt, session| {
                Box::pin(async move {
                    // Every active GSI projection is staged with its
                    // primary item on the new path, in this same native
                    // transaction model.
                    read_sequence_in_attempt(attempt, session.cache(), session.bindings(), &state)
                        .await
                })
            },
        )
        .await
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
        let session = crate::dynamo_table_bindings::CatalogSession::new(self.metadata_cache());
        let input = Arc::new(UpdateTtlInput {
            request,
            now,
            session: session.clone(),
        });
        let response = DynamoPort::run(
            self,
            crate::constants::SCHEMA_CHANGE,
            input,
            |attempt, input, _| Box::pin(update_time_to_live_step(attempt, input)),
        )
        .await?;
        session.cache().invalidate(&name);
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
        let state = Arc::new(request);
        read_operation(
            self,
            crate::constants::POINT_READ,
            move |attempt, session| {
                Box::pin(async move {
                    describe_time_to_live_in_attempt(
                        attempt,
                        session.cache(),
                        session.bindings(),
                        &state,
                    )
                    .await
                })
            },
        )
        .await
    }

    async fn read_gsi_readiness_records(
        &self,
        table_name: &TableName,
        index_names: &[IndexName],
    ) -> StorageResult<Vec<GsiReadinessRecord>> {
        let name = table_name.clone();
        let selected_index = (index_names.len() == 1).then(|| index_names[0].clone());
        let state = Arc::new((name, selected_index));
        read_operation(
            self,
            crate::constants::POINT_READ,
            move |attempt, session| {
                Box::pin(async move {
                    let (name, selected_index) = state.as_ref();
                    let (table, publication) = session.resolve_table(attempt, name).await?;
                    let records = attempt
                        .read_gsi_readiness(&table, selected_index.as_ref())
                        .await?
                        .into_records();
                    Ok((records, publication))
                })
            },
        )
        .await
    }
}

/// The TTL change of one UpdateTimeToLive and the catalog session every try
/// shares.
struct UpdateTtlInput {
    request: UpdateTimeToLiveRequest,
    now: TimestampMillis,
    session: crate::dynamo_table_bindings::CatalogSession,
}

/// One try: the table under its catalog stamp, then the TTL record.
async fn update_time_to_live_step<
    A: crate::CreateTableAttempt
        + crate::DeleteTableAttempt
        + crate::ItemReadAttempt
        + crate::ItemWriteAttempt
        + crate::DynamoTransactWriteAttempt
        + crate::ListingAttempt
        + crate::maintenance::StreamRetentionPort,
>(
    attempt: &mut A,
    input: &UpdateTtlInput,
) -> StorageResult<UpdateTimeToLiveResponse> {
    let (table, _) = input
        .session
        .resolve_table(attempt, &input.request.table_name)
        .await?;
    update_time_to_live_in_attempt(
        attempt,
        &table,
        &input.request.time_to_live_specification,
        input.now,
    )
    .await?
    .into_result()
}
