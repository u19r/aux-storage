//! Table appends join catalog authority, regional capture and the stream row.

use async_trait::async_trait;
use infrastructure_provider::physical_attempt::PhysicalRecordOperation;
use storage_common::retry::retry_native_conflicts;
use storage_types::{
    AttemptMode, BoundTableInfo, StorageError, StorageResult, StreamItemId, TableHistoryTarget,
};
use stream_provider::{
    StreamAppendAllocation, StreamAppendOperationId, StreamItemIdAllocation, StreamOperations,
    StreamPort, TableAppendCapture, TableAppendReconciliation, TableAppendRequest,
    UserStreamAppend,
};

use crate::{CatalogAttempt, DYNAMO_WRITE_RETRY};

#[async_trait]
pub trait TableAppendOperations: Send + Sync {
    async fn append_table_item(
        &self,
        operation: StreamAppendOperationId,
        target: TableHistoryTarget,
        data: &[u8],
    ) -> StorageResult<StreamItemId>;

    async fn reconcile_table_append(
        &self,
        operation: StreamAppendOperationId,
        target: TableHistoryTarget,
        data: &[u8],
    ) -> StorageResult<TableAppendReconciliation>;
}

/// Native service bundles expose both capabilities as one coherent object.
pub trait StorageStreamOperations: StreamOperations + TableAppendOperations {}
impl<T: StreamOperations + TableAppendOperations> StorageStreamOperations for T {}

#[derive(Clone)]
struct TableAppendInput {
    request: TableAppendRequest,
    append: UserStreamAppend,
    allocation: StreamItemIdAllocation,
}

#[async_trait]
impl<P: StreamPort> TableAppendOperations for P
where for<'native> P::Attempt<'native>: CatalogAttempt
{
    async fn append_table_item(
        &self,
        operation: StreamAppendOperationId,
        target: TableHistoryTarget,
        data: &[u8],
    ) -> StorageResult<StreamItemId> {
        let append = UserStreamAppend::try_new(data.to_vec(), None, self.now(), 0)
            .map_err(|error| StorageError::from(error.into_storage_enum()))?;
        let input = TableAppendInput {
            request: TableAppendRequest::new(operation, target, data),
            append,
            allocation: self.item_id_allocation(),
        };
        let capture = self.table_append_capture();
        let result = retry_native_conflicts(self, DYNAMO_WRITE_RETRY, || {
            let input = input.clone();
            let capture = capture.clone();
            self.with_committed_stream_attempt(move |attempt| {
                Box::pin(async move { execute_table_append(attempt, &capture, input).await })
            })
        })
        .await;
        let (staged, stamp) = result.map_err(|error| qualify_append_error(error, operation))?;
        staged
            .resolve(stamp)
            .map_err(|error| qualify_append_error(error, operation))
    }

    async fn reconcile_table_append(
        &self,
        operation: StreamAppendOperationId,
        target: TableHistoryTarget,
        data: &[u8],
    ) -> StorageResult<TableAppendReconciliation> {
        UserStreamAppend::validate_data(data)
            .map_err(|error| StorageError::from(error.into_storage_enum()))?;
        let request = TableAppendRequest::new(operation, target, data);
        let capture = self.table_append_capture();
        self.with_stream_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                resolve_append_table(attempt, request.target()).await?;
                capture.reconcile(attempt, &request).await
            })
        })
        .await
    }
}

async fn execute_table_append<A, C>(
    attempt: &mut A,
    capture: &C,
    input: TableAppendInput,
) -> StorageResult<StreamAppendAllocation>
where
    A: stream_provider::StreamAttempt + CatalogAttempt,
    C: TableAppendCapture<A>,
{
    let mut operation = PhysicalRecordOperation::new(attempt);
    let attempt = operation.as_mut();
    let result = async {
        let table = resolve_append_table(attempt, input.request.target()).await?;
        if let TableAppendReconciliation::Durable(id) =
            capture.reconcile(attempt, &input.request).await?
        {
            return Ok(StreamAppendAllocation::AttemptAllocated(id));
        }
        let captured = capture
            .capture(attempt, &input.request, &input.append, &table)
            .await?;
        let staged = stream_provider::append_table_row_in_attempt(
            attempt,
            &table,
            input.append,
            input.allocation,
        )
        .await?;
        if let Some(captured) = captured {
            capture.store_response(attempt, captured, staged).await?;
        }
        Ok(staged)
    }
    .await;
    operation.complete(result)
}

async fn resolve_append_table<A: CatalogAttempt>(
    attempt: &mut A,
    target: &TableHistoryTarget,
) -> StorageResult<std::sync::Arc<BoundTableInfo>> {
    let stamp = attempt
        .read_catalog_stamp(&target.table_name)
        .await?
        .filter(|stamp| stamp.history_id == target.history_id)
        .ok_or_else(|| StorageError::table_not_found(&target.table_name))?;
    attempt.read_table_at_stamp(&target.table_name, stamp).await
}

fn qualify_append_error(error: StorageError, operation: StreamAppendOperationId) -> StorageError {
    if error.is_commit_unknown() {
        return StorageError::sync_submission_ambiguous(
            Some(operation.token()),
            "table append commit outcome is unresolved",
        );
    }
    error
}
