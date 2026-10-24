//! One ReadSequence node invocation over a caller-owned read context.

use std::future::Future;

use async_trait::async_trait;
use storage_types::{
    BatchGetItemRequest, BatchGetItemResponse, GetItemRequest, GetItemResponse, KeyAttributes,
    QueryRequest, QueryResponse, ReadSequenceConsistency, ReadSequenceInvocationPayload,
    ReadSequenceNodeOperation, StorageEnum, StorageResult, context::WrappedError as _,
};

use crate::CachePublication;

/// The graph executor retains cursors separately from projected Query items so
/// a response budget can trim a page without repeating the physical read.
pub struct DynamoReadSequenceStep {
    pub payload: ReadSequenceInvocationPayload,
    pub item_cursors: Option<Vec<KeyAttributes>>,
    pub publications: Vec<CachePublication>,
}

/// Three physical reads needed by the shared ReadSequence scheduler. Native
/// stores lend one attempt to all calls; remote stores may issue several
/// network requests. Dispatch and partial BatchGet retries stay identical.
#[async_trait]
pub trait ReadSequenceStepReader: Send {
    async fn get(
        &mut self,
        request: GetItemRequest,
    ) -> StorageResult<(GetItemResponse, Option<CachePublication>)>;

    async fn batch_get_once(
        &mut self,
        request: BatchGetItemRequest,
    ) -> StorageResult<(BatchGetItemResponse, Vec<CachePublication>)>;

    async fn query(
        &mut self,
        request: QueryRequest,
    ) -> StorageResult<(
        QueryResponse,
        Option<Vec<KeyAttributes>>,
        Option<CachePublication>,
    )>;

    async fn after_unprocessed_batch_get(&mut self, _retry: usize) {}

    async fn read_step(
        &mut self,
        operation: ReadSequenceNodeOperation,
        consistency: ReadSequenceConsistency,
    ) -> StorageResult<DynamoReadSequenceStep> {
        match operation.with_consistency(consistency) {
            ReadSequenceNodeOperation::Get(request) => {
                let (response, publication) =
                    observe_provider_read("read_sequence.get_item", self.get(*request)).await?;
                Ok(DynamoReadSequenceStep {
                    payload: ReadSequenceInvocationPayload::Get(response),
                    item_cursors: None,
                    publications: publication.into_iter().collect(),
                })
            }
            ReadSequenceNodeOperation::BatchGet(request) => {
                let (response, publications) = complete_batch_get(self, *request).await?;
                Ok(DynamoReadSequenceStep {
                    payload: ReadSequenceInvocationPayload::BatchGet(response),
                    item_cursors: None,
                    publications,
                })
            }
            ReadSequenceNodeOperation::Query(request) => {
                let (response, item_cursors, publication) =
                    observe_provider_read("read_sequence.query_table", self.query(*request))
                        .await?;
                Ok(DynamoReadSequenceStep {
                    payload: ReadSequenceInvocationPayload::Query(response),
                    item_cursors,
                    publications: publication.into_iter().collect(),
                })
            }
        }
    }
}

async fn observe_provider_read<T>(
    operation: &'static str,
    read: impl Future<Output = StorageResult<T>>,
) -> StorageResult<T> {
    let database_call = metrics_facade::begin_database_call(operation);
    let result = read.await;
    drop(database_call);
    if let Err(error) = &result {
        let error_kind = match error.to_enum() {
            StorageEnum::TransactionConflict { .. } => "transaction_conflict",
            StorageEnum::Throttled { .. } => "throttled",
            StorageEnum::InternalServerError { .. } => "internal_server_error",
            _ => "other",
        };
        tracing::warn!(operation, error_kind, "read sequence provider read failed");
    }
    result
}

pub(crate) async fn complete_batch_get<R: ReadSequenceStepReader + ?Sized>(
    reader: &mut R,
    mut request: BatchGetItemRequest,
) -> StorageResult<(BatchGetItemResponse, Vec<CachePublication>)> {
    let mut items = storage_types::VecMap::new();
    let mut publications = Vec::new();
    for retry in 0..4 {
        let (mut response, mut loaded) = observe_provider_read(
            "read_sequence.batch_get_item",
            reader.batch_get_once(request),
        )
        .await?;
        publications.append(&mut loaded);
        for (table, mut returned) in response.responses.take().unwrap_or_default() {
            items
                .get_or_insert_with(table, Vec::new)
                .append(&mut returned);
        }
        let unprocessed = response.unprocessed_keys.take().unwrap_or_default();
        if unprocessed.values().all(|keys| keys.keys.is_empty()) {
            return Ok((
                BatchGetItemResponse {
                    responses: Some(items),
                    unprocessed_keys: None,
                    consumed_capacity: None,
                },
                publications,
            ));
        }
        if retry == 3 {
            let count = unprocessed
                .values()
                .map(|keys| keys.keys.len())
                .sum::<usize>();
            return Err(StorageEnum::Throttled {
                message: format!(
                    "ReadSequence BatchGetItem exhausted retries with {count} unprocessed key(s)"
                ),
            }
            .into());
        }
        reader.after_unprocessed_batch_get(retry).await;
        request = BatchGetItemRequest {
            request_items: unprocessed,
            return_consumed_capacity: None,
        };
    }
    unreachable!("bounded BatchGet retry loop returns")
}
