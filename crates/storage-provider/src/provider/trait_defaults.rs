use std::collections::HashMap;

use storage_types::{
    AttributeValue, BatchGetExecutionRequest, BatchGetExecutionResult, BatchProofSelection,
    BatchWriteItemEncodeRequest, BatchWriteItemRequest, BatchWriteItemResponse,
    DurablePointReadProof, DurablePointReadRequest, GuardedDeleteItemRequest,
    GuardedPutItemRequest, GuardedTransactWriteItemsRequest, GuardedUpdateItemRequest,
    KeyAttributes, KeySchemaElement, PutItemEncodeRequest, PutItemRequest, PutItemResponse,
    ReadSequenceConsistency, SplitDynamoItem, StorageError, StorageResult, TableName,
    TransactWriteItemsEncodeRequest, UpdateItemResponse, WriteRetryPolicy,
};

use super::{
    read_context_trait::StorageProviderReadContext, storage_provider_trait::StorageProvider,
};
use crate::{
    AtomicItemReadModifyWriteRequest, BatchWriteContexts, OrderedOperationContexts, PhysicalWrite,
    ReadSequenceExecution, ReadSequenceExecutionBudget, ReadSequenceUnsupportedReason,
    TtlWriteContext,
};

pub(super) fn take_retryable_read_failure() -> bool {
    false
}

pub(super) fn supports_read_sequence_mapped_range() -> bool {
    false
}

pub(super) fn take_admission_pressure_signal() -> bool {
    false
}

pub(super) fn purge_stream_history_for_test() -> StorageResult<()> {
    Ok(())
}

pub(super) fn execute_read_sequence_plan() -> ReadSequenceExecution {
    ReadSequenceExecution::unsupported()
}

pub(super) async fn execute_read_sequence_plan_with_budget<P: StorageProvider + ?Sized>(
    provider: &P,
    plan: &storage_types::ReadSequencePlan,
    consistency: ReadSequenceConsistency,
    continuation: Option<&str>,
    budget: ReadSequenceExecutionBudget,
) -> StorageResult<ReadSequenceExecution> {
    if budget.is_unbounded() {
        provider
            .execute_read_sequence_plan(plan, consistency, continuation)
            .await
    } else {
        Ok(ReadSequenceExecution::Unsupported(
            ReadSequenceUnsupportedReason::ParameterLimit,
        ))
    }
}

pub(super) fn execute_read_sequence_independent_roots() -> ReadSequenceExecution {
    ReadSequenceExecution::Unsupported(ReadSequenceUnsupportedReason::OperationShape)
}

pub(super) fn supports_guarded_writes() -> bool {
    false
}

pub(super) fn supports_guarded_transaction_writes() -> bool {
    false
}

pub(super) fn supports_change_index() -> bool {
    false
}

pub(super) async fn unsupported_begin_read_sequence_read_context()
-> StorageResult<Box<dyn StorageProviderReadContext>> {
    Err(StorageError::unsupported(
        "read-sequence provider read contexts are not supported by this backend",
    ))
}

pub(super) async fn atomic_item_read_modify_write(
    _request: AtomicItemReadModifyWriteRequest,
) -> StorageResult<Vec<u8>> {
    Err(StorageError::unsupported(
        "atomic item read-modify-write is not supported by this backend",
    ))
}

pub(super) async fn put_item_request_with_retry<P: StorageProvider + ?Sized>(
    provider: &P,
    write: PhysicalWrite<PutItemRequest, TtlWriteContext>,
    policy: WriteRetryPolicy,
) -> StorageResult<PutItemResponse> {
    if policy.max_attempts() <= 1 {
        return provider.put_item_request(write).await;
    }
    for _ in 1..policy.max_attempts() {
        match provider.put_item_request(write.clone()).await {
            Ok(response) => return Ok(response),
            Err(error) if error.is_retryable_write() => {
                tokio::time::sleep(policy.delay()).await;
            }
            Err(error) => return Err(error),
        }
    }
    provider.put_item_request(write).await
}

pub(super) async fn put_item_encode<P: StorageProvider + ?Sized>(
    provider: &P,
    write: PhysicalWrite<PutItemEncodeRequest, TtlWriteContext>,
) -> StorageResult<PutItemResponse> {
    let mapped = write.try_map_request(PutItemRequest::try_from)?;
    provider.put_item_request(mapped).await
}

pub(super) async fn put_item_encode_with_retry<P: StorageProvider + ?Sized>(
    provider: &P,
    write: PhysicalWrite<PutItemEncodeRequest, TtlWriteContext>,
    policy: WriteRetryPolicy,
) -> StorageResult<PutItemResponse> {
    if policy.max_attempts() <= 1 {
        return provider.put_item_encode(write).await;
    }
    for _ in 1..policy.max_attempts() {
        match provider
            .put_item_encode_with_retry(write.clone(), WriteRetryPolicy::no_retry())
            .await
        {
            Ok(response) => return Ok(response),
            Err(error) if error.is_retryable_write() => {
                tokio::time::sleep(policy.delay()).await;
            }
            Err(error) => return Err(error),
        }
    }
    provider
        .put_item_encode_with_retry(write, WriteRetryPolicy::no_retry())
        .await
}

pub(super) async fn get_item_with_durable_proof(
    _request: DurablePointReadRequest,
) -> StorageResult<DurablePointReadProof> {
    Err(StorageError::unsupported(
        "durable point-read proofs are not supported by this backend",
    ))
}

/// Default shared execution path. Ordinary BatchGet remains available on all
/// providers; proof selection requires a provider-owned one-snapshot path.
pub(super) async fn execute_batch_get<P: StorageProvider + ?Sized>(
    provider: &P,
    request: BatchGetExecutionRequest,
) -> StorageResult<BatchGetExecutionResult> {
    if request.proof_selection != BatchProofSelection::None {
        return Err(StorageError::unsupported(
            "durable BatchGet proofs are not supported by this storage provider",
        ));
    }
    Ok(BatchGetExecutionResult::without_proofs(
        provider.batch_get_item(request.request).await?,
    ))
}

pub(super) async fn guarded_put_item(
    _write: PhysicalWrite<GuardedPutItemRequest, TtlWriteContext>,
) -> StorageResult<PutItemResponse> {
    Err(StorageError::unsupported(
        "guarded put is not supported by this backend",
    ))
}

pub(super) async fn guarded_delete_item(
    _write: PhysicalWrite<GuardedDeleteItemRequest, TtlWriteContext>,
) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
    Err(StorageError::unsupported(
        "guarded delete is not supported by this backend",
    ))
}

pub(super) async fn guarded_update_item(
    _write: PhysicalWrite<GuardedUpdateItemRequest, TtlWriteContext>,
) -> StorageResult<UpdateItemResponse> {
    Err(StorageError::unsupported(
        "guarded update is not supported by this backend",
    ))
}

pub(super) async fn batch_write_item_encode<P: StorageProvider + ?Sized>(
    provider: &P,
    request: BatchWriteItemEncodeRequest,
    contexts: BatchWriteContexts,
    should_write_to_stream: bool,
) -> StorageResult<BatchWriteItemResponse> {
    let mapped = BatchWriteItemRequest::try_from(request)?;
    provider
        .batch_write_item(mapped, contexts, should_write_to_stream)
        .await
}

pub(super) async fn guarded_transact_write_items(
    _request: GuardedTransactWriteItemsRequest,
    _contexts: OrderedOperationContexts,
) -> StorageResult<storage_types::TransactWriteItemsResponse> {
    Err(StorageError::unsupported(
        "guarded transaction writes are not implemented by this backend",
    ))
}

pub(super) async fn transact_write_items_encode<P: StorageProvider + ?Sized>(
    provider: &P,
    request: TransactWriteItemsEncodeRequest,
    contexts: OrderedOperationContexts,
) -> StorageResult<storage_types::TransactWriteItemsResponse> {
    let mapped = storage_types::TransactWriteItemsRequest::try_from(request)?;
    provider.transact_write_items(mapped, contexts).await
}

pub(super) async fn transact_write_items_encode_with_retry<P: StorageProvider + ?Sized>(
    provider: &P,
    request: TransactWriteItemsEncodeRequest,
    contexts: OrderedOperationContexts,
    policy: WriteRetryPolicy,
) -> StorageResult<storage_types::TransactWriteItemsResponse> {
    if policy.max_attempts() <= 1 {
        return provider
            .transact_write_items_encode(request, contexts)
            .await;
    }
    for _ in 1..policy.max_attempts() {
        match provider
            .transact_write_items_encode(request.clone(), contexts.clone())
            .await
        {
            Ok(response) => return Ok(response),
            Err(error) if error.is_retryable_write() => {
                tokio::time::sleep(policy.delay()).await;
            }
            Err(error) => return Err(error),
        }
    }
    provider
        .transact_write_items_encode(request, contexts)
        .await
}

pub(super) async fn update_time_to_live(
    _request: storage_types::UpdateTimeToLiveRequest,
) -> StorageResult<storage_types::UpdateTimeToLiveResponse> {
    Err(StorageError::internal(
        "Time to live configuration is not supported by this backend",
    ))
}

pub(super) async fn describe_time_to_live(
    _table_name: &TableName,
) -> StorageResult<storage_types::DescribeTimeToLiveResponse> {
    Err(StorageError::internal(
        "Time to live configuration is not supported by this backend",
    ))
}

pub(super) async fn split_item_into_key_and_attributes<P: StorageProvider + ?Sized>(
    provider: &P,
    item: HashMap<String, AttributeValue>,
    table_name: &TableName,
) -> StorageResult<SplitDynamoItem> {
    let table_info = provider.get_table_info(table_name).await?;
    super::table_item_split::split_item_into_key_and_attributes_sync(item, &table_info)
}

pub(super) fn get_key_attributes(
    item: &HashMap<String, AttributeValue>,
    key_schema: &[KeySchemaElement],
) -> StorageResult<KeyAttributes> {
    super::table_item_split::key_attributes_from_item(item, key_schema)
}
