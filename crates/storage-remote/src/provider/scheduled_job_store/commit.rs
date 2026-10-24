use std::{sync::Arc, time::Instant};

use bytes::Bytes;
use http_request::HttpRequestDispatchStatus;
use scheduled_bg_jobs::store::{
    Check, CommitOperationId, CommitOutcome, Mutation, StoreTransaction,
};
use storage_provider::RawOrderedJobStoreError;
use storage_types::{
    GetItemRequest, GetItemResponse, StorageEnum, StorageError, StorageResult,
    TransactConditionCheckRequest, TransactDeleteRequest, TransactPutRequest, TransactWriteItem,
    TransactWriteItemsRequest, context::WrappedError,
};

use crate::provider::{
    implementation::{RemoteInvocationError, RemoteStorageProvider},
    scheduled_job_store::{constants::*, encoding, provider::job_table},
};

struct Receipt {
    fingerprint: [u8; RECEIPT_DIGEST_BYTES],
    outcome: CommitOutcome,
}

pub(super) enum SendTransactionError {
    Backend(StorageError),
    Reconcile,
    Timeout,
}

pub(super) enum AttemptDecision {
    Outcome(CommitOutcome),
    Retry,
}

struct AttemptContext<'a> {
    transaction: &'a StoreTransaction,
    fingerprint: &'a [u8; RECEIPT_DIGEST_BYTES],
    deadline: Instant,
}

pub(crate) async fn run(
    provider: Arc<RemoteStorageProvider>,
    transaction: &StoreTransaction,
) -> Result<CommitOutcome, RawOrderedJobStoreError> {
    let deadline = Instant::now()
        .checked_add(MAX_TRANSACTION_DURATION)
        .unwrap_or_else(Instant::now);
    let fingerprint = encoding::transaction_fingerprint(transaction);
    for _ in 0..MAX_COMMIT_ATTEMPTS {
        ensure_before_dispatch(deadline)?;
        match run_attempt(Arc::clone(&provider), transaction, &fingerprint, deadline).await? {
            AttemptDecision::Outcome(outcome) => return Ok(outcome),
            AttemptDecision::Retry => {}
        }
    }
    Err(RawOrderedJobStoreError::Backend(
        StorageError::service_unavailable(1),
    ))
}

pub(super) async fn run_attempt(
    provider: Arc<RemoteStorageProvider>,
    transaction: &StoreTransaction,
    fingerprint: &[u8; RECEIPT_DIGEST_BYTES],
    deadline: Instant,
) -> Result<AttemptDecision, RawOrderedJobStoreError> {
    let attempt = AttemptContext {
        transaction,
        fingerprint,
        deadline,
    };
    ensure_before_dispatch(attempt.deadline)?;
    if let Some(decision) = existing_receipt(Arc::clone(&provider), &attempt).await? {
        return Ok(decision);
    }
    if let Some(failed) = first_failed_check(Arc::clone(&provider), &attempt).await? {
        ensure_before_dispatch(attempt.deadline)?;
        return conflict_attempt(provider, &attempt, failed).await;
    }
    ensure_before_dispatch(attempt.deadline)?;
    match persist_committed(Arc::clone(&provider), &attempt).await {
        Ok(()) => Ok(AttemptDecision::Outcome(CommitOutcome::Committed)),
        Err(SendTransactionError::Timeout) => Err(RawOrderedJobStoreError::Timeout),
        Err(SendTransactionError::Backend(error)) if is_operation_reuse_mismatch(&error) => {
            Err(RawOrderedJobStoreError::OperationReuseMismatch)
        }
        Err(SendTransactionError::Backend(error)) if is_transaction_cancelled(&error) => {
            Ok(AttemptDecision::Retry)
        }
        Err(SendTransactionError::Reconcile) => reconcile_after_dispatch(
            provider,
            attempt.transaction.operation_id(),
            attempt.fingerprint,
            attempt.deadline,
        )
        .await
        .map(AttemptDecision::Outcome),
        Err(SendTransactionError::Backend(error)) => Err(RawOrderedJobStoreError::Backend(error)),
    }
}

async fn existing_receipt(
    provider: Arc<RemoteStorageProvider>,
    attempt: &AttemptContext<'_>,
) -> Result<Option<AttemptDecision>, RawOrderedJobStoreError> {
    match load_receipt(
        provider,
        attempt.transaction.operation_id(),
        attempt.deadline,
    )
    .await?
    {
        Some(receipt) => Ok(Some(AttemptDecision::Outcome(reconcile(
            receipt,
            attempt.fingerprint,
        )?))),
        None => Ok(None),
    }
}

async fn conflict_attempt(
    provider: Arc<RemoteStorageProvider>,
    attempt: &AttemptContext<'_>,
    failed_check: usize,
) -> Result<AttemptDecision, RawOrderedJobStoreError> {
    match persist_conflict(Arc::clone(&provider), attempt, failed_check).await {
        Ok(Some(outcome)) => Ok(AttemptDecision::Outcome(outcome)),
        Ok(None) => Ok(AttemptDecision::Retry),
        Err(SendTransactionError::Timeout) => Err(RawOrderedJobStoreError::Timeout),
        Err(SendTransactionError::Backend(error)) if is_operation_reuse_mismatch(&error) => {
            Err(RawOrderedJobStoreError::OperationReuseMismatch)
        }
        Err(SendTransactionError::Reconcile) => reconcile_after_dispatch(
            provider,
            attempt.transaction.operation_id(),
            attempt.fingerprint,
            attempt.deadline,
        )
        .await
        .map(AttemptDecision::Outcome),
        Err(SendTransactionError::Backend(error)) => Err(RawOrderedJobStoreError::Backend(error)),
    }
}

async fn reconcile_after_dispatch(
    provider: Arc<RemoteStorageProvider>,
    operation_id: CommitOperationId,
    fingerprint: &[u8; RECEIPT_DIGEST_BYTES],
    deadline: Instant,
) -> Result<CommitOutcome, RawOrderedJobStoreError> {
    match load_receipt(provider, operation_id, deadline).await {
        Ok(Some(receipt)) => reconcile(receipt, fingerprint),
        Ok(None)
        | Err(RawOrderedJobStoreError::Backend(_))
        | Err(RawOrderedJobStoreError::Timeout) => Ok(CommitOutcome::Unknown),
        Err(error) => Err(error),
    }
}

fn ensure_before_dispatch(deadline: Instant) -> Result<(), RawOrderedJobStoreError> {
    if Instant::now() >= deadline {
        Err(RawOrderedJobStoreError::Timeout)
    } else {
        Ok(())
    }
}

async fn load_receipt(
    provider: Arc<RemoteStorageProvider>,
    operation_id: CommitOperationId,
    deadline: Instant,
) -> Result<Option<Receipt>, RawOrderedJobStoreError> {
    let mut request = GetItemRequest::new(
        job_table(),
        encoding::key_attributes(encoding::receipt_sort_key(operation_id)),
    );
    request.consistent_read = Some(true);
    let response: GetItemResponse =
        crate::provider::scheduled_job_store::deadline::invoke_with_deadline(
            provider,
            "DynamoDB_20120810.GetItem",
            &request,
            deadline,
        )
        .await?;
    let Some(item) = response.item else {
        return Ok(None);
    };
    let item =
        storage_types::AttributeMap::try_from(item).map_err(RawOrderedJobStoreError::Backend)?;
    encoding::validate_receipt_identity(&item, operation_id)?;
    let kind = encoding::string_attribute(&item, RECEIPT_KIND_ATTRIBUTE)?;
    if kind != RECEIPT_KIND {
        return Err(RawOrderedJobStoreError::Corrupt(
            "scheduled receipt has an unknown format",
        ));
    }
    let fingerprint = encoding::decode_receipt_fingerprint(&item)?;
    let outcome = match encoding::receipt_outcome(&item)? {
        OUTCOME_COMMITTED => CommitOutcome::Committed,
        OUTCOME_CONFLICT => CommitOutcome::Conflict,
        _ => {
            return Err(RawOrderedJobStoreError::Corrupt(
                "scheduled receipt has an invalid outcome",
            ));
        }
    };
    Ok(Some(Receipt {
        fingerprint,
        outcome,
    }))
}

fn reconcile(
    receipt: Receipt,
    fingerprint: &[u8; RECEIPT_DIGEST_BYTES],
) -> Result<CommitOutcome, RawOrderedJobStoreError> {
    if receipt.fingerprint != *fingerprint {
        return Err(RawOrderedJobStoreError::OperationReuseMismatch);
    }
    Ok(receipt.outcome)
}

async fn first_failed_check(
    provider: Arc<RemoteStorageProvider>,
    attempt: &AttemptContext<'_>,
) -> Result<Option<usize>, RawOrderedJobStoreError> {
    let keys: Vec<_> = attempt
        .transaction
        .checks()
        .iter()
        .map(|check| check.key().clone())
        .collect();
    if keys.is_empty() {
        return Ok(None);
    }
    let values = crate::provider::scheduled_job_store::read::get_many_until(
        provider,
        &keys,
        Some(attempt.deadline),
    )
    .await?;
    Ok(attempt
        .transaction
        .checks()
        .iter()
        .zip(values)
        .position(|(check, value)| !check_holds(check, value.as_ref())))
}

fn check_holds(check: &Check, value: Option<&Bytes>) -> bool {
    match check {
        Check::Absent(_) => value.is_none(),
        Check::Value(_, expected) => value.is_some_and(|actual| actual == expected),
    }
}

async fn persist_conflict(
    provider: Arc<RemoteStorageProvider>,
    attempt: &AttemptContext<'_>,
    failed_check: usize,
) -> Result<Option<CommitOutcome>, SendTransactionError> {
    let check = &attempt.transaction.checks()[failed_check];
    let items = vec![
        conflict_guard(check),
        receipt_put(
            attempt.transaction.operation_id(),
            attempt.fingerprint,
            OUTCOME_CONFLICT,
        )
        .map_err(SendTransactionError::Backend)?,
    ];
    let request = transact_request(attempt.transaction.operation_id(), "f", items);
    match send_transaction(provider, &request, attempt.deadline).await {
        Ok(()) => Ok(Some(CommitOutcome::Conflict)),
        Err(SendTransactionError::Backend(error)) if is_transaction_cancelled(&error) => Ok(None),
        Err(error) => Err(error),
    }
}

fn conflict_guard(check: &Check) -> TransactWriteItem {
    let (expression, values) = match check {
        Check::Absent(_) => ("attribute_exists(#pk)".to_string(), None),
        Check::Value(_, expected) => (
            "attribute_not_exists(#pk) OR attribute_not_exists(#v) OR #v <> :expected".to_string(),
            Some(storage_types::AttributeMap::from_iter([(
                ":expected".to_string(),
                encoding::value_attribute(expected),
            )])),
        ),
    };
    TransactWriteItem {
        condition_check: Some(TransactConditionCheckRequest {
            table_name: job_table(),
            key: encoding::key_attributes(encoding::data_sort_key(check.key())),
            condition_expression: expression,
            expression_attribute_names: Some(attribute_names_for_check(check)),
            expression_attribute_values: values,
            return_values_on_condition_check_failure: None,
        }),
        ..TransactWriteItem::default()
    }
}

async fn persist_committed(
    provider: Arc<RemoteStorageProvider>,
    attempt: &AttemptContext<'_>,
) -> Result<(), SendTransactionError> {
    let mut items = attempt
        .transaction
        .mutations()
        .iter()
        .map(|mutation| {
            let check = attempt
                .transaction
                .checks()
                .iter()
                .find(|check| check.key() == mutation.key());
            mutation_item(mutation, check)
        })
        .collect::<StorageResult<Vec<_>>>()
        .map_err(SendTransactionError::Backend)?;
    items.extend(
        attempt
            .transaction
            .checks()
            .iter()
            .filter(|check| {
                !attempt
                    .transaction
                    .mutations()
                    .iter()
                    .any(|mutation| mutation.key() == check.key())
            })
            .map(condition_item),
    );
    items.push(
        receipt_put(
            attempt.transaction.operation_id(),
            attempt.fingerprint,
            OUTCOME_COMMITTED,
        )
        .map_err(SendTransactionError::Backend)?,
    );
    let request = transact_request(attempt.transaction.operation_id(), "c", items);
    send_transaction(provider, &request, attempt.deadline).await
}

fn condition_item(check: &Check) -> TransactWriteItem {
    let (expression, values) = check_condition(check);
    TransactWriteItem {
        condition_check: Some(TransactConditionCheckRequest {
            table_name: job_table(),
            key: encoding::key_attributes(encoding::data_sort_key(check.key())),
            condition_expression: expression,
            expression_attribute_names: Some(attribute_names_for_check(check)),
            expression_attribute_values: values,
            return_values_on_condition_check_failure: None,
        }),
        ..TransactWriteItem::default()
    }
}

fn mutation_item(mutation: &Mutation, check: Option<&Check>) -> StorageResult<TransactWriteItem> {
    let condition = check.map(check_condition);
    Ok(match mutation {
        Mutation::Put(key, value) => TransactWriteItem {
            put: Some(TransactPutRequest {
                table_name: job_table(),
                item: encoding::data_item(key, value)?,
                indexers: None,
                condition_expression: condition.as_ref().map(|(expression, _)| expression.clone()),
                expression_attribute_names: check.map(attribute_names_for_check),
                expression_attribute_values: condition.and_then(|(_, values)| values),
                return_values_on_condition_check_failure: None,
                aux_item_stream_ttl_hours: None,
            }),
            ..TransactWriteItem::default()
        },
        Mutation::Delete(key) => TransactWriteItem {
            delete: Some(TransactDeleteRequest {
                table_name: job_table(),
                key: encoding::key_attributes(encoding::data_sort_key(key)),
                condition_expression: condition.as_ref().map(|(expression, _)| expression.clone()),
                expression_attribute_names: check.map(attribute_names_for_check),
                expression_attribute_values: condition.and_then(|(_, values)| values),
                return_values_on_condition_check_failure: None,
                aux_item_stream_ttl_hours: None,
            }),
            ..TransactWriteItem::default()
        },
    })
}

fn check_condition(check: &Check) -> (String, Option<storage_types::AttributeMap>) {
    match check {
        Check::Absent(_) => ("attribute_not_exists(#pk)".to_string(), None),
        Check::Value(_, expected) => (
            "attribute_exists(#pk) AND #v = :expected".to_string(),
            Some(storage_types::AttributeMap::from_iter([(
                ":expected".to_string(),
                encoding::value_attribute(expected),
            )])),
        ),
    }
}

fn attribute_names_for_check(check: &Check) -> storage_types::VecMap<String, String> {
    let mut names = key_attribute_names();
    if matches!(check, Check::Value(_, _)) {
        names.insert("#v".to_string(), VALUE_ATTRIBUTE.to_string());
    }
    names
}

fn key_attribute_names() -> storage_types::VecMap<String, String> {
    [("#pk".to_string(), KEY_ATTRIBUTE.to_string())]
        .into_iter()
        .collect()
}

fn receipt_put(
    operation_id: CommitOperationId,
    fingerprint: &[u8; RECEIPT_DIGEST_BYTES],
    outcome: &'static str,
) -> StorageResult<TransactWriteItem> {
    Ok(TransactWriteItem {
        put: Some(TransactPutRequest {
            table_name: job_table(),
            item: encoding::receipt_item(operation_id, fingerprint, outcome)?,
            indexers: None,
            condition_expression: Some("attribute_not_exists(#pk)".to_string()),
            expression_attribute_names: Some(key_attribute_names()),
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
            aux_item_stream_ttl_hours: None,
        }),
        ..TransactWriteItem::default()
    })
}

fn transact_request(
    operation_id: CommitOperationId,
    phase: &str,
    transact_items: Vec<TransactWriteItem>,
) -> TransactWriteItemsRequest {
    TransactWriteItemsRequest {
        transact_items,
        client_request_token: Some(format!("{}-{phase}", encoding::operation_hex(operation_id))),
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    }
}

pub(super) fn classify_send_transaction_error(
    failure: RemoteInvocationError,
) -> SendTransactionError {
    let RemoteInvocationError {
        error,
        dispatch_status,
    } = failure;
    if matches!(
        dispatch_status,
        HttpRequestDispatchStatus::PossiblyDispatched
    ) && !is_known_not_applied(&error)
    {
        SendTransactionError::Reconcile
    } else {
        SendTransactionError::Backend(error)
    }
}

fn is_known_not_applied(error: &StorageError) -> bool {
    matches!(
        error.to_enum(),
        StorageEnum::KeyValidation(_)
            | StorageEnum::ResourceNotFound { .. }
            | StorageEnum::IndexNotFound { .. }
            | StorageEnum::Validation { .. }
            | StorageEnum::RawValidation { .. }
            | StorageEnum::Unsupported { .. }
            | StorageEnum::TableNotFound { .. }
            | StorageEnum::TableAlreadyExists { .. }
            | StorageEnum::AccessDenied { .. }
            | StorageEnum::Authentication { .. }
            | StorageEnum::MissingAuthenticationToken
            | StorageEnum::TransactionCanceled { .. }
            | StorageEnum::TransactionConflict { .. }
            | StorageEnum::ConditionalCheckFailed
            | StorageEnum::ConditionalCheckFailedWithItem { .. }
            | StorageEnum::IdempotentParameterMismatch
            | StorageEnum::AwsSerialization(_)
            | StorageEnum::Database(_)
    )
}

fn is_operation_reuse_mismatch(error: &StorageError) -> bool {
    matches!(error.to_enum(), StorageEnum::IdempotentParameterMismatch)
}

async fn send_transaction(
    provider: Arc<RemoteStorageProvider>,
    request: &TransactWriteItemsRequest,
    deadline: Instant,
) -> Result<(), SendTransactionError> {
    let remaining = crate::provider::scheduled_job_store::deadline::remaining(deadline)
        .map_err(|_| SendTransactionError::Timeout)?;
    let response = tokio::time::timeout(remaining, provider.invoke_transaction(request))
        .await
        .map_err(|_| SendTransactionError::Reconcile)?
        .map_err(classify_send_transaction_error)?;
    let _ = response;
    Ok(())
}

fn is_transaction_cancelled(error: &StorageError) -> bool {
    matches!(error.to_enum(), StorageEnum::TransactionCanceled { .. })
}
