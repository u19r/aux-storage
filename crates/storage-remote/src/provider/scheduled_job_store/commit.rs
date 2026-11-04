use std::{sync::Arc, time::Instant};

use scheduled_bg_jobs::store::{Check, CommitOutcome, Mutation, StoreTransaction};
use storage_provider::RawOrderedJobStoreError;
use storage_types::{
    StorageEnum, StorageError, StorageResult, TransactConditionCheckRequest, TransactDeleteRequest,
    TransactPutRequest, TransactWriteItem, TransactWriteItemsRequest, context::WrappedError,
};

use crate::provider::{
    implementation::{RemoteInvocationError, RemoteStorageProvider},
    scheduled_job_store::{constants::*, encoding, provider::job_table},
};

pub(super) enum SendTransactionError {
    Backend(StorageError),
    Reconcile,
    Timeout,
}
pub(super) enum AttemptDecision {
    Outcome(CommitOutcome),
    Retry,
}

pub(crate) async fn run(
    provider: Arc<RemoteStorageProvider>,
    transaction: &StoreTransaction,
) -> Result<CommitOutcome, RawOrderedJobStoreError> {
    let deadline = Instant::now()
        .checked_add(MAX_TRANSACTION_DURATION)
        .unwrap_or_else(Instant::now);
    for _ in 0..MAX_COMMIT_ATTEMPTS {
        match run_attempt(Arc::clone(&provider), transaction, deadline).await? {
            AttemptDecision::Outcome(outcome) => return Ok(outcome),
            AttemptDecision::Retry => {}
        }
    }
    Err(StorageError::service_unavailable(1).into())
}

pub(super) async fn run_attempt(
    provider: Arc<RemoteStorageProvider>,
    transaction: &StoreTransaction,
    deadline: Instant,
) -> Result<AttemptDecision, RawOrderedJobStoreError> {
    let request = encode_request(transaction)?;
    match send_transaction(provider, &request, deadline).await {
        Ok(()) => Ok(AttemptDecision::Outcome(CommitOutcome::Committed)),
        Err(SendTransactionError::Reconcile) => {
            Ok(AttemptDecision::Outcome(CommitOutcome::Unknown))
        }
        Err(SendTransactionError::Timeout) => Err(RawOrderedJobStoreError::Timeout),
        Err(SendTransactionError::Backend(error)) => classify_abort(error),
    }
}

fn classify_abort(error: StorageError) -> Result<AttemptDecision, RawOrderedJobStoreError> {
    match error.to_enum() {
        StorageEnum::ConditionalCheckFailed
        | StorageEnum::ConditionalCheckFailedWithItem { .. } => {
            Ok(AttemptDecision::Outcome(CommitOutcome::Conflict))
        }
        StorageEnum::TransactionCanceled { reasons }
            if reasons
                .iter()
                .any(|reason| reason.split('\t').next() == Some("ConditionalCheckFailed")) =>
        {
            Ok(AttemptDecision::Outcome(CommitOutcome::Conflict))
        }
        StorageEnum::TransactionConflict { .. } => Ok(AttemptDecision::Retry),
        _ => Err(RawOrderedJobStoreError::Backend(error)),
    }
}

fn encode_request(transaction: &StoreTransaction) -> StorageResult<TransactWriteItemsRequest> {
    let mut items = transaction
        .mutations()
        .iter()
        .map(|mutation| {
            mutation_item(
                mutation,
                transaction
                    .checks()
                    .iter()
                    .find(|check| check.key() == mutation.key()),
            )
        })
        .collect::<StorageResult<Vec<_>>>()?;
    items.extend(
        transaction
            .checks()
            .iter()
            .filter(|check| {
                !transaction
                    .mutations()
                    .iter()
                    .any(|mutation| mutation.key() == check.key())
            })
            .map(condition_item),
    );
    Ok(TransactWriteItemsRequest {
        transact_items: items,
        // The journal's exact head check is the replay fence. DynamoDB's
        // short-lived token cache would bypass a later retired-head check.
        client_request_token: None,
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    })
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

pub(super) fn classify_send_transaction_error(
    failure: RemoteInvocationError,
) -> SendTransactionError {
    if failure.is_ambiguous() {
        SendTransactionError::Reconcile
    } else {
        SendTransactionError::Backend(failure.error)
    }
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
