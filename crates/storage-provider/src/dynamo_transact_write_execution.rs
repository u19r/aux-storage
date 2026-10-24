//! Shared validation, retry, and native attempt ownership for destination
//! writes.

use storage_common::retry::retry_native_conflicts;
use storage_types::{
    AttemptMode, StorageEnum, StorageResult, TableName, TimestampMillis, TransactWriteItemsRequest,
    TransactWriteItemsResponse, context::WrappedError as _,
};

use crate::{DYNAMO_WRITE_RETRY, DynamoPort, DynamoTransactWriteShape, PreparedWriteAttempt};

pub(crate) async fn write_transaction<P: DynamoPort>(
    port: &P,
    request: TransactWriteItemsRequest,
    authority: Option<crate::job_write_authority::JobWriteAuthority>,
) -> StorageResult<TransactWriteItemsResponse> {
    let shape = DynamoTransactWriteShape::try_from_request(request)?;
    let shape = match authority {
        Some(authority) => shape.authorize_effect(authority)?,
        None => shape,
    };
    let captured_at = port.now();
    retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        write_transaction_attempt(port, shape.clone(), captured_at)
    })
    .await
    .inspect_err(|error| {
        if matches!(error.to_enum(), StorageEnum::IdempotentParameterMismatch) {
            tracing::info!(
                operation = "TransactWriteItems",
                error_code = "IdempotentParameterMismatchException",
                reason = "token_reused_with_different_request",
                "transaction request rejected"
            );
        }
    })
}

async fn write_transaction_attempt<P: DynamoPort>(
    port: &P,
    shape: DynamoTransactWriteShape,
    captured_at: TimestampMillis,
) -> StorageResult<TransactWriteItemsResponse> {
    let cache = port.metadata_cache();
    let tables = crate::dynamo_write_preparation::PreparedWriteTables::load(
        port,
        &cache,
        shape.table_names(),
    )
    .await?;
    let bound = shape.bind(
        tables.ensure_present().map(|()| {
            |name: &TableName| {
                let table = tables.table(name)?;
                let ttl = crate::dynamo_write_preparation::prepared_ttl_policy(&cache, &table)?;
                Ok(crate::WriteTableBinding { table, ttl })
            }
        }),
        captured_at,
    );
    let writes = port.prepare_put_items(&bound.put_items());
    let clock = port.clock();
    port.with_dynamo_attempt(AttemptMode::ReadWriteItems, move |attempt| {
        Box::pin(async move {
            attempt.install_prepared_writes(writes);
            bound
                .execute(attempt, &cache, &tables, (captured_at, clock))
                .await
        })
    })
    .await
}
