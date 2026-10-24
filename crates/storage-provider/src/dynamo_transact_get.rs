//! One coherent native attempt for every member of TransactGetItems.

use std::sync::Arc;

use storage_types::{BoundTableInfo, StorageError, StorageResult, TransactGetItemsResponse};

use crate::{
    CachePublication, GetItemAttempt, TableMetadataCache, TransactGetPlan,
    resolve_tables_batch_in_attempt,
};

/// Reuse the existing shared request-order, duplicate-key, projection, and
/// consumed-capacity decisions while both backend families supply physical
/// point reads on the same native snapshot.
pub async fn transact_get_items_in_attempt<A: GetItemAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    plan: TransactGetPlan,
) -> StorageResult<(TransactGetItemsResponse, Vec<CachePublication>)> {
    let tables = resolve_tables_batch_in_attempt(attempt, cache, plan.table_names()).await?;
    let mut publications = Vec::new();
    let tables = tables.into_iter().map(|result| {
        result.map(|(table, publication)| {
            publications.extend(publication);
            table
        })
    });
    let response = transact_get_items_from_tables(attempt, plan, tables).await?;
    Ok((response, publications))
}

/// Request-order validation and reads use the already-qualified catalog
/// observations. No second metadata snapshot or provider algorithm exists.
pub async fn transact_get_items_from_tables<A: GetItemAttempt>(
    attempt: &mut A,
    plan: TransactGetPlan,
    tables: impl ExactSizeIterator<Item = StorageResult<Arc<BoundTableInfo>>>,
) -> StorageResult<TransactGetItemsResponse> {
    if tables.len() != plan.table_names().len() {
        return Err(StorageError::internal(
            "transact-get metadata count differs from request",
        ));
    }
    let mut metadata = Vec::with_capacity(tables.len());
    let mut bound = Vec::with_capacity(tables.len());
    for (name, result) in plan.table_names().iter().zip(tables) {
        match result {
            Ok(table) => {
                metadata.push(Ok((*table.info).clone()));
                bound.push((name, table));
            }
            Err(error) => metadata.push(Err(error)),
        }
    }
    let prepared = plan.bind(metadata)?;
    let reads = prepared
        .reads()
        .map(|(table, key)| {
            bound
                .iter()
                .find(|(name, _)| *name == &table.table_name)
                .map(|(_, bound)| (bound.as_ref(), key))
                .ok_or_else(|| StorageError::internal("transact-get table binding is missing"))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let _read_call = metrics_facade::begin_database_call("dynamo.transact_get.primary_batch");
    let items = attempt.read_primary_pairs(&reads).await?;
    prepared.finish(items)
}
