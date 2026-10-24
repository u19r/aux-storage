//! One native read attempt for a BatchGetItem request.

use std::sync::Arc;

use storage_types::{
    BatchGetItemRequest, BatchGetItemResponse, BoundTableInfo, StorageError, StorageResult,
};

use crate::{
    BatchGetPlan, BatchGetTableResult, CachePublication, GetItemAttempt, TableMetadataCache,
    resolve_tables_batch_in_attempt,
};

pub async fn batch_get_item_in_attempt<A: GetItemAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    request: BatchGetItemRequest,
) -> StorageResult<(BatchGetItemResponse, Vec<CachePublication>)> {
    let plan = BatchGetPlan::from(request);
    let names = plan
        .tables
        .iter()
        .map(|table| table.table_name.clone())
        .collect::<Vec<_>>();
    let metadata = match resolve_tables_batch_in_attempt(attempt, cache, &names).await {
        Ok(metadata) => metadata,
        Err(_) => {
            let results = (0..plan.tables.len())
                .map(|_| BatchGetTableResult::Unprocessed)
                .collect();
            return Ok((plan.finish_api_response(results)?, Vec::new()));
        }
    };
    let mut publications = Vec::new();
    let tables = metadata.into_iter().map(|result| {
        result.map(|(table, publication)| {
            publications.extend(publication);
            table
        })
    });
    let response = batch_get_item_from_tables(attempt, plan, tables).await?;
    Ok((response, publications))
}

/// Execute against catalog observations already acquired in this native
/// snapshot. Replication can qualify those observations without resolving
/// metadata twice; the ordinary path retains the same response decisions.
pub async fn batch_get_item_from_tables<A: GetItemAttempt>(
    attempt: &mut A,
    plan: BatchGetPlan,
    tables: impl ExactSizeIterator<Item = StorageResult<Arc<BoundTableInfo>>>,
) -> StorageResult<BatchGetItemResponse> {
    BoundBatchGet::try_new(plan, tables)?.read(attempt).await
}

struct BoundBatchGet {
    plan: BatchGetPlan,
    tables: Vec<Option<Arc<BoundTableInfo>>>,
}

impl BoundBatchGet {
    fn try_new(
        plan: BatchGetPlan,
        tables: impl ExactSizeIterator<Item = StorageResult<Arc<BoundTableInfo>>>,
    ) -> StorageResult<Self> {
        if tables.len() != plan.tables.len() {
            return Err(StorageError::internal(
                "batch-get metadata count differs from request",
            ));
        }
        let mut bound = Vec::with_capacity(tables.len());
        for (table_plan, metadata) in plan.tables.iter().zip(tables) {
            match metadata {
                Ok(table) => {
                    let ready = table_plan.bind(Ok(Arc::clone(&table.info)))?.is_some();
                    bound.push(ready.then_some(table));
                }
                Err(error) => {
                    table_plan.bind(Err(error))?;
                    bound.push(None);
                }
            }
        }
        Ok(Self {
            plan,
            tables: bound,
        })
    }

    async fn read<A: GetItemAttempt>(self, attempt: &mut A) -> StorageResult<BatchGetItemResponse> {
        let reads = self
            .plan
            .tables
            .iter()
            .zip(&self.tables)
            .flat_map(|(plan, table)| {
                table.iter().flat_map(move |table| {
                    plan.keys.keys.iter().map(move |key| (table.as_ref(), key))
                })
            })
            .collect::<Vec<_>>();
        let observed = if reads.is_empty() {
            Ok(Vec::new())
        } else {
            attempt.read_primary_pairs(&reads).await
        };
        let mut items = observed.ok().map(Vec::into_iter);
        let mut results = Vec::with_capacity(self.plan.tables.len());
        for (plan, table) in self.plan.tables.iter().zip(self.tables) {
            let Some(items) = items.as_mut().filter(|_| table.is_some()) else {
                results.push(BatchGetTableResult::Unprocessed);
                continue;
            };
            results
                .push(plan.finish_read(Ok(items.by_ref().take(plan.keys.keys.len()).collect()))?);
        }
        self.plan.finish_api_response(results)
    }
}
