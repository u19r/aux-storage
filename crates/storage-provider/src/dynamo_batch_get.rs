//! One native read attempt for a BatchGetItem request.

use std::sync::Arc;

use storage_types::{BatchGetItemResponse, BoundTableInfo, Exact, StorageResult};

use crate::{
    BatchGetPlan, BatchGetTableResult, CachePublication, ItemReadAttempt, TableMetadataCache,
    resolve_tables_batch_in_attempt,
};

pub async fn batch_get_item_in_attempt<A: ItemReadAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    plan: &BatchGetPlan,
    bindings: &crate::RequestTableBindings,
) -> StorageResult<(BatchGetItemResponse, Vec<CachePublication>)> {
    let names = plan
        .tables
        .iter()
        .map(|table| table.table_name.clone())
        .collect::<Vec<_>>();
    let metadata = resolve_tables_batch_in_attempt(attempt, cache, &names, bindings).await?;
    let mut publications = Vec::new();
    let tables = metadata.map(|result| {
        result.and_then(|(table, publication)| {
            table.require_data_readable()?;
            publications.extend(publication);
            Ok(table)
        })
    });
    let response = batch_get_item_from_tables(attempt, plan, tables).await?;
    Ok((response, publications))
}

pub(crate) async fn finish_batch_get_item_in_attempt<A: ItemReadAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    plan: &BatchGetPlan,
    bindings: &crate::RequestTableBindings,
) -> StorageResult<(BatchGetItemResponse, Vec<crate::CachePublication>)> {
    use crate::dynamo_read_execution::{collect_read_candidates, confirm_read_observation};
    let Some(candidates) =
        collect_read_candidates(cache, plan.tables.iter().map(|table| &table.table_name))
    else {
        let (response, publications) =
            batch_get_item_in_attempt(attempt, cache, plan, bindings).await?;
        return Ok((response, publications));
    };
    if candidates.is_empty() {
        let (response, publications) =
            batch_get_item_in_attempt(attempt, cache, plan, bindings).await?;
        return Ok((response, publications));
    }
    let bound = BoundBatchGet::try_new(
        plan,
        candidates
            .iter()
            .map(|(candidate, _)| Ok(Arc::clone(&candidate.table))),
    );
    let reads = bound
        .as_ref()
        .map_or_else(|_| Vec::new(), |bound| bound.reads());
    let tables = candidates
        .iter()
        .map(|(candidate, _)| candidate.read_candidate())
        .collect::<Vec<_>>();
    let result = attempt
        .finish_primary_read(crate::PrimaryReadBatch {
            tables: &tables,
            reads: &reads,
        })
        .await?;
    let (items, publications) =
        confirm_read_observation(cache, bindings, candidates.as_slice(), result)?;
    Ok((bound?.finish(items)?, publications))
}

/// Execute against catalog observations already acquired in this native
/// snapshot. Replication can qualify those observations without resolving
/// metadata twice; the ordinary path retains the same response decisions.
pub async fn batch_get_item_from_tables<A: ItemReadAttempt>(
    attempt: &mut A,
    plan: &BatchGetPlan,
    tables: Exact<StorageResult<Arc<BoundTableInfo>>>,
) -> StorageResult<BatchGetItemResponse> {
    BoundBatchGet::try_new(plan, tables)?.read(attempt).await
}

struct BoundBatchGet<'a> {
    plan: &'a BatchGetPlan,
    tables: Vec<Option<Arc<BoundTableInfo>>>,
}

impl<'a> BoundBatchGet<'a> {
    /// `tables` holds one result per plan table, in plan order: an `Exact`
    /// from the catalog resolver, or candidates collected from the plan's
    /// names.
    fn try_new(
        plan: &'a BatchGetPlan,
        tables: impl IntoIterator<Item = StorageResult<Arc<BoundTableInfo>>>,
    ) -> StorageResult<Self> {
        let mut bound = Vec::with_capacity(plan.tables.len());
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

    fn reads(&self) -> Vec<(&BoundTableInfo, &storage_types::KeyAttributes)> {
        self.plan
            .tables
            .iter()
            .zip(&self.tables)
            .flat_map(|(plan, table)| {
                table.iter().flat_map(move |table| {
                    plan.keys.keys.iter().map(move |key| (table.as_ref(), key))
                })
            })
            .collect()
    }

    async fn read<A: ItemReadAttempt>(
        self,
        attempt: &mut A,
    ) -> StorageResult<BatchGetItemResponse> {
        let reads = self.reads();
        let observed = if reads.is_empty() {
            Ok(Exact::empty())
        } else {
            attempt.read_primary_pairs(&reads).await
        };
        self.finish(observed?)
    }

    fn finish(
        self,
        items: Exact<Option<storage_types::WireItem>>,
    ) -> StorageResult<BatchGetItemResponse> {
        let mut items = items.into_iter();
        let mut results = Vec::with_capacity(self.plan.tables.len());
        for (plan, table) in self.plan.tables.iter().zip(self.tables) {
            if table.is_none() {
                results.push(BatchGetTableResult::Unprocessed);
                continue;
            }
            results.push(plan.finish_read(&mut items));
        }
        self.plan.finish_api_response(results)
    }
}
