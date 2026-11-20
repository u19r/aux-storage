//! One coherent native attempt for every member of TransactGetItems.

use std::sync::Arc;

use storage_types::{BoundTableInfo, StorageError, StorageResult, TransactGetItemsResponse};

use crate::{
    CachePublication, ItemReadAttempt, TableMetadataCache, TransactGetPlan,
    resolve_tables_batch_in_attempt,
};

/// Reuse the existing shared request-order, duplicate-key, projection, and
/// consumed-capacity decisions while both backend families supply physical
/// point reads on the same native snapshot.
pub async fn transact_get_items_in_attempt<A: ItemReadAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    plan: &TransactGetPlan,
    bindings: &crate::RequestTableBindings,
) -> StorageResult<(TransactGetItemsResponse, Vec<CachePublication>)> {
    let tables =
        resolve_tables_batch_in_attempt(attempt, cache, plan.table_names(), bindings).await?;
    let mut publications = Vec::new();
    let tables = tables.map(|result| {
        result.and_then(|(table, publication)| {
            table.require_data_readable()?;
            publications.extend(publication);
            Ok(table)
        })
    });
    let response = transact_get_items_from_tables(attempt, plan, tables).await?;
    Ok((response, publications))
}

pub(crate) async fn finish_transact_get_items_in_attempt<A: ItemReadAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    plan: &TransactGetPlan,
    bindings: &crate::RequestTableBindings,
) -> StorageResult<(TransactGetItemsResponse, Vec<crate::CachePublication>)> {
    use crate::dynamo_read_execution::{collect_read_candidates, confirm_read_observation};
    let Some(candidates) = collect_read_candidates(cache, plan.table_names().iter()) else {
        let (response, publications) =
            transact_get_items_in_attempt(attempt, cache, plan, bindings).await?;
        return Ok((response, publications));
    };
    let bound = BoundTransactGet::try_new(
        plan,
        candidates
            .iter()
            .map(|(candidate, _)| Ok(Arc::clone(&candidate.table))),
    );
    let reads = match &bound {
        Ok(bound) => bound.reads()?,
        Err(_) => Vec::new(),
    };
    let tables = candidates
        .iter()
        .map(|(candidate, _)| candidate.read_candidate())
        .collect::<Vec<_>>();
    let result = {
        let _read_call = metrics_facade::begin_database_call("dynamo.transact_get.primary_batch");
        attempt
            .finish_primary_read(crate::PrimaryReadBatch {
                tables: &tables,
                reads: &reads,
            })
            .await?
    };
    let (items, publications) =
        confirm_read_observation(cache, bindings, candidates.as_slice(), result).map_err(
            crate::transact_get::plan::transact_get_table_not_found_as_resource_not_found,
        )?;
    let response = bound?.prepared.finish(items)?;
    Ok((response, publications))
}

/// Request-order validation and reads use the already-qualified catalog
/// observations. No second metadata snapshot or provider algorithm exists.
pub async fn transact_get_items_from_tables<A: ItemReadAttempt>(
    attempt: &mut A,
    plan: &TransactGetPlan,
    tables: storage_types::Exact<StorageResult<Arc<BoundTableInfo>>>,
) -> StorageResult<TransactGetItemsResponse> {
    let bound = BoundTransactGet::try_new(plan, tables)?;
    let reads = bound.reads()?;
    let items = {
        let _read_call = metrics_facade::begin_database_call("dynamo.transact_get.primary_batch");
        attempt.read_primary_pairs(&reads).await?
    };
    bound.prepared.finish(items)
}

/// Keep binding, pair association and finishing identical for ordinary and
/// final reads.
struct BoundTransactGet<'a> {
    prepared: crate::PreparedTransactGet<'a>,
    tables: Vec<Arc<BoundTableInfo>>,
}

impl<'a> BoundTransactGet<'a> {
    /// `tables` holds one result per plan table name, in plan order: an
    /// `Exact` from the catalog resolver, or candidates collected from the
    /// plan's names.
    fn try_new(
        plan: &'a TransactGetPlan,
        tables: impl IntoIterator<Item = StorageResult<Arc<BoundTableInfo>>>,
    ) -> StorageResult<Self> {
        let mut metadata = Vec::with_capacity(plan.table_names().len());
        let mut bound = Vec::with_capacity(plan.table_names().len());
        for result in tables {
            match result {
                Ok(table) => {
                    metadata.push(Ok((*table.info).clone()));
                    bound.push(table);
                }
                Err(error) => metadata.push(Err(error)),
            }
        }
        Ok(Self {
            prepared: plan.bind(metadata)?,
            tables: bound,
        })
    }

    fn reads(&self) -> StorageResult<Vec<(&BoundTableInfo, &storage_types::KeyAttributes)>> {
        self.prepared
            .reads()
            .map(|(table, key)| {
                self.tables
                    .iter()
                    .find(|bound| bound.info.table_name == table.table_name)
                    .map(|bound| (bound.as_ref(), key))
                    .ok_or_else(|| StorageError::internal("transact-get table binding is missing"))
            })
            .collect()
    }
}
