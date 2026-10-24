//! Metadata-bound preparation before admission to a native writer.

use std::sync::Arc;

use storage_common::TtlConfigRecord;
use storage_types::{AttemptMode, BoundTableInfo, StorageError, StorageResult, TableName};

use crate::{DescribeTimeToLiveAttempt, TtlPolicyLookup};

/// Native-family preparation. Ordinary callers can encode before admission;
/// callers that select a layout inside the attempt prepare through that view.
pub trait PreparedWriteAttempt: Send {
    type PreparedWrites: Default + Send + 'static;
    /// Install preparation and retain any failure in native settlement, even
    /// when no upsert consumes it. Later installation cannot reopen the
    /// attempt.
    fn install_prepared_writes(&mut self, writes: StorageResult<Self::PreparedWrites>);
    /// Encode and install against this attempt's physical layout. Failure
    /// discards previous preparation and rejects native settlement with its
    /// typed cause; no root-layout values are borrowed.
    fn prepare_put_items(&mut self, items: &[&crate::BoundPutItem]) -> StorageResult<()>;
}

use crate::{
    DynamoPort, TableMetadataCache,
    dynamo_catalog::{PreparedTable, resolve_prepared_table_in_attempt},
};

pub(crate) async fn prepare_write_table<P: DynamoPort>(
    port: &P,
    cache: &Arc<TableMetadataCache>,
    name: &TableName,
) -> StorageResult<PreparedTable> {
    if let Some(candidate) = cache.prepared_candidate(name) {
        warm_ttl_policies(port, cache, std::slice::from_ref(&candidate.table)).await?;
        return Ok(candidate);
    }
    let name = name.clone();
    let publication_cache = Arc::clone(cache);
    let cache = Arc::clone(cache);
    let (prepared, publication) = port
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                resolve_prepared_table_in_attempt(attempt, &cache, &name)
                    .await?
                    .into_result()
            })
        })
        .await?;
    publication_cache.publish(publication);
    warm_ttl_policies(
        port,
        &publication_cache,
        std::slice::from_ref(&prepared.table),
    )
    .await?;
    Ok(prepared)
}

pub(crate) struct PreparedWriteTables {
    names: Vec<TableName>,
    tables: Vec<Option<PreparedTable>>,
}

impl PreparedWriteTables {
    pub(crate) async fn load<P: DynamoPort>(
        port: &P,
        cache: &Arc<TableMetadataCache>,
        names: Vec<TableName>,
    ) -> StorageResult<Self> {
        let candidates = names
            .iter()
            .map(|name| cache.prepared_candidate(name))
            .collect::<Vec<_>>();
        let prepared = if candidates.iter().all(Option::is_some) {
            Self {
                names,
                tables: candidates,
            }
        } else {
            let read_names = names.clone();
            let read_cache = Arc::clone(cache);
            let observations = port
                .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
                    Box::pin(async move {
                        crate::dynamo_catalog::resolve_prepared_tables_in_attempt(
                            attempt,
                            &read_cache,
                            &read_names,
                        )
                        .await
                    })
                })
                .await?;
            Self::from_observations(names, observations, cache)?
        };
        let tables = prepared
            .tables
            .iter()
            .flatten()
            .map(|table| Arc::clone(&table.table))
            .collect::<Vec<_>>();
        warm_ttl_policies(port, cache, &tables).await?;
        Ok(prepared)
    }

    fn from_observations(
        names: Vec<TableName>,
        observations: Vec<StorageResult<(PreparedTable, Option<crate::CachePublication>)>>,
        cache: &TableMetadataCache,
    ) -> StorageResult<Self> {
        let mut publications = Vec::new();
        let tables = observations
            .into_iter()
            .map(|result| {
                let table = match result {
                    Ok((table, publication)) => {
                        publications.extend(publication);
                        Some(table)
                    }
                    Err(error)
                        if matches!(
                            storage_types::context::WrappedError::to_enum(&error),
                            storage_types::StorageEnum::TableNotFound { .. }
                        ) =>
                    {
                        None
                    }
                    Err(error) => return Err(error),
                };
                Ok(table)
            })
            .collect::<StorageResult<Vec<_>>>()?;
        cache.publish(publications);
        Ok(Self { names, tables })
    }

    pub(crate) fn ensure_present(&self) -> StorageResult<()> {
        for (name, table) in self.names.iter().zip(&self.tables) {
            if table.is_none() {
                return Err(storage_types::StorageError::table_not_found(name));
            }
        }
        Ok(())
    }

    pub(crate) fn table(
        &self,
        name: &TableName,
    ) -> StorageResult<Arc<storage_types::BoundTableInfo>> {
        self.names
            .iter()
            .zip(&self.tables)
            .find(|(candidate, _)| *candidate == name)
            .and_then(|(_, table)| table.as_ref())
            .map(|table| Arc::clone(&table.table))
            .ok_or_else(|| storage_types::StorageError::table_not_found(name))
    }

    pub(crate) async fn confirm<A: crate::CatalogAttempt>(
        &self,
        attempt: &mut A,
        cache: &TableMetadataCache,
    ) -> StorageResult<()> {
        let stamps = attempt.read_catalog_stamps(&self.names).await?;
        if stamps.len() != self.names.len() {
            return Err(storage_types::StorageError::internal(
                "prepared catalog stamp count differs",
            ));
        }
        for ((name, table), stamp) in self.names.iter().zip(&self.tables).zip(stamps) {
            cache.confirm_stamp(name, table.as_ref().map(|table| table.stamp), stamp)?;
        }
        Ok(())
    }
}

/// A bounded stale TTL policy is the existing tracking contract; table stamps
/// still fence native identity. Publish cold policy reads after settlement.
async fn warm_ttl_policies<P: DynamoPort>(
    port: &P,
    cache: &Arc<TableMetadataCache>,
    tables: &[Arc<BoundTableInfo>],
) -> StorageResult<()> {
    let missing = tables
        .iter()
        .filter_map(|table| match cache.lookup_ttl_policy(table) {
            TtlPolicyLookup::Hit(_) => None,
            TtlPolicyLookup::Miss(generation) => Some((table.table_name.clone(), generation)),
        })
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(());
    }
    let read_cache = Arc::clone(cache);
    let policies = port
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                let mut policies = Vec::with_capacity(missing.len());
                for (name, generation) in missing {
                    let (table, _) = resolve_prepared_table_in_attempt(attempt, &read_cache, &name)
                        .await?
                        .into_result()?;
                    let config = attempt.read_ttl_config(&table.table).await?;
                    policies.push((table.table, config, generation));
                }
                Ok(policies)
            })
        })
        .await?;
    for (table, config, generation) in policies {
        cache.publish_ttl_policy(&table, config, generation);
    }
    Ok(())
}

pub(crate) fn prepared_ttl_policy(
    cache: &TableMetadataCache,
    table: &BoundTableInfo,
) -> StorageResult<Option<TtlConfigRecord>> {
    match cache.lookup_ttl_policy(table) {
        TtlPolicyLookup::Hit(config) => Ok(config),
        TtlPolicyLookup::Miss(_) => Err(StorageError::from(
            storage_types::StorageEnum::TransactionConflict {
                message: "prepared TTL policy expired".to_owned(),
            },
        )),
    }
}
