//! Metadata-bound preparation before admission to a native writer.

use std::sync::Arc;

use storage_types::{AttemptMode, AttemptPlan, StorageResult, TableName};

use crate::{
    DynamoPort, TableMetadataCache,
    dynamo_catalog::{PreparedTable, resolve_prepared_table_in_attempt},
};

pub(crate) async fn prepare_write_table<P: DynamoPort>(
    port: &P,
    cache: &Arc<TableMetadataCache>,
    name: &TableName,
    bindings: &crate::RequestTableBindings,
    deadline: storage_types::AttemptDeadline,
) -> StorageResult<PreparedTable> {
    if let Some(candidate) = cache.prepared_candidate(name) {
        return Ok(candidate);
    }
    deadline.require_remaining(storage_types::AttemptPhase::Acquire)?;
    let name = name.clone();
    let publication_cache = Arc::clone(cache);
    let cache = Arc::clone(cache);
    let read_bindings = bindings.clone();
    let (prepared, publication) = port
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, deadline.class).started(deadline),
            move |attempt| {
                Box::pin(async move {
                    resolve_prepared_table_in_attempt(attempt, &cache, &name, &read_bindings)
                        .await?
                        .into_result()
                })
            },
        )
        .await?;
    publication_cache.publish(publication);
    Ok(prepared)
}

pub(crate) struct PreparedWriteTables {
    names: Vec<TableName>,
    tables: Vec<Option<PreparedTable>>,
}

/// Each accepted write observation with the GSI projection lifecycle it was
/// read under.
type ObservedWrites = storage_types::Exact<(
    crate::ItemWriteObservation,
    storage_backfill::GsiProjectionLifecycle,
)>;

impl PreparedWriteTables {
    pub(crate) async fn load<P: DynamoPort>(
        port: &P,
        cache: &Arc<TableMetadataCache>,
        names: Vec<TableName>,
        bindings: &crate::RequestTableBindings,
        deadline: storage_types::AttemptDeadline,
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
            let read_bindings = bindings.clone();
            deadline.require_remaining(storage_types::AttemptPhase::Acquire)?;
            let observations = port
                .with_attempt(
                    AttemptPlan::new(AttemptMode::ReadOnly, deadline.class).started(deadline),
                    move |attempt| {
                        Box::pin(async move {
                            crate::dynamo_catalog::resolve_prepared_tables_in_attempt(
                                attempt,
                                &read_cache,
                                &read_names,
                                &read_bindings,
                            )
                            .await
                        })
                    },
                )
                .await?;
            Self::from_observations(names, observations, cache)?
        };
        Ok(prepared)
    }

    /// One table already prepared for a single-item write.
    pub(crate) fn from_prepared(table: PreparedTable) -> Self {
        Self {
            names: vec![table.table.info.table_name.clone()],
            tables: vec![Some(table)],
        }
    }

    fn from_observations(
        names: Vec<TableName>,
        observations: storage_types::Exact<
            StorageResult<(PreparedTable, Option<crate::CachePublication>)>,
        >,
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

    pub(crate) fn inventory(&self) -> Vec<(&TableName, Option<crate::TableReadCandidate<'_>>)> {
        self.names
            .iter()
            .zip(&self.tables)
            .map(|(name, table)| (name, table.as_ref().map(PreparedTable::read_candidate)))
            .collect()
    }

    pub(crate) fn confirm_observations(
        &self,
        cache: &TableMetadataCache,
        bindings: &crate::RequestTableBindings,
        version: Option<storage_types::CatalogVersion>,
        stamps: storage_types::Exact<Option<crate::CatalogStamp>>,
        generation: u64,
    ) -> StorageResult<crate::SemanticDecision<Vec<crate::CachePublication>>> {
        let decision = bindings.confirm_write_observations(
            cache,
            self.names
                .iter()
                .zip(&self.tables)
                .zip(stamps)
                .map(|((name, table), observed)| {
                    crate::dynamo_table_bindings::CatalogConfirmation {
                        name,
                        prepared: table.as_ref().map(|table| table.stamp),
                        observed,
                    }
                }),
        )?;
        Ok(decision.map(|()| {
            self.names
                .iter()
                .zip(&self.tables)
                .filter_map(|(name, table)| {
                    table
                        .as_ref()
                        .and_then(|table| table.cache_update_for_version(name, version, generation))
                })
                .collect()
        }))
    }
}

/// The confirmed observations of one write attempt. `decision` stays apart
/// from the outer result: an idempotency receipt answers before it.
pub(crate) struct AcceptedObservations {
    pub(crate) decision: StorageResult<crate::SemanticDecision<Option<ObservedWrites>>>,
    pub(crate) publications: Vec<crate::CachePublication>,
}

impl PreparedWriteTables {
    pub(crate) fn accept_observations(
        &self,
        cache: &TableMetadataCache,
        bindings: &crate::RequestTableBindings,
        observed: crate::WriteObservationBatchResult,
        generation: u64,
    ) -> StorageResult<AcceptedObservations> {
        let has_stamps = observed.stamps.is_some();
        let confirmation = match observed.stamps {
            Some(stamps) => self.confirm_observations(
                cache,
                bindings,
                observed.catalog_version,
                stamps,
                generation,
            ),
            None => Ok(crate::SemanticDecision::Accepted(Vec::new())),
        };
        let observations = match observed.observations {
            Ok(values) => values,
            Err(crate::WriteObservationFailure { candidates, error }) => {
                return Err(self.stale_observation(&confirmation, &candidates, error));
            }
        };
        if !has_stamps {
            return Err(storage_types::StorageError::internal(
                "successful write observation has no actual stamps",
            ));
        }
        let mut publications = observed.publications;
        let decision = confirmation.map(|decision| {
            decision.map(|confirmed| {
                publications.extend(confirmed);
                observations
            })
        });
        Ok(AcceptedObservations {
            decision,
            publications,
        })
    }

    /// A missing relation of prepared tables. A stamp change seen in the same
    /// snapshot answers first: `confirm_stamp` already invalidated that entry
    /// and the conflict retry backs off. Otherwise the error goes to the retry
    /// owner, which invalidates the request's tables and makes the one fresh
    /// try. An identity rejection is not returned here: the fresh try decides
    /// it again, after any idempotency receipt.
    fn stale_observation(
        &self,
        confirmation: &StorageResult<crate::SemanticDecision<Vec<crate::CachePublication>>>,
        candidates: &[usize],
        error: storage_types::StorageError,
    ) -> storage_types::StorageError {
        if error.attempt_kind() != Some(storage_types::AttemptKind::FreshValidation) {
            return error;
        }
        if let Err(changed) = confirmation {
            return changed.clone();
        }
        let prepared = !candidates.is_empty()
            && candidates
                .iter()
                .all(|index| self.tables.get(*index).is_some_and(Option::is_some));
        if !prepared {
            return storage_types::StorageError::internal(
                "missing relation outside the prepared write tables",
            );
        }
        error
    }
}
