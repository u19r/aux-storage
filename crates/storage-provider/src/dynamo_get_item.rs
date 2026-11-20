//! One GetItem decision over a native SQL or KV read attempt.

use std::future::Future;

use storage_types::{
    AttributeProjection, BoundTableInfo, CatalogVersion, Exact, GetItemRequest, GetItemResponse,
    IndexName, KeyAttributes, StorageError, StorageResult, WireItem, read_key_validation_error,
    validate_transact_key,
};

use crate::{
    BoundRangeReadPage, BoundRangeReadPlan, CachePublication, CatalogAttempt, CatalogStamp,
    GsiReadinessSnapshot, RangeReadItem, RangeReadPage, TableMetadataCache,
    read_range_observation_default,
};

/// Borrowed metadata whose authority still needs confirmation.
#[derive(Clone, Copy)]
pub struct TableReadCandidate<'a> {
    pub table: &'a BoundTableInfo,
    pub stamp: CatalogStamp,
    pub catalog_version: Option<CatalogVersion>,
}

pub struct PrimaryReadBatch<'a> {
    pub tables: &'a [TableReadCandidate<'a>],
    pub reads: &'a [(&'a BoundTableInfo, &'a KeyAttributes)],
}

pub struct ReadObservationResult<T> {
    pub catalog_version: Option<CatalogVersion>,
    pub stamps: Option<Exact<Option<CatalogStamp>>>,
    pub result: StorageResult<T>,
}

/// Only physical point lookup belongs to the SQL or KV family.
///
/// The family lowerer checks index publication and reads a bounded physical
/// page. It does not parse Dynamo expressions or shape the API response.
pub trait ItemReadAttempt: CatalogAttempt {
    /// Last physical read of a complete operation. KV settles with its caller.
    fn finish_primary_read(
        &mut self,
        batch: PrimaryReadBatch<'_>,
    ) -> impl Future<Output = StorageResult<ReadObservationResult<Exact<Option<WireItem>>>>> + Send
    {
        finish_primary_read_default(self, batch)
    }

    fn read_primary_item(
        &mut self,
        table: &BoundTableInfo,
        key: &KeyAttributes,
    ) -> impl Future<Output = StorageResult<Option<WireItem>>> + Send;

    /// Batch point reads across tables on one native snapshot. The returned
    /// positions match the supplied pairs exactly.
    fn read_primary_pairs(
        &mut self,
        reads: &[(&BoundTableInfo, &KeyAttributes)],
    ) -> impl Future<Output = StorageResult<Exact<Option<WireItem>>>> + Send;

    fn read_gsi_readiness(
        &mut self,
        table: &BoundTableInfo,
        index: Option<&IndexName>,
    ) -> impl Future<Output = StorageResult<GsiReadinessSnapshot>> + Send {
        async move { crate::gsi_readiness_from_catalog(table, index) }
    }

    fn require_gsi_queryable(
        &mut self,
        table: &BoundTableInfo,
        index: &IndexName,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        async move {
            self.read_gsi_readiness(table, Some(index))
                .await?
                .require_queryable(index)
        }
    }

    /// Observe actual authority and read only the matching candidate. A missing
    /// range represents a provisional binding error, never a synthetic target.
    fn read_range_observation(
        &mut self,
        candidate: crate::TableReadCandidate<'_>,
        range: Option<(&BoundRangeReadPlan<'_>, &BoundRangeReadPage<'_>)>,
    ) -> impl Future<
        Output = StorageResult<crate::ReadObservationResult<Option<RangeReadPage<RangeReadItem>>>>,
    > + Send {
        read_range_observation_default(self, candidate, range)
    }

    fn read_range_page(
        &mut self,
        table: &BoundTableInfo,
        bound: &BoundRangeReadPlan<'_>,
        page: &BoundRangeReadPage<'_>,
    ) -> impl Future<Output = StorageResult<RangeReadPage<RangeReadItem>>> + Send;
}

/// Bind metadata and the item within one coherent native read attempt.
/// Projection and response shaping remain shared Dynamo policy.
pub async fn get_item_in_attempt<A: ItemReadAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    bindings: &crate::RequestTableBindings,
    request: &GetItemRequest,
) -> StorageResult<(GetItemResponse, Option<CachePublication>)> {
    let (table, publication) = bindings
        .resolve_table(attempt, cache, &request.table_name)
        .await?;
    table.require_data_readable()?;
    let response = get_item_from_table(attempt, &table, request).await?;
    Ok((response, publication))
}

/// Execute against metadata already resolved in the caller's native snapshot.
/// The caller owns the request's table binding; item decisions stay shared.
pub async fn get_item_from_table<A: ItemReadAttempt>(
    attempt: &mut A,
    table: &BoundTableInfo,
    request: &GetItemRequest,
) -> StorageResult<GetItemResponse> {
    validate_get_key(table, request)?;
    finish_get_item(
        attempt.read_primary_item(table, &request.key).await?,
        request,
    )
}

fn validate_get_key(table: &BoundTableInfo, request: &GetItemRequest) -> StorageResult<()> {
    if request.key.len() != table.info.key_schema.len() {
        return Err(StorageError::raw_validation(
            "The provided key element does not match the schema",
        ));
    }
    validate_transact_key(&table.info, &request.key).map_err(read_key_validation_error)?;
    Ok(())
}

fn finish_get_item(
    item: Option<WireItem>,
    request: &GetItemRequest,
) -> StorageResult<GetItemResponse> {
    let Some(projection) = AttributeProjection::new(
        request.projection_expression.as_deref(),
        request.attributes_to_get.as_deref(),
        request.expression_attribute_names.as_ref(),
    ) else {
        return Ok(GetItemResponse {
            item,
            consumed_capacity: None,
        });
    };
    let item = item
        .map(|item| projection.project_wire(&item))
        .transpose()?;
    Ok(GetItemResponse {
        item,
        consumed_capacity: None,
    })
}

impl PrimaryReadBatch<'_> {
    pub fn check_pairs(&self) -> StorageResult<()> {
        for (slot, candidate) in self.tables.iter().enumerate() {
            if candidate.stamp.history_id != candidate.table.history_id
                || self.tables[..slot].iter().any(|previous| {
                    previous.table.info.table_name == candidate.table.info.table_name
                })
            {
                return Err(StorageError::internal(
                    "point-read candidate identity differs or repeats",
                ));
            }
        }
        for (table, _) in self.reads {
            if !self
                .tables
                .iter()
                .any(|candidate| std::ptr::eq(candidate.table, *table))
            {
                return Err(StorageError::internal("point-read table has no candidate"));
            }
        }
        Ok(())
    }
}

pub(crate) async fn finish_get_item_in_attempt<A: ItemReadAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    bindings: &crate::RequestTableBindings,
    request: &GetItemRequest,
) -> StorageResult<(GetItemResponse, Vec<crate::CachePublication>)> {
    use crate::dynamo_read_execution::confirm_read_observation;
    let (candidate, generation) = cache.candidate(&request.table_name);
    let Some(candidate) = candidate else {
        let (response, publication) =
            get_item_in_attempt(attempt, cache, bindings, request).await?;
        return Ok((response, publication.into_iter().collect()));
    };
    let prepared = validate_get_key(&candidate.table, request);
    let tables = [candidate.read_candidate()];
    let reads = [(&*candidate.table, &request.key)];
    let result = attempt
        .finish_primary_read(PrimaryReadBatch {
            tables: &tables,
            reads: if prepared.is_ok() { &reads } else { &[] },
        })
        .await?;
    let (items, publications) =
        confirm_read_observation(cache, bindings, &[(candidate, generation)], result)?;
    prepared?;
    let item = items
        .into_single()
        .ok_or_else(|| StorageError::internal("point-read produced no item slot"))?;
    let response = finish_get_item(item, request)?;
    Ok((response, publications))
}

/// Version-aware authority and item loading for engines without early
/// settlement.
pub async fn finish_primary_read_default<A: ItemReadAttempt + ?Sized>(
    attempt: &mut A,
    batch: PrimaryReadBatch<'_>,
) -> StorageResult<ReadObservationResult<Exact<Option<WireItem>>>> {
    batch.check_pairs()?;
    if crate::dynamo_pipelined_authority::begin_pipelined_authority(
        attempt,
        batch
            .tables
            .iter()
            .map(|candidate| candidate.catalog_version),
    )? {
        let items = attempt.read_primary_pairs(batch.reads).await;
        let authority = crate::dynamo_pipelined_authority::finish_pipelined_authority(
            attempt,
            batch
                .tables
                .iter()
                .map(|candidate| (&candidate.table.info.table_name, Some(*candidate))),
            items.is_ok(),
        )
        .await?;
        return Ok(match authority {
            crate::dynamo_pipelined_authority::PipelinedAuthority::Observed {
                version,
                stamps,
                confirmed,
            } => ReadObservationResult {
                catalog_version: version,
                stamps: Some(stamps),
                result: if confirmed {
                    items
                } else {
                    Err(StorageError::internal("unconfirmed point-read authority"))
                },
            },
            crate::dynamo_pipelined_authority::PipelinedAuthority::ReadAgain {
                version,
                stamps,
            } => ReadObservationResult {
                catalog_version: version,
                stamps: Some(stamps),
                result: attempt.read_primary_pairs(batch.reads).await,
            },
            crate::dynamo_pipelined_authority::PipelinedAuthority::Failed => {
                ReadObservationResult {
                    catalog_version: None,
                    stamps: None,
                    result: items,
                }
            }
        });
    }
    let version = attempt.read_catalog_version().await?;
    let stamps = crate::dynamo_catalog::observe_catalog_stamps(
        attempt,
        batch
            .tables
            .iter()
            .map(|candidate| (&candidate.table.info.table_name, Some(*candidate))),
        version,
    )
    .await?;
    let items = if batch
        .tables
        .iter()
        .zip(&stamps)
        .all(|(candidate, stamp)| Some(candidate.stamp) == *stamp)
    {
        attempt.read_primary_pairs(batch.reads).await
    } else {
        Err(StorageError::internal("unconfirmed point-read authority"))
    };
    Ok(ReadObservationResult {
        catalog_version: version,
        stamps: Some(stamps),
        result: items,
    })
}
