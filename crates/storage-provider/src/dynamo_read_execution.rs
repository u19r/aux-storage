//! One native snapshot and metadata publication lifecycle for reads.

use std::future::Future;

use storage_types::{
    BatchGetItemResponse, DynamoRequestValidate, GetItemRequest, GetItemResponse, StorageError,
    StorageResult, TransactGetItemsResponse,
};

use crate::{CachePublication, ItemReadAttempt, TableMetadataCache, TransactGetPlan};

/// Capture candidates before any authority I/O; a mixed request uses ordinary
/// resolution.
pub(crate) fn collect_read_candidates<'a>(
    cache: &TableMetadataCache,
    names: impl ExactSizeIterator<Item = &'a storage_types::TableName>,
) -> Option<storage_types::Exact<(crate::dynamo_catalog::PreparedTable, u64)>> {
    let expected = names.len();
    let candidates = names
        .map(|name| {
            let (candidate, generation) = cache.candidate(name);
            candidate.map(|candidate| (candidate, generation))
        })
        .collect::<Option<Vec<_>>>()?;
    storage_types::Exact::try_new(expected, candidates).ok()
}

/// Actual observations pin every returned incarnation before any response or
/// schema decision. Publication remains deferred until the complete operation
/// settles.
pub(crate) fn confirm_read_observation<T>(
    cache: &TableMetadataCache,
    bindings: &crate::RequestTableBindings,
    candidates: &[(crate::dynamo_catalog::PreparedTable, u64)],
    result: crate::ReadObservationResult<T>,
) -> StorageResult<(T, Vec<CachePublication>)> {
    let has_observations = result.stamps.is_some();
    let confirmation = match result.stamps {
        Some(stamps) => bindings.confirm_observations(
            cache,
            candidates
                .iter()
                .zip(stamps)
                .map(|((candidate, _), observed)| {
                    crate::dynamo_table_bindings::CatalogConfirmation {
                        name: &candidate.table.info.table_name,
                        prepared: Some(candidate.stamp),
                        observed,
                    }
                }),
        ),
        None => Ok(()),
    };
    let items = match result.result {
        Ok(items) => items,
        Err(error) => {
            // A stamp change or identity rejection of this snapshot answers
            // first: a missing relation then retries as that conflict, with
            // backoff. Otherwise a missing relation (`FreshValidation`) goes to
            // the retry owner, which invalidates the request's tables and makes
            // the one fresh try. Physical/codec errors cannot become BatchGet
            // Unprocessed success, including local decoding after native COMMIT
            // has acknowledged.
            confirmation?;
            return Err(error);
        }
    };
    if !has_observations {
        return Err(StorageError::internal(
            "read result has no authority observations",
        ));
    }
    confirmation?;
    for (candidate, _) in candidates {
        candidate.table.require_data_readable()?;
    }
    let publications = candidates
        .iter()
        .filter_map(|(candidate, generation)| {
            candidate.cache_update_for_version(
                &candidate.table.info.table_name,
                result.catalog_version,
                *generation,
            )
        })
        .collect();
    Ok((items, publications))
}

/// A missing relation or column means the request's cached table metadata
/// is stale: drop those entries so the one fresh try resolves them again.
/// Every other failure keeps the cache warm (FoundationDB's
/// `StaleObservation` included; a stamp change was already invalidated by
/// `confirm_stamp`).
pub(crate) fn invalidate_on_missing_relation<'a>(
    cache: &TableMetadataCache,
    error: &StorageError,
    names: impl IntoIterator<Item = &'a storage_types::TableName>,
) {
    if matches!(
        error.as_ref(),
        storage_types::StorageEnum::Attempt(storage_types::AttemptFailure::FreshValidation(
            storage_types::FreshCause::MissingRelation(_)
        ))
    ) {
        for name in names {
            cache.invalidate(name);
        }
    }
}

/// Raw DTOs remain mutable; only this owned carrier enters ordinary reads.
pub(crate) struct ValidatedDynamoReadRequest<R>(R);

impl<R: DynamoRequestValidate> ValidatedDynamoReadRequest<R> {
    pub(crate) fn try_new(request: R) -> StorageResult<Self> {
        request
            .validate_for_dynamodb()
            .map_err(StorageError::raw_validation)?;
        Ok(Self(request))
    }
}

/// A read chooses its response type while the family store owns the
/// native read attempt. Requests are retained and borrowed across aborted
/// attempts.
pub(crate) trait DynamoReadShape: Send + Sync + 'static {
    type Response: Send + 'static;

    fn execute<A: ItemReadAttempt>(
        &self,
        attempt: &mut A,
        cache: &TableMetadataCache,
        bindings: &crate::RequestTableBindings,
    ) -> impl Future<Output = StorageResult<(Self::Response, Vec<CachePublication>)>> + Send;

    /// The tables this read resolves; the retry owner invalidates them after
    /// a missing relation.
    fn read_table_names(&self) -> impl Iterator<Item = &storage_types::TableName>;
}

impl DynamoReadShape for ValidatedDynamoReadRequest<GetItemRequest> {
    type Response = GetItemResponse;

    async fn execute<A: ItemReadAttempt>(
        &self,
        attempt: &mut A,
        cache: &TableMetadataCache,
        bindings: &crate::RequestTableBindings,
    ) -> StorageResult<(Self::Response, Vec<CachePublication>)> {
        crate::dynamo_get_item::finish_get_item_in_attempt(attempt, cache, bindings, &self.0).await
    }

    fn read_table_names(&self) -> impl Iterator<Item = &storage_types::TableName> {
        std::iter::once(&self.0.table_name)
    }
}

impl DynamoReadShape for crate::BatchGetPlan {
    type Response = BatchGetItemResponse;

    async fn execute<A: ItemReadAttempt>(
        &self,
        attempt: &mut A,
        cache: &TableMetadataCache,
        bindings: &crate::RequestTableBindings,
    ) -> StorageResult<(Self::Response, Vec<CachePublication>)> {
        crate::dynamo_batch_get::finish_batch_get_item_in_attempt(attempt, cache, self, bindings)
            .await
    }

    fn read_table_names(&self) -> impl Iterator<Item = &storage_types::TableName> {
        self.tables.iter().map(|table| &table.table_name)
    }
}

impl DynamoReadShape for TransactGetPlan {
    type Response = TransactGetItemsResponse;

    async fn execute<A: ItemReadAttempt>(
        &self,
        attempt: &mut A,
        cache: &TableMetadataCache,
        bindings: &crate::RequestTableBindings,
    ) -> StorageResult<(Self::Response, Vec<CachePublication>)> {
        crate::dynamo_transact_get::finish_transact_get_items_in_attempt(
            attempt, cache, self, bindings,
        )
        .await
    }

    fn read_table_names(&self) -> impl Iterator<Item = &storage_types::TableName> {
        self.table_names().iter()
    }
}
