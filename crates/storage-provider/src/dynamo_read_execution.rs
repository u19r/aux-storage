//! One native snapshot and metadata publication lifecycle for item reads.

use std::future::Future;

use storage_types::{
    BatchGetItemRequest, BatchGetItemResponse, DynamoRequestValidate, GetItemRequest,
    GetItemResponse, StorageError, StorageResult, TransactGetItemsResponse,
};

use crate::{
    CachePublication, GetItemAttempt, TableMetadataCache, TransactGetPlan,
    batch_get_item_in_attempt, get_item_in_attempt, transact_get_items_in_attempt,
};

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

/// An item read chooses its response type while the family store owns the
/// native read attempt. Reads need no write timestamp, retry, or mutation.
pub(crate) trait DynamoPointReadShape: Send + 'static {
    type Response: Send + 'static;

    fn execute<A: GetItemAttempt>(
        self,
        attempt: &mut A,
        cache: &TableMetadataCache,
    ) -> impl Future<Output = StorageResult<(Self::Response, Vec<CachePublication>)>> + Send;
}

impl DynamoPointReadShape for ValidatedDynamoReadRequest<GetItemRequest> {
    type Response = GetItemResponse;

    async fn execute<A: GetItemAttempt>(
        self,
        attempt: &mut A,
        cache: &TableMetadataCache,
    ) -> StorageResult<(Self::Response, Vec<CachePublication>)> {
        let (response, publication) = get_item_in_attempt(attempt, cache, &self.0).await?;
        Ok((response, publication.into_iter().collect()))
    }
}

impl DynamoPointReadShape for ValidatedDynamoReadRequest<BatchGetItemRequest> {
    type Response = BatchGetItemResponse;

    fn execute<A: GetItemAttempt>(
        self,
        attempt: &mut A,
        cache: &TableMetadataCache,
    ) -> impl Future<Output = StorageResult<(Self::Response, Vec<CachePublication>)>> + Send {
        batch_get_item_in_attempt(attempt, cache, self.0)
    }
}

impl DynamoPointReadShape for TransactGetPlan {
    type Response = TransactGetItemsResponse;

    fn execute<A: GetItemAttempt>(
        self,
        attempt: &mut A,
        cache: &TableMetadataCache,
    ) -> impl Future<Output = StorageResult<(Self::Response, Vec<CachePublication>)>> + Send {
        transact_get_items_in_attempt(attempt, cache, self)
    }
}
