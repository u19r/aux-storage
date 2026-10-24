//! One GetItem decision over a native SQL or KV read attempt.

use std::future::Future;

use storage_types::{
    AttributeMap, BoundTableInfo, GetItemRequest, GetItemResponse, KeyAttributes, StorageError,
    StorageResult, WireItem, project_attribute_map, read_key_validation_error,
    validate_transact_key,
};

use crate::{CachePublication, CatalogAttempt, TableMetadataCache, resolve_table_in_attempt};

/// Only physical point lookup belongs to the SQL or KV family.
pub trait GetItemAttempt: CatalogAttempt {
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
    ) -> impl Future<Output = StorageResult<Vec<Option<WireItem>>>> + Send;
}

/// Bind metadata and the item within one coherent native read attempt.
/// Projection and response shaping remain shared Dynamo policy.
pub async fn get_item_in_attempt<A: GetItemAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    request: &GetItemRequest,
) -> StorageResult<(GetItemResponse, Option<CachePublication>)> {
    let (table, publication) =
        resolve_table_in_attempt(attempt, cache, &request.table_name).await?;
    let response = get_item_from_table(attempt, &table, request).await?;
    Ok((response, publication))
}

/// Execute against metadata already resolved in the caller's native snapshot.
/// The caller owns the request's table binding; item decisions stay shared.
pub async fn get_item_from_table<A: GetItemAttempt>(
    attempt: &mut A,
    table: &BoundTableInfo,
    request: &GetItemRequest,
) -> StorageResult<GetItemResponse> {
    if request.key.len() != table.info.key_schema.len() {
        return Err(StorageError::raw_validation(
            "The provided key element does not match the schema",
        ));
    }
    validate_transact_key(&table.info, &request.key).map_err(read_key_validation_error)?;
    let mut item = attempt.read_primary_item(table, &request.key).await?;
    if request.projection_expression.is_some() || request.attributes_to_get.is_some() {
        item = item
            .map(AttributeMap::try_from)
            .transpose()?
            .map(|item| {
                project_attribute_map(
                    item,
                    request.projection_expression.as_deref(),
                    request.attributes_to_get.as_deref(),
                    request.expression_attribute_names.as_ref(),
                )
            })
            .map(WireItem::try_from)
            .transpose()?;
    }
    Ok(GetItemResponse { item })
}
