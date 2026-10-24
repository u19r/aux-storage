//! One catalog-to-Dynamo table description decision.

use storage_types::{
    BoundTableInfo, DescribeTableRequest, DescribeTableResponse, StorageResult, TableDescription,
};

use crate::{
    CachePublication, DynamoRangeReadAttempt, TableMetadataCache, resolve_table_in_attempt,
};

/// The native attempt supplies a coherent catalog snapshot. Publication is
/// returned separately so the caller can wait for native settlement first.
pub async fn describe_table_in_attempt<A: DynamoRangeReadAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    request: &DescribeTableRequest,
) -> StorageResult<(DescribeTableResponse, Option<CachePublication>)> {
    let (bound, publication) =
        resolve_table_in_attempt(attempt, cache, &request.table_name).await?;
    let response = describe_table_from_table(attempt, &bound).await?;
    Ok((response, publication))
}

/// Catalog description shares the caller's already bound native snapshot.
pub async fn describe_table_from_table<A: DynamoRangeReadAttempt>(
    attempt: &mut A,
    bound: &BoundTableInfo,
) -> StorageResult<DescribeTableResponse> {
    let table = bound.info.as_ref().clone();
    let status = table.table_status.clone();
    let mut description = TableDescription::from_stored_table_info(table, status);
    if description
        .global_secondary_indexes
        .as_ref()
        .is_some_and(|indexes| !indexes.is_empty())
    {
        attempt
            .read_gsi_readiness(bound, None)
            .await?
            .apply_to_description(&mut description)?;
    }
    Ok(DescribeTableResponse { table: description })
}
