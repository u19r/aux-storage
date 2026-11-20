//! One catalog-to-Dynamo table description decision.

use storage_types::{
    BoundTableInfo, DescribeTableRequest, DescribeTableResponse, StorageResult, TableDescription,
};

use crate::{CachePublication, ItemReadAttempt, TableMetadataCache};

/// The native attempt supplies a coherent catalog snapshot. Publication is
/// returned separately so the caller can wait for native settlement first.
pub async fn describe_table_in_attempt<A: ItemReadAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    bindings: &crate::RequestTableBindings,
    request: &DescribeTableRequest,
) -> StorageResult<(DescribeTableResponse, Option<CachePublication>)> {
    let (bound, publication) = bindings
        .resolve_table(attempt, cache, &request.table_name)
        .await?;
    let response = describe_table_from_table(attempt, &bound).await?;
    Ok((response, publication))
}

/// Catalog description shares the caller's already bound native snapshot.
pub async fn describe_table_from_table<A: ItemReadAttempt>(
    attempt: &mut A,
    bound: &BoundTableInfo,
) -> StorageResult<DescribeTableResponse> {
    let status = bound.info.table_status.clone();
    let mut description = TableDescription::from_bound_table_info(bound.clone(), status);
    // DescribeTable drops the stream specification once the stream is off.
    if description
        .stream_specification
        .as_ref()
        .is_some_and(|specification| !specification.stream_enabled)
    {
        description.stream_specification = None;
    }
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
