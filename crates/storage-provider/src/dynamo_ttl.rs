//! Dynamo TTL policy decisions over one authoritative table attempt.

use std::future::Future;

use storage_common::TtlConfigRecord;
use storage_types::{
    BoundTableInfo, DescribeTimeToLiveRequest, DescribeTimeToLiveResponse, StorageError,
    StorageResult,
};

use crate::{CachePublication, CatalogAttempt, TableMetadataCache, resolve_table_in_attempt};

/// Only the physical TTL record lookup belongs to a backend family.
pub trait DescribeTimeToLiveAttempt: CatalogAttempt {
    fn read_ttl_config(
        &mut self,
        table: &BoundTableInfo,
    ) -> impl Future<Output = StorageResult<Option<TtlConfigRecord>>> + Send {
        async move {
            self.read_ttl_configs(&[table])
                .await?
                .pop()
                .ok_or_else(|| StorageError::internal("TTL policy observation is missing"))
        }
    }

    /// One observation per bound incarnation, preserving input order.
    fn read_ttl_configs(
        &mut self,
        tables: &[&BoundTableInfo],
    ) -> impl Future<Output = StorageResult<Vec<Option<TtlConfigRecord>>>> + Send;
}

pub async fn describe_time_to_live_in_attempt<A: DescribeTimeToLiveAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    request: &DescribeTimeToLiveRequest,
) -> StorageResult<(DescribeTimeToLiveResponse, Option<CachePublication>)> {
    let (table, publication) =
        resolve_table_in_attempt(attempt, cache, &request.table_name).await?;
    let response = describe_time_to_live_from_table(attempt, &table).await?;
    Ok((response, publication))
}

/// A qualified catalog observation joins the TTL record in the caller's
/// snapshot.
pub async fn describe_time_to_live_from_table<A: DescribeTimeToLiveAttempt>(
    attempt: &mut A,
    table: &BoundTableInfo,
) -> StorageResult<DescribeTimeToLiveResponse> {
    let config = attempt.read_ttl_config(table).await?;
    Ok(DescribeTimeToLiveResponse {
        time_to_live_description: Some(TtlConfigRecord::describe(config)),
    })
}
