//! Dynamo TTL policy decisions over one authoritative table attempt.

use std::future::Future;

use storage_common::{TtlConfigRecord, ttl::TtlPolicyChange};
use storage_types::{
    BoundTableInfo, DescribeTimeToLiveRequest, DescribeTimeToLiveResponse, StorageResult,
};

use crate::{CachePublication, CatalogAttempt, TableMetadataCache, TtlCheckpointChange};

/// Only the physical TTL record lookup belongs to a backend family.
pub trait TtlPolicyAttempt: CatalogAttempt + crate::TableUpdateTransaction {
    fn read_ttl_config(
        &mut self,
        table: &BoundTableInfo,
    ) -> impl Future<Output = StorageResult<Option<TtlConfigRecord>>> + Send {
        async move {
            let [config] = self.read_ttl_configs(&[table]).await?.into_array::<1>()?;
            Ok(config)
        }
    }

    /// One observation per bound incarnation, preserving input order.
    fn read_ttl_configs(
        &mut self,
        tables: &[&BoundTableInfo],
    ) -> impl Future<Output = StorageResult<storage_types::Exact<Option<TtlConfigRecord>>>> + Send;

    fn write_ttl_config(
        &mut self,
        table: &BoundTableInfo,
        config: TtlConfigRecord,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    /// Conditionally replace the checkpoint observed in this native attempt.
    /// KV uses its protected policy-record read; SQL compares prior job bytes.
    fn write_ttl_checkpoint(
        &mut self,
        table: &BoundTableInfo,
        change: TtlCheckpointChange,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn install_ttl_policy(
        &mut self,
        table: &BoundTableInfo,
        change: TtlPolicyChange,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        async move {
            let Some(layout) = crate::TableUpdateChange::for_ttl_policy(&table.info, &change)
            else {
                return Ok(());
            };
            let updated = self.persist_table_update(table.clone(), layout).await?;
            match change {
                TtlPolicyChange::Enable(config) | TtlPolicyChange::Disable(config) => {
                    self.write_ttl_config(&updated, config).await
                }
                TtlPolicyChange::Unchanged => unreachable!("unchanged policy has no layout effect"),
            }
        }
    }
}

pub async fn describe_time_to_live_in_attempt<A: TtlPolicyAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    bindings: &crate::RequestTableBindings,
    request: &DescribeTimeToLiveRequest,
) -> StorageResult<(DescribeTimeToLiveResponse, Option<CachePublication>)> {
    let (table, publication) = bindings
        .resolve_table(attempt, cache, &request.table_name)
        .await?;
    let response = describe_time_to_live_from_table(attempt, &table).await?;
    Ok((response, publication))
}

/// A qualified catalog observation joins the TTL record in the caller's
/// snapshot.
pub async fn describe_time_to_live_from_table<A: TtlPolicyAttempt>(
    attempt: &mut A,
    table: &BoundTableInfo,
) -> StorageResult<DescribeTimeToLiveResponse> {
    let config = attempt.read_ttl_config(table).await?;
    Ok(DescribeTimeToLiveResponse {
        time_to_live_description: Some(TtlConfigRecord::describe(config)),
    })
}
