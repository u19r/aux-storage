//! One TTL policy transition under the caller's protected table attempt.

use std::future::Future;

use storage_common::ttl::{TtlConfigRecord, TtlPolicyChange};
use storage_types::{
    BoundTableInfo, StorageResult, TimestampMillis, UpdateTimeToLiveRequest,
    UpdateTimeToLiveResponse,
};

use crate::{
    DescribeTimeToLiveAttempt, SemanticDecision, TableMetadataCache,
    dynamo_catalog::resolve_prepared_table_in_attempt,
};

pub trait UpdateTimeToLiveAttempt:
    DescribeTimeToLiveAttempt + crate::TableUpdateTransaction<Catalog = BoundTableInfo>
{
    fn write_ttl_config(
        &mut self,
        table: &BoundTableInfo,
        config: TtlConfigRecord,
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

/// Keep the response tied to the request; the persisted policy may remain
/// `Enabling` until the bounded maintenance job finishes its existing items.
pub async fn update_time_to_live_in_attempt<A: UpdateTimeToLiveAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    request: &UpdateTimeToLiveRequest,
    now: TimestampMillis,
) -> StorageResult<SemanticDecision<UpdateTimeToLiveResponse>> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let (prepared, _) =
            match resolve_prepared_table_in_attempt(attempt, cache, &request.table_name).await? {
                SemanticDecision::Accepted(table) => table,
                SemanticDecision::Rejected(error) => return Ok(SemanticDecision::Rejected(error)),
            };
        let table = prepared.table;
        let existing = attempt.read_ttl_config(&table).await?;
        let change = match TtlPolicyChange::try_new(
            &request.table_name,
            &request.time_to_live_specification,
            existing.as_ref(),
            now,
        )? {
            SemanticDecision::Accepted(change) => change,
            SemanticDecision::Rejected(error) => return Ok(SemanticDecision::Rejected(error)),
        };
        attempt.install_ttl_policy(&table, change).await?;
        Ok(SemanticDecision::Accepted(UpdateTimeToLiveResponse {
            time_to_live_specification: request.time_to_live_specification.clone(),
        }))
    }
    .await;
    settlement.complete(result)
}
