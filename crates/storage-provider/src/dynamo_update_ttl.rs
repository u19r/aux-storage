//! One TTL policy transition under the caller's protected table attempt.

use std::future::Future;

use storage_common::ttl::{TtlConfigRecord, TtlPolicyChange};
use storage_types::{
    BoundTableInfo, StorageResult, TimeToLiveSpecification, TimestampMillis,
    UpdateTimeToLiveResponse,
};

use crate::{DescribeTimeToLiveAttempt, SemanticDecision};

pub trait UpdateTimeToLiveAttempt:
    DescribeTimeToLiveAttempt + crate::TableUpdateTransaction
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
    table: &BoundTableInfo,
    specification: &TimeToLiveSpecification,
    now: TimestampMillis,
) -> StorageResult<SemanticDecision<UpdateTimeToLiveResponse>> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let existing = attempt.read_ttl_config(table).await?;
        let change = match TtlPolicyChange::try_new(
            &table.info.table_name,
            specification,
            existing.as_ref(),
            now,
        )? {
            SemanticDecision::Accepted(change) => change,
            SemanticDecision::Rejected(error) => return Ok(SemanticDecision::Rejected(error)),
        };
        attempt.install_ttl_policy(table, change).await?;
        Ok(SemanticDecision::Accepted(UpdateTimeToLiveResponse {
            time_to_live_specification: specification.clone(),
        }))
    }
    .await;
    settlement.complete(result)
}
