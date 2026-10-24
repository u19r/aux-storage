use std::sync::Arc;

use replication_types::configuration::{control::ConfigurationChange, proposal::ControlProposal};
use storage_common::ttl::TtlPolicyChange;
use storage_provider::{DescribeTimeToLiveAttempt, UpdateTimeToLiveAttempt};
use storage_types::{BoundTableInfo, StorageResult};

use crate::{journal::JournalStore, native_record::corrupt_record};

impl JournalStore {
    pub(super) async fn prepare_ttl_policy<
        A: DescribeTimeToLiveAttempt
            + infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
    ) -> StorageResult<(Arc<BoundTableInfo>, TtlPolicyChange)> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let ConfigurationChange::TtlPolicy {
                definition,
                requested_at,
            } = &proposal.operation().change
            else {
                return Err(corrupt_record());
            };
            let participant = proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?;
            let state = participant.before.as_ref().ok_or_else(corrupt_record)?;
            let stamp = attempt
                .read_catalog_stamp(&state.name)
                .await?
                .ok_or_else(corrupt_record)?;
            if stamp.history_id != state.physical {
                return Err(corrupt_record());
            }
            let table = attempt.read_table_at_stamp(&state.name, stamp).await?;
            let existing = attempt.read_ttl_config(&table).await?;
            let change = TtlPolicyChange::try_new(
                &state.name,
                &definition.0,
                existing.as_ref(),
                *requested_at,
            )?
            .into_result()?;
            Ok((table, change))
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn install_ttl_policy<
        A: UpdateTimeToLiveAttempt + infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if !matches!(
                proposal.operation().change,
                ConfigurationChange::TtlPolicy { .. }
            ) {
                return Ok(());
            }
            let (table, change) = self.prepare_ttl_policy(attempt, proposal).await?;
            attempt.install_ttl_policy(&table, change).await
        }
        .await;
        settlement.complete(result)
    }
}
