use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::proposal::ControlProposal;
use storage_provider::DynamoAttempt;
use storage_types::{ReadProtection, StorageResult, TimestampMillis};

use super::record::SealPhase;
use crate::{
    control::ControlDecision, journal::JournalStore, lifecycle::SealRequest,
    native_record::corrupt_record,
};

impl JournalStore {
    /// Global completion certifies every installation, including survivor seal
    /// vectors. Native deletion only retires this exact physical incarnation;
    /// the ordinary reclamation job remains the single physical cleanup owner.
    pub(crate) async fn retire_removed_table<A: PhysicalRecordAttempt + DynamoAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        now: TimestampMillis,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let request = SealRequest::try_new(proposal.clone(), self.region.clone())?;
            let control = self
                .load_control(attempt, proposal, ReadProtection::Shared)
                .await?
                .ok_or_else(corrupt_record)?;
            let control = self.decode_control(&control, proposal)?;
            if control.decision != ControlDecision::Completed {
                return Err(corrupt_record());
            }
            self.require_retirement_evidence(attempt, proposal, &control.retirement)
                .await?;
            let (row, mut root) = self
                .load_table_seal(attempt, &request, ReadProtection::Conflict)
                .await?
                .ok_or_else(corrupt_record)?;
            if matches!(root.phase, SealPhase::Retired) {
                return Ok(());
            }
            if !matches!(root.phase, SealPhase::Removed) {
                return Err(corrupt_record());
            }
            let expected = &proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?
                .proposed;
            let state = self
                .observe_table_state(
                    attempt,
                    expected.configuration.table,
                    ReadProtection::Conflict,
                )
                .await?
                .ok_or_else(corrupt_record)?;
            if state.state() != expected {
                return Err(corrupt_record());
            }
            let stamp = attempt
                .read_catalog_stamp(&expected.name)
                .await?
                .ok_or_else(corrupt_record)?;
            if stamp.history_id != expected.physical {
                return Err(corrupt_record());
            }
            let table = attempt.read_table_at_stamp(&expected.name, stamp).await?;
            attempt.delete_table_layout(&table, now).await?;
            root.phase = SealPhase::Retired;
            self.store_table_seal(attempt, &root, Some(row.revision))
                .await
        }
        .await;
        settlement.complete(result)
    }
}
