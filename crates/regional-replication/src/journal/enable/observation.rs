use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::DurableRecord};
use storage_types::{ReadProtection, StorageResult};

use crate::{
    constants::ENABLE_OPERATION_SUFFIX,
    enable::{EnablePhase, EnablePlan, EnableRecord},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn load_enable<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        plan: &EnablePlan,
        protection: ReadProtection,
    ) -> StorageResult<(Option<DurableRecord>, EnableRecord)> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key = self.identity_key(ENABLE_OPERATION_SUFFIX, &plan.operation.id)?;
            let rows = self.records.load(attempt, &[key], protection).await?;
            let Some(row) = rows.into_iter().next().flatten() else {
                return Ok((
                    None,
                    EnableRecord {
                        content: plan.content()?,
                        plan: plan.clone(),
                        reservations: Vec::new(),
                        proposal: None,
                        phase: EnablePhase::Reserving,
                    },
                ));
            };
            let record = self.decode_enable_record(&row)?;
            if record.content != plan.content()? {
                return Err(corrupt_record());
            }
            Ok((Some(row), record))
        }
        .await;
        settlement.complete(result)
    }

    pub(super) fn decode_enable_record(&self, row: &DurableRecord) -> StorageResult<EnableRecord> {
        let record: EnableRecord = self.records.decode(row)?;
        let plan = &record.plan;
        plan.validate()?;
        if row.key != self.identity_key(ENABLE_OPERATION_SUFFIX, &plan.operation.id)?
            || plan.content()? != record.content
            || record.reservations.len() > plan.targets.len()
        {
            return Err(corrupt_record());
        }
        for (index, state) in record.reservations.iter().enumerate() {
            plan.require_reservation(index, state)?;
        }
        if record.phase == EnablePhase::Reserving
            || (record.proposal.is_none()
                && matches!(
                    record.phase,
                    EnablePhase::Cancelling(_) | EnablePhase::Cancelled
                ))
        {
            if record.proposal.is_some() || record.reservations.len() == plan.targets.len() {
                return Err(corrupt_record());
            }
        } else {
            let proposal = plan.bind_proposal(&record.reservations)?;
            if record.proposal.as_ref() != Some(&proposal) {
                return Err(corrupt_record());
            }
            if let EnablePhase::Bootstrapping(index) = record.phase
                && index as usize >= plan.targets.len()
            {
                return Err(corrupt_record());
            }
        }
        if let EnablePhase::Cancelling(index) = record.phase
            && index as usize > plan.targets.len()
        {
            return Err(corrupt_record());
        }
        Ok(record)
    }

    pub(in crate::journal) async fn require_enable_capture_plan<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &replication_types::configuration::proposal::ControlProposal,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key = self.identity_key(ENABLE_OPERATION_SUFFIX, &proposal.operation().id)?;
            let row = self
                .records
                .load(attempt, &[key], ReadProtection::Conflict)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let record = self.decode_enable_record(&row)?;
            if record.proposal.as_ref() != Some(proposal)
                || !matches!(
                    record.phase,
                    EnablePhase::Bootstrapping(_) | EnablePhase::Controlling
                )
            {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }
}
