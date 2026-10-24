use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::RecordAddress};
use replication_types::{configuration::proposal::ControlProposal, protocol::CanonicalDigest};
use storage_types::{ReadProtection, StorageResult};

use super::record::{InventoryPhase, InventoryRecord, RetainedLineage};
use crate::{
    constants::REENABLE_INVENTORY_SUFFIX, control::ControlDecision, journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(in crate::journal) async fn begin_reenable_inventory<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let before = proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?
                .before
                .as_ref()
                .ok_or_else(corrupt_record)?;
            if u64::from(before.configuration.config) == 0 {
                return Ok(());
            }
            let previous = self
                .load_configuration_control(attempt, before, ReadProtection::Shared)
                .await?;
            if previous.decision != ControlDecision::Completed {
                return Err(corrupt_record());
            }
            let lineage = RetainedLineage {
                disable: previous.proposal,
                survivor: before.clone(),
            };
            self.require_retained_lineage(proposal, &lineage)?;
            let key = self.retained_lineage_key(&lineage)?;
            let existing = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?
                .pop()
                .flatten();
            if let Some(row) = &existing {
                let retained: RetainedLineage = self.records.decode(row)?;
                if retained.disable != lineage.disable || retained.survivor != lineage.survivor {
                    return Err(corrupt_record());
                }
            } else {
                self.records
                    .store(attempt, &[self.records.prepare(key, None, &lineage)?])
                    .await?;
            }
            let key = self.identity_key(REENABLE_INVENTORY_SUFFIX, &proposal.operation().id)?;
            if self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?[0]
                .is_some()
            {
                return Err(corrupt_record());
            }
            let vector = CanonicalDigest::of_json(&(proposal.operation().content, before.physical))
                .map_err(|_| corrupt_record())?;
            let record = InventoryRecord {
                proposal: proposal.clone(),
                phase: InventoryPhase::Lineages { after: None },
                vector,
                cuts: 0,
            };
            self.records
                .store(attempt, &[self.records.prepare(key, None, &record)?])
                .await?;
            self.retain_reenable_custody(attempt, &record).await
        }
        .await;
        settlement.complete(result)
    }

    pub(super) fn retained_lineage_key(
        &self,
        lineage: &RetainedLineage,
    ) -> StorageResult<infrastructure_provider::record::DurableRecordKey> {
        let mut key: RecordAddress = self
            .retained_lineage_prefix(lineage.survivor.configuration.table)?
            .into();
        key.suffix
            .extend_from_slice(lineage.survivor.configuration.lineage.as_ref());
        key.try_into()
    }
}
