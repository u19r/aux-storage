use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::DurableRecord};
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, Revision, StorageResult};

use super::record::{SealPhase, SealRecord};
use crate::{
    constants::TABLE_SEAL_SUFFIX,
    journal::{JournalStore, control_slot::ControlSlotOwner},
    lifecycle::{SealRequest, SealStatus},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn load_table_seal<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &SealRequest,
        protection: ReadProtection,
    ) -> StorageResult<Option<(DurableRecord, SealRecord)>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            request.validate()?;
            if request.region != self.region {
                return Err(corrupt_record());
            }
            self.require_local_source(request.before()?)?;
            let key = self.identity_key(TABLE_SEAL_SUFFIX, &request.proposal.operation().id)?;
            let rows = self.records.load(attempt, &[key], protection).await?;
            let Some(row) = rows.into_iter().next().flatten() else {
                return Ok(None);
            };
            let record: SealRecord = self.records.decode(&row)?;
            if record.content != request.content()? || record.request.content()? != record.content {
                return Err(corrupt_record());
            }
            if let SealPhase::RecordingCuts(next) | SealPhase::Draining(next) = record.phase
                && next >= u32::from(self.partitions)
            {
                return Err(corrupt_record());
            }
            Ok(Some((row, record)))
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn observe_table_seal<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &SealRequest,
    ) -> StorageResult<Option<SealStatus>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            self.load_table_seal(attempt, request, ReadProtection::Snapshot)
                .await?
                .map(|(_, record)| record.status())
                .transpose()
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn store_table_seal<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &SealRecord,
        revision: Option<Revision>,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key =
                self.identity_key(TABLE_SEAL_SUFFIX, &record.request.proposal.operation().id)?;
            self.records
                .store(attempt, &[self.records.prepare(key, revision, record)?])
                .await
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn require_sealing_table<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        request: &SealRequest,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let before = request.before()?;
            let observed = self
                .observe_table_state(attempt, before.configuration.table, ReadProtection::Shared)
                .await?
                .ok_or_else(corrupt_record)?;
            let mut expected = before.clone();
            expected.phase =
                replication_types::configuration::installed::RegionalTablePhase::Sealing;
            if observed.state() != &expected {
                return Err(corrupt_record());
            }
            let slot = self.identity_key(
                crate::constants::CONTROL_SLOT_SUFFIX,
                &before.configuration.table,
            )?;
            let rows = self
                .records
                .load(attempt, &[slot], ReadProtection::Shared)
                .await?;
            let owner = self.decode_control_slot(
                rows[0].as_ref().ok_or_else(corrupt_record)?,
                before.configuration.table,
            )?;
            if owner != ControlSlotOwner::Configuration(request.proposal.operation().id)
                && owner != ControlSlotOwner::Idle
            {
                return Err(corrupt_record());
            }
            if owner == ControlSlotOwner::Idle {
                let control = self
                    .load_control(attempt, &request.proposal, ReadProtection::Shared)
                    .await?
                    .ok_or_else(corrupt_record)?;
                if self.decode_control(&control, &request.proposal)?.decision
                    != crate::control::ControlDecision::Cancelled
                {
                    return Err(corrupt_record());
                }
            }
            let stamp = attempt
                .read_catalog_stamp(&before.name)
                .await?
                .ok_or_else(corrupt_record)?;
            if stamp.history_id != before.physical {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }
}
