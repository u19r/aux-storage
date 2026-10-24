use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::installed::RegionalTablePhase;
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, Revision, StorageResult};

use super::record::{TargetAdoptionRecord, TargetRecord};
use crate::{
    bootstrap::HiddenTargetRequest,
    constants::CONTROL_SLOT_SUFFIX,
    history_collection::{
        HistoryAdoptionDefinition, HistoryCollectionChange, HistoryCollectionObservation,
        HistoryCollectionRecord,
    },
    journal::{JournalStore, control_slot::ControlSlotOwner},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn begin_target_adoption<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        request: &HiddenTargetRequest,
        definition: &HistoryAdoptionDefinition,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (row, mut root) = self
                .load_hidden_target(attempt, request, ReadProtection::Conflict)
                .await?
                .ok_or_else(corrupt_record)?;
            self.require_adoption_definition(&root, definition)?;
            self.require_adoption_reservation(attempt, &root, ReadProtection::Shared)
                .await?;
            if let Some(adoption) = &root.adoption {
                if adoption.definition.content() != definition.content() {
                    return Err(corrupt_record());
                }
                return adoption.validate();
            }
            if root.reenable.is_some()
                || root.definition.is_some()
                || root.source_vector.is_some()
                || root.next_page != 0
                || root.baselines_complete
                || root.verification.is_some()
            {
                return Err(corrupt_record());
            }
            let collection =
                HistoryCollectionRecord::from_primary_definition(definition.current())?;
            root.adoption = Some(TargetAdoptionRecord {
                definition: definition.clone(),
                collection,
                declared_cuts: 0,
            });
            self.records
                .store(
                    attempt,
                    &[self.records.prepare(row.key, Some(row.revision), &root)?],
                )
                .await
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn observe_target_adoption<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        request: &HiddenTargetRequest,
        definition: &HistoryAdoptionDefinition,
    ) -> StorageResult<Option<HistoryCollectionObservation>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (row, root) = self
                .load_hidden_target(attempt, request, ReadProtection::Snapshot)
                .await?
                .ok_or_else(corrupt_record)?;
            self.require_adoption_definition(&root, definition)?;
            self.require_adoption_reservation(attempt, &root, ReadProtection::Snapshot)
                .await?;
            let Some(adoption) = root.adoption else {
                return Ok(None);
            };
            adoption.validate()?;
            if adoption.definition.content() != definition.content() {
                return Err(corrupt_record());
            }
            Ok(Some(HistoryCollectionObservation {
                revision: row.revision,
                record: adoption.collection,
            }))
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn accept_target_adoption<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        request: &HiddenTargetRequest,
        input: (
            &HistoryAdoptionDefinition,
            Revision,
            &HistoryCollectionChange,
        ),
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (row, mut root) = self
                .load_hidden_target(attempt, request, ReadProtection::Conflict)
                .await?
                .ok_or_else(corrupt_record)?;
            self.require_adoption_definition(&root, input.0)?;
            self.require_adoption_reservation(attempt, &root, ReadProtection::Shared)
                .await?;
            let adoption = root.adoption.as_mut().ok_or_else(corrupt_record)?;
            adoption.validate()?;
            if adoption.definition.content() != input.0.content() {
                return Err(corrupt_record());
            }
            // A concurrent worker or an ambiguous commit may already have
            // advanced this cursor.
            if row.revision != input.1 {
                return Ok(());
            }
            if root.verification.is_some() || root.definition.is_some() {
                return Err(corrupt_record());
            }
            let mut cuts = self
                .prepare_adoption_cuts(attempt, (&root, input.2))
                .await?;
            let retirement = self
                .prepare_adopted_retirement(
                    attempt,
                    (
                        request.operation.id,
                        (
                            root.state.configuration.table,
                            root.state.configuration.lineage,
                        ),
                        input.2,
                    ),
                )
                .await?;
            let adoption = root.adoption.as_mut().ok_or_else(corrupt_record)?;
            let fact = self.prepare_collected_fact(&mut adoption.collection, input.2)?;
            adoption.declared_cuts = adoption
                .declared_cuts
                .checked_add(u64::try_from(cuts.len()).map_err(|_| corrupt_record())?)
                .ok_or_else(corrupt_record)?;
            adoption.validate()?;
            if adoption.collection.is_complete() {
                root.definition = Some(adoption.definition.content());
                root.source_vector = Some(adoption.definition.collection());
            }
            let change = self.records.prepare(row.key, Some(row.revision), &root)?;
            cuts.extend(retirement);
            cuts.extend([fact, change]);
            self.records.store(attempt, &cuts).await
        }
        .await;
        settlement.complete(result)
    }

    pub(super) fn require_adoption_definition(
        &self,
        root: &TargetRecord,
        definition: &HistoryAdoptionDefinition,
    ) -> StorageResult<()> {
        definition.validate()?;
        if root.reenable.is_some() {
            return Err(corrupt_record());
        }
        let proposal = definition.proposal();
        let target = proposal
            .participant(&self.region)
            .map_err(|_| corrupt_record())?;
        let mut active = root.state.clone();
        active.phase = RegionalTablePhase::Active;
        if target.before.is_some()
            || target.proposed != active
            || !root
                .request
                .matches_capture(proposal, definition.current().schema())?
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(super) fn require_target_adoption(
        &self,
        root: &TargetRecord,
        definition: &HistoryAdoptionDefinition,
    ) -> StorageResult<()> {
        self.require_adoption_definition(root, definition)?;
        let adoption = root.adoption.as_ref().ok_or_else(corrupt_record)?;
        adoption.validate()?;
        if adoption.definition.content() != definition.content()
            || !adoption.collection.is_complete()
            || root.definition != Some(definition.content())
            || root.source_vector != Some(definition.collection())
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(super) async fn require_adoption_reservation<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        root: &TargetRecord,
        protection: ReadProtection,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            self.require_hidden_table(attempt, root, protection).await?;
            let key = self.identity_key(CONTROL_SLOT_SUFFIX, &root.state.configuration.table)?;
            let row = self
                .records
                .load(attempt, &[key], protection)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let owner = self.decode_control_slot(&row, root.state.configuration.table)?;
            if owner != ControlSlotOwner::Configuration(root.request.operation.id) {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }
}

impl TargetAdoptionRecord {
    fn validate(&self) -> StorageResult<()> {
        self.definition.require_collection(&self.collection)?;
        if self.collection.is_complete() && self.declared_cuts == 0 {
            return Err(corrupt_record());
        }
        Ok(())
    }
}
