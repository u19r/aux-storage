//! Historical scope qualification resolves actual installed native inventory.
use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::DurableRecordKey};
use replication_types::configuration::installed::InstalledTableState;
use storage_types::{ReadProtection, StorageResult};

use super::record::{CatalogActivation, CatalogReference, CatalogSlot};
use crate::{
    bootstrap::{ReenableHistoryPage, TargetBootstrapDefinition},
    constants::{CONTROL_OPERATION_SUFFIX, RETAINED_CATALOG_ACTIVATION_SUFFIX},
    control::{ControlDecision, ControlRecord},
    history::source_selection::CanonicalSourceSelection,
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn load_catalog_activation<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        reference: &CatalogReference,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<Option<(CatalogActivation, ControlRecord)>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let keys = [
                self.identity_key(RETAINED_CATALOG_ACTIVATION_SUFFIX, &reference.operation)?,
                self.identity_key(CONTROL_OPERATION_SUFFIX, &reference.operation)?,
            ];
            let rows = self.records.load(attempt, &keys, protection).await?;
            let Some(row) = &rows[0] else {
                return Ok(None);
            };
            let activation: CatalogActivation = self.records.decode(row)?;
            let control =
                self.decode_control_record(rows[1].as_ref().ok_or_else(corrupt_record)?)?;
            let local = &control
                .proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?
                .proposed;
            if row.revision != storage_types::Revision::INITIAL
                || activation.cuts == 0
                || activation.operation != reference.operation
                || control.proposal.operation().id != reference.operation
                || activation.local != local.configuration.table
                || activation.physical != local.physical
                || control.bootstrap != Some(activation.definition)
                || reference.ordinal >= activation.cuts
            {
                return Err(corrupt_record());
            }
            if !matches!(
                control.decision,
                ControlDecision::Installed | ControlDecision::Completed
            ) {
                return Err(corrupt_record());
            }
            Ok(Some((activation, control)))
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn load_installed_retained_scope<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        state: &InstalledTableState,
        selection: &CanonicalSourceSelection,
        protection: ReadProtection,
    ) -> StorageResult<crate::history::source_cut::CanonicalSourceCut> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let key = self.retained_catalog_key(
                (state.physical, selection.lineage),
                (&selection.source, &selection.origin),
            )?;
            let page = self
                .load_installed_catalog_entry(attempt, state, &key, protection)
                .await?
                .ok_or(storage_types::StorageEnum::HistoryUnavailable)?;
            page.cut.cut.require_selection(selection)?;
            Ok(page.cut.cut)
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) async fn load_installed_catalog_entry<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        state: &InstalledTableState,
        key: &DurableRecordKey,
        protection: ReadProtection,
    ) -> StorageResult<Option<ReenableHistoryPage>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let Some((slot, reference, activation, control)) = self
                .load_catalog_slot_reference(attempt, state, key, protection)
                .await?
            else {
                return Ok(None);
            };
            let definition = self
                .load_catalog_bootstrap(attempt, &control, protection)
                .await?;
            let page = self
                .load_activated_catalog_page(
                    attempt,
                    state,
                    &reference,
                    (&activation, control, definition, protection),
                )
                .await?;
            if page.cut.cut.lineage != slot.lineage
                || page.cut.cut.source != slot.source
                || page.cut.cut.origin != slot.origin
            {
                return Err(corrupt_record());
            }
            Ok(Some(page))
        }
        .await;
        settlement_guard.complete(result)
    }

    async fn load_catalog_slot_reference<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        state: &InstalledTableState,
        key: &DurableRecordKey,
        protection: ReadProtection,
    ) -> StorageResult<
        Option<(
            CatalogSlot,
            CatalogReference,
            CatalogActivation,
            ControlRecord,
        )>,
    > {
        let Some(row) = self
            .records
            .load(attempt, std::slice::from_ref(key), protection)
            .await?
            .pop()
            .flatten()
        else {
            return Ok(None);
        };
        let slot: CatalogSlot = self.records.decode(&row)?;
        if slot.local != state.configuration.table
            || slot.physical != state.physical
            || self
                .retained_catalog_key((slot.physical, slot.lineage), (&slot.source, &slot.origin))?
                != *key
        {
            return Err(corrupt_record());
        }
        let (reference, activation, control) = match self
            .load_catalog_activation(attempt, &slot.candidate, protection)
            .await?
        {
            Some((activation, control)) => (slot.candidate.clone(), activation, control),
            None => match &slot.previous {
                Some(reference) => {
                    let (activation, control) = self
                        .load_catalog_activation(attempt, reference, protection)
                        .await?
                        .ok_or_else(corrupt_record)?;
                    (reference.clone(), activation, control)
                }
                None => return Ok(None),
            },
        };
        if activation.local != state.configuration.table || activation.physical != state.physical {
            return Err(corrupt_record());
        }
        Ok(Some((slot, reference, activation, control)))
    }

    pub(in crate::journal) async fn load_retained_custody_page<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &replication_types::configuration::proposal::ControlProposal,
        custody: &crate::bootstrap::ReenableHistoryCustody,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<ReenableHistoryPage> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            custody.require_custodian(proposal)?;
            let local = &proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?
                .before
                .as_ref()
                .ok_or_else(corrupt_record)?;
            let reference = CatalogReference {
                operation: custody.definition.proposal.operation().id,
                ordinal: custody.ordinal,
            };
            let proof = self
                .load_catalog_activation(attempt, &reference, protection)
                .await?
                .ok_or_else(corrupt_record)?;
            if proof.0.definition != custody.definition.bootstrap
                || proof.0.cuts != custody.definition.cuts
            {
                return Err(corrupt_record());
            }
            let definition = self
                .load_catalog_bootstrap(attempt, &proof.1, protection)
                .await?;
            custody.definition.require_native_inventory(
                &definition
                    .retained_inventory()?
                    .ok_or_else(corrupt_record)?,
            )?;
            self.load_activated_catalog_page(
                attempt,
                local,
                &reference,
                (&proof.0, proof.1, definition, protection),
            )
            .await
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(super) async fn load_activated_catalog_page<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        state: &InstalledTableState,
        reference: &CatalogReference,
        proof: (
            &CatalogActivation,
            ControlRecord,
            TargetBootstrapDefinition,
            ReadProtection,
        ),
    ) -> StorageResult<ReenableHistoryPage> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (activation, control, definition, protection) = proof;
            if activation.local != state.configuration.table
                || activation.physical != state.physical
            {
                return Err(corrupt_record());
            }
            let inventory = definition
                .retained_inventory()?
                .ok_or_else(corrupt_record)?;
            if inventory.bootstrap != activation.definition || inventory.cuts != activation.cuts {
                return Err(corrupt_record());
            }
            let is_target = control
                .proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?
                .before
                .is_none();
            let page = if is_target {
                self.load_target_retained_catalog_page(
                    attempt,
                    &control.proposal,
                    reference.ordinal,
                    protection,
                )
                .await?
            } else {
                match definition {
                    TargetBootstrapDefinition::Addition(definition) => {
                        self.load_collected_retained_page(
                            attempt,
                            &definition,
                            reference.ordinal,
                            protection,
                        )
                        .await?
                    }
                    TargetBootstrapDefinition::Reenable(definition) => {
                        self.load_reenable_inventory_page(
                            attempt,
                            &control.proposal,
                            (&definition, reference.ordinal),
                            protection,
                        )
                        .await?
                    }
                    _ => return Err(corrupt_record()),
                }
            };
            if page.definition != activation.definition || page.cut.ordinal != reference.ordinal {
                return Err(corrupt_record());
            }
            Ok(page)
        }
        .await;
        settlement.complete(result)
    }
}

impl JournalStore {
    pub(in crate::journal) async fn load_catalog_bootstrap<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        control: &ControlRecord,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<TargetBootstrapDefinition> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let participant = control
                .proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?;
            if participant.before.is_none() {
                return self
                    .load_retained_bootstrap_definition(attempt, &control.proposal, protection)
                    .await;
            }
            match control.proposal.operation().change {
                replication_types::configuration::control::ConfigurationChange::AddRegion {
                    ..
                } => self
                    .load_completed_history_definition(attempt, &control.proposal, protection)
                    .await?
                    .map(TargetBootstrapDefinition::from)
                    .ok_or_else(corrupt_record),
                replication_types::configuration::control::ConfigurationChange::EnableGlobal {
                    ..
                } if control.proposal.operation().home_region == self.region => {
                    self.load_enable_bootstrap_definition(attempt, &control.proposal, protection)
                        .await
                }
                _ => Err(corrupt_record()),
            }
        }
        .await;
        settlement.complete(result)
    }
}
