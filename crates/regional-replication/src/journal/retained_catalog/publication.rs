//! Bounded catalog staging joins cut writes; configuration install publishes
//! one root.
use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecordKey, RecordAddress},
};
use replication_types::{
    configuration::{control::ConfigurationChange, installed::InstalledTableState},
    protocol::CanonicalDigest,
};
use storage_types::{ReadProtection, StorageResult};

use super::record::{CatalogActivation, CatalogReference, CatalogSlot};
use crate::{
    bootstrap::ReenableHistoryCut,
    constants::{RETAINED_CATALOG_ACTIVATION_SUFFIX, RETAINED_CATALOG_SUFFIX},
    control::{ControlDecision, ControlRecord},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(in crate::journal) fn retained_catalog_prefix(
        &self,
        physical: storage_types::TableHistoryId,
    ) -> StorageResult<DurableRecordKey> {
        self.identity_key(RETAINED_CATALOG_SUFFIX, &physical)
    }
    pub(super) fn retained_catalog_key(
        &self,
        local: (
            storage_types::TableHistoryId,
            replication_types::identity::resource::TableLineage,
        ),
        source: (
            &replication_types::configuration::installed::RegionalSourceBinding,
            &replication_types::identity::origin::OriginId,
        ),
    ) -> StorageResult<DurableRecordKey> {
        let mut key: RecordAddress = self.retained_catalog_prefix(local.0)?.into();
        key.suffix.extend_from_slice(local.1.as_ref());
        key.suffix.extend_from_slice(
            CanonicalDigest::of_json(&source)
                .map_err(|_| corrupt_record())?
                .as_ref(),
        );
        key.try_into()
    }
    pub(in crate::journal) async fn store_retained_catalog_cut<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        local: &InstalledTableState,
        input: (
            replication_types::identity::resource::ControlOperationId,
            &ReenableHistoryCut,
        ),
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let cut = &input.1.cut;
            input.1.visibility.require_cut(cut)?;
            let mut slot = CatalogSlot {
                local: local.configuration.table,
                physical: local.physical,
                lineage: cut.lineage,
                source: cut.source.clone(),
                origin: cut.origin.clone(),
                candidate: CatalogReference {
                    operation: input.0,
                    ordinal: input.1.ordinal,
                },
                previous: None,
            };
            let key = self.retained_catalog_key(
                (slot.physical, slot.lineage),
                (&slot.source, &slot.origin),
            )?;
            let row = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?
                .pop()
                .flatten();
            if let Some(row) = &row {
                let old: CatalogSlot = self.records.decode(row)?;
                if self
                    .retained_catalog_key((old.physical, old.lineage), (&old.source, &old.origin))?
                    != key
                    || old.local != slot.local
                {
                    return Err(corrupt_record());
                }
                if old.candidate.operation == input.0 {
                    if old.candidate != slot.candidate {
                        return Err(corrupt_record());
                    }
                    self.require_retained_catalog_replay(attempt, local, input)
                        .await?;
                    return Ok(());
                }
                slot.previous = if self
                    .load_catalog_activation(
                        attempt,
                        &old.candidate,
                        storage_types::ReadProtection::Shared,
                    )
                    .await?
                    .is_some()
                {
                    Some(old.candidate)
                } else {
                    self.require_cancelled_catalog_candidate(attempt, &old.candidate)
                        .await?;
                    old.previous
                };
            }
            self.store_retained_catalog_lineage(attempt, local, input)
                .await?;
            self.retain_history_epoch(attempt, cut.lineage).await?;
            self.records
                .store(
                    attempt,
                    &[self
                        .records
                        .prepare(key, row.map(|row| row.revision), &slot)?],
                )
                .await
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) async fn retain_installed_history_catalog<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        control: &ControlRecord,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let proposal = &control.proposal;
            if !matches!(
                proposal.operation().change,
                ConfigurationChange::EnableGlobal { .. } | ConfigurationChange::AddRegion { .. }
            ) {
                return Ok(());
            }
            let definition = self
                .load_catalog_bootstrap(attempt, control, storage_types::ReadProtection::Shared)
                .await?;
            let Some(inventory) = definition.retained_inventory()? else {
                return Ok(());
            };
            if control.bootstrap != Some(inventory.bootstrap) {
                return Err(corrupt_record());
            }
            let local = &proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?
                .proposed;
            let activation = CatalogActivation {
                local: local.configuration.table,
                physical: local.physical,
                operation: proposal.operation().id,
                definition: inventory.bootstrap,
                cuts: inventory.cuts,
            };
            let key =
                self.identity_key(RETAINED_CATALOG_ACTIVATION_SUFFIX, &activation.operation)?;
            let row = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?
                .pop()
                .flatten();
            if let Some(row) = row {
                let old: CatalogActivation = self.records.decode(&row)?;
                if row.revision != storage_types::Revision::INITIAL || old != activation {
                    return Err(corrupt_record());
                }
                return Ok(());
            }
            self.retain_journal_epoch(attempt).await?;
            self.records
                .store(attempt, &[self.records.prepare(key, None, &activation)?])
                .await
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(super) async fn require_cancelled_catalog_candidate<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        reference: &CatalogReference,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key = self.identity_key(
                crate::constants::CONTROL_OPERATION_SUFFIX,
                &reference.operation,
            )?;
            let row = self
                .records
                .load(attempt, &[key], ReadProtection::Shared)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            if self.decode_control_record(&row)?.decision != ControlDecision::Cancelled {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }
}
