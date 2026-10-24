//! A native lineage root certifies the complete staged inventory after
//! activation.
use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::DurableRecordKey};
use replication_types::{
    configuration::{installed::InstalledTableState, membership::MembershipRevision},
    identity::resource::{ControlOperationId, TableLineage},
};
use storage_types::{ReadProtection, StorageResult};

use super::record::{CatalogLineage, CatalogLineageReference, CatalogReference};
use crate::{
    bootstrap::ReenableHistoryCut, constants::RETAINED_CATALOG_LINEAGE_SUFFIX,
    journal::JournalStore, native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn require_retained_catalog_replay<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        local: &InstalledTableState,
        input: (ControlOperationId, &ReenableHistoryCut),
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let cut = &input.1.cut;
            let key = self.retained_catalog_lineage_key(local.physical, cut.lineage)?;
            let row = self
                .records
                .load(attempt, &[key], ReadProtection::Conflict)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let root: CatalogLineage = self.records.decode(&row)?;
            let reference = CatalogReference {
                operation: input.0,
                ordinal: input.1.ordinal,
            };
            if root.local != local.configuration.table
                || root.physical != local.physical
                || root.lineage != cut.lineage
                || root.membership != cut.membership
                || !(root.candidate.has_reference(&reference)
                    || root
                        .previous
                        .as_ref()
                        .is_some_and(|previous| previous.has_reference(&reference)))
            {
                return Err(corrupt_record());
            }
            self.stage_retained_trim_authority(attempt, local, input)
                .await
        }
        .await;
        settlement.complete(result)
    }

    fn retained_catalog_lineage_key(
        &self,
        physical: storage_types::TableHistoryId,
        lineage: TableLineage,
    ) -> StorageResult<DurableRecordKey> {
        self.identity_key(RETAINED_CATALOG_LINEAGE_SUFFIX, &(physical, lineage))
    }
    pub(super) async fn store_retained_catalog_lineage<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        local: &InstalledTableState,
        input: (ControlOperationId, &ReenableHistoryCut),
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let cut = &input.1.cut;
            let key = self.retained_catalog_lineage_key(local.physical, cut.lineage)?;
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
            let mut root = CatalogLineage {
                local: local.configuration.table,
                physical: local.physical,
                lineage: cut.lineage,
                membership: cut.membership,
                candidate: CatalogLineageReference {
                    first: CatalogReference {
                        operation: input.0,
                        ordinal: input.1.ordinal,
                    },
                    last: input.1.ordinal,
                    count: 0,
                    vector: replication_types::protocol::CanonicalDigest::of_json(&(
                        local.physical,
                        cut.lineage,
                        cut.membership,
                    ))
                    .map_err(|_| corrupt_record())?,
                },
                previous: None,
            };
            if let Some(row) = &row {
                let old: CatalogLineage = self.records.decode(row)?;
                if old.local != root.local
                    || old.physical != root.physical
                    || old.lineage != root.lineage
                    || old.membership != root.membership
                {
                    return Err(corrupt_record());
                }
                if old.candidate.first.operation == input.0 {
                    if input.1.ordinal <= old.candidate.last {
                        return Err(corrupt_record());
                    }
                    root = old;
                } else {
                    root.previous = if self
                        .load_catalog_activation(
                            attempt,
                            &old.candidate.first,
                            storage_types::ReadProtection::Shared,
                        )
                        .await?
                        .is_some()
                    {
                        Some(old.candidate)
                    } else {
                        self.require_cancelled_catalog_candidate(attempt, &old.candidate.first)
                            .await?;
                        old.previous
                    };
                }
            }
            self.stage_retained_trim_authority(attempt, local, input)
                .await?;
            root.candidate.vector = CatalogLineage::fold(root.candidate.vector, input.1)?;
            root.candidate.count = root
                .candidate
                .count
                .checked_add(1)
                .ok_or_else(corrupt_record)?;
            root.candidate.last = input.1.ordinal;
            self.records
                .store(
                    attempt,
                    &[self
                        .records
                        .prepare(key, row.map(|row| row.revision), &root)?],
                )
                .await
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn load_installed_catalog_lineage<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        state: &InstalledTableState,
        scope: (TableLineage, MembershipRevision),
        protection: ReadProtection,
    ) -> StorageResult<(
        CatalogLineage,
        CatalogLineageReference,
        super::record::CatalogActivation,
    )> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let key = self.retained_catalog_lineage_key(state.physical, scope.0)?;
            let row = self
                .records
                .load(attempt, &[key], protection)
                .await?
                .pop()
                .flatten()
                .ok_or(storage_types::StorageEnum::HistoryUnavailable)?;
            let root: CatalogLineage = self.records.decode(&row)?;
            if root.local != state.configuration.table
                || root.physical != state.physical
                || root.lineage != scope.0
            {
                return Err(corrupt_record());
            }
            if root.membership != scope.1 {
                return Err(storage_types::StorageError::validation(
                    "Historical lineage membership does not match",
                ));
            }
            self.load_public_trim_head(attempt, (state, scope.0), protection)
                .await?;
            let (reference, proof) = match self
                .load_catalog_activation(attempt, &root.candidate.first, protection)
                .await?
            {
                Some(proof) => (root.candidate.clone(), proof),
                None => {
                    let reference = root
                        .previous
                        .clone()
                        .ok_or(storage_types::StorageEnum::HistoryUnavailable)?;
                    let proof = self
                        .load_catalog_activation(attempt, &reference.first, protection)
                        .await?
                        .ok_or_else(corrupt_record)?;
                    (reference, proof)
                }
            };
            if !reference.has_reference(&reference.first)
                || reference.count > proof.0.cuts
                || reference.last >= proof.0.cuts
                || reference.last < reference.first.ordinal
            {
                return Err(corrupt_record());
            }
            let definition = self
                .load_catalog_bootstrap(attempt, &proof.1, protection)
                .await?;
            let page = self
                .load_activated_catalog_page(
                    attempt,
                    state,
                    &reference.first,
                    (&proof.0, proof.1, definition, protection),
                )
                .await?;
            if page.cut.cut.lineage != root.lineage || page.cut.cut.membership != root.membership {
                return Err(corrupt_record());
            }
            Ok((root, reference, proof.0))
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) async fn require_installed_history_lineage<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        state: &InstalledTableState,
        scope: (TableLineage, MembershipRevision),
        protection: ReadProtection,
    ) -> StorageResult<()> {
        self.load_installed_catalog_lineage(attempt, state, scope, protection)
            .await?;
        Ok(())
    }
}

impl JournalStore {
    pub(in crate::journal) async fn load_transferred_trim_snapshot<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        incarnation: super::super::public_trim_authority::PublicTrimIncarnation,
        input: (ControlOperationId, u64),
        protection: ReadProtection,
    ) -> StorageResult<crate::history::trim_authority::HistoricalTrimSnapshot> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let reference = CatalogReference {
                operation: input.0,
                ordinal: input.1,
            };
            let (activation, control) = self
                .load_catalog_activation(attempt, &reference, protection)
                .await?
                .ok_or_else(corrupt_record)?;
            let local = control
                .proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?
                .proposed
                .clone();
            let definition = self
                .load_catalog_bootstrap(attempt, &control, protection)
                .await?;
            let page = self
                .load_activated_catalog_page(
                    attempt,
                    &local,
                    &reference,
                    (&activation, control, definition, protection),
                )
                .await?;
            if incarnation != (&local, page.cut.cut.lineage).into() {
                return Err(corrupt_record());
            }
            page.cut.visibility.require_cut(&page.cut.cut)?;
            Ok(page.cut.visibility)
        }
        .await;
        settlement_guard.complete(result)
    }
}
