//! Subsequent re-enable carries forward installed foreign history in bounded
//! native turns.
use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_types::{ReadProtection, StorageResult};

use super::record::{InventoryPhase, InventoryRecord, RetainedCut};
use crate::{bootstrap::ReenableHistoryCut, journal::JournalStore, native_record::corrupt_record};

impl JournalStore {
    pub(super) async fn begin_inherited_inventory<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        root: &mut InventoryRecord,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let local = root
                .proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?
                .before
                .as_ref()
                .ok_or_else(corrupt_record)?;
            let definition = self
                .load_lineage_bootstrap_definition(
                    attempt,
                    local,
                    storage_types::ReadProtection::Shared,
                )
                .await?;
            root.phase = match definition.retained_inventory()? {
                None => InventoryPhase::Ready,
                Some(definition) => {
                    let vector = definition.inventory_seed()?;
                    let custody = crate::bootstrap::ReenableHistoryCustody {
                        definition,
                        ordinal: 0,
                    };
                    custody.require_custodian(&root.proposal)?;
                    InventoryPhase::Inherited { custody, vector }
                }
            };
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn capture_inherited_inventory<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        root: &mut InventoryRecord,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let InventoryPhase::Inherited { custody, vector } = root.phase.clone() else {
                return Err(corrupt_record());
            };
            let local = root
                .proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?
                .before
                .as_ref()
                .ok_or_else(corrupt_record)?
                .clone();
            let mut page = self
                .load_retained_custody_page(
                    attempt,
                    &root.proposal,
                    &custody,
                    storage_types::ReadProtection::Shared,
                )
                .await?;
            let vector = custody
                .definition
                .require_page(&page, (custody.ordinal, vector))?;
            let next = page.next_ordinal;
            if page.cut.survivor.physical != local.physical
                || page.cut.survivor.configuration.table != local.configuration.table
            {
                page.cut.ordinal = root.cuts;
                page.cut.custody = Some(custody.clone());
                self.store_inherited_inventory_cut(attempt, root, &page.cut)
                    .await?;
            }
            root.phase = match next {
                Some(ordinal) => InventoryPhase::Inherited {
                    custody: crate::bootstrap::ReenableHistoryCustody { ordinal, ..custody },
                    vector,
                },
                None => InventoryPhase::Ready,
            };
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    async fn store_inherited_inventory_cut<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        root: &mut InventoryRecord,
        history: &ReenableHistoryCut,
    ) -> StorageResult<()> {
        self.store_reenable_baseline_inventory(
            attempt,
            root.proposal.operation().id,
            &history.baseline,
        )
        .await?;
        let local = &root
            .proposal
            .participant(&self.region)
            .map_err(|_| corrupt_record())?
            .proposed;
        self.store_retained_catalog_cut(attempt, local, (root.proposal.operation().id, history))
            .await?;
        let key = self.reenable_cut_key(root.proposal.operation().id, history)?;
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
        self.retain_history_epoch(attempt, history.cut.lineage)
            .await?;
        let previous = root.vector;
        root.vector = history.fold_inventory(previous)?;
        root.cuts = root.cuts.checked_add(1).ok_or_else(corrupt_record)?;
        self.records
            .store(
                attempt,
                &[self.records.prepare(
                    key,
                    None,
                    &RetainedCut {
                        previous,
                        operation: root.proposal.operation().id,
                        history: history.clone(),
                    },
                )?],
            )
            .await
    }
}
