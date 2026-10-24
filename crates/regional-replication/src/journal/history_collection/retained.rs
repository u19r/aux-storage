//! Complete primary inventory folds and immutable operation-scoped original
//! declarations.
use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::RecordChange};
use replication_types::protocol::CanonicalDigest;
use storage_types::StorageResult;

use super::retained_record::{CollectedRetainedCut, CollectedRetainedDefinition};
use crate::{
    bootstrap::{ReenableHistoryCut, ReenableHistoryPage},
    history_collection::{
        AncestryCollectionStage, HistoryCollectionChange, HistoryCollectionRecord,
    },
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) fn prepare_collected_retained(
        &self,
        record: &mut HistoryCollectionRecord,
        change: &HistoryCollectionChange,
    ) -> StorageResult<RecordChange> {
        match (&record.ancestry, change) {
            (
                AncestryCollectionStage::RetainedDefinition,
                HistoryCollectionChange::RetainedDefinition(definition),
            ) => {
                if record.retained.is_some() {
                    return Err(corrupt_record());
                }
                if let Some(definition) = definition {
                    crate::bootstrap::ReenableHistoryCustody {
                        definition: definition.clone(),
                        ordinal: 0,
                    }
                    .require_custodian(&record.proposal)?;
                    record.ancestry = AncestryCollectionStage::RetainedPages {
                        next: 0,
                        vector: definition.inventory_seed()?,
                    };
                    record.vector = record.retained_seed()?;
                } else {
                    record.ancestry = AncestryCollectionStage::Complete;
                }
                record.retained = definition.clone();
                record.content = CanonicalDigest::of_json(&(record.content, definition))
                    .map_err(|_| corrupt_record())?;
                let key = self.identity_key(
                    crate::constants::COLLECTED_RETAINED_DEFINITION_SUFFIX,
                    &record.proposal.operation().id,
                )?;
                self.records.prepare(
                    key,
                    None,
                    &CollectedRetainedDefinition {
                        proposal: record.proposal.clone(),
                        inventory: definition.clone(),
                    },
                )
            }
            (
                AncestryCollectionStage::RetainedPages { next, vector },
                HistoryCollectionChange::RetainedPage(page),
            ) => {
                let next = *next;
                let vector = record
                    .retained
                    .as_ref()
                    .ok_or_else(corrupt_record)?
                    .require_page(page, (next, *vector))?;
                let cut = record.adopt_retained_cut(page)?;
                let previous = record.vector;
                record.vector = cut.fold_inventory(previous)?;
                record.content = CanonicalDigest::of_json(&(record.content, &cut))
                    .map_err(|_| corrupt_record())?;
                record.ancestry = match page.next_ordinal {
                    Some(next) => AncestryCollectionStage::RetainedPages { next, vector },
                    None => AncestryCollectionStage::Complete,
                };
                let key = self.identity_key(
                    crate::constants::COLLECTED_RETAINED_PAGE_SUFFIX,
                    &(record.proposal.operation().id, next),
                )?;
                self.records.prepare(
                    key,
                    None,
                    &CollectedRetainedCut {
                        proposal: record.proposal.clone(),
                        previous,
                        cut,
                    },
                )
            }
            _ => Err(corrupt_record()),
        }
    }

    pub(in crate::journal) async fn load_collected_retained_page<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        definition: &crate::history_collection::HistoryAdoptionDefinition,
        ordinal: u64,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<ReenableHistoryPage> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let inventory = definition
                .retained_inventory()?
                .ok_or_else(corrupt_record)?;
            let key = self.identity_key(
                crate::constants::COLLECTED_RETAINED_PAGE_SUFFIX,
                &(definition.proposal().operation().id, ordinal),
            )?;
            let row = self
                .load_immutable_waves(attempt, std::slice::from_ref(&key), protection)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let fact: CollectedRetainedCut = self.decode_collected_retained_cut(&row)?;
            if fact.proposal != *definition.proposal() || fact.cut.ordinal != ordinal {
                return Err(corrupt_record());
            }
            let next = ordinal.checked_add(1).ok_or_else(corrupt_record)?;
            let page = ReenableHistoryPage {
                definition: inventory.bootstrap,
                cut: fact.cut,
                next_ordinal: (next < inventory.cuts).then_some(next),
            };
            inventory.require_page(&page, (ordinal, fact.previous))?;
            Ok(page)
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn require_native_retained_collection<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &HistoryCollectionRecord,
        change: &HistoryCollectionChange,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if self.region != record.proposal.operation().home_region {
                return Ok(());
            }
            let request = crate::history_capture::HistoryCaptureRequest {
                proposal: record.proposal.clone(),
                region: self.region.clone(),
            };
            match change {
                HistoryCollectionChange::RetainedDefinition(definition) => {
                    let native = self
                        .load_capture_retained_inventory(
                            attempt,
                            &request,
                            storage_types::ReadProtection::Shared,
                        )
                        .await?;
                    if CanonicalDigest::of_json(definition).map_err(|_| corrupt_record())?
                        != CanonicalDigest::of_json(&native).map_err(|_| corrupt_record())?
                    {
                        return Err(corrupt_record());
                    }
                }
                HistoryCollectionChange::RetainedPage(page) => {
                    let native = self
                        .load_capture_retained_page(
                            attempt,
                            &request,
                            page.cut.ordinal,
                            storage_types::ReadProtection::Shared,
                        )
                        .await?;
                    if CanonicalDigest::of_json(page).map_err(|_| corrupt_record())?
                        != CanonicalDigest::of_json(&native).map_err(|_| corrupt_record())?
                    {
                        return Err(corrupt_record());
                    }
                }
                _ => return Err(corrupt_record()),
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }
}

impl HistoryCollectionRecord {
    pub(crate) fn adopt_retained_cut(
        &self,
        page: &ReenableHistoryPage,
    ) -> StorageResult<ReenableHistoryCut> {
        let AncestryCollectionStage::RetainedPages { next, vector } = self.ancestry else {
            return Err(corrupt_record());
        };
        let definition = self.retained.as_ref().ok_or_else(corrupt_record)?;
        definition.require_page(page, (next, vector))?;
        let mut cut = page.cut.clone();
        cut.custody = Some(crate::bootstrap::ReenableHistoryCustody {
            definition: definition.clone(),
            ordinal: next,
        });
        cut.require_scope(&self.proposal)?;
        Ok(cut)
    }
}
