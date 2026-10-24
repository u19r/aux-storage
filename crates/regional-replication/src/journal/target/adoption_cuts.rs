use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::RecordChange};
use replication_types::configuration::installed::RegionalSourceBinding;
use storage_types::{ReadProtection, StorageResult};

use super::{cuts::TargetCut, record::TargetRecord};
use crate::{
    bootstrap::SourceCaptureCut, history_collection::HistoryCollectionChange,
    journal::JournalStore, native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn prepare_adoption_cuts<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (&TargetRecord, &HistoryCollectionChange),
    ) -> StorageResult<Vec<RecordChange>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (root, change) = input;
            let adoption = root.adoption.as_ref().ok_or_else(corrupt_record)?;
            match change {
                HistoryCollectionChange::Page(page) => {
                    let definition = adoption
                        .collection
                        .current
                        .as_ref()
                        .ok_or_else(corrupt_record)?;
                    definition.require_page(page, adoption.collection.next_partition)?;
                    let installed = replication_provider::control::borrow_addition_source(
                        &definition.request().proposal,
                        &definition.request().region,
                    )
                    .map_err(|_| corrupt_record())?;
                    let source = installed
                        .sources
                        .iter()
                        .find(|source| source.region == definition.request().region)
                        .ok_or_else(corrupt_record)?;
                    self.prepare_adopted_cut_records(attempt, root, (source, &page.cuts))
                        .await
                }
                HistoryCollectionChange::Retired(page) => {
                    let crate::history_collection::AncestryCollectionStage::Retired {
                        definition,
                        next,
                        ..
                    } = &adoption.collection.ancestry
                    else {
                        return Err(corrupt_record());
                    };
                    definition.require_page(page, *next)?;
                    let source = page.definition.source()?;
                    let cuts = page
                        .cuts
                        .iter()
                        .map(|cut| SourceCaptureCut {
                            origin: cut.origin.clone(),
                            through: cut.through,
                        })
                        .collect::<Vec<_>>();
                    self.prepare_adopted_cut_records(attempt, root, (source, &cuts))
                        .await
                }
                HistoryCollectionChange::RetainedPage(page) => {
                    let history = adoption.collection.adopt_retained_cut(page)?;
                    let key = self.target_cut_key(
                        root.request.operation.id,
                        (
                            history.cut.source.table,
                            history.cut.lineage,
                            &history.cut.origin,
                        ),
                    )?;
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
                    let cut =
                        TargetCut::from_retained(root, &history, adoption.definition.content())?;
                    self.store_retained_catalog_cut(
                        attempt,
                        &root.state,
                        (root.request.operation.id, &history),
                    )
                    .await?;
                    let page_key =
                        self.target_retained_page_key(root.request.operation.id, history.ordinal)?;
                    let page = crate::bootstrap::ReenableHistoryPage {
                        definition: adoption.definition.content(),
                        cut: history,
                        next_ordinal: page.next_ordinal,
                    };
                    self.records
                        .store(attempt, &[self.records.prepare(page_key, None, &page)?])
                        .await?;
                    Ok(vec![self.records.prepare(key, None, &cut)?])
                }
                _ => Ok(Vec::new()),
            }
        }
        .await;
        settlement.complete(result)
    }

    async fn prepare_adopted_cut_records<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        root: &TargetRecord,
        input: (&RegionalSourceBinding, &[SourceCaptureCut]),
    ) -> StorageResult<Vec<RecordChange>> {
        let (source, cuts) = input;
        let definition = root
            .adoption
            .as_ref()
            .ok_or_else(corrupt_record)?
            .definition
            .content();
        let keys = cuts
            .iter()
            .map(|cut| {
                self.target_cut_key(
                    root.request.operation.id,
                    (source.table, root.state.configuration.lineage, &cut.origin),
                )
            })
            .collect::<StorageResult<Vec<_>>>()?;
        let mut changes = Vec::with_capacity(keys.len());
        let rows = self
            .records
            .load(attempt, &keys, ReadProtection::Conflict)
            .await?;
        for ((key, cut), row) in keys.into_iter().zip(cuts).zip(rows) {
            if let Some(row) = row {
                let existing: TargetCut = self.records.decode(&row)?;
                existing.require_declaration(
                    root,
                    (source, cut, definition),
                    self.records.limits.max_records(),
                )?;
            } else {
                let record =
                    TargetCut::from_declaration(root, source.clone(), cut.clone(), definition)?;
                changes.push(self.records.prepare(key, None, &record)?);
            }
        }
        Ok(changes)
    }
}
