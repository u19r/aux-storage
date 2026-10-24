use infrastructure_provider::record::RecordChange;
use replication_types::protocol::CanonicalDigest;
use storage_types::StorageResult;

use crate::{
    constants::COLLECTED_BASELINE_DEFINITION_SUFFIX,
    history_collection::{
        AncestryCollectionStage, HistoryCollectionChange, HistoryCollectionRecord,
    },
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) fn prepare_collected_ancestry(
        &self,
        record: &mut HistoryCollectionRecord,
        change: &HistoryCollectionChange,
    ) -> StorageResult<RecordChange> {
        match (&record.ancestry, change) {
            (AncestryCollectionStage::Catalog { .. }, HistoryCollectionChange::Catalog(page)) => {
                self.prepare_collected_catalog(record, page)
            }
            (AncestryCollectionStage::Retired { .. }, HistoryCollectionChange::Retired(page)) => {
                self.prepare_collected_retired_page(record, page)
            }
            (AncestryCollectionStage::Baseline, HistoryCollectionChange::Baseline(definition)) => {
                self.prepare_collected_baseline(record, definition)
            }
            _ => Err(corrupt_record()),
        }
    }

    fn prepare_collected_catalog(
        &self,
        record: &mut HistoryCollectionRecord,
        page: &crate::history_capture::RetiredHistoryCatalogPage,
    ) -> StorageResult<RecordChange> {
        let AncestryCollectionStage::Catalog { after } = &record.ancestry else {
            return Err(corrupt_record());
        };
        let request = record.request()?.ok_or_else(corrupt_record)?;
        if page.after != *after {
            return Err(corrupt_record());
        }
        let fact = self.prepare_collected_catalog_page(&request, page)?;
        record.content = fold_content(record.content, page.content)?;
        record.ancestry = match page.entries.first() {
            Some(definition) => AncestryCollectionStage::Retired {
                definition: Box::new(definition.clone()),
                next: 0,
                vector: definition.removal_content()?,
                after: page.continuation.clone(),
            },
            None => AncestryCollectionStage::Baseline,
        };
        Ok(fact)
    }

    fn prepare_collected_retired_page(
        &self,
        record: &mut HistoryCollectionRecord,
        page: &crate::history_capture::RetiredHistoryPage,
    ) -> StorageResult<RecordChange> {
        let AncestryCollectionStage::Retired {
            definition,
            next,
            vector,
            after,
        } = &record.ancestry
        else {
            return Err(corrupt_record());
        };
        definition.require_page(page, *next)?;
        let mut folded = *vector;
        for cut in &page.cuts {
            folded = CanonicalDigest::of_json(&(folded, &cut.origin, cut.through))
                .map_err(|_| corrupt_record())?;
        }
        let fact = self.prepare_collected_retired_index(page, *next)?;
        record.ancestry = match page.next_partition {
            Some(next) => AncestryCollectionStage::Retired {
                definition: definition.clone(),
                next,
                vector: folded,
                after: after.clone(),
            },
            None => {
                if folded != definition.vector() {
                    return Err(corrupt_record());
                }
                let before = record
                    .proposal
                    .participant(&record.proposal.operation().home_region)
                    .map_err(|_| corrupt_record())?
                    .before
                    .as_ref()
                    .ok_or_else(corrupt_record)?;
                let source = definition.source()?;
                if before.sources.iter().any(|known| {
                    known.region == source.region
                        && known.table == source.table
                        && known.database_group == source.database_group
                        && known.generation == source.generation
                        && known.partitions == source.partitions
                }) {
                    record
                        .remaining_retired
                        .retain(|region| region != &source.region);
                }
                record.content = fold_content(record.content, definition.content())?;
                match after {
                    Some(after) => AncestryCollectionStage::Catalog {
                        after: Some(after.clone()),
                    },
                    None => AncestryCollectionStage::Baseline,
                }
            }
        };
        Ok(fact)
    }

    fn prepare_collected_baseline(
        &self,
        record: &mut HistoryCollectionRecord,
        definition: &crate::history_capture::RetainedBaselineDefinition,
    ) -> StorageResult<RecordChange> {
        let request = record.request()?.ok_or_else(corrupt_record)?;
        if !record.remaining_retired.is_empty() {
            return Err(corrupt_record());
        }
        definition.require_request(&request)?;
        if record
            .initial
            .as_ref()
            .is_some_and(|initial| initial.content() != definition.initial().content())
        {
            return Err(corrupt_record());
        }
        let key = self.identity_key(
            COLLECTED_BASELINE_DEFINITION_SUFFIX,
            &(request.proposal.operation().id, &request.region),
        )?;
        let fact = self.records.prepare(key, None, definition)?;
        record.initial = Some(definition.initial().clone());
        record.content = fold_content(record.content, definition.content())?;
        record.source_index = record
            .source_index
            .checked_add(1)
            .ok_or_else(corrupt_record)?;
        record.ancestry = if record.source_index == record.sources()?.len() {
            AncestryCollectionStage::RetainedDefinition
        } else {
            AncestryCollectionStage::Active
        };
        record.remaining_retired = if record.source_index < record.sources()?.len() {
            record.required_retired_regions()?
        } else {
            Vec::new()
        };
        Ok(fact)
    }
}

fn fold_content(
    previous: CanonicalDigest,
    next: CanonicalDigest,
) -> StorageResult<CanonicalDigest> {
    CanonicalDigest::of_json(&(previous, next)).map_err(|_| corrupt_record())
}
