use std::sync::Arc;

use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::DurableRecordKey};
use replication_types::{identity::replica::RegionId, protocol::CanonicalDigest};
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, StorageResult};

use super::original_cursor::{OriginalCursor, OriginalStage};
use crate::{
    bootstrap::{
        AdoptedOriginalPage, AdoptedOriginalScope, BaselineCopyScope, SourceCaptureCut,
        TargetBootstrapDefinition,
    },
    history_collection::HistoryAdoptionDefinition,
    journal::JournalStore,
    native_record::corrupt_record,
};

pub(crate) enum OriginalTraversalWork {
    Pending,
    Transfer(Arc<AdoptedOriginalScope>),
    Complete,
}

enum OriginalSelection {
    Next(OriginalCursor),
    Transfer(Box<AdoptedOriginalScope>, OriginalCursor),
    Complete,
}

impl JournalStore {
    pub(crate) async fn plan_original_traversal<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        scope: &BaselineCopyScope,
    ) -> StorageResult<OriginalTraversalWork> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let definition = self.require_original_traversal(attempt, scope).await?;
            let (row, cursor) = self
                .load_original_cursor(attempt, scope, definition.content())
                .await?;
            match self
                .select_original_traversal(attempt, (scope, &definition), &cursor)
                .await?
            {
                OriginalSelection::Next(next) => {
                    self.store_original_cursor(attempt, scope, (row, definition.content(), next))
                        .await?;
                    Ok(OriginalTraversalWork::Pending)
                }
                OriginalSelection::Transfer(original, _) => {
                    Ok(OriginalTraversalWork::Transfer(Arc::new(*original)))
                }
                OriginalSelection::Complete => Ok(OriginalTraversalWork::Complete),
            }
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(crate) async fn advance_original_traversal<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        original: &AdoptedOriginalScope,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let scope = &original.transfer;
            let definition = self.require_original_traversal(attempt, scope).await?;
            let (row, cursor) = self
                .load_original_cursor(attempt, scope, definition.content())
                .await?;
            let OriginalSelection::Transfer(selected, next) = self
                .select_original_traversal(attempt, (scope, &definition), &cursor)
                .await?
            else {
                return Ok(());
            };
            if serde_json::to_vec(&selected).map_err(|_| corrupt_record())?
                != serde_json::to_vec(original).map_err(|_| corrupt_record())?
            {
                return Ok(());
            }
            let covered = self.require_adopted_original(attempt, &selected).await?;
            if covered.through() != selected.cut.through || !covered.pending().is_empty() {
                return Err(corrupt_record());
            }
            self.store_original_cursor(attempt, scope, (row, definition.content(), next))
                .await
        }
        .await;
        settlement_guard.complete(result)
    }

    async fn require_original_traversal<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        scope: &BaselineCopyScope,
    ) -> StorageResult<HistoryAdoptionDefinition> {
        let definition = self
            .load_source_bootstrap_definition(
                attempt,
                &scope.proposal,
                storage_types::ReadProtection::Shared,
            )
            .await?;
        let (_, copy) = self
            .load_source_copy(attempt, (scope, &definition), ReadProtection::Shared)
            .await?;
        if !copy.complete {
            return Err(corrupt_record());
        }
        let TargetBootstrapDefinition::Addition(definition) = definition else {
            return Err(corrupt_record());
        };
        Ok(*definition)
    }

    async fn select_original_traversal<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (&BaselineCopyScope, &HistoryAdoptionDefinition),
        cursor: &OriginalCursor,
    ) -> StorageResult<OriginalSelection> {
        let (scope, definition) = input;
        let before = replication_provider::control::borrow_addition_source(
            &scope.proposal,
            &scope.proposal.operation().home_region,
        )
        .map_err(|_| corrupt_record())?;
        let replication_types::configuration::table::RegionalMode::Global { members, .. } =
            &before.configuration.regional
        else {
            return Err(corrupt_record());
        };
        let Some(supplier) = members.get(cursor.source) else {
            return Ok(OriginalSelection::Complete);
        };
        match &cursor.stage {
            OriginalStage::Active { page, index } => {
                self.select_active_original(
                    attempt,
                    input,
                    (supplier, *page, *index),
                    cursor.source,
                )
                .await
            }
            OriginalStage::Catalog { after } => {
                self.select_original_catalog(
                    attempt,
                    scope,
                    (supplier, after.clone()),
                    cursor.source,
                )
                .await
            }
            OriginalStage::Retired {
                definition: retired,
                page,
                index,
                next_catalog,
            } => {
                self.select_retired_original(
                    attempt,
                    (scope, definition, supplier),
                    (*retired, *page, *index, next_catalog.clone()),
                    cursor.source,
                )
                .await
            }
        }
    }

    async fn select_active_original<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (&BaselineCopyScope, &HistoryAdoptionDefinition),
        selection: (&RegionId, u32, usize),
        source_index: usize,
    ) -> StorageResult<OriginalSelection> {
        let (scope, definition) = input;
        let (supplier, after, index) = selection;
        let page = self
            .load_collected_history_page(
                attempt,
                &scope.proposal,
                (supplier, after),
                storage_types::ReadProtection::Shared,
            )
            .await?;
        let cut = page.cuts.get(index).ok_or_else(corrupt_record)?.clone();
        let installed =
            replication_provider::control::borrow_addition_source(&scope.proposal, supplier)
                .map_err(|_| corrupt_record())?;
        let source = installed
            .sources
            .iter()
            .find(|source| &source.region == supplier)
            .ok_or_else(corrupt_record)?
            .clone();
        let original = AdoptedOriginalScope {
            transfer: scope.clone(),
            definition: definition.clone(),
            page: AdoptedOriginalPage::Active {
                supplier: supplier.clone(),
                after,
            },
            source,
            cut,
        };
        let stage = if index + 1 < page.cuts.len() {
            OriginalStage::Active {
                page: after,
                index: index + 1,
            }
        } else if let Some(page) = page.next_partition {
            OriginalStage::Active { page, index: 0 }
        } else {
            OriginalStage::Catalog { after: None }
        };
        Ok(OriginalSelection::Transfer(
            Box::new(original),
            OriginalCursor {
                source: source_index,
                stage,
            },
        ))
    }

    async fn select_original_catalog<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        scope: &BaselineCopyScope,
        selection: (&RegionId, Option<DurableRecordKey>),
        source: usize,
    ) -> StorageResult<OriginalSelection> {
        let page = self
            .load_collected_retired_catalog(
                attempt,
                &scope.proposal,
                selection,
                storage_types::ReadProtection::Shared,
            )
            .await?;
        let next = if let Some(definition) = page.entries.first() {
            OriginalCursor {
                source,
                stage: OriginalStage::Retired {
                    definition: definition.content(),
                    page: 0,
                    index: 0,
                    next_catalog: page.continuation,
                },
            }
        } else {
            next_source_cursor(source)?
        };
        Ok(OriginalSelection::Next(next))
    }

    async fn select_retired_original<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (&BaselineCopyScope, &HistoryAdoptionDefinition, &RegionId),
        selection: (CanonicalDigest, u32, usize, Option<DurableRecordKey>),
        source_index: usize,
    ) -> StorageResult<OriginalSelection> {
        let (scope, definition, supplier) = input;
        let (retired, after, index, next_catalog) = selection;
        let page = self
            .load_collected_retired_page(
                attempt,
                &scope.proposal,
                (supplier, retired, after),
                storage_types::ReadProtection::Shared,
            )
            .await?;
        let sealed = page.cuts.get(index).ok_or_else(corrupt_record)?;
        let cut = SourceCaptureCut {
            origin: sealed.origin.clone(),
            through: sealed.through,
        };
        let original = AdoptedOriginalScope {
            transfer: scope.clone(),
            definition: definition.clone(),
            page: AdoptedOriginalPage::Retired {
                supplier: supplier.clone(),
                definition: retired,
                after,
            },
            source: page.definition.source()?.clone(),
            cut,
        };
        let next = if index + 1 < page.cuts.len() {
            OriginalCursor {
                source: source_index,
                stage: OriginalStage::Retired {
                    definition: retired,
                    page: after,
                    index: index + 1,
                    next_catalog,
                },
            }
        } else if let Some(page) = page.next_partition {
            OriginalCursor {
                source: source_index,
                stage: OriginalStage::Retired {
                    definition: retired,
                    page,
                    index: 0,
                    next_catalog,
                },
            }
        } else if let Some(after) = next_catalog {
            OriginalCursor {
                source: source_index,
                stage: OriginalStage::Catalog { after: Some(after) },
            }
        } else {
            next_source_cursor(source_index)?
        };
        Ok(OriginalSelection::Transfer(Box::new(original), next))
    }
}

fn next_source_cursor(source: usize) -> StorageResult<OriginalCursor> {
    Ok(OriginalCursor {
        source: source.checked_add(1).ok_or_else(corrupt_record)?,
        stage: OriginalStage::Active { page: 0, index: 0 },
    })
}
