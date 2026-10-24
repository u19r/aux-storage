//! Ordered native declarations and immutable replay receipts for old lineages.
use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::protocol::CanonicalDigest;
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, StorageResult};

use super::{
    cuts::TargetCut,
    record::{TargetRecord, TargetRetainedHistory},
};
use crate::{
    bootstrap::{
        HiddenTargetRequest, ReenableHistoryDefinition, ReenableHistoryPage,
        TargetRetainedHistoryObservation,
    },
    constants::TARGET_RETAINED_PAGE_SUFFIX,
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn begin_target_retained_history<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        request: &HiddenTargetRequest,
        definition: &ReenableHistoryDefinition,
    ) -> StorageResult<TargetRetainedHistoryObservation> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (row, mut root) = self
                .load_hidden_target(attempt, request, ReadProtection::Conflict)
                .await?
                .ok_or_else(corrupt_record)?;
            self.require_reenable_definition(&root, definition)?;
            self.require_adoption_reservation(attempt, &root, ReadProtection::Shared)
                .await?;
            if root.reenable.is_some() {
                return self.observe_retained_root(&root, definition);
            }
            if root.adoption.is_some()
                || root.definition.is_some()
                || root.next_page != 0
                || root.source_vector.is_some()
                || root.baselines_complete
                || root.verification.is_some()
            {
                return Err(corrupt_record());
            }
            root.reenable = Some(TargetRetainedHistory {
                definition: definition.clone(),
                next_ordinal: 0,
                vector: definition.inventory_seed()?,
            });
            let observation = self.observe_retained_root(&root, definition)?;
            self.records
                .store(
                    attempt,
                    &[self.records.prepare(row.key, Some(row.revision), &root)?],
                )
                .await?;
            Ok(observation)
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn observe_target_retained_history<
        A: PhysicalRecordAttempt + CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        request: &HiddenTargetRequest,
        definition: &ReenableHistoryDefinition,
    ) -> StorageResult<Option<TargetRetainedHistoryObservation>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (_, root) = self
                .load_hidden_target(attempt, request, ReadProtection::Shared)
                .await?
                .ok_or_else(corrupt_record)?;
            self.require_reenable_definition(&root, definition)?;
            self.require_adoption_reservation(attempt, &root, ReadProtection::Shared)
                .await?;
            if root.reenable.is_none() {
                return Ok(None);
            }
            self.observe_retained_root(&root, definition).map(Some)
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn accept_target_retained_page<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        request: &HiddenTargetRequest,
        input: (&ReenableHistoryDefinition, &ReenableHistoryPage),
    ) -> StorageResult<TargetRetainedHistoryObservation> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (row, mut root) = self
                .load_hidden_target(attempt, request, ReadProtection::Conflict)
                .await?
                .ok_or_else(corrupt_record)?;
            self.require_reenable_definition(&root, input.0)?;
            self.require_adoption_reservation(attempt, &root, ReadProtection::Shared)
                .await?;
            let current = self.observe_retained_root(&root, input.0)?;
            let key = self.target_retained_page_key(request.operation.id, input.1.cut.ordinal)?;
            let existing = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?
                .pop()
                .flatten();
            if let Some(existing) = existing {
                let stored: ReenableHistoryPage = self.records.decode(&existing)?;
                if CanonicalDigest::of_json(&stored).map_err(|_| corrupt_record())?
                    != CanonicalDigest::of_json(input.1).map_err(|_| corrupt_record())?
                    || input.1.cut.ordinal >= current.next_ordinal
                {
                    return Err(corrupt_record());
                }
                return Ok(current);
            }
            if root.verification.is_some() || root.baselines_complete || root.next_page != 0 {
                return Err(corrupt_record());
            }
            let vector = input
                .0
                .require_page(input.1, (current.next_ordinal, current.vector))?;
            let cut_key = self.target_cut_key(
                request.operation.id,
                (
                    input.1.cut.cut.source.table,
                    input.1.cut.cut.lineage,
                    &input.1.cut.cut.origin,
                ),
            )?;
            if self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&cut_key),
                    ReadProtection::Conflict,
                )
                .await?[0]
                .is_some()
            {
                return Err(corrupt_record());
            }
            let cut = TargetCut::from_retained(&root, &input.1.cut, input.0.content())?;
            self.store_retained_catalog_cut(
                attempt,
                &root.state,
                (request.operation.id, &input.1.cut),
            )
            .await?;
            let retained = root.reenable.as_mut().ok_or_else(corrupt_record)?;
            retained.vector = vector;
            retained.next_ordinal = retained
                .next_ordinal
                .checked_add(1)
                .ok_or_else(corrupt_record)?;
            if retained.next_ordinal == input.0.cuts() {
                root.definition = Some(input.0.content());
                root.source_vector = Some(input.0.initial().vector());
            }
            let observation = self.observe_retained_root(&root, input.0)?;
            self.records
                .store(
                    attempt,
                    &[
                        self.records.prepare(key, None, input.1)?,
                        self.records.prepare(cut_key, None, &cut)?,
                        self.records.prepare(row.key, Some(row.revision), &root)?,
                    ],
                )
                .await?;
            Ok(observation)
        }
        .await;
        settlement.complete(result)
    }

    fn require_reenable_definition(
        &self,
        root: &TargetRecord,
        definition: &ReenableHistoryDefinition,
    ) -> StorageResult<()> {
        definition.validate()?;
        let proposal = definition.initial().proposal();
        let participant = proposal
            .participant(&self.region)
            .map_err(|_| corrupt_record())?;
        let mut expected = participant.proposed.clone();
        expected.phase = replication_types::configuration::installed::RegionalTablePhase::Joining;
        if root.adoption.is_some()
            || participant.before.is_some()
            || expected != root.state
            || !root.request.matches_definition(definition.initial())?
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    fn observe_retained_root(
        &self,
        root: &TargetRecord,
        definition: &ReenableHistoryDefinition,
    ) -> StorageResult<TargetRetainedHistoryObservation> {
        let retained = root.reenable.as_ref().ok_or_else(corrupt_record)?;
        if retained.definition.content() != definition.content()
            || retained.next_ordinal > definition.cuts()
            || (retained.next_ordinal == 0 && retained.vector != definition.inventory_seed()?)
            || (retained.next_ordinal == definition.cuts()
                && retained.vector != definition.inventory())
        {
            return Err(corrupt_record());
        }
        Ok(TargetRetainedHistoryObservation {
            definition: definition.content(),
            next_ordinal: retained.next_ordinal,
            vector: retained.vector,
        })
    }

    pub(super) fn require_target_retained_history(
        &self,
        root: &TargetRecord,
        definition: &ReenableHistoryDefinition,
    ) -> StorageResult<()> {
        self.require_reenable_definition(root, definition)?;
        let observation = self.observe_retained_root(root, definition)?;
        if observation.next_ordinal != definition.cuts()
            || root.definition != Some(definition.content())
            || root.source_vector != Some(definition.initial().vector())
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(super) fn target_retained_page_key(
        &self,
        operation: replication_types::identity::resource::ControlOperationId,
        ordinal: u64,
    ) -> StorageResult<infrastructure_provider::record::DurableRecordKey> {
        self.identity_key(TARGET_RETAINED_PAGE_SUFFIX, &(operation, ordinal))
    }

    pub(super) async fn load_retained_import_authority<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        root: &TargetRecord,
        input: (
            &TargetCut,
            u64,
            replication_types::configuration::membership::MembershipRevision,
        ),
    ) -> StorageResult<crate::import::ImportAuthority> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let retained = root.retained_inventory()?;
            let key = self.target_retained_page_key(root.request.operation.id, input.1)?;
            let row = self
                .records
                .load(attempt, &[key], ReadProtection::Shared)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let page: ReenableHistoryPage = self.records.decode(&row)?;
            self.require_target_retained_baselines(
                attempt,
                root,
                &page,
                storage_types::ReadProtection::Shared,
            )
            .await?;
            input.0.require_retained_declaration(
                root,
                &page.cut,
                self.records.limits.max_records(),
            )?;
            if page.definition != retained.bootstrap
                || page.cut.ordinal != input.1
                || page.cut.cut.membership != input.2
            {
                return Err(corrupt_record());
            }
            Ok(crate::import::ImportAuthority::Retained(
                page.cut.survivor.schemas,
            ))
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn verify_target_retained_history<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        root: &TargetRecord,
        input: (u64, &mut crate::bootstrap::VerificationProgress),
        encoding: (
            &replication_provider::canonical_event_codec::CanonicalJsonCodec,
            &replication_provider::runtime_configuration::limits::WorkLimits,
        ),
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let retained = self.require_target_retained_inventory(root)?;
            let key = self.target_retained_page_key(root.request.operation.id, input.0)?;
            let row = self
                .records
                .load(attempt, &[key], ReadProtection::Shared)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let page: ReenableHistoryPage = self.records.decode(&row)?;
            let vector = retained.require_page(&page, (input.0, input.1.vector))?;
            if !self
                .require_target_retained_baselines(
                    attempt,
                    root,
                    &page,
                    storage_types::ReadProtection::Shared,
                )
                .await?
            {
                input.1.stage = crate::bootstrap::VerificationStage::RetainedBaselines {
                    ordinal: input.0,
                    after: None,
                    count: 0,
                    vector: page.cut.baseline.seed()?,
                };
                return Ok(());
            }
            let cut_key = self.target_cut_key(
                root.request.operation.id,
                (
                    page.cut.cut.source.table,
                    page.cut.cut.lineage,
                    &page.cut.cut.origin,
                ),
            )?;
            let cut_row = self
                .records
                .load(attempt, &[cut_key], ReadProtection::Shared)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let cut: TargetCut = self.records.decode(&cut_row)?;
            cut.require_retained_declaration(root, &page.cut, self.records.limits.max_records())?;
            cut.require_complete_coverage()?;
            let selected = &page.cut.cut;
            let selection = crate::history::source_selection::CanonicalSourceSelection {
                serving_region: self.region.clone(),
                serving_group: self.group,
                table: root.state.name.clone(),
                lineage: selected.lineage,
                membership: selected.membership,
                source: selected.source.clone(),
                origin: selected.origin.clone(),
            };
            let terminal = self
                .load_history_terminal_manifest(
                    attempt,
                    (&selection, selected.through),
                    (encoding.1, encoding.0),
                )
                .await?;
            if terminal != selected.terminal_manifest {
                return Err(corrupt_record());
            }
            input.1.cut_count = input
                .1
                .cut_count
                .checked_add(1)
                .ok_or_else(corrupt_record)?;
            input.1.history = CanonicalDigest::of_json(&(input.1.history, &page))
                .map_err(|_| corrupt_record())?;
            input.1.vector = vector;
            input.1.stage = match page.next_ordinal {
                Some(next) => crate::bootstrap::VerificationStage::RetainedCuts(next),
                None => match &root.adoption {
                    Some(adoption) => {
                        input.1.vector = adoption.definition.collection();
                        crate::bootstrap::VerificationStage::AdoptedCuts(None)
                    }
                    None => {
                        input.1.vector = root
                            .reenable
                            .as_ref()
                            .ok_or_else(corrupt_record)?
                            .definition
                            .initial()
                            .initialize_vector()?;
                        crate::bootstrap::VerificationStage::Cuts(0)
                    }
                },
            };
            Ok(())
        }
        .await;
        settlement.complete(result)
    }
}

impl JournalStore {
    pub(super) fn require_target_retained_inventory(
        &self,
        root: &TargetRecord,
    ) -> StorageResult<crate::bootstrap::RetainedHistoryInventory> {
        match (&root.reenable, &root.adoption) {
            (Some(retained), None) => {
                self.require_target_retained_history(root, &retained.definition)?
            }
            (None, Some(adoption)) => self.require_target_adoption(root, &adoption.definition)?,
            _ => return Err(corrupt_record()),
        }
        root.retained_inventory()
    }
}
