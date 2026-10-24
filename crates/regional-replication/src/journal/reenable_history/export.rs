//! Export only frozen, actively held native inventory facts in their fold
//! order.
use infrastructure_provider::{
    page::{BoundedRecordRange, RecordPageRequest},
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecordKey, RecordAddress},
};
use replication_types::{
    configuration::proposal::ControlProposal, identity::resource::ControlOperationId,
    protocol::CanonicalDigest,
};
use storage_types::{ReadProtection, StorageResult};

use super::record::{InventoryPhase, RetainedCut, RetainedLineage};
use crate::{
    bootstrap::{ReenableHistoryCut, ReenableHistoryDefinition, ReenableHistoryPage},
    constants::REENABLE_CUT_SUFFIX,
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(in crate::journal) async fn load_reenable_trim_inventory<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        operation: ControlOperationId,
    ) -> StorageResult<Option<(ControlProposal, crate::bootstrap::RetainedHistoryInventory)>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (_, root) = self
                .load_reenable_inventory(attempt, operation, ReadProtection::Shared)
                .await?
                .ok_or_else(corrupt_record)?;
            if matches!(
                root.phase,
                InventoryPhase::Released | InventoryPhase::Cancelled
            ) {
                return Err(corrupt_record());
            }
            self.require_reenable_custody(attempt, &root, ReadProtection::Shared)
                .await?;
            if !matches!(root.phase, InventoryPhase::Ready) {
                return Ok(None);
            }
            let definition = self
                .qualify_reenable_definition(attempt, &root.proposal, &root, ReadProtection::Shared)
                .await?;
            let inventory = crate::bootstrap::RetainedHistoryInventory::try_from(&definition)?;
            Ok(Some((root.proposal, inventory)))
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(crate) async fn load_reenable_history_definition<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        protection: ReadProtection,
    ) -> StorageResult<ReenableHistoryDefinition> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (_, root) = self
                .load_reenable_inventory(attempt, proposal.operation().id, protection)
                .await?
                .ok_or_else(corrupt_record)?;
            self.qualify_reenable_definition(attempt, proposal, &root, protection)
                .await
        }
        .await;
        settlement_guard.complete(result)
    }

    async fn qualify_reenable_definition<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        root: &super::record::InventoryRecord,
        protection: ReadProtection,
    ) -> StorageResult<ReenableHistoryDefinition> {
        if root.proposal != *proposal
            || !matches!(root.phase, InventoryPhase::Ready | InventoryPhase::Released)
        {
            return Err(corrupt_record());
        }
        if matches!(root.phase, InventoryPhase::Ready) {
            self.require_reenable_custody(attempt, root, protection)
                .await?;
        }
        let before = proposal
            .participant(&self.region)
            .map_err(|_| corrupt_record())?
            .before
            .as_ref()
            .ok_or_else(corrupt_record)?;
        let previous = self
            .load_configuration_control(attempt, before, protection)
            .await?;
        if previous.decision != crate::control::ControlDecision::Completed {
            return Err(corrupt_record());
        }
        let initial = self
            .load_source_capture_definition(attempt, proposal, protection)
            .await?;
        let definition = ReenableHistoryDefinition::from_native(
            initial,
            previous.proposal,
            (root.vector, root.cuts),
        )?;
        if matches!(root.phase, InventoryPhase::Released) {
            let row = self
                .load_control(attempt, proposal, protection)
                .await?
                .ok_or_else(corrupt_record)?;
            let control = self.decode_control(&row, proposal)?;
            if control.decision != crate::control::ControlDecision::Completed
                || control.bootstrap != Some(definition.content())
            {
                return Err(corrupt_record());
            }
        }
        Ok(definition)
    }

    pub(crate) async fn load_reenable_history_page<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        ordinal: u64,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<ReenableHistoryPage> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            self.load_reenable_history_export(attempt, proposal, ordinal, protection)
                .await
                .map(|(_, page)| page)
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn load_reenable_history_export<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        ordinal: u64,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<(ReenableHistoryDefinition, ReenableHistoryPage)> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (_, root) = self
                .load_reenable_inventory(attempt, proposal.operation().id, protection)
                .await?
                .ok_or_else(corrupt_record)?;
            if root.proposal != *proposal {
                return Err(corrupt_record());
            }
            self.export_reenable_inventory(attempt, &root, ordinal, protection)
                .await
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) async fn load_reenable_trim_export<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        operation: ControlOperationId,
        ordinal: u64,
    ) -> StorageResult<(ReenableHistoryDefinition, ReenableHistoryPage)> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (_, root) = self
                .load_reenable_inventory(attempt, operation, ReadProtection::Shared)
                .await?
                .ok_or_else(corrupt_record)?;
            self.export_reenable_inventory(
                attempt,
                &root,
                ordinal,
                storage_types::ReadProtection::Shared,
            )
            .await
        }
        .await;
        settlement_guard.complete(result)
    }

    async fn export_reenable_inventory<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        root: &super::record::InventoryRecord,
        ordinal: u64,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<(ReenableHistoryDefinition, ReenableHistoryPage)> {
        if !matches!(root.phase, InventoryPhase::Ready) {
            return Err(corrupt_record());
        }
        let proposal = &root.proposal;
        let definition = self
            .qualify_reenable_definition(attempt, proposal, root, protection)
            .await?;
        let page = self
            .load_reenable_inventory_page(attempt, proposal, (&definition, ordinal), protection)
            .await?;
        Box::pin(self.require_exported_history(attempt, proposal, &page.cut, protection)).await?;
        Ok((definition, page))
    }

    pub(in crate::journal) async fn load_reenable_inventory_page<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        input: (&ReenableHistoryDefinition, u64),
        protection: storage_types::ReadProtection,
    ) -> StorageResult<ReenableHistoryPage> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (definition, ordinal) = input;
            if ordinal >= definition.cuts() {
                return Err(corrupt_record());
            }
            let prefix = self.reenable_cut_prefix(proposal.operation().id, ordinal)?;
            let bounds = self.record_prefix_range(&prefix)?;
            let range = BoundedRecordRange::try_new(
                prefix,
                bounds.bounds().1.clone(),
                std::num::NonZeroUsize::MIN,
            )?;
            let page = self
                .records
                .scan(
                    attempt,
                    RecordPageRequest {
                        range,
                        after: None,
                        max_bytes: self.records.limits.max_bytes(),
                        protection,
                    },
                )
                .await?;
            if page.continuation.is_some() || page.records.len() != 1 {
                return Err(corrupt_record());
            }
            let row = &page.records[0];
            let retained: RetainedCut = self.records.decode(row)?;
            let history = retained.history;
            if retained.operation != proposal.operation().id
                || history.ordinal != ordinal
                || row.key != self.reenable_cut_key(retained.operation, &history)?
            {
                return Err(corrupt_record());
            }
            let baseline = self
                .load_reenable_baseline_inventory(
                    attempt,
                    proposal.operation().id,
                    &history.survivor,
                    protection,
                )
                .await?;
            if baseline.content != history.baseline.content {
                return Err(corrupt_record());
            }
            let next = ordinal.checked_add(1).ok_or_else(corrupt_record)?;
            let page = ReenableHistoryPage {
                definition: definition.content(),
                cut: history,
                next_ordinal: (next < definition.cuts()).then_some(next),
            };
            definition.require_page(&page, (ordinal, retained.previous))?;
            Ok(page)
        }
        .await;
        settlement.complete(result)
    }

    async fn require_exported_history<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        history: &ReenableHistoryCut,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<()> {
        if let Some(custody) = &history.custody {
            let native = self
                .load_retained_custody_page(attempt, proposal, custody, protection)
                .await?;
            let expected = CanonicalDigest::of_json(&(
                &history.disable,
                &history.survivor,
                &history.cut,
                &history.visibility,
                &history.baseline,
            ))
            .map_err(|_| corrupt_record())?;
            let actual = CanonicalDigest::of_json(&(
                &native.cut.disable,
                &native.cut.survivor,
                &native.cut.cut,
                &native.cut.visibility,
                &native.cut.baseline,
            ))
            .map_err(|_| corrupt_record())?;
            if expected != actual {
                return Err(corrupt_record());
            }
            return Ok(());
        }
        self.require_retained_lineage(
            proposal,
            &RetainedLineage {
                disable: history.disable.clone(),
                survivor: history.survivor.clone(),
            },
        )?;
        let receipt = self
            .load_control(attempt, &history.disable, protection)
            .await?
            .ok_or_else(corrupt_record)?;
        if self.decode_control(&receipt, &history.disable)?.decision
            != crate::control::ControlDecision::Completed
        {
            return Err(corrupt_record());
        }
        let baseline = self
            .load_reenable_baseline_inventory(
                attempt,
                proposal.operation().id,
                &history.survivor,
                protection,
            )
            .await?;
        if baseline.content != history.baseline.content {
            return Err(corrupt_record());
        }
        let cut = &history.cut;
        history.visibility.require_cut(cut)?;
        let authority = self
            .load_public_trim_head(attempt, (&history.survivor, cut.lineage), protection)
            .await?;
        if authority.capture_historical_visibility()? != history.visibility {
            return Err(corrupt_record());
        }
        if cut.origin.region == self.region {
            if !history
                .survivor
                .sources
                .iter()
                .any(|source| source == &cut.source)
            {
                return Err(corrupt_record());
            }
        } else {
            let authority = self
                .load_retired_source_authority(attempt, &history.survivor, &cut.source, protection)
                .await?;
            let native = self
                .load_retired_source_cuts(
                    attempt,
                    &history.survivor,
                    (&authority, std::slice::from_ref(&cut.origin)),
                    protection,
                )
                .await?
                .pop()
                .ok_or_else(corrupt_record)?;
            if native.through != cut.through {
                return Err(corrupt_record());
            }
        }
        if cut.lineage != history.survivor.configuration.lineage
            || cut.membership != history.survivor.membership
            || cut.source.region != cut.origin.region
            || cut.source.database_group != cut.origin.database_group
            || cut.source.generation != cut.origin.journal_generation
            || u32::from(u16::from(cut.origin.journal_partition))
                >= u32::from(cut.source.partitions)
            || (u64::from(cut.through) == 0) != cut.terminal_manifest.is_none()
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(crate) async fn load_reenable_original_frame<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        request: &crate::bootstrap::ReenableOriginalRequest,
        encoding: (
            &replication_provider::canonical_event_codec::CanonicalJsonCodec,
            &replication_provider::runtime_configuration::limits::WorkLimits,
        ),
        protection: storage_types::ReadProtection,
    ) -> StorageResult<Option<replication_provider::delivery::frame::SourceFrame>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let page = self
                .load_reenable_history_page(attempt, &request.proposal, request.ordinal, protection)
                .await?;
            let cut = page.cut.cut;
            if page.definition != request.definition || request.after > cut.through {
                return Err(corrupt_record());
            }
            if request.after == cut.through {
                return Ok(None);
            }
            let range = replication_types::event::interval::ClosedSourceInterval::try_from((
                request.after,
                cut.through,
            ))
            .map_err(|_| corrupt_record())?;
            let selection = crate::source_frame::SourceFrameRequest {
                origin: cut.origin,
                range,
                membership: cut.membership,
                tables: vec![(cut.source.table, cut.lineage)],
            };
            crate::delivery::load_bounded_frame(self, attempt, selection, encoding)
                .await
                .map(Some)
                .map_err(storage_types::StorageError::from)
        }
        .await;
        settlement_guard.complete(result)
    }

    fn reenable_cut_prefix(
        &self,
        operation: ControlOperationId,
        ordinal: u64,
    ) -> StorageResult<DurableRecordKey> {
        let mut key: RecordAddress = self.identity_key(REENABLE_CUT_SUFFIX, &operation)?.into();
        key.suffix.extend_from_slice(&ordinal.to_be_bytes());
        key.try_into()
    }

    pub(super) fn reenable_cut_key(
        &self,
        operation: ControlOperationId,
        history: &ReenableHistoryCut,
    ) -> StorageResult<DurableRecordKey> {
        let mut key: RecordAddress = self.reenable_cut_prefix(operation, history.ordinal)?.into();
        let cut = &history.cut;
        key.suffix.extend_from_slice(
            CanonicalDigest::of_json(&(cut.source.table, cut.lineage, &cut.origin))
                .map_err(|_| corrupt_record())?
                .as_ref(),
        );
        key.try_into()
    }
}

impl JournalStore {
    pub(in crate::journal) async fn load_retained_history_export<
        A: PhysicalRecordAttempt + storage_provider::CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        ordinal: u64,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<(
        crate::bootstrap::TargetBootstrapDefinition,
        ReenableHistoryPage,
    )> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let definition = self
                .load_source_bootstrap_definition(attempt, proposal, protection)
                .await?;
            let page = match &definition {
                crate::bootstrap::TargetBootstrapDefinition::Reenable(_) => {
                    self.require_capture_table(attempt, proposal, protection)
                        .await?;
                    self.load_reenable_history_export(attempt, proposal, ordinal, protection)
                        .await?
                        .1
                }
                crate::bootstrap::TargetBootstrapDefinition::Addition(adoption) => {
                    let page = self
                        .load_collected_retained_page(attempt, adoption, ordinal, protection)
                        .await?;
                    Box::pin(
                        self.require_exported_history(attempt, proposal, &page.cut, protection),
                    )
                    .await?;
                    page
                }
                _ => return Err(corrupt_record()),
            };
            Ok((definition, page))
        }
        .await;
        settlement_guard.complete(result)
    }
}
