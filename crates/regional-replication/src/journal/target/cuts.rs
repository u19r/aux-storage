use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, RecordAddress},
};
use replication_provider::{
    delivery::coverage::DeliveryCoverage, regional_application::observation::ImportPageScope,
};
use replication_types::{
    configuration::installed::{InstalledTableState, RegionalTablePhase},
    identity::{
        origin::OriginId,
        resource::{ControlOperationId, TableIdentity, TableLineage},
    },
    protocol::CanonicalDigest,
    regional_import::RegionalImport,
};
use serde::{Deserialize, Serialize};
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, StorageResult};

use crate::{
    bootstrap::{HiddenTargetRequest, SourceCaptureCut, SourceCaptureDefinition},
    constants::{CONTROL_SLOT_SUFFIX, TARGET_BOOTSTRAP_SUFFIX, TARGET_CUT_SUFFIX},
    journal::JournalStore,
    native_record::corrupt_record,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TargetCut {
    operation: ControlOperationId,
    request: CanonicalDigest,
    definition: CanonicalDigest,
    lineage: TableLineage,
    source: replication_types::configuration::installed::RegionalSourceBinding,
    cut: SourceCaptureCut,
    coverage: DeliveryCoverage,
    retained_ordinal: Option<u64>,
}

impl JournalStore {
    pub(super) fn target_cut_key(
        &self,
        operation: ControlOperationId,
        scope: (TableIdentity, TableLineage, &OriginId),
    ) -> StorageResult<infrastructure_provider::record::DurableRecordKey> {
        let mut key: RecordAddress = self.identity_key(TARGET_CUT_SUFFIX, &operation)?.into();
        let serialized_digest = CanonicalDigest::of_json(&scope).map_err(|_| corrupt_record())?;
        key.suffix.extend_from_slice(serialized_digest.as_ref());
        key.try_into()
    }

    pub(crate) async fn declare_bootstrap_cut<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        request: &HiddenTargetRequest,
        input: (&SourceCaptureDefinition, &SourceCaptureCut),
    ) -> StorageResult<SourceCaptureCut> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (definition, cut) = input;
            let (root_row, mut root) = self
                .load_hidden_target(attempt, request, ReadProtection::Conflict)
                .await?
                .ok_or_else(corrupt_record)?;
            self.require_hidden_table(attempt, &root, ReadProtection::Shared)
                .await?;
            self.require_initial_target_definition(&root, definition)?;
            let definition_content = if let Some(retained) = &root.reenable {
                self.require_target_retained_history(&root, &retained.definition)?;
                retained.definition.content()
            } else {
                crate::bootstrap::TargetBootstrapDefinition::Initial(Box::new(definition.clone()))
                    .validate()?;
                definition.content()
            };

            let source = root
                .state
                .sources
                .iter()
                .find(|source| source.region == cut.origin.region)
                .ok_or_else(corrupt_record)?;
            if source.region != request.operation.home_region
                || source.database_group != cut.origin.database_group
                || source.generation != cut.origin.journal_generation
            {
                return Err(corrupt_record());
            }
            source
                .partitions
                .require_partition(cut.origin.journal_partition)
                .map_err(|_| corrupt_record())?;
            let key = self.target_cut_key(
                request.operation.id,
                (source.table, root.state.configuration.lineage, &cut.origin),
            )?;
            let rows = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?;
            if let Some(row) = &rows[0] {
                let existing: TargetCut = self.records.decode(row)?;
                existing.require_declaration(
                    &root,
                    (source, cut, definition_content),
                    self.records.limits.max_records(),
                )?;
            } else {
                if root
                    .verification
                    .as_ref()
                    .is_some_and(|progress| progress.history_frozen())
                {
                    return Err(corrupt_record());
                }
                let record = TargetCut::from_declaration(
                    &root,
                    source.clone(),
                    cut.clone(),
                    definition_content,
                )?;
                self.records
                    .store(attempt, &[self.records.prepare(key, None, &record)?])
                    .await?;
            }
            if root.definition.is_none() {
                root.definition = Some(definition_content);
                root.source_vector = Some(definition.vector());
                let change = self
                    .records
                    .prepare(root_row.key, Some(root_row.revision), &root)?;
                self.records.store(attempt, &[change]).await?;
            }
            Ok(cut.clone())
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn require_import_binding<
        A: stream_provider::retained_image::RetainedOwnershipAttempt + CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        state: &InstalledTableState,
        input: (
            &replication_types::configuration::membership::RegionalTableBinding,
            &RegionalImport,
            &replication_provider::canonical_event_codec::CanonicalJsonCodec,
        ),
    ) -> StorageResult<crate::import::ImportAuthority> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (binding, import, codec) = input;
            if state.phase != RegionalTablePhase::Joining {
                return state.validate_import((binding, &import.manifest().origin), &self.region).map(|()| crate::import::ImportAuthority::Current).map_err(|error| replication_provider::regional_application::error::RegionalApplicationError::from(error).into());
            }
            let (_, cut, root) = self.load_joining_cut(attempt, state, (import, ReadProtection::Shared, codec)).await?;
            cut.require_binding(state, binding, import)?;
            let Some(ordinal) = cut.retained_ordinal else { return Ok(crate::import::ImportAuthority::Current); };
            self.load_retained_import_authority(attempt, &root, (&cut, ordinal, import.manifest().membership)).await
        }.await;
        settlement.complete(result)
    }

    pub(crate) async fn record_joining_coverage<
        A: stream_provider::retained_image::RetainedOwnershipAttempt + CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        state: &InstalledTableState,
        import: &RegionalImport,
        codec: &replication_provider::canonical_event_codec::CanonicalJsonCodec,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if state.phase != RegionalTablePhase::Joining {
                return Ok(());
            }
            let (row, mut cut, _) = self
                .load_joining_cut(attempt, state, (import, ReadProtection::Conflict, codec))
                .await?;
            let next = cut
                .coverage
                .accept(import.manifest().range, self.records.limits.max_records())
                .map_err(|_| corrupt_record())?;
            if next == cut.coverage {
                return Ok(());
            }
            cut.coverage = next;
            self.records
                .store(
                    attempt,
                    &[self.records.prepare(row.key, Some(row.revision), &cut)?],
                )
                .await
        }
        .await;
        settlement.complete(result)
    }

    async fn load_joining_cut<
        A: stream_provider::retained_image::RetainedOwnershipAttempt + CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        state: &InstalledTableState,
        input: (
            &RegionalImport,
            ReadProtection,
            &replication_provider::canonical_event_codec::CanonicalJsonCodec,
        ),
    ) -> StorageResult<(DurableRecord, TargetCut, super::record::TargetRecord)> {
        let (import, protection, codec) = input;
        let slot = self.identity_key(CONTROL_SLOT_SUFFIX, &state.configuration.table)?;
        let slots = self
            .records
            .load(attempt, &[slot], ReadProtection::Shared)
            .await?;
        let operation = self.decode_control_slot(
            slots[0].as_ref().ok_or_else(corrupt_record)?,
            state.configuration.table,
        )?;
        let operation = operation.require_configuration()?;
        let root_key = self.identity_key(TARGET_BOOTSTRAP_SUFFIX, &operation)?;
        let roots = self
            .records
            .load(attempt, &[root_key], ReadProtection::Shared)
            .await?;
        let root: super::record::TargetRecord = self
            .records
            .decode(roots[0].as_ref().ok_or_else(corrupt_record)?)?;
        if root.state != *state
            || root.request.operation.id != operation
            || root.content != root.request.content()?
        {
            return Err(corrupt_record());
        }
        self.require_hidden_table(attempt, &root, ReadProtection::Shared)
            .await?;
        if let Some(adoption) = &root.adoption {
            self.require_target_adoption(&root, &adoption.definition)?;
        }
        if let Some(retained) = &root.reenable {
            self.require_target_retained_history(&root, &retained.definition)?;
        }
        let binding = root.require_joining_import(import, &self.region)?;
        if root
            .verification
            .as_ref()
            .is_some_and(|progress| progress.history_frozen())
            && self
                .load_import_pages(attempt, import, codec, ImportPageScope::Selected)
                .await?
                .is_none()
        {
            return Err(storage_types::StorageError::unsupported(
                "hidden verification has frozen its accepted history",
            ));
        }
        let key = self.target_cut_key(
            operation,
            (
                binding.source_table,
                binding.lineage,
                &import.manifest().origin,
            ),
        )?;
        let mut rows = self.records.load(attempt, &[key], protection).await?;
        let row = rows.pop().flatten().ok_or_else(corrupt_record)?;
        let cut: TargetCut = self.records.decode(&row)?;
        self.require_joining_range((&root, &cut), import)?;
        cut.require_binding(state, binding, import)?;
        Ok((row, cut, root))
    }

    fn require_joining_range(
        &self,
        input: (&super::record::TargetRecord, &TargetCut),
        import: &RegionalImport,
    ) -> StorageResult<()> {
        let (root, cut) = input;
        cut.coverage
            .validate(self.records.limits.max_records())
            .map_err(|_| corrupt_record())?;
        cut.require_source()?;
        if !cut.matches_lineage(root)
            || cut.operation != root.request.operation.id
            || cut.request != root.content
            || Some(cut.definition) != root.definition
            || cut.cut.origin != import.manifest().origin
            || import.manifest().range.through() > cut.cut.through
        {
            return Err(corrupt_record());
        }
        Ok(())
    }
}

impl JournalStore {
    pub(super) async fn load_verified_target_cuts<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        root: &super::record::TargetRecord,
        after: u32,
    ) -> StorageResult<(Vec<SourceCaptureCut>, Option<u32>)> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let source = root
                .state
                .sources
                .iter()
                .find(|source| source.region == root.request.operation.home_region)
                .ok_or_else(corrupt_record)?;
            let count = u32::from(source.partitions);
            if after >= count {
                return Err(corrupt_record());
            }
            let width = u32::try_from(
                self.records
                    .limits
                    .max_records()
                    .get()
                    .saturating_sub(1)
                    .clamp(1, crate::constants::HISTORY_CUT_PAGE_WIDTH),
            )
            .map_err(|_| corrupt_record())?;
            let end = after.saturating_add(width).min(count);
            let origins = (after..end)
                .map(|partition| replication_types::identity::origin::OriginId {
                    region: source.region.clone(),
                    database_group: source.database_group,
                    journal_generation: source.generation,
                    journal_partition: (partition as u16).into(),
                })
                .collect::<Vec<_>>();
            let keys = origins
                .iter()
                .map(|origin| {
                    self.target_cut_key(
                        root.request.operation.id,
                        (source.table, root.state.configuration.lineage, origin),
                    )
                })
                .collect::<StorageResult<Vec<_>>>()?;
            let rows = self
                .records
                .load(attempt, &keys, ReadProtection::Shared)
                .await?;
            let mut cuts = Vec::with_capacity(rows.len());
            for (row, origin) in rows.into_iter().zip(origins) {
                let cut: TargetCut = self.records.decode(&row.ok_or_else(corrupt_record)?)?;
                self.require_target_cut(root, &cut)?;
                if cut.source != *source
                    || cut.cut.origin != origin
                    || cut.coverage.through() != cut.cut.through
                    || !cut.coverage.pending().is_empty()
                {
                    return Err(corrupt_record());
                }
                cuts.push(cut.cut);
            }
            Ok((cuts, (end < count).then_some(end)))
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn require_reconstruction_link<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        root: &super::record::TargetRecord,
        retained: &super::super::retained_links::RetainedSourceLink,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key = self.target_cut_key(
                root.request.operation.id,
                (retained.link.table, retained.link.lineage, &retained.origin),
            )?;
            let rows = self
                .records
                .load(attempt, &[key], ReadProtection::Snapshot)
                .await?;
            let cut: TargetCut = self
                .records
                .decode(rows[0].as_ref().ok_or_else(corrupt_record)?)?;
            self.require_target_cut(root, &cut)?;
            if cut.source.table != retained.link.table || retained.link.lineage != cut.lineage {
                return Err(corrupt_record());
            }
            if cut.cut.origin != retained.origin
                || retained.link.position > cut.cut.through
                || u64::from(retained.link.position) == 0
            {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    pub(super) fn require_target_cut(
        &self,
        root: &super::record::TargetRecord,
        cut: &TargetCut,
    ) -> StorageResult<()> {
        cut.coverage
            .validate(self.records.limits.max_records())
            .map_err(|_| corrupt_record())?;
        cut.require_source()?;
        if !cut.matches_lineage(root)
            || cut.operation != root.request.operation.id
            || cut.request != root.content
            || Some(cut.definition) != root.definition
        {
            return Err(corrupt_record());
        }
        Ok(())
    }
}

impl TargetCut {
    fn require_binding(
        &self,
        state: &InstalledTableState,
        binding: &replication_types::configuration::membership::RegionalTableBinding,
        import: &RegionalImport,
    ) -> StorageResult<()> {
        if binding.local_table != state.configuration.table
            || binding.local_config != state.configuration.config
            || (self.retained_ordinal.is_none() && binding.lineage != state.configuration.lineage)
            || binding.lineage != self.lineage
            || binding.source_region != self.source.region
            || binding.source_table != self.source.table
            || binding.membership != import.manifest().membership
            || (self.retained_ordinal.is_none() && binding.membership > state.membership)
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(super) fn from_declaration(
        root: &super::record::TargetRecord,
        source: replication_types::configuration::installed::RegionalSourceBinding,
        cut: SourceCaptureCut,
        definition: CanonicalDigest,
    ) -> StorageResult<Self> {
        let record = Self {
            operation: root.request.operation.id,
            request: root.content,
            definition,
            lineage: root.state.configuration.lineage,
            source,
            cut,
            coverage: DeliveryCoverage::default(),
            retained_ordinal: None,
        };
        record.require_source()?;
        Ok(record)
    }

    pub(super) fn require_declaration(
        &self,
        root: &super::record::TargetRecord,
        input: (
            &replication_types::configuration::installed::RegionalSourceBinding,
            &SourceCaptureCut,
            CanonicalDigest,
        ),
        max_pending: std::num::NonZeroUsize,
    ) -> StorageResult<()> {
        self.require_source()?;
        self.coverage
            .validate(max_pending)
            .map_err(|_| corrupt_record())?;
        if self.lineage != root.state.configuration.lineage
            || self.operation != root.request.operation.id
            || self.request != root.content
            || self.definition != input.2
            || self.source != *input.0
            || self.cut != *input.1
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(super) fn scope(&self) -> (TableIdentity, TableLineage, &OriginId) {
        (self.source.table, self.lineage, &self.cut.origin)
    }

    pub(super) fn require_complete_coverage(&self) -> StorageResult<()> {
        if self.coverage.through() != self.cut.through || !self.coverage.pending().is_empty() {
            return Err(corrupt_record());
        }
        Ok(())
    }

    fn require_source(&self) -> StorageResult<()> {
        if self.source.region != self.cut.origin.region
            || self.source.database_group != self.cut.origin.database_group
            || self.source.generation != self.cut.origin.journal_generation
        {
            return Err(corrupt_record());
        }
        self.source
            .partitions
            .require_partition(self.cut.origin.journal_partition)
            .map_err(|_| corrupt_record())
    }
}

impl TargetCut {
    pub(super) fn from_retained(
        root: &super::record::TargetRecord,
        history: &crate::bootstrap::ReenableHistoryCut,
        definition: CanonicalDigest,
    ) -> StorageResult<Self> {
        let mut record = Self::from_declaration(
            root,
            history.cut.source.clone(),
            SourceCaptureCut {
                origin: history.cut.origin.clone(),
                through: history.cut.through,
            },
            definition,
        )?;
        record.lineage = history.cut.lineage;
        record.retained_ordinal = Some(history.ordinal);
        Ok(record)
    }

    pub(super) fn require_retained_declaration(
        &self,
        root: &super::record::TargetRecord,
        history: &crate::bootstrap::ReenableHistoryCut,
        max_pending: std::num::NonZeroUsize,
    ) -> StorageResult<()> {
        let definition = root.retained_inventory()?.bootstrap;
        self.coverage
            .validate(max_pending)
            .map_err(|_| corrupt_record())?;
        let expected = Self::from_retained(root, history, definition)?;
        if self.operation != expected.operation
            || self.request != expected.request
            || self.definition != expected.definition
            || self.lineage != expected.lineage
            || self.source != expected.source
            || self.cut != expected.cut
            || self.retained_ordinal != expected.retained_ordinal
        {
            return Err(corrupt_record());
        }
        Ok(())
    }
}

impl TargetCut {
    fn matches_lineage(&self, root: &super::record::TargetRecord) -> bool {
        match self.retained_ordinal {
            None => self.lineage == root.state.configuration.lineage,
            Some(ordinal) => root.retained_inventory().is_ok_and(|retained| {
                ordinal < retained.cuts && self.lineage != root.state.configuration.lineage
            }),
        }
    }
}
