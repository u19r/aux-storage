//! Native dependency identity and semantic binding for archived publications.
use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey},
};
use replication_types::{
    event::{
        content::CanonicalEvent, disposition::CanonicalAcceptanceReceipt,
        identity::CanonicalEventId,
    },
    identity::resource::TableLineage,
    protocol::CanonicalDigest,
};
use storage_types::{ReadProtection, Revision, StorageResult};

use crate::{
    archive::ArchiveKind,
    history::view::CanonicalViewPointer,
    journal::{
        JournalStore, SourcePosition, history_view::NativeAcceptanceRecord,
        retained_links::RetainedSourceLink,
    },
    native_record::corrupt_record,
};

pub(super) enum ArchivePublicationScope {
    IndependentMetadata,
    Collection { definition: DurableRecord },
    Source(SourcePosition),
    Acceptance(NativeAcceptanceRecord),
    Receiver(CanonicalViewPointer),
    RetainedLink(RetainedSourceLink),
    Imported(super::imported_scope::ImportedArchiveScope),
}

#[derive(Clone, PartialEq, Eq)]
pub(super) struct ArchiveEventDependency {
    pub event: CanonicalEventId,
    pub digest: CanonicalDigest,
    pub lineage: TableLineage,
    pub delivery: Option<CanonicalViewPointer>,
    pub receipt: Option<CanonicalAcceptanceReceipt>,
}

#[derive(Clone, PartialEq, Eq)]
pub(super) enum ArchivePublicationDependency {
    Event(Box<ArchiveEventDependency>),
    Metadata(crate::archive::ArchivePointer),
}

impl ArchivePublicationScope {
    pub(super) fn collect_original_dependencies(&self) -> Vec<ArchiveEventDependency> {
        match self {
            Self::IndependentMetadata | Self::Collection { .. } => Vec::new(),
            Self::Imported(imported) => imported.events.clone(),
            Self::Source(source) => source
                .events
                .iter()
                .map(|link| ArchiveEventDependency {
                    event: link.event.clone(),
                    digest: link.digest,
                    lineage: link.lineage,
                    delivery: None,
                    receipt: None,
                })
                .collect::<Vec<_>>(),
            Self::RetainedLink(retained) => vec![ArchiveEventDependency {
                event: retained.link.event.clone(),
                digest: retained.link.digest,
                lineage: retained.link.lineage,
                delivery: None,
                receipt: None,
            }],
            Self::Acceptance(accepted) => vec![ArchiveEventDependency {
                event: accepted.receipt.event.clone(),
                digest: accepted.receipt.digest,
                lineage: accepted.delivery.lineage,
                delivery: None,
                receipt: None,
            }],
            Self::Receiver(pointer) => vec![ArchiveEventDependency {
                event: pointer.event.clone(),
                digest: pointer.digest,
                lineage: pointer.lineage,
                delivery: Some(pointer.clone()),
                receipt: None,
            }],
        }
    }

    pub(super) fn metadata_dependencies(&self) -> &[DurableRecord] {
        match self {
            Self::Imported(imported) => &imported.metadata,
            Self::Collection { definition } => std::slice::from_ref(definition),
            _ => &[],
        }
    }

    pub(super) fn dependencies(&self) -> Vec<(ArchiveKind, ArchiveEventDependency)> {
        let events = self.collect_original_dependencies();
        let mut dependencies = events
            .iter()
            .cloned()
            .map(|event| (ArchiveKind::Event, event))
            .collect::<Vec<_>>();
        if !matches!(self, Self::Acceptance(_)) {
            dependencies.extend(
                events
                    .into_iter()
                    .map(|event| (ArchiveKind::Acceptance, event)),
            );
        }
        dependencies
    }
}

impl JournalStore {
    pub(super) async fn load_archive_publication_scope<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &DurableRecord,
        protection: ReadProtection,
    ) -> StorageResult<ArchivePublicationScope> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            match ArchiveKind::from_key(&record.key) {
                Some(ArchiveKind::CollectionPage) => {
                    let page = self.decode_collected_history_page(record)?;
                    let key = page.definition_key(self)?;
                    let definition = self
                        .load_archive_dependency_metadata(attempt, &key, protection)
                        .await?;
                    page.qualify(&self.decode_collected_history_definition(&definition)?)?;
                    Ok(ArchivePublicationScope::Collection { definition })
                }
                Some(ArchiveKind::ImportPage) => self
                    .load_imported_page_scope(
                        attempt,
                        (self.decode_import_page_record(record)?, Vec::new()),
                        protection,
                    )
                    .await
                    .map(ArchivePublicationScope::Imported),
                Some(ArchiveKind::RetainedPosition) => {
                    let selected = self.decode_retained_position_record(record)?;
                    let key =
                        self.identity_key(crate::constants::IMPORT_PAGE_SUFFIX, &selected.page)?;
                    let page_record = self
                        .load_archive_dependency_metadata(attempt, &key, protection)
                        .await?;
                    let page = self.decode_import_page_record(&page_record)?;
                    if page.page != selected.page {
                        return Err(corrupt_record());
                    }
                    self.load_imported_page_scope(attempt, (page, vec![page_record]), protection)
                        .await
                        .map(ArchivePublicationScope::Imported)
                }
                _ => self.decode_archive_publication(record),
            }
        }
        .await;
        settlement.complete(result)
    }

    pub(super) fn decode_archive_publication(
        &self,
        record: &DurableRecord,
    ) -> StorageResult<ArchivePublicationScope> {
        if record.revision != Revision::INITIAL {
            return Err(corrupt_record());
        }
        match ArchiveKind::from_key(&record.key) {
            Some(ArchiveKind::BaselineDefinition) => self
                .decode_baseline_lineage_definition(record)
                .map(|_| ArchivePublicationScope::IndependentMetadata),
            Some(ArchiveKind::BaselineInventory) => self
                .decode_reenable_baseline_inventory(record)
                .map(|_| ArchivePublicationScope::IndependentMetadata),
            Some(ArchiveKind::CollectionBaseline) => self
                .require_collected_baseline_definition(record)
                .map(|_| ArchivePublicationScope::IndependentMetadata),
            Some(ArchiveKind::CollectionRetainedDefinition) => self
                .require_collected_retained_definition(record)
                .map(|_| ArchivePublicationScope::IndependentMetadata),
            Some(ArchiveKind::CollectionRetainedCut) => self
                .decode_collected_retained_cut(record)
                .map(|_| ArchivePublicationScope::IndependentMetadata),
            Some(ArchiveKind::CollectionCatalog) => self
                .decode_collected_catalog_page(record)
                .map(|_| ArchivePublicationScope::IndependentMetadata),
            Some(ArchiveKind::CollectionRetiredPage) => self
                .decode_collected_retired_index(record)
                .map(|_| ArchivePublicationScope::IndependentMetadata),
            Some(ArchiveKind::CollectionDeclaration) => self
                .decode_source_collection_declaration(record)
                .map(|_| ArchivePublicationScope::IndependentMetadata),
            Some(ArchiveKind::CollectionDefinition) => self
                .decode_collected_history_definition(record)
                .map(|_| ArchivePublicationScope::IndependentMetadata),
            Some(ArchiveKind::Source) => self
                .decode_archive_source(record)
                .map(ArchivePublicationScope::Source),
            Some(ArchiveKind::Acceptance) => self
                .decode_native_acceptance(record)
                .map(ArchivePublicationScope::Acceptance),
            Some(ArchiveKind::ImportManifest) => {
                self.decode_import_manifest_identity(record).map(|_| {
                    ArchivePublicationScope::Imported(super::imported_scope::ImportedArchiveScope {
                        events: Vec::new(),
                        metadata: Vec::new(),
                        links: Vec::new(),
                    })
                })
            }
            Some(ArchiveKind::RetainedLink) => self
                .decode_retained_source_link(record)
                .map(ArchivePublicationScope::RetainedLink),
            Some(ArchiveKind::ReceiverView) => self
                .decode_receiver_pointer(record)
                .map(ArchivePublicationScope::Receiver),
            _ => Err(corrupt_record()),
        }
    }

    pub(super) fn decode_archive_source(
        &self,
        record: &DurableRecord,
    ) -> StorageResult<SourcePosition> {
        let source: SourcePosition = self.records.decode(record)?;
        source.validate()?;
        if record.revision != Revision::INITIAL
            || u64::from(source.position) == 0
            || !self.is_local_origin(&source.origin)
            || record.key != self.source_key(&source.origin, source.position)?
        {
            return Err(corrupt_record());
        }
        self.partitions
            .require_partition(source.origin.journal_partition)
            .map_err(|_| corrupt_record())?;
        Ok(source)
    }

    pub(super) fn archive_publication_dependencies(
        &self,
        scope: &ArchivePublicationScope,
    ) -> StorageResult<Vec<(DurableRecordKey, ArchivePublicationDependency)>> {
        let mut entries = std::collections::BTreeMap::new();
        for (kind, event) in scope.dependencies() {
            let key = self.identity_key(kind.prefix(), &event.event)?;
            let expected = ArchivePublicationDependency::Event(Box::new(event));
            if entries
                .insert(key, expected.clone())
                .is_some_and(|old| old != expected)
            {
                return Err(corrupt_record());
            }
        }
        for record in scope.metadata_dependencies() {
            let pointer = self.archive_metadata_pointer(record)?;
            let expected = ArchivePublicationDependency::Metadata(pointer.clone());
            if entries
                .insert(pointer.key.clone(), expected.clone())
                .is_some_and(|old| old != expected)
            {
                return Err(corrupt_record());
            }
        }
        Ok(entries.into_iter().collect())
    }

    pub(super) fn require_archive_original(
        &self,
        scope: &ArchivePublicationScope,
        event: &CanonicalEvent,
    ) -> StorageResult<()> {
        let original = event.as_ref();
        if !scope.dependencies().iter().any(|(kind, expected)| {
            *kind == ArchiveKind::Event
                && expected.event == original.id
                && expected.digest == original.digest
                && expected.lineage == event.lineage()
        }) {
            return Err(corrupt_record());
        }
        if let ArchivePublicationScope::Source(source) = scope {
            for link in source
                .events
                .iter()
                .filter(|link| link.event == original.id)
            {
                link.require_event(event, &source.origin)
                    .map_err(|_| corrupt_record())?;
            }
        }
        if let ArchivePublicationScope::Imported(imported) = scope {
            for retained in imported
                .links
                .iter()
                .filter(|retained| retained.link.event == original.id)
            {
                retained
                    .link
                    .require_event(event, &retained.origin)
                    .map_err(|_| corrupt_record())?;
            }
        }
        if let ArchivePublicationScope::RetainedLink(retained) = scope {
            retained
                .link
                .require_event(event, &retained.origin)
                .map_err(|_| corrupt_record())?;
        }
        if let ArchivePublicationScope::Acceptance(accepted) = scope {
            self.require_acceptance_binding(
                (&accepted.receipt, &accepted.delivery),
                (&original.id, event),
            )?;
        }
        if let ArchivePublicationScope::Receiver(pointer) = scope
            && pointer.receiver.journal_partition != self.select_delivery_partition(event.into())
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(super) fn require_archive_acceptance(
        &self,
        row: &DurableRecord,
        expected: &ArchiveEventDependency,
    ) -> StorageResult<()> {
        let accepted = self.decode_native_acceptance(row)?;
        if accepted.receipt.event != expected.event
            || accepted.receipt.digest != expected.digest
            || accepted.delivery.lineage != expected.lineage
            || expected
                .delivery
                .as_ref()
                .is_some_and(|delivery| *delivery != accepted.delivery)
            || expected
                .receipt
                .as_ref()
                .is_some_and(|receipt| *receipt != accepted.receipt)
        {
            return Err(corrupt_record());
        }
        Ok(())
    }
}
