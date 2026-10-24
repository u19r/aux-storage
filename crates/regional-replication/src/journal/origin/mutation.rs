use replication_types::{
    configuration::{installed::RegionalTablePhase, table::RegionalMode},
    event::{
        content::{MutationCause, SourceImagesRef},
        identity::CanonicalItemKey,
    },
    identity::{journal_partition::JournalPartition, origin::MutationOrdinal},
};
use storage_provider::AcceptedMutationRef;
use storage_types::{BoundTableInfo, ItemProvenance, KeyAttributes, StorageResult};

use crate::{
    journal::{JournalStore, TableStateObservation},
    native_record::corrupt_record,
};

pub struct OriginMutation<'accepted> {
    pub table: &'accepted TableStateObservation,
    pub mutation: AcceptedMutationRef<'accepted>,
    pub cause: MutationCause,
    pub ordinal: MutationOrdinal,
}

pub(super) struct PreparedOriginMutation<'accepted> {
    pub table: &'accepted TableStateObservation,
    pub key: &'accepted KeyAttributes,
    pub canonical_key: CanonicalItemKey,
    pub images: SourceImagesRef<'accepted>,
    pub predecessor: Option<super::predecessor::OriginPredecessor>,
    pub configuration_parent: Option<replication_types::event::identity::CanonicalEventId>,
    pub partition: JournalPartition,
    pub cause: MutationCause,
    pub ordinal: MutationOrdinal,
}

pub(super) struct MutationView<'accepted> {
    pub physical: &'accepted BoundTableInfo,
    pub key: &'accepted KeyAttributes,
    pub images: SourceImagesRef<'accepted>,
    pub provenance: Option<&'accepted ItemProvenance>,
    pub source: Option<storage_types::StoredImageSource>,
}

impl<'accepted> OriginMutation<'accepted> {
    pub(super) fn view(&self) -> StorageResult<MutationView<'accepted>> {
        let view = match &self.mutation {
            AcceptedMutationRef::Upsert(item)
                if matches!(self.cause, MutationCause::Put | MutationCause::Update) =>
            {
                MutationView {
                    physical: &item.table,
                    key: &item.key,
                    provenance: item.old_provenance.as_ref(),
                    source: item.old_item.as_ref().and_then(|item| item.stored_source()),
                    images: SourceImagesRef {
                        old: item.old_item.as_deref(),
                        new: Some(&item.new_item),
                        old_indexers: item.old_item.as_ref().map(|_| item.old_indexers.as_slice()),
                        new_indexers: Some(item.declaration.names()),
                    },
                }
            }
            AcceptedMutationRef::Delete(item) if self.cause == MutationCause::Delete => {
                MutationView {
                    physical: &item.table,
                    key: &item.key,
                    provenance: item.old_provenance.as_ref(),
                    source: item.old_item.as_ref().and_then(|item| item.stored_source()),
                    images: SourceImagesRef {
                        old: item.old_item.as_deref(),
                        new: None,
                        old_indexers: item.old_item.as_ref().map(|_| item.old_indexers.as_slice()),
                        new_indexers: None,
                    },
                }
            }
            _ => return Err(corrupt_record()),
        };
        Ok(view)
    }
}

impl TableStateObservation {
    pub(super) fn require_origination(
        &self,
        journal: &JournalStore,
        physical: &BoundTableInfo,
    ) -> StorageResult<()> {
        let state = self.state();
        if !matches!(
            state.phase,
            RegionalTablePhase::Active | RegionalTablePhase::Capturing
        ) || state.physical != physical.history_id
            || state.name != physical.table_name
        {
            return Err(corrupt_record());
        }
        match (&state.configuration.regional, state.phase) {
            (RegionalMode::Global { members, .. }, RegionalTablePhase::Active)
                if members.contains(&journal.region) => {}
            (RegionalMode::Local, RegionalTablePhase::Capturing) => {}
            _ => return Err(corrupt_record()),
        }
        journal.require_local_source(state)?;
        Ok(())
    }
}
