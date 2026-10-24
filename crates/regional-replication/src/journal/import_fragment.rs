use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec,
    regional_application::observation::{ImportPageScope, StoredAcceptance, TablePageAcceptance},
};
use replication_types::{
    event::{identity::CanonicalEventId, interval::CanonicalEventLink},
    regional_import::RegionalImport,
};
use storage_types::{InfrastructureResultKind, StorageResult};

use super::{JournalStore, acceptance_batch::AcceptanceReadSelection};
use crate::native_record::corrupt_record;

impl JournalStore {
    /// A fragment receipt never establishes coverage by itself. Reconstruct
    /// complete table receipts only from exact accepted native original events.
    pub(in crate::journal) async fn load_complete_fragment_pages<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        import: &RegionalImport,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<Option<Vec<TablePageAcceptance>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let result = async {
            let prepared = CompleteFragmentPreparation::try_new(import)?;
            let selection = AcceptanceReadSelection::SourceProof {
                identities: &prepared.identities,
                maximum: self.source_proof_allowance(import.manifest(), codec)?,
            };
            let accepted = self
                .load_acceptance_inventory(
                    settlement.as_mut(),
                    (&selection, storage_types::ReadProtection::Shared),
                    codec,
                )
                .await
                .map_err(storage_types::StorageError::from)?
                .ok_or_else(corrupt_record)?;
            prepared.bind_acceptances(import, accepted)
        }
        .await;
        settlement.complete(result)
    }
}

/// Every page has space for its complete source declaration before originals
/// are hydrated. An absent sibling does not waive validation of present events.
struct CompleteFragmentPreparation<'input> {
    pages: Vec<TablePageAcceptance>,
    selected: Vec<(usize, &'input CanonicalEventLink)>,
    identities: Vec<&'input CanonicalEventId>,
}

impl<'input> CompleteFragmentPreparation<'input> {
    fn try_new(import: &'input RegionalImport) -> StorageResult<Self> {
        let mut prepared = Self {
            pages: Vec::new(),
            selected: Vec::new(),
            identities: Vec::new(),
        };
        prepared
            .pages
            .try_reserve_exact(import.bindings().len())
            .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
        prepared
            .selected
            .try_reserve_exact(import.manifest().links.len())
            .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
        prepared
            .identities
            .try_reserve_exact(import.manifest().links.len())
            .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
        for binding in import.bindings() {
            prepared.pages.push(TablePageAcceptance {
                page: ImportPageScope::Complete
                    .page_id(import, binding)
                    .map_err(|_| corrupt_record())?,
                events: Vec::new(),
                selected_tables: None,
            });
        }
        for link in &import.manifest().links {
            let Ok(slot) = import
                .bindings()
                .binary_search_by_key(&link.table, |binding| binding.source_table)
            else {
                continue;
            };
            prepared.selected.push((slot, link));
            prepared.identities.push(&link.event);
        }
        for (slot, page) in prepared.pages.iter_mut().enumerate() {
            let count = prepared
                .selected
                .iter()
                .filter(|(index, _)| *index == slot)
                .count();
            page.events
                .try_reserve_exact(count)
                .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
        }
        Ok(prepared)
    }

    fn bind_acceptances(
        mut self,
        import: &RegionalImport,
        accepted: Vec<Option<(StoredAcceptance, crate::history::view::CanonicalViewPointer)>>,
    ) -> StorageResult<Option<Vec<TablePageAcceptance>>> {
        if accepted.len() != self.selected.len() {
            return Err(corrupt_record());
        }
        let is_incomplete = accepted.iter().any(Option::is_none);
        for ((slot, link), accepted) in self.selected.into_iter().zip(accepted) {
            let Some((accepted, _)) = accepted else {
                continue;
            };
            link.require_event(&accepted.event, &import.manifest().origin)
                .map_err(|_| corrupt_record())?;
            if accepted.receipt.event != link.event || accepted.receipt.digest != link.digest {
                return Err(corrupt_record());
            }
            self.pages[slot].events.push(accepted.receipt);
        }
        if is_incomplete {
            return Ok(None);
        }
        Ok(Some(self.pages))
    }
}
