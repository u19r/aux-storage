use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, RecordChange},
};
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec,
    regional_application::observation::{ImportPageScope, TablePageAcceptance},
};
use replication_types::{event::interval::TablePageId, regional_import::RegionalImport};
use storage_types::{ReadProtection, Revision, StorageResult};

use super::{JournalStore, acceptance_batch::AcceptanceReadSelection};
use crate::{constants::IMPORT_PAGE_SUFFIX, native_record::corrupt_record};

impl JournalStore {
    pub(in crate::journal) fn decode_import_page_record(
        &self,
        row: &DurableRecord,
    ) -> StorageResult<TablePageAcceptance> {
        let page: TablePageAcceptance = self.records.decode(row)?;
        page.require_selection().map_err(|_| corrupt_record())?;
        if row.revision != Revision::INITIAL
            || row.key != self.identity_key(IMPORT_PAGE_SUFFIX, &page.page)?
        {
            return Err(corrupt_record());
        }
        Ok(page)
    }

    /// Complete pages are sealed only after every original event receipt exists
    /// in this same native attempt. The caller still owes commit completion.
    pub async fn seal_import_pages<A: stream_provider::retained_image::RetainedOwnershipAttempt>(
        &self,
        attempt: &mut A,
        input: (&RegionalImport, &[TablePageAcceptance]),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<Option<Vec<TablePageAcceptance>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (import, pages) = input;
            ImportPageScope::Selected
                .require_pages(import, pages)
                .map_err(|_| corrupt_record())?;
            let count = pages
                .len()
                .checked_add(import.bindings().len())
                .and_then(|count| count.checked_add(1))
                .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            let mut changes = Vec::new();
            changes
                .try_reserve_exact(count)
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            self.verify_page_acceptances(attempt, (pages, import.manifest()), codec)
                .await?;
            changes.extend(self.prepare_import_pages(attempt, pages).await?);
            if let Some(change) = self
                .prepare_import_manifest(attempt, import.manifest(), codec)
                .await?
            {
                changes.push(change);
            }
            let completed = if import.fragment().is_some() {
                let Some(pages) = self
                    .load_complete_fragment_pages(attempt, import, codec)
                    .await?
                else {
                    self.records.store(attempt, &changes).await?;
                    self.retain_fragment_obligations(attempt, import).await?;
                    return Ok(None);
                };
                changes.extend(self.prepare_import_pages(attempt, &pages).await?);
                pages
            } else {
                pages.to_vec()
            };
            self.records.store(attempt, &changes).await?;
            self.seal_complete_import(attempt, import, codec).await?;
            if import.fragment().is_some() {
                self.finish_fragment_obligations(attempt, import).await?;
            }
            Ok(Some(completed))
        }
        .await;
        settlement.complete(result)
    }

    async fn seal_complete_import<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        import: &RegionalImport,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<()> {
        let mut changes = self
            .prepare_import_positions(attempt, import, codec)
            .await?;
        changes.extend(self.prepare_import_coverage(attempt, import).await?);
        let links = import.manifest().links.iter().filter(|link| {
            import
                .bindings()
                .binary_search_by_key(&link.table, |binding| binding.source_table)
                .is_ok()
        });
        changes.extend(
            self.prepare_retained_publications(
                attempt,
                links.map(|link| (&import.manifest().origin, link)),
            )
            .await?,
        );
        self.records.store(attempt, &changes).await
    }

    /// An absent receipt remains unresolved. Run this in a fresh native read
    /// after an unknown commit; it never grants permission to retry a mutation.
    pub(crate) async fn load_import_pages<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        import: &RegionalImport,
        codec: &CanonicalJsonCodec,
        scope: ImportPageScope,
    ) -> StorageResult<Option<Vec<TablePageAcceptance>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let result = async {
            let attempt = settlement.as_mut();
            let mut ids = Vec::new();
            ids.try_reserve_exact(import.bindings().len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            for binding in import.bindings() {
                ids.push(
                    scope
                        .page_id(import, binding)
                        .map_err(|_| corrupt_record())?,
                );
            }
            let Some(pages) = self.load_page_records(attempt, &ids).await? else {
                return Ok(None);
            };
            scope
                .require_pages(import, &pages)
                .map_err(|_| corrupt_record())?;
            self.require_import_manifest(attempt, import.manifest(), codec)
                .await?;
            self.verify_page_acceptances(attempt, (&pages, import.manifest()), codec)
                .await?;
            if scope == ImportPageScope::Complete || import.fragment().is_none() {
                self.require_complete_import_coverage(attempt, import)
                    .await?;
            }
            Ok(Some(pages))
        }
        .await;
        settlement.complete(result)
    }

    async fn load_page_records<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        ids: &[TablePageId],
    ) -> StorageResult<Option<Vec<TablePageAcceptance>>> {
        let mut keys = Vec::new();
        let mut pages = Vec::new();
        keys.try_reserve_exact(ids.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        pages
            .try_reserve_exact(ids.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        for id in ids {
            keys.push(self.identity_key(IMPORT_PAGE_SUFFIX, id)?);
        }
        let rows = self
            .load_immutable_waves(attempt, &keys, ReadProtection::Snapshot)
            .await?;
        if rows.len() != ids.len() {
            return Err(corrupt_record());
        }
        let is_incomplete = rows.iter().any(Option::is_none);
        for (row, id) in rows.iter().zip(ids) {
            let Some(row) = row else {
                continue;
            };
            let page = self.decode_import_page_record(row)?;
            if &page.page != id {
                return Err(corrupt_record());
            }
            pages.push(page);
        }
        if is_incomplete {
            return Ok(None);
        }
        Ok(Some(pages))
    }

    pub(in crate::journal) async fn prepare_import_pages<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        pages: &[TablePageAcceptance],
    ) -> StorageResult<Vec<RecordChange>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let mut inventory = ImportPagePreparation::try_new(self, pages)?;
            let rows = self
                .load_immutable_waves(attempt, &inventory.keys, ReadProtection::Conflict)
                .await?;
            if rows.len() != inventory.entries.len() {
                return Err(corrupt_record());
            }
            for ((page, prepared), row) in inventory.entries.into_iter().zip(rows) {
                if let Some(row) = row {
                    if self.decode_import_page_record(&row)? != *page {
                        return Err(corrupt_record());
                    }
                } else {
                    inventory.changes.push(prepared);
                }
            }
            Ok(inventory.changes)
        }
        .await;
        settlement.complete(result)
    }

    async fn verify_page_acceptances<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (
            &[TablePageAcceptance],
            &replication_types::event::interval::SourceIntervalManifest,
        ),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<()> {
        let (pages, manifest) = input;
        let count = pages.iter().try_fold(0usize, |count, page| {
            count
                .checked_add(page.events.len())
                .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)
        })?;
        let mut expected = Vec::new();
        let mut identities = Vec::new();
        expected
            .try_reserve_exact(count)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        identities
            .try_reserve_exact(count)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        for page in pages {
            let links = page
                .require_manifest(manifest)
                .map_err(|_| corrupt_record())?;
            expected.extend(page.events.iter().zip(links));
        }
        identities.extend(expected.iter().map(|(receipt, _)| &receipt.event));
        let selection = AcceptanceReadSelection::SourceProof {
            identities: &identities,
            maximum: self.source_proof_allowance(manifest, codec)?,
        };
        let accepted = self
            .load_acceptance_inventory(attempt, (&selection, ReadProtection::Snapshot), codec)
            .await
            .map_err(storage_types::StorageError::from)?
            .ok_or_else(corrupt_record)?;
        if accepted.len() != expected.len() {
            return Err(corrupt_record());
        }
        for ((receipt, link), accepted) in expected.into_iter().zip(accepted) {
            let (accepted, _) = accepted.ok_or_else(corrupt_record)?;
            link.require_event(&accepted.event, &manifest.origin)
                .map_err(|_| corrupt_record())?;
            if accepted.receipt != *receipt {
                return Err(corrupt_record());
            }
        }
        Ok(())
    }
}

/// Immutable page writes are encoded before their conflict observations. Native
/// replay either proves exact equality or consumes the already prepared change.
struct ImportPagePreparation<'input> {
    keys: Vec<infrastructure_provider::record::DurableRecordKey>,
    entries: Vec<(&'input TablePageAcceptance, RecordChange)>,
    changes: Vec<RecordChange>,
}

impl<'input> ImportPagePreparation<'input> {
    fn try_new(
        journal: &JournalStore,
        pages: &'input [TablePageAcceptance],
    ) -> StorageResult<Self> {
        let mut inventory = Vec::new();
        inventory
            .try_reserve_exact(pages.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        for page in pages {
            inventory.push((journal.identity_key(IMPORT_PAGE_SUFFIX, &page.page)?, page));
        }
        inventory.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        for adjacent in inventory.windows(2) {
            if adjacent[0].0 == adjacent[1].0 && adjacent[0].1 != adjacent[1].1 {
                return Err(corrupt_record());
            }
        }
        inventory.dedup_by(|right, left| right.0 == left.0);
        let mut prepared = Self {
            keys: Vec::new(),
            entries: Vec::new(),
            changes: Vec::new(),
        };
        prepared
            .keys
            .try_reserve_exact(inventory.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        prepared
            .entries
            .try_reserve_exact(inventory.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        prepared
            .changes
            .try_reserve_exact(inventory.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        for (key, page) in inventory {
            prepared
                .keys
                .push(infrastructure_provider::record::DurableRecordKey::try_from(
                    &key,
                )?);
            prepared
                .entries
                .push((page, journal.records.prepare(key, None, page)?));
        }
        Ok(prepared)
    }
}
