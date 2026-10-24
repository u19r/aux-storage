//! Pending partial imports participate in the complete native custody
//! inventory.
use std::collections::BTreeSet;

use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey, RecordChange},
};
use replication_provider::regional_application::observation::ImportPageScope;
use replication_types::{
    configuration::membership::RegionalTableBinding, event::interval::TablePageId,
    identity::resource::TableIdentity, protocol::CanonicalDigest, regional_import::RegionalImport,
};
use serde::{Deserialize, Serialize};
use storage_types::{ReadProtection, Revision, StorageResult};

use super::{JournalStore, reader_hold::RetentionHoldReference};
use crate::{
    constants::{IMPORT_FRAGMENT_PENDING_SUFFIX, READER_HOLD_SUFFIX},
    history::hold_discovery::ReaderHoldObservation,
    native_record::corrupt_record,
};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::journal) struct FragmentHoldReference {
    pub local: TableIdentity,
    pub page: TablePageId,
}

impl FragmentHoldReference {
    fn try_from_import(
        import: &RegionalImport,
        binding: &RegionalTableBinding,
    ) -> StorageResult<Self> {
        Ok(Self {
            local: binding.local_table,
            page: ImportPageScope::Complete
                .page_id(import, binding)
                .map_err(|_| corrupt_record())?,
        })
    }
    fn require_complete_page(&self) -> StorageResult<()> {
        if self.page.fragment.is_some() || self.page.selection.is_some() {
            return Err(corrupt_record());
        }
        Ok(())
    }
}

impl JournalStore {
    fn fragment_obligation_key(
        &self,
        reference: &FragmentHoldReference,
    ) -> StorageResult<DurableRecordKey> {
        reference.require_complete_page()?;
        self.identity_key(
            IMPORT_FRAGMENT_PENDING_SUFFIX,
            &(reference.local, &reference.page),
        )
    }

    pub(in crate::journal) fn fragment_hold_key(
        &self,
        reference: &FragmentHoldReference,
    ) -> StorageResult<DurableRecordKey> {
        reference.require_complete_page()?;
        let mut suffix = READER_HOLD_SUFFIX.to_vec();
        suffix.push(7);
        suffix.extend_from_slice(
            CanonicalDigest::of_json(reference)
                .map_err(|_| corrupt_record())?
                .as_ref(),
        );
        self.record_key(suffix)
    }

    fn decode_fragment_obligation(
        &self,
        row: &DurableRecord,
        reference: &FragmentHoldReference,
    ) -> StorageResult<RegionalTableBinding> {
        let binding: RegionalTableBinding = self.records.decode(row)?;
        if row.revision != Revision::INITIAL
            || row.key != self.fragment_obligation_key(reference)?
            || binding.local_table != reference.local
            || binding.source_table != reference.page.table
            || binding.lineage != reference.page.lineage
            || binding.source_region != reference.page.origin.region
        {
            return Err(corrupt_record());
        }
        Ok(binding)
    }

    /// Both immutable records and retention epochs join the customer's attempt.
    pub(in crate::journal) async fn retain_fragment_obligations<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        import: &RegionalImport,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let references = import
                .bindings()
                .iter()
                .map(|binding| FragmentHoldReference::try_from_import(import, binding))
                .collect::<StorageResult<Vec<_>>>()?;
            let observed = self
                .load_fragment_obligations(attempt, &references, import.bindings())
                .await?;
            let mut changes = Vec::new();
            let mut lineages = BTreeSet::new();
            let mut growing = Vec::new();
            for ((reference, binding), observed) in
                references.iter().zip(import.bindings()).zip(observed)
            {
                if observed.is_some() {
                    continue;
                }
                let hold = RetentionHoldReference::ImportFragment(reference.clone());
                changes.push(self.records.prepare(
                    self.fragment_obligation_key(reference)?,
                    None,
                    binding,
                )?);
                changes.push(self.records.prepare(
                    self.fragment_hold_key(reference)?,
                    None,
                    &hold,
                )?);
                growing.push(
                    crate::history::source_retention::SourceRetentionRange::Fragment {
                        binding,
                        page: &reference.page,
                    },
                );
                lineages.insert(reference.page.lineage);
            }
            if !growing.is_empty() {
                self.require_reservation_admission(
                    attempt,
                    super::trim_reservation::ReservationAdmission::Sources(&growing),
                )
                .await?;
            }
            self.retain_history_epochs(attempt, &lineages).await?;
            if changes.is_empty() {
                return Ok(());
            }
            self.records.store(attempt, &changes).await
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn finish_fragment_obligations<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        import: &RegionalImport,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let references = import
                .bindings()
                .iter()
                .map(|binding| FragmentHoldReference::try_from_import(import, binding))
                .collect::<StorageResult<Vec<_>>>()?;
            let observed = self
                .load_fragment_obligations(attempt, &references, import.bindings())
                .await?;
            let mut changes =
                Vec::with_capacity(references.len().checked_mul(2).ok_or_else(corrupt_record)?);
            let mut lineages = BTreeSet::new();
            for (reference, observed) in references.iter().zip(observed) {
                let Some((marker, index)) = observed else {
                    continue;
                };
                changes.push(RecordChange::Delete {
                    key: self.fragment_obligation_key(reference)?,
                    expected: marker,
                });
                changes.push(RecordChange::Delete {
                    key: self.fragment_hold_key(reference)?,
                    expected: index,
                });
                lineages.insert(reference.page.lineage);
            }
            self.retain_history_epochs(attempt, &lineages).await?;
            if changes.is_empty() {
                return Ok(());
            }
            self.records.store(attempt, &changes).await
        }
        .await;
        settlement.complete(result)
    }

    async fn load_fragment_obligations<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        references: &[FragmentHoldReference],
        bindings: &[RegionalTableBinding],
    ) -> StorageResult<Vec<Option<(Revision, Revision)>>> {
        if references.len() != bindings.len() {
            return Err(corrupt_record());
        }
        let keys = self.fragment_inventory_keys(references)?;
        let waves = self.records.read_waves(&keys, |key| key)?;
        let mut observations = Vec::with_capacity(references.len());
        let mut marker = None;
        for wave in waves {
            let rows = self
                .records
                .load(attempt, &keys[wave.clone()], ReadProtection::Conflict)
                .await?;
            for (position, row) in wave.zip(rows) {
                let reference = &references[position / 2];
                if position % 2 == 0 {
                    marker =
                        self.fragment_marker_revision(row, reference, &bindings[position / 2])?;
                } else {
                    let index = self.fragment_hold_revision(row, reference)?;
                    observations.push(Self::require_fragment_revision_pair(marker.take(), index)?);
                }
            }
        }
        if observations.len() != references.len() || marker.is_some() {
            return Err(corrupt_record());
        }
        Ok(observations)
    }

    fn fragment_marker_revision(
        &self,
        row: Option<DurableRecord>,
        reference: &FragmentHoldReference,
        binding: &RegionalTableBinding,
    ) -> StorageResult<Option<Revision>> {
        let Some(row) = row else {
            return Ok(None);
        };
        if self.decode_fragment_obligation(&row, reference)? != *binding {
            return Err(corrupt_record());
        }
        Ok(Some(row.revision))
    }

    fn fragment_hold_revision(
        &self,
        row: Option<DurableRecord>,
        reference: &FragmentHoldReference,
    ) -> StorageResult<Option<Revision>> {
        let Some(row) = row else {
            return Ok(None);
        };
        if self.decode_retention_hold(&row)?
            != RetentionHoldReference::ImportFragment(reference.clone())
        {
            return Err(corrupt_record());
        }
        Ok(Some(row.revision))
    }

    fn require_fragment_revision_pair(
        marker: Option<Revision>,
        index: Option<Revision>,
    ) -> StorageResult<Option<(Revision, Revision)>> {
        match (marker, index) {
            (Some(marker), Some(index)) => Ok(Some((marker, index))),
            (None, None) => Ok(None),
            _ => Err(corrupt_record()),
        }
    }

    fn fragment_inventory_keys(
        &self,
        references: &[FragmentHoldReference],
    ) -> StorageResult<Vec<DurableRecordKey>> {
        let mut keys =
            Vec::with_capacity(references.len().checked_mul(2).ok_or_else(corrupt_record)?);
        for reference in references {
            keys.push(self.fragment_obligation_key(reference)?);
            keys.push(self.fragment_hold_key(reference)?);
        }
        Ok(keys)
    }

    pub(in crate::journal) async fn observe_fragment_hold<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        reference: &FragmentHoldReference,
        protection: ReadProtection,
    ) -> StorageResult<ReaderHoldObservation> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let key = self.fragment_obligation_key(reference)?;
            let row = self
                .records
                .load(attempt, &[key], protection)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let binding = self.decode_fragment_obligation(&row, reference)?;
            Ok(ReaderHoldObservation::ImportFragmentPending {
                binding,
                page: reference.page.clone(),
            })
        }
        .await;
        settlement_guard.complete(result)
    }
}
