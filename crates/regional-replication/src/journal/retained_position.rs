//! Complete accepted table selection at an immutable original position.

use std::collections::BTreeMap;

use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey, RecordAddress, RecordChange},
};
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, regional_application::observation::ImportPageScope,
};
use replication_types::{
    event::interval::{CanonicalEventLink, PositionCoverage, SourceIntervalManifest, TablePageId},
    identity::{
        origin::{OriginId, OriginPosition},
        resource::{TableIdentity, TableLineage},
    },
    protocol::CanonicalDigest,
    regional_import::RegionalImport,
};
use serde::{Deserialize, Serialize};
use storage_types::{ReadProtection, Revision, StorageResult};

use super::JournalStore;
use crate::{
    constants::{IMPORT_MANIFEST_SUFFIX, IMPORT_PAGE_SUFFIX, RETAINED_POSITION_SUFFIX},
    native_record::corrupt_record,
    source_frame::SourceFrameRequest,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::journal) struct RetainedTablePosition {
    pub(in crate::journal) page: TablePageId,
    pub(in crate::journal) position: OriginPosition,
}

impl JournalStore {
    pub(in crate::journal) fn decode_retained_position_record(
        &self,
        row: &DurableRecord,
    ) -> StorageResult<RetainedTablePosition> {
        let selected: RetainedTablePosition = self.records.decode(row)?;
        if row.revision != Revision::INITIAL
            || selected.page.fragment.is_some()
            || selected.page.selection.is_some()
            || !selected.page.range.contains(selected.position)
            || row.key
                != self.retained_position_key(
                    (
                        &selected.page.origin,
                        selected.page.table,
                        selected.page.lineage,
                    ),
                    selected.position,
                )?
        {
            return Err(corrupt_record());
        }
        Ok(selected)
    }

    fn retained_position_key(
        &self,
        identity: (&OriginId, TableIdentity, TableLineage),
        position: OriginPosition,
    ) -> StorageResult<DurableRecordKey> {
        let mut address: RecordAddress = self
            .identity_key(RETAINED_POSITION_SUFFIX, &identity)?
            .into();
        address
            .suffix
            .extend_from_slice(&u64::from(position).to_be_bytes());
        address.try_into()
    }

    pub(in crate::journal) async fn prepare_import_positions<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        import: &RegionalImport,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<Vec<RecordChange>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let mut changes = Vec::new();
            for binding in import.bindings() {
                let page = ImportPageScope::Complete
                    .page_id(import, binding)
                    .map_err(|_| corrupt_record())?;
                changes.extend(
                    self.prepare_table_positions(attempt, page, (import.manifest(), codec))
                        .await?,
                );
            }
            Ok(changes)
        }
        .await;
        settlement.complete(result)
    }

    async fn prepare_table_positions<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        page: TablePageId,
        input: (&SourceIntervalManifest, &CanonicalJsonCodec),
    ) -> StorageResult<Vec<RecordChange>> {
        let selected = (u64::from(page.range.after()) + 1..=u64::from(page.range.through()))
            .map(|position| RetainedTablePosition {
                page: page.clone(),
                position: position.into(),
            })
            .collect::<Vec<_>>();
        let keys = selected
            .iter()
            .map(|position| {
                self.retained_position_key(
                    (&page.origin, page.table, page.lineage),
                    position.position,
                )
            })
            .collect::<StorageResult<Vec<_>>>()?;
        let rows = self
            .load_immutable_waves(attempt, &keys, ReadProtection::Conflict)
            .await?;
        if rows.len() != selected.len() {
            return Err(corrupt_record());
        }
        let mut existing = Vec::new();
        let mut changes = Vec::new();
        for ((selected, key), row) in selected.iter().zip(keys).zip(rows) {
            let Some(row) = row else {
                changes.push(self.records.prepare(key, None, selected)?);
                continue;
            };
            let old = self.decode_retained_position_record(&row)?;
            if old.position != selected.position
                || old.page.origin != page.origin
                || old.page.table != page.table
                || old.page.lineage != page.lineage
            {
                return Err(corrupt_record());
            }
            existing.push(old);
        }
        let manifests = self
            .load_position_manifests(attempt, &existing.iter().collect::<Vec<_>>(), input.1)
            .await?;
        for old in &existing {
            let manifest = manifests
                .get(&old.page.manifest)
                .ok_or_else(corrupt_record)?;
            let incoming = RetainedTablePosition {
                page: page.clone(),
                position: old.position,
            };
            if select_position_links(manifest, old) != select_position_links(input.0, &incoming) {
                return Err(corrupt_record());
            }
        }
        Ok(changes)
    }

    async fn load_position_manifests<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        positions: &[&RetainedTablePosition],
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<BTreeMap<CanonicalDigest, SourceIntervalManifest>> {
        let mut pages = BTreeMap::new();
        for selected in positions {
            if let Some(previous) = pages.insert(selected.page.manifest, &selected.page)
                && previous != &selected.page
            {
                return Err(corrupt_record());
            }
        }
        let keys = pages
            .iter()
            .map(|(digest, page)| {
                Ok([
                    self.identity_key(IMPORT_MANIFEST_SUFFIX, digest)?,
                    self.identity_key(IMPORT_PAGE_SUFFIX, page)?,
                ])
            })
            .collect::<StorageResult<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let rows = self
            .load_immutable_waves(attempt, &keys, ReadProtection::Snapshot)
            .await?;
        if rows.len() != keys.len() {
            return Err(corrupt_record());
        }
        let mut manifests = BTreeMap::new();
        for ((digest, id), pair) in pages.into_iter().zip(rows.as_chunks::<2>().0.iter()) {
            let manifest = self.decode_import_manifest_record(
                pair[0].as_ref().ok_or_else(corrupt_record)?,
                codec,
            )?;
            let page =
                self.decode_import_page_record(pair[1].as_ref().ok_or_else(corrupt_record)?)?;
            if manifest.digest != digest || &page.page != id {
                return Err(corrupt_record());
            }
            page.require_manifest(&manifest)
                .map_err(|_| corrupt_record())?;
            manifests.insert(digest, manifest);
        }
        if positions.iter().any(|selected| {
            manifests
                .get(&selected.page.manifest)
                .is_none_or(|manifest| !manifest.range.contains(selected.position))
        }) {
            return Err(corrupt_record());
        }
        Ok(manifests)
    }

    pub(crate) async fn select_retained_source_manifest<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &SourceFrameRequest,
        input: (
            &CanonicalJsonCodec,
            &replication_provider::runtime_configuration::limits::WorkLimits,
        ),
    ) -> Result<Option<SourceIntervalManifest>, replication_provider::delivery::error::DeliveryError>
    {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let [(table, lineage)] = request.tables.as_slice() else {
                return Err(corrupt_record().into());
            };
            let keys = (u64::from(request.range.after()) + 1..=u64::from(request.range.through()))
                .map(|position| {
                    self.retained_position_key((&request.origin, *table, *lineage), position.into())
                })
                .collect::<StorageResult<Vec<_>>>()?;
            let mut selection = crate::source_frame::SourceManifestSelection::new(request, input.1);
            for wave in self.records.read_waves(&keys, |key| key)? {
                let rows = self
                    .load_immutable_waves(attempt, &keys[wave.clone()], ReadProtection::Snapshot)
                    .await?;
                let positions = wave
                    .zip(rows)
                    .map(|(index, row)| {
                        let row = row.ok_or_else(corrupt_record)?;
                        let selected = self.decode_retained_position_record(&row)?;
                        self.require_selected_position(&selected, request, index)?;
                        Ok((selected, CanonicalDigest::of(&row.bytes)))
                    })
                    .collect::<StorageResult<Vec<_>>>()?;
                if !self
                    .append_retained_position_wave(attempt, &mut selection, positions, input.0)
                    .await?
                {
                    return Ok(None);
                }
            }
            Ok(Some(selection.into_manifest()))
        }
        .await;
        settlement.complete(result)
    }

    fn require_selected_position(
        &self,
        selected: &RetainedTablePosition,
        request: &SourceFrameRequest,
        index: usize,
    ) -> StorageResult<()> {
        let [(table, lineage)] = request.tables.as_slice() else {
            return Err(corrupt_record());
        };
        if selected.position != (u64::from(request.range.after()) + index as u64 + 1).into()
            || selected.page.origin != request.origin
            || selected.page.table != *table
            || selected.page.lineage != *lineage
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    async fn append_retained_position_wave<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        selection: &mut crate::source_frame::SourceManifestSelection,
        positions: Vec<(RetainedTablePosition, CanonicalDigest)>,
        codec: &CanonicalJsonCodec,
    ) -> Result<bool, replication_provider::delivery::error::DeliveryError> {
        let selected = positions
            .iter()
            .map(|(position, _)| position)
            .collect::<Vec<_>>();
        let originals = self
            .load_position_manifests(attempt, &selected, codec)
            .await?;
        for (position, receipt) in positions {
            let original = originals
                .get(&position.page.manifest)
                .ok_or_else(corrupt_record)?;
            let links = select_position_links(original, &position);
            let coverage = select_position_coverage((original, &position), &links, receipt)?;
            if !selection
                .append_position((links.into_iter().cloned().collect(), coverage), codec)?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

fn select_position_links<'manifest>(
    manifest: &'manifest SourceIntervalManifest,
    selected: &RetainedTablePosition,
) -> Vec<&'manifest CanonicalEventLink> {
    manifest
        .links
        .iter()
        .filter(|link| {
            link.position == selected.position
                && link.table == selected.page.table
                && link.lineage == selected.page.lineage
        })
        .collect()
}

fn select_position_coverage(
    input: (&SourceIntervalManifest, &RetainedTablePosition),
    links: &[&CanonicalEventLink],
    receipt: CanonicalDigest,
) -> StorageResult<PositionCoverage> {
    let (manifest, selected) = input;
    if !links.is_empty() {
        return Ok(PositionCoverage::Selected {
            position: selected.position,
            ordinals: links.iter().map(|link| link.ordinal).collect(),
        });
    }
    let offset =
        usize::try_from(u64::from(selected.position) - u64::from(manifest.range.after()) - 1)
            .map_err(|_| corrupt_record())?;
    let original = manifest.coverage.get(offset).ok_or_else(corrupt_record)?;
    if matches!(original, PositionCoverage::Excluded { .. }) {
        return Ok(original.clone());
    }
    Ok(PositionCoverage::Excluded {
        position: selected.position,
        sealed_receipt: receipt,
        reason: replication_types::event::interval::ExclusionReason::UnselectedTables,
    })
}
