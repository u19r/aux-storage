//! Hot manifest receipt publication and release join the caller's native
//! attempt.

use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, RecordChange},
};
use storage_types::{ReadProtection, StorageResult};
use stream_provider::retained_image::{
    RetainedImageManifest, RetainedOwnerKind, RetainedOwnerRecord,
};

use super::JournalStore;
use crate::{archive::ArchiveKind, native_record::corrupt_record};

impl JournalStore {
    pub(in crate::journal) async fn retain_original_owner<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (
            &RetainedOwnerRecord,
            &infrastructure_provider::record::DurableRecordKey,
        ),
    ) -> StorageResult<()> {
        self.retain_original_owners(attempt, &[input]).await
    }

    pub(in crate::journal) async fn retain_original_owners<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: &[(
            &RetainedOwnerRecord,
            &infrastructure_provider::record::DurableRecordKey,
        )],
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let keys = input
                .iter()
                .map(|(owner, _)| owner.key())
                .collect::<StorageResult<Vec<_>>>()?;
            if keys.iter().collect::<std::collections::BTreeSet<_>>().len() != keys.len() {
                return Err(corrupt_record());
            }
            let rows = self
                .records
                .load(attempt, &keys, ReadProtection::Conflict)
                .await?;
            let mut changes = Vec::new();
            let mut created = Vec::new();
            let mut existing = Vec::new();
            let mut sources = Vec::new();
            for ((owner, locator), row) in input.iter().zip(rows) {
                if let Some(row) = row {
                    owner
                        .require_observation(&row, self.records.limits.max_record_bytes().get())
                        .map_err(|_| corrupt_record())?;
                    existing.push((*owner).clone());
                    sources.push((*locator).clone());
                } else {
                    changes
                        .push(owner.prepare_change(self.records.limits.max_record_bytes().get())?);
                    created.push(*owner);
                }
            }
            let sources = self
                .records
                .load(attempt, &sources, ReadProtection::Conflict)
                .await?;
            for (owner, source) in existing.iter().zip(sources) {
                owner
                    .require_descriptor(&source.ok_or_else(corrupt_record)?)
                    .map_err(|_| corrupt_record())?;
            }
            RetainedOwnerRecord::require_graph_batch(attempt, &existing).await?;
            self.records.store(attempt, &changes).await?;
            for owner in created {
                owner.stage_registration(attempt)?;
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) fn prepare_original_owner(
        &self,
        row: &DurableRecord,
        manifest: &RetainedImageManifest,
    ) -> StorageResult<Option<RetainedOwnerRecord>> {
        let kind = match ArchiveKind::from_key(&row.key) {
            Some(ArchiveKind::Event) => RetainedOwnerKind::Event,
            Some(ArchiveKind::Baseline) => RetainedOwnerKind::Baseline,
            _ => return Err(corrupt_record()),
        };
        if row.revision != storage_types::Revision::INITIAL {
            return Err(corrupt_record());
        }
        manifest.require_causal_scope(&row.key)?;
        manifest.prepare_owner_record(kind, row.key.clone(), &row.bytes)
    }

    pub(in crate::journal) async fn release_original_owner<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (&DurableRecord, usize),
    ) -> StorageResult<Option<RecordChange>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (row, original_length) = input;
            if !ArchiveKind::from_key(&row.key).is_some_and(ArchiveKind::is_payload) {
                return Ok(None);
            }
            let maximum = RetainedImageManifest::metadata_length_bound(original_length)?;
            let metadata =
                stream_provider::retained_image::RetainedPayload::decode(&row.bytes, maximum)?;
            let manifest = RetainedImageManifest::decode(&metadata, original_length)?;
            if manifest.original_length() != original_length {
                return Err(corrupt_record());
            }
            let Some(owner) = self.prepare_original_owner(row, &manifest)? else {
                return Ok(None);
            };
            let observed = self
                .records
                .load(attempt, &[owner.key()?], ReadProtection::Conflict)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            owner
                .require_observation(&observed, self.records.limits.max_record_bytes().get())
                .map_err(|_| corrupt_record())?;
            owner.stage_retirement(attempt)?;
            Ok(Some(RecordChange::Delete {
                key: observed.key,
                expected: observed.revision,
            }))
        }
        .await;
        settlement.complete(result)
    }
}

impl JournalStore {
    pub(in crate::journal) async fn qualify_original_owners<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        rows: &[DurableRecord],
        manifests: &[RetainedImageManifest],
    ) -> StorageResult<Vec<Option<crate::archive::ArchivePointer>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if rows.len() != manifests.len() {
                return Err(corrupt_record());
            }
            let owners = rows
                .iter()
                .zip(manifests)
                .enumerate()
                .filter_map(|(slot, (row, manifest))| {
                    self.prepare_original_owner(row, manifest)
                        .transpose()
                        .map(|owner| owner.map(|owner| (slot, owner)))
                })
                .collect::<StorageResult<Vec<_>>>()?;
            let keys = owners
                .iter()
                .map(|(_, owner)| owner.key())
                .collect::<StorageResult<Vec<_>>>()?;
            let (hot_owners, missing) = self
                .load_hot_original_owners(attempt, &keys, owners)
                .await?;
            RetainedOwnerRecord::require_graph_batch(attempt, &hot_owners).await?;
            self.qualify_archived_owners(attempt, (rows, manifests, missing))
                .await
        }
        .await;
        settlement.complete(result)
    }

    async fn load_hot_original_owners<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        keys: &[infrastructure_provider::record::DurableRecordKey],
        owners: Vec<(usize, RetainedOwnerRecord)>,
    ) -> StorageResult<(Vec<RetainedOwnerRecord>, Vec<usize>)> {
        let waves = self.records.read_waves(keys, |key| key)?;
        let protection = attempt.read_protection().shared();
        let mut owners = owners.into_iter();
        let mut missing = Vec::new();
        let mut hot_owners = Vec::new();
        for wave in waves {
            let observed = self.records.load(attempt, &keys[wave], protection).await?;
            for observed in observed {
                let (slot, owner) = owners.next().ok_or_else(corrupt_record)?;
                match observed {
                    Some(row) => {
                        owner
                            .require_observation(&row, self.records.limits.max_record_bytes().get())
                            .map_err(|_| corrupt_record())?;
                        hot_owners.push(owner);
                    }
                    None => missing.push(slot),
                }
            }
        }
        if owners.next().is_some() {
            return Err(corrupt_record());
        }
        Ok((hot_owners, missing))
    }

    async fn qualify_archived_owners<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (&[DurableRecord], &[RetainedImageManifest], Vec<usize>),
    ) -> StorageResult<Vec<Option<crate::archive::ArchivePointer>>> {
        let (rows, manifests, missing) = input;
        let protection = attempt.read_protection().shared();
        let mut pointers = vec![None; rows.len()];
        if missing.is_empty() {
            return Ok(pointers);
        }
        if self.archive.get().is_none() {
            return Err(corrupt_record());
        }
        let keys = missing
            .iter()
            .map(|slot| rows[*slot].key.clone())
            .collect::<Vec<_>>();
        let waves = self.records.read_waves(&keys, |key| key)?;
        for wave in waves {
            let hot = self.records.load(attempt, &keys[wave], protection).await?;
            if hot.iter().any(Option::is_some) {
                return Err(corrupt_record());
            }
        }
        let keys = keys
            .iter()
            .map(|key| self.archive_pointer_key(key))
            .collect::<StorageResult<Vec<_>>>()?;
        let waves = self.records.read_waves(&keys, |key| key)?;
        for wave in waves {
            let observed = self
                .records
                .load(attempt, &keys[wave.clone()], protection)
                .await?;
            for (slot, observed) in missing[wave].iter().copied().zip(observed) {
                let pointer = self.decode_archive_pointer(&observed.ok_or_else(corrupt_record)?)?;
                pointer.require_record(&rows[slot])?;
                if pointer.length != manifests[slot].original_length() {
                    return Err(corrupt_record());
                }
                pointers[slot] = Some(pointer);
            }
        }
        Ok(pointers)
    }
}
