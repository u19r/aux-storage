//! Atomic archive coverage for originals and their publication metadata.
use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey, RecordChange},
};
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::protocol::CanonicalDigest;
use serde::{Deserialize, Serialize};
use storage_types::{ReadProtection, Revision, StorageResult};

use super::publication_scope::{
    ArchiveEventDependency, ArchivePublicationDependency, ArchivePublicationScope,
};
use crate::{
    archive::{ArchiveHold, ArchiveKind, ArchivePlan, ArchivePointer, VerifiedArchive},
    journal::JournalStore,
    native_record::corrupt_record,
};

#[derive(PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchivePublicationCoverage {
    publication: ArchivePointer,
    count: usize,
    dependencies: CanonicalDigest,
}

impl JournalStore {
    pub(crate) async fn prepare_archive_dependency_recovery<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (&ArchiveHold, &ArchivePointer),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<ArchivePlan> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (hold, pointer) = input;
            self.require_archive_hold(hold)?;
            if !hold.cursor.kind.has_publication_coverage() { return Err(corrupt_record()); }
            let family = self.load_archive_state(attempt, hold.cursor.kind, ReadProtection::Snapshot).await?;
            if !matches!(&family.state, crate::archive::ArchiveState::Pending(pending) if pending == hold) { return Err(corrupt_record()); }
            let row = self.load_archive_publication_record(attempt, &hold.pointer, ReadProtection::Snapshot).await?;
            let scope = self.load_archive_publication_scope(attempt, &row, ReadProtection::Snapshot).await?;
            let entries = self.archive_publication_dependencies(&scope)?;
            let (key, expected) = entries.into_iter().find(|(key, _)| key == &pointer.key).ok_or_else(corrupt_record)?;
            self.require_archive_dependency_pointer(attempt, (pointer, &expected), ReadProtection::Snapshot).await?;
            self.prepare_publication_dependency(attempt, (hold, &scope, key, expected), (codec, ReadProtection::Snapshot)).await
        }.await;
        settlement_guard.complete(result)
    }

    pub(super) async fn prepare_archive_publication<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        hold: ArchiveHold,
        record: DurableRecord,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<ArchivePlan> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let scope = self
                .load_archive_publication_scope(attempt, &record, ReadProtection::Conflict)
                .await?;
            self.verify_archive_single_original(attempt, &scope, (codec, ReadProtection::Conflict))
                .await?;
            let entries = self.archive_publication_dependencies(&scope)?;
            let pointers = self
                .load_publication_pointers(attempt, &entries, ReadProtection::Conflict)
                .await?;
            // Originals precede acceptance publication even when native key
            // order differs.
            let missing = entries
                .iter()
                .zip(&pointers)
                .filter(|(_, pointer)| pointer.is_none())
                .min_by_key(|((key, _), _)| match ArchiveKind::from_key(key) {
                    Some(ArchiveKind::Event) => 0,
                    Some(ArchiveKind::Acceptance) => 1,
                    Some(ArchiveKind::ImportManifest) => 2,
                    Some(ArchiveKind::ImportPage) => 3,
                    _ => 4,
                });
            if let Some(((key, expected), _)) = missing {
                return self
                    .prepare_publication_dependency(
                        attempt,
                        (&hold, &scope, key.clone(), expected.clone()),
                        (codec, ReadProtection::Conflict),
                    )
                    .await;
            }
            let dependencies = pointers
                .into_iter()
                .map(|pointer| pointer.ok_or_else(corrupt_record))
                .collect::<StorageResult<Vec<_>>>()?;
            self.require_publication_dependencies(
                attempt,
                (&entries, &dependencies),
                ReadProtection::Conflict,
            )
            .await?;
            let bytes = std::sync::Arc::clone(&record.bytes);
            ArchivePlan::try_new(hold, record, bytes)?.with_publication_dependencies(dependencies)
        }
        .await;
        settlement.complete(result)
    }

    async fn prepare_publication_dependency<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (
            &ArchiveHold,
            &ArchivePublicationScope,
            DurableRecordKey,
            ArchivePublicationDependency,
        ),
        encoding: (&CanonicalJsonCodec, ReadProtection),
    ) -> StorageResult<ArchivePlan> {
        let (hold, scope, key, expected) = input;
        let (codec, protection) = encoding;
        let record = self
            .load_immutable_waves(attempt, std::slice::from_ref(&key), protection)
            .await?
            .pop()
            .flatten()
            .ok_or_else(corrupt_record)?;
        self.require_archive_metadata(&record, codec)?;
        match &expected {
            ArchivePublicationDependency::Event(event)
                if ArchiveKind::from_key(&key) == Some(ArchiveKind::Acceptance) =>
            {
                self.require_archive_acceptance(&record, event)?
            }
            ArchivePublicationDependency::Metadata(pointer) => pointer.require_record(&record)?,
            ArchivePublicationDependency::Event(_)
                if ArchiveKind::from_key(&key) == Some(ArchiveKind::Event) => {}
            _ => return Err(corrupt_record()),
        }
        if ArchiveKind::from_key(&key).is_some_and(ArchiveKind::has_publication_coverage) {
            let child = self
                .load_archive_publication_scope(attempt, &record, protection)
                .await?;
            self.verify_archive_single_original(attempt, &child, (codec, protection))
                .await?;
            let dependencies = self
                .load_publication_dependencies(attempt, &child, protection)
                .await?;
            let bytes = std::sync::Arc::clone(&record.bytes);
            return ArchivePlan::try_dependency(hold.clone(), record, bytes)?
                .with_publication_dependencies(dependencies);
        }
        let bytes = self.load_retained_bytes(attempt, &record, codec).await?;
        let event = self.decode_retained_event(&record, &bytes, codec)?;
        self.require_archive_original(scope, &event)?;
        ArchivePlan::try_dependency(hold.clone(), record, bytes)
    }

    async fn verify_archive_single_original<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        scope: &ArchivePublicationScope,
        encoding: (&CanonicalJsonCodec, ReadProtection),
    ) -> StorageResult<()> {
        let (codec, protection) = encoding;
        if matches!(
            scope,
            ArchivePublicationScope::IndependentMetadata
                | ArchivePublicationScope::Collection { .. }
                | ArchivePublicationScope::Source(_)
                | ArchivePublicationScope::Imported(_)
        ) {
            return Ok(());
        }
        let (_, expected) = scope
            .dependencies()
            .into_iter()
            .next()
            .ok_or_else(corrupt_record)?;
        let key = self.identity_key(crate::constants::EVENT_SUFFIX, &expected.event)?;
        let row = self
            .load_immutable_waves(attempt, &[key], protection)
            .await?
            .pop()
            .flatten()
            .ok_or_else(corrupt_record)?;
        let bytes = self.load_retained_bytes(attempt, &row, codec).await?;
        let original = self.decode_retained_event(&row, &bytes, codec)?;
        self.require_archive_original(scope, &original)
    }

    pub(super) async fn prepare_publication_coverage<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (&DurableRecord, &VerifiedArchive),
    ) -> StorageResult<Option<RecordChange>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (record, verified) = input;
            if !ArchiveKind::from_key(&record.key)
                .is_some_and(ArchiveKind::has_publication_coverage)
            {
                if verified.dependencies().is_some() {
                    return Err(corrupt_record());
                }
                return Ok(None);
            }
            let scope = self
                .load_archive_publication_scope(attempt, record, ReadProtection::Conflict)
                .await?;
            let expected = self
                .load_publication_dependencies(attempt, &scope, ReadProtection::Conflict)
                .await?;
            if verified.dependencies() != Some(expected.as_slice()) {
                return Err(corrupt_record());
            }
            let coverage = ArchivePublicationCoverage {
                publication: verified.object().clone(),
                count: expected.len(),
                dependencies: CanonicalDigest::of_json(&expected).map_err(|_| corrupt_record())?,
            };
            let key = self.publication_coverage_key(&record.key)?;
            let row = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?
                .pop()
                .flatten();
            if let Some(row) = row {
                if row.revision != Revision::INITIAL
                    || self.records.decode::<ArchivePublicationCoverage>(&row)? != coverage
                {
                    return Err(corrupt_record());
                }
                return Ok(None);
            }
            self.records.prepare(key, None, &coverage).map(Some)
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn require_archive_publication_coverage<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &DurableRecord,
        protection: ReadProtection,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let scope = self
                .load_archive_publication_scope(attempt, record, protection)
                .await?;
            let key = self.publication_coverage_key(&record.key)?;
            let row = self
                .records
                .load(attempt, &[key], protection)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let coverage: ArchivePublicationCoverage = self.records.decode(&row)?;
            if row.revision != Revision::INITIAL {
                return Err(corrupt_record());
            }
            self.archive_record_size(&coverage.publication)?;
            coverage.publication.require_record(record)?;
            let dependencies = self
                .load_publication_dependencies(attempt, &scope, protection)
                .await?;
            if coverage.count != dependencies.len()
                || coverage.dependencies
                    != CanonicalDigest::of_json(&dependencies).map_err(|_| corrupt_record())?
            {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn require_archive_parent_dependency<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (&DurableRecord, &ArchivePointer),
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (parent, pointer) = input;
            let scope = self
                .load_archive_publication_scope(attempt, parent, ReadProtection::Conflict)
                .await?;
            let expected = self
                .archive_publication_dependencies(&scope)?
                .into_iter()
                .find(|(key, _)| key == &pointer.key)
                .ok_or_else(corrupt_record)?
                .1;
            self.require_archive_dependency_pointer(
                attempt,
                (pointer, &expected),
                ReadProtection::Conflict,
            )
            .await
        }
        .await;
        settlement.complete(result)
    }

    async fn load_publication_dependencies<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        scope: &ArchivePublicationScope,
        protection: ReadProtection,
    ) -> StorageResult<Vec<ArchivePointer>> {
        let entries = self.archive_publication_dependencies(scope)?;
        let pointers = self
            .load_publication_pointers(attempt, &entries, protection)
            .await?;
        let pointers = pointers
            .into_iter()
            .map(|pointer| pointer.ok_or_else(corrupt_record))
            .collect::<StorageResult<Vec<_>>>()?;
        self.require_publication_dependencies(attempt, (&entries, &pointers), protection)
            .await?;
        Ok(pointers)
    }

    async fn load_publication_pointers<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        entries: &[(DurableRecordKey, ArchivePublicationDependency)],
        protection: ReadProtection,
    ) -> StorageResult<Vec<Option<ArchivePointer>>> {
        let keys = entries
            .iter()
            .map(|(key, _)| self.archive_pointer_key(key))
            .collect::<StorageResult<Vec<_>>>()?;
        let waves = self.records.read_waves(&keys, |key| key)?;
        let mut pointers = Vec::with_capacity(entries.len());
        for wave in waves {
            let rows = self
                .records
                .load(attempt, &keys[wave.clone()], protection)
                .await?;
            for (entry, row) in entries[wave].iter().zip(rows) {
                let pointer = row
                    .map(|row| self.qualify_publication_dependency(entry, row))
                    .transpose()?;
                pointers.push(pointer);
            }
        }
        Ok(pointers)
    }

    async fn require_publication_dependencies<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (
            &[(DurableRecordKey, ArchivePublicationDependency)],
            &[ArchivePointer],
        ),
        protection: ReadProtection,
    ) -> StorageResult<()> {
        let (entries, pointers) = input;
        if entries.len() != pointers.len() {
            return Err(corrupt_record());
        }
        let mut acceptances = Vec::new();
        let mut metadata = Vec::new();
        for ((key, expected), pointer) in entries.iter().zip(pointers) {
            match expected {
                ArchivePublicationDependency::Event(event)
                    if ArchiveKind::from_key(key) == Some(ArchiveKind::Acceptance) =>
                {
                    acceptances.push((pointer, event.as_ref()))
                }
                ArchivePublicationDependency::Metadata(_) => metadata.push(pointer),
                ArchivePublicationDependency::Event(_) => {}
            }
        }
        self.require_dependency_acceptances(attempt, &acceptances, protection)
            .await?;
        self.require_dependency_coverages(attempt, (&metadata, pointers), protection)
            .await
    }

    fn qualify_publication_dependency(
        &self,
        entry: &(DurableRecordKey, ArchivePublicationDependency),
        row: DurableRecord,
    ) -> StorageResult<ArchivePointer> {
        let (key, expected) = entry;
        let pointer = self.decode_archive_pointer(&row)?;
        if pointer.key != *key {
            return Err(corrupt_record());
        }
        match expected {
            ArchivePublicationDependency::Event(event) => match ArchiveKind::from_key(key) {
                Some(ArchiveKind::Event) if pointer.content == event.digest => {}
                Some(ArchiveKind::Acceptance) => {}
                _ => return Err(corrupt_record()),
            },
            ArchivePublicationDependency::Metadata(expected) if pointer == *expected => {}
            ArchivePublicationDependency::Metadata(_) => return Err(corrupt_record()),
        }
        Ok(pointer)
    }

    async fn require_dependency_coverages<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (&[&ArchivePointer], &[ArchivePointer]),
        protection: ReadProtection,
    ) -> StorageResult<()> {
        let (metadata, all) = input;
        let keys = metadata
            .iter()
            .map(|pointer| self.publication_coverage_key(&pointer.key))
            .collect::<StorageResult<Vec<_>>>()?;
        let waves = self.records.read_waves(&keys, |key| key)?;
        for wave in waves {
            let rows = self
                .records
                .load(attempt, &keys[wave.clone()], protection)
                .await?;
            for (pointer, row) in metadata[wave].iter().zip(rows) {
                let dependencies: Vec<_> = match ArchiveKind::from_key(&pointer.key) {
                    Some(ArchiveKind::ImportManifest | ArchiveKind::CollectionDefinition) => {
                        Vec::new()
                    }
                    Some(ArchiveKind::ImportPage) => all
                        .iter()
                        .filter(|candidate| candidate.key != pointer.key)
                        .collect(),
                    _ => return Err(corrupt_record()),
                };
                let row = row.ok_or_else(corrupt_record)?;
                let coverage: ArchivePublicationCoverage = self.records.decode(&row)?;
                if row.revision != Revision::INITIAL
                    || coverage.publication != **pointer
                    || coverage.count != dependencies.len()
                    || coverage.dependencies
                        != CanonicalDigest::of_json(&dependencies).map_err(|_| corrupt_record())?
                {
                    return Err(corrupt_record());
                }
            }
        }
        Ok(())
    }

    async fn require_dependency_acceptances<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        dependencies: &[(&ArchivePointer, &ArchiveEventDependency)],
        protection: ReadProtection,
    ) -> StorageResult<()> {
        let keys = dependencies
            .iter()
            .map(|(pointer, _)| pointer.key.clone())
            .collect::<Vec<_>>();
        let waves = self.records.read_waves(&keys, |key| key)?;
        for wave in &waves {
            let bytes =
                dependencies[wave.clone()]
                    .iter()
                    .try_fold(0usize, |used, (pointer, _)| {
                        used.checked_add(self.archive_record_size(pointer)?)
                            .ok_or_else(corrupt_record)
                    })?;
            self.records.limits.check_batch(wave.len(), bytes)?;
        }
        for wave in waves {
            let rows = self
                .records
                .load(attempt, &keys[wave.clone()], protection)
                .await?;
            // Every dispatched metadata fetch settles before any validation
            // failure is returned.
            let records =
                futures::future::join_all(dependencies[wave.clone()].iter().zip(rows).map(
                    |((pointer, _), row)| async move {
                        match row {
                            Some(row) => Ok(row),
                            None => self.load_archive_record(pointer).await,
                        }
                    },
                ))
                .await;
            for (record, (pointer, expected)) in records.into_iter().zip(&dependencies[wave]) {
                let record = record?;
                pointer.require_record(&record)?;
                self.require_archive_acceptance(&record, expected)?;
            }
        }
        Ok(())
    }

    async fn require_archive_dependency_pointer<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (&ArchivePointer, &ArchivePublicationDependency),
        protection: ReadProtection,
    ) -> StorageResult<()> {
        let (pointer, expected) = input;
        self.archive_record_size(pointer)?;
        let expected = match expected {
            ArchivePublicationDependency::Metadata(expected) => {
                return if pointer == expected {
                    Ok(())
                } else {
                    Err(corrupt_record())
                };
            }
            ArchivePublicationDependency::Event(expected) => expected,
        };
        match ArchiveKind::from_key(&pointer.key) {
            Some(ArchiveKind::Event)
                if pointer.content == expected.digest
                    && pointer.key
                        == self
                            .identity_key(crate::constants::EVENT_SUFFIX, &expected.event)? =>
            {
                Ok(())
            }
            Some(ArchiveKind::Acceptance)
                if pointer.key
                    == self
                        .identity_key(crate::constants::ACCEPTANCE_SUFFIX, &expected.event)? =>
            {
                let record = self
                    .load_archive_publication_record(attempt, pointer, protection)
                    .await?;
                self.require_archive_acceptance(&record, expected)
            }
            _ => Err(corrupt_record()),
        }
    }

    async fn load_archive_publication_record<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        pointer: &ArchivePointer,
        protection: ReadProtection,
    ) -> StorageResult<DurableRecord> {
        let record = match self
            .records
            .load(attempt, std::slice::from_ref(&pointer.key), protection)
            .await?
            .pop()
            .flatten()
        {
            Some(record) => record,
            None => self.load_archive_record(pointer).await?,
        };
        pointer.require_record(&record)?;
        Ok(record)
    }

    fn publication_coverage_key(
        &self,
        publication: &DurableRecordKey,
    ) -> StorageResult<DurableRecordKey> {
        let mut suffix = crate::constants::ARCHIVE_PUBLICATION_COVERAGE_SUFFIX.to_vec();
        suffix.extend_from_slice(&publication.as_ref().suffix);
        self.record_key(suffix)
    }
}
