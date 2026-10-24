use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_types::{ReadProtection, StorageResult};

use crate::{
    archive::{ArchiveState, VerifiedArchive},
    journal::JournalStore,
    native_record::corrupt_record,
};

struct ArchiveObjectPublication<'record> {
    record: std::borrow::Cow<'record, infrastructure_provider::record::DurableRecord>,
    pointer: Option<infrastructure_provider::record::RecordChange>,
    release: Vec<infrastructure_provider::record::RecordChange>,
}

impl ArchiveObjectPublication<'_> {
    fn into_changes(
        self,
        coverage: Option<infrastructure_provider::record::RecordChange>,
    ) -> Vec<infrastructure_provider::record::RecordChange> {
        self.pointer
            .into_iter()
            .chain(coverage)
            .chain(self.release)
            .collect()
    }
}

impl JournalStore {
    pub(crate) async fn publish_archive_pointer<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        verified: &VerifiedArchive,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let archive = self
                .archive
                .get()
                .filter(|archive| archive.identity() == verified.archive_identity())
                .ok_or_else(corrupt_record)?;
            self.bind_archive(attempt, archive.physical_identity())
                .await?;
            let hold = verified.hold();
            self.require_archive_hold(hold)?;
            let family = self
                .load_archive_state(attempt, hold.cursor.kind, ReadProtection::Conflict)
                .await?;
            let (record, hot) = match self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&hold.pointer.key),
                    ReadProtection::Conflict,
                )
                .await?
                .pop()
                .flatten()
            {
                Some(record) => (record, true),
                None => (self.load_archive_record(&hold.pointer).await?, false),
            };
            hold.pointer.require_record(&record)?;
            let object = self
                .prepare_archive_object_publication(attempt, verified, (&record, hot))
                .await?;
            if self
                .is_archive_publication_reconciled(
                    attempt,
                    &family.state,
                    (verified, &record, &object),
                )
                .await?
            {
                let changes = object.release.into_iter().collect::<Vec<_>>();
                return self.records.store(attempt, &changes).await;
            }
            if verified.object() != &hold.pointer {
                self.require_archive_parent_dependency(attempt, (&record, verified.object()))
                    .await?;
            }
            let coverage = self
                .prepare_publication_coverage(attempt, (object.record.as_ref(), verified))
                .await?;
            if verified.object() != &hold.pointer {
                return self
                    .records
                    .store(attempt, &object.into_changes(coverage))
                    .await;
            }
            self.complete_archive_hold(attempt, family, hold, object.into_changes(coverage))
                .await
        }
        .await;
        settlement_guard.complete(result)
    }

    async fn is_archive_publication_reconciled<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        state: &ArchiveState,
        input: (
            &VerifiedArchive,
            &infrastructure_provider::record::DurableRecord,
            &ArchiveObjectPublication<'_>,
        ),
    ) -> StorageResult<bool> {
        let (verified, record, object) = input;
        let hold = verified.hold();
        if matches!(state, ArchiveState::Qualifying { hold: pending, .. } if pending == hold) {
            return Err(corrupt_record());
        }
        if let ArchiveState::Pending(pending) = state {
            if pending == hold {
                return Ok(false);
            }
            if pending.pointer == hold.pointer {
                return Err(corrupt_record());
            }
        }
        if object.pointer.is_some() {
            return Err(corrupt_record());
        }
        if hold.cursor.kind.has_publication_coverage() {
            self.require_archive_publication_coverage(attempt, record, ReadProtection::Conflict)
                .await?;
        }
        if object.record.key != record.key
            && crate::archive::ArchiveKind::from_key(&object.record.key)
                .is_some_and(crate::archive::ArchiveKind::has_publication_coverage)
        {
            self.require_archive_publication_coverage(
                attempt,
                object.record.as_ref(),
                ReadProtection::Conflict,
            )
            .await?;
        }
        Ok(true)
    }

    async fn complete_archive_hold<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        observed: super::scan::ArchiveFamilyObservation,
        hold: &crate::archive::ArchiveHold,
        mut changes: Vec<infrastructure_provider::record::RecordChange>,
    ) -> StorageResult<()> {
        let super::scan::ArchiveFamilyObservation {
            row: root,
            cursor_row,
            cursor,
            index,
            ..
        } = observed;
        let index = index.ok_or_else(corrupt_record)?;
        if cursor != hold.cursor {
            return Err(corrupt_record());
        }
        changes.push(self.prepare_archive_scan_advance((cursor_row, cursor), &hold.pointer.key)?);
        let next = ArchiveState::Scanning(hold.cursor.kind);
        changes.push(self.records.prepare(root.key, Some(root.revision), &next)?);
        changes.push(infrastructure_provider::record::RecordChange::Delete {
            key: index.key,
            expected: index.revision,
        });
        self.retain_journal_epoch(attempt).await?;
        self.records.store(attempt, &changes).await
    }

    async fn prepare_archive_object_publication<
        'record,
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        verified: &VerifiedArchive,
        parent: (
            &'record infrastructure_provider::record::DurableRecord,
            bool,
        ),
    ) -> StorageResult<ArchiveObjectPublication<'record>> {
        let pointer = verified.object();
        self.archive_record_size(pointer)?;
        let (record, hot) = if pointer == &verified.hold().pointer {
            (std::borrow::Cow::Borrowed(parent.0), parent.1)
        } else {
            let (child, hot) = match self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&pointer.key),
                    ReadProtection::Conflict,
                )
                .await?
                .pop()
                .flatten()
            {
                Some(record) => (record, true),
                None => (self.load_archive_record(pointer).await?, false),
            };
            (std::borrow::Cow::Owned(child), hot)
        };
        pointer.require_record(record.as_ref())?;
        let change = match self
            .load_archive_pointer(attempt, record.as_ref(), ReadProtection::Conflict)
            .await?
        {
            Some(existing) if existing == *pointer => None,
            Some(_) => return Err(corrupt_record()),
            None => Some(self.records.prepare(
                self.archive_pointer_key(&pointer.key)?,
                None,
                pointer,
            )?),
        };
        let mut release = Vec::new();
        if hot {
            release.extend(
                self.release_original_owner(attempt, (record.as_ref(), pointer.length))
                    .await?,
            );
            release.push(infrastructure_provider::record::RecordChange::Delete {
                key: record.key.clone(),
                expected: record.revision,
            });
        }
        Ok(ArchiveObjectPublication {
            record,
            pointer: change,
            release,
        })
    }
}
