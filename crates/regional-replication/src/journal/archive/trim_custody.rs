//! Pull one validated original dependency from an actual pending archive root.
use infrastructure_provider::record::DurableRecord;
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::{
    event::{content::CanonicalEvent, interval::SourceIntervalManifest},
    protocol::CanonicalDigest,
};
use storage_types::{ReadProtection, StorageResult};

use super::publication_scope::{ArchiveEventDependency, ArchivePublicationScope};
use crate::{
    archive::{ArchiveHold, ArchiveKind},
    journal::JournalStore,
    native_record::corrupt_record,
};

pub(in crate::journal) enum ArchiveTrimPage {
    Original {
        event: CanonicalEvent,
        scope: CanonicalDigest,
        next: Option<u64>,
    },
    Independent {
        scope: CanonicalDigest,
    },
    Manifest {
        manifest: SourceIntervalManifest,
        scope: CanonicalDigest,
    },
}

impl JournalStore {
    pub(in crate::journal) async fn load_archive_trim_page<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        selected: (&ArchiveHold, u64),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<ArchiveTrimPage> {
        let (hold, ordinal) = selected;
        self.observe_archive_hold(attempt, hold, ReadProtection::Shared)
            .await?;
        self.load_archive_candidate_page(attempt, (hold, ordinal), codec)
            .await
    }

    pub(in crate::journal) async fn load_archive_candidate_page<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        selected: (&ArchiveHold, u64),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<ArchiveTrimPage> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (hold, ordinal) = selected;
            self.require_archive_hold(hold)?;
            let record = self
                .load_archive_dependency_metadata(
                    attempt,
                    &hold.pointer.key,
                    ReadProtection::Snapshot,
                )
                .await?;
            hold.pointer.require_record(&record)?;
            if hold.cursor.kind.is_payload() {
                return self
                    .load_archive_trim_payload(attempt, (&record, hold, ordinal), codec)
                    .await;
            }
            if hold.cursor.kind == ArchiveKind::ImportManifest {
                if ordinal != 0 {
                    return Err(corrupt_record());
                }
                let manifest = self.decode_import_manifest_record(&record, codec)?;
                return Ok(ArchiveTrimPage::Manifest {
                    manifest,
                    scope: hold.pointer.manifest,
                });
            }
            let scope = self
                .load_archive_publication_scope(attempt, &record, ReadProtection::Snapshot)
                .await?;
            let (events, digest) = self.qualify_archive_trim_dependencies(&scope, hold, codec)?;
            if events.is_empty() {
                if ordinal != 0 {
                    return Err(corrupt_record());
                }
                return Ok(ArchiveTrimPage::Independent { scope: digest });
            }
            let offset = usize::try_from(ordinal).map_err(|_| corrupt_record())?;
            let expected = events.get(offset).ok_or_else(corrupt_record)?;
            let event = self
                .load_archive_trim_original(attempt, (&scope, expected), codec)
                .await?;
            let next = ordinal.checked_add(1).ok_or_else(corrupt_record)?;
            Ok(ArchiveTrimPage::Original {
                event,
                scope: digest,
                next: (offset + 1 < events.len()).then_some(next),
            })
        }
        .await;
        settlement_guard.complete(result)
    }

    fn qualify_archive_trim_dependencies(
        &self,
        scope: &ArchivePublicationScope,
        hold: &ArchiveHold,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<(Vec<ArchiveEventDependency>, CanonicalDigest)> {
        let mut metadata = Vec::new();
        for record in scope.metadata_dependencies() {
            self.require_archive_metadata(record, codec)?;
            metadata.push(self.archive_metadata_pointer(record)?);
        }
        let events = scope.collect_original_dependencies();
        let identity = events
            .iter()
            .map(|event| {
                (
                    &event.event,
                    event.digest,
                    event.lineage,
                    &event.delivery,
                    &event.receipt,
                )
            })
            .collect::<Vec<_>>();
        let digest = self
            .records
            .content_digest(&(&hold.pointer, identity, metadata))?;
        Ok((events, digest))
    }

    async fn load_archive_trim_original<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        selected: (&ArchivePublicationScope, &ArchiveEventDependency),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<CanonicalEvent> {
        let (scope, expected) = selected;
        let keys = [
            self.identity_key(crate::constants::EVENT_SUFFIX, &expected.event)?,
            self.identity_key(crate::constants::ACCEPTANCE_SUFFIX, &expected.event)?,
        ];
        let rows = self
            .load_immutable_waves(attempt, &keys, ReadProtection::Snapshot)
            .await?;
        if rows.len() != keys.len() {
            return Err(corrupt_record());
        }
        let row = rows[0].as_ref().ok_or_else(corrupt_record)?;
        let bytes = self.load_retained_bytes(attempt, row, codec).await?;
        let event = self.decode_retained_event(row, &bytes, codec)?;
        self.require_archive_original(scope, &event)?;
        let acceptance = rows[1].as_ref().ok_or_else(corrupt_record)?;
        self.require_archive_acceptance(acceptance, expected)?;
        let accepted = self.decode_native_acceptance(acceptance)?;
        self.require_acceptance_binding(
            (&accepted.receipt, &accepted.delivery),
            (&expected.event, &event),
        )?;
        Ok(event)
    }

    async fn load_archive_trim_payload<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        selected: (&DurableRecord, &ArchiveHold, u64),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<ArchiveTrimPage> {
        let (record, hold, ordinal) = selected;
        if ordinal != 0 {
            return Err(corrupt_record());
        }
        let bytes = self.load_retained_bytes(attempt, record, codec).await?;
        if CanonicalDigest::of(&bytes) != hold.pointer.content || bytes.len() != hold.pointer.length
        {
            return Err(corrupt_record());
        }
        match hold.cursor.kind {
            ArchiveKind::Event => Ok(ArchiveTrimPage::Original {
                event: self.decode_retained_event(record, &bytes, codec)?,
                scope: hold.pointer.manifest,
                next: None,
            }),
            ArchiveKind::Baseline => {
                self.decode_retained_baseline(record, bytes, codec)?;
                Ok(ArchiveTrimPage::Independent {
                    scope: hold.pointer.manifest,
                })
            }
            _ => Err(corrupt_record()),
        }
    }
}
