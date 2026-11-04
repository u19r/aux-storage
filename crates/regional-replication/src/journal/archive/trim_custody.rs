//! A pending export holds one independently readable original.
use infrastructure_provider::record::DurableRecord;
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::{event::content::CanonicalEvent, protocol::CanonicalDigest};
use storage_types::{ReadProtection, StorageResult};

use crate::{
    archive::{ArchiveHold, ArchiveKind},
    journal::JournalStore,
    native_record::corrupt_record,
};

pub(in crate::journal) enum ArchiveOriginal {
    Event {
        event: CanonicalEvent,
        scope: CanonicalDigest,
    },
    Baseline {
        scope: CanonicalDigest,
    },
}

impl JournalStore {
    pub(in crate::journal) async fn load_archive_trim_page<
        A: infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
        hold: &ArchiveHold,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<ArchiveOriginal> {
        self.observe_archive_hold(attempt, hold, ReadProtection::Shared)
            .await?;
        self.load_archive_candidate_page(attempt, hold, codec).await
    }

    pub(in crate::journal) async fn load_archive_candidate_page<
        A: infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
        hold: &ArchiveHold,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<ArchiveOriginal> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let result = async {
            self.require_archive_hold(hold)?;
            let record = self
                .load_archive_original_record(
                    operation.as_mut(),
                    &hold.pointer.key,
                    ReadProtection::Snapshot,
                )
                .await?;
            hold.pointer.require_record(&record)?;
            self.decode_archive_trim_payload((&record, hold), codec)
        }
        .await;
        operation.complete(result)
    }

    fn decode_archive_trim_payload(
        &self,
        selected: (&DurableRecord, &ArchiveHold),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<ArchiveOriginal> {
        let (record, hold) = selected;
        let bytes = self.decode_original_bytes(record, codec)?;
        if CanonicalDigest::of(&bytes) != hold.pointer.content || bytes.len() != hold.pointer.length
        {
            return Err(corrupt_record());
        }
        match hold.cursor.kind {
            ArchiveKind::Event => Ok(ArchiveOriginal::Event {
                event: self.decode_retained_event(record, &bytes, codec)?,
                scope: hold.pointer.manifest,
            }),
            ArchiveKind::Baseline => {
                self.decode_retained_baseline(record, bytes, codec)?;
                Ok(ArchiveOriginal::Baseline {
                    scope: hold.pointer.manifest,
                })
            }
        }
    }
}
