use std::sync::Arc;

use infrastructure_provider::record::{DurableRecord, DurableRecordKey};
use replication_types::protocol::CanonicalDigest;
use serde::{Deserialize, Serialize};
use storage_types::{Revision, StorageResult};

/// Complete immutable originals own their archive objects. Indexes stay native.
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ArchiveKind {
    Event,
    Baseline,
}

impl ArchiveKind {
    pub(crate) fn prefix(self) -> &'static [u8] {
        match self {
            Self::Event => crate::constants::EVENT_SUFFIX,
            Self::Baseline => crate::constants::BASELINE_SUFFIX,
        }
    }
    pub(crate) fn next(self) -> Self {
        match self {
            Self::Event => Self::Baseline,
            Self::Baseline => Self::Event,
        }
    }
    pub(crate) fn kinds() -> impl Iterator<Item = Self> {
        [Self::Event, Self::Baseline].into_iter()
    }
    pub(crate) fn from_key(key: &DurableRecordKey) -> Option<Self> {
        Self::kinds().find(|kind| key.as_ref().suffix.starts_with(kind.prefix()))
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ArchiveCursor {
    pub kind: ArchiveKind,
    pub after: Option<DurableRecordKey>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ArchivePointer {
    pub history: storage_types::TableHistoryId,
    pub key: DurableRecordKey,
    pub revision: Revision,
    pub manifest: CanonicalDigest,
    pub metadata_length: usize,
    pub content: CanonicalDigest,
    pub length: usize,
}

impl ArchivePointer {
    pub(crate) fn require_record(&self, record: &DurableRecord) -> StorageResult<()> {
        if ArchiveKind::from_key(&self.key).is_none()
            || self.key != record.key
            || self.revision != record.revision
            || self.manifest != CanonicalDigest::of(&record.bytes)
            || self.metadata_length != record.bytes.len()
            || self.length == 0
            || self.metadata_length == 0
            || self.revision != Revision::INITIAL
        {
            return Err(crate::native_record::corrupt_record());
        }
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
struct ArchivePublicationId(uuid::Uuid);

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ArchiveHold {
    publication: ArchivePublicationId,
    pub cursor: ArchiveCursor,
    pub pointer: ArchivePointer,
}

impl ArchiveHold {
    pub(crate) fn new(cursor: ArchiveCursor, pointer: ArchivePointer) -> Self {
        Self {
            publication: ArchivePublicationId(uuid::Uuid::new_v4()),
            cursor,
            pointer,
        }
    }

    pub(crate) fn require_publication(&self) -> StorageResult<()> {
        if self.publication.0.is_nil() {
            return Err(crate::native_record::corrupt_record());
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ArchiveSchedule {
    pub next: ArchiveKind,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum ArchiveState {
    Scanning(ArchiveKind),
    Qualifying {
        hold: ArchiveHold,
        scan: Box<super::admission::ArchiveAdmissionScan>,
    },
    Pending(ArchiveHold),
}

pub(crate) struct ArchivePlan {
    hold: ArchiveHold,
    bytes: Arc<[u8]>,
    metadata: Arc<[u8]>,
}

impl ArchivePlan {
    pub(crate) fn try_new(
        hold: ArchiveHold,
        record: DurableRecord,
        bytes: Arc<[u8]>,
    ) -> StorageResult<Self> {
        hold.pointer.require_record(&record)?;
        if bytes.len() != hold.pointer.length || CanonicalDigest::of(&bytes) != hold.pointer.content
        {
            return Err(crate::native_record::corrupt_record());
        }
        Ok(Self {
            hold,
            bytes,
            metadata: record.bytes,
        })
    }

    pub(crate) async fn publish(
        self,
        archive: &super::CanonicalArchive,
    ) -> StorageResult<VerifiedArchive> {
        let pointer = &self.hold.pointer;
        archive.require_object_length(pointer.metadata_length)?;
        archive.require_object_length(pointer.length)?;
        // Settle both writes before releasing the export's source capacity.
        let (metadata, content) = futures::join!(
            archive.publish_object(pointer.history, pointer.manifest, &self.metadata),
            archive.publish_object(pointer.history, pointer.content, &self.bytes),
        );
        content?.bind_hold(metadata?, self.hold)
    }
}

pub(crate) enum ArchiveTurn {
    Export(Box<ArchivePlan>),
    Advanced,
    Idle,
}
/// Ephemeral readback evidence; neither operators nor native rows can
/// deserialize it.
pub(crate) struct VerifiedObject {
    archive: [u8; 16],
    history: storage_types::TableHistoryId,
    digest: CanonicalDigest,
    length: usize,
}

impl VerifiedObject {
    pub(super) fn new(
        archive: [u8; 16],
        history: storage_types::TableHistoryId,
        digest: CanonicalDigest,
        length: usize,
    ) -> Self {
        Self {
            archive,
            history,
            digest,
            length,
        }
    }

    fn bind_hold(self, metadata: Self, hold: ArchiveHold) -> StorageResult<VerifiedArchive> {
        let pointer = &hold.pointer;
        if self.history != pointer.history
            || metadata.history != pointer.history
            || self.archive != metadata.archive
            || self.digest != pointer.content
            || self.length != pointer.length
            || metadata.digest != pointer.manifest
            || metadata.length != pointer.metadata_length
        {
            return Err(crate::native_record::corrupt_record());
        }
        Ok(VerifiedArchive {
            archive: self.archive,
            hold,
        })
    }
}

pub(crate) struct VerifiedArchive {
    archive: [u8; 16],
    hold: ArchiveHold,
}

impl VerifiedArchive {
    pub(crate) fn archive_identity(&self) -> [u8; 16] {
        self.archive
    }

    pub(crate) fn hold(&self) -> &ArchiveHold {
        &self.hold
    }
}
