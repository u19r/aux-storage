use std::sync::Arc;

use infrastructure_provider::record::{DurableRecord, DurableRecordKey};
use replication_types::protocol::CanonicalDigest;
use serde::{Deserialize, Serialize};
use storage_types::{Revision, StorageResult};

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ArchiveKind {
    Event,
    BaselineDefinition,
    BaselineInventory,
    Baseline,
    CollectionDefinition,
    CollectionPage,
    CollectionCatalog,
    CollectionRetiredPage,
    CollectionDeclaration,
    CollectionBaseline,
    CollectionRetainedDefinition,
    CollectionRetainedCut,
    Source,
    Acceptance,
    ImportManifest,
    ImportPage,
    RetainedPosition,
    RetainedLink,
    ReceiverView,
}

impl ArchiveKind {
    pub(crate) fn prefix(self) -> &'static [u8] {
        match self {
            Self::Event => crate::constants::EVENT_SUFFIX,
            Self::BaselineDefinition => crate::constants::BASELINE_LINEAGE_AUTHORITY_SUFFIX,
            Self::BaselineInventory => crate::constants::REENABLE_BASELINE_INVENTORY_SUFFIX,
            Self::Baseline => crate::constants::BASELINE_SUFFIX,
            Self::CollectionDefinition => crate::constants::COLLECTED_HISTORY_DEFINITION_SUFFIX,
            Self::CollectionPage => crate::constants::COLLECTED_HISTORY_PAGE_SUFFIX,
            Self::CollectionCatalog => crate::constants::COLLECTED_CATALOG_SUFFIX,
            Self::CollectionRetiredPage => crate::constants::COLLECTED_RETIRED_PAGE_SUFFIX,
            Self::CollectionDeclaration => crate::constants::SOURCE_COLLECTION_DECLARATION_SUFFIX,
            Self::CollectionBaseline => crate::constants::COLLECTED_BASELINE_DEFINITION_SUFFIX,
            Self::CollectionRetainedDefinition => {
                crate::constants::COLLECTED_RETAINED_DEFINITION_SUFFIX
            }
            Self::CollectionRetainedCut => crate::constants::COLLECTED_RETAINED_PAGE_SUFFIX,
            Self::Source => crate::constants::SOURCE_SUFFIX,
            Self::Acceptance => crate::constants::ACCEPTANCE_SUFFIX,
            Self::ImportManifest => crate::constants::IMPORT_MANIFEST_SUFFIX,
            Self::ImportPage => crate::constants::IMPORT_PAGE_SUFFIX,
            Self::RetainedPosition => crate::constants::RETAINED_POSITION_SUFFIX,
            Self::RetainedLink => crate::constants::RETAINED_LINK_SUFFIX,
            Self::ReceiverView => crate::constants::CANONICAL_VIEW_SUFFIX,
        }
    }
    pub(crate) fn next(self) -> Self {
        match self {
            Self::Event => Self::BaselineDefinition,
            Self::BaselineDefinition => Self::BaselineInventory,
            Self::BaselineInventory => Self::Baseline,
            Self::Baseline => Self::CollectionDefinition,
            Self::CollectionDefinition => Self::CollectionPage,
            Self::CollectionPage => Self::CollectionCatalog,
            Self::CollectionCatalog => Self::CollectionRetiredPage,
            Self::CollectionRetiredPage => Self::CollectionDeclaration,
            Self::CollectionDeclaration => Self::CollectionBaseline,
            Self::CollectionBaseline => Self::CollectionRetainedDefinition,
            Self::CollectionRetainedDefinition => Self::CollectionRetainedCut,
            Self::CollectionRetainedCut => Self::Source,
            Self::Source => Self::Acceptance,
            Self::Acceptance => Self::ImportManifest,
            Self::ImportManifest => Self::ImportPage,
            Self::ImportPage => Self::RetainedPosition,
            Self::RetainedPosition => Self::RetainedLink,
            Self::RetainedLink => Self::ReceiverView,
            Self::ReceiverView => Self::Event,
        }
    }
    pub(crate) fn has_publication_coverage(self) -> bool {
        matches!(
            self,
            Self::BaselineDefinition
                | Self::BaselineInventory
                | Self::CollectionDefinition
                | Self::CollectionPage
                | Self::CollectionCatalog
                | Self::CollectionRetiredPage
                | Self::CollectionDeclaration
                | Self::CollectionBaseline
                | Self::CollectionRetainedDefinition
                | Self::CollectionRetainedCut
                | Self::Source
                | Self::Acceptance
                | Self::ReceiverView
                | Self::RetainedLink
                | Self::ImportManifest
                | Self::ImportPage
                | Self::RetainedPosition
        )
    }
    pub(crate) fn is_payload(self) -> bool {
        matches!(self, Self::Event | Self::Baseline)
    }
    pub(crate) fn kinds() -> impl Iterator<Item = Self> {
        std::iter::successors(Some(Self::Event), |kind| {
            let next = kind.next();
            (next != Self::Event).then_some(next)
        })
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
    pub key: DurableRecordKey,
    pub revision: Revision,
    pub manifest: CanonicalDigest,
    pub metadata_length: usize,
    pub content: CanonicalDigest,
    pub length: usize,
}

impl ArchivePointer {
    pub(crate) fn require_record(&self, record: &DurableRecord) -> StorageResult<()> {
        if self.key != record.key
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
    object: ArchivePointer,
    bytes: Arc<[u8]>,
    metadata: Arc<[u8]>,
    dependencies: Option<Vec<ArchivePointer>>,
}

impl ArchivePlan {
    pub(crate) fn try_new(
        hold: ArchiveHold,
        record: DurableRecord,
        bytes: Arc<[u8]>,
    ) -> StorageResult<Self> {
        let object = hold.pointer.clone();
        Self::try_object((hold, object), record, bytes)
    }

    pub(crate) fn try_dependency(
        hold: ArchiveHold,
        record: DurableRecord,
        bytes: Arc<[u8]>,
    ) -> StorageResult<Self> {
        let object = ArchivePointer {
            key: record.key.clone(),
            revision: record.revision,
            manifest: CanonicalDigest::of(&record.bytes),
            metadata_length: record.bytes.len(),
            content: CanonicalDigest::of(&bytes),
            length: bytes.len(),
        };
        Self::try_object((hold, object), record, bytes)
    }

    fn try_object(
        binding: (ArchiveHold, ArchivePointer),
        record: DurableRecord,
        bytes: Arc<[u8]>,
    ) -> StorageResult<Self> {
        let (hold, object) = binding;
        object.require_record(&record)?;
        if bytes.len() != object.length || CanonicalDigest::of(&bytes) != object.content {
            return Err(crate::native_record::corrupt_record());
        }
        Ok(Self {
            hold,
            object,
            bytes,
            metadata: record.bytes,
            dependencies: None,
        })
    }

    pub(crate) fn with_publication_dependencies(
        mut self,
        dependencies: Vec<ArchivePointer>,
    ) -> StorageResult<Self> {
        if !ArchiveKind::from_key(&self.object.key)
            .is_some_and(ArchiveKind::has_publication_coverage)
        {
            return Err(crate::native_record::corrupt_record());
        }
        self.dependencies = Some(dependencies);
        Ok(self)
    }

    pub(crate) async fn publish(
        self,
        archive: &super::CanonicalArchive,
    ) -> StorageResult<ArchivePublication> {
        self.require_object_bounds(archive)?;
        if let Some(pointer) = self.find_unavailable_dependency(archive).await? {
            let pointer = pointer.clone();
            return Ok(ArchivePublication::RecoverDependency {
                hold: self.hold,
                pointer,
            });
        }
        if self.metadata.as_ref() == self.bytes.as_ref() {
            let object = archive
                .publish_object(self.object.manifest, &self.metadata)
                .await?;
            return object
                .clone()
                .bind_hold(object, self.hold, self.object, self.dependencies)
                .map(ArchivePublication::Verified);
        }
        // Both publications settle even when one fails; the native hold stays
        // pending.
        let (metadata, content) = futures::join!(
            archive.publish_object(self.object.manifest, &self.metadata),
            archive.publish_object(self.object.content, &self.bytes),
        );
        let metadata = metadata?;
        content?
            .bind_hold(metadata, self.hold, self.object, self.dependencies)
            .map(ArchivePublication::Verified)
    }

    fn require_object_bounds(&self, archive: &super::CanonicalArchive) -> StorageResult<()> {
        for pointer in std::iter::once(&self.object).chain(self.dependencies.iter().flatten()) {
            archive.require_object_length(pointer.metadata_length)?;
            archive.require_object_length(pointer.length)?;
            if pointer.content == pointer.manifest && pointer.length != pointer.metadata_length {
                return Err(crate::native_record::corrupt_record());
            }
        }
        Ok(())
    }

    async fn find_unavailable_dependency(
        &self,
        archive: &super::CanonicalArchive,
    ) -> StorageResult<Option<&ArchivePointer>> {
        let mut missing = None;
        for pointer in self.dependencies.iter().flatten() {
            let metadata = archive
                .fetch_object(pointer.manifest, pointer.metadata_length)
                .await
                .map(|_| ());
            let content = if pointer.content != pointer.manifest {
                archive
                    .fetch_object(pointer.content, pointer.length)
                    .await
                    .map(|_| ())
            } else {
                Ok(())
            };
            for result in [metadata, content] {
                match result {
                    Ok(()) => {}
                    Err(error)
                        if matches!(
                            error.as_ref(),
                            storage_types::StorageEnum::HistoryUnavailable
                        ) =>
                    {
                        missing.get_or_insert(pointer);
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(missing)
    }
}

pub(crate) enum ArchiveTurn {
    Export(Box<ArchivePlan>),
    Advanced,
    Idle,
}
pub(crate) enum ArchivePublication {
    Verified(VerifiedArchive),
    RecoverDependency {
        hold: ArchiveHold,
        pointer: ArchivePointer,
    },
}

/// Ephemeral readback evidence; neither operators nor native rows can
/// deserialize it.
#[derive(Clone)]
pub(crate) struct VerifiedObject {
    archive: [u8; 16],
    digest: CanonicalDigest,
    length: usize,
}

impl VerifiedObject {
    pub(super) fn new(archive: [u8; 16], digest: CanonicalDigest, length: usize) -> Self {
        Self {
            archive,
            digest,
            length,
        }
    }

    fn bind_hold(
        self,
        metadata: Self,
        hold: ArchiveHold,
        object: ArchivePointer,
        dependencies: Option<Vec<ArchivePointer>>,
    ) -> StorageResult<VerifiedArchive> {
        if self.archive != metadata.archive
            || self.digest != object.content
            || self.length != object.length
            || metadata.digest != object.manifest
            || metadata.length != object.metadata_length
        {
            return Err(crate::native_record::corrupt_record());
        }
        Ok(VerifiedArchive {
            archive: self.archive,
            hold,
            object,
            dependencies,
        })
    }
}

pub(crate) struct VerifiedArchive {
    archive: [u8; 16],
    hold: ArchiveHold,
    object: ArchivePointer,
    dependencies: Option<Vec<ArchivePointer>>,
}

impl VerifiedArchive {
    pub(crate) fn archive_identity(&self) -> [u8; 16] {
        self.archive
    }

    pub(crate) fn hold(&self) -> &ArchiveHold {
        &self.hold
    }

    pub(crate) fn object(&self) -> &ArchivePointer {
        &self.object
    }

    pub(crate) fn dependencies(&self) -> Option<&[ArchivePointer]> {
        self.dependencies.as_deref()
    }
}
