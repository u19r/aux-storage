//! Immutable history owns snapshots. Retention belongs exclusively to its
//! table.

use serde::{Deserialize, Deserializer, Serialize};

use crate::{
    IndexerDeclaration, ItemHistoryAddress, ItemStreamVersion, KeyAttributes, StorageError,
    StorageResult, SystemStreamPosition, SystemStreamScope, TimestampMillis, WireItem,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ItemHistoryKind {
    Image,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ItemHistoryPublication {
    Event {
        kind: ItemHistoryKind,
        source_scope: SystemStreamScope,
        table_position: SystemStreamPosition,
        created_at: TimestampMillis,
    },
    /// An imported current value becomes an old image without inventing an
    /// event.
    PredecessorAnchor,
}

impl ItemHistoryPublication {
    fn require_predecessor(&self, previous: Option<ItemStreamVersion>) -> StorageResult<()> {
        if let Self::Event { table_position, .. } = self
            && table_position
                .offset()
                .order()
                .map_err(|_| StorageError::internal("Invalid history publication position"))?
                == 0
        {
            return Err(StorageError::internal(
                "History publication starts after the initial frontier",
            ));
        }
        match (self, previous) {
            (Self::PredecessorAnchor, Some(_))
            | (
                Self::Event {
                    kind: ItemHistoryKind::Deleted,
                    ..
                },
                None,
            ) => Err(StorageError::internal(
                "Invalid history publication predecessor",
            )),
            _ => Ok(()),
        }
    }
}

/// One immutable source/version pair shared by pointer indexes and exact reads.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct ItemHistoryReference {
    address: ItemHistoryAddress,
    version: ItemStreamVersion,
}

impl ItemHistoryReference {
    pub fn try_new(address: ItemHistoryAddress, version: ItemStreamVersion) -> StorageResult<Self> {
        address.physical_stream_name()?;
        if address.history_id.as_ref() == &[0; 16]
            || address.stream_name.item_table_name().is_none()
            || version.get() == 0
        {
            return Err(StorageError::internal("Invalid item history reference"));
        }
        Ok(Self { address, version })
    }

    pub fn address(&self) -> &ItemHistoryAddress {
        &self.address
    }
    pub fn version(&self) -> ItemStreamVersion {
        self.version
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ItemHistoryReferenceInput {
    address: ItemHistoryAddress,
    version: ItemStreamVersion,
}

impl<'de> Deserialize<'de> for ItemHistoryReference {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let input = ItemHistoryReferenceInput::deserialize(deserializer)?;
        Self::try_new(input.address, input.version).map_err(serde::de::Error::custom)
    }
}

/// Bounded history facts, decoded independently of an item's image payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ItemHistoryMetadata {
    #[serde(flatten)]
    reference: ItemHistoryReference,
    previous_version: Option<ItemStreamVersion>,
    publication: ItemHistoryPublication,
}

impl ItemHistoryMetadata {
    pub fn try_new(
        address: ItemHistoryAddress,
        version: ItemStreamVersion,
        previous_version: Option<ItemStreamVersion>,
        publication: ItemHistoryPublication,
    ) -> StorageResult<Self> {
        let reference = ItemHistoryReference::try_new(address, version)?;
        if previous_version.is_some_and(|previous| previous.get() == 0 || previous >= version) {
            return Err(StorageError::internal("Invalid item history metadata"));
        }
        publication.require_predecessor(previous_version)?;
        Ok(Self {
            reference,
            previous_version,
            publication,
        })
    }

    pub fn reference(&self) -> &ItemHistoryReference {
        &self.reference
    }

    pub fn address(&self) -> &ItemHistoryAddress {
        self.reference.address()
    }

    pub fn version(&self) -> ItemStreamVersion {
        self.reference.version()
    }

    pub fn previous_version(&self) -> Option<ItemStreamVersion> {
        self.previous_version
    }

    pub fn publication(&self) -> &ItemHistoryPublication {
        &self.publication
    }

    pub(crate) fn native_versionstamp(&self) -> Option<[u8; 12]> {
        match &self.publication {
            ItemHistoryPublication::Event { table_position, .. } => {
                table_position.offset().versionstamp()
            }
            ItemHistoryPublication::PredecessorAnchor => None,
        }
    }

    pub(crate) fn bind_versionstamp(mut self, stamp: [u8; 12]) -> StorageResult<Self> {
        let Some(declared) = self.native_versionstamp() else {
            return if stamp == [0; 12] {
                Ok(self)
            } else {
                Err(StorageError::internal(
                    "Non-native history has a commit stamp",
                ))
            };
        };
        // FDB replaces the ten transaction bytes; its user ordinal is fixed
        // before commit. A committed declaration must already agree exactly.
        if declared != stamp && (declared[..10] != [255; 10] || declared[10..] != stamp[10..]) {
            return Err(StorageError::internal(
                "History commit stamp differs from its publication",
            ));
        }
        if let ItemHistoryPublication::Event { table_position, .. } = &mut self.publication {
            *table_position = table_position
                .with_versionstamp(stamp)
                .map_err(|_| StorageError::internal("Invalid history commit stamp"))?;
        }
        Ok(self)
    }

    pub fn kind(&self) -> ItemHistoryKind {
        match self.publication {
            ItemHistoryPublication::Event { kind, .. } => kind,
            ItemHistoryPublication::PredecessorAnchor => ItemHistoryKind::Image,
        }
    }
}

pub enum ItemHistoryChange {
    Image {
        item: WireItem,
        indexers: IndexerDeclaration,
    },
    Deleted {
        key: KeyAttributes,
    },
}

pub struct ItemHistoryEntry {
    metadata: ItemHistoryMetadata,
    change: ItemHistoryChange,
}

impl ItemHistoryEntry {
    pub fn try_new(
        metadata: ItemHistoryMetadata,
        change: ItemHistoryChange,
    ) -> StorageResult<Self> {
        let kind = match &change {
            ItemHistoryChange::Image { .. } => ItemHistoryKind::Image,
            ItemHistoryChange::Deleted { .. } => ItemHistoryKind::Deleted,
        };
        if metadata.kind() != kind {
            return Err(StorageError::internal(
                "History payload differs from its metadata kind",
            ));
        }
        if let ItemHistoryChange::Deleted { key } = &change
            && key.is_empty()
        {
            return Err(StorageError::internal("Invalid history delete marker"));
        }
        Ok(Self { metadata, change })
    }

    pub fn metadata(&self) -> &ItemHistoryMetadata {
        &self.metadata
    }

    pub fn change(&self) -> &ItemHistoryChange {
        &self.change
    }

    pub fn into_parts(self) -> (ItemHistoryMetadata, ItemHistoryChange) {
        (self.metadata, self.change)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ItemHistoryMetadataInput {
    address: ItemHistoryAddress,
    version: ItemStreamVersion,
    previous_version: Option<ItemStreamVersion>,
    publication: ItemHistoryPublication,
}

impl<'de> Deserialize<'de> for ItemHistoryMetadata {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let input = ItemHistoryMetadataInput::deserialize(deserializer)?;
        Self::try_new(
            input.address,
            input.version,
            input.previous_version,
            input.publication,
        )
        .map_err(serde::de::Error::custom)
    }
}
