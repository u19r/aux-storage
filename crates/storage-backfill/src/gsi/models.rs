use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use storage_common::TtlIndexKey;
use storage_types::{
    AttributeValue, GlobalSecondaryIndex, IndexName, IndexerDeclaration, KeyAttributes,
    StoredTableInfo, SystemStreamCheckpoint, TableName, WireItem, maintenance::TargetIncarnation,
};

use super::{GsiBuildPhase, GsiRecordError};

#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct GsiBuildTarget {
    pub history_id: storage_types::TableHistoryId,
    pub table_name: TableName,
    pub index_name: IndexName,
    pub incarnation: TargetIncarnation,
}

#[derive(Clone, Default, PartialEq)]
pub struct GsiProjectionLifecycle {
    pub(super) writable_targets: Vec<GsiBuildTarget>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct GsiBuild<Cursor> {
    pub target: GsiBuildTarget,
    pub state: GsiState<Cursor>,
}

pub struct GsiBuildCandidates<Cursor> {
    pub scanned_through: Option<GsiBuildTarget>,
    pub(super) selected: Option<GsiBuild<Cursor>>,
}

pub enum GsiBuildDiscovery {
    Continue,
    SelectionComplete,
}

pub struct GsiRecord {
    pub history_id: storage_types::TableHistoryId,
    pub table_name: String,
    pub index_name: String,
    pub incarnation: Vec<u8>,
    pub phase: String,
    pub state: Vec<u8>,
}

pub struct GsiStateRecord {
    pub phase: &'static str,
    pub state: Vec<u8>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum GsiState<Cursor> {
    Filling(GsiScanning<Cursor>),
    Building(GsiNativeBuilding),
    Active(GsiActive),
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GsiNativeBuilding {
    pub low: SystemStreamCheckpoint,
    /// An interrupted native index must be removed before the new build starts.
    pub retired: Option<TargetIncarnation>,
}

pub type GsiScanning<Cursor> = crate::ScanProgress<SystemStreamCheckpoint, Cursor>;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GsiActive {
    pub cursor: SystemStreamCheckpoint,
}

impl<Cursor: Serialize> TryFrom<&GsiState<Cursor>> for GsiStateRecord {
    type Error = GsiRecordError;

    fn try_from(state: &GsiState<Cursor>) -> Result<Self, Self::Error> {
        Ok(Self {
            phase: <&'static str>::from(state),
            state: serde_json::to_vec(&state).map_err(|_| GsiRecordError::Position)?,
        })
    }
}

impl<Cursor> From<&GsiState<Cursor>> for &'static str {
    fn from(state: &GsiState<Cursor>) -> Self {
        match state {
            GsiState::Filling(_) => "filling",
            GsiState::Building(_) => "building",
            GsiState::Active(_) => "active",
        }
    }
}

impl<Cursor: serde::de::DeserializeOwned> TryFrom<GsiRecord> for GsiBuild<Cursor> {
    type Error = GsiRecordError;

    fn try_from(record: GsiRecord) -> Result<Self, Self::Error> {
        let target = GsiBuildTarget::try_from((
            record.history_id,
            record.table_name,
            record.index_name,
            record.incarnation,
        ))?;
        let phase = GsiBuildPhase::try_from(record.phase.as_str())?;
        let state: GsiState<Cursor> =
            serde_json::from_slice(&record.state).map_err(|_| GsiRecordError::Position)?;
        if phase != GsiBuildPhase::from(&state) {
            return Err(GsiRecordError::Phase);
        }
        Ok(Self { target, state })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct GsiPageLimit(usize);

pub struct GsiScanObservation<Cursor> {
    pub expected_after: Option<Cursor>,
    pub end: GsiScanPageEnd<Cursor>,
    pub projections: GsiProjectionWriteBatch,
}

/// Provider-decoded base-table page before any projection decisions are made.
pub struct GsiScanSourcePage<Cursor> {
    pub table: std::sync::Arc<StoredTableInfo>,
    pub expected_after: Option<Cursor>,
    pub end: GsiScanPageEnd<Cursor>,
    pub items: Vec<GsiScannedSourceItem>,
}

pub type GsiScanPageEnd<Cursor> = crate::ScanPageEnd<Cursor, ()>;

/// Durable provider state needed to decide whether activation is safe.
pub struct GsiActivationState;

#[derive(Clone, PartialEq)]
pub struct GsiProjectionKey {
    pub index_name: IndexName,
    /// Attributes that place the row in the secondary index.
    pub index_key: KeyAttributes,
    /// Primary-table attributes that identify the indexed source item.
    pub table_item_key: KeyAttributes,
    /// Typed TTL destination when the build targets the hidden expiration
    /// index. Ordinary secondary-index projections leave this empty.
    pub ttl_index_key: Option<TtlIndexKey>,
}

pub struct GsiProjectedRow {
    pub key: GsiProjectionKey,
    /// Complete source value used to derive backend-native secondary indexes.
    pub source_item: WireItem,
    /// Field selection captured with the source version, never current
    /// metadata.
    pub projection: storage_types::FrozenImageProjection,
    /// Validated declaration captured with the source value. Physical
    /// lowerers use its order for native covering-index columns.
    pub indexers: IndexerDeclaration,
}

impl GsiProjectedRow {
    pub fn projected_image(&self) -> storage_types::StorageResult<std::borrow::Cow<'_, WireItem>> {
        self.projection.project(&self.source_item)
    }
}

/// Source row decoded by a backend during the base-table scan.
pub struct GsiScannedSourceItem {
    pub item: WireItem,
    pub indexers: Vec<String>,
}

pub struct GsiProjectionWrite {
    /// Exact physical projection row affected by this write.
    pub key: GsiProjectionKey,
    /// Final projection value, or no value to clear the entry.
    pub row: Option<GsiProjectedRow>,
}

/// Provider-ready projection writes with at most one final write per physical
/// key.
#[derive(Default)]
pub struct GsiProjectionWriteBatch(pub(super) Vec<GsiProjectionWrite>);

impl IntoIterator for GsiProjectionWriteBatch {
    type Item = GsiProjectionWrite;
    type IntoIter = std::vec::IntoIter<GsiProjectionWrite>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

pub struct GsiWriteRequest<'a> {
    pub old_item: Option<&'a WireItem>,
    pub new_item: Option<&'a WireItem>,
    pub indexers: &'a [String],
}

#[derive(PartialEq)]
pub struct GsiKeyPart<'a> {
    pub name: &'a str,
    pub value: AttributeValue,
}

pub type GsiKeyParts<'a> = SmallVec<[GsiKeyPart<'a>; 2]>;

pub enum GsiWriteAction<'a> {
    Delete {
        index: &'a GlobalSecondaryIndex,
        gsi_key: GsiKeyParts<'a>,
        table_key: GsiKeyParts<'a>,
    },
    Put {
        index: &'a GlobalSecondaryIndex,
        gsi_key: GsiKeyParts<'a>,
        table_key: GsiKeyParts<'a>,
        projection: storage_types::FrozenImageProjection,
    },
}

pub type GsiIndexWriteActions<'a> = SmallVec<[GsiWriteAction<'a>; 2]>;

pub enum GsiChange<Cursor> {
    Persist {
        projections: GsiProjectionWriteBatch,
        next_state: GsiState<Cursor>,
    },
    Superseded,
    Idle,
}

impl TryFrom<usize> for GsiPageLimit {
    type Error = super::GsiLifecycleError;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        if value == 0 || value >= u32::MAX as usize {
            return Err(super::GsiLifecycleError::InvalidPageLimit);
        }
        Ok(Self(value))
    }
}

impl GsiPageLimit {
    pub const fn halved(self) -> Self {
        Self(if self.0 > 1 { self.0 / 2 } else { 1 })
    }

    #[must_use]
    pub const fn including_lookahead(self) -> u32 {
        self.0 as u32 + 1
    }
}

impl From<GsiPageLimit> for u32 {
    fn from(value: GsiPageLimit) -> Self {
        value.0 as Self
    }
}

impl From<GsiPageLimit> for usize {
    fn from(value: GsiPageLimit) -> Self {
        value.0
    }
}
