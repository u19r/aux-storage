use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use storage_common::TtlIndexKey;
use storage_types::{
    AttributeValue, GlobalSecondaryIndex, IndexName, IndexerDeclaration, ItemRevision,
    KeyAttributes, StoredTableInfo, StreamItemId, SystemStreamCheckpoint, TableName,
    TimestampMillis, WireItem, maintenance::TargetIncarnation,
};
use stream_provider::{StoredStreamPointer, StreamItem};

use super::{GsiBuildPhase, GsiRecordError};

#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct GsiBuildTarget {
    pub table_name: TableName,
    pub index_name: IndexName,
    pub incarnation: TargetIncarnation,
}

#[derive(Clone, Default, PartialEq)]
pub struct GsiProjectionLifecycle {
    pub(super) writable_targets: Vec<GsiBuildTarget>,
}

#[derive(Serialize, Deserialize, PartialEq)]
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

#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub enum GsiState<Cursor> {
    Scanning(GsiScanning<Cursor>),
    CatchingUp(GsiCatchingUp),
    Active(GsiActive),
    Blocked(GsiBlocked),
}

pub type GsiScanning<Cursor> = crate::ScanProgress<SystemStreamCheckpoint, Cursor>;
pub type GsiCatchingUp = crate::TailProgress<SystemStreamCheckpoint>;

#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub struct GsiActive {
    pub cursor: SystemStreamCheckpoint,
    pub pending_high: Option<SystemStreamCheckpoint>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub struct GsiBlocked {
    pub reason: GsiFailureReason,
    pub retained_from: SystemStreamCheckpoint,
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub enum GsiFailureReason {
    HistoryUnavailable,
    ProjectionConflict,
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
            GsiState::Scanning(_) => "scanning",
            GsiState::CatchingUp(_) => "catching-up",
            GsiState::Active(_) => "active",
            GsiState::Blocked(_) => "blocked",
        }
    }
}

impl<Cursor: serde::de::DeserializeOwned> TryFrom<GsiRecord> for GsiBuild<Cursor> {
    type Error = GsiRecordError;

    fn try_from(record: GsiRecord) -> Result<Self, Self::Error> {
        let target =
            GsiBuildTarget::try_from((record.table_name, record.index_name, record.incarnation))?;
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

#[derive(PartialEq, Eq)]
pub enum GsiReadRequest<Cursor> {
    Scan {
        after: Option<Cursor>,
        limit: GsiPageLimit,
    },
    CatchUp {
        after: SystemStreamCheckpoint,
        through: SystemStreamCheckpoint,
        limit: GsiPageLimit,
    },
    Continue {
        after: SystemStreamCheckpoint,
        pending_high: Option<SystemStreamCheckpoint>,
        limit: GsiPageLimit,
    },
    InspectActivation {
        required_from: SystemStreamCheckpoint,
        through: SystemStreamCheckpoint,
    },
}

pub enum GsiObservation<Cursor> {
    Scan(GsiScanObservation<Cursor>),
    Stream(GsiStreamObservation),
    Activation(GsiActivationObservation),
}

pub struct GsiScanObservation<Cursor> {
    pub expected_after: Option<Cursor>,
    pub end: GsiScanPageEnd<Cursor>,
    pub projections: Vec<GsiBuildProjectionObservation>,
}

/// Provider-decoded base-table page before any projection decisions are made.
pub struct GsiScanSourcePage<Cursor> {
    pub table: std::sync::Arc<StoredTableInfo>,
    pub expected_after: Option<Cursor>,
    pub end: GsiScanPageEnd<Cursor>,
    pub items: Vec<GsiScannedSourceItem>,
}

pub type GsiScanPageEnd<Cursor> = crate::ScanPageEnd<Cursor, SystemStreamCheckpoint>;

pub struct GsiStreamObservation {
    pub expected_cursor: SystemStreamCheckpoint,
    /// Closed stream frontier used for this page. Catch-up supplies the build's
    /// fixed high boundary; continuing projection observes the current tail.
    pub through: SystemStreamCheckpoint,
    pub end: GsiStreamPageEnd,
    pub projections: Vec<GsiBuildProjectionObservation>,
}

/// Provider-decoded SystemStream page before referenced images are interpreted.
pub struct GsiStreamSourcePage {
    pub table: storage_types::BoundTableInfo,
    pub end: GsiStreamPageEnd,
    pub candidates: Vec<GsiStreamCandidate>,
}

pub struct GsiStreamCandidate {
    /// Physical pointer identity. This value is never a replay cursor.
    pub pointer_id: StreamItemId,
    /// Timestamp inherited by items embedded in the SystemStream pointer.
    pub created_at: TimestampMillis,
    pub pointer: StoredStreamPointer,
    /// Items loaded from the referenced item stream. Embedded pointers leave
    /// this empty because their images are carried by `pointer`.
    pub referenced_items: Option<Vec<StreamItem>>,
}

/// One provider-decoded item from a referenced item stream, correlated to the
/// SystemStream candidate that requested it.
pub struct GsiReferencedHistoryItem {
    pub candidate_ordinal: usize,
    pub item: StreamItem,
}

pub type GsiStreamPageEnd = crate::TailPageEnd<SystemStreamCheckpoint>;

pub struct GsiActivationObservation {
    pub target_incarnation: TargetIncarnation,
    pub projected_through: SystemStreamCheckpoint,
}

/// Durable provider state needed to decide whether activation is safe.
pub struct GsiActivationState {
    pub target_incarnation: TargetIncarnation,
    pub projected_through: SystemStreamCheckpoint,
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub struct GsiProjection<Key> {
    pub item_version: ItemRevision,
    pub projection: Option<Key>,
}

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
    pub fn projected_image(&self) -> storage_types::StorageResult<WireItem> {
        self.projection.project(&self.source_item)
    }
}

/// Source row decoded by a backend during the base-table scan.
pub struct GsiScannedSourceItem {
    pub item: WireItem,
    pub indexers: Vec<String>,
    pub revision: ItemRevision,
}

pub enum GsiSourceProjection {
    Missing {
        item_version: ItemRevision,
        /// Destination row removed by a stream mutation. A base-table scan
        /// has no previous destination row and therefore leaves this empty.
        previous_key: Option<Box<GsiProjectionKey>>,
    },
    Present {
        item_version: ItemRevision,
        row: Box<GsiProjectedRow>,
    },
}

pub struct GsiBuildProjectionObservation {
    /// Projection derived from the immutable source item or stream mutation.
    pub source: GsiSourceProjection,
    /// Version and physical key currently stored by the destination.
    pub current: Option<GsiProjection<GsiProjectionKey>>,
}

/// Destination projection state required to decide one source observation.
pub struct GsiProjectionStateRead<'a> {
    pub source_ordinal: usize,
    pub key: &'a GsiProjectionKey,
}

/// Complete provider input for one batched destination-state read.
pub struct GsiProjectionStateReadBatch<'a> {
    pub table: &'a StoredTableInfo,
    pub target: &'a GsiBuildTarget,
    pub sources: &'a [GsiSourceProjection],
    pub reads: &'a [GsiProjectionStateRead<'a>],
}

pub(super) enum GsiBuildProjectionChange {
    IgnoreStale,
    Replay,
    Replace {
        remove: Option<Box<GsiProjectionKey>>,
        write: Option<Box<GsiProjectedRow>>,
        item_version: ItemRevision,
    },
    FailClosed(GsiFailureReason),
}

pub struct GsiProjectionWrite {
    /// Exact physical projection row affected by this write.
    pub key: GsiProjectionKey,
    /// Final row value, or no value when the physical row is a tombstone.
    pub row: Option<GsiProjectedRow>,
    /// Source-item version used by provider-native conditional persistence.
    pub item_version: ItemRevision,
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
    pub source_version: ItemRevision,
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

#[derive(PartialEq, Eq)]
pub enum GsiProjectionChange<Key> {
    IgnoreStale,
    Replay,
    Replace {
        remove: Option<Key>,
        write: Option<Key>,
        item_version: ItemRevision,
    },
    FailClosed(GsiFailureReason),
}

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
