//! Pair messages (design B, REG-0 "Wire shapes"). JSON in `AUXR` zlib frames.

use serde::{Deserialize, Serialize};
use storage_types::{
    KeyAttributes, PairId, RegionId, RegionalOrder, StoredTableInfo, StoredWireItem,
    SystemStreamLineageId, SystemStreamOffset, SystemStreamShardId, TableName,
};

/// Every request names its sender and receiver rank; the server rejects any
/// other pair of regions.
#[derive(Clone, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct RegionalEnvelope {
    pub from: RegionId,
    pub to: RegionId,
    pub request: RegionalRequest,
}

#[derive(Clone, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub enum RegionalRequest {
    /// Source → target: create the hidden target table and its receive cursors
    /// (REG-10).
    Prepare(PrepareTarget),
    /// Source → Joining target: live items; per-key LWW; no cursor (REG-10).
    Scan(ScanPage),
    /// Sender → receiver: one interval of one source lineage (REG-6, REG-9).
    Page(ReplicationPage),
    /// Source → target: close scan acceptance, Joining → Active (REG-10).
    Activate(PairTable),
    /// Coordinator → leaving region: Active → Leaving; reply with the final
    /// heads (REG-11).
    Leave(PairTable),
    /// Leaving region → remaining region: finish when its cursors cover `last`
    /// (REG-11).
    Finish(FinishPair),
    /// Declared loss: end the pair at once (REG-11).
    Force(PairTable),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RegionalResponse {
    Prepared,
    ScanApplied,
    /// The committed receive prefix of the page's lineage after this request.
    Ack(PageAck),
    /// The page does not start at the receiver's cursor; nothing changed.
    Gap {
        expected: Option<SystemStreamOffset>,
    },
    Activated,
    Left {
        last: Vec<LineageHead>,
    },
    Finished,
    /// Finish: the receiver's cursors do not yet cover the final heads.
    Pending,
    Forced,
    Rejected(RegionalRejection),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RegionalRejection {
    /// The table has no binding of this pair (ended, replaced, never existed).
    UnknownPair,
    /// Prepare: the target table already exists without this pair.
    TableExists,
    /// Scan or Activate at a table that is not Joining in this pair.
    NotJoining,
    /// Membership race: the receiver's own operation wins (REG-11 rank rule).
    Conflict,
    /// A child lineage's first page arrived before each parent cursor was
    /// sealed.
    ParentNotSealed,
    /// Envelope regions do not match this server's configuration.
    WrongRegion,
    /// The handler failed (native error, deadline, unknown outcome): the caller
    /// retries later.
    Unavailable,
    /// This build does not handle the request yet.
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LineageRef {
    pub shard: SystemStreamShardId,
    pub lineage: SystemStreamLineageId,
}

/// A position prefix of one lineage: everything through `through` (None:
/// nothing), and whether the lineage is sealed with `through` as its last
/// entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineageHead {
    pub lineage: LineageRef,
    pub through: Option<SystemStreamOffset>,
    pub sealed: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PairTable {
    pub pair: PairId,
    pub table: TableName,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrepareTarget {
    pub pair: PairId,
    /// The source table's schema, GSIs, stream specification and retention (B
    /// resets the counters).
    pub table: StoredTableInfo,
    /// The source's send holds at enable: the target's initial receive cursors.
    pub heads: Vec<LineageHead>,
}

#[derive(Clone, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct ScanItem {
    pub item: StoredWireItem,
    pub order: RegionalOrder,
}

#[derive(Clone, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct ScanPage {
    pub pair: PairId,
    pub table: TableName,
    pub items: Vec<ScanItem>,
}

#[derive(Clone, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub enum PageChange {
    /// Boxed: an item image is several hundred bytes and the other variants
    /// carry none.
    Put {
        item: Box<StoredWireItem>,
    },
    Delete,
    /// R1: the sender applied the receiver's delete of this key at this order.
    Echo,
}

#[derive(Clone, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct PageEntry {
    pub position: SystemStreamOffset,
    pub keys: KeyAttributes,
    pub order: RegionalOrder,
    pub change: PageChange,
}

/// One interval `(after, through]` of one source lineage. Entries are the sent
/// ones (Origin, Echo) in position order; Applied and unmarked entries are
/// covered by the interval but not listed.
#[derive(Clone, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct ReplicationPage {
    pub pair: PairId,
    pub table: TableName,
    pub lineage: LineageRef,
    /// Present only on a lineage's first page (`after == None`) when it has
    /// parents.
    pub parents: Vec<LineageRef>,
    pub after: Option<SystemStreamOffset>,
    pub through: Option<SystemStreamOffset>,
    /// `through` is the last entry of a sealed lineage.
    pub sealed: bool,
    pub entries: Vec<PageEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageAck {
    pub through: Option<SystemStreamOffset>,
    pub sealed: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FinishPair {
    pub pair: PairId,
    pub table: TableName,
    pub last: Vec<LineageHead>,
}
