use replication_types::{
    identity::resource::{ControlOperationId, TableIdentity, TableLineage},
    protocol::CanonicalDigest,
};
use storage_types::TableHistoryId;

/// Native capability: a Ready seal with every survivor's contiguous delivery
/// receipt. TCP bytes cannot construct it directly.
#[derive(Clone)]
pub struct SealedTableOrigin {
    pub(crate) operation: ControlOperationId,
    pub(crate) table: TableIdentity,
    pub(crate) lineage: TableLineage,
    pub(crate) physical: TableHistoryId,
    pub(crate) content: CanonicalDigest,
    pub(crate) vector: CanonicalDigest,
}

impl SealedTableOrigin {
    pub fn operation(&self) -> ControlOperationId {
        self.operation
    }
    pub fn table(&self) -> TableIdentity {
        self.table
    }
    pub fn lineage(&self) -> TableLineage {
        self.lineage
    }
    pub fn physical(&self) -> TableHistoryId {
        self.physical
    }
    pub fn content(&self) -> CanonicalDigest {
        self.content
    }
    pub fn vector(&self) -> CanonicalDigest {
        self.vector
    }
}

pub enum SealStatus {
    RecordingCuts(u32),
    Draining(u32),
    Ready(SealedTableOrigin),
    Removed,
    Cancelled,
}
