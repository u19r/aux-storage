use replication_types::{
    identity::origin::{OriginId, OriginPosition},
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use crate::lifecycle::{SealRequest, SealStatus, SealedTableOrigin};

#[derive(Clone, Copy, Serialize, Deserialize)]
pub(super) enum SealPhase {
    RecordingCuts(u32),
    Draining(u32),
    Ready,
    Removed,
    Retired,
    Cancelled,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SealRecord {
    pub request: SealRequest,
    pub content: CanonicalDigest,
    pub vector: CanonicalDigest,
    pub phase: SealPhase,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SealCut {
    pub content: CanonicalDigest,
    pub origin: OriginId,
    pub through: OriginPosition,
    pub drained: bool,
    pub receipts: Vec<(
        replication_types::identity::replica::RegionId,
        OriginPosition,
    )>,
}

impl SealRecord {
    pub(super) fn status(&self) -> StorageResult<SealStatus> {
        Ok(match self.phase {
            SealPhase::RecordingCuts(next) => SealStatus::RecordingCuts(next),
            SealPhase::Draining(next) => SealStatus::Draining(next),
            SealPhase::Ready => {
                let before = self.request.before()?;
                SealStatus::Ready(SealedTableOrigin {
                    operation: self.request.proposal.operation().id,
                    table: before.configuration.table,
                    lineage: before.configuration.lineage,
                    physical: before.physical,
                    content: self.content,
                    vector: self.vector,
                })
            }
            SealPhase::Removed | SealPhase::Retired => SealStatus::Removed,
            SealPhase::Cancelled => SealStatus::Cancelled,
        })
    }
}
