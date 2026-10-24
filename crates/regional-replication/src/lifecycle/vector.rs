use replication_types::{
    identity::{
        origin::{OriginId, OriginPosition},
        replica::RegionId,
    },
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};

/// Transportable facts; receiving these values does not grant removal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealedOriginCut {
    pub origin: OriginId,
    pub through: OriginPosition,
    pub receipts: Vec<(RegionId, OriginPosition)>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealVectorPage {
    pub content: CanonicalDigest,
    pub vector: CanonicalDigest,
    pub cuts: Vec<SealedOriginCut>,
    pub next_partition: Option<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealReceptionProgress {
    pub next_partition: u32,
    pub complete: bool,
}
