use serde::{Deserialize, Serialize};

use super::table::ConfigRevision;
use crate::identity::{
    origin::{OriginId, OriginPosition},
    replica::RegionId,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MembershipRevision(u64);

impl From<u64> for MembershipRevision {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<MembershipRevision> for u64 {
    fn from(value: MembershipRevision) -> Self {
        value.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealedOriginCut {
    pub origin: OriginId,
    pub through: OriginPosition,
}

/// Protected catalog binding. Source identities remain in canonical history;
/// destination-local identities select physical materialization only.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegionalTableBinding {
    pub source_region: RegionId,
    pub source_table: crate::identity::resource::TableIdentity,
    pub local_table: crate::identity::resource::TableIdentity,
    pub lineage: crate::identity::resource::TableLineage,
    pub membership: MembershipRevision,
    pub local_config: ConfigRevision,
}
