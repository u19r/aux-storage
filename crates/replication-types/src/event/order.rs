use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use super::identity::{CanonicalEventId, CanonicalItemKey, VersionIdentity};
use crate::{
    identity::{
        origin::{MutationOrdinal, OriginId, OriginPosition},
        resource::TableLineage,
    },
    protocol::CanonicalDigest,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct HybridLogicalClock {
    pub physical_ms: u64,
    pub logical: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum EffectRank {
    Mutation,
    Expiration,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WinnerOrder {
    Baseline {
        lineage: TableLineage,
        key: CanonicalItemKey,
        rank: EffectRank,
    },
    Mutation {
        clock: HybridLogicalClock,
        origin: OriginId,
        position: OriginPosition,
        ordinal: MutationOrdinal,
        rank: EffectRank,
    },
}

impl Ord for WinnerOrder {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (
                Self::Baseline {
                    lineage: a,
                    key: b,
                    rank: c,
                },
                Self::Baseline {
                    lineage: x,
                    key: y,
                    rank: z,
                },
            ) => (a, b, c).cmp(&(x, y, z)),
            (Self::Baseline { .. }, Self::Mutation { .. }) => Ordering::Less,
            (Self::Mutation { .. }, Self::Baseline { .. }) => Ordering::Greater,
            (
                Self::Mutation {
                    clock: a,
                    origin: b,
                    position: c,
                    ordinal: d,
                    rank: e,
                },
                Self::Mutation {
                    clock: x,
                    origin: y,
                    position: z,
                    ordinal: w,
                    rank: v,
                },
            ) => (a, b, c, d, e).cmp(&(x, y, z, w, v)),
        }
    }
}

impl PartialOrd for WinnerOrder {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl WinnerOrder {
    /// The victim's tuple is unchanged except for rank; no discovery-region
    /// timestamp.
    pub fn expiration_child(&self) -> Self {
        let mut child = self.clone();
        match &mut child {
            Self::Baseline { rank, .. } | Self::Mutation { rank, .. } => {
                *rank = EffectRank::Expiration
            }
        }
        child
    }
    pub fn is_expiration(&self) -> bool {
        matches!(
            self,
            Self::Baseline {
                rank: EffectRank::Expiration,
                ..
            } | Self::Mutation {
                rank: EffectRank::Expiration,
                ..
            }
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WinnerProvenance {
    pub lineage: TableLineage,
    pub key: CanonicalItemKey,
    pub version: VersionIdentity,
    pub event: Option<CanonicalEventId>,
    pub order: WinnerOrder,
    pub digest: CanonicalDigest,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletedWinnerFloor {
    pub winner: WinnerProvenance,
}

impl WinnerProvenance {
    pub fn validate(&self) -> Result<(), crate::error::ReplicationProtocolError> {
        use crate::error::ReplicationProtocolError as E;
        self.version.validate()?;
        if let Some(event) = &self.event {
            event.validate()?;
        }
        if self.version.lineage() != self.lineage
            || !self.version.is_bound_to(&self.order, &self.key)
        {
            return Err(E::IdentityContentMismatch {
                context: "order.validate",
            });
        }
        match (&self.event, &self.version) {
            (None, VersionIdentity::Baseline { .. }) if !self.order.is_expiration() => Ok(()),
            (Some(CanonicalEventId::Origin(id)), VersionIdentity::Mutation(version))
                if id == version && !self.order.is_expiration() =>
            {
                Ok(())
            }
            (Some(CanonicalEventId::Expiration { lineage, victim }), _)
                if *lineage == self.lineage
                    && victim == &self.version
                    && self.order.is_expiration() =>
            {
                Ok(())
            }
            _ => Err(E::IdentityContentMismatch {
                context: "order.validate",
            }),
        }
    }
}
