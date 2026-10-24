use serde::{Deserialize, Serialize};

use crate::{
    identity::{
        origin::{MutationOrdinal, OriginId, OriginPosition},
        resource::{ControlOperationId, HistoryIdentity, OriginalStreamRecordId, TableLineage},
    },
    protocol::CanonicalBytes,
};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct OriginEventId {
    pub lineage: TableLineage,
    pub origin: OriginId,
    pub position: OriginPosition,
    pub ordinal: MutationOrdinal,
}

impl OriginEventId {
    pub(super) fn validate(&self) -> Result<(), crate::error::ReplicationProtocolError> {
        if u64::from(self.position) == 0 {
            return Err(crate::error::ReplicationProtocolError::InvalidOrigin {
                context: "event source position",
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum VersionIdentity {
    Mutation(OriginEventId),
    Baseline {
        lineage: TableLineage,
        key: CanonicalItemKey,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum CanonicalEventId {
    Origin(OriginEventId),
    Control {
        lineage: TableLineage,
        operation: ControlOperationId,
    },
    Adopted {
        history: HistoryIdentity,
        original: OriginalStreamRecordId,
    },
    Expiration {
        lineage: TableLineage,
        victim: VersionIdentity,
    },
}

impl From<OriginEventId> for CanonicalEventId {
    fn from(value: OriginEventId) -> Self {
        Self::Origin(value)
    }
}

impl VersionIdentity {
    pub(super) fn validate(&self) -> Result<(), crate::error::ReplicationProtocolError> {
        match self {
            Self::Mutation(id) => id.validate(),
            Self::Baseline { .. } => Ok(()),
        }
    }

    pub fn lineage(&self) -> TableLineage {
        match self {
            Self::Mutation(id) => id.lineage,
            Self::Baseline { lineage, .. } => *lineage,
        }
    }
    pub fn expiration_id(self) -> CanonicalEventId {
        CanonicalEventId::Expiration {
            lineage: self.lineage(),
            victim: self,
        }
    }
}

/// A canonical key codec produces these bytes. No Debug output discloses the
/// key.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "CanonicalBytes", into = "CanonicalBytes")]
pub struct CanonicalItemKey(CanonicalBytes);

impl TryFrom<CanonicalBytes> for CanonicalItemKey {
    type Error = crate::error::ReplicationProtocolError;
    fn try_from(value: CanonicalBytes) -> Result<Self, Self::Error> {
        validate_item_key(&value)?;
        Ok(Self(value))
    }
}

impl From<CanonicalItemKey> for CanonicalBytes {
    fn from(value: CanonicalItemKey) -> Self {
        value.0
    }
}

impl AsRef<[u8]> for CanonicalItemKey {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl std::fmt::Debug for CanonicalItemKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CanonicalItemKey(<redacted>)")
    }
}

impl CanonicalEventId {
    pub(super) fn validate(&self) -> Result<(), crate::error::ReplicationProtocolError> {
        match self {
            Self::Origin(id) => id.validate(),
            Self::Expiration { lineage, victim } => {
                victim.validate()?;
                if victim.lineage() != *lineage {
                    return Err(crate::error::ReplicationProtocolError::LineageMismatch {
                        context: "expiration victim identity",
                    });
                }
                Ok(())
            }
            Self::Control { .. } | Self::Adopted { .. } => Ok(()),
        }
    }

    pub fn is_bound_to(&self, lineage: TableLineage) -> bool {
        match self {
            Self::Origin(id) => id.lineage == lineage,
            Self::Control { lineage: own, .. } => *own == lineage,
            Self::Expiration {
                lineage: own,
                victim,
            } => *own == lineage && victim.lineage() == lineage,
            // Adopted history receives its lineage binding from the adoption
            // manifest.
            Self::Adopted { .. } => true,
        }
    }
}

impl VersionIdentity {
    pub fn is_bound_to(&self, order: &super::order::WinnerOrder, key: &CanonicalItemKey) -> bool {
        use super::order::WinnerOrder;
        match (self, order) {
            (
                Self::Baseline { lineage, key: own },
                WinnerOrder::Baseline {
                    lineage: ordered,
                    key: ordered_key,
                    ..
                },
            ) => lineage == ordered && own == key && own == ordered_key,
            (
                Self::Mutation(id),
                WinnerOrder::Mutation {
                    origin,
                    position,
                    ordinal,
                    ..
                },
            ) => &id.origin == origin && id.position == *position && id.ordinal == *ordinal,
            _ => false,
        }
    }
}

fn validate_item_key(value: &CanonicalBytes) -> Result<(), crate::error::ReplicationProtocolError> {
    if value.as_ref().is_empty() {
        return Err(crate::error::ReplicationProtocolError::InvalidIdentity {
            context: "identity.validate_item_key",
        });
    }
    Ok(())
}
