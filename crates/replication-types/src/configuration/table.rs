use serde::{Deserialize, Serialize};

use crate::{
    error::ReplicationProtocolError,
    identity::{
        replica::RegionId,
        resource::{OperationIdentity, TableIdentity, TableLineage, TenantId},
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ConfigRevision(u64);
impl From<u64> for ConfigRevision {
    fn from(value: u64) -> Self {
        Self(value)
    }
}
impl From<ConfigRevision> for u64 {
    fn from(value: ConfigRevision) -> Self {
        value.0
    }
}
impl ConfigRevision {
    pub fn checked_next(self) -> Result<Self, ReplicationProtocolError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(ReplicationProtocolError::CounterOverflow {
                context: "table.checked_next",
            })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SchemaRevision(u64);
impl From<u64> for SchemaRevision {
    fn from(value: u64) -> Self {
        Self(value)
    }
}
impl From<SchemaRevision> for u64 {
    fn from(value: SchemaRevision) -> Self {
        value.0
    }
}
impl SchemaRevision {
    pub fn checked_next(self) -> Result<Self, ReplicationProtocolError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(ReplicationProtocolError::CounterOverflow {
                context: "table.checked_next",
            })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RegionalMode {
    Local,
    Global {
        primary: RegionId,
        members: Vec<RegionId>,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableReplicationConfig {
    pub table: TableIdentity,
    pub lineage: TableLineage,
    pub tenant: TenantId,
    pub regional: RegionalMode,
    pub config: ConfigRevision,
    pub schema: SchemaRevision,
}
impl TableReplicationConfig {
    /// Fresh regional creation cannot reuse a retired resource or event
    /// namespace. Existing-table registration and import retain their
    /// assigned identities.
    pub fn require_creation(
        &self,
        operation: OperationIdentity,
    ) -> Result<(), ReplicationProtocolError> {
        if self.table != TableIdentity::from_creation(operation)
            || self.lineage != TableLineage::from_creation(operation)
        {
            return Err(ReplicationProtocolError::IdentityContentMismatch {
                context: "table.require_creation",
            });
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<(), ReplicationProtocolError> {
        if let RegionalMode::Global { primary, members } = &self.regional
            && (members.len() < 2
                || !members.contains(primary)
                || members.windows(2).any(|w| w[0] >= w[1]))
        {
            return Err(ReplicationProtocolError::IncompleteCertificate {
                context: "table.validate",
            });
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TtlPolicyChange(pub storage_types::TimeToLiveSpecification);

impl PartialEq for TtlPolicyChange {
    fn eq(&self, other: &Self) -> bool {
        self.0.enabled == other.0.enabled && self.0.attribute_name == other.0.attribute_name
    }
}

impl Eq for TtlPolicyChange {}

impl std::fmt::Debug for TtlPolicyChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TtlPolicyChange")
            .field("enabled", &self.0.enabled)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StreamPolicyChange(pub storage_types::StreamSpecification);

impl PartialEq for StreamPolicyChange {
    fn eq(&self, other: &Self) -> bool {
        self.0.stream_enabled == other.0.stream_enabled
            && self.0.stream_view_type == other.0.stream_view_type
    }
}

impl Eq for StreamPolicyChange {}

impl std::fmt::Debug for StreamPolicyChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamPolicyChange")
            .field("enabled", &self.0.stream_enabled)
            .finish_non_exhaustive()
    }
}
