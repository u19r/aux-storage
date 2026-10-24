use serde::{Deserialize, Serialize};

use super::table::ConfigRevision;
use crate::{
    identity::{
        replica::{DatabaseGroupId, RegionId},
        resource::{ControlOperationId, TableIdentity, TableLineage},
    },
    protocol::CanonicalDigest,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConfigurationChange {
    CreateTable {
        home: DatabaseGroupId,
    },
    RetireTable,
    EnableGlobal {
        primary: RegionId,
        members: Vec<RegionId>,
    },
    AddRegion {
        region: RegionId,
    },
    RemoveRegion {
        region: RegionId,
    },
    DisableGlobal,
    TtlPolicy {
        definition: super::table::TtlPolicyChange,
        requested_at: storage_types::TimestampMillis,
    },
    TableMetadata {
        definition: super::metadata::TableMetadataChange,
        requested_at: storage_types::TimestampMillis,
    },
    TransferCoordinator {
        target: RegionId,
    },
}

/// Content-bound immutable operation, reused through prepare, install and
/// recovery.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlOperation {
    pub id: ControlOperationId,
    pub table: TableIdentity,
    pub lineage: TableLineage,
    pub expected: ConfigRevision,
    pub proposed: ConfigRevision,
    pub content: CanonicalDigest,
    pub change: ConfigurationChange,
    pub home_region: RegionId,
    pub participants: Vec<RegionId>,
    pub before: Option<super::table::TableReplicationConfig>,
}

impl ControlOperation {
    pub fn validate(&self) -> Result<(), crate::error::ReplicationProtocolError> {
        if self.proposed != self.expected.checked_next()? {
            return Err(crate::error::ReplicationProtocolError::RevisionMismatch {
                context: "control.validate",
            });
        }
        if self.participants.is_empty() || self.participants.windows(2).any(|w| w[0] >= w[1]) {
            return Err(
                crate::error::ReplicationProtocolError::IncompleteCertificate {
                    context: "control participants must be nonempty, unique and ordered",
                },
            );
        }
        if let Some(before) = &self.before {
            before.validate()?;
            if before.table != self.table
                || (!matches!(self.change, ConfigurationChange::EnableGlobal { .. })
                    && before.lineage != self.lineage)
                || before.config != self.expected
            {
                return Err(crate::error::ReplicationProtocolError::RevisionMismatch {
                    context: "control.validate",
                });
            }
        } else if !matches!(self.change, ConfigurationChange::CreateTable { .. }) {
            return Err(
                crate::error::ReplicationProtocolError::IncompleteCertificate {
                    context: "control.validate",
                },
            );
        }
        self.validate_home_region()?;
        self.validate_regional_change()?;
        Ok(())
    }

    fn validate_home_region(&self) -> Result<(), crate::error::ReplicationProtocolError> {
        if let Some(super::table::RegionalMode::Global { primary, .. }) =
            self.before.as_ref().map(|before| &before.regional)
            && primary != &self.home_region
        {
            return Err(crate::error::ReplicationProtocolError::InvalidOrigin {
                context: "control primary",
            });
        }
        if let ConfigurationChange::EnableGlobal { primary, .. } = &self.change
            && (primary != &self.home_region
                || self
                    .before
                    .as_ref()
                    .is_some_and(|before| before.lineage == self.lineage)
                || self.lineage
                    != crate::identity::resource::TableLineage::try_from(<[u8; 16]>::from(
                        self.id,
                    ))?)
        {
            return Err(crate::error::ReplicationProtocolError::InvalidOrigin {
                context: "fresh enable lineage",
            });
        }
        Ok(())
    }

    fn validate_regional_change(&self) -> Result<(), crate::error::ReplicationProtocolError> {
        let mut affected = self.affected_regions()?;
        affected.sort();
        if affected != self.participants || !affected.contains(&self.home_region) {
            return Err(
                crate::error::ReplicationProtocolError::IncompleteCertificate {
                    context: "control.validate_regional_change",
                },
            );
        }
        Ok(())
    }

    fn affected_regions(&self) -> Result<Vec<RegionId>, crate::error::ReplicationProtocolError> {
        use ConfigurationChange::{AddRegion, DisableGlobal, EnableGlobal, RemoveRegion};

        use super::table::RegionalMode;
        let before = self.before.as_ref().map(|b| &b.regional);
        let members = match before {
            Some(RegionalMode::Global { members, .. }) => Some(members),
            _ => None,
        };
        match (&self.change, before) {
            (EnableGlobal { primary, members }, Some(RegionalMode::Local))
                if members.len() >= 2 && members.contains(primary) =>
            {
                Ok(members.clone())
            }
            (AddRegion { region }, Some(RegionalMode::Global { members, .. }))
                if !members.contains(region) =>
            {
                let mut next = members.clone();
                next.push(region.clone());
                Ok(next)
            }
            (RemoveRegion { region }, Some(RegionalMode::Global { primary, members }))
                if members.contains(region) && region != primary =>
            {
                Ok(members.clone())
            }
            (DisableGlobal, Some(RegionalMode::Global { members, .. })) => Ok(members.clone()),
            (EnableGlobal { .. } | AddRegion { .. } | RemoveRegion { .. } | DisableGlobal, _) => {
                Err(crate::error::ReplicationProtocolError::InvalidTransition {
                    context: "control.affected_regions",
                })
            }
            _ => Ok(members
                .cloned()
                .unwrap_or_else(|| self.participants.clone())),
        }
    }
}
