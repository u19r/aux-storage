use serde::{Deserialize, Serialize};
use storage_types::{TableHistoryId, TableName};

use super::{
    membership::{MembershipRevision, RegionalTableBinding},
    table::{RegionalMode, SchemaRevision, TableReplicationConfig},
};
use crate::{
    error::ReplicationProtocolError,
    identity::{
        journal_partition::JournalPartitionCount,
        origin::{JournalGeneration, OriginId},
        replica::{DatabaseGroupId, RegionId},
        resource::TableIdentity,
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RegionalTablePhase {
    Capturing,
    Joining,
    Active,
    Sealing,
    Removed,
    Blocked,
}

/// A source is a durable database generation, never a sending API process.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegionalSourceBinding {
    pub region: RegionId,
    pub database_group: DatabaseGroupId,
    pub generation: JournalGeneration,
    pub partitions: JournalPartitionCount,
    pub table: TableIdentity,
    pub config: super::table::ConfigRevision,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstalledTableState {
    pub name: TableName,
    pub physical: TableHistoryId,
    pub configuration: TableReplicationConfig,
    pub membership: MembershipRevision,
    pub phase: RegionalTablePhase,
    pub sources: Vec<RegionalSourceBinding>,
    pub schemas: Vec<SchemaRevision>,
}

impl InstalledTableState {
    pub fn validate(&self) -> Result<(), ReplicationProtocolError> {
        self.configuration.validate()?;
        if self.sources.windows(2).any(|w| w[0].region >= w[1].region)
            || self.schemas.windows(2).any(|w| w[0] >= w[1])
            || self.schemas.last() != Some(&self.configuration.schema)
        {
            return Err(ReplicationProtocolError::InvalidScope {
                context: "installed table state",
            });
        }
        if let RegionalMode::Global { members, .. } = &self.configuration.regional
            && members
                .iter()
                .any(|region| !self.sources.iter().any(|source| &source.region == region))
        {
            return Err(ReplicationProtocolError::InvalidOrigin {
                context: "installed member source absent",
            });
        }
        Ok(())
    }

    pub fn captures_origin(&self) -> bool {
        self.phase == RegionalTablePhase::Capturing
            || matches!(self.configuration.regional, RegionalMode::Global { .. })
    }

    fn require_import_phase(&self) -> Result<(), ReplicationProtocolError> {
        if !matches!(
            self.phase,
            RegionalTablePhase::Active | RegionalTablePhase::Joining | RegionalTablePhase::Sealing
        ) {
            return Err(ReplicationProtocolError::InvalidTransition {
                context: "regional table cannot accept imports",
            });
        }
        Ok(())
    }

    pub fn validate_import(
        &self,
        input: (&RegionalTableBinding, &OriginId),
        local: &RegionId,
    ) -> Result<(), ReplicationProtocolError> {
        self.validate()?;
        self.require_import_phase()?;
        let (binding, origin) = input;
        if !self.sources.iter().any(|source| {
            &source.region == local
                && source.table == self.configuration.table
                && source.config == self.configuration.config
        }) {
            return Err(ReplicationProtocolError::InvalidOrigin {
                context: "installed local source binding",
            });
        }
        if binding.local_table != self.configuration.table
            || binding.lineage != self.configuration.lineage
            || binding.local_config != self.configuration.config
            || binding.membership != self.membership
            || binding.source_region != origin.region
        {
            return Err(ReplicationProtocolError::RevisionMismatch {
                context: "installed import binding",
            });
        }
        let RegionalMode::Global { members, .. } = &self.configuration.regional else {
            return Err(ReplicationProtocolError::InvalidScope {
                context: "import into local table",
            });
        };
        if !members.contains(local) || !members.contains(&origin.region) {
            return Err(ReplicationProtocolError::InvalidOrigin {
                context: "import member absent",
            });
        }
        let source = self
            .sources
            .iter()
            .find(|source| source.region == origin.region)
            .ok_or(ReplicationProtocolError::InvalidOrigin {
                context: "import source absent",
            })?;
        if source.database_group != origin.database_group
            || source.generation != origin.journal_generation
            || source.table != binding.source_table
        {
            return Err(ReplicationProtocolError::InvalidOrigin {
                context: "import source generation",
            });
        }
        source
            .partitions
            .require_partition(origin.journal_partition)
    }
}
