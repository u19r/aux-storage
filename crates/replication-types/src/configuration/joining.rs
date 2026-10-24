use serde::{Deserialize, Serialize};
use storage_types::{TableHistoryId, TableName};

use super::{
    installed::{InstalledTableState, RegionalSourceBinding, RegionalTablePhase},
    membership::MembershipRevision,
    table::{SchemaRevision, TableReplicationConfig},
};

/// The physical database allocates its incarnation when reserving this hidden
/// definition. Callers cannot predict a KV allocator or select an existing row.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoiningTableDefinition {
    pub name: TableName,
    pub configuration: TableReplicationConfig,
    pub membership: MembershipRevision,
    pub sources: Vec<RegionalSourceBinding>,
    pub schemas: Vec<SchemaRevision>,
}

impl JoiningTableDefinition {
    pub fn bind_physical(&self, physical: TableHistoryId) -> InstalledTableState {
        InstalledTableState {
            name: self.name.clone(),
            physical,
            configuration: self.configuration.clone(),
            membership: self.membership,
            phase: RegionalTablePhase::Joining,
            sources: self.sources.clone(),
            schemas: self.schemas.clone(),
        }
    }
}
