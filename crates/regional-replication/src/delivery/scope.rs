use replication_types::{
    configuration::{installed::RegionalSourceBinding, membership::MembershipRevision},
    identity::{
        origin::OriginId,
        resource::{TableIdentity, TableLineage},
    },
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryScope {
    pub origin: OriginId,
    /// Original publishing table, independent of the relay incarnation.
    pub table: TableIdentity,
    pub local_table: TableIdentity,
    pub lineage: TableLineage,
    pub membership: MembershipRevision,
    pub destination: RegionalSourceBinding,
}

impl DeliveryScope {
    pub(crate) fn source_request(
        &self,
        range: replication_types::event::interval::ClosedSourceInterval,
    ) -> crate::source_frame::SourceFrameRequest {
        crate::source_frame::SourceFrameRequest {
            origin: self.origin.clone(),
            range,
            membership: self.membership,
            tables: vec![(self.table, self.lineage)],
        }
    }

    pub(crate) fn binding(
        &self,
    ) -> replication_types::configuration::membership::RegionalTableBinding {
        replication_types::configuration::membership::RegionalTableBinding {
            source_region: self.origin.region.clone(),
            source_table: self.table,
            lineage: self.lineage,
            local_table: self.destination.table,
            local_config: self.destination.config,
            membership: self.membership,
        }
    }

    pub(crate) fn require_import(
        &self,
        import: &replication_types::regional_import::RegionalImport,
    ) -> storage_types::StorageResult<()> {
        if import.manifest().origin != self.origin
            || import.manifest().membership != self.membership
            || import.bindings().len() != 1
        {
            return Err(crate::native_record::corrupt_record());
        }
        if import.bindings()[0] != self.binding() {
            return Err(crate::native_record::corrupt_record());
        }
        Ok(())
    }
}
