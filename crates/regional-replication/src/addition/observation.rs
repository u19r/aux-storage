use replication_types::configuration::proposal::ControlProposal;

use crate::bootstrap::SourceSchemaObservation;

/// A descriptive installed snapshot; capture preparation rechecks its revision.
#[derive(Clone)]
pub struct AdditionSourceObservation {
    pub(crate) current: ControlProposal,
    pub(crate) schema: SourceSchemaObservation,
}

impl AdditionSourceObservation {
    pub fn source(&self) -> &replication_types::configuration::installed::InstalledTableState {
        self.schema.source()
    }
    pub fn table(&self) -> &storage_types::StoredTableInfo {
        self.schema.table()
    }
    pub fn ttl(&self) -> Option<&crate::bootstrap::CaptureTtlPolicy> {
        self.schema.ttl()
    }
}
