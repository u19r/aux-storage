use replication_provider::runtime_configuration::deployment::DeploymentConfiguration;
use replication_types::configuration::{installed::InstalledTableState, proposal::ControlProposal};
use storage_types::{StorageResult, TableName, UpdateTableRequest};

use super::{primary_transfer::PrimaryTransferRequest, replica_request::ReplicaRemovalRequest};

pub(crate) enum LifecycleUpdate {
    Remove(ReplicaRemovalRequest),
    Transfer(PrimaryTransferRequest),
}

impl LifecycleUpdate {
    pub(crate) fn try_from_update(request: UpdateTableRequest) -> StorageResult<Self> {
        if request.aux_configuration_primary_region.is_some() {
            return PrimaryTransferRequest::try_from_update(request).map(Self::Transfer);
        }
        ReplicaRemovalRequest::try_from_update(request).map(Self::Remove)
    }

    pub(crate) fn table_name(&self) -> &TableName {
        match self {
            Self::Remove(request) => &request.table,
            Self::Transfer(request) => &request.table,
        }
    }

    pub(crate) fn bind(
        &self,
        state: &InstalledTableState,
        current: &ControlProposal,
        configuration: &DeploymentConfiguration,
    ) -> StorageResult<ControlProposal> {
        match self {
            Self::Remove(request) => request.bind(state, current, configuration),
            Self::Transfer(request) => request.bind(state, current, configuration),
        }
    }
}
