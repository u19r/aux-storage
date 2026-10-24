use replication_provider::runtime_configuration::deployment::DeploymentConfiguration;
use replication_types::{
    configuration::{
        installed::{InstalledTableState, RegionalTablePhase},
        proposal::ControlProposal,
        table::RegionalMode,
    },
    identity::{replica::RegionId, resource::ControlOperationId},
};
use storage_types::{StorageError, StorageResult, TableName, UpdateTableRequest};

use crate::{native_record::corrupt_record, serving::replica_request::require_separate_metadata};

pub(crate) struct PrimaryTransferRequest {
    pub table: TableName,
    operation: ControlOperationId,
    target: RegionId,
}

impl PrimaryTransferRequest {
    pub(super) fn try_from_update(request: UpdateTableRequest) -> StorageResult<Self> {
        require_separate_metadata(&request)?;
        if request.replica_updates.is_some() {
            return Err(StorageError::validation(
                "primary transfer and replica changes require separate controls",
            ));
        }
        let target = request
            .aux_configuration_primary_region
            .ok_or_else(|| StorageError::validation("AuxConfigurationPrimaryRegion is required"))?
            .parse::<RegionId>()
            .map_err(|_| StorageError::validation("invalid configuration primary region"))?;
        Ok(Self {
            table: request.table_name,
            operation: uuid::Uuid::new_v4()
                .into_bytes()
                .try_into()
                .map_err(|_| corrupt_record())?,
            target,
        })
    }

    pub(super) fn bind(
        &self,
        state: &InstalledTableState,
        current: &ControlProposal,
        configuration: &DeploymentConfiguration,
    ) -> StorageResult<ControlProposal> {
        let RegionalMode::Global { primary, members } = &state.configuration.regional else {
            return Err(StorageError::validation(
                "primary transfer requires a global table",
            ));
        };
        if state.phase != RegionalTablePhase::Active || primary != &configuration.region {
            return Err(StorageError::validation(
                "primary transfer requires the active configuration primary",
            ));
        }
        if primary == &self.target || !members.contains(&self.target) {
            return Err(StorageError::validation(
                "transfer target must be another active replica",
            ));
        }
        let proposal =
            replication_provider::control::bind_transfer(self.operation, current, &self.target)
                .map_err(|_| corrupt_record())?;
        super::deployment::require_control_deployment(&proposal, configuration)?;
        Ok(proposal)
    }
}
