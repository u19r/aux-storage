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

use crate::{
    native_record::corrupt_record,
    serving::replica_request::{ReplicaActionKind, validate_replica_request},
};

pub(crate) struct ReplicaRemovalRequest {
    pub table: TableName,
    operation: ControlOperationId,
    regions: Vec<RegionId>,
}

impl ReplicaRemovalRequest {
    pub(crate) fn try_from_update(request: UpdateTableRequest) -> StorageResult<Self> {
        if validate_replica_request(&request)? != ReplicaActionKind::Delete {
            return Err(StorageError::validation(
                "replica removal requires delete actions",
            ));
        }
        let updates = request.replica_updates.ok_or_else(corrupt_record)?;
        let mut regions = updates
            .into_iter()
            .map(|update| {
                update
                    .delete
                    .ok_or_else(corrupt_record)?
                    .region_name
                    .parse::<RegionId>()
                    .map_err(|_| StorageError::validation("invalid replica region"))
            })
            .collect::<StorageResult<Vec<_>>>()?;
        regions.sort();
        if regions.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(StorageError::validation("duplicate replica region"));
        }
        Ok(Self {
            table: request.table_name,
            operation: uuid::Uuid::new_v4()
                .into_bytes()
                .try_into()
                .map_err(|_| corrupt_record())?,
            regions,
        })
    }

    pub(crate) fn bind(
        &self,
        state: &InstalledTableState,
        current: &ControlProposal,
        configuration: &DeploymentConfiguration,
    ) -> StorageResult<ControlProposal> {
        let RegionalMode::Global { primary, members } = &state.configuration.regional else {
            return Err(StorageError::validation(
                "replica removal requires a global table",
            ));
        };
        if state.phase != RegionalTablePhase::Active || primary != &configuration.region {
            return Err(StorageError::validation(
                "replica removal requires the active configuration primary",
            ));
        }
        if self.regions.contains(primary) {
            return Err(StorageError::validation(
                "transfer the configuration primary before removing its replica",
            ));
        }
        if self.regions.iter().any(|region| !members.contains(region)) {
            return Err(StorageError::validation(
                "replica region is not a current member",
            ));
        }
        let proposal =
            replication_provider::control::bind_removal(self.operation, current, &self.regions)
                .map_err(|_| {
                    StorageError::validation(
                        "remove one secondary or all secondaries in a replica control",
                    )
                })?;
        super::deployment::require_control_deployment(&proposal, configuration)?;
        Ok(proposal)
    }
}
