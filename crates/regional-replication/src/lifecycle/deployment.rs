use replication_provider::runtime_configuration::deployment::DeploymentConfiguration;
use replication_types::configuration::proposal::ControlProposal;
use storage_types::{StorageError, StorageResult};

use crate::native_record::corrupt_record;

pub(crate) fn require_control_deployment(
    proposal: &ControlProposal,
    configuration: &DeploymentConfiguration,
) -> StorageResult<()> {
    for participant in proposal.participants() {
        let source = participant
            .proposed
            .sources
            .iter()
            .find(|source| source.region == participant.region)
            .ok_or_else(corrupt_record)?;
        if participant.proposed.configuration.tenant != configuration.tenant
            || source.partitions != configuration.journal_partitions
        {
            return Err(StorageError::validation(
                "regional control database layout mismatch",
            ));
        }
        if source.region == configuration.region {
            if source.database_group != configuration.group
                || source.generation != configuration.journal_generation
            {
                return Err(StorageError::validation(
                    "regional control local database generation mismatch",
                ));
            }
        } else if !configuration.peers.iter().any(|peer| {
            peer.region == source.region
                && peer.group == source.database_group
                && peer.generation == source.generation
        }) {
            return Err(StorageError::validation(
                "regional control member has no configured database generation",
            ));
        }
    }
    Ok(())
}
