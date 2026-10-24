use replication_types::{
    configuration::{
        installed::RegionalTablePhase, proposal::ControlProposal, table::RegionalMode,
    },
    error::ReplicationProtocolError,
    identity::{
        replica::DatabaseGroupId,
        resource::{OperationIdentity, TableIdentity, TableLineage},
    },
};

pub(super) fn validate_creation(
    proposal: &ControlProposal,
    home: DatabaseGroupId,
) -> Result<(), ReplicationProtocolError> {
    let operation = proposal.operation();
    let primary = proposal.participant(&operation.home_region)?;
    let state = &primary.proposed;
    let identity = OperationIdentity::try_from(<[u8; 16]>::from(operation.id))?;
    if proposal.participants().len() != 1
        || primary.before.is_some()
        || u64::from(operation.expected) != 0
        || state.configuration.table != TableIdentity::from_creation(identity)
        || state.configuration.lineage != TableLineage::from_creation(identity)
        || state.configuration.regional != RegionalMode::Local
        || state.phase != RegionalTablePhase::Active
        || state.sources.len() != 1
        || state.sources[0].database_group != home
    {
        return Err(ReplicationProtocolError::InvalidTransition {
            context: "local creation registration",
        });
    }
    Ok(())
}
