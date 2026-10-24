use replication_types::{
    configuration::{
        control::{ConfigurationChange, ControlOperation},
        installed::InstalledTableState,
        joining::JoiningTableDefinition,
        table::RegionalMode,
    },
    error::ReplicationProtocolError,
    identity::replica::RegionId,
};

pub fn validate_joining_definition(
    operation: &ControlOperation,
    input: (&InstalledTableState, &JoiningTableDefinition),
    region: &RegionId,
) -> Result<(), ReplicationProtocolError> {
    match &operation.change {
        ConfigurationChange::EnableGlobal { .. } => {
            validate_enable_joining_definition(operation, input, region)
        }
        ConfigurationChange::AddRegion { .. } => {
            super::addition::validate_addition_target(operation, input, region)
        }
        _ => Err(invalid_join()),
    }
}

fn validate_enable_joining_definition(
    operation: &ControlOperation,
    input: (&InstalledTableState, &JoiningTableDefinition),
    region: &RegionId,
) -> Result<(), ReplicationProtocolError> {
    operation.validate()?;
    let (source, target) = input;
    source.validate()?;
    target.configuration.validate()?;
    let ConfigurationChange::EnableGlobal { primary, members } = &operation.change else {
        return Err(invalid_join());
    };
    if primary == region
        || !members.contains(region)
        || primary != &operation.home_region
        || operation.before.as_ref() != Some(&source.configuration)
        || source.phase != replication_types::configuration::installed::RegionalTablePhase::Active
        || source.configuration.regional != RegionalMode::Local
        || target.name != source.name
        || target.configuration.table == source.configuration.table
        || target.configuration.tenant != source.configuration.tenant
        || target.configuration.lineage != operation.lineage
        || target.configuration.config != operation.proposed
        || target.configuration.schema != source.configuration.schema
        || !matches!(&target.configuration.regional,
            RegionalMode::Global { primary: candidate_primary, members: candidate_members }
                if candidate_primary == primary && candidate_members == members)
        || u64::from(target.membership)
            != u64::from(source.membership)
                .checked_add(1)
                .ok_or_else(invalid_join)?
        || target.schemas != source.schemas
        || target
            .sources
            .windows(2)
            .any(|pair| pair[0].region >= pair[1].region)
        || members.len() != target.sources.len()
        || members
            .iter()
            .zip(&target.sources)
            .any(|(member, binding)| member != &binding.region)
        || !target.sources.iter().any(|binding| {
            &binding.region == region
                && binding.table == target.configuration.table
                && binding.config == operation.proposed
        })
    {
        return Err(invalid_join());
    }
    Ok(())
}

fn invalid_join() -> ReplicationProtocolError {
    ReplicationProtocolError::InvalidTransition {
        context: "hidden first-enable target",
    }
}
