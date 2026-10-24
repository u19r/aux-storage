//! Exact active-member transition before AddRegion history preparation.

use replication_types::{
    configuration::{
        control::{ConfigurationChange, ControlOperation},
        installed::{InstalledTableState, RegionalTablePhase},
        joining::JoiningTableDefinition,
        proposal::ControlProposal,
        table::RegionalMode,
    },
    error::ReplicationProtocolError,
    identity::replica::RegionId,
};

pub fn validate_addition(proposal: &ControlProposal) -> Result<(), ReplicationProtocolError> {
    proposal.validate()?;
    let ConfigurationChange::AddRegion { region } = &proposal.operation().change else {
        return Err(invalid_addition());
    };
    let source = proposal
        .participant(&proposal.operation().home_region)?
        .before
        .as_ref()
        .ok_or_else(invalid_addition)?;
    let RegionalMode::Global { primary, members } = &source.configuration.regional else {
        return Err(invalid_addition());
    };
    if primary != &proposal.operation().home_region || members.contains(region) {
        return Err(invalid_addition());
    }
    let mut next_members = members.clone();
    next_members.push(region.clone());
    next_members.sort();
    if proposal.operation().participants != next_members {
        return Err(invalid_addition());
    }
    let target = proposal.participant(region)?;
    if target.before.is_some()
        || target.proposed.phase != RegionalTablePhase::Active
        || source
            .sources
            .iter()
            .any(|binding| binding.table == target.proposed.configuration.table)
    {
        return Err(invalid_addition());
    }
    let logical = JoiningTableDefinition {
        name: target.proposed.name.clone(),
        configuration: target.proposed.configuration.clone(),
        membership: target.proposed.membership,
        sources: target.proposed.sources.clone(),
        schemas: target.proposed.schemas.clone(),
    };
    validate_addition_target(proposal.operation(), (source, &logical), region)?;
    let regional = RegionalMode::Global {
        primary: primary.clone(),
        members: next_members,
    };
    for member in members {
        let participant = proposal.participant(member)?;
        let before = participant.before.as_ref().ok_or_else(invalid_addition)?;
        if before.phase != RegionalTablePhase::Active
            || before.configuration.regional != source.configuration.regional
            || before.membership != source.membership
            || before.configuration.lineage != source.configuration.lineage
            || before.schemas != source.schemas
            || before.sources != source.sources
        {
            return Err(invalid_addition());
        }
        let mut expected = before.clone();
        expected.configuration.config = proposal.operation().proposed;
        expected.configuration.regional = regional.clone();
        expected.membership = target.proposed.membership;
        expected.sources = target.proposed.sources.clone();
        if participant.proposed != expected {
            return Err(invalid_addition());
        }
    }
    Ok(())
}

pub fn addition_source(
    proposal: &ControlProposal,
    region: &RegionId,
) -> Result<InstalledTableState, ReplicationProtocolError> {
    borrow_addition_source(proposal, region).cloned()
}

pub fn borrow_addition_source<'proposal>(
    proposal: &'proposal ControlProposal,
    region: &RegionId,
) -> Result<&'proposal InstalledTableState, ReplicationProtocolError> {
    validate_addition(proposal)?;
    proposal
        .participant(region)?
        .before
        .as_ref()
        .ok_or_else(invalid_addition)
}

pub(super) fn validate_addition_target(
    operation: &ControlOperation,
    input: (&InstalledTableState, &JoiningTableDefinition),
    region: &RegionId,
) -> Result<(), ReplicationProtocolError> {
    operation.validate()?;
    let (source, target) = input;
    source.validate()?;
    target.configuration.validate()?;
    let ConfigurationChange::AddRegion { region: added } = &operation.change else {
        return Err(invalid_addition());
    };
    let RegionalMode::Global { primary, members } = &source.configuration.regional else {
        return Err(invalid_addition());
    };
    let mut next_members = members.clone();
    next_members.push(region.clone());
    next_members.sort();
    if added != region
        || primary != &operation.home_region
        || members.contains(region)
        || operation.before.as_ref() != Some(&source.configuration)
        || source.phase != RegionalTablePhase::Active
        || target.name != source.name
        || target.configuration.table == source.configuration.table
        || target.configuration.tenant != source.configuration.tenant
        || target.configuration.lineage != source.configuration.lineage
        || target.configuration.schema != source.configuration.schema
        || target.configuration.config != operation.proposed
        || target.schemas != source.schemas
        || operation.participants != next_members
        || target.configuration.regional
            != (RegionalMode::Global {
                primary: primary.clone(),
                members: next_members,
            })
        || u64::from(target.membership)
            != u64::from(source.membership)
                .checked_add(1)
                .ok_or_else(invalid_addition)?
        || target
            .sources
            .windows(2)
            .any(|pair| pair[0].region >= pair[1].region)
    {
        return Err(invalid_addition());
    }
    require_added_source_bindings(operation, source, target)
}

fn require_added_source_bindings(
    operation: &ControlOperation,
    source: &InstalledTableState,
    target: &JoiningTableDefinition,
) -> Result<(), ReplicationProtocolError> {
    let ConfigurationChange::AddRegion { region } = &operation.change else {
        return Err(invalid_addition());
    };
    let added = target
        .sources
        .iter()
        .find(|binding| &binding.region == region)
        .ok_or_else(invalid_addition)?;
    if added.table != target.configuration.table
        || added.config != operation.proposed
        || source.sources.iter().any(|old| old.table == added.table)
        || source
            .sources
            .iter()
            .any(|old| &old.region == region && old.generation == added.generation)
    {
        return Err(invalid_addition());
    }
    for old in &source.sources {
        if &old.region == region {
            continue;
        }
        let mut expected = old.clone();
        expected.config = operation.proposed;
        if !target.sources.contains(&expected) {
            return Err(invalid_addition());
        }
    }
    if target.sources.len()
        != source.sources.len()
            + usize::from(
                !source
                    .sources
                    .iter()
                    .any(|binding| &binding.region == region),
            )
    {
        return Err(invalid_addition());
    }
    Ok(())
}

fn invalid_addition() -> ReplicationProtocolError {
    ReplicationProtocolError::InvalidTransition {
        context: "active global history addition",
    }
}
