use replication_types::{
    configuration::{
        control::{ConfigurationChange, ControlOperation},
        installed::{InstalledTableState, RegionalTablePhase},
        proposal::{ControlParticipant, ControlProposal},
        table::RegionalMode,
    },
    error::ReplicationProtocolError,
    identity::{replica::RegionId, resource::ControlOperationId},
};

pub fn retiring_regions(
    proposal: &ControlProposal,
) -> Result<Vec<RegionId>, ReplicationProtocolError> {
    proposal.validate()?;
    let before = proposal.operation().before.as_ref().ok_or(invalid())?;
    let RegionalMode::Global { primary, members } = &before.regional else {
        return Err(invalid());
    };
    match &proposal.operation().change {
        ConfigurationChange::RemoveRegion { region }
            if region != primary && members.contains(region) && members.len() > 2 =>
        {
            Ok(vec![region.clone()])
        }
        ConfigurationChange::DisableGlobal => Ok(members
            .iter()
            .filter(|region| *region != primary)
            .cloned()
            .collect()),
        _ => Err(invalid()),
    }
}

pub fn validate_removal(proposal: &ControlProposal) -> Result<(), ReplicationProtocolError> {
    let retiring = retiring_regions(proposal)?;
    for participant in proposal.participants() {
        let before = participant.before.as_ref().ok_or(invalid())?;
        if before.phase != RegionalTablePhase::Active
            || before.configuration.regional
                != proposal
                    .operation()
                    .before
                    .as_ref()
                    .ok_or(invalid())?
                    .regional
        {
            return Err(invalid());
        }
        let expected = removal_state(
            proposal.operation(),
            before,
            retiring.contains(&participant.region),
        )?;
        if participant.proposed != expected {
            return Err(ReplicationProtocolError::RevisionMismatch {
                context: "graceful removal configuration",
            });
        }
    }
    Ok(())
}

fn removal_state(
    operation: &ControlOperation,
    before: &InstalledTableState,
    retiring: bool,
) -> Result<InstalledTableState, ReplicationProtocolError> {
    let mut next = before.clone();
    next.configuration.config = operation.proposed;
    next.membership = u64::from(before.membership)
        .checked_add(1)
        .ok_or(ReplicationProtocolError::CounterOverflow {
            context: "removal membership",
        })?
        .into();
    next.phase = if retiring {
        RegionalTablePhase::Removed
    } else {
        RegionalTablePhase::Active
    };
    for source in &mut next.sources {
        source.config = operation.proposed;
    }
    next.configuration.regional = match (&operation.change, &before.configuration.regional) {
        (
            ConfigurationChange::RemoveRegion { region },
            RegionalMode::Global { primary, members },
        ) => RegionalMode::Global {
            primary: primary.clone(),
            members: members
                .iter()
                .filter(|member| *member != region)
                .cloned()
                .collect(),
        },
        (ConfigurationChange::DisableGlobal, _) => RegionalMode::Local,
        _ => return Err(invalid()),
    };
    Ok(next)
}

fn invalid() -> ReplicationProtocolError {
    ReplicationProtocolError::InvalidTransition {
        context: "graceful removal requires active global participants and a surviving primary",
    }
}

/// Build from installed configuration evidence, preserving native incarnations.
pub fn bind_removal(
    id: ControlOperationId,
    current: &ControlProposal,
    requested: &[RegionId],
) -> Result<ControlProposal, ReplicationProtocolError> {
    current.validate()?;
    let RegionalMode::Global { primary, members } = &current
        .participants()
        .first()
        .ok_or(invalid())?
        .proposed
        .configuration
        .regional
    else {
        return Err(invalid());
    };
    if requested.is_empty()
        || requested.windows(2).any(|pair| pair[0] >= pair[1])
        || requested
            .iter()
            .any(|region| region == primary || !members.contains(region))
    {
        return Err(invalid());
    }
    let secondary = members
        .iter()
        .filter(|region| *region != primary)
        .cloned()
        .collect::<Vec<_>>();
    let change = if requested == secondary {
        ConfigurationChange::DisableGlobal
    } else if requested.len() == 1 {
        ConfigurationChange::RemoveRegion {
            region: requested[0].clone(),
        }
    } else {
        return Err(invalid());
    };
    let before = &current.participant(primary)?.proposed;
    let operation = ControlOperation {
        id,
        table: before.configuration.table,
        lineage: before.configuration.lineage,
        expected: before.configuration.config,
        proposed: before.configuration.config.checked_next()?,
        content: [0; 32].into(),
        change,
        home_region: primary.clone(),
        participants: members.clone(),
        before: Some(before.configuration.clone()),
    };
    let mut participants = Vec::with_capacity(members.len());
    for region in members {
        let before = current.participant(region)?.proposed.clone();
        let proposed = removal_state(&operation, &before, requested.contains(region))?;
        participants.push(ControlParticipant {
            region: region.clone(),
            before: Some(before),
            proposed,
        });
    }
    let proposal = ControlProposal::bind(operation, participants)?;
    validate_removal(&proposal)?;
    Ok(proposal)
}
