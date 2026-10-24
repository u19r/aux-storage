use replication_types::{
    configuration::{
        control::{ConfigurationChange, ControlOperation},
        installed::{InstalledTableState, RegionalTablePhase},
        proposal::ControlProposal,
        table::RegionalMode,
    },
    error::ReplicationProtocolError,
};

/// Derive source capture without publishing the proposed global membership.
/// The native caller still owns catalog/slot protection and commit settlement.
pub fn source_capture_state(
    proposal: &ControlProposal,
) -> Result<InstalledTableState, ReplicationProtocolError> {
    proposal.validate()?;
    let ConfigurationChange::EnableGlobal { primary, .. } = &proposal.operation().change else {
        return Err(invalid_capture());
    };
    let source = proposal.participant(primary)?;
    let before = source.before.as_ref().ok_or_else(invalid_capture)?;
    validate_capture_source(proposal.operation(), (before, &source.proposed))?;
    if proposal
        .participants()
        .iter()
        .any(|target| target.region != *primary && target.before.is_some())
    {
        return Err(invalid_capture());
    }
    let mut capture = before.clone();
    capture.phase = RegionalTablePhase::Capturing;
    capture.configuration.lineage = proposal.operation().lineage;
    capture.validate()?;
    Ok(capture)
}

/// Logical source preparation can be checked before target physical allocation.
pub fn validate_capture_source(
    operation: &ControlOperation,
    source: (&InstalledTableState, &InstalledTableState),
) -> Result<(), ReplicationProtocolError> {
    operation.validate()?;
    let ConfigurationChange::EnableGlobal { primary, members } = &operation.change else {
        return Err(invalid_capture());
    };
    let (before, proposed) = source;
    before.validate()?;
    proposed.validate()?;
    if primary != &operation.home_region
        || operation.before.as_ref() != Some(&before.configuration)
        || before.phase != RegionalTablePhase::Active
        || before.configuration.regional != RegionalMode::Local
        || proposed.phase != RegionalTablePhase::Active
        || proposed.physical != before.physical
        || proposed.name != before.name
        || proposed.configuration.table != operation.table
        || proposed.configuration.table != before.configuration.table
        || proposed.configuration.tenant != before.configuration.tenant
        || proposed.configuration.schema != before.configuration.schema
        || proposed.configuration.lineage != operation.lineage
        || proposed.configuration.config != operation.proposed
        || !matches!(&proposed.configuration.regional,
            RegionalMode::Global { primary: candidate_primary, members: candidate_members }
                if candidate_primary == primary && candidate_members == members)
        || u64::from(proposed.membership)
            != u64::from(before.membership)
                .checked_add(1)
                .ok_or_else(invalid_capture)?
        || proposed.schemas != before.schemas
    {
        return Err(invalid_capture());
    }
    Ok(())
}

fn invalid_capture() -> ReplicationProtocolError {
    ReplicationProtocolError::InvalidTransition {
        context: "first global source capture",
    }
}
