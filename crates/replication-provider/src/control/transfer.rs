use replication_types::{
    configuration::{
        control::{ConfigurationChange, ControlOperation},
        installed::RegionalTablePhase,
        proposal::{ControlParticipant, ControlProposal},
        table::RegionalMode,
    },
    error::ReplicationProtocolError,
    identity::{replica::RegionId, resource::ControlOperationId},
};

pub(super) fn validate_transfer(
    proposal: &ControlProposal,
    target: &RegionId,
) -> Result<(), ReplicationProtocolError> {
    for participant in proposal.participants() {
        let before =
            participant
                .before
                .as_ref()
                .ok_or(ReplicationProtocolError::IncompleteCertificate {
                    context: "control transfer installed participant",
                })?;
        let RegionalMode::Global { primary, members } = &before.configuration.regional else {
            return Err(ReplicationProtocolError::InvalidTransition {
                context: "transfer local coordinator",
            });
        };
        if primary != &proposal.operation().home_region
            || primary == target
            || !members.contains(target)
            || before.phase != RegionalTablePhase::Active
        {
            return Err(ReplicationProtocolError::InvalidTransition {
                context: "control transfer active members",
            });
        }
        let mut expected = before.clone();
        expected.configuration.config = proposal.operation().proposed;
        expected.configuration.regional = RegionalMode::Global {
            primary: target.clone(),
            members: members.clone(),
        };
        for source in &mut expected.sources {
            source.config = proposal.operation().proposed;
        }
        if participant.proposed != expected {
            return Err(ReplicationProtocolError::RevisionMismatch {
                context: "control transfer changes only ownership",
            });
        }
    }
    Ok(())
}

/// Bind every active participant from immutable installed configuration
/// evidence. Membership, lineage, physical incarnations and schemas stay fixed.
pub fn bind_transfer(
    id: ControlOperationId,
    current: &ControlProposal,
    target: &RegionId,
) -> Result<ControlProposal, ReplicationProtocolError> {
    current.validate()?;
    let RegionalMode::Global { primary, members } = &current
        .participants()
        .first()
        .ok_or(ReplicationProtocolError::IncompleteCertificate {
            context: "transfer installed members",
        })?
        .proposed
        .configuration
        .regional
    else {
        return Err(ReplicationProtocolError::InvalidTransition {
            context: "transfer requires global configuration",
        });
    };
    let before = &current.participant(primary)?.proposed;
    let operation = ControlOperation {
        id,
        table: before.configuration.table,
        lineage: before.configuration.lineage,
        expected: before.configuration.config,
        proposed: before.configuration.config.checked_next()?,
        content: [0; 32].into(),
        change: ConfigurationChange::TransferCoordinator {
            target: target.clone(),
        },
        home_region: primary.clone(),
        participants: members.clone(),
        before: Some(before.configuration.clone()),
    };
    let mut participants = Vec::with_capacity(members.len());
    for region in members {
        let before = current.participant(region)?.proposed.clone();
        let mut proposed = before.clone();
        proposed.configuration.config = operation.proposed;
        proposed.configuration.regional = RegionalMode::Global {
            primary: target.clone(),
            members: members.clone(),
        };
        for source in &mut proposed.sources {
            source.config = operation.proposed;
        }
        participants.push(ControlParticipant {
            region: region.clone(),
            before: Some(before),
            proposed,
        });
    }
    let proposal = ControlProposal::bind(operation, participants)?;
    validate_transfer(&proposal, target)?;
    Ok(proposal)
}
