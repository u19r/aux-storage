use replication_types::{
    configuration::{
        control::{ConfigurationChange, ControlOperation},
        installed::RegionalTablePhase,
        proposal::{ControlParticipant, ControlProposal},
        table::RegionalMode,
    },
    error::ReplicationProtocolError,
    identity::resource::ControlOperationId,
};

pub(super) fn validate_metadata_control(
    proposal: &ControlProposal,
) -> Result<(), ReplicationProtocolError> {
    for participant in proposal.participants() {
        let before =
            participant
                .before
                .as_ref()
                .ok_or(ReplicationProtocolError::IncompleteCertificate {
                    context: "metadata installed participant",
                })?;
        let RegionalMode::Global { primary, .. } = &before.configuration.regional else {
            return Err(ReplicationProtocolError::InvalidTransition {
                context: "metadata requires global configuration",
            });
        };
        if before.phase != RegionalTablePhase::Active
            || primary != &proposal.operation().home_region
        {
            return Err(ReplicationProtocolError::InvalidTransition {
                context: "metadata active configuration primary",
            });
        }
        let expected = advance_metadata_state(before, proposal.operation())?;
        if participant.proposed != expected {
            return Err(ReplicationProtocolError::RevisionMismatch {
                context: "metadata control preserves item interpretation and membership",
            });
        }
    }
    Ok(())
}

/// Metadata changes retain lineage and incarnations. Schema changes retain
/// prior revision interpretations while each participant installs the shared
/// definition.
pub fn bind_metadata_control(
    id: ControlOperationId,
    current: &ControlProposal,
    change: ConfigurationChange,
) -> Result<ControlProposal, ReplicationProtocolError> {
    current.validate()?;
    if !matches!(
        change,
        ConfigurationChange::TtlPolicy { .. } | ConfigurationChange::TableMetadata { .. }
    ) {
        return Err(ReplicationProtocolError::InvalidTransition {
            context: "unsupported metadata change",
        });
    }
    let RegionalMode::Global { primary, members } = &current
        .participants()
        .first()
        .ok_or(ReplicationProtocolError::IncompleteCertificate {
            context: "metadata installed members",
        })?
        .proposed
        .configuration
        .regional
    else {
        return Err(ReplicationProtocolError::InvalidTransition {
            context: "metadata requires global configuration",
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
        change,
        home_region: primary.clone(),
        participants: members.clone(),
        before: Some(before.configuration.clone()),
    };
    let participants = members
        .iter()
        .map(|region| {
            let before = current.participant(region)?.proposed.clone();
            let proposed = advance_metadata_state(&before, &operation)?;
            Ok(ControlParticipant {
                region: region.clone(),
                before: Some(before),
                proposed,
            })
        })
        .collect::<Result<Vec<_>, ReplicationProtocolError>>()?;
    let proposal = ControlProposal::bind(operation, participants)?;
    validate_metadata_control(&proposal)?;
    Ok(proposal)
}

fn advance_metadata_state(
    before: &replication_types::configuration::installed::InstalledTableState,
    operation: &ControlOperation,
) -> Result<
    replication_types::configuration::installed::InstalledTableState,
    ReplicationProtocolError,
> {
    let mut proposed = before.clone();
    proposed.configuration.config = operation.proposed;
    for source in &mut proposed.sources {
        source.config = operation.proposed;
    }
    if let ConfigurationChange::TableMetadata { definition, .. } = &operation.change
        && definition.changes_schema()
    {
        proposed.configuration.schema = before.configuration.schema.checked_next()?;
        proposed.schemas.push(proposed.configuration.schema);
    }
    Ok(proposed)
}
