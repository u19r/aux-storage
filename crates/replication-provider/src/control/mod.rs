mod addition;
mod capture;
mod creation;
mod joining;
mod metadata;
mod removal;
mod transfer;

use replication_types::{
    configuration::{control::ConfigurationChange, proposal::ControlProposal},
    error::ReplicationProtocolError,
};

/// Controls cannot prepare ahead of their required bootstrap, drain, or schema
/// owner. Each completed flow admits its exact transition here.
pub fn validate_preparation(proposal: &ControlProposal) -> Result<(), ReplicationProtocolError> {
    proposal.validate()?;
    match &proposal.operation().change {
        ConfigurationChange::CreateTable { home } => creation::validate_creation(proposal, *home),
        ConfigurationChange::EnableGlobal { .. } => {
            capture::source_capture_state(proposal).map(|_| ())
        }
        ConfigurationChange::AddRegion { .. } => addition::validate_addition(proposal),
        ConfigurationChange::TtlPolicy { .. } | ConfigurationChange::TableMetadata { .. } => {
            metadata::validate_metadata_control(proposal)
        }
        ConfigurationChange::RemoveRegion { .. }
        | ConfigurationChange::DisableGlobal
        | ConfigurationChange::TransferCoordinator { .. } => validate_lifecycle(proposal),
        _ => Err(ReplicationProtocolError::InvalidTransition {
            context: "control prerequisite owner is not installed",
        }),
    }
}

pub use capture::{source_capture_state, validate_capture_source};
pub use joining::validate_joining_definition;
pub use metadata::bind_metadata_control;
pub use removal::{bind_removal, retiring_regions, validate_removal};
pub use transfer::bind_transfer;

/// The durable pending outbox serves lifecycle and qualified metadata controls.
pub fn validate_pending_control(
    proposal: &ControlProposal,
) -> Result<(), ReplicationProtocolError> {
    proposal.validate()?;
    match &proposal.operation().change {
        ConfigurationChange::TtlPolicy { .. } | ConfigurationChange::TableMetadata { .. } => {
            metadata::validate_metadata_control(proposal)
        }
        _ => validate_lifecycle(proposal),
    }
}

/// Accepted lifecycle outbox work shares one prerequisite validator. Bootstrap
/// and metadata controls enter only through their respective domain owners.
pub fn validate_lifecycle(proposal: &ControlProposal) -> Result<(), ReplicationProtocolError> {
    proposal.validate()?;
    match &proposal.operation().change {
        ConfigurationChange::RemoveRegion { .. } | ConfigurationChange::DisableGlobal => {
            removal::validate_removal(proposal)
        }
        ConfigurationChange::TransferCoordinator { target } => {
            transfer::validate_transfer(proposal, target)
        }
        _ => Err(ReplicationProtocolError::InvalidTransition {
            context: "unsupported regional lifecycle control",
        }),
    }
}

pub use addition::{addition_source, borrow_addition_source, validate_addition};
