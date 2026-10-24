use serde::{Deserialize, Serialize};

use super::{control::ControlOperation, installed::InstalledTableState};
use crate::{
    error::ReplicationProtocolError, identity::replica::RegionId, protocol::CanonicalDigest,
};

/// Every participant binds its own physical incarnation to the same decision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlParticipant {
    pub region: RegionId,
    pub before: Option<InstalledTableState>,
    pub proposed: InstalledTableState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlProposal {
    operation: ControlOperation,
    participants: Vec<ControlParticipant>,
}

impl ControlProposal {
    pub fn bind(
        operation: ControlOperation,
        participants: Vec<ControlParticipant>,
    ) -> Result<Self, ReplicationProtocolError> {
        let mut proposal = Self {
            operation,
            participants,
        };
        proposal.operation.content = proposal.content()?;
        proposal.validate()?;
        Ok(proposal)
    }

    pub fn operation(&self) -> &ControlOperation {
        &self.operation
    }

    pub fn participants(&self) -> &[ControlParticipant] {
        &self.participants
    }

    pub fn participant(
        &self,
        region: &RegionId,
    ) -> Result<&ControlParticipant, ReplicationProtocolError> {
        self.participants
            .iter()
            .find(|participant| &participant.region == region)
            .ok_or(ReplicationProtocolError::IncompleteCertificate {
                context: "control participant incarnation",
            })
    }

    pub fn validate(&self) -> Result<(), ReplicationProtocolError> {
        self.operation.validate()?;
        if self.content()? != self.operation.content
            || self.participants.len() != self.operation.participants.len()
            || self
                .participants
                .iter()
                .zip(&self.operation.participants)
                .any(|(target, region)| &target.region != region)
        {
            return Err(ReplicationProtocolError::IdentityContentMismatch {
                context: "control proposal",
            });
        }
        for participant in &self.participants {
            self.validate_participant(participant)?;
        }
        let primary = self.participant(&self.operation.home_region)?;
        if primary.proposed.configuration.table != self.operation.table
            || primary.before.as_ref().map(|before| &before.configuration)
                != self.operation.before.as_ref()
        {
            return Err(ReplicationProtocolError::RevisionMismatch {
                context: "control primary incarnation",
            });
        }
        self.validate_bindings()?;
        Ok(())
    }

    fn validate_bindings(&self) -> Result<(), ReplicationProtocolError> {
        let primary = &self.participant(&self.operation.home_region)?.proposed;
        for participant in &self.participants {
            let state = &participant.proposed;
            if state.name != primary.name
                || state.membership != primary.membership
                || state.configuration.tenant != primary.configuration.tenant
                || state.configuration.schema != primary.configuration.schema
                || state.configuration.regional != primary.configuration.regional
            {
                return Err(ReplicationProtocolError::RevisionMismatch {
                    context: "control shared configuration",
                });
            }
            for target in &self.participants {
                let binding = target
                    .proposed
                    .sources
                    .iter()
                    .find(|source| source.region == target.region);
                if binding.is_none()
                    || state
                        .sources
                        .iter()
                        .find(|source| source.region == target.region)
                        != binding
                {
                    return Err(ReplicationProtocolError::InvalidOrigin {
                        context: "control participant source agreement",
                    });
                }
            }
        }
        Ok(())
    }

    fn validate_participant(
        &self,
        participant: &ControlParticipant,
    ) -> Result<(), ReplicationProtocolError> {
        let next = &participant.proposed;
        next.validate()?;
        if next.configuration.lineage != self.operation.lineage
            || next.configuration.config != self.operation.proposed
        {
            return Err(ReplicationProtocolError::RevisionMismatch {
                context: "control proposed incarnation",
            });
        }
        if let Some(before) = &participant.before {
            before.validate()?;
            if before.name != next.name
                || before.physical != next.physical
                || before.configuration.table != next.configuration.table
                || before.configuration.tenant != next.configuration.tenant
                || before.configuration.config != self.operation.expected
            {
                return Err(ReplicationProtocolError::RevisionMismatch {
                    context: "control expected incarnation",
                });
            }
        }
        if !next.sources.iter().any(|source| {
            source.region == participant.region
                && source.table == next.configuration.table
                && source.config == next.configuration.config
        }) {
            return Err(ReplicationProtocolError::InvalidOrigin {
                context: "control proposed local source",
            });
        }
        Ok(())
    }

    fn content(&self) -> Result<CanonicalDigest, ReplicationProtocolError> {
        let mut operation = self.operation.clone();
        operation.content = [0; 32].into();
        let bytes = serde_json::to_vec(&(&operation, &self.participants)).map_err(|_| {
            ReplicationProtocolError::IdentityContentMismatch {
                context: "control proposal encoding",
            }
        })?;
        Ok(CanonicalDigest::of(&bytes))
    }
}
