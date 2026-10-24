use replication_types::configuration::{
    control::{ConfigurationChange, ControlOperation},
    installed::{InstalledTableState, RegionalSourceBinding},
    joining::JoiningTableDefinition,
    proposal::{ControlParticipant, ControlProposal},
    table::RegionalMode,
};
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use super::AdditionSourceObservation;
use crate::{
    bootstrap::{HiddenTargetRequest, HiddenTargetReservation},
    native_record::corrupt_record,
};

/// Frozen existing incarnations and one logical target; its physical ID is
/// native-only.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AddRegionPlan {
    pub(crate) operation: ControlOperation,
    before: Vec<ControlParticipant>,
    pub(crate) target: HiddenTargetRequest,
}

impl AddRegionPlan {
    pub fn try_new(
        operation: ControlOperation,
        observation: &AdditionSourceObservation,
        target: JoiningTableDefinition,
    ) -> StorageResult<Self> {
        let RegionalMode::Global { members, .. } = &observation.source().configuration.regional
        else {
            return Err(corrupt_record());
        };
        let before = members
            .iter()
            .map(|region| {
                let state = observation
                    .current
                    .participant(region)
                    .map_err(|_| corrupt_record())?
                    .proposed
                    .clone();
                Ok(ControlParticipant {
                    region: region.clone(),
                    before: Some(state.clone()),
                    proposed: state,
                })
            })
            .collect::<StorageResult<Vec<_>>>()?;
        let target = HiddenTargetRequest::bind(operation.clone(), target, &observation.schema);
        let plan = Self {
            operation,
            before,
            target,
        };
        plan.validate()?;
        Ok(plan)
    }

    pub(crate) fn validate(&self) -> StorageResult<()> {
        let destination = self.destination()?;
        replication_provider::control::validate_joining_definition(
            &self.operation,
            (&self.target.source, &self.target.target),
            &destination.region,
        )
        .map_err(|_| corrupt_record())?;
        self.target.schema.validate(&self.target.target.name)?;
        let mut operation = self.operation.clone();
        operation.content = [0; 32].into();
        if operation != self.target.operation {
            return Err(corrupt_record());
        }
        let RegionalMode::Global { members, .. } = &self.target.source.configuration.regional
        else {
            return Err(corrupt_record());
        };
        if self.before.len() != members.len()
            || self
                .before
                .iter()
                .zip(members)
                .any(|(participant, region)| {
                    &participant.region != region
                        || participant.before.as_ref() != Some(&participant.proposed)
                })
        {
            return Err(corrupt_record());
        }
        let primary = self
            .before
            .iter()
            .find(|participant| participant.region == operation.home_region)
            .ok_or_else(corrupt_record)?;
        if primary.proposed != self.target.source {
            return Err(corrupt_record());
        }
        for participant in &self.before {
            let state = &participant.proposed;
            state.validate().map_err(|_| corrupt_record())?;
            if state.phase
                != replication_types::configuration::installed::RegionalTablePhase::Active
                || state.configuration.regional != self.target.source.configuration.regional
                || state.configuration.lineage != self.operation.lineage
                || state.configuration.tenant != self.target.source.configuration.tenant
                || state.membership != self.target.source.membership
                || state.sources != self.target.source.sources
                || state.schemas != self.target.source.schemas
            {
                return Err(corrupt_record());
            }
        }
        Ok(())
    }

    pub(crate) fn require_current(&self, current: &ControlProposal) -> StorageResult<()> {
        for participant in &self.before {
            let installed = current
                .participant(&participant.region)
                .map_err(|_| corrupt_record())?;
            if installed.proposed != participant.proposed {
                return Err(corrupt_record());
            }
        }
        Ok(())
    }

    pub fn target(&self) -> &HiddenTargetRequest {
        &self.target
    }

    pub fn bind_reservation(
        &self,
        reservation: &HiddenTargetReservation,
    ) -> StorageResult<ControlProposal> {
        let state = reservation.proposed();
        self.require_reservation(reservation.operation(), reservation.content(), &state)?;
        self.bind_reserved_state(state)
    }

    pub fn bind_remote_reservation(
        &self,
        reservation: &crate::transport::RemoteTargetReservation,
    ) -> StorageResult<ControlProposal> {
        self.require_reservation(
            reservation.operation(),
            reservation.content(),
            reservation.proposed(),
        )?;
        self.bind_reserved_state(reservation.proposed().clone())
    }

    pub(crate) fn require_reservation(
        &self,
        operation: replication_types::identity::resource::ControlOperationId,
        content: replication_types::protocol::CanonicalDigest,
        state: &InstalledTableState,
    ) -> StorageResult<()> {
        self.validate()?;
        let mut expected = self.target.target.bind_physical(state.physical);
        expected.phase = replication_types::configuration::installed::RegionalTablePhase::Active;
        if operation != self.operation.id || content != self.target.content()? || state != &expected
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(crate) fn bind_reserved_state(
        &self,
        state: InstalledTableState,
    ) -> StorageResult<ControlProposal> {
        let mut participants = self.before.clone();
        for participant in &mut participants {
            participant.proposed.configuration.config = self.operation.proposed;
            participant.proposed.configuration.regional = state.configuration.regional.clone();
            participant.proposed.membership = state.membership;
            participant.proposed.sources = state.sources.clone();
        }
        participants.push(ControlParticipant {
            region: self.destination()?.region.clone(),
            before: None,
            proposed: state,
        });
        participants.sort_by(|left, right| left.region.cmp(&right.region));
        let proposal = ControlProposal::bind(self.operation.clone(), participants)
            .map_err(|_| corrupt_record())?;
        replication_provider::control::validate_addition(&proposal)
            .map_err(|_| corrupt_record())?;
        Ok(proposal)
    }

    pub fn destination(&self) -> StorageResult<&RegionalSourceBinding> {
        let ConfigurationChange::AddRegion { region } = &self.operation.change else {
            return Err(corrupt_record());
        };
        self.target
            .target
            .sources
            .iter()
            .find(|source| &source.region == region)
            .ok_or_else(corrupt_record)
    }

    pub(crate) fn require_deployment(
        &self,
        configuration: &replication_provider::runtime_configuration::deployment::DeploymentConfiguration,
    ) -> StorageResult<()> {
        self.validate()?;
        if self.operation.home_region != configuration.region {
            return Err(corrupt_record());
        }
        for source in &self.target.target.sources {
            if source.region == configuration.region {
                continue;
            }
            // Retired generations belong to ancestry, rather than the live
            // endpoint set.
            if !self.operation.participants.contains(&source.region) {
                continue;
            }
            if source.partitions != configuration.journal_partitions
                || !configuration.peers.iter().any(|peer| {
                    peer.region == source.region
                        && peer.group == source.database_group
                        && peer.generation == source.generation
                })
            {
                return Err(corrupt_record());
            }
        }
        Ok(())
    }

    pub(crate) fn content(&self) -> StorageResult<replication_types::protocol::CanonicalDigest> {
        self.validate()?;
        replication_types::protocol::CanonicalDigest::of_json(self).map_err(|_| corrupt_record())
    }
}
