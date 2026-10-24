use replication_types::{
    configuration::{
        control::ControlOperation,
        installed::{InstalledTableState, RegionalSourceBinding},
        proposal::{ControlParticipant, ControlProposal},
    },
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use crate::{
    bootstrap::{HiddenTargetRequest, SourceSchemaObservation},
    native_record::corrupt_record,
};

/// The physical targets are deliberately absent until their native reservations
/// commit. Every retry retains the same operation, lineage and logical targets.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnablePlan {
    pub(crate) operation: ControlOperation,
    pub(crate) source: InstalledTableState,
    pub(crate) targets: Vec<HiddenTargetRequest>,
}

impl EnablePlan {
    pub fn try_new(
        operation: ControlOperation,
        source: InstalledTableState,
        observation: &SourceSchemaObservation,
        targets: Vec<replication_types::configuration::joining::JoiningTableDefinition>,
    ) -> StorageResult<Self> {
        let targets = targets
            .into_iter()
            .map(|target| HiddenTargetRequest::bind(operation.clone(), target, observation))
            .collect();
        let plan = Self {
            operation,
            source,
            targets,
        };
        plan.validate()?;
        Ok(plan)
    }

    pub(crate) fn validate(&self) -> StorageResult<()> {
        self.operation.validate().map_err(|_| corrupt_record())?;
        self.source.validate().map_err(|_| corrupt_record())?;
        if self.targets.is_empty()
            || self.targets.len().checked_add(1) != Some(self.operation.participants.len())
        {
            return Err(corrupt_record());
        }
        let schema = serde_json::to_vec(&self.targets[0].schema).map_err(|_| corrupt_record())?;
        let mut regions = vec![self.operation.home_region.clone()];
        for target in &self.targets {
            target.schema.validate(&target.target.name)?;
            if serde_json::to_vec(&target.schema).map_err(|_| corrupt_record())? != schema {
                return Err(corrupt_record());
            }
            let destination = self.destination(target)?;
            replication_provider::control::validate_joining_definition(
                &self.operation,
                (&target.source, &target.target),
                &destination.region,
            )
            .map_err(|_| corrupt_record())?;
            let mut operation = self.operation.clone();
            operation.content = [0; 32].into();
            if target.operation != operation || target.source != self.targets[0].source {
                return Err(corrupt_record());
            }
            regions.push(destination.region.clone());
        }
        regions.sort();
        if regions != self.operation.participants {
            return Err(corrupt_record());
        }
        replication_provider::control::validate_capture_source(
            &self.operation,
            (&self.targets[0].source, &self.source),
        )
        .map_err(|_| corrupt_record())?;
        if self.targets.iter().any(|target| {
            target.target.sources != self.source.sources
                || target.target.membership != self.source.membership
                || target.target.schemas != self.source.schemas
                || target.target.configuration.regional != self.source.configuration.regional
        }) {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(crate) fn destination<'a>(
        &self,
        target: &'a HiddenTargetRequest,
    ) -> StorageResult<&'a RegionalSourceBinding> {
        let mut bindings = target
            .target
            .sources
            .iter()
            .filter(|binding| binding.table == target.target.configuration.table);
        let destination = bindings.next().ok_or_else(corrupt_record)?;
        if bindings.next().is_some() || destination.region == self.operation.home_region {
            return Err(corrupt_record());
        }
        Ok(destination)
    }

    pub(crate) fn bind_proposal(
        &self,
        reservations: &[InstalledTableState],
    ) -> StorageResult<ControlProposal> {
        if reservations.len() != self.targets.len() {
            return Err(corrupt_record());
        }
        let mut participants = vec![ControlParticipant {
            region: self.operation.home_region.clone(),
            before: Some(self.targets[0].source.clone()),
            proposed: self.source.clone(),
        }];
        for (index, (target, state)) in self.targets.iter().zip(reservations).enumerate() {
            self.require_reservation(index, state)?;
            participants.push(ControlParticipant {
                region: self.destination(target)?.region.clone(),
                before: None,
                proposed: state.clone(),
            });
        }
        participants.sort_by(|left, right| left.region.cmp(&right.region));
        let proposal = ControlProposal::bind(self.operation.clone(), participants)
            .map_err(|_| corrupt_record())?;
        replication_provider::control::source_capture_state(&proposal)
            .map_err(|_| corrupt_record())?;
        Ok(proposal)
    }

    pub(crate) fn require_reservation(
        &self,
        index: usize,
        state: &InstalledTableState,
    ) -> StorageResult<()> {
        let target = self.targets.get(index).ok_or_else(corrupt_record)?;
        let mut expected = target.target.bind_physical(state.physical);
        expected.phase = replication_types::configuration::installed::RegionalTablePhase::Active;
        if &expected != state {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(crate) fn require_deployment(
        &self,
        configuration: &replication_provider::runtime_configuration::deployment::DeploymentConfiguration,
    ) -> StorageResult<()> {
        self.validate()?;
        if self.operation.home_region != configuration.region {
            return Err(storage_types::StorageError::validation(
                "replica update requires the configuration primary",
            ));
        }
        for target in &self.targets {
            let destination = self.destination(target)?;
            if destination.partitions != configuration.journal_partitions
                || !configuration.peers.iter().any(|peer| {
                    peer.region == destination.region
                        && peer.group == destination.database_group
                        && peer.generation == destination.generation
                })
            {
                return Err(storage_types::StorageError::validation(
                    "replica region has no configured database generation",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn content(&self) -> StorageResult<CanonicalDigest> {
        CanonicalDigest::of_json(self).map_err(|_| corrupt_record())
    }
}
