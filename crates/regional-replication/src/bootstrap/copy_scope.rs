use replication_types::{
    configuration::{
        installed::{RegionalSourceBinding, RegionalTablePhase},
        proposal::ControlProposal,
    },
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use super::{HiddenTargetRequest, TargetBootstrapDefinition};
use crate::native_record::corrupt_record;

/// One immutable source capture and native target incarnation. Scheduler
/// payloads identify work; native records alone acknowledge transferred pages.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BaselineCopyScope {
    pub(crate) proposal: ControlProposal,
    pub(crate) target: HiddenTargetRequest,
    pub(crate) destination: RegionalSourceBinding,
}

impl BaselineCopyScope {
    pub fn try_new(
        proposal: ControlProposal,
        target: HiddenTargetRequest,
        destination: RegionalSourceBinding,
    ) -> StorageResult<Self> {
        let scope = Self {
            proposal,
            target,
            destination,
        };
        scope.validate()?;
        Ok(scope)
    }

    pub(crate) fn validate(&self) -> StorageResult<()> {
        match self.proposal.operation().change {
            replication_types::configuration::control::ConfigurationChange::EnableGlobal {
                ..
            } => {
                replication_provider::control::source_capture_state(&self.proposal)
                    .map_err(|_| corrupt_record())?;
            }
            replication_types::configuration::control::ConfigurationChange::AddRegion {
                ..
            } => replication_provider::control::validate_addition(&self.proposal)
                .map_err(|_| corrupt_record())?,
            _ => return Err(corrupt_record()),
        }
        let participant = self
            .proposal
            .participant(&self.destination.region)
            .map_err(|_| corrupt_record())?;
        let mut expected = self
            .target
            .target
            .bind_physical(participant.proposed.physical);
        expected.phase = RegionalTablePhase::Active;
        if self.destination.region == self.proposal.operation().home_region
            || participant.before.is_some()
            || participant.proposed != expected
            || !expected.sources.contains(&self.destination)
        {
            return Err(corrupt_record());
        }
        replication_provider::control::validate_joining_definition(
            &self.target.operation,
            (&self.target.source, &self.target.target),
            &self.destination.region,
        )
        .map_err(|_| corrupt_record())?;
        Ok(())
    }

    pub(crate) fn require_definition(
        &self,
        definition: &TargetBootstrapDefinition,
    ) -> StorageResult<()> {
        self.validate()?;
        definition.validate()?;
        if definition.proposal() != &self.proposal
            || !self
                .target
                .matches_capture(definition.proposal(), definition.schema())?
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(crate) fn content(&self) -> StorageResult<CanonicalDigest> {
        CanonicalDigest::of_json(self).map_err(|_| corrupt_record())
    }
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub(crate) struct BaselineCopyBudget {
    pub records: std::num::NonZeroUsize,
    pub bytes: std::num::NonZeroUsize,
}
