use replication_types::configuration::proposal::ControlProposal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ControlDecision {
    Prepared,
    Committed,
    Cancelled,
    Installed,
    Completed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ControlRecord {
    pub(crate) proposal: ControlProposal,
    pub(crate) decision: ControlDecision,
    pub(crate) retirement: Vec<super::RetiredSealEvidence>,
    pub(crate) bootstrap: Option<replication_types::protocol::CanonicalDigest>,
}

impl ControlRecord {
    pub(crate) fn validate_evidence(&self) -> storage_types::StorageResult<()> {
        Self::require_evidence(
            &self.proposal,
            self.decision,
            self.bootstrap,
            &self.retirement,
        )
    }

    pub(crate) fn require_evidence(
        proposal: &ControlProposal,
        decision: ControlDecision,
        bootstrap: Option<replication_types::protocol::CanonicalDigest>,
        retirement: &[super::RetiredSealEvidence],
    ) -> storage_types::StorageResult<()> {
        use replication_types::configuration::control::ConfigurationChange;
        let bootstrap_required = matches!(
            proposal.operation().change,
            ConfigurationChange::EnableGlobal { .. } | ConfigurationChange::AddRegion { .. }
        );
        if (!bootstrap_required && bootstrap.is_some())
            || (decision != ControlDecision::Cancelled && bootstrap_required != bootstrap.is_some())
        {
            return Err(crate::native_record::corrupt_record());
        }
        Self::require_retirement(proposal, decision, retirement)
    }

    fn require_retirement(
        proposal: &ControlProposal,
        decision: ControlDecision,
        retirement: &[super::RetiredSealEvidence],
    ) -> storage_types::StorageResult<()> {
        use replication_types::configuration::control::ConfigurationChange;

        use crate::native_record::corrupt_record;
        let removal = matches!(
            proposal.operation().change,
            ConfigurationChange::RemoveRegion { .. } | ConfigurationChange::DisableGlobal
        );
        if !removal
            || matches!(
                decision,
                ControlDecision::Prepared | ControlDecision::Cancelled
            )
        {
            return if retirement.is_empty() {
                Ok(())
            } else {
                Err(corrupt_record())
            };
        }
        let retiring = replication_provider::control::retiring_regions(proposal)
            .map_err(|_| corrupt_record())?;
        if retirement.len() != retiring.len()
            || retirement
                .iter()
                .zip(retiring)
                .any(|(evidence, region)| evidence.region != region)
        {
            return Err(corrupt_record());
        }
        Ok(())
    }
}
