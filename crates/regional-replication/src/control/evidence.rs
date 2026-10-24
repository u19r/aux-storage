use replication_types::{
    configuration::{control::ControlOperation, proposal::ControlProposal},
    identity::replica::RegionId,
};

use super::{ControlDecision, ControlRecord};

/// Evidence is issued after native commit or a fresh durable observation.
/// A caller cannot turn an operation candidate into a participant grant.
#[derive(Clone, Debug)]
pub struct DurableGrant {
    pub(crate) proposal: ControlProposal,
    pub(crate) bootstrap: Option<replication_types::protocol::CanonicalDigest>,
    pub(crate) region: RegionId,
}

#[derive(Clone, Debug)]
pub struct CommittedControl {
    pub(crate) proposal: ControlProposal,
    pub(crate) retirement: Vec<super::RetiredSealEvidence>,
    pub(crate) bootstrap: Option<replication_types::protocol::CanonicalDigest>,
}

#[derive(Clone, Debug)]
pub struct CancelledControl {
    pub(crate) proposal: ControlProposal,
    pub(crate) bootstrap: Option<replication_types::protocol::CanonicalDigest>,
}

#[derive(Clone, Debug)]
pub struct DurableInstallation {
    pub(crate) proposal: ControlProposal,
    pub(crate) retirement: Vec<super::RetiredSealEvidence>,
    pub(crate) bootstrap: Option<replication_types::protocol::CanonicalDigest>,
    pub(crate) region: RegionId,
}

#[derive(Clone, Debug)]
pub struct CompletedControl {
    pub(crate) proposal: ControlProposal,
    pub(crate) retirement: Vec<super::RetiredSealEvidence>,
    pub(crate) bootstrap: Option<replication_types::protocol::CanonicalDigest>,
}

#[derive(Clone, Debug)]
pub enum ControlOutcome {
    Prepared(DurableGrant),
    Committed(CommittedControl),
    Cancelled(CancelledControl),
    Installed(DurableInstallation),
    Completed(CompletedControl),
}

impl ControlOutcome {
    pub(crate) fn from_durable(record: ControlRecord, region: RegionId) -> Self {
        match record.decision {
            ControlDecision::Prepared => Self::Prepared(DurableGrant {
                proposal: record.proposal,
                bootstrap: record.bootstrap,
                region,
            }),
            ControlDecision::Committed => Self::Committed(CommittedControl {
                proposal: record.proposal,
                bootstrap: record.bootstrap,
                retirement: record.retirement,
            }),
            ControlDecision::Installed => Self::Installed(DurableInstallation {
                proposal: record.proposal,
                bootstrap: record.bootstrap,
                retirement: record.retirement,
                region,
            }),
            ControlDecision::Completed => Self::Completed(CompletedControl {
                proposal: record.proposal,
                bootstrap: record.bootstrap,
                retirement: record.retirement,
            }),
            ControlDecision::Cancelled => Self::Cancelled(CancelledControl {
                proposal: record.proposal,
                bootstrap: record.bootstrap,
            }),
        }
    }
}

impl DurableGrant {
    pub fn region(&self) -> &RegionId {
        &self.region
    }

    pub fn operation(&self) -> &ControlOperation {
        self.proposal.operation()
    }
}

impl CommittedControl {
    pub fn operation(&self) -> &ControlOperation {
        self.proposal.operation()
    }
}

impl CancelledControl {
    pub fn operation(&self) -> &ControlOperation {
        self.proposal.operation()
    }
}

impl DurableInstallation {
    pub fn region(&self) -> &RegionId {
        &self.region
    }

    pub fn committed_control(&self) -> CommittedControl {
        CommittedControl {
            proposal: self.proposal.clone(),
            bootstrap: self.bootstrap,
            retirement: self.retirement.clone(),
        }
    }
}

impl CompletedControl {
    pub fn operation(&self) -> &ControlOperation {
        self.proposal.operation()
    }
}

impl ControlOutcome {
    pub(crate) fn into_record(self) -> ControlRecord {
        let (proposal, decision, bootstrap, retirement) = match self {
            Self::Prepared(value) => (
                value.proposal,
                ControlDecision::Prepared,
                value.bootstrap,
                Vec::new(),
            ),
            Self::Committed(value) => (
                value.proposal,
                ControlDecision::Committed,
                value.bootstrap,
                value.retirement,
            ),
            Self::Cancelled(value) => (
                value.proposal,
                ControlDecision::Cancelled,
                value.bootstrap,
                Vec::new(),
            ),
            Self::Installed(value) => (
                value.proposal,
                ControlDecision::Installed,
                value.bootstrap,
                value.retirement,
            ),
            Self::Completed(value) => (
                value.proposal,
                ControlDecision::Completed,
                value.bootstrap,
                value.retirement,
            ),
        };
        ControlRecord {
            proposal,
            decision,
            bootstrap,
            retirement,
        }
    }
}
