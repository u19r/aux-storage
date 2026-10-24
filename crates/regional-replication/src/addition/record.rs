use replication_types::{
    configuration::{installed::InstalledTableState, proposal::ControlProposal},
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum AdditionPhase {
    Reserving,
    Bootstrapping,
    Controlling,
    Cancelling,
    Completed,
    Cancelled,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AdditionRecord {
    pub content: CanonicalDigest,
    pub plan: super::AddRegionPlan,
    pub reservation: Option<InstalledTableState>,
    pub proposal: Option<ControlProposal>,
    pub phase: AdditionPhase,
}

pub(crate) enum AdditionChange {
    Observe,
    Reserve(Box<InstalledTableState>),
    Advance(AdditionPhase),
    Cancel,
}
