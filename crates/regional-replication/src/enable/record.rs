use replication_types::{
    configuration::{installed::InstalledTableState, proposal::ControlProposal},
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum EnablePhase {
    Reserving,
    Bootstrapping(u32),
    Controlling,
    Cancelling(u32),
    Completed,
    Cancelled,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EnableRecord {
    pub content: CanonicalDigest,
    pub plan: super::plan::EnablePlan,
    pub reservations: Vec<InstalledTableState>,
    pub proposal: Option<ControlProposal>,
    pub phase: EnablePhase,
}
