//! A typed pending pointer must resolve its actual immutable operation before
//! dispatch.
use replication_types::{
    configuration::proposal::ControlProposal, identity::resource::ControlOperationId,
};
use serde::{Deserialize, Serialize};

use crate::trim::proposal::PublicTrimProposal;

#[derive(Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum PendingControlReference {
    Configuration(ControlOperationId),
    Trim(ControlOperationId),
}

impl PendingControlReference {
    pub(crate) fn operation(self) -> ControlOperationId {
        match self {
            Self::Configuration(operation) | Self::Trim(operation) => operation,
        }
    }
}

pub(crate) enum PendingControlWork {
    Configuration(ControlProposal),
    Trim(PublicTrimProposal),
}
