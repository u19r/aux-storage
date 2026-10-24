//! Shared table write preparation (phase 4, §4.1).
//!
//! Wiring only: the member vocabulary lives in `member`/`condition_check`,
//! snapshot selection plus conversion plus validation live in `operation`.

mod condition_check;
mod member;
mod observation;
mod operation;

#[cfg(test)]
mod observation_tests;

pub use condition_check::TableCheckMember;
pub use member::TableWriteMember;
pub use observation::{
    ObservedProvenance, ObservedWriteHistory, ValidatedWriteObservations, WriteObservation,
    WriteObservationIdentity, WriteObservationRequirement, WriteObservationRequirements,
    WriteObservationRole, WriteObservationState, validate_observation_coverage,
};
pub(crate) use observation::{ObservedWriteFinish, decode_write_histories};
pub use operation::TableWritePreparation;
