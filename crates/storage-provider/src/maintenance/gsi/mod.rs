mod admission;
mod catalog;
mod execution;
mod handler;
mod models;
mod observation;
#[cfg(test)]
mod observation_model_tests;
#[cfg(test)]
mod observation_test_support;
#[cfg(test)]
mod observation_tests;
mod port;
mod repository;
mod transaction;

pub use handler::GsiBuildHandler;
pub use models::{GsiBuildAdvance, GsiBuildRequest, GsiTransition};
pub use port::{GsiCandidateObservation, GsiPort, GsiScanSource, GsiStreamSource};
pub use repository::{
    GsiBuildRepository, GsiBuildTransaction, GsiCandidateRead, GsiLifecycleRepository,
};

mod scope;
pub use scope::{GsiTargetAdmission, OrdinaryGsiBuild};
pub use transaction::GsiAttempt;
