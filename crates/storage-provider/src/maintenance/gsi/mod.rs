mod admission;
mod catalog;
mod execution;
mod handler;
mod models;
mod native_index;
mod observation;

mod port;
mod repository;
mod transaction;

pub use handler::GsiBuildHandler;
pub use models::{GsiBuildAdvance, GsiBuildRequest, GsiTransition, PrimaryTtlMetadata};
pub use native_index::{
    NativeGsiFill, NativeGsiPage, NativeGsiProgress, NativeGsiRow, NativeGsiRowVersion,
    NativeGsiTransition, NativeIndexAction, NativeIndexState, NativeIndexWork,
};
pub use port::{GsiPort, GsiScanSource};
pub(crate) use repository::gsi_build_in_attempt;
pub use repository::{GsiBuildTransaction, GsiCandidateRead};

mod scope;
pub use scope::{GsiTargetAdmission, OrdinaryGsiBuild};
pub use transaction::GsiAttempt;
