mod executor;
mod observation;
mod plan;
mod record;
mod transfer;
pub use executor::{observe_addition_source, reserve_addition_target};
pub use observation::AdditionSourceObservation;
pub use plan::AddRegionPlan;
pub(crate) use record::{AdditionChange, AdditionPhase, AdditionRecord};
pub(crate) use transfer::AdditionBootstrap;

mod job;
mod lifecycle_executor;
mod recovery;
mod runner;
pub use job::AdditionJob;
pub use lifecycle_executor::{accept_addition_plan, cancel_addition_plan};
