//! Internal queue service implementation and provider factory.
//!
//! This crate is the public queue facade. Consumers should depend on this crate
//! for queue managers, provider traits, request/response types, and runtime
//! construction.

mod immediate_jobs;
mod manager;
mod operation_metrics;
pub mod provider {
    pub use queue_provider::*;
}
pub use immediate_jobs::*;
pub use manager::*;
pub use queue_provider::*;

#[cfg(test)]
mod immediate_jobs_tests;
#[cfg(test)]
mod queue_correctness_tests;
#[cfg(test)]
mod queue_tests;

mod constants;
mod factory;
pub use factory::create_queue_provider;
