//! Internal queue service implementation and provider factory.
//!
//! This crate is the public queue facade. Consumers should depend on this crate
//! for queue managers, provider traits, request/response types, and runtime
//! construction.

mod immediate_jobs;
mod immediate_queue_worker;
mod manager;
mod operation_metrics;
pub use immediate_jobs::*;
pub use immediate_queue_worker::wait_for_immediate_job_receive_retry;
pub use manager::*;
pub use queue_provider::*;

#[cfg(test)]
mod immediate_jobs_tests;
#[cfg(test)]
mod queue_correctness_tests;
#[cfg(test)]
mod queue_test_support_tests;
#[cfg(test)]
mod queue_tests;

mod constants;
#[cfg(test)]
mod constants_tests;
mod factory;
pub use factory::create_queue_provider;

#[cfg(test)]
mod immediate_queue_worker_tests;

#[cfg(test)]
mod queue_publication_tests;
