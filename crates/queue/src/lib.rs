//! Internal queue service implementation.
//!
//! This crate is the public queue facade. Consumers should depend on this crate
//! for queue managers, provider traits and request/response types. A process
//! opens its queue store through `storage::DatabaseManager`.

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

#[cfg(test)]
mod immediate_queue_worker_tests;

#[cfg(test)]
mod queue_publication_tests;

#[cfg(test)]
mod quint_queue_claim_tests;
