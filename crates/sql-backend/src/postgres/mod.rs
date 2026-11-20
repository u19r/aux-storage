//! Postgres native driver; domain SQL text and row layout live above it.

mod attempt;
mod driver;
mod error;
mod input_limits;
mod network;
mod pool;
#[cfg(feature = "fault-injection")]
mod pool_test_support;
#[cfg(test)]
mod pool_tests;
mod queue_wake;
mod record_limits;
mod statements;
#[cfg(feature = "test-support")]
pub mod test_schema;
mod value;

#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod transaction_tests;

pub use attempt::PostgresAttempt;
pub use driver::PostgresBackend;

#[cfg(test)]
mod attempt_tests;

#[cfg(test)]
mod error_tests;

#[cfg(test)]
mod item_lock_tests;

#[cfg(test)]
mod read_batch_tests;

#[cfg(test)]
mod write_observation_tests;

#[cfg(test)]
mod write_batch_tests;

#[cfg(test)]
mod catalog_lock_tests;

#[cfg(test)]
mod queue_wake_tests;

#[cfg(test)]
mod driver_tests;

#[cfg(all(test, feature = "fault-injection"))]
mod acquisition_tests;

#[cfg(all(test, feature = "fault-injection"))]
mod connection_cleanup_tests;

#[cfg(test)]
mod owned_server_tests;

#[cfg(all(test, feature = "fault-injection"))]
mod deadline_tests;

#[cfg(all(test, feature = "fault-injection"))]
mod reply_loss_tests;

#[cfg(all(test, feature = "fault-injection"))]
mod lazy_begin_tests;

#[cfg(all(test, feature = "fault-injection"))]
mod commit_tag_tests;

#[cfg(feature = "fault-injection")]
pub mod reply_loss_test_support;
#[cfg(feature = "fault-injection")]
pub use reply_loss_test_support as reply_loss;

#[cfg(test)]
mod owned_server_test_support;
