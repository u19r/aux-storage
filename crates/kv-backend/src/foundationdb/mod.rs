//! Native FoundationDB ordered-key driver. Key layout and domain decisions live
//! above it.

mod attempt;
mod catalog_pipeline;
#[cfg(test)]
mod deadline_tests;
mod driver;
mod error;
#[cfg(test)]
mod error_tests;
#[cfg(all(test, feature = "fault-injection"))]
mod format_unknown_tests;
mod input_limits;
mod input_preparation;
mod mutation_preparation;
mod network;
mod queue_wake;
#[cfg(test)]
mod queue_wake_tests;
#[cfg(test)]
mod receipt_layout_tests;
mod shutdown_observation;

pub use attempt::FoundationDbAttempt;
pub use driver::{FoundationDbBackend, FoundationDbOptions};
pub use network::shutdown_foundationdb_network;

#[cfg(test)]
mod shutdown_observation_tests;

#[cfg(test)]
mod restored_order_tests;

#[cfg(test)]
mod native_cache_tests;
