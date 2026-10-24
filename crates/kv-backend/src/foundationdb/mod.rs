//! Native FoundationDB ordered-key driver. Key layout and domain decisions live
//! above it.

mod attempt;
mod driver;
mod error;
mod input_limits;
mod input_preparation;
mod mutation_preparation;
mod network;
mod read_window;
mod shutdown_observation;

pub use attempt::FoundationDbAttempt;
pub use driver::{FoundationDbBackend, FoundationDbOptions};
pub use network::shutdown_foundationdb_network;

#[cfg(test)]
mod shutdown_observation_tests;
