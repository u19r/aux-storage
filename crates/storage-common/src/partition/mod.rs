//! Canonical partition routing, topology and pressure decisions for shared
//! owners.
mod decision;
mod error;
mod lifecycle;
mod models;
mod validation;

pub use error::PartitionTransitionError;
pub use models::*;

#[cfg(test)]
mod decision_tests;
#[cfg(test)]
mod validation_tests;
