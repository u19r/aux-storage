//! Provider-neutral ordinary point-read plans (phase 6, §4).
//!
//! `BatchGetPlan` owns validated BatchGet requests and binds them to metadata
//! observations. They depend only on `storage-types`.

mod batch;
mod table;

#[cfg(test)]
mod batch_tests;

pub use batch::{BatchGetKeyIdentity, BatchGetPlan, BatchGetTablePlan, BatchGetTableResult};
pub use table::PointReadTable;
