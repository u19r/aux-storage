//! Provider-neutral ordinary point-read plans (phase 6, §4).
//!
//! `GetItemPlan` and `BatchGetPlan` own validated Get/BatchGet requests and
//! bind them to metadata observations. They depend only on `storage-types`.

mod batch;
mod get;
mod table;

#[cfg(test)]
mod batch_tests;
#[cfg(test)]
mod get_tests;

pub use batch::{BatchGetKeyIdentity, BatchGetPlan, BatchGetTablePlan, BatchGetTableResult};
pub use get::GetItemPlan;
pub use table::PointReadTable;
