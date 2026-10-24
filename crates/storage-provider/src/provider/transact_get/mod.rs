//! Provider-neutral TransactGetItems plan owner (phase 5, §3.1).
//!
//! `TransactGetPlan` performs pure request validation and normalization;
//! `PreparedTransactGet` binds per-table metadata observations and builds the
//! ordered response. The existing `DatabaseManager` fallback migrates onto
//! this owner in P5-S; the native Postgres operation consumes it in P5-I.

mod execute;
mod plan;
mod prepared;
mod projection;
#[cfg(test)]
mod projection_tests;
mod response;

pub use execute::execute_transact_get;
pub use plan::TransactGetPlan;
pub use prepared::PreparedTransactGet;
