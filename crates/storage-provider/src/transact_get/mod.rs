//! Provider-neutral TransactGetItems plan owner (phase 5, §3.1).
//!
//! `TransactGetPlan` performs pure request validation and normalization;
//! `PreparedTransactGet` binds per-table metadata observations and builds the
//! ordered response. The existing `DatabaseManager` fallback migrates onto
//! this owner in P5-S; the native Postgres operation consumes it in P5-I.

pub(crate) mod plan;
mod prepared;

pub use plan::TransactGetPlan;
pub use prepared::PreparedTransactGet;
