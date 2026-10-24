//! Real-provider physical, receipt, and regional operation fixtures.
pub mod commit_fault;
#[cfg(test)]
mod commit_hold;
pub mod fixture;
#[cfg(test)]
mod fixture_tests;
#[cfg(test)]
mod format_tests;
#[cfg(feature = "native-foundationdb")]
pub mod foundationdb;
#[cfg(test)]
mod job_claim;
#[cfg(all(test, feature = "native-sqlite"))]
mod job_outcome_settlement_tests;
#[cfg(test)]
mod native_job_settlement;
#[cfg(test)]
mod physical_tests;
#[cfg(test)]
mod protected_read_tests;
#[cfg(test)]
mod record_settlement_tests;
#[cfg(feature = "native-rocksdb")]
pub mod rocksdb;
#[cfg(test)]
mod stream_physical_tests;
#[cfg(feature = "native-turso")]
pub mod turso;
#[cfg(test)]
mod work_budget_tests;
