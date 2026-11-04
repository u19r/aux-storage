//! One retention format and transaction lowering for all three SQL drivers.

#[cfg(all(test, feature = "sqlite"))]
mod discovery_tests;
#[cfg(all(test, feature = "sqlite"))]
mod lineage_tests;
mod read;
mod repository;
mod sql_statements;
mod write;

pub(crate) use read::load_stream_trim_states;
pub use write::{initialize_retention, write_stream_retention};
