//! One retention format and transaction lowering for both KV drivers.

#[cfg(all(test, feature = "rocksdb"))]
mod discovery_tests;
mod key_layout;
mod read;
mod repository;
mod write;

pub(crate) use read::{load_stream_trim_states, load_table_retention};
pub(crate) use write::delete_table_retention;
pub use write::write_stream_retention;
