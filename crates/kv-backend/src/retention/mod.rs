//! One retention format and transaction lowering for both KV drivers.

#[cfg(all(test, feature = "rocksdb"))]
mod discovery_tests;
mod key_layout;
#[cfg(test)]
mod key_layout_tests;
mod read;
mod repository;
mod write;

#[cfg(test)]
pub(crate) use key_layout::{derived_due_entry, state_key};
#[cfg(test)]
pub(crate) use read::decode_trim_state;
pub(crate) use read::{load_stream_trim_states, load_table_retention};
pub(crate) use write::delete_table_retention;
pub use write::write_stream_retention;
