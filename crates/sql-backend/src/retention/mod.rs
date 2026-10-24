//! One retention format and transaction lowering for all three SQL drivers.

mod boundary;
mod page;
mod pointer;
mod read;
mod repository;
mod sql_statements;
mod write;

pub(crate) use read::{decode_trim_state, load_stream_trim_states};
pub(crate) use sql_statements::state as state_statement;
pub(crate) use write::stream_retention_statements;
pub use write::{initialize_retention, write_stream_retention};
