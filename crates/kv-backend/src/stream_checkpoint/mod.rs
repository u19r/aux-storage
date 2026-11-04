//! Durable logical stream checkpoint receipts and retention frontiers.

pub(crate) mod key_layout;
mod read;
mod write;

pub(crate) use read::read_receipt;
pub(crate) use write::write_checkpoint;
