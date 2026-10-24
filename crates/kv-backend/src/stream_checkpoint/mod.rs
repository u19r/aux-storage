//! Durable logical stream checkpoint receipts and retention frontiers.

pub(crate) mod key_layout;
mod read;
mod write;

pub(crate) use key_layout::frontier_prefix;
pub(crate) use read::{decode_protected_frontiers, read_receipt};
pub(crate) use write::write_checkpoint;
