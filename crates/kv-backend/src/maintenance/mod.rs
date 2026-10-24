//! Durable task identity and progress layout shared by KV maintenance jobs.
mod admission;
mod identity;
mod key_layout;
mod port;
mod progress;

pub(crate) use admission::read_admission;
pub(crate) use identity::initialize_maintenance;
pub(crate) use progress::{ProgressFence, write_progress};
