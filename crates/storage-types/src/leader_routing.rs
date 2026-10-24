//! Wire identifiers understood by remote storage clients.
//!
//! These belong to the transport contract, independently of a server's
//! consensus implementation. Retain them for callers of existing servers.

pub const STORAGE_LEADER_HINT_HEADER: &str = "x-aux-storage-leader";
pub const STORAGE_NOT_LEADER_ERROR_TYPE: &str = "NotLeaderException";
