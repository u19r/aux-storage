//! The stream an operation addresses. The operation resolves it on its own
//! native attempt: a user stream by its catalog name, an internal name as a
//! user stream's internal id or a table-history stream.

use storage_types::{StreamName, UserStreamName};

#[derive(Debug, Clone)]
pub enum StreamAddress {
    /// A user stream; its catalog row is read by name.
    User(UserStreamName),
    /// A user stream's internal id (catalog row read by id) or a
    /// table-history append stream (history state read).
    Internal(StreamName),
}

impl From<UserStreamName> for StreamAddress {
    fn from(name: UserStreamName) -> Self {
        Self::User(name)
    }
}

impl From<StreamName> for StreamAddress {
    fn from(stream: StreamName) -> Self {
        Self::Internal(stream)
    }
}
